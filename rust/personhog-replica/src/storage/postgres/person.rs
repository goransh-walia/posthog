use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use sqlx::postgres::PgPool;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use personhog_common::grpc::{current_client_name, current_method_name};

use super::{PostgresStorage, DB_BULK_CHUNKS, DB_QUERY_DURATION, DB_ROWS_RETURNED};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::traits::PersonLookup;
use crate::storage::types::{Person, SplitResult, TombstonedDeleteOutcome, TrimOutcome};

/// Version offset for split person/PDI rows — mirrors the Django convention.
const SPLIT_VERSION_OFFSET: i64 = 101;

const POOL_LABEL: &str = "replica";
const BULK_POOL_LABEL: &str = "bulk_replica";

#[async_trait]
impl PersonLookup for PostgresStorage {
    async fn get_person_by_id(
        &self,
        team_id: i64,
        person_id: i64,
    ) -> StorageResult<Option<Person>> {
        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            ("operation".to_string(), "get_person_by_id".to_string()),
            ("pool".to_string(), POOL_LABEL.to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let mut conn = PostgresStorage::acquire_timed(&self.replica_pool, POOL_LABEL).await?;

        let row = sqlx::query_as!(
            Person,
            r#"
            SELECT id, uuid, team_id::bigint as "team_id!", properties::text as "properties?",
                   properties_last_updated_at::text as "properties_last_updated_at?",
                   properties_last_operation::text as "properties_last_operation?",
                   created_at, version, is_identified,
                   CASE WHEN is_user_id IS NULL THEN NULL ELSE (is_user_id != 0) END as is_user_id,
                   last_seen_at
            FROM posthog_person
            WHERE team_id = $1 AND id = $2 AND is_deleted = false
            "#,
            team_id as i32,
            person_id
        )
        .fetch_optional(&mut *conn)
        .await?;

        Ok(row)
    }

    async fn get_person_by_uuid(&self, team_id: i64, uuid: Uuid) -> StorageResult<Option<Person>> {
        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            ("operation".to_string(), "get_person_by_uuid".to_string()),
            ("pool".to_string(), POOL_LABEL.to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let mut conn = PostgresStorage::acquire_timed(&self.replica_pool, POOL_LABEL).await?;

        let row = sqlx::query_as!(
            Person,
            r#"
            SELECT id, uuid, team_id::bigint as "team_id!", properties::text as "properties?",
                   properties_last_updated_at::text as "properties_last_updated_at?",
                   properties_last_operation::text as "properties_last_operation?",
                   created_at, version, is_identified,
                   CASE WHEN is_user_id IS NULL THEN NULL ELSE (is_user_id != 0) END as is_user_id,
                   last_seen_at
            FROM posthog_person
            WHERE team_id = $1 AND uuid = $2 AND is_deleted = false
            "#,
            team_id as i32,
            uuid
        )
        .fetch_optional(&mut *conn)
        .await?;

        Ok(row)
    }

    async fn get_persons_by_ids(
        &self,
        team_id: i64,
        person_ids: &[i64],
        include_properties: bool,
    ) -> StorageResult<Vec<Person>> {
        if person_ids.is_empty() {
            return Ok(Vec::new());
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            ("operation".to_string(), "get_persons_by_ids".to_string()),
            ("pool".to_string(), BULK_POOL_LABEL.to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let pool = self.bulk_replica_pool.clone();
        let chunks: Vec<Vec<i64>> = person_ids
            .chunks(self.bulk_chunk_size)
            .map(|c| c.to_vec())
            .collect();
        common_metrics::histogram(
            DB_BULK_CHUNKS,
            &[("operation".to_string(), "get_persons_by_ids".to_string())],
            chunks.len() as f64,
        );
        let results: Vec<Vec<Person>> = stream::iter(chunks.into_iter().map(|chunk| {
            let pool = pool.clone();
            async move {
                let mut conn =
                    PostgresStorage::acquire_timed(&pool, BULK_POOL_LABEL).await?;
                let rows = sqlx::query_as!(
                    Person,
                    r#"
                    SELECT id, uuid, team_id::bigint as "team_id!",
                           CASE WHEN $3::boolean THEN properties::text ELSE NULL END as "properties?",
                           CASE WHEN $3::boolean THEN properties_last_updated_at::text ELSE NULL END as "properties_last_updated_at?",
                           CASE WHEN $3::boolean THEN properties_last_operation::text ELSE NULL END as "properties_last_operation?",
                           created_at, version, is_identified,
                           CASE WHEN is_user_id IS NULL THEN NULL ELSE (is_user_id != 0) END as is_user_id,
                           last_seen_at
                    FROM posthog_person
                    WHERE team_id = $1 AND id = ANY($2) AND is_deleted = false
                    "#,
                    team_id as i32,
                    &chunk,
                    include_properties
                )
                .fetch_all(&mut *conn)
                .await?;
                Ok::<_, StorageError>(rows)
            }
        }))
        .buffer_unordered(self.bulk_max_concurrent_chunks)
        .try_collect()
        .await?;

        let rows: Vec<Person> = results.into_iter().flatten().collect();
        common_metrics::histogram(
            DB_ROWS_RETURNED,
            &[
                ("operation".to_string(), "get_persons_by_ids".to_string()),
                ("client".to_string(), client.to_string()),
                ("method".to_string(), method.to_string()),
            ],
            rows.len() as f64,
        );

        Ok(rows)
    }

    async fn get_persons_by_uuids(
        &self,
        team_id: i64,
        uuids: &[Uuid],
        include_properties: bool,
    ) -> StorageResult<Vec<Person>> {
        if uuids.is_empty() {
            return Ok(Vec::new());
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            ("operation".to_string(), "get_persons_by_uuids".to_string()),
            ("pool".to_string(), BULK_POOL_LABEL.to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let pool = self.bulk_replica_pool.clone();
        let chunks: Vec<Vec<Uuid>> = uuids
            .chunks(self.bulk_chunk_size)
            .map(|c| c.to_vec())
            .collect();
        common_metrics::histogram(
            DB_BULK_CHUNKS,
            &[("operation".to_string(), "get_persons_by_uuids".to_string())],
            chunks.len() as f64,
        );
        let results: Vec<Vec<Person>> = stream::iter(chunks.into_iter().map(|chunk| {
            let pool = pool.clone();
            async move {
                let mut conn =
                    PostgresStorage::acquire_timed(&pool, BULK_POOL_LABEL).await?;
                let rows = sqlx::query_as!(
                    Person,
                    r#"
                    SELECT id, uuid, team_id::bigint as "team_id!",
                           CASE WHEN $3::boolean THEN properties::text ELSE NULL END as "properties?",
                           CASE WHEN $3::boolean THEN properties_last_updated_at::text ELSE NULL END as "properties_last_updated_at?",
                           CASE WHEN $3::boolean THEN properties_last_operation::text ELSE NULL END as "properties_last_operation?",
                           created_at, version, is_identified,
                           CASE WHEN is_user_id IS NULL THEN NULL ELSE (is_user_id != 0) END as is_user_id,
                           last_seen_at
                    FROM posthog_person
                    WHERE team_id = $1 AND uuid = ANY($2) AND is_deleted = false
                    "#,
                    team_id as i32,
                    &chunk,
                    include_properties
                )
                .fetch_all(&mut *conn)
                .await?;
                Ok::<_, StorageError>(rows)
            }
        }))
        .buffer_unordered(self.bulk_max_concurrent_chunks)
        .try_collect()
        .await?;

        let rows: Vec<Person> = results.into_iter().flatten().collect();
        common_metrics::histogram(
            DB_ROWS_RETURNED,
            &[
                ("operation".to_string(), "get_persons_by_uuids".to_string()),
                ("client".to_string(), client.to_string()),
                ("method".to_string(), method.to_string()),
            ],
            rows.len() as f64,
        );

        Ok(rows)
    }

    async fn get_person_by_distinct_id(
        &self,
        team_id: i64,
        distinct_id: &str,
    ) -> StorageResult<Option<Person>> {
        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "get_person_by_distinct_id".to_string(),
            ),
            ("pool".to_string(), POOL_LABEL.to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let mut conn = PostgresStorage::acquire_timed(&self.replica_pool, POOL_LABEL).await?;

        let row = sqlx::query_as!(
            Person,
            r#"
            SELECT p.id, p.uuid, p.team_id::bigint as "team_id!", p.properties::text as "properties?",
                   p.properties_last_updated_at::text as "properties_last_updated_at?",
                   p.properties_last_operation::text as "properties_last_operation?",
                   p.created_at, p.version, p.is_identified,
                   CASE WHEN p.is_user_id IS NULL THEN NULL ELSE (p.is_user_id != 0) END as is_user_id,
                   p.last_seen_at
            FROM posthog_person p
            INNER JOIN posthog_persondistinctid d ON p.id = d.person_id AND p.team_id = d.team_id
            WHERE p.team_id = $1 AND d.distinct_id = $2
              AND p.is_deleted = false AND d.is_deleted = false
            LIMIT 1
            "#,
            team_id as i32,
            distinct_id
        )
        .fetch_optional(&mut *conn)
        .await?;

        Ok(row)
    }

    async fn get_persons_by_distinct_ids_in_team(
        &self,
        team_id: i64,
        distinct_ids: &[String],
        include_properties: bool,
    ) -> StorageResult<Vec<(String, Option<Person>)>> {
        if distinct_ids.is_empty() {
            return Ok(Vec::new());
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "get_persons_by_distinct_ids_in_team".to_string(),
            ),
            ("pool".to_string(), BULK_POOL_LABEL.to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let pool = self.bulk_replica_pool.clone();
        // Drive from the supplied distinct IDs via UNNEST and probe the
        // (team_id, distinct_id) index, rather than `d.distinct_id = ANY($2)`
        // with the team predicate parked on `posthog_person`. The latter lets
        // the planner start from every person in the team and hash-join, which
        // is catastrophic for large teams. Deduplicate the input first so
        // UNNEST emits one row per distinct ID; the response below still
        // mirrors the caller's original list, repeats included.
        let mut seen: HashSet<&str> = HashSet::with_capacity(distinct_ids.len());
        let unique_ids: Vec<&str> = distinct_ids
            .iter()
            .map(|d| d.as_str())
            .filter(|&d| seen.insert(d))
            .collect();
        let chunks: Vec<Vec<String>> = unique_ids
            .chunks(self.bulk_chunk_size)
            .map(|c| c.iter().map(|&s| s.to_string()).collect())
            .collect();
        common_metrics::histogram(
            DB_BULK_CHUNKS,
            &[(
                "operation".to_string(),
                "get_persons_by_distinct_ids_in_team".to_string(),
            )],
            chunks.len() as f64,
        );
        let chunk_results: Vec<Vec<(String, Person)>> =
            stream::iter(chunks.into_iter().map(|chunk| {
                let pool = pool.clone();
                async move {
                    let mut conn =
                        PostgresStorage::acquire_timed(&pool, BULK_POOL_LABEL).await?;
                    let rows = sqlx::query!(
                        r#"
                        SELECT p.id, p.uuid as "uuid!", p.team_id::bigint as "team_id!",
                               CASE WHEN $3::boolean THEN p.properties::text ELSE NULL END as "properties?",
                               CASE WHEN $3::boolean THEN p.properties_last_updated_at::text ELSE NULL END as "properties_last_updated_at?",
                               CASE WHEN $3::boolean THEN p.properties_last_operation::text ELSE NULL END as "properties_last_operation?",
                               p.created_at as "created_at!", p.version, p.is_identified as "is_identified!",
                               CASE WHEN p.is_user_id IS NULL THEN NULL ELSE (p.is_user_id != 0) END as is_user_id,
                               p.last_seen_at,
                               d.distinct_id as "distinct_id!"
                        FROM UNNEST($2::text[]) AS batch(distinct_id)
                        INNER JOIN posthog_persondistinctid d
                            ON d.team_id = $1 AND d.distinct_id = batch.distinct_id
                            AND d.is_deleted = false
                        INNER JOIN posthog_person p
                            ON p.id = d.person_id AND p.team_id = d.team_id
                            AND p.is_deleted = false
                        "#,
                        team_id as i32,
                        &chunk,
                        include_properties
                    )
                    .fetch_all(&mut *conn)
                    .await?;
                    Ok::<_, StorageError>(
                        rows.into_iter()
                            .map(|row| {
                                let person = Person {
                                    id: row.id,
                                    uuid: row.uuid,
                                    team_id: row.team_id,
                                    properties: row.properties,
                                    properties_last_updated_at: row.properties_last_updated_at,
                                    properties_last_operation: row.properties_last_operation,
                                    created_at: row.created_at,
                                    version: row.version,
                                    is_identified: row.is_identified,
                                    is_user_id: row.is_user_id,
                                    last_seen_at: row.last_seen_at,
                                };
                                (row.distinct_id, person)
                            })
                            .collect(),
                    )
                }
            }))
            .buffer_unordered(self.bulk_max_concurrent_chunks)
            .try_collect()
            .await?;

        let mut found: HashMap<String, Person> = chunk_results.into_iter().flatten().collect();

        common_metrics::histogram(
            DB_ROWS_RETURNED,
            &[
                (
                    "operation".to_string(),
                    "get_persons_by_distinct_ids_in_team".to_string(),
                ),
                ("client".to_string(), client.to_string()),
                ("method".to_string(), method.to_string()),
            ],
            found.len() as f64,
        );

        Ok(distinct_ids
            .iter()
            .map(|did| (did.clone(), found.remove(did)))
            .collect())
    }

    async fn delete_persons(&self, team_id: i64, uuids: &[Uuid]) -> StorageResult<i64> {
        if uuids.is_empty() {
            return Ok(0);
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            ("operation".to_string(), "delete_persons".to_string()),
            ("pool".to_string(), "bulk_primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        // Resolve UUIDs to integer IDs in one query, then chunk and delete
        // by ID. This avoids scanning the UUID index per-chunk.
        let person_ids: Vec<i64> = sqlx::query_scalar!(
            r#"
            SELECT id::bigint as "id!" FROM posthog_person
            WHERE team_id = $1 AND uuid = ANY($2)
            "#,
            team_id as i32,
            uuids
        )
        .fetch_all(&self.bulk_primary_pool)
        .await?;

        if person_ids.is_empty() {
            return Ok(0);
        }

        // Split into fixed-size chunks and delete concurrently. On the first
        // error, stop starting new chunks and return the error. Chunks that
        // already committed are durable; the caller retries the full UUID
        // list and already-deleted UUIDs are idempotent no-ops.
        let pool = self.bulk_primary_pool.clone();
        let chunks: Vec<Vec<i64>> = person_ids
            .chunks(self.bulk_chunk_size)
            .map(|c| c.to_vec())
            .collect();
        common_metrics::histogram(
            DB_BULK_CHUNKS,
            &[("operation".to_string(), "delete_persons".to_string())],
            chunks.len() as f64,
        );
        let results: Vec<i64> =
            stream::iter(
                chunks.into_iter().map(|chunk| {
                    let pool = pool.clone();
                    let client = client.clone();
                    // Per-person delete: also clear cohort memberships (no DB cascade).
                    async move {
                        delete_persons_by_ids_chunk(&pool, team_id, &chunk, &client, true).await
                    }
                }),
            )
            .buffer_unordered(self.bulk_max_concurrent_chunks)
            .try_collect()
            .await?;

        Ok(results.iter().sum())
    }

    async fn delete_persons_batch_for_team(
        &self,
        team_id: i64,
        batch_size: i64,
    ) -> StorageResult<i64> {
        if batch_size <= 0 {
            return Ok(0);
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "delete_persons_batch_for_team".to_string(),
            ),
            ("pool".to_string(), "bulk_primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        // Select up to batch_size person IDs.
        let person_ids: Vec<i64> = sqlx::query_scalar!(
            r#"
            SELECT id::bigint as "id!" FROM posthog_person
            WHERE team_id = $1
            LIMIT $2
            "#,
            team_id as i32,
            batch_size
        )
        .fetch_all(&self.bulk_primary_pool)
        .await?;

        if person_ids.is_empty() {
            return Ok(0);
        }

        // Split into fixed-size chunks and delete concurrently.
        let pool = self.bulk_primary_pool.clone();
        let chunks: Vec<Vec<i64>> = person_ids
            .chunks(self.bulk_chunk_size)
            .map(|c| c.to_vec())
            .collect();
        common_metrics::histogram(
            DB_BULK_CHUNKS,
            &[(
                "operation".to_string(),
                "delete_persons_batch_for_team".to_string(),
            )],
            chunks.len() as f64,
        );
        let results: Vec<i64> =
            stream::iter(
                chunks.into_iter().map(|chunk| {
                    let pool = pool.clone();
                    let client = client.clone();
                    // Team teardown clears cohortpeople separately, by cohort, before this runs.
                    async move {
                        delete_persons_by_ids_chunk(&pool, team_id, &chunk, &client, false).await
                    }
                }),
            )
            .buffer_unordered(self.bulk_max_concurrent_chunks)
            .try_collect()
            .await?;

        Ok(results.iter().sum())
    }

    async fn delete_tombstoned_persons(
        &self,
        team_id: i64,
        uuids: &[Uuid],
    ) -> StorageResult<TombstonedDeleteOutcome> {
        if uuids.is_empty() {
            return Ok(TombstonedDeleteOutcome::default());
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "delete_tombstoned_persons".to_string(),
            ),
            ("pool".to_string(), "bulk_primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        // One outcome per uuid: a duplicate must not be counted or reported twice.
        let mut seen = HashSet::with_capacity(uuids.len());
        let unique: Vec<Uuid> = uuids.iter().copied().filter(|u| seen.insert(*u)).collect();

        let chunks: Vec<&[Uuid]> = unique.chunks(self.bulk_chunk_size).collect();
        common_metrics::histogram(
            DB_BULK_CHUNKS,
            &[(
                "operation".to_string(),
                "delete_tombstoned_persons".to_string(),
            )],
            chunks.len() as f64,
        );

        // Sequential on purpose. This serves a background drain, so one bulk connection at a
        // time keeps its footprint on the persons writer small and predictable.
        let mut outcome = TombstonedDeleteOutcome::default();
        for chunk in chunks {
            let chunk_outcome = delete_tombstoned_persons_chunk(
                &self.bulk_primary_pool,
                team_id,
                chunk,
                self.tombstoned_limits.max_dependent_rows as i64,
                self.tombstoned_limits.max_rows_per_transaction as i64,
                &client,
            )
            .await?;
            outcome.deleted += chunk_outcome.deleted;
            outcome.skipped_live += chunk_outcome.skipped_live;
            outcome.blocked_uuids.extend(chunk_outcome.blocked_uuids);
            outcome
                .oversized_uuids
                .extend(chunk_outcome.oversized_uuids);
        }

        Ok(outcome)
    }

    async fn trim_tombstoned_person(
        &self,
        team_id: i64,
        uuid: Uuid,
        max_rows: i64,
    ) -> StorageResult<TrimOutcome> {
        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "trim_tombstoned_person".to_string(),
            ),
            ("pool".to_string(), "bulk_primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        // The caller picks the step; the server caps it so no call deletes more than it can
        // finish inside its deadline.
        let mut budget = max_rows.clamp(1, self.tombstoned_limits.trim_max_rows as i64);
        let cap = self.tombstoned_limits.max_dependent_rows as i64;

        let mut tx = self.bulk_primary_pool.begin().await?;
        sqlx::query("SET LOCAL lock_timeout = '2s'")
            .execute(&mut *tx)
            .await?;

        // The person lock is what makes the step safe: a revival waits for it, then finds the
        // person live and leaves the remaining rows alone.
        let person_id: Option<i64> = sqlx::query_scalar!(
            r#"
            SELECT id::bigint AS "id!"
            FROM posthog_person
            WHERE team_id = $1 AND uuid = $2 AND is_deleted
            FOR UPDATE
            "#,
            team_id as i32,
            uuid
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(person_id) = person_id else {
            tx.commit().await?;
            return Ok(TrimOutcome::default());
        };

        let mut outcome = TrimOutcome {
            person_tombstoned: true,
            ..TrimOutcome::default()
        };

        // Tombstoned distinct ids first: they are the rows that grow without bound. A live one
        // stays, so the person reads as blocked afterwards instead of losing a mapping.
        let ids: Vec<i64> = sqlx::query_scalar!(
            r#"
            SELECT id::bigint AS "id!"
            FROM posthog_persondistinctid
            WHERE team_id = $1 AND person_id = $2 AND is_deleted
            LIMIT $3
            "#,
            team_id as i32,
            person_id,
            budget
        )
        .fetch_all(&mut *tx)
        .await?;
        if !ids.is_empty() {
            outcome.distinct_ids_deleted = sqlx::query!(
                "DELETE FROM posthog_persondistinctid WHERE team_id = $1 AND id = ANY($2)",
                team_id as i32,
                ids.as_slice()
            )
            .execute(&mut *tx)
            .await?
            .rows_affected() as i64;
            budget -= outcome.distinct_ids_deleted;
        }

        if budget > 0 {
            let ids: Vec<i64> = sqlx::query_scalar!(
                r#"
                SELECT id::bigint AS "id!"
                FROM posthog_featureflaghashkeyoverride
                WHERE team_id = $1 AND person_id = $2
                LIMIT $3
                "#,
                team_id as i32,
                person_id,
                budget
            )
            .fetch_all(&mut *tx)
            .await?;
            if !ids.is_empty() {
                outcome.hash_key_overrides_deleted = sqlx::query!(
                    "DELETE FROM posthog_featureflaghashkeyoverride WHERE team_id = $1 AND id = ANY($2::bigint[])",
                    team_id as i32,
                    ids.as_slice()
                )
                .execute(&mut *tx)
                .await?
                .rows_affected() as i64;
                budget -= outcome.hash_key_overrides_deleted;
            }
        }

        if budget > 0 {
            let ids: Vec<i64> = sqlx::query_scalar!(
                r#"
                SELECT id::bigint AS "id!"
                FROM posthog_cohortpeople
                WHERE person_id = $1
                LIMIT $2
                "#,
                person_id,
                budget
            )
            .fetch_all(&mut *tx)
            .await?;
            if !ids.is_empty() {
                outcome.cohort_memberships_deleted = sqlx::query!(
                    "DELETE FROM posthog_cohortpeople WHERE id = ANY($1)",
                    ids.as_slice()
                )
                .execute(&mut *tx)
                .await?
                .rows_affected() as i64;
            }
        }

        let counts = probe_dependent_rows(&mut *tx, team_id, &[person_id], cap).await?;
        outcome.over_cap = counts
            .get(&person_id)
            .copied()
            .unwrap_or_default()
            .over_cap(cap);

        tx.commit().await?;

        for (operation, value) in [
            (
                "trim_tombstoned_person_distinct_ids",
                outcome.distinct_ids_deleted,
            ),
            (
                "trim_tombstoned_person_hash_key_overrides",
                outcome.hash_key_overrides_deleted,
            ),
            (
                "trim_tombstoned_person_cohort_memberships",
                outcome.cohort_memberships_deleted,
            ),
        ] {
            common_metrics::histogram(
                DB_ROWS_RETURNED,
                &[
                    ("operation".to_string(), operation.to_string()),
                    ("pool".to_string(), "bulk_primary".to_string()),
                    ("client".to_string(), client.to_string()),
                    ("method".to_string(), method.to_string()),
                ],
                value as f64,
            );
        }

        Ok(outcome)
    }

    async fn get_persons_by_distinct_ids_cross_team(
        &self,
        team_distinct_ids: &[(i64, String)],
        include_properties: bool,
    ) -> StorageResult<Vec<((i64, String), Option<Person>)>> {
        if team_distinct_ids.is_empty() {
            return Ok(Vec::new());
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "get_persons_by_distinct_ids_cross_team".to_string(),
            ),
            ("pool".to_string(), BULK_POOL_LABEL.to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let mut conn =
            PostgresStorage::acquire_timed(&self.bulk_replica_pool, BULK_POOL_LABEL).await?;

        let team_ids: Vec<i32> = team_distinct_ids.iter().map(|(t, _)| *t as i32).collect();
        let distinct_ids: Vec<String> = team_distinct_ids.iter().map(|(_, d)| d.clone()).collect();

        let rows = sqlx::query!(
            r#"
            SELECT p.id, p.uuid as "uuid!", p.team_id::bigint as "team_id!",
                   CASE WHEN $3::boolean THEN p.properties::text ELSE NULL END as "properties?",
                   CASE WHEN $3::boolean THEN p.properties_last_updated_at::text ELSE NULL END as "properties_last_updated_at?",
                   CASE WHEN $3::boolean THEN p.properties_last_operation::text ELSE NULL END as "properties_last_operation?",
                   p.created_at as "created_at!", p.version, p.is_identified as "is_identified!",
                   CASE WHEN p.is_user_id IS NULL THEN NULL ELSE (p.is_user_id != 0) END as is_user_id,
                   p.last_seen_at,
                   d.distinct_id as "distinct_id!"
            FROM posthog_person p
            INNER JOIN posthog_persondistinctid d ON d.person_id = p.id AND d.team_id = p.team_id
            INNER JOIN UNNEST($1::integer[], $2::text[]) AS batch(team_id, distinct_id)
                ON d.team_id = batch.team_id AND d.distinct_id = batch.distinct_id
            WHERE p.is_deleted = false AND d.is_deleted = false
            "#,
            &team_ids,
            &distinct_ids,
            include_properties
        )
        .fetch_all(&mut *conn)
        .await?;

        common_metrics::histogram(
            DB_ROWS_RETURNED,
            &[
                (
                    "operation".to_string(),
                    "get_persons_by_distinct_ids_cross_team".to_string(),
                ),
                ("client".to_string(), client.to_string()),
                ("method".to_string(), method.to_string()),
            ],
            rows.len() as f64,
        );

        let mut found: HashMap<(i64, String), Person> = rows
            .into_iter()
            .map(|row| {
                let key = (row.team_id, row.distinct_id.clone());
                let person = Person {
                    id: row.id,
                    uuid: row.uuid,
                    team_id: row.team_id,
                    properties: row.properties,
                    properties_last_updated_at: row.properties_last_updated_at,
                    properties_last_operation: row.properties_last_operation,
                    created_at: row.created_at,
                    version: row.version,
                    is_identified: row.is_identified,
                    is_user_id: row.is_user_id,
                    last_seen_at: row.last_seen_at,
                };
                (key, person)
            })
            .collect();

        Ok(team_distinct_ids
            .iter()
            .map(|(team_id, did)| {
                let key = (*team_id, did.clone());
                (key.clone(), found.remove(&key))
            })
            .collect())
    }

    async fn split_person(
        &self,
        team_id: i64,
        person_id: i64,
        distinct_ids_to_split: &[String],
    ) -> StorageResult<Vec<SplitResult>> {
        if distinct_ids_to_split.is_empty() {
            return Ok(vec![]);
        }

        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            ("operation".to_string(), "split_person".to_string()),
            ("pool".to_string(), "bulk_primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        // All-or-nothing transaction on the bulk primary pool. The statement count
        // is constant (not per-distinct_id), so the lock window stays short; the
        // service-layer cap bounds the number of rows locked.
        let mut tx = self.bulk_primary_pool.begin().await?;

        // No FOR UPDATE on the source person: deletes lock PDI rows before person
        // rows, so locking the person first here would invert that order and risk
        // deadlock. The PDI locks below are what guard the reassignment.
        let person_version: i64 = sqlx::query_scalar!(
            r#"
            SELECT COALESCE(version, 0)::bigint as "version!"
            FROM posthog_person
            WHERE team_id = $1 AND id = $2
            "#,
            team_id as i32,
            person_id
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            StorageError::NotFound(format!("person_id={person_id} (team_id={team_id})"))
        })?;

        // Lock the PDI rows and validate ownership under the lock: any requested
        // distinct_id that didn't lock either doesn't exist or belongs to another
        // person, and the whole request is rejected.
        let locked_pdis = sqlx::query!(
            r#"
            SELECT distinct_id as "distinct_id!", COALESCE(version, 0)::bigint as "version!"
            FROM posthog_persondistinctid
            WHERE team_id = $1 AND person_id = $2 AND distinct_id = ANY($3)
            FOR UPDATE
            "#,
            team_id as i32,
            person_id,
            distinct_ids_to_split
        )
        .fetch_all(&mut *tx)
        .await?;

        if locked_pdis.len() != distinct_ids_to_split.len() {
            let owned_set: HashSet<&str> = locked_pdis
                .iter()
                .map(|pdi| pdi.distinct_id.as_str())
                .collect();
            let unknown: Vec<&str> = distinct_ids_to_split
                .iter()
                .filter(|did| !owned_set.contains(did.as_str()))
                .map(|s| s.as_str())
                .collect();
            return Err(StorageError::NotFound(format!(
                "distinct_ids {unknown:?} do not belong to person_id={person_id} (team_id={team_id})"
            )));
        }

        let new_person_version = person_version + SPLIT_VERSION_OFFSET;
        // Build all arrays in request order so the response order matches the
        // input order (part of the RPC contract).
        let pdi_version_by_did: HashMap<&str, i64> = locked_pdis
            .iter()
            .map(|pdi| (pdi.distinct_id.as_str(), pdi.version))
            .collect();
        let dids: Vec<String> = distinct_ids_to_split.to_vec();
        let new_uuids: Vec<Uuid> = dids
            .iter()
            .map(|did| personhog_common::persons::person_uuid(team_id, did))
            .collect();
        let pdi_versions: Vec<i64> = dids
            .iter()
            .map(|did| pdi_version_by_did[did.as_str()] + SPLIT_VERSION_OFFSET)
            .collect();

        // Find persons that already exist for these UUIDs (idempotent re-split).
        // No ON CONFLICT — the partitioned table has a unique index, not a
        // unique constraint, so ON CONFLICT inference doesn't work.
        let existing_persons = sqlx::query!(
            r#"
            SELECT id::bigint as "id!", uuid as "uuid!", created_at as "created_at!"
            FROM posthog_person
            WHERE team_id = $1 AND uuid = ANY($2)
            FOR UPDATE
            "#,
            team_id as i32,
            &new_uuids
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut person_by_uuid: HashMap<Uuid, (i64, DateTime<Utc>)> = existing_persons
            .into_iter()
            .map(|r| (r.uuid, (r.id, r.created_at)))
            .collect();

        let existing_uuids: HashSet<Uuid> = person_by_uuid.keys().copied().collect();

        let uuids_to_insert: Vec<Uuid> = new_uuids
            .iter()
            .filter(|u| !existing_uuids.contains(u))
            .copied()
            .collect();

        if !uuids_to_insert.is_empty() {
            let inserted = sqlx::query!(
                r#"
                INSERT INTO posthog_person (uuid, team_id, properties, created_at, version, is_identified)
                SELECT u.uuid, $2, '{}'::jsonb, NOW(), $3, false
                FROM unnest($1::uuid[]) AS u(uuid)
                RETURNING id::bigint as "id!", uuid as "uuid!", created_at as "created_at!"
                "#,
                &uuids_to_insert,
                team_id as i32,
                new_person_version
            )
            .fetch_all(&mut *tx)
            .await?;
            for r in inserted {
                person_by_uuid.insert(r.uuid, (r.id, r.created_at));
            }
        }

        let uuids_to_update: Vec<Uuid> = existing_uuids.into_iter().collect();

        if !uuids_to_update.is_empty() {
            sqlx::query!(
                r#"
                UPDATE posthog_person SET version = $3
                WHERE team_id = $1 AND uuid = ANY($2)
                "#,
                team_id as i32,
                &uuids_to_update,
                new_person_version
            )
            .execute(&mut *tx)
            .await?;
        }

        let new_person_rows: Vec<(i64, DateTime<Utc>)> = new_uuids
            .iter()
            .map(|u| {
                person_by_uuid.get(u).copied().ok_or_else(|| {
                    StorageError::Query(format!("person insert did not return a row for uuid {u}"))
                })
            })
            .collect::<StorageResult<_>>()?;
        let new_person_ids: Vec<i64> = new_person_rows.iter().map(|(id, _)| *id).collect();

        // Reassign all PDIs to their new persons in one statement.
        let update_result = sqlx::query!(
            r#"
            UPDATE posthog_persondistinctid AS pdi
            SET person_id = m.new_person_id, version = m.new_version
            FROM unnest($2::text[], $3::bigint[], $4::bigint[]) AS m(distinct_id, new_person_id, new_version)
            WHERE pdi.team_id = $1 AND pdi.distinct_id = m.distinct_id
            "#,
            team_id as i32,
            &dids,
            &new_person_ids,
            &pdi_versions
        )
        .execute(&mut *tx)
        .await?;

        if update_result.rows_affected() != dids.len() as u64 {
            return Err(StorageError::Query(format!(
                "expected to reassign {} PDIs but updated {} (team_id={team_id}, person_id={person_id})",
                dids.len(),
                update_result.rows_affected()
            )));
        }

        tx.commit().await?;

        let results: Vec<SplitResult> = dids
            .into_iter()
            .zip(new_uuids)
            .zip(pdi_versions)
            .zip(new_person_rows)
            .map(
                |(((distinct_id, new_person_uuid), pdi_version), (_, new_person_created_at))| {
                    SplitResult {
                        distinct_id,
                        new_person_uuid,
                        new_person_version,
                        pdi_version,
                        new_person_created_at,
                    }
                },
            )
            .collect();

        common_metrics::histogram(
            DB_ROWS_RETURNED,
            &[
                ("operation".to_string(), "split_person".to_string()),
                ("pool".to_string(), "bulk_primary".to_string()),
                ("client".to_string(), client.to_string()),
                ("method".to_string(), method.to_string()),
            ],
            results.len() as f64,
        );

        Ok(results)
    }

    async fn set_person_distinct_id_version_floor(
        &self,
        team_id: i64,
        distinct_id: &str,
        min_version: i64,
    ) -> StorageResult<Option<Person>> {
        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "set_person_distinct_id_version_floor".to_string(),
            ),
            ("pool".to_string(), "primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let mut conn = PostgresStorage::acquire_timed(&self.primary_pool, "primary").await?;

        // Resolve the distinct_id's person and guardedly bump its version in one
        // round-trip. The `target` CTE returns the person whenever the distinct_id
        // exists, while the `UPDATE` only fires when the stored version is below
        // min_version — so an already-higher version is left intact but the person is
        // still returned. No matching distinct_id yields no person.
        let row = sqlx::query_as!(
            Person,
            r#"
            WITH target AS (
                SELECT person_id FROM posthog_persondistinctid
                WHERE team_id = $1 AND distinct_id = $2
            ),
            updated AS (
                UPDATE posthog_persondistinctid
                SET version = $3
                WHERE team_id = $1 AND distinct_id = $2 AND version < $3
                RETURNING person_id
            )
            SELECT p.id, p.uuid, p.team_id::bigint as "team_id!", p.properties::text as "properties?",
                   p.properties_last_updated_at::text as "properties_last_updated_at?",
                   p.properties_last_operation::text as "properties_last_operation?",
                   p.created_at, p.version, p.is_identified,
                   CASE WHEN p.is_user_id IS NULL THEN NULL ELSE (p.is_user_id != 0) END as is_user_id,
                   p.last_seen_at
            FROM posthog_person p
            INNER JOIN target t ON p.id = t.person_id AND p.team_id = $1
            "#,
            team_id as i32,
            distinct_id,
            min_version
        )
        .fetch_optional(&mut *conn)
        .await?;

        Ok(row)
    }

    async fn set_person_version_floor(
        &self,
        team_id: i64,
        person_id: i64,
        min_version: i64,
    ) -> StorageResult<bool> {
        let client = current_client_name();
        let method = current_method_name();
        let labels = [
            (
                "operation".to_string(),
                "set_person_version_floor".to_string(),
            ),
            ("pool".to_string(), "primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), method.to_string()),
        ];
        let _timer = common_metrics::timing_guard(DB_QUERY_DURATION, &labels);

        let mut conn = PostgresStorage::acquire_timed(&self.primary_pool, "primary").await?;

        // Guarded bump: never lowers an existing version.
        let result = sqlx::query!(
            r#"
            UPDATE posthog_person
            SET version = $3
            WHERE team_id = $1 AND id = $2 AND version < $3
            "#,
            team_id as i32,
            person_id,
            min_version
        )
        .execute(&mut *conn)
        .await?;

        Ok(result.rows_affected() > 0)
    }
}

/// Delete a chunk of persons by integer ID in a single transaction:
/// distinct_ids first (FK is NO ACTION), then persons (feature flag hash
/// key overrides cascade at the DB level).
async fn delete_persons_by_ids_chunk(
    pool: &PgPool,
    team_id: i64,
    person_ids: &[i64],
    client: &str,
    delete_cohortpeople: bool,
) -> StorageResult<i64> {
    if person_ids.is_empty() {
        return Ok(0);
    }

    let chunk_labels = [
        (
            "operation".to_string(),
            "delete_persons_batch_for_team_chunk".to_string(),
        ),
        ("pool".to_string(), "bulk_primary".to_string()),
        ("client".to_string(), client.to_string()),
        ("method".to_string(), current_method_name().to_string()),
    ];
    let _chunk_timer = common_metrics::timing_guard(DB_QUERY_DURATION, &chunk_labels);

    let mut tx = pool.begin().await?;
    let deleted =
        delete_persons_by_ids_in_tx(&mut tx, team_id, person_ids, client, delete_cohortpeople)
            .await?;
    tx.commit().await?;

    Ok(deleted)
}

/// The delete statements every person delete path shares, run inside the caller's
/// transaction so a tombstone check can hold its row locks across them.
async fn delete_persons_by_ids_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    team_id: i64,
    person_ids: &[i64],
    client: &str,
    delete_cohortpeople: bool,
) -> StorageResult<i64> {
    if person_ids.is_empty() {
        return Ok(0);
    }

    // Delete distinct_id rows first — FK is NO ACTION.
    let did_result = sqlx::query!(
        r#"
        DELETE FROM posthog_persondistinctid
        WHERE team_id = $1 AND person_id = ANY($2)
        "#,
        team_id as i32,
        person_ids
    )
    .execute(&mut **tx)
    .await?;

    common_metrics::histogram(
        DB_ROWS_RETURNED,
        &[
            (
                "operation".to_string(),
                "delete_distinct_ids_batch_for_team".to_string(),
            ),
            ("pool".to_string(), "bulk_primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), current_method_name().to_string()),
        ],
        did_result.rows_affected() as f64,
    );

    // Cohort memberships have no FK to posthog_person (the constraint was dropped
    // during person-table partitioning), so they don't cascade — delete them
    // explicitly for these persons. Gated because the team-teardown path already
    // clears cohortpeople up front by cohort; only the per-person delete paths
    // need this here.
    if delete_cohortpeople {
        let cohort_result = sqlx::query!(
            r#"
            DELETE FROM posthog_cohortpeople
            WHERE person_id = ANY($1)
            "#,
            person_ids
        )
        .execute(&mut **tx)
        .await?;

        common_metrics::histogram(
            DB_ROWS_RETURNED,
            &[
                (
                    "operation".to_string(),
                    "delete_cohortpeople_for_persons".to_string(),
                ),
                ("pool".to_string(), "bulk_primary".to_string()),
                ("client".to_string(), client.to_string()),
                ("method".to_string(), current_method_name().to_string()),
            ],
            cohort_result.rows_affected() as f64,
        );
    }

    // Delete person rows (hash key overrides cascade at DB level).
    let result = sqlx::query!(
        r#"
        DELETE FROM posthog_person
        WHERE team_id = $1 AND id = ANY($2)
        "#,
        team_id as i32,
        person_ids
    )
    .execute(&mut **tx)
    .await?;

    common_metrics::histogram(
        DB_ROWS_RETURNED,
        &[
            (
                "operation".to_string(),
                "delete_persons_batch_for_team".to_string(),
            ),
            ("pool".to_string(), "bulk_primary".to_string()),
            ("client".to_string(), client.to_string()),
            ("method".to_string(), current_method_name().to_string()),
        ],
        result.rows_affected() as f64,
    );

    Ok(result.rows_affected() as i64)
}

/// Dependent rows of one tombstoned person. Each count is read with `LIMIT cap + 1`, so a
/// probe never costs more than cap + 1 index entries per table however many rows the person
/// owns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DependentRowCounts {
    distinct_ids: i64,
    hash_key_overrides: i64,
    cohort_memberships: i64,
}

impl DependentRowCounts {
    fn total(self) -> i64 {
        self.distinct_ids + self.hash_key_overrides + self.cohort_memberships
    }

    fn over_cap(self, cap: i64) -> bool {
        self.distinct_ids > cap || self.hash_key_overrides > cap || self.cohort_memberships > cap
    }
}

async fn probe_dependent_rows<'e, E>(
    executor: E,
    team_id: i64,
    person_ids: &[i64],
    cap: i64,
) -> StorageResult<HashMap<i64, DependentRowCounts>>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query!(
        r#"
        SELECT p.id AS "id!",
            (SELECT count(*) FROM (SELECT 1 FROM posthog_persondistinctid
                WHERE team_id = $1 AND person_id = p.id LIMIT $3) t) AS "distinct_ids!",
            (SELECT count(*) FROM (SELECT 1 FROM posthog_featureflaghashkeyoverride
                WHERE team_id = $1 AND person_id = p.id LIMIT $3) t) AS "hash_key_overrides!",
            (SELECT count(*) FROM (SELECT 1 FROM posthog_cohortpeople
                WHERE person_id = p.id LIMIT $3) t) AS "cohort_memberships!"
        FROM unnest($2::bigint[]) AS p(id)
        "#,
        team_id as i32,
        person_ids,
        cap + 1
    )
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.id,
                DependentRowCounts {
                    distinct_ids: row.distinct_ids,
                    hash_key_overrides: row.hash_key_overrides,
                    cohort_memberships: row.cohort_memberships,
                },
            )
        })
        .collect())
}

/// Groups candidates, in the given order, so no group carries more than `budget` dependent
/// rows. A candidate that does not fit next to the others starts a new group, and a candidate
/// over the budget on its own still forms a group, so every call makes progress.
fn group_by_row_budget<T: Copy>(items: &[(T, DependentRowCounts)], budget: i64) -> Vec<Vec<T>> {
    let mut groups: Vec<Vec<T>> = Vec::new();
    let mut current: Vec<T> = Vec::new();
    let mut used = 0i64;
    for (item, counts) in items {
        let rows = counts.total();
        if !current.is_empty() && used + rows > budget {
            groups.push(std::mem::take(&mut current));
            used = 0;
        }
        current.push(*item);
        used += rows;
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// One request chunk: resolve the still-tombstoned persons, size their dependent rows, and
/// delete them in transactions that each stay under `max_rows_per_transaction` dependent rows.
async fn delete_tombstoned_persons_chunk(
    pool: &PgPool,
    team_id: i64,
    uuids: &[Uuid],
    max_dependent_rows: i64,
    max_rows_per_transaction: i64,
    client: &str,
) -> StorageResult<TombstonedDeleteOutcome> {
    if uuids.is_empty() {
        return Ok(TombstonedDeleteOutcome::default());
    }

    let chunk_labels = [
        (
            "operation".to_string(),
            "delete_tombstoned_persons_chunk".to_string(),
        ),
        ("pool".to_string(), "bulk_primary".to_string()),
        ("client".to_string(), client.to_string()),
        ("method".to_string(), current_method_name().to_string()),
    ];
    let _chunk_timer = common_metrics::timing_guard(DB_QUERY_DURATION, &chunk_labels);

    // Resolved without locks: each group re-checks the tombstone under its row lock, so a person
    // revived in between drops out of its group and reads as neither deleted nor live.
    let candidates: Vec<(i64, Uuid)> = sqlx::query!(
        r#"
        SELECT id::bigint AS "id!", uuid AS "uuid!"
        FROM posthog_person
        WHERE team_id = $1 AND uuid = ANY($2) AND is_deleted
        ORDER BY id
        "#,
        team_id as i32,
        uuids
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| (row.id, row.uuid))
    .collect();

    let skipped_live: i64 = sqlx::query_scalar!(
        r#"
        SELECT count(*) AS "count!"
        FROM posthog_person
        WHERE team_id = $1 AND uuid = ANY($2) AND is_deleted = false
        "#,
        team_id as i32,
        uuids
    )
    .fetch_one(pool)
    .await?;

    if candidates.is_empty() {
        return Ok(TombstonedDeleteOutcome {
            skipped_live,
            ..TombstonedDeleteOutcome::default()
        });
    }

    // A person over the cap in any table would hold row locks and write WAL far past the
    // caller's deadline, so it is reported back for TrimTombstonedPerson instead.
    let ids: Vec<i64> = candidates.iter().map(|(id, _)| *id).collect();
    let counts = probe_dependent_rows(pool, team_id, &ids, max_dependent_rows).await?;
    let mut oversized_uuids = Vec::new();
    let mut sized: Vec<(Uuid, DependentRowCounts)> = Vec::new();
    for (id, uuid) in &candidates {
        let person_counts = counts.get(id).copied().unwrap_or_default();
        if person_counts.over_cap(max_dependent_rows) {
            oversized_uuids.push(*uuid);
        } else {
            sized.push((*uuid, person_counts));
        }
    }

    let groups = group_by_row_budget(&sized, max_rows_per_transaction);
    common_metrics::histogram(
        DB_BULK_CHUNKS,
        &[(
            "operation".to_string(),
            "delete_tombstoned_persons_transactions".to_string(),
        )],
        groups.len() as f64,
    );

    let mut outcome = TombstonedDeleteOutcome {
        skipped_live,
        oversized_uuids,
        ..TombstonedDeleteOutcome::default()
    };
    for group in groups {
        let group_outcome = delete_tombstoned_group(pool, team_id, &group, client).await?;
        outcome.deleted += group_outcome.deleted;
        outcome.blocked_uuids.extend(group_outcome.blocked_uuids);
    }

    for (operation, value) in [
        (
            "delete_tombstoned_persons_blocked",
            outcome.blocked_uuids.len(),
        ),
        (
            "delete_tombstoned_persons_oversized",
            outcome.oversized_uuids.len(),
        ),
        (
            "delete_tombstoned_persons_skipped_live",
            skipped_live as usize,
        ),
    ] {
        common_metrics::histogram(
            DB_ROWS_RETURNED,
            &[
                ("operation".to_string(), operation.to_string()),
                ("pool".to_string(), "bulk_primary".to_string()),
                ("client".to_string(), client.to_string()),
                ("method".to_string(), current_method_name().to_string()),
            ],
            value as f64,
        );
    }

    Ok(outcome)
}

#[derive(Debug, Default)]
struct GroupOutcome {
    deleted: i64,
    blocked_uuids: Vec<Uuid>,
}

/// One tombstone-guarded transaction, locking persons then their distinct ids in id order. Live
/// writers touch live persons, never locked here, and the identity saga locks in this order.
async fn delete_tombstoned_group(
    pool: &PgPool,
    team_id: i64,
    uuids: &[Uuid],
    client: &str,
) -> StorageResult<GroupOutcome> {
    let mut tx = pool.begin().await?;

    // A held row means a revival or merge in flight: fail fast and let the caller retry. Kept
    // under the router's 5 s backend deadline so the caller sees an error, not a timeout.
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *tx)
        .await?;

    // Lock only the persons that are still tombstoned. READ COMMITTED re-checks is_deleted on
    // the row version that wins the lock, so a person revived a moment ago drops out here.
    let candidates: Vec<(i64, Uuid)> = sqlx::query!(
        r#"
        SELECT id::bigint AS "id!", uuid AS "uuid!"
        FROM posthog_person
        WHERE team_id = $1 AND uuid = ANY($2) AND is_deleted
        ORDER BY id
        FOR UPDATE
        "#,
        team_id as i32,
        uuids
    )
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .map(|row| (row.id, row.uuid))
    .collect();

    if candidates.is_empty() {
        tx.commit().await?;
        return Ok(GroupOutcome::default());
    }
    let candidate_ids: Vec<i64> = candidates.iter().map(|(id, _)| *id).collect();

    // Lock the distinct ids too and read their state under the lock. A live mapping means
    // ingestion can still reach the person, so it must stay.
    let live_owners: HashSet<i64> = sqlx::query!(
        r#"
        SELECT person_id AS "person_id!", is_deleted AS "is_deleted!"
        FROM posthog_persondistinctid
        WHERE team_id = $1 AND person_id = ANY($2)
        ORDER BY id
        FOR UPDATE
        "#,
        team_id as i32,
        candidate_ids.as_slice()
    )
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .filter(|row| !row.is_deleted)
    .map(|row| row.person_id)
    .collect();

    let blocked_uuids: Vec<Uuid> = candidates
        .iter()
        .filter(|(id, _)| live_owners.contains(id))
        .map(|(_, uuid)| *uuid)
        .collect();
    let victims: Vec<i64> = candidates
        .iter()
        .filter(|(id, _)| !live_owners.contains(id))
        .map(|(id, _)| *id)
        .collect();

    // The hash key override FK cascades in production but not in every environment built from
    // the sqlx migrations, so remove the overrides here instead of relying on the cascade.
    if !victims.is_empty() {
        sqlx::query!(
            "DELETE FROM posthog_featureflaghashkeyoverride WHERE team_id = $1 AND person_id = ANY($2)",
            team_id as i32,
            victims.as_slice()
        )
        .execute(&mut *tx)
        .await?;
    }

    let deleted = delete_persons_by_ids_in_tx(&mut tx, team_id, &victims, client, true).await?;

    tx.commit().await?;

    Ok(GroupOutcome {
        deleted,
        blocked_uuids,
    })
}

#[cfg(test)]
mod tests {
    use super::{group_by_row_budget, DependentRowCounts};

    fn counts(
        distinct_ids: i64,
        hash_key_overrides: i64,
        cohort_memberships: i64,
    ) -> DependentRowCounts {
        DependentRowCounts {
            distinct_ids,
            hash_key_overrides,
            cohort_memberships,
        }
    }

    #[test]
    fn groups_stay_under_the_budget_and_keep_order() {
        let items = [
            ("a", counts(2, 1, 0)),
            ("b", counts(3, 0, 0)),
            ("c", counts(0, 0, 4)),
            ("d", counts(1, 1, 1)),
            ("e", counts(0, 0, 0)),
        ];

        assert_eq!(
            group_by_row_budget(&items, 6),
            vec![vec!["a", "b"], vec!["c"], vec!["d", "e"]]
        );
    }

    #[test]
    fn a_candidate_over_the_budget_still_gets_its_own_group() {
        let items = [("big", counts(9, 0, 0)), ("small", counts(1, 0, 0))];

        assert_eq!(
            group_by_row_budget(&items, 5),
            vec![vec!["big"], vec!["small"]]
        );
    }

    #[test]
    fn over_cap_checks_each_table_on_its_own() {
        assert!(counts(4, 0, 0).over_cap(3));
        assert!(counts(0, 4, 0).over_cap(3));
        assert!(counts(0, 0, 4).over_cap(3));
        assert!(!counts(3, 3, 3).over_cap(3));
    }
}
