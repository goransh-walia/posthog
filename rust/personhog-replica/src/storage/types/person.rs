use uuid::Uuid;

pub use personhog_common::persons::Person;

#[derive(Debug, Clone)]
pub struct DistinctIdMapping {
    pub person_id: i64,
    pub distinct_id: String,
    pub version: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct DistinctIdWithVersion {
    pub distinct_id: String,
    pub version: Option<i64>,
}

/// Outcome of a tombstone-guarded delete. Every requested uuid lands in at most one bucket;
/// a uuid with no Postgres row lands in none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TombstonedDeleteOutcome {
    /// Persons hard-deleted together with their distinct ids and cohort memberships.
    pub deleted: i64,
    /// Persons found with is_deleted = false, so revived after the caller queued them. Untouched.
    pub skipped_live: i64,
    /// Persons still tombstoned but referenced by a live distinct id. Untouched. Ingestion never
    /// produces this state, so the caller should surface it rather than retry blindly.
    pub blocked_uuids: Vec<Uuid>,
    /// Persons still tombstoned but owning more rows in a dependent table than one transaction
    /// may delete. Untouched; TrimTombstonedPerson takes them down first.
    pub oversized_uuids: Vec<Uuid>,
}

/// One bounded trim of a tombstoned person's dependent rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrimOutcome {
    /// False when the person is live or missing. Nothing was deleted then.
    pub person_tombstoned: bool,
    pub distinct_ids_deleted: i64,
    pub hash_key_overrides_deleted: i64,
    pub cohort_memberships_deleted: i64,
    /// A dependent table still holds more rows than the cap. With nothing deleted in this call,
    /// the remaining rows are live distinct ids and the person is blocked, not oversized.
    pub over_cap: bool,
}

#[derive(Debug, Clone)]
pub struct SplitResult {
    pub distinct_id: String,
    pub new_person_uuid: Uuid,
    pub new_person_version: i64,
    pub pdi_version: i64,
    /// For pre-existing persons (idempotent re-split) this is the original
    /// created_at, preserved by the upsert — not the time of this request.
    pub new_person_created_at: chrono::DateTime<chrono::Utc>,
}
