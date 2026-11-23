//! Actor registry: the stable map between Google identity and ownership.
//!
//! An actor row holds the internal owner id (a UUID), the Google subject, and
//! the email. The primary actor is seeded before its first sign-in with a
//! `NULL` subject; the first allowlisted sign-in binds it. A different Google
//! subject presenting an already-bound email is rejected so it cannot inherit
//! another actor's data.

use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

use sqlx::PgPool;
use uuid::Uuid;

/// One actor registry row.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ActorRecord {
    /// Stable owner id stored in every ownership column.
    pub id: Uuid,
    /// Google subject; `None` only for the seeded primary actor.
    pub google_sub: Option<String>,
    /// Google email, normalized to lower case.
    pub email: String,
}

/// Why actor persistence failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorError {
    /// The identity conflicts with an existing binding and must be rejected.
    Forbidden,
    /// The registry could not be read or written.
    Unavailable(String),
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forbidden => write!(f, "identity is already bound to another account"),
            Self::Unavailable(detail) => write!(f, "actor registry unavailable: {detail}"),
        }
    }
}

impl std::error::Error for ActorError {}

impl From<sqlx::Error> for ActorError {
    fn from(error: sqlx::Error) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// Actor persistence: PostgreSQL in production, cache-only for tests.
#[derive(Clone)]
pub enum ActorStore {
    /// Durable actor table.
    Postgres(PgPool),
    /// Cache-only test double.
    Memory,
}

/// In-memory actor cache used by every request.
#[derive(Default)]
pub struct ActorCache {
    actors: RwLock<HashMap<Uuid, ActorRecord>>,
}

impl ActorCache {
    /// Looks up an actor by owner id.
    pub fn get(&self, id: Uuid) -> Option<ActorRecord> {
        self.actors.read().ok()?.get(&id).cloned()
    }

    /// Looks up an actor by Google subject.
    pub fn by_sub(&self, sub: &str) -> Option<ActorRecord> {
        self.actors
            .read()
            .ok()?
            .values()
            .find(|actor| actor.google_sub.as_deref() == Some(sub))
            .cloned()
    }

    /// Looks up an actor by email, case-insensitively.
    pub fn by_email(&self, email: &str) -> Option<ActorRecord> {
        let email = crate::auth::normalize_email(email);
        self.actors
            .read()
            .ok()?
            .values()
            .find(|actor| actor.email == email)
            .cloned()
    }

    /// Inserts or replaces one actor.
    pub fn insert(&self, actor: ActorRecord) {
        if let Ok(mut actors) = self.actors.write() {
            actors.insert(actor.id, actor);
        }
    }

    /// Replaces the whole cache from a registry snapshot.
    pub fn replace_all(&self, actors: Vec<ActorRecord>) {
        if let Ok(mut cache) = self.actors.write() {
            cache.clear();
            for actor in actors {
                cache.insert(actor.id, actor);
            }
        }
    }

    /// Number of cached actors.
    pub fn len(&self) -> usize {
        self.actors.read().map(|actors| actors.len()).unwrap_or(0)
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ActorStore {
    /// Reads every actor row.
    pub async fn load(&self) -> Result<Vec<ActorRecord>, ActorError> {
        match self {
            Self::Postgres(pool) => sqlx::query_as::<_, ActorRecord>(
                "SELECT id, google_sub, email FROM actors ORDER BY created_at",
            )
            .fetch_all(pool)
            .await
            .map_err(ActorError::from),
            Self::Memory => Ok(Vec::new()),
        }
    }

    /// Seeds the primary actor by email, leaving its subject unbound.
    pub async fn seed_primary(&self, email: &str) -> Result<ActorRecord, ActorError> {
        match self {
            Self::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO actors (email) VALUES ($1)
                     ON CONFLICT (lower(email)) DO NOTHING",
                )
                .bind(email)
                .execute(pool)
                .await
                .map_err(ActorError::from)?;
                self.find_by_email(email)
                    .await?
                    .ok_or_else(|| ActorError::Unavailable("primary actor missing".to_string()))
            }
            Self::Memory => Ok(ActorRecord {
                id: Uuid::new_v4(),
                google_sub: None,
                email: crate::auth::normalize_email(email),
            }),
        }
    }

    async fn find_by_sub(&self, sub: &str) -> Result<Option<ActorRecord>, ActorError> {
        match self {
            Self::Postgres(pool) => sqlx::query_as::<_, ActorRecord>(
                "SELECT id, google_sub, email FROM actors WHERE google_sub = $1",
            )
            .bind(sub)
            .fetch_optional(pool)
            .await
            .map_err(ActorError::from),
            Self::Memory => Ok(None),
        }
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<ActorRecord>, ActorError> {
        match self {
            Self::Postgres(pool) => sqlx::query_as::<_, ActorRecord>(
                "SELECT id, google_sub, email FROM actors WHERE lower(email) = lower($1)",
            )
            .bind(email)
            .fetch_optional(pool)
            .await
            .map_err(ActorError::from),
            Self::Memory => Ok(None),
        }
    }

    async fn create(&self, sub: &str, email: &str) -> Result<ActorRecord, ActorError> {
        match self {
            Self::Postgres(pool) => sqlx::query_as::<_, ActorRecord>(
                "INSERT INTO actors (google_sub, email) VALUES ($1, $2)
                 RETURNING id, google_sub, email",
            )
            .bind(sub)
            .bind(email)
            .fetch_one(pool)
            .await
            .map_err(ActorError::from),
            Self::Memory => Ok(ActorRecord {
                id: Uuid::new_v4(),
                google_sub: Some(sub.to_string()),
                email: crate::auth::normalize_email(email),
            }),
        }
    }

    async fn bind_subject(&self, id: Uuid, sub: &str) -> Result<(), ActorError> {
        match self {
            Self::Postgres(pool) => {
                sqlx::query("UPDATE actors SET google_sub = $2, updated_at = NOW() WHERE id = $1")
                    .bind(id)
                    .bind(sub)
                    .execute(pool)
                    .await
                    .map(|_| ())
                    .map_err(unique_to_forbidden)
            }
            Self::Memory => Ok(()),
        }
    }

    async fn update_email(&self, id: Uuid, email: &str) -> Result<(), ActorError> {
        match self {
            Self::Postgres(pool) => {
                sqlx::query("UPDATE actors SET email = $2, updated_at = NOW() WHERE id = $1")
                    .bind(id)
                    .bind(email)
                    .execute(pool)
                    .await
                    .map(|_| ())
                    .map_err(unique_to_forbidden)
            }
            Self::Memory => Ok(()),
        }
    }

    async fn touch_login(&self, id: Uuid) -> Result<(), ActorError> {
        match self {
            Self::Postgres(pool) => {
                sqlx::query("UPDATE actors SET last_login_at = NOW() WHERE id = $1")
                    .bind(id)
                    .execute(pool)
                    .await
                    .map(|_| ())
                    .map_err(ActorError::from)
            }
            Self::Memory => Ok(()),
        }
    }
}

/// Binds a verified Google identity to an actor row.
///
/// Creation happens for a new allowlisted email; the seeded primary actor
/// binds on its first sign-in; a new subject presenting an already-bound email
/// is rejected.
pub async fn bind_or_create(
    store: &ActorStore,
    cache: &ActorCache,
    sub: &str,
    email: &str,
) -> Result<ActorRecord, ActorError> {
    let email = crate::auth::normalize_email(email);
    let by_sub = match cache.by_sub(sub) {
        Some(record) => Some(record),
        None => store.find_by_sub(sub).await?,
    };
    if let Some(record) = by_sub {
        return refresh_login(store, cache, record, &email).await;
    }
    let by_email = match cache.by_email(&email) {
        Some(record) => Some(record),
        None => store.find_by_email(&email).await?,
    };
    if let Some(mut record) = by_email {
        match record.google_sub.as_deref() {
            Some(bound) if bound != sub => return Err(ActorError::Forbidden),
            Some(_) => {}
            None => {
                store.bind_subject(record.id, sub).await?;
                record.google_sub = Some(sub.to_string());
            }
        }
        return refresh_login(store, cache, record, &email).await;
    }
    let record = match store.create(sub, &email).await {
        Ok(record) => record,
        Err(ActorError::Forbidden) => return Err(ActorError::Forbidden),
        Err(ActorError::Unavailable(_)) => {
            // A concurrent first sign-in may have won the insert race; prefer
            // the now-existing binding over a spurious failure.
            if let Some(record) = store.find_by_sub(sub).await? {
                record
            } else if let Some(record) = store.find_by_email(&email).await? {
                if record.google_sub.is_some() {
                    return Err(ActorError::Forbidden);
                }
                store.bind_subject(record.id, sub).await?;
                record
            } else {
                return Err(ActorError::Unavailable("actor insert failed".to_string()));
            }
        }
    };
    store.touch_login(record.id).await?;
    cache.insert(record.clone());
    Ok(record)
}

async fn refresh_login(
    store: &ActorStore,
    cache: &ActorCache,
    mut record: ActorRecord,
    email: &str,
) -> Result<ActorRecord, ActorError> {
    if record.email != email {
        store.update_email(record.id, email).await?;
        record.email = email.to_string();
    }
    store.touch_login(record.id).await?;
    cache.insert(record.clone());
    Ok(record)
}

fn unique_to_forbidden(error: sqlx::Error) -> ActorError {
    if is_unique_violation(&error) {
        ActorError::Forbidden
    } else {
        ActorError::Unavailable(error.to_string())
    }
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|database_error| database_error.is_unique_violation())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(cache: &ActorCache, email: &str, sub: Option<&str>) -> ActorRecord {
        let record = ActorRecord {
            id: Uuid::new_v4(),
            google_sub: sub.map(str::to_string),
            email: email.to_string(),
        };
        cache.insert(record.clone());
        record
    }

    #[tokio::test]
    async fn first_sign_in_binds_the_seeded_primary_actor() {
        let cache = ActorCache::default();
        let primary = seed(&cache, "alice@example.com", None);

        let bound = bind_or_create(
            &ActorStore::Memory,
            &cache,
            "google-alice",
            "alice@example.com",
        )
        .await
        .expect("bind");
        assert_eq!(bound.id, primary.id);
        assert_eq!(bound.google_sub.as_deref(), Some("google-alice"));
        assert_eq!(
            cache.by_sub("google-alice").map(|actor| actor.id),
            Some(primary.id)
        );
    }

    #[tokio::test]
    async fn a_different_subject_cannot_inherit_a_bound_email() {
        let cache = ActorCache::default();
        seed(&cache, "alice@example.com", Some("google-alice"));
        assert_eq!(
            bind_or_create(
                &ActorStore::Memory,
                &cache,
                "google-imposter",
                "alice@example.com"
            )
            .await,
            Err(ActorError::Forbidden)
        );
    }

    #[tokio::test]
    async fn a_matching_subject_may_change_its_email() {
        let cache = ActorCache::default();
        let actor = seed(&cache, "alice@example.com", Some("google-alice"));
        let updated = bind_or_create(
            &ActorStore::Memory,
            &cache,
            "google-alice",
            "alice.new@example.com",
        )
        .await
        .expect("email change");
        assert_eq!(updated.id, actor.id);
        assert_eq!(updated.email, "alice.new@example.com");
        assert!(cache.by_email("alice@example.com").is_none());
    }

    #[tokio::test]
    async fn a_new_subject_creates_a_new_actor() {
        let cache = ActorCache::default();
        let actor = bind_or_create(&ActorStore::Memory, &cache, "google-bob", "Bob@Example.com")
            .await
            .expect("create");
        assert_eq!(actor.email, "bob@example.com");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_lookups_are_case_insensitive() {
        let cache = ActorCache::default();
        seed(&cache, "alice@example.com", Some("sub"));
        assert!(cache.by_email("ALICE@example.com").is_some());
        assert!(cache.by_email("other@example.com").is_none());
        assert!(cache.by_sub("sub").is_some());
        assert!(!cache.is_empty());
    }

    #[test]
    fn unique_violation_detection_handles_non_database_errors() {
        let error = sqlx::Error::RowNotFound;
        assert!(!is_unique_violation(&error));
        assert_eq!(
            unique_to_forbidden(error),
            ActorError::Unavailable(sqlx::Error::RowNotFound.to_string())
        );
    }
}
