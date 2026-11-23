//! Google Identity Services ID-token verification.
//!
//! The browser obtains an ID token from Google; this module verifies its
//! RS256 signature against Google's JWKS, plus issuer, audience, expiry, and
//! the `email_verified` claim. Keys are cached and refetched once when an
//! unknown key id appears, so Google key rotation does not end sessions.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, DecodingKey, Header, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};

/// Google's published signing keys.
pub const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";

/// Network deadline for one JWKS fetch.
pub const JWKS_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a fetched key set is considered fresh.
const JWKS_TTL: Duration = Duration::from_secs(60 * 60);

/// Tolerance for clock skew between Google and this server.
const CLOCK_LEEWAY_SECONDS: u64 = 30;

/// A verified Google identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedGoogleIdentity {
    /// Stable Google subject id.
    pub sub: String,
    /// Verified email address.
    pub email: String,
}

/// Why a Google credential was not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoogleAuthError {
    /// The token is malformed, expired, wrong-audience, wrong-issuer,
    /// signed by an unknown key, or carries an unverified email.
    InvalidCredential(String),
    /// The key set could not be fetched.
    Unavailable(String),
}

impl fmt::Display for GoogleAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCredential(detail) => {
                write!(f, "invalid Google credential: {detail}")
            }
            Self::Unavailable(detail) => write!(f, "Google verification unavailable: {detail}"),
        }
    }
}

impl std::error::Error for GoogleAuthError {}

/// Verifies one Google ID token.
#[async_trait]
pub trait GoogleVerifier: Send + Sync {
    /// Returns the verified identity or a typed failure.
    async fn verify(&self, id_token: &str) -> Result<VerifiedGoogleIdentity, GoogleAuthError>;
}

/// Fetches the current Google key set.
#[async_trait]
pub trait JwksProvider: Send + Sync {
    /// Fetches and parses the JWKS document.
    async fn fetch(&self) -> Result<JwksDocument, GoogleAuthError>;
}

/// One JWKS key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    /// Key id used by the token header.
    pub kid: String,
    /// RSA modulus, base64url without padding.
    pub n: String,
    /// RSA public exponent, base64url without padding.
    pub e: String,
}

/// A JWKS document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwksDocument {
    /// Published keys.
    #[serde(default)]
    pub keys: Vec<Jwk>,
}

/// JWKS fetched over HTTPS.
pub struct HttpJwksProvider {
    client: Result<reqwest::Client, String>,
    url: String,
}

impl HttpJwksProvider {
    /// Builds the provider with the 5-second network deadline.
    ///
    /// A client build failure is retained and surfaced as `Unavailable` on
    /// the first fetch, so construction stays infallible.
    pub fn new(url: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(JWKS_TIMEOUT)
            .build()
            .map_err(|error| error.to_string());
        Self {
            client,
            url: url.into(),
        }
    }
}

#[async_trait]
impl JwksProvider for HttpJwksProvider {
    async fn fetch(&self) -> Result<JwksDocument, GoogleAuthError> {
        let client = self
            .client
            .as_ref()
            .map_err(|error| GoogleAuthError::Unavailable(error.clone()))?;
        let response = client
            .get(&self.url)
            .send()
            .await
            .map_err(|error| GoogleAuthError::Unavailable(error.to_string()))?;
        response
            .error_for_status()
            .map_err(|error| GoogleAuthError::Unavailable(error.to_string()))?
            .json::<JwksDocument>()
            .await
            .map_err(|error| GoogleAuthError::Unavailable(error.to_string()))
    }
}

#[derive(Default)]
struct JwksCache {
    keys: HashMap<String, DecodingKey>,
    fetched_at: Option<Instant>,
}

/// RS256 verifier backed by a cached Google JWKS.
pub struct JwksVerifier {
    client_id: String,
    provider: Arc<dyn JwksProvider>,
    cache: RwLock<JwksCache>,
}

impl JwksVerifier {
    /// Builds a verifier for the configured Google client id.
    pub fn new(client_id: impl Into<String>) -> Self {
        let provider: Arc<dyn JwksProvider> = Arc::new(HttpJwksProvider::new(GOOGLE_JWKS_URL));
        Self {
            client_id: client_id.into(),
            provider,
            cache: RwLock::new(JwksCache::default()),
        }
    }

    /// Builds a verifier with an explicit key source, for tests and fixtures.
    pub fn with_provider(client_id: impl Into<String>, provider: Arc<dyn JwksProvider>) -> Self {
        Self {
            client_id: client_id.into(),
            provider,
            cache: RwLock::new(JwksCache::default()),
        }
    }

    async fn key_for(&self, kid: &str) -> Result<DecodingKey, GoogleAuthError> {
        if let Some(key) = self.fresh_key(kid) {
            return Ok(key);
        }
        self.refresh().await?;
        if let Some(key) = self.cached_key(kid) {
            return Ok(key);
        }
        // An unknown key id may mean Google rotated keys after the cache was
        // filled; refetch once more before failing closed.
        self.refresh().await?;
        self.cached_key(kid)
            .ok_or_else(|| GoogleAuthError::InvalidCredential("unknown signing key".to_string()))
    }

    fn fresh_key(&self, kid: &str) -> Option<DecodingKey> {
        let cache = self.cache.read().ok()?;
        let fetched_at = cache.fetched_at?;
        if fetched_at.elapsed() > JWKS_TTL {
            return None;
        }
        cache.keys.get(kid).cloned()
    }

    fn cached_key(&self, kid: &str) -> Option<DecodingKey> {
        self.cache.read().ok()?.keys.get(kid).cloned()
    }

    async fn refresh(&self) -> Result<(), GoogleAuthError> {
        let document = self.provider.fetch().await?;
        let mut keys = HashMap::new();
        for jwk in document.keys {
            if let Ok(key) = DecodingKey::from_rsa_components(&jwk.n, &jwk.e) {
                keys.insert(jwk.kid, key);
            }
        }
        if let Ok(mut cache) = self.cache.write() {
            cache.keys = keys;
            cache.fetched_at = Some(Instant::now());
        }
        Ok(())
    }

    fn validation(&self) -> Validation {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.leeway = CLOCK_LEEWAY_SECONDS;
        validation.set_audience(&[self.client_id.as_str()]);
        validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation
    }
}

#[async_trait]
impl GoogleVerifier for JwksVerifier {
    async fn verify(&self, id_token: &str) -> Result<VerifiedGoogleIdentity, GoogleAuthError> {
        let header = decode_header(id_token)
            .map_err(|error| GoogleAuthError::InvalidCredential(error.to_string()))?;
        if header.alg != Algorithm::RS256 {
            return Err(GoogleAuthError::InvalidCredential(
                "unexpected signing algorithm".to_string(),
            ));
        }
        let kid = header
            .kid
            .ok_or_else(|| GoogleAuthError::InvalidCredential("token has no key id".to_string()))?;
        let key = self.key_for(&kid).await?;
        let claims = decode::<GoogleClaims>(id_token, &key, &self.validation())
            .map_err(|error| GoogleAuthError::InvalidCredential(error.to_string()))?
            .claims;
        if !is_verified(&claims.email_verified) {
            return Err(GoogleAuthError::InvalidCredential(
                "email is not verified".to_string(),
            ));
        }
        let email = claims.email.trim().to_ascii_lowercase();
        if claims.sub.trim().is_empty() || email.is_empty() {
            return Err(GoogleAuthError::InvalidCredential(
                "identity is missing a subject or email".to_string(),
            ));
        }
        Ok(VerifiedGoogleIdentity {
            sub: claims.sub,
            email,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GoogleClaims {
    sub: String,
    email: String,
    #[serde(default)]
    email_verified: Option<serde_json::Value>,
}

fn is_verified(value: &Option<serde_json::Value>) -> bool {
    match value {
        Some(serde_json::Value::Bool(verified)) => *verified,
        Some(serde_json::Value::String(verified)) => verified.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// Header for a test or fixture RS256 Google token.
pub fn google_header(kid: &str) -> Header {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_string());
    header
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use jsonwebtoken::{EncodingKey, encode};
    use serde_json::json;

    const CLIENT_ID: &str = "test-client.apps.googleusercontent.com";
    const JWKS: &str = include_str!("../../tests/fixtures/google_jwks.json");
    const JWKS_ROTATED: &str = include_str!("../../tests/fixtures/google_jwks_rotated.json");
    const KEY_ONE: &str = include_str!("../../tests/fixtures/google_test_key.pem");
    const KEY_TWO: &str = include_str!("../../tests/fixtures/google_test_key_2.pem");

    struct SequenceProvider {
        documents: Mutex<Vec<JwksDocument>>,
        calls: AtomicUsize,
    }

    impl SequenceProvider {
        fn new(documents: Vec<JwksDocument>) -> Self {
            Self {
                documents: Mutex::new(documents),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl JwksProvider for SequenceProvider {
        async fn fetch(&self) -> Result<JwksDocument, GoogleAuthError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut documents = self
                .documents
                .lock()
                .map_err(|_| GoogleAuthError::Unavailable("poisoned fixture".to_string()))?;
            if documents.len() > 1 {
                Ok(documents.remove(0))
            } else {
                documents
                    .first()
                    .cloned()
                    .ok_or_else(|| GoogleAuthError::Unavailable("no fixture".to_string()))
            }
        }
    }

    fn document(raw: &str) -> JwksDocument {
        serde_json::from_str(raw).expect("fixture JWKS")
    }

    fn first_key_only(mut document: JwksDocument) -> JwksDocument {
        document.keys.truncate(1);
        document
    }

    fn token(kid: &str, key: &str, claims: serde_json::Value) -> String {
        encode(
            &google_header(kid),
            &claims,
            &EncodingKey::from_rsa_pem(key.as_bytes()).expect("fixture key"),
        )
        .expect("signed token")
    }

    fn claims(email_verified: serde_json::Value) -> serde_json::Value {
        json!({
            "sub": "google-subject-1",
            "email": "Alice@Example.com",
            "email_verified": email_verified,
            "iss": "https://accounts.google.com",
            "aud": CLIENT_ID,
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().timestamp() + 600
        })
    }

    fn verifier() -> JwksVerifier {
        JwksVerifier::with_provider(
            CLIENT_ID,
            Arc::new(SequenceProvider::new(vec![document(JWKS)])),
        )
    }

    #[tokio::test]
    async fn verifies_a_valid_google_token() {
        let token = token("test-key-1", KEY_ONE, claims(json!(true)));
        let identity = verifier().verify(&token).await.expect("valid token");
        assert_eq!(identity.sub, "google-subject-1");
        assert_eq!(identity.email, "alice@example.com");
    }

    #[tokio::test]
    async fn rejects_expired_wrong_audience_and_wrong_issuer() {
        let verifier = verifier();

        let mut expired = claims(json!(true));
        expired["exp"] = json!(chrono::Utc::now().timestamp() - 3_600);
        let expired_token = token("test-key-1", KEY_ONE, expired);
        assert!(matches!(
            verifier.verify(&expired_token).await,
            Err(GoogleAuthError::InvalidCredential(_))
        ));

        let mut wrong_audience = claims(json!(true));
        wrong_audience["aud"] = json!("other-client");
        let wrong_audience_token = token("test-key-1", KEY_ONE, wrong_audience);
        assert!(matches!(
            verifier.verify(&wrong_audience_token).await,
            Err(GoogleAuthError::InvalidCredential(_))
        ));

        let mut wrong_issuer = claims(json!(true));
        wrong_issuer["iss"] = json!("https://evil.example.com");
        let wrong_issuer_token = token("test-key-1", KEY_ONE, wrong_issuer);
        assert!(matches!(
            verifier.verify(&wrong_issuer_token).await,
            Err(GoogleAuthError::InvalidCredential(_))
        ));
    }

    #[tokio::test]
    async fn rejects_an_unverified_email() {
        let token = token("test-key-1", KEY_ONE, claims(json!(false)));
        assert!(matches!(
            verifier().verify(&token).await,
            Err(GoogleAuthError::InvalidCredential(_))
        ));
    }

    #[tokio::test]
    async fn accepts_the_bare_issuer_form_and_string_verified_flag() {
        let mut claims = claims(json!("true"));
        claims["iss"] = json!("accounts.google.com");
        let token = token("test-key-1", KEY_ONE, claims);
        let identity = verifier().verify(&token).await.expect("valid token");
        assert_eq!(identity.email, "alice@example.com");
    }

    #[tokio::test]
    async fn refetches_once_when_the_key_id_is_unknown() {
        let provider = Arc::new(SequenceProvider::new(vec![
            first_key_only(document(JWKS)),
            document(JWKS_ROTATED),
        ]));
        let verifier = JwksVerifier::with_provider(CLIENT_ID, provider.clone());
        let token = token("test-key-2", KEY_TWO, claims(json!(true)));
        let identity = verifier.verify(&token).await.expect("rotated key");
        assert_eq!(identity.sub, "google-subject-1");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn rejects_a_token_signed_with_an_unknown_key_after_one_refetch() {
        let provider = Arc::new(SequenceProvider::new(vec![document(JWKS)]));
        let verifier = JwksVerifier::with_provider(CLIENT_ID, provider.clone());
        let token = token("not-published", KEY_TWO, claims(json!(true)));
        assert!(matches!(
            verifier.verify(&token).await,
            Err(GoogleAuthError::InvalidCredential(_))
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fails_closed_when_the_key_source_is_unavailable() {
        let provider = Arc::new(SequenceProvider::new(Vec::new()));
        let verifier = JwksVerifier::with_provider(CLIENT_ID, provider);
        let token = token("test-key-1", KEY_ONE, claims(json!(true)));
        assert!(matches!(
            verifier.verify(&token).await,
            Err(GoogleAuthError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn rejects_non_rs256_tokens() {
        use jsonwebtoken::{EncodingKey, Header};
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("test-key-1".to_string());
        let token = encode(
            &header,
            &claims(json!(true)),
            &EncodingKey::from_secret(b"secret"),
        )
        .expect("token");
        assert!(matches!(
            verifier().verify(&token).await,
            Err(GoogleAuthError::InvalidCredential(_))
        ));
    }

    #[test]
    fn verified_flag_accepts_only_true_forms() {
        assert!(is_verified(&Some(json!(true))));
        assert!(is_verified(&Some(json!("TRUE"))));
        assert!(!is_verified(&Some(json!(false))));
        assert!(!is_verified(&Some(json!("false"))));
        assert!(!is_verified(&None));
    }
}
