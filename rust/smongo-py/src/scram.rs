//! SCRAM-SHA-256 authentication (RFC 7677 / RFC 5802).
//!
//! Implements the server side of the SCRAM protocol:
//! - Credential generation from plaintext passwords (PBKDF2-HMAC-SHA-256)
//! - Two-step conversation: server-first-message, server-final-message

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_ITERATIONS: u32 = 15000;

// ---------------------------------------------------------------------------
// ScramCredential -- stored per user
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ScramCredential {
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
    pub iteration_count: u32,
}

/// Derive SCRAM-SHA-256 credentials from a plaintext password.
pub fn hash_password(password: &str, salt: &[u8], iterations: u32) -> ScramCredential {
    let mut salted_password = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);

    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted_password, b"Server Key");

    ScramCredential {
        salt: salt.to_vec(),
        stored_key,
        server_key,
        iteration_count: iterations,
    }
}

/// Generate a fresh random salt (24 bytes).
///
/// # Panics
/// Panics if the OS CSPRNG is unavailable (fatal for security).
#[allow(clippy::expect_used)]
pub fn generate_salt() -> Vec<u8> {
    let mut salt = vec![0u8; 24];
    getrandom::getrandom(&mut salt).expect("CSPRNG unavailable — cannot generate secure salt");
    salt
}

/// Default iteration count for new credentials.
pub fn default_iterations() -> u32 {
    DEFAULT_ITERATIONS
}

// ---------------------------------------------------------------------------
// ScramConversation -- per-connection handshake state
// ---------------------------------------------------------------------------

pub struct ScramConversation {
    pub username: String,
    #[allow(dead_code)]
    client_nonce: String,
    server_nonce: String,
    client_first_bare: String,
    server_first: String,
    stored_key: [u8; 32],
    server_key: [u8; 32],
}

#[derive(Debug)]
pub struct ScramError(pub String);

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ScramError {}

impl ScramConversation {
    /// Process client-first-message and produce server-first-message.
    ///
    /// `payload` is the raw bytes from the `saslStart` command's `payload` field.
    #[allow(clippy::expect_used)]
    pub fn from_client_first(
        payload: &[u8],
        credential: &ScramCredential,
    ) -> Result<Self, ScramError> {
        let msg = std::str::from_utf8(payload)
            .map_err(|_| ScramError("invalid UTF-8 in client-first-message".into()))?;

        // client-first-message = gs2-header client-first-message-bare
        // gs2-header = "n,," (no channel binding, no authzid)
        let client_first_bare = if let Some(rest) = msg.strip_prefix("n,,") {
            rest
        } else if let Some(rest) = msg.strip_prefix("p=") {
            return Err(ScramError(format!(
                "channel binding not supported: p={rest}"
            )));
        } else {
            return Err(ScramError(
                "invalid gs2-header in client-first-message".into(),
            ));
        };

        let mut username = None;
        let mut client_nonce = None;

        for attr in client_first_bare.split(',') {
            if let Some(val) = attr.strip_prefix("n=") {
                username = Some(val.to_string());
            } else if let Some(val) = attr.strip_prefix("r=") {
                client_nonce = Some(val.to_string());
            }
        }

        let username = username
            .ok_or_else(|| ScramError("missing n= (username) in client-first-message".into()))?;
        let client_nonce = client_nonce
            .ok_or_else(|| ScramError("missing r= (nonce) in client-first-message".into()))?;

        let mut server_nonce_bytes = [0u8; 24];
        getrandom::getrandom(&mut server_nonce_bytes)
            .expect("CSPRNG unavailable — cannot generate secure nonce");
        let server_nonce_suffix = B64.encode(server_nonce_bytes);
        let combined_nonce = format!("{client_nonce}{server_nonce_suffix}");

        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            B64.encode(&credential.salt),
            credential.iteration_count
        );

        Ok(Self {
            username,
            client_nonce,
            server_nonce: combined_nonce,
            client_first_bare: client_first_bare.to_string(),
            server_first: server_first.clone(),
            stored_key: credential.stored_key,
            server_key: credential.server_key,
        })
    }

    /// Return the server-first-message bytes (to be sent as `payload` in the response).
    pub fn server_first_message(&self) -> Vec<u8> {
        self.server_first.as_bytes().to_vec()
    }

    /// Process client-final-message and produce server-final-message.
    ///
    /// Returns the server-final-message bytes on success.
    pub fn verify_client_final(&self, payload: &[u8]) -> Result<Vec<u8>, ScramError> {
        let msg = std::str::from_utf8(payload)
            .map_err(|_| ScramError("invalid UTF-8 in client-final-message".into()))?;

        let mut channel_binding = None;
        let mut received_nonce = None;
        let mut proof_b64 = None;

        for attr in msg.split(',') {
            if let Some(val) = attr.strip_prefix("c=") {
                channel_binding = Some(val.to_string());
            } else if let Some(val) = attr.strip_prefix("r=") {
                received_nonce = Some(val.to_string());
            } else if let Some(val) = attr.strip_prefix("p=") {
                proof_b64 = Some(val.to_string());
            }
        }

        let _cb = channel_binding
            .ok_or_else(|| ScramError("missing c= in client-final-message".into()))?;
        let received_nonce = received_nonce
            .ok_or_else(|| ScramError("missing r= in client-final-message".into()))?;
        let proof_b64 =
            proof_b64.ok_or_else(|| ScramError("missing p= in client-final-message".into()))?;

        if received_nonce != self.server_nonce {
            return Err(ScramError("nonce mismatch".into()));
        }

        // client-final-message-without-proof = everything before ",p="
        let without_proof = match msg.rfind(",p=") {
            Some(idx) => &msg[..idx],
            None => return Err(ScramError("malformed client-final-message".into())),
        };

        // AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(&self.stored_key, auth_message.as_bytes());

        // Decode client proof
        let client_proof = B64
            .decode(&proof_b64)
            .map_err(|_| ScramError("invalid base64 in client proof".into()))?;
        if client_proof.len() != 32 {
            return Err(ScramError("client proof wrong length".into()));
        }

        // ClientKey = ClientProof XOR ClientSignature
        let mut recovered_client_key = [0u8; 32];
        for i in 0..32 {
            recovered_client_key[i] = client_proof[i] ^ client_signature[i];
        }

        // Verify: SHA-256(recovered_client_key) == StoredKey
        let recovered_stored_key = sha256(&recovered_client_key);
        if recovered_stored_key != self.stored_key {
            return Err(ScramError("authentication failed".into()));
        }

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let server_signature = hmac_sha256(&self.server_key, auth_message.as_bytes());
        let server_final = format!("v={}", B64.encode(server_signature));

        Ok(server_final.into_bytes())
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Extract the username from a SCRAM client-first-message payload.
pub fn parse_username(payload: &[u8]) -> Result<String, ScramError> {
    let msg = std::str::from_utf8(payload)
        .map_err(|_| ScramError("invalid UTF-8 in client-first-message".into()))?;
    let bare = msg
        .strip_prefix("n,,")
        .ok_or_else(|| ScramError("invalid gs2-header in client-first-message".into()))?;
    for attr in bare.split(',') {
        if let Some(val) = attr.strip_prefix("n=") {
            return Ok(val.to_string());
        }
    }
    Err(ScramError(
        "missing n= (username) in client-first-message".into(),
    ))
}

// ---------------------------------------------------------------------------
// Crypto helpers
// ---------------------------------------------------------------------------

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key)
        .unwrap_or_else(|_| HmacSha256::new_from_slice(&[0]).unwrap_or_else(|_| unreachable!("HMAC-SHA256 accepts any key length")));
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_and_verify_roundtrip() {
        let salt = generate_salt();
        let cred = hash_password("testPassword123", &salt, 4096);
        assert_eq!(cred.salt, salt);
        assert_eq!(cred.iteration_count, 4096);
        assert_ne!(cred.stored_key, [0u8; 32]);
        assert_ne!(cred.server_key, [0u8; 32]);
    }

    #[test]
    fn test_scram_conversation() {
        let salt = b"serversalt123456serversalt123456"[..24].to_vec();
        let cred = hash_password("pencil", &salt, 4096);

        // Simulate client-first-message
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first = format!("n,,n=user,r={client_nonce}");

        let conv = ScramConversation::from_client_first(client_first.as_bytes(), &cred).unwrap();
        assert_eq!(conv.username, "user");

        let server_first = conv.server_first_message();
        let server_first_str = std::str::from_utf8(&server_first).unwrap();
        assert!(server_first_str.starts_with(&format!("r={client_nonce}")));
        assert!(server_first_str.contains(",s="));
        assert!(server_first_str.contains(",i=4096"));

        // Parse server-first to build client-final
        let mut combined_nonce = String::new();
        let mut salt_b64 = String::new();
        let mut iterations = 0u32;
        for attr in server_first_str.split(',') {
            if let Some(v) = attr.strip_prefix("r=") {
                combined_nonce = v.to_string();
            }
            if let Some(v) = attr.strip_prefix("s=") {
                salt_b64 = v.to_string();
            }
            if let Some(v) = attr.strip_prefix("i=") {
                iterations = v.parse().unwrap();
            }
        }

        let salt_decoded = B64.decode(&salt_b64).unwrap();
        let mut salted_password = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(b"pencil", &salt_decoded, iterations, &mut salted_password);

        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key_check = sha256(&client_key);
        assert_eq!(stored_key_check, cred.stored_key);

        let client_final_without_proof = format!("c=biws,r={combined_nonce}");
        let client_first_bare = format!("n=user,r={client_nonce}");
        let auth_message =
            format!("{client_first_bare},{server_first_str},{client_final_without_proof}");

        let client_signature = hmac_sha256(&stored_key_check, auth_message.as_bytes());
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            B64.encode(client_proof)
        );

        let result = conv.verify_client_final(client_final.as_bytes());
        assert!(result.is_ok(), "verify failed: {:?}", result.err());
        let server_final_bytes = result.unwrap();
        let server_final = std::str::from_utf8(&server_final_bytes).unwrap();
        assert!(server_final.starts_with("v="));
    }
}
