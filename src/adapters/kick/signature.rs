//! Verificación de la firma de los webhooks de Kick:
//! RSA PKCS#1 v1.5 + SHA-256 sobre `"{message_id}.{timestamp}.{body}"`.

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use rsa::{pkcs8::DecodePublicKey, Pkcs1v15Sign, RsaPublicKey};
use sha2::{Digest, Sha256};

pub struct Verifier {
    key: RsaPublicKey,
}

impl Verifier {
    pub fn from_pem(pem: &str) -> Result<Self> {
        let key = RsaPublicKey::from_public_key_pem(pem.trim())
            .context("la clave pública de Kick no es un PEM válido")?;
        Ok(Self { key })
    }

    pub fn verify(&self, message_id: &str, timestamp: &str, body: &[u8], signature_b64: &str) -> bool {
        let Ok(signature) = STANDARD.decode(signature_b64.trim()) else {
            return false;
        };
        let mut hasher = Sha256::new();
        hasher.update(message_id.as_bytes());
        hasher.update(b".");
        hasher.update(timestamp.as_bytes());
        hasher.update(b".");
        hasher.update(body);
        let digest = hasher.finalize();
        self.key
            .verify(Pkcs1v15Sign::new::<Sha256>(), &digest, &signature)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::{pkcs8::EncodePublicKey, RsaPrivateKey};

    #[test]
    fn acepta_firma_valida_y_rechaza_alteraciones() {
        let private = RsaPrivateKey::new(&mut rand::thread_rng(), 1024).unwrap();
        let pem = private
            .to_public_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let verifier = Verifier::from_pem(&pem).unwrap();

        let (id, ts, body) = ("01HXYZ", "2026-11-04T18:00:00Z", br#"{"hola":1}"#);
        let digest = Sha256::digest(format!("{id}.{ts}.{}", std::str::from_utf8(body).unwrap()));
        let signature = private.sign(Pkcs1v15Sign::new::<Sha256>(), &digest).unwrap();
        let signature = STANDARD.encode(signature);

        assert!(verifier.verify(id, ts, body, &signature));
        assert!(!verifier.verify(id, ts, br#"{"hola":2}"#, &signature));
        assert!(!verifier.verify("otro-id", ts, body, &signature));
        assert!(!verifier.verify(id, ts, body, "no-es-base64!!"));
    }
}
