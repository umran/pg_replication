use anyhow::anyhow;
use hkdf::Hkdf;
use producer::error::ReplicationError;
use sha2::Sha256;
use sqlx::{PgConnection, Postgres, Transaction};

pub struct Lock<Key: ToString + Clone> {
    key: Key,
}

impl<Key: ToString + Clone> Lock<Key> {
    pub fn new(key: Key) -> Self {
        Self { key }
    }

    pub async fn acquire(self, tx: &mut Transaction<'_, Postgres>) -> Result<(), ReplicationError> {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(self.key()?)
            .execute(tx as &mut PgConnection)
            .await
            .map_err(|err| {
                tracing::warn!("db error: treating as recoverable error: {}", err);
                ReplicationError::Recoverable(anyhow!("db error: {}", err))
            })?;

        Ok(())
    }
}

impl<Key: ToString + Clone> Lock<Key> {
    fn key(&self) -> Result<i64, ReplicationError> {
        let input_key_material = self.key.clone().to_string();

        let hkdf: Hkdf<_> = Hkdf::<Sha256>::new(None, input_key_material.as_bytes());

        let mut output_key_material = [0u8; 8];

        hkdf.expand(
            b"SQLx (Rust) Postgres advisory lock",
            &mut output_key_material,
        )
        // `Hkdf::expand()` only returns an error if you ask for more than 255 times the digest size.
        // This is specified by RFC 5869 but not elaborated upon:
        // https://datatracker.ietf.org/doc/html/rfc5869#section-2.3
        // Since we're only asking for 8 bytes, this error shouldn't be returned.
        .map_err(|_| {
            ReplicationError::Fatal(anyhow!(
                "BUG: `output_key_material` should be of acceptable length"
            ))
        })?;

        Ok(i64::from_le_bytes(output_key_material))
    }
}
