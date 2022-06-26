use crate::error::Error;
use sequoia_openpgp::crypto::{KeyPair, SessionKey};
use sequoia_openpgp::packet::key::{PublicParts, UnspecifiedRole};
use sequoia_openpgp::packet::{Key, PKESK, SKESK};
use sequoia_openpgp::parse::stream::{
    DecryptionHelper, DecryptorBuilder, MessageStructure, VerificationHelper,
};
use sequoia_openpgp::parse::Parse;
use sequoia_openpgp::policy::{Policy, StandardPolicy};
use sequoia_openpgp::serialize::stream::{
    Armorer, Compressor, Encryptor, LiteralWriter, Message, Recipient,
};
use sequoia_openpgp::types::{CompressionAlgorithm, KeyFlags, SymmetricAlgorithm};
use sequoia_openpgp::{Cert, Fingerprint, KeyHandle, KeyID};
use std::collections::HashMap;
use std::io;
use std::io::{Read, Write};

pub struct Pgp {
    public_keys: HashMap<KeyID, (Fingerprint, Key<PublicParts, UnspecifiedRole>)>,
    secret_keys: HashMap<KeyID, (Fingerprint, KeyPair)>,
    ascii_armor: bool,
    policy: Box<dyn Policy>,
}

pub trait PgpConfig {
    fn get_pgp_key(&self) -> Result<&str, Error>;
    fn get_pgp_armor(&self) -> bool;
    fn get_pgp_passphrase(&self) -> Option<&str>;
}

impl Pgp {
    pub fn new(
        cert_file: &str,
        passphrase: Option<&str>,
        ascii_armor: bool,
    ) -> Result<Self, Error> {
        log::debug!("Reading key from {}", cert_file);
        let policy = StandardPolicy::new();
        let mode = KeyFlags::empty()
            .set_transport_encryption()
            .set_storage_encryption();
        let cert = Cert::from_file(cert_file).map_err(Error::from)?;
        let cert = cert.with_policy(&policy, None).map_err(Error::from)?;

        let mut public_keys = HashMap::new();
        let mut secret_keys = HashMap::new();
        for key in cert
            .keys()
            .supported()
            .alive()
            .revoked(false)
            .key_flags(&mode)
        {
            // todo refactor this
            if key.has_secret() && passphrase.is_some() {
                log::trace!("Key has secret part");
                let key_with_clear_secret = key
                    .clone()
                    .parts_into_secret()
                    .map_err(Error::from)?
                    .key()
                    .to_owned()
                    .decrypt_secret(&passphrase.unwrap().into())
                    .ok();
                if let Some(key) = key_with_clear_secret {
                    secret_keys.insert(
                        key.keyid(),
                        (cert.fingerprint(), key.into_keypair().map_err(Error::from)?),
                    );
                } else {
                    eprintln!("Could not decrypt {}'s secret", key.keyid());
                    public_keys.insert(key.keyid(), (cert.fingerprint(), key.key().clone()));
                }
            } else {
                public_keys.insert(key.keyid(), (cert.fingerprint(), key.key().clone()));
            }
        }

        if secret_keys.is_empty() && public_keys.is_empty() {
            // todo depends on the context...
            Err(Error::new("No keys found."))
        } else {
            log::debug!(
                "Read {} public keys and {} secret keys",
                public_keys.len(),
                secret_keys.len()
            );
            Ok(Pgp {
                public_keys,
                secret_keys,
                ascii_armor,
                policy: Box::new(policy),
            })
        }
    }

    pub fn encrypt<R, W>(&self, reader: &mut R, writer: &mut W) -> Result<usize, Error>
    where
        R: Read,
        W: Write + Send + Sync,
    {
        let mut message = Message::new(writer);
        if self.ascii_armor {
            message = Armorer::new(message).build().unwrap();
        }
        let message = Encryptor::for_recipients(message, self.get_recipients())
            .build()
            .map_err(Error::from)?;
        let message = Compressor::new(message)
            .algo(CompressionAlgorithm::BZip2)
            .build()
            .map_err(Error::from)?;
        let mut message = LiteralWriter::new(message).build().map_err(Error::from)?;

        let read = io::copy(reader, &mut message).map_err(Error::from)?;
        match message.finalize().map_err(Error::from) {
            Ok(_) => Ok(read as usize),
            Err(e) => Err(e),
        }
    }

    fn get_recipients(&self) -> Vec<Recipient> {
        let mut recipients = Vec::<Recipient>::new();
        for (_keyid, (_fingerprint, pubkey)) in &self.public_keys {
            let recipient = Recipient::from(pubkey);
            recipients.push(recipient)
        }
        for (_keyid, (_fingerprint, keypair)) in &self.secret_keys {
            let recipient = Recipient::from(keypair.public());
            recipients.push(recipient)
        }
        recipients
    }

    pub fn decrypt<R, W>(&self, reader: R, writer: &mut W) -> Result<usize, Error>
    where
        R: Read + Send + Sync,
        W: Write,
    {
        let mut decryptor = DecryptorBuilder::from_reader(reader)
            .map_err(Error::from)?
            .with_policy(self.policy.as_ref(), None, self)?;

        Ok(io::copy(&mut decryptor, writer)? as usize)
    }
}

impl VerificationHelper for &Pgp {
    fn get_certs(&mut self, _ids: &[KeyHandle]) -> sequoia_openpgp::Result<Vec<Cert>> {
        // todo https://gitlab.com/sequoia-pgp/sequoia/blob/main/openpgp/examples/decrypt-with.rs
        Ok(Vec::new())
    }

    fn check(&mut self, _structure: MessageStructure) -> sequoia_openpgp::Result<()> {
        // todo https://gitlab.com/sequoia-pgp/sequoia/blob/main/openpgp/examples/decrypt-with.rs
        Ok(())
    }
}
impl DecryptionHelper for &Pgp {
    fn decrypt<D>(
        &mut self,
        pkesks: &[PKESK],
        _skesks: &[SKESK],
        sym_algo: Option<SymmetricAlgorithm>,
        mut decrypt: D,
    ) -> sequoia_openpgp::Result<Option<Fingerprint>>
    where
        D: FnMut(SymmetricAlgorithm, &SessionKey) -> bool,
    {
        // Try each PKESK until we succeed.
        let mut recipient = None;
        for pkesk in pkesks {
            if let Some((fingerprint, key)) = self.secret_keys.get(pkesk.recipient()) {
                let mut key = key.clone();
                if pkesk
                    .decrypt(&mut key, sym_algo)
                    .map(|(algo, session_key)| decrypt(algo, &session_key))
                    .unwrap_or(false)
                {
                    recipient = Some(fingerprint.clone());
                    break;
                }
            }
        }

        Ok(recipient)
    }
}
