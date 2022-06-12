use crate::error::Error;
use sequoia_openpgp::packet::key::{PublicParts, UnspecifiedRole};
use sequoia_openpgp::packet::Key;
use sequoia_openpgp::parse::Parse;
use sequoia_openpgp::policy::StandardPolicy;
use sequoia_openpgp::serialize::stream::{
    Armorer, Compressor, Encryptor, LiteralWriter, Message, Recipient,
};
use sequoia_openpgp::types::{CompressionAlgorithm, KeyFlags};
use sequoia_openpgp::Cert;
use std::io;
use std::io::{Read, Write};

pub struct Pgp {
    keys: Vec<Key<PublicParts, UnspecifiedRole>>,
    ascii_armor: bool,
}

impl Pgp {
    pub(crate) fn new(cert_file: &str, ascii_armor: bool) -> Result<Self, Error> {
        println!("Reading key from {}", cert_file);
        let policy = StandardPolicy::new();
        let mode = KeyFlags::empty().set_transport_encryption();
        let cert = Cert::from_file(cert_file).map_err(Error::from)?;
        let cert = cert.with_policy(&policy, None).map_err(Error::from)?;

        let mut pub_keys = Vec::new();
        for key in cert
            .keys()
            .supported()
            .alive()
            .revoked(false)
            .key_flags(&mode)
        {
            pub_keys.push(key.key().to_owned());
        }

        if pub_keys.is_empty() {
            Err(Error::new("No recipient found."))
        } else {
            Ok(Pgp {
                keys: pub_keys,
                ascii_armor,
            })
        }
    }

    pub(crate) fn encrypt<R, W>(&self, reader: &mut R, writer: &mut W) -> Result<usize, Error>
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
        self.keys.iter().map(Recipient::from).collect()
    }
}
