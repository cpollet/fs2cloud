use crate::store::StoreKind;
use crate::Error;
use byte_unit::Byte;
use std::fs;
use std::path::Path;
use yaml_rust::{Yaml, YamlLoader};

pub struct Config {
    file: String,
    yaml: Yaml,
}

const MAX_CHUNK_SIZE: &str = "1GB";
const DEFAULT_CHUNK_SIZE: &str = "75MB";

impl Config {
    pub fn new(file: &str) -> Result<Self, Error> {
        match fs::read_to_string(Path::new(file))
            .map_err(Error::from)
            .and_then(|yaml| YamlLoader::load_from_str(&yaml).map_err(Error::from))
        {
            Ok(mut configs) => match configs.pop() {
                None => Err(Error::new(
                    format!("No configuration found in {}", file).as_str(),
                )),
                Some(yaml) => Ok(Self {
                    file: file.into(),
                    yaml,
                }),
            },
            Err(e) => {
                return Err(Error::new(
                    format!("Unable to open configuration file: {}", e).as_str(),
                ));
            }
        }
    }

    pub fn get_cache_folder(&self) -> Option<&str> {
        self.yaml["cache"].as_str()
    }

    pub fn get_chunk_size(&self) -> Byte {
        Byte::from_str(
            self.yaml["chunks"]["size"]
                .as_str()
                .unwrap_or(DEFAULT_CHUNK_SIZE),
        )
        .unwrap()
        .min(Byte::from_str(MAX_CHUNK_SIZE).unwrap())
    }

    pub fn get_pgp_key(&self) -> Result<&str, Error> {
        self.yaml["pgp"]["key"].as_str().ok_or_else(|| {
            Error::new(&format!(
                "Unable to load configuration from {}: `pgp.key` key is mandatory",
                self.file
            ))
        })
    }

    pub fn get_pgp_armor(&self) -> bool {
        self.yaml["pgp"]["ascii"].as_bool().unwrap_or(false)
    }

    pub fn get_pgp_passphrase(&self) -> Option<&str> {
        self.yaml["pgp"]["passphrase"].as_str()
    }

    pub fn get_max_workers_count(&self) -> usize {
        self.yaml["workers"].as_i64().unwrap_or_default().max(1) as usize
    }

    pub fn get_max_queue_size(&self) -> usize {
        self.yaml["queue_size"].as_i64().unwrap_or_default().max(0) as usize
    }

    pub fn get_database_path(&self) -> Result<&str, Error> {
        self.yaml["database"].as_str().ok_or_else(|| {
            Error::new(&format!(
                "Unable to load configuration from {}: `database` key is mandatory",
                self.file
            ))
        })
    }

    pub fn get_store_type(&self) -> Result<StoreKind, Error> {
        let store = self.yaml["store"]["type"].as_str().unwrap_or("log");
        match store {
            "log" => Ok(StoreKind::Log),
            "s3" => Ok(StoreKind::S3),
            "s3-official" => Ok(StoreKind::S3Official),
            "local" => Ok(StoreKind::Local),
            _ => Err(Error::new(&format!(
                "Unable to load configuration from {}: `store.type` {} is invalid",
                self.file, store
            ))),
        }
    }

    pub fn get_local_store_path(&self) -> Result<&str, Error> {
        self.yaml["store"]["local"]["path"].as_str().ok_or_else(|| {
            Error::new(&format!(
                "Unable to load configuration from {}: `store.local.path` is mandatory",
                self.file
            ))
        })
    }

    pub fn get_s3_access_key(&self) -> Option<&str> {
        self.yaml["store"]["s3"]["access_key"].as_str()
    }

    pub fn get_s3_secret_key(&self) -> Option<&str> {
        self.yaml["store"]["s3"]["secret_key"].as_str()
    }

    pub fn get_s3_region(&self) -> Result<&str, Error> {
        self.yaml["store"]["s3"]["region"].as_str().ok_or_else(|| {
            Error::new(&format!(
                "Unable to load configuration from {}: `store.s3.region` key is mandatory",
                self.file
            ))
        })
    }

    pub fn get_s3_bucket(&self) -> Result<&str, Error> {
        self.yaml["store"]["s3"]["bucket"].as_str().ok_or_else(|| {
            Error::new(&format!(
                "Unable to load configuration from {}: `store.s3.bucket` key is mandatory",
                self.file
            ))
        })
    }

    pub fn get_s3_official_bucket(&self) -> Result<&str, Error> {
        self.yaml["store"]["s3-official"]["bucket"]
            .as_str()
            .ok_or_else(|| {
                Error::new(&format!(
                    "Unable to load configuration from {}: `store.s3.bucket` key is mandatory",
                    self.file
                ))
            })
    }
}
