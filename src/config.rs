use crate::store::StoreKind;
use crate::Error;
use anyhow::{anyhow, bail, Result};
use byte_unit::Byte;
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::fs;
use std::path::Path;
use yaml_rust::yaml::Array;
use yaml_rust::{Yaml, YamlLoader};

pub struct Config {
    file: String,
    yaml: Yaml,
}

const MAX_CHUNK_SIZE: &str = "1GB";
const DEFAULT_CHUNK_SIZE: &str = "100MB";

impl Config {
    pub fn new(file: &str) -> Result<Self> {
        match fs::read_to_string(Path::new(file))
            .map_err(Error::from)
            .and_then(|yaml| YamlLoader::load_from_str(&yaml).map_err(Error::from))
        {
            Ok(mut configs) => match configs.pop() {
                None => bail!("No configuration found in {}", file),
                Some(yaml) => Ok(Self {
                    file: file.into(),
                    yaml,
                }),
            },
            Err(e) => bail!("Unable to open configuration file: {}", e),
        }
    }

    pub fn get_cache_folder(&self) -> Option<&str> {
        self.yaml["cache"].as_str()
    }

    pub fn get_chunk_size(&self) -> Byte {
        if let Some(size) = self.yaml["chunks"]["size"].as_str() {
            Byte::from_str(size)
                .unwrap()
                .min(Byte::from_str(MAX_CHUNK_SIZE).unwrap())
        } else {
            Byte::from_str(DEFAULT_CHUNK_SIZE).unwrap()
        }
    }

    pub fn get_aggregate_min_size(&self) -> Byte {
        if let Some(size) = self.yaml["aggregate"]["min_size"].as_str() {
            Byte::from_str(size).unwrap().min(self.get_chunk_size())
        } else {
            Byte::from_bytes(0)
        }
    }

    pub fn get_aggregate_size(&self) -> Byte {
        if let Some(size) = self.yaml["aggregate"]["size"].as_str() {
            Byte::from_str(size).unwrap().min(self.get_chunk_size())
        } else {
            self.get_chunk_size()
        }
    }

    pub fn get_pgp_key(&self) -> Result<&str> {
        self.yaml["pgp"]["key"].as_str().ok_or_else(|| {
            anyhow!(
                "Unable to load configuration from {}: `pgp.key` key is mandatory",
                self.file
            )
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

    pub fn get_database_path(&self) -> Result<&str> {
        self.yaml["database"].as_str().ok_or_else(|| {
            anyhow!(
                "Unable to load configuration from {}: `database` key is mandatory",
                self.file
            )
        })
    }

    pub fn get_root_path(&self) -> Result<&str> {
        self.yaml["root"].as_str().ok_or_else(|| {
            anyhow!(
                "Unable to load configuration from {}: `root` key is mandatory",
                self.file
            )
        })
    }

    pub fn get_ignored_files(&self) -> Result<GlobSet> {
        let mut globs = GlobSetBuilder::new();
        for glob in self.yaml["ignore"]
            .as_vec()
            .unwrap_or(&Array::new())
            .iter()
            .map(|item| item.as_str().unwrap_or_default())
        {
            globs.add(Glob::new(glob)?);
        }
        Ok(globs.build()?)
    }

    pub fn get_store_type(&self) -> Result<StoreKind> {
        let store = self.yaml["store"]["type"].as_str().unwrap_or("log");
        match store {
            "log" => Ok(StoreKind::Log),
            "s3" => Ok(StoreKind::S3),
            "s3-official" => Ok(StoreKind::S3Official),
            "local" => Ok(StoreKind::Local),
            _ => bail!(
                "Unable to load configuration from {}: `store.type` {} is invalid",
                self.file,
                store
            ),
        }
    }

    pub fn get_local_store_path(&self) -> Result<&str> {
        self.yaml["store"]["local"]["path"].as_str().ok_or_else(|| {
            anyhow!(
                "Unable to load configuration from {}: `store.local.path` is mandatory",
                self.file
            )
        })
    }

    pub fn get_s3_access_key(&self) -> Option<&str> {
        self.yaml["store"]["s3"]["access_key"].as_str()
    }

    pub fn get_s3_secret_key(&self) -> Option<&str> {
        self.yaml["store"]["s3"]["secret_key"].as_str()
    }

    pub fn get_s3_region(&self) -> Result<&str> {
        self.yaml["store"]["s3"]["region"].as_str().ok_or_else(|| {
            anyhow!(
                "Unable to load configuration from {}: `store.s3.region` key is mandatory",
                self.file
            )
        })
    }

    pub fn get_s3_bucket(&self) -> Result<&str> {
        self.yaml["store"]["s3"]["bucket"].as_str().ok_or_else(|| {
            anyhow!(
                "Unable to load configuration from {}: `store.s3.bucket` key is mandatory",
                self.file
            )
        })
    }

    pub fn get_s3_official_bucket(&self) -> Result<&str> {
        self.yaml["store"]["s3-official"]["bucket"]
            .as_str()
            .ok_or_else(|| {
                anyhow!(
                    "Unable to load configuration from {}: `store.s3.bucket` key is mandatory",
                    self.file
                )
            })
    }

    pub fn get_s3_official_multipart_part_size(&self) -> u64 {
        self.yaml["store"]["s3-official"]["multipart_part_size"]
            .as_str()
            .map(|b| Byte::from_str(b).unwrap().get_bytes() as u64)
            .unwrap_or_default()
    }
}
