//! Where tables live: a local folder for testing, or object storage (S3,
//! Azure, Google Cloud Storage) in production.
//!
//! A location is written as a path (`tables`, `/var/lib/analytics`) or a URL
//! (`s3://bucket/analytics`, `az://container/analytics`,
//! `gs://bucket/analytics`). Credentials come from the usual environment
//! variables of each cloud (`AWS_ACCESS_KEY_ID`, `AWS_REGION`,
//! `AZURE_STORAGE_ACCOUNT_NAME`, `GOOGLE_APPLICATION_CREDENTIALS`…), and can
//! be given or overridden as storage options. Cloud storage needs the
//! crate's `s3`, `azure` or `gcs` feature.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use deltalake::logstore::object_store::path::Path as StorePath;
use deltalake::logstore::object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use deltalake::logstore::store_for;
use deltalake::{DeltaTable, DeltaTableBuilder, ensure_table_uri};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub url: Url,
    /// Extra options for the object store, such as `aws_endpoint` for
    /// MinIO or `aws_region`. Most setups need none.
    pub options: HashMap<String, String>,
}

impl Location {
    /// A path or a URL. A relative path is taken from the current folder.
    pub fn parse(text: &str, options: HashMap<String, String>) -> Result<Location> {
        let text = text.trim();
        if text.is_empty() {
            return Err(anyhow!("no table location given"));
        }
        let url = if text.contains("://") {
            Url::parse(text).with_context(|| format!("{text} is not a valid URL"))?
        } else {
            let path = std::path::absolute(text).with_context(|| format!("bad path {text}"))?;
            ensure_table_uri(path.to_string_lossy()).with_context(|| format!("bad path {text}"))?
        };
        Ok(Location { url, options })
    }

    /// The local folder, if this is one.
    pub fn local_path(&self) -> Option<PathBuf> {
        (self.url.scheme() == "file")
            .then(|| self.url.to_file_path().ok())
            .flatten()
    }

    /// The location `name` inside this one.
    pub fn join(&self, name: &str) -> Location {
        let mut url = self.url.clone();
        let path = format!("{}/{}", url.path().trim_end_matches('/'), name);
        url.set_path(&path);
        Location {
            url,
            options: self.options.clone(),
        }
    }

    /// The last part of the path, e.g. the table folder's name.
    pub fn name(&self) -> String {
        self.url
            .path()
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string()
    }

    /// The object store rooted here.
    pub fn store(&self) -> Result<Arc<dyn ObjectStore>> {
        if let Some(path) = self.local_path() {
            std::fs::create_dir_all(&path).ok();
        }
        store_for(&self.url, self.options.iter()).map_err(|e| self.error(e))
    }

    /// The Delta table here, loaded if it exists, or ready to create.
    pub async fn table(&self) -> Result<DeltaTable> {
        if let Some(path) = self.local_path() {
            std::fs::create_dir_all(&path)
                .with_context(|| format!("could not create {}", path.display()))?;
        }
        let mut table = DeltaTableBuilder::from_url(self.url.clone())
            .map_err(|e| self.error(e))?
            .with_storage_options(self.options.clone())
            .build()
            .map_err(|e| self.error(e))?;
        if table
            .verify_deltatable_existence()
            .await
            .map_err(|e| self.error(e))?
        {
            table.load().await.map_err(|e| self.error(e))?;
        }
        Ok(table)
    }

    fn error(&self, e: impl std::fmt::Display) -> anyhow::Error {
        let hint = match self.url.scheme() {
            "file" => "",
            "s3" | "s3a" => " (needs the `s3` feature and AWS credentials)",
            "az" | "abfs" | "abfss" | "azure" => {
                " (needs the `azure` feature and Azure credentials)"
            }
            "gs" => " (needs the `gcs` feature and Google credentials)",
            _ => "",
        };
        anyhow!("{}: {e}{hint}", self.url)
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.local_path() {
            Some(p) => write!(f, "{}", p.display()),
            None => write!(f, "{}", self.url),
        }
    }
}

/// Parses `key=value` storage options, as given on a command line.
pub fn parse_options(pairs: &[String]) -> Result<HashMap<String, String>> {
    pairs
        .iter()
        .map(|p| {
            let (k, v) = p
                .split_once('=')
                .ok_or_else(|| anyhow!("storage option {p:?} is not key=value"))?;
            Ok((k.trim().to_string(), v.trim().to_string()))
        })
        .collect()
}

/// Reads a small file from a table's store; `None` if it doesn't exist.
pub async fn read_file(store: &dyn ObjectStore, path: &str) -> Result<Option<Vec<u8>>> {
    match store.get(&StorePath::from(path)).await {
        Ok(r) => Ok(Some(r.bytes().await?.to_vec())),
        Err(deltalake::logstore::object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow!("could not read {path}: {e}")),
    }
}

/// Writes a small file to a table's store, replacing it whole.
pub async fn write_file(store: &dyn ObjectStore, path: &str, bytes: Vec<u8>) -> Result<()> {
    store
        .put(&StorePath::from(path), PutPayload::from(bytes))
        .await
        .map_err(|e| anyhow!("could not write {path}: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_and_urls() {
        let l = Location::parse("tables", HashMap::new()).unwrap();
        assert!(l.local_path().unwrap().ends_with("tables"));
        assert_eq!(l.join("contract-lifetimes").name(), "contract-lifetimes");
        let s = Location::parse("s3://bucket/analytics/", HashMap::new()).unwrap();
        assert_eq!(s.local_path(), None);
        assert_eq!(s.join("x").url.as_str(), "s3://bucket/analytics/x");
        assert_eq!(
            parse_options(&["aws_region = eu-central-1".into()]).unwrap()["aws_region"],
            "eu-central-1"
        );
    }
}
