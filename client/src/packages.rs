//! Package metadata (name and version) from the Ledger API.
//!
//! [`PackageClient`] asks `PackageService.ListVettedPackages` first, because it
//! answers for many package ids in one call. Packages it doesn't list, such as
//! an old version that was unvetted after an upgrade but still has active
//! contracts, are read with `PackageService.GetPackage`, whose Daml-LF archive
//! carries the name and version in its metadata.

use std::time::Duration;

use anyhow::{Context, Result};
use ledger_api::v2::{
    GetPackageRequest, ListVettedPackagesRequest, PackageMetadataFilter, TopologyStateFilter,
    admin::{
        GetParticipantIdRequest, party_management_service_client::PartyManagementServiceClient,
    },
    package_service_client::PackageServiceClient,
};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

/// How long to wait for a connection or for one call.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest `GetPackage` response accepted. Daml-LF archives can be larger
/// than tonic's 4 MiB default.
const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Name and version of a Daml package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageInfo {
    pub package_id: String,
    pub package_name: String,
    pub package_version: String,
}

/// Whether to limit `ListVettedPackages` to this participant.
#[derive(Debug, Clone)]
enum ParticipantFilter {
    /// Not asked yet.
    Unknown,
    /// This participant's id.
    Id(String),
    /// `GetParticipantId` was refused (it needs participant admin rights), so
    /// list the vetting state of every participant.
    Unavailable,
}

/// A Ledger API connection for package lookups. Every call has a timeout, and
/// the connection is reused between calls.
pub struct PackageClient {
    url: String,
    channel: Option<Channel>,
    participant: ParticipantFilter,
}

impl PackageClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            channel: None,
            participant: ParticipantFilter::Unknown,
        }
    }

    async fn channel(&mut self) -> Result<Channel> {
        if let Some(channel) = &self.channel {
            return Ok(channel.clone());
        }
        let channel = Endpoint::new(self.url.clone())?
            .connect_timeout(CALL_TIMEOUT)
            .timeout(CALL_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connecting to {}", self.url))?;
        self.channel = Some(channel.clone());
        Ok(channel)
    }

    /// Runs `call`, and drops the connection if it fails, so the next call
    /// connects again.
    async fn with_channel<T, F, Fut>(&mut self, call: F) -> Result<T>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let channel = self.channel().await?;
        let result = call(channel).await;
        if result.is_err() {
            self.channel = None;
        }
        result
    }

    /// Looks up the name and version of `package_ids`. Ids the ledger doesn't
    /// know are missing from the result. An error means a call failed; the
    /// caller may ask again later.
    pub async fn lookup(
        &mut self,
        access_token: Option<&str>,
        package_ids: &[String],
    ) -> Result<Vec<PackageInfo>> {
        let mut found = self.list_vetted(access_token, package_ids).await?;
        for id in package_ids {
            if found.iter().any(|f| &f.package_id == id) {
                continue;
            }
            if let Some(info) = self.get_package(access_token, id).await? {
                found.push(info);
            }
        }
        Ok(found)
    }

    /// The participant id to filter `ListVettedPackages` by, if this user may
    /// ask for it.
    async fn participant_id(&mut self, access_token: Option<&str>) -> Result<Option<String>> {
        match &self.participant {
            ParticipantFilter::Id(id) => return Ok(Some(id.clone())),
            ParticipantFilter::Unavailable => return Ok(None),
            ParticipantFilter::Unknown => {}
        }
        let req = authorized(GetParticipantIdRequest {}, access_token)?;
        let result = self
            .with_channel(|channel| async move {
                Ok(PartyManagementServiceClient::new(channel)
                    .get_participant_id(req)
                    .await)
            })
            .await?;
        match result {
            Ok(response) => {
                let id = response.into_inner().participant_id;
                self.participant = ParticipantFilter::Id(id.clone());
                Ok(Some(id))
            }
            Err(status)
                if matches!(
                    status.code(),
                    Code::PermissionDenied | Code::Unauthenticated | Code::Unimplemented
                ) =>
            {
                tracing::debug!(
                    "GetParticipantId refused ({}); listing vetted packages of all participants",
                    status.message()
                );
                self.participant = ParticipantFilter::Unavailable;
                Ok(None)
            }
            Err(status) => {
                self.channel = None;
                Err(status).context("GetParticipantId")
            }
        }
    }

    /// `ListVettedPackages` for `package_ids`. Entries without a name (the
    /// ledger leaves it empty for packages not uploaded to this participant)
    /// are skipped.
    async fn list_vetted(
        &mut self,
        access_token: Option<&str>,
        package_ids: &[String],
    ) -> Result<Vec<PackageInfo>> {
        let participant = self.participant_id(access_token).await?;
        let topology_state_filter = participant.map(|id| TopologyStateFilter {
            participant_ids: vec![id],
            synchronizer_ids: vec![],
        });
        let mut found: Vec<PackageInfo> = Vec::new();
        let mut page_token = String::new();
        loop {
            let req = authorized(
                ListVettedPackagesRequest {
                    package_metadata_filter: Some(PackageMetadataFilter {
                        package_ids: package_ids.to_vec(),
                        package_name_prefixes: vec![],
                    }),
                    topology_state_filter: topology_state_filter.clone(),
                    page_token: page_token.clone(),
                    page_size: 0,
                },
                access_token,
            )?;
            let response = self
                .with_channel(|channel| async move {
                    Ok(PackageServiceClient::new(channel)
                        .list_vetted_packages(req)
                        .await
                        .context("ListVettedPackages")?
                        .into_inner())
                })
                .await?;
            for vetted in response.vetted_packages {
                for p in vetted.packages {
                    // A page can hold other packages of the same participant,
                    // so keep only the ones asked for.
                    if !p.package_name.is_empty()
                        && package_ids.contains(&p.package_id)
                        && !found.iter().any(|f| f.package_id == p.package_id)
                    {
                        found.push(PackageInfo {
                            package_id: p.package_id,
                            package_name: p.package_name,
                            package_version: p.package_version,
                        });
                    }
                }
            }
            if response.next_page_token.is_empty() {
                return Ok(found);
            }
            page_token = response.next_page_token;
        }
    }

    /// Reads the name and version from the package's Daml-LF archive.
    /// `None` if the ledger doesn't know the package.
    async fn get_package(
        &mut self,
        access_token: Option<&str>,
        package_id: &str,
    ) -> Result<Option<PackageInfo>> {
        let req = authorized(
            GetPackageRequest {
                package_id: package_id.to_string(),
            },
            access_token,
        )?;
        let result = self
            .with_channel(|channel| async move {
                Ok(PackageServiceClient::new(channel)
                    .max_decoding_message_size(MAX_MESSAGE_SIZE)
                    .get_package(req)
                    .await)
            })
            .await?;
        let response = match result {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => {
                self.channel = None;
                return Err(status).with_context(|| format!("GetPackage {package_id}"));
            }
        };
        let (package_name, package_version) = name_and_version(&response.archive_payload)
            .with_context(|| format!("reading the metadata of package {package_id}"))?;
        Ok(Some(PackageInfo {
            package_id: package_id.to_string(),
            package_name,
            package_version,
        }))
    }
}

fn authorized<T>(message: T, access_token: Option<&str>) -> Result<Request<T>> {
    let mut req = Request::new(message);
    if let Some(token) = access_token {
        let meta = MetadataValue::try_from(format!("Bearer {token}"))?;
        req.metadata_mut().insert("authorization", meta);
    }
    Ok(req)
}

// The few Daml-LF fields needed to read a package's name and version (see
// `daml_lf.proto` and `daml_lf2.proto`). prost skips every other field.

#[derive(Clone, PartialEq, Message)]
struct LfArchivePayload {
    #[prost(bytes = "vec", tag = "2")]
    daml_lf_1: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    daml_lf_2: Vec<u8>,
}

/// `Package` has the same tags for these fields in Daml-LF 1 and 2.
#[derive(Clone, PartialEq, Message)]
struct LfPackage {
    #[prost(string, repeated, tag = "2")]
    interned_strings: Vec<String>,
    #[prost(message, optional, tag = "4")]
    metadata: Option<LfPackageMetadata>,
}

#[derive(Clone, PartialEq, Message)]
struct LfPackageMetadata {
    #[prost(int32, tag = "1")]
    name_interned_str: i32,
    #[prost(int32, tag = "2")]
    version_interned_str: i32,
}

/// Name and version from a `GetPackage` archive payload (a Daml-LF
/// `ArchivePayload`).
pub fn name_and_version(archive_payload: &[u8]) -> Result<(String, String)> {
    let payload = LfArchivePayload::decode(archive_payload)?;
    let package_bytes = if payload.daml_lf_2.is_empty() {
        payload.daml_lf_1
    } else {
        payload.daml_lf_2
    };
    let package = LfPackage::decode(package_bytes.as_slice())?;
    let metadata = package
        .metadata
        .context("the package has no metadata (Daml-LF before 1.8)")?;
    let interned = |i: i32| {
        usize::try_from(i)
            .ok()
            .and_then(|i| package.interned_strings.get(i))
            .cloned()
            .with_context(|| format!("interned string {i} is out of range"))
    };
    Ok((
        interned(metadata.name_interned_str)?,
        interned(metadata.version_interned_str)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(name: &str, version: &str) -> Vec<u8> {
        let package = LfPackage {
            interned_strings: vec!["Main".into(), name.into(), version.into()],
            metadata: Some(LfPackageMetadata {
                name_interned_str: 1,
                version_interned_str: 2,
            }),
        };
        LfArchivePayload {
            daml_lf_1: vec![],
            daml_lf_2: package.encode_to_vec(),
        }
        .encode_to_vec()
    }

    #[test]
    fn reads_name_and_version_from_an_archive() {
        let (name, version) = name_and_version(&archive("my-assets", "1.2.0")).unwrap();
        assert_eq!((name.as_str(), version.as_str()), ("my-assets", "1.2.0"));
    }

    #[test]
    fn rejects_a_package_without_metadata() {
        let payload = LfArchivePayload {
            daml_lf_1: vec![],
            daml_lf_2: LfPackage {
                interned_strings: vec![],
                metadata: None,
            }
            .encode_to_vec(),
        }
        .encode_to_vec();
        assert!(name_and_version(&payload).is_err());
    }

    #[test]
    fn rejects_an_out_of_range_index() {
        let package = LfPackage {
            interned_strings: vec!["x".into()],
            metadata: Some(LfPackageMetadata {
                name_interned_str: 0,
                version_interned_str: 7,
            }),
        };
        let payload = LfArchivePayload {
            daml_lf_1: vec![],
            daml_lf_2: package.encode_to_vec(),
        }
        .encode_to_vec();
        assert!(name_and_version(&payload).is_err());
    }
}
