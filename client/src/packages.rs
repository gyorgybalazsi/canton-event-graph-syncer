//! Package metadata from the Ledger API `PackageService`.

use anyhow::Result;
use ledger_api::v2::{
    ListVettedPackagesRequest, PackageMetadataFilter, package_service_client::PackageServiceClient,
};
use tonic::Request;
use tonic::metadata::MetadataValue;

/// Name and version of a Daml package, as vetted on the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageInfo {
    pub package_id: String,
    pub package_name: String,
    pub package_version: String,
}

/// Looks up the name and version of the given package ids with
/// `PackageService.ListVettedPackages`, which any authenticated user may call.
///
/// Only packages that are vetted on a synchronizer the participant is
/// connected to are returned, so an id can be missing from the result. Each
/// package appears once, even if several participants vetted it.
pub async fn list_vetted_packages(
    url: &str,
    access_token: Option<&str>,
    package_ids: &[String],
) -> Result<Vec<PackageInfo>> {
    let mut client = PackageServiceClient::connect(url.to_string()).await?;
    let mut found: Vec<PackageInfo> = Vec::new();
    let mut page_token = String::new();
    loop {
        let mut req = Request::new(ListVettedPackagesRequest {
            package_metadata_filter: Some(PackageMetadataFilter {
                package_ids: package_ids.to_vec(),
                package_name_prefixes: vec![],
            }),
            topology_state_filter: None,
            page_token: page_token.clone(),
            page_size: 0,
        });
        if let Some(token) = access_token {
            let meta = MetadataValue::try_from(format!("Bearer {token}"))?;
            req.metadata_mut().insert("authorization", meta);
        }
        let response = client.list_vetted_packages(req).await?.into_inner();
        for vetted in response.vetted_packages {
            for p in vetted.packages {
                // A page can hold other packages of the same participant, so
                // keep only the ones asked for.
                if package_ids.contains(&p.package_id)
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
