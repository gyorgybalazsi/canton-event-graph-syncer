use crate::jwt::{KeycloakAuthMethod, KeycloakConfig};
use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

fn placeholder_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Matches `${VAR}` where VAR uses [A-Za-z0-9_]. Letters of either case
    // are accepted; the convention in this repo is uppercase
    // (SURREAL_PASSWORD, MAINNET_CLIENT_SECRET, etc.).
    RE.get_or_init(|| Regex::new(r"\$\{([A-Za-z0-9_]+)\}").unwrap())
}

/// Replaces `${VAR}` substrings with the value of the environment variable `VAR`.
///
/// `field` is a human-readable path (e.g. `"keycloak.client_secret"`) used
/// only to produce an actionable error message when a referenced variable is
/// unset. Returns `s` unchanged when it contains no placeholders.
pub fn substitute_env(s: &str, field: &str) -> Result<String> {
    let re = placeholder_re();
    let mut out = String::with_capacity(s.len());
    let mut last_end = 0;
    for cap in re.captures_iter(s) {
        let m = cap.get(0).unwrap();
        let name = &cap[1];
        let val = std::env::var(name).with_context(|| {
            format!(
                "config field '{}' references ${{{}}} but the env var is not set. \
             Set it in the environment or `.env`, or run under \
             `op run --env-file=secrets.env.1password -- ...` (see README, \"Secrets\")",
                field, name
            )
        })?;
        out.push_str(&s[last_end..m.start()]);
        out.push_str(&val);
        last_end = m.end();
    }
    out.push_str(&s[last_end..]);
    Ok(out)
}

/// Resolves `${VAR}` placeholders in the secret-bearing Keycloak fields
/// (client secret, user name and password).
pub fn resolve_keycloak_secrets(kc: &mut KeycloakConfig) -> Result<()> {
    match &mut kc.auth_method {
        KeycloakAuthMethod::ClientCredentials { client_secret } => {
            *client_secret = substitute_env(client_secret, "keycloak.client_secret")?;
        }
        KeycloakAuthMethod::Password {
            username,
            password,
            client_secret,
        } => {
            *username = substitute_env(username, "keycloak.username")?;
            *password = substitute_env(password, "keycloak.password")?;
            if let Some(cs) = client_secret.as_mut() {
                *cs = substitute_env(cs, "keycloak.client_secret")?;
            }
        }
    }
    Ok(())
}

/// Top-level config file structure. Only deserializes the fields the client needs;
/// unknown keys (logging, surrealdb, etc.) are silently ignored by serde.
#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    pub active_profile: String,
    pub profiles: HashMap<String, ProfileConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ProfileConfig {
    pub ledger: LedgerConfig,
    pub keycloak: Option<KeycloakConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LedgerConfig {
    pub url: String,
    pub parties: Option<Vec<String>>,
    pub fake_jwt_user: String,
}

/// Resolved config after selecting a profile.
#[derive(Debug)]
pub struct Config {
    pub ledger: LedgerConfig,
    pub keycloak: Option<KeycloakConfig>,
}

/// Read and parse a config file, then resolve the given profile.
pub fn read_config<P: AsRef<Path>>(path: P, profile: Option<&str>) -> Result<Config> {
    let s = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read config file '{}'", path.as_ref().display()))?;
    let config_file: ConfigFile = toml::from_str(&s).context("failed to parse TOML config")?;
    resolve_config(config_file, profile)
}

/// Auto-discover config.toml and resolve the given profile.
///
/// Lookup order:
/// 1. `./config/config.toml` (relative to cwd)
/// 2. `./syncer/config/config.toml` (repo root fallback)
/// 3. `CARGO_MANIFEST_DIR/config/config.toml` (for `cargo run`)
pub fn read_config_from_toml(profile: Option<&str>) -> Result<Config> {
    let candidates = [
        Some(std::path::PathBuf::from("config").join("config.toml")),
        Some(
            std::path::PathBuf::from("syncer")
                .join("config")
                .join("config.toml"),
        ),
        std::env::var("CARGO_MANIFEST_DIR").ok().map(|root| {
            std::path::PathBuf::from(root)
                .join("config")
                .join("config.toml")
        }),
    ];

    for candidate in candidates.into_iter().flatten() {
        if candidate.exists() {
            return read_config(&candidate, profile);
        }
    }

    anyhow::bail!(
        "Could not find config.toml. Searched: ./config/config.toml, \
         ./syncer/config/config.toml, CARGO_MANIFEST_DIR/config/config.toml. \
         Use --config-file to specify a path."
    )
}

fn resolve_config(config_file: ConfigFile, profile_override: Option<&str>) -> Result<Config> {
    let profile_name = profile_override.unwrap_or(&config_file.active_profile);

    let profile = config_file.profiles.get(profile_name).with_context(|| {
        format!(
            "profile '{}' not found. Available profiles: {:?}",
            profile_name,
            config_file.profiles.keys().collect::<Vec<_>>()
        )
    })?;

    let mut keycloak = profile.keycloak.clone();
    if let Some(kc) = keycloak.as_mut() {
        resolve_keycloak_secrets(kc)?;
    }
    Ok(Config {
        ledger: profile.ledger.clone(),
        keycloak,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"
active_profile = "mainnet"

[profiles.mainnet.ledger]
url = "https://ledger.example.com"
fake_jwt_user = "unused"

[profiles.mainnet.keycloak]
client_id = "reader"
token_endpoint = "https://keycloak.example.com/token"
grant_type = "client_credentials"
client_secret = "${TEST_CLIENT_CONFIG_SECRET}"
"#;

    #[test]
    fn resolve_config_substitutes_keycloak_secret() {
        // SAFETY: env mutation in a unit test; the var name is unique to it.
        unsafe { std::env::set_var("TEST_CLIENT_CONFIG_SECRET", "s3cret") };
        let file: ConfigFile = toml::from_str(CONFIG).unwrap();
        let cfg = resolve_config(file, None).unwrap();
        unsafe { std::env::remove_var("TEST_CLIENT_CONFIG_SECRET") };
        match cfg.keycloak.unwrap().auth_method {
            KeycloakAuthMethod::ClientCredentials { client_secret } => {
                assert_eq!(client_secret, "s3cret")
            }
            other => panic!("unexpected auth method: {other:?}"),
        }
    }

    #[test]
    fn resolve_config_fails_on_unset_secret() {
        let file: ConfigFile =
            toml::from_str(&CONFIG.replace("TEST_CLIENT_CONFIG_SECRET", "TEST_CLIENT_UNSET"))
                .unwrap();
        let err = format!("{:#}", resolve_config(file, None).unwrap_err());
        assert!(err.contains("TEST_CLIENT_UNSET"), "{err}");
    }
}
