//! Self-upgrade command implementation.

use anyhow::Result;
use minisign_verify::{PublicKey, Signature};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::path::Path;
use tracing::{info, warn};

use super::common::{confirm, fsync_dir, http_client};

const DEFAULT_UPGRADE_URL: &str = "https://pgbattery.io/releases";

/// Exit code for `upgrade --check` when a newer version is available.
/// Deliberately not 1 (generic failure) or 2 (clap usage error) so automation
/// can distinguish "update available" from "check failed".
const UPDATE_AVAILABLE_EXIT_CODE: i32 = 10;

/// Current version from Cargo.toml.
pub(super) const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Minisign public key embedded for verifying release artifacts.
///
/// `None` until the project publishes a signing key. Once set to the base64
/// key line (`Some("RWQ…")`), every `upgrade` *requires* a valid `.minisig`
/// signature over the downloaded binary. See `docs/RELEASING.md` for how to
/// generate the keypair and fill this in.
const RELEASE_PUBLIC_KEY: Option<&str> = None;

/// Env var holding a minisign public key (base64 line), overriding the embedded
/// one. Operators can pin authenticity even before a key is baked into the binary.
const RELEASE_PUBLIC_KEY_ENV: &str = "PGBATTERY_RELEASE_PUBLIC_KEY";

/// Run the upgrade command.
///
/// # Errors
/// Returns an error if the version check, download, checksum, or signature
/// verification fails, if the release URL is insecure, or if the binary cannot
/// be replaced (e.g. insufficient permissions).
pub async fn run_upgrade(
    check: bool,
    version: Option<String>,
    url: Option<String>,
    yes: bool,
    allow_insecure_http: bool,
    public_key: Option<String>,
) -> Result<()> {
    // Initialize minimal logging for CLI
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(false)
        .without_time()
        .init();

    // Windows not supported
    if cfg!(windows) {
        anyhow::bail!(
            "Self-upgrade is not supported on Windows. \
             Download the new binary manually from the releases page."
        );
    }

    let base_url = url.unwrap_or_else(|| DEFAULT_UPGRADE_URL.to_string());
    let base_url = base_url.trim_end_matches('/');

    // The upgrade replaces the running binary, so authenticity matters. We have
    // no release-signing infrastructure yet (that needs a signing key + signed
    // artifacts), so HTTPS is the authenticity boundary: TLS authenticates the
    // release server, and the SHA-256 check guarantees integrity of the bytes.
    // Refuse plain http unless the operator explicitly opts in.
    if base_url.starts_with("http://") && !allow_insecure_http {
        anyhow::bail!(
            "Refusing to upgrade over plain HTTP ({base_url}): the response could be \
             tampered with in transit. Use an https:// URL, or pass \
             --allow-insecure-http to override."
        );
    }

    // Resolve the release signing key up front (CLI > env > embedded) so a bad
    // path or malformed key fails before we touch the network. When present, a
    // valid signature becomes mandatory below; when absent we fall back to
    // checksum + HTTPS.
    let release_key = resolve_release_public_key(public_key.as_deref())?;

    // Fetch latest version
    let latest = fetch_latest_version(base_url).await?;

    info!(current = VERSION, latest = %latest, "Version check");

    if check {
        if latest == VERSION {
            info!("Already running latest version.");
            return Ok(());
        }
        info!("Update available. Run 'pgbattery upgrade' to install.");
        // Distinct exit code so automation can branch on "update available"
        // without parsing log output (documented in `--check`'s help text).
        std::process::exit(UPDATE_AVAILABLE_EXIT_CODE);
    }

    // Determine target version
    let target = version.unwrap_or_else(|| latest.clone());

    if target == VERSION {
        info!("Already at version {}", VERSION);
        return Ok(());
    }

    // Safety warning
    warn!(
        "If this node is running in a cluster, drain it first: \
         pgbattery cluster remove --self"
    );

    let exe_path = std::env::current_exe()?;

    // Replacing the running binary is irreversible without the backup copy;
    // confirm before touching it (or require --yes for non-interactive use).
    if !confirm(
        &format!(
            "Upgrade pgbattery {} -> {} and replace the binary at {}?",
            VERSION,
            target,
            exe_path.display()
        ),
        yes,
    )? {
        info!("Upgrade aborted.");
        return Ok(());
    }

    if release_key.is_none() {
        warn!(
            "No release signing key configured: authenticity is not cryptographically \
             verified (integrity still enforced via SHA-256 over HTTPS). See docs/RELEASING.md."
        );
    }

    // Download new binary
    let binary_name = platform_binary_name();
    let download_url = format!("{base_url}/v{target}/{binary_name}");
    let checksum_url = format!("{download_url}.sha256");
    let signature_url = format!("{download_url}.minisig");

    info!(url = %download_url, "Downloading");

    // Check write permissions before downloading
    check_write_permissions(&exe_path)?;

    // Download, verify, and replace
    let backup_path = download_verify_replace(
        &download_url,
        &checksum_url,
        &signature_url,
        release_key.as_ref(),
        &exe_path,
    )
    .await?;

    info!(version = %target, "Upgrade complete");
    info!(backup = %backup_path.display(), "Previous version backed up (delete after verification or keep for rollback)");
    info!("Restart the service: systemctl restart pgbattery");

    Ok(())
}

fn platform_binary_name() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    format!("pgbattery-{os}-{arch}")
}

async fn fetch_latest_version(base_url: &str) -> Result<String> {
    let client = http_client(30)?;
    let url = format!("{base_url}/latest");

    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch version info: {e}"))?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "Failed to fetch latest version ({}). Check URL: {}",
            resp.status(),
            url
        );
    }

    Ok(resp.text().await?.trim().to_string())
}

fn check_write_permissions(exe_path: &Path) -> Result<()> {
    let parent = exe_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Cannot determine parent directory of executable"))?;

    // Try to create a test file to check permissions
    let test_path = parent.join(".pgbattery_upgrade_test");
    match std::fs::write(&test_path, b"test") {
        Ok(()) => {
            std::fs::remove_file(&test_path).ok();
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            anyhow::bail!(
                "Permission denied: cannot write to {}. \
                 Run with sudo or download manually: \
                 curl -fsSL <url> -o /tmp/pgbattery && sudo mv /tmp/pgbattery {}",
                parent.display(),
                exe_path.display()
            );
        }
        Err(e) => {
            anyhow::bail!("Cannot write to {}: {}", parent.display(), e);
        }
    }
}

async fn download_verify_replace(
    binary_url: &str,
    checksum_url: &str,
    signature_url: &str,
    release_key: Option<&PublicKey>,
    exe_path: &Path,
) -> Result<std::path::PathBuf> {
    let client = http_client(300)?; // 5 min timeout for large binary

    // Download checksum first
    info!("Fetching checksum");
    let expected_checksum = fetch_checksum(&client, checksum_url).await?;

    // Download binary
    info!("Downloading binary");
    let resp = client
        .get(binary_url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Download failed: {e}"))?;

    if !resp.status().is_success() {
        anyhow::bail!("Download failed ({}): {}", resp.status(), binary_url);
    }

    let bytes = resp.bytes().await?;
    if bytes.is_empty() {
        anyhow::bail!("Downloaded file is empty");
    }

    info!(bytes = bytes.len(), "Download complete");

    // Verify checksum
    info!("Verifying checksum");
    let actual_checksum = compute_sha256(&bytes);
    if actual_checksum != expected_checksum {
        anyhow::bail!(
            "Checksum mismatch! Expected: {expected_checksum}, Got: {actual_checksum}. \
             The download may be corrupted or tampered with."
        );
    }
    info!("Checksum verified");

    // Verify the signature when a release key is configured. The checksum only
    // proves the bytes match the (also-downloaded) .sha256; the signature proves
    // they were produced by the holder of the release private key.
    if let Some(key) = release_key {
        info!("Verifying signature");
        let sig_text = fetch_signature(&client, signature_url).await?;
        let signature = Signature::decode(&sig_text)
            .map_err(|e| anyhow::anyhow!("Malformed signature file ({signature_url}): {e}"))?;
        key.verify(&bytes, &signature, false).map_err(|e| {
            anyhow::anyhow!(
                "Signature verification FAILED: {e}. The binary is not signed by the \
                 expected release key — refusing to install."
            )
        })?;
        info!("Signature verified");
    }

    // Write to temp file
    let temp_path = exe_path.with_extension("new");
    let backup_path = exe_path.with_extension("old");

    // Clean up any leftover temp files from previous failed attempts
    std::fs::remove_file(&temp_path).ok();

    if let Err(e) = write_and_sync(&temp_path, &bytes) {
        std::fs::remove_file(&temp_path).ok();
        anyhow::bail!("Failed to write temporary file: {e}");
    }

    // Make executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o755))
        {
            std::fs::remove_file(&temp_path).ok();
            anyhow::bail!("Failed to set executable permissions: {e}");
        }
    }

    // Backup current binary
    if exe_path.exists()
        && let Err(e) = std::fs::rename(exe_path, &backup_path)
    {
        std::fs::remove_file(&temp_path).ok();
        anyhow::bail!("Failed to backup current binary: {e}. Original binary unchanged.");
    }

    // Move new binary into place
    if let Err(e) = std::fs::rename(&temp_path, exe_path) {
        std::fs::remove_file(&temp_path).ok();
        // Rollback: restore backup. A failed restore leaves the host with NO
        // binary at exe_path, so it must be reported as such — claiming
        // "rolled back" would send the operator to a restart that cannot work.
        match std::fs::rename(&backup_path, exe_path) {
            Ok(()) => {
                anyhow::bail!("Failed to install new binary: {e}. Rolled back to previous version.")
            }
            Err(rollback_err) => anyhow::bail!(
                "Failed to install new binary: {e}. Rollback FAILED ({rollback_err}): no binary \
                 remains at {}. Restore manually: mv {} {}",
                exe_path.display(),
                backup_path.display(),
                exe_path.display()
            ),
        }
    }

    // fsync the parent directory so the rename survives a power loss. Without
    // this the kernel can replay the old directory entry on recovery, leaving
    // either the backup or the new binary in an unknown state.
    if let Some(parent) = exe_path.parent()
        && let Err(e) = fsync_dir(parent)
    {
        warn!("Failed to fsync parent directory after install: {e}");
    }

    Ok(backup_path)
}

/// Write `bytes` to `path` and fsync the file before returning. Required for
/// the upgrade temp binary: a torn write followed by a rename leaves a binary
/// that links but corrupts at runtime.
fn write_and_sync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}

/// Resolve the release signing key from CLI flag, env var, or the embedded
/// constant (in that precedence). The flag value is a path to a minisign
/// public-key file; the env var and embedded constant are the base64 key line.
/// Returns `Ok(None)` when no key is configured anywhere.
fn resolve_release_public_key(public_key_path: Option<&str>) -> Result<Option<PublicKey>> {
    if let Some(path) = public_key_path {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read public key file '{path}': {e}"))?;
        return parse_minisign_public_key(&contents).map(Some);
    }
    if let Ok(env_key) = std::env::var(RELEASE_PUBLIC_KEY_ENV) {
        let trimmed = env_key.trim();
        if !trimmed.is_empty() {
            return parse_minisign_public_key(trimmed).map(Some);
        }
    }
    RELEASE_PUBLIC_KEY.map_or_else(
        || Ok(None),
        |embedded| parse_minisign_public_key(embedded).map(Some),
    )
}

/// Parse a minisign public key from either a full `.pub` file (a comment line
/// followed by the base64 key) or a bare base64 key line.
fn parse_minisign_public_key(input: &str) -> Result<PublicKey> {
    // The key line is the last non-empty, non-comment line.
    let key_line = input
        .lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("untrusted comment:"))
        .ok_or_else(|| anyhow::anyhow!("No key line found in public key input"))?;
    PublicKey::from_base64(key_line)
        .map_err(|e| anyhow::anyhow!("Invalid minisign public key: {e}"))
}

async fn fetch_signature(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch signature: {e}"))?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "Failed to fetch signature ({}): {}. A release signing key is configured, \
             so an unsigned release cannot be installed. Ensure {} exists on the server.",
            resp.status(),
            url,
            url
        );
    }

    Ok(resp.text().await?)
}

async fn fetch_checksum(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch checksum: {e}"))?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "Failed to fetch checksum ({}): {}. \
             Ensure {}.sha256 exists on the release server.",
            resp.status(),
            url,
            url.trim_end_matches(".sha256")
        );
    }

    // Parse checksum (format: "abc123  filename" or just "abc123")
    let text = resp.text().await?;
    let checksum = text
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Empty checksum file"))?
        .to_lowercase();

    // Validate it looks like a SHA256 hash
    if checksum.len() != 64 || !checksum.chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!("Invalid checksum format: {checksum}");
    }

    Ok(checksum)
}

fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use super::parse_minisign_public_key;

    // minisign's documented example public key (jedisct1/minisign README).
    const EXAMPLE_KEY: &str = "RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3";

    #[test]
    fn parses_full_pub_file_with_comment() {
        let pubfile = format!("untrusted comment: minisign public key 1234\n{EXAMPLE_KEY}\n");
        assert!(parse_minisign_public_key(&pubfile).is_ok());
    }

    #[test]
    fn parses_bare_key_line() {
        assert!(parse_minisign_public_key(EXAMPLE_KEY).is_ok());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_minisign_public_key("not a key").is_err());
        assert!(parse_minisign_public_key("untrusted comment: only a comment\n").is_err());
    }
}
