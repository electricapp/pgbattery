//! Self-upgrade command implementation.

use anyhow::Result;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::path::Path;
use tracing::{info, warn};

use super::common::{confirm, fsync_dir, http_client};

mod cosign;
use cosign::CosignVerifier;

const DEFAULT_UPGRADE_URL: &str = "https://pgbattery.io/releases";

/// Exit code for `upgrade --check` when a newer version is available.
/// Deliberately not 1 (generic failure) or 2 (clap usage error) so automation
/// can distinguish "update available" from "check failed".
const UPDATE_AVAILABLE_EXIT_CODE: i32 = 10;

/// Current version from Cargo.toml.
pub(super) const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Run the upgrade command.
///
/// # Errors
/// Returns an error if the version check, download, checksum, or signature
/// verification fails, if the release URL is insecure, or if the binary cannot
/// be replaced (e.g. insufficient permissions).
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "each bool maps 1:1 to a clap CLI flag for the upgrade subcommand"
)]
pub async fn run_upgrade(
    check: bool,
    version: Option<String>,
    url: Option<String>,
    yes: bool,
    allow_insecure_http: bool,
    identity: Option<String>,
    insecure_no_verify: bool,
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

    // The upgrade replaces the running binary, so authenticity matters. By
    // default we require a Sigstore cosign keyless signature (see below); when
    // verification is explicitly disabled with --insecure-no-verify, HTTPS is
    // the only authenticity boundary, so refuse plain http unless the operator
    // also opts into that.
    if base_url.starts_with("http://") && !allow_insecure_http {
        anyhow::bail!(
            "Refusing to upgrade over plain HTTP ({base_url}): the response could be \
             tampered with in transit. Use an https:// URL, or pass \
             --allow-insecure-http to override."
        );
    }

    // Build the cosign keyless verifier up front (compiles the identity regex
    // and reads the issuer/identity overrides) so a malformed override fails
    // before we touch the network. The Sigstore trust root (Fulcio CA certs) is
    // fetched lazily, only once we actually have a binary to verify.
    //
    // When --insecure-no-verify is set we skip building the verifier entirely:
    // the upgrade proceeds on SHA-256 integrity + HTTPS only (see the refusal
    // below for the security trade-off).
    let verifier = if insecure_no_verify {
        None
    } else {
        Some(CosignVerifier::from_env(identity.as_deref())?)
    };

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

    // Require either cosign keyless verification or an explicit insecure opt-in
    // before installing. Without verification the binary's authenticity cannot
    // be cryptographically established (only SHA-256 integrity over the
    // transport), so refuse by default — symmetric with --allow-insecure-http.
    if verifier.is_none() && !insecure_no_verify {
        // Unreachable in practice (verifier is always Some unless
        // insecure_no_verify), but kept as a defensive guard.
        anyhow::bail!(
            "Release signature verification is not configured: the new binary's authenticity \
             cannot be cryptographically verified. Pass --insecure-no-verify to upgrade anyway \
             (integrity is still checked via SHA-256 over HTTPS)."
        );
    }

    // current_exe() resolves symlinks, so this is the real file replaced in
    // place. For a symlinked or package-manager install the resolved (often
    // versioned) target — not the symlink — is overwritten; the confirmation
    // below shows that path. Upgrade through the package manager instead if that
    // is how this node was installed.
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

    if verifier.is_none() {
        warn!(
            "--insecure-no-verify: the new binary's authenticity is NOT cryptographically \
             verified (integrity still enforced via SHA-256 over HTTPS). See docs/RELEASING.md."
        );
    }

    // Download new binary
    let binary_name = platform_binary_name();
    let download_url = format!("{base_url}/v{target}/{binary_name}");
    let checksum_url = format!("{download_url}.sha256");
    let signature_url = format!("{download_url}.sig");
    let certificate_url = format!("{download_url}.pem");

    info!(url = %download_url, "Downloading");

    // Check write permissions before downloading
    check_write_permissions(&exe_path)?;

    // Download, verify, and replace
    let backup_path = download_verify_replace(
        &download_url,
        &checksum_url,
        &signature_url,
        &certificate_url,
        verifier.as_ref(),
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
    certificate_url: &str,
    verifier: Option<&CosignVerifier>,
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

    // Verify the Sigstore cosign keyless signature when verification is enabled.
    // The checksum only proves the bytes match the (also-downloaded) .sha256;
    // the signature + Fulcio certificate prove the bytes were signed in this
    // repository's release workflow via GitHub Actions OIDC.
    if let Some(verifier) = verifier {
        info!("Fetching signature + certificate");
        let signature = fetch_text(&client, signature_url, "signature").await?;
        let certificate = fetch_text(&client, certificate_url, "certificate").await?;

        info!("Verifying cosign keyless signature");
        verifier
            .verify_blob(&bytes, signature.trim(), &certificate)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Signature verification FAILED: {e:#}. The binary is not a trusted \
                     pgbattery release — refusing to install."
                )
            })?;
        info!("Signature verified (Sigstore cosign keyless)");
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

/// Fetch a small text asset (signature or certificate) that must exist when
/// verification is enabled. A 404 here means the release is unsigned, which —
/// because verification is configured — is a hard failure.
async fn fetch_text(client: &reqwest::Client, url: &str, what: &str) -> Result<String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch {what}: {e}"))?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "Failed to fetch {what} ({}): {}. Cosign keyless verification is enabled, so an \
             unsigned release cannot be installed. Ensure {} exists on the server (or pass \
             --insecure-no-verify to skip verification).",
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
