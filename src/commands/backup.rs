//! Backup command implementations.

use anyhow::Result;

use super::common::{
    MANAGEMENT_API_TOKEN_HEADER, ceprintln, colors, confirm, cprintln, format_size, hints,
    http_client, management_api_token, resolve_node_addr, with_spinner,
};
use crate::cli::BackupTypeArg;

fn build_restore_request<'a>(
    client: &reqwest::Client,
    node_addr: &str,
    filename: &'a str,
    database: Option<&'a str>,
    allow_primary: bool,
    management_token: Option<&str>,
) -> reqwest::RequestBuilder {
    let url = format!("http://{node_addr}/api/v1/backup/restore");
    let mut query = vec![("filename", filename)];
    if let Some(db) = database {
        query.push(("database", db));
    }
    // Dump restores replay SQL into the running primary; the server demands
    // this explicit acknowledgement (the operator confirmed via the prompt).
    if allow_primary {
        query.push(("allow_primary", "true"));
    }
    let request = client.post(&url).query(&query);
    if let Some(token) = management_token {
        request.header(MANAGEMENT_API_TOKEN_HEADER, token)
    } else {
        request
    }
}

fn build_create_request(
    client: &reqwest::Client,
    node_addr: &str,
    backup_type: &str,
    management_token: Option<&str>,
) -> reqwest::RequestBuilder {
    let url = format!("http://{node_addr}/api/v1/backup/create");
    let request = client.post(&url).query(&[("type", backup_type)]);
    if let Some(token) = management_token {
        request.header(MANAGEMENT_API_TOKEN_HEADER, token)
    } else {
        request
    }
}

/// Create a backup.
///
/// # Errors
/// Returns an error if the node is unreachable or the backup request fails.
pub async fn run_backup_create(
    backup_type: BackupTypeArg,
    node: Option<String>,
    config_path: Option<String>,
) -> Result<()> {
    use colors::{GREEN, RESET};

    #[derive(serde::Deserialize)]
    struct BackupResponse {
        success: bool,
        message: String,
        path: Option<String>,
        size_bytes: Option<u64>,
        backup_type: Option<crate::config::BackupType>,
        compressed: Option<bool>,
    }

    let node_addr = resolve_node_addr(node, config_path.as_deref()).await?;

    let backup_type_str = match backup_type {
        BackupTypeArg::Full => "full",
        BackupTypeArg::Dump => "dump",
    };

    // Server-side budget: pg_basebackup/pg_dumpall get 1h each
    // (BACKUP_SUBPROCESS_BUDGET), and full backups add a pg_verifybackup
    // pass plus fsync. A shorter client timeout abandons an operation that
    // is still running server-side and invites a concurrent retry.
    let client = http_client(7_200)?;
    let token = management_api_token(config_path.as_deref());

    let resp = with_spinner(
        &format!("Creating {backup_type_str} backup via node {node_addr}"),
        build_create_request(&client, &node_addr, backup_type_str, token.as_deref()).send(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{}\nError: {}", hints::connection_failed(&node_addr), e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "Backup request failed ({}): {}\n{}",
            status,
            body,
            hints::backup_failed()
        );
    }

    let response: BackupResponse = resp.json().await?;
    if response.success {
        cprintln!("{}✓ {}{}", GREEN, response.message, RESET);
        if let Some(path) = response.path {
            cprintln!("  Path: {path}");
        }
        if let Some(size) = response.size_bytes {
            cprintln!("  Size: {}", format_size(size));
        }
        if let Some(backup_type) = response.backup_type {
            let backup_type_str = match backup_type {
                crate::config::BackupType::Full => "full",
                crate::config::BackupType::Dump => "dump",
            };
            cprintln!("  Type: {backup_type_str}");
        }
        if let Some(compressed) = response.compressed {
            cprintln!("  Compressed: {}", if compressed { "yes" } else { "no" });
        }
    } else {
        anyhow::bail!(
            "Backup failed: {}\n{}",
            response.message,
            hints::backup_failed()
        );
    }

    Ok(())
}

/// List backups.
///
/// # Errors
/// Returns an error if the node is unreachable or the list request fails.
pub async fn run_backup_list(
    node: Option<String>,
    json: bool,
    config_path: Option<String>,
) -> Result<()> {
    use colors::{BOLD, CYAN, DIM, GREEN, RESET};

    #[derive(serde::Deserialize, serde::Serialize)]
    struct BackupListResponse {
        backups: Vec<BackupItem>,
        backup_dir: String,
        retention_count: u32,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct BackupItem {
        path: String,
        timestamp: String,
        backup_type: crate::config::BackupType,
        size_bytes: Option<u64>,
        compressed: bool,
    }

    let node_addr = resolve_node_addr(node, config_path.as_deref()).await?;

    let client = http_client(30)?;

    let url = format!("http://{node_addr}/api/v1/backup/list");
    let resp =
        client.get(&url).send().await.map_err(|e| {
            anyhow::anyhow!("{}\nError: {}", hints::connection_failed(&node_addr), e)
        })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Backup list request failed ({status}): {body}");
    }

    let response: BackupListResponse = resp.json().await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    cprintln!(
        "{}Backups{} (dir: {}, retention: {})\n",
        BOLD,
        RESET,
        response.backup_dir,
        response.retention_count
    );

    if response.backups.is_empty() {
        cprintln!("  {DIM}No backups found{RESET}");
    } else {
        cprintln!(
            "  {}{:<32}  {:<6}  {:<10}  {:<5}  TIMESTAMP{}",
            DIM,
            "FILENAME",
            "TYPE",
            "SIZE",
            "GZIP",
            RESET
        );
        cprintln!("  {}{}{}", DIM, "─".repeat(78), RESET);

        for backup in &response.backups {
            // Extract filename from path
            let filename = std::path::Path::new(&backup.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&backup.path);

            let size_str = backup
                .size_bytes
                .map_or_else(|| "-".to_string(), format_size);

            let (type_color, backup_type_str) = match backup.backup_type {
                crate::config::BackupType::Full => (GREEN, "full"),
                crate::config::BackupType::Dump => (CYAN, "dump"),
            };

            cprintln!(
                "  {:<32}  {}{:<6}{}  {:<10}  {:<5}  {}",
                filename,
                type_color,
                backup_type_str,
                RESET,
                size_str,
                if backup.compressed { "yes" } else { "no" },
                backup.timestamp,
            );
        }
    }
    cprintln!();

    Ok(())
}

/// Restore from a backup.
///
/// # Errors
/// Returns an error if confirmation is declined non-interactively, the node is
/// unreachable, or the restore request fails.
pub async fn run_backup_restore(
    filename: String,
    node: Option<String>,
    database: Option<String>,
    yes: bool,
    config_path: Option<String>,
) -> Result<()> {
    use colors::{GREEN, RESET};

    #[derive(serde::Deserialize)]
    struct RestoreResponse {
        success: bool,
        message: String,
    }

    // Client-side defense in depth: the server canonicalizes within its backup
    // directory, but the CLI should never send a name that isn't a bare
    // basename. This also stops a name like `../dump_x` from spoofing the
    // `dump_` prefix heuristic that selects the destructive allow_primary path.
    if filename.is_empty()
        || filename.contains('/')
        || filename.contains('\\')
        || filename.contains("..")
    {
        anyhow::bail!("Invalid backup filename {filename:?}: must be a bare filename, not a path");
    }

    let node_addr = resolve_node_addr(node, config_path.as_deref()).await?;

    // Dump restores replay SQL into the running primary, so the server only
    // accepts them on the leader with an explicit allow_primary
    // acknowledgement; full restores overwrite a standby's data directory.
    let is_dump = filename.starts_with("dump_");
    let prompt = if is_dump {
        format!(
            "Restore dump '{filename}' on node {node_addr}? This replays destructive SQL \
             (DROP/CREATE DATABASE, roles) into the running primary."
        )
    } else {
        format!(
            "Restore backup '{filename}' on node {node_addr}? \
             This overwrites the data directory and the node must be stopped."
        )
    };

    if !confirm(&prompt, yes)? {
        ceprintln!("Restore aborted.");
        return Ok(());
    }

    // Server-side budget: each restore subprocess gets 1h
    // (BACKUP_SUBPROCESS_BUDGET) plus decompression and fsync passes. A
    // shorter client timeout abandons an operation that is still running
    // server-side and invites a concurrent retry.
    let client = http_client(7_200)?;
    let token = management_api_token(config_path.as_deref());

    let resp = with_spinner(
        &format!("Restoring backup '{filename}' via node {node_addr}"),
        build_restore_request(
            &client,
            &node_addr,
            &filename,
            database.as_deref(),
            is_dump,
            token.as_deref(),
        )
        .send(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{}\nError: {}", hints::connection_failed(&node_addr), e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "Restore request failed ({}): {}\n{}",
            status,
            body,
            hints::restore_failed()
        );
    }

    let response: RestoreResponse = resp.json().await?;
    if response.success {
        cprintln!("{}✓ {}{}", GREEN, response.message, RESET);
    } else {
        anyhow::bail!(
            "Restore failed: {}\n{}",
            response.message,
            hints::restore_failed()
        );
    }

    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::{MANAGEMENT_API_TOKEN_HEADER, build_create_request, build_restore_request};

    #[test]
    fn test_build_restore_request_encodes_query_values() {
        let client = reqwest::Client::new();
        let request = build_restore_request(
            &client,
            "127.0.0.1:9091",
            "full backup&v1.sql",
            Some("app/db?prod=true"),
            false,
            None,
        )
        .build();
        assert!(request.is_ok());
        let request = request.unwrap_or_else(|_| unreachable!());
        let query = request.url().query().unwrap_or_default().to_string();
        assert!(query.contains("filename=full+backup%26v1.sql"));
        assert!(query.contains("database=app%2Fdb%3Fprod%3Dtrue"));
        assert!(!query.contains("allow_primary"));
    }

    #[test]
    fn test_build_restore_request_sends_allow_primary_for_dumps() {
        let client = reqwest::Client::new();
        let request = build_restore_request(
            &client,
            "127.0.0.1:9091",
            "dump_20260101_120000.sql",
            None,
            true,
            None,
        )
        .build();
        assert!(request.is_ok());
        let request = request.unwrap_or_else(|_| unreachable!());
        let query = request.url().query().unwrap_or_default().to_string();
        assert!(query.contains("allow_primary=true"));
    }

    #[test]
    fn test_build_create_request_includes_token_and_type() {
        let client = reqwest::Client::new();
        let request =
            build_create_request(&client, "127.0.0.1:9091", "dump", Some("secret-token")).build();
        assert!(request.is_ok());
        let request = request.unwrap_or_else(|_| unreachable!());
        let query = request.url().query().unwrap_or_default().to_string();
        assert!(query.contains("type=dump"));
        let header = request.headers().get(MANAGEMENT_API_TOKEN_HEADER);
        assert_eq!(header.and_then(|v| v.to_str().ok()), Some("secret-token"));
    }
}
