//! Join command implementation.

use std::path::Path;

use anyhow::Result;
use tracing::{info, warn};

use super::common::{ceprintln, colors, cprintln, fsync_dir, hints, http_client};
use super::init::{JoinConfigParams, generate_join_config};

/// Marker file beside `raft.db` recording the consensus identity (node id)
/// this Raft state was created under. The config file is not authoritative
/// for a joined node: join auto-assigns ids, so a restart that trusted a
/// stale config `node_id` could resume claiming another node's identity.
const NODE_ID_MARKER_FILE: &str = "node_id";

/// Serialize a node id for the marker file.
fn format_node_id_marker(node_id: u64) -> String {
    format!("{node_id}\n")
}

/// Parse the marker file contents back into a node id.
fn parse_node_id_marker(contents: &str) -> Result<u64> {
    let id: u64 = contents
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("not a node id: {contents:?}"))?;
    if id == 0 {
        anyhow::bail!("node id 0 is the auto-assign sentinel, not a valid consensus identity");
    }
    Ok(id)
}

/// Read the persisted consensus identity, if a marker exists.
fn read_node_id_marker(raft_dir: &Path) -> Result<Option<u64>> {
    let path = raft_dir.join(NODE_ID_MARKER_FILE);
    match std::fs::read_to_string(&path) {
        Ok(contents) => parse_node_id_marker(&contents).map(Some).map_err(|e| {
            anyhow::anyhow!(
                "Corrupt node-id marker at {}: {e}. Fix or remove the file \
                 (its sole content must be this node's numeric id).",
                path.display()
            )
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::anyhow!(
            "Failed to read node-id marker at {}: {e}",
            path.display()
        )),
    }
}

/// Atomically persist the consensus identity beside `raft.db` (temp file +
/// fsync + rename), so a crash never leaves a torn marker.
fn write_node_id_marker(raft_dir: &Path, node_id: u64) -> Result<()> {
    use std::io::Write as _;

    std::fs::create_dir_all(raft_dir)
        .map_err(|e| anyhow::anyhow!("Failed to create raft dir {}: {e}", raft_dir.display()))?;
    let path = raft_dir.join(NODE_ID_MARKER_FILE);
    let tmp_path = raft_dir.join(format!(".{NODE_ID_MARKER_FILE}.tmp.{}", std::process::id()));
    let write = || -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|e| anyhow::anyhow!("Failed to create {}: {e}", tmp_path.display()))?;
        file.write_all(format_node_id_marker(node_id).as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, &path)?;
        fsync_dir(raft_dir).ok();
        Ok(())
    };
    write().map_err(|e| {
        std::fs::remove_file(&tmp_path).ok();
        anyhow::anyhow!("Failed to write node-id marker at {}: {e}", path.display())
    })
}

/// Resolve the identity to resume with when local Raft state already exists.
///
/// The marker is authoritative: it was written when this Raft state was
/// created. A conflicting explicit `--node-id` or config `node_id` means the
/// operator is about to resume under a *different* node's consensus identity
/// (duplicate-identity split-brain), so it is a hard error, not a fallback.
fn resolve_resume_node_id(
    raft_dir: &Path,
    cli_node_id: Option<u64>,
    config_node_id: u64,
) -> Result<u64> {
    let marker_path = raft_dir.join(NODE_ID_MARKER_FILE);
    if let Some(marker) = read_node_id_marker(raft_dir)? {
        if let Some(explicit) = cli_node_id
            && explicit != marker
        {
            anyhow::bail!(
                "--node-id {explicit} conflicts with the persisted consensus identity \
                 {marker} ({}). This Raft state belongs to node {marker}; resuming as \
                 {explicit} would duplicate another node's identity. Re-run without \
                 --node-id, or wipe {} to join as a new node.",
                marker_path.display(),
                raft_dir.display()
            );
        }
        if config_node_id != 0 && config_node_id != marker {
            anyhow::bail!(
                "config node_id = {config_node_id} conflicts with the persisted consensus \
                 identity {marker} ({}). Set node_id = {marker} in the config (or remove \
                 it), or wipe {} to join as a new node.",
                marker_path.display(),
                raft_dir.display()
            );
        }
        return Ok(marker);
    }

    // Pre-marker deployment: the config/CLI id is the only record of
    // this state's identity. Persist it so future restarts cannot
    // drift if the config changes.
    let id = cli_node_id.unwrap_or(config_node_id);
    if id == 0 {
        anyhow::bail!(
            "Cannot resume: existing Raft state at {} has no node-id marker and no \
             node_id is configured (0 is the auto-assign sentinel). Pass --node-id \
             <id> matching this node's original identity, or wipe the directory to \
             join as a new node.",
            raft_dir.display()
        );
    }
    warn!(
        node_id = id,
        "No node-id marker beside raft.db; trusting configured identity and recording it"
    );
    write_node_id_marker(raft_dir, id)?;
    Ok(id)
}

/// Response from /api/v1/cluster/join-info.
#[derive(Debug, serde::Deserialize)]
struct JoinInfoResponse {
    next_node_id: u64,
    peers: Vec<PeerInfoResponse>,
}

#[derive(Debug, serde::Deserialize)]
struct PeerInfoResponse {
    id: u64,
    raft_addr: String,
    pg_addr: String,
    mgmt_addr: String,
    metrics_addr: String,
}

fn load_config(config_path: Option<&str>) -> Result<crate::config::Config> {
    let loaded = config_path.map_or_else(crate::config::Config::load, |path| {
        crate::config::Config::load_from(path)
    });
    loaded.map_err(|e| {
        anyhow::anyhow!(
            "Failed to load config: {}\n{}",
            e,
            hints::config_not_found()
        )
    })
}

/// Run the join command - join an existing cluster as a learner.
///
/// # Errors
/// Returns an error if the peer is unreachable, cluster info is invalid, or the
/// join flow (basebackup, registration, startup) fails.
pub async fn run_join(
    peer: String,
    node_id: Option<u64>,
    voter: bool,
    write_config: Option<String>,
    config_path: Option<String>,
) -> Result<()> {
    // 0 is the auto-assign sentinel; a node must never claim it as an
    // explicit consensus identity (tooling treats 0 as "unset").
    if node_id == Some(0) {
        anyhow::bail!("--node-id 0 is invalid (0 means auto-assign; omit --node-id instead)");
    }

    // --write-config is a standalone "generate config file" mode that does
    // not start a node; handle it before we touch local state or init logging.
    if let Some(output_path) = write_config {
        let client = http_client(30)?;
        ceprintln!("Fetching cluster information from {peer}...");
        let join_info = fetch_join_info(&client, &peer).await?;
        let actual_node_id = node_id.unwrap_or(join_info.next_node_id);
        return write_join_config(&output_path, actual_node_id, &join_info);
    }

    // Load config and initialize logging before any code paths that might
    // emit tracing events (HTTP client, Raft startup, etc.).
    let mut config = load_config(config_path.as_deref())?;
    crate::observability::logging::init_logging(config.log_json)?;

    // Restart fast-path: if this node already has local Raft state, skip the
    // join-info fetch and hand off to run_join_flow, which already contains
    // the smart "resume vs rejoin" logic (app.rs:1534). Reaching the
    // bootstrap peer must not be required here: local state plus the
    // persisted identity marker is enough to resume even when that single
    // peer is down. The identity comes from the marker, never from a
    // possibly-stale config node_id.
    let raft_dir = config.get_raft_data_dir();
    if raft_dir.join("raft.db").exists() {
        config.node_id = resolve_resume_node_id(&raft_dir, node_id, config.node_id)?;
        info!(
            node_id = config.node_id,
            "Existing Raft state found - resuming (skipping join-info fetch)"
        );
        let app = crate::app::App::new(config);
        return app.run_join_flow(peer, voter).await;
    }

    // Fresh-join path: fetch cluster info to discover peers and node_id.
    //
    // join trusts the peer address: it pulls the node id, peer list, and leader
    // from `peer` over the (assumed-trusted) management network and then runs
    // pg_basebackup, which overwrites this node's PostgreSQL data directory.
    // There is no cluster-identity handshake, so pointing --peer at the wrong
    // host enrolls this node into the wrong cluster and destroys its data.
    warn!(
        peer = %peer,
        "join will enroll this node into the cluster reached at this peer address and \
         overwrite the local PostgreSQL data directory via pg_basebackup; ensure the peer \
         is a trusted member of the intended cluster"
    );
    let client = http_client(30)?;
    info!(peer = %peer, "Fetching cluster information");
    let join_info = fetch_join_info(&client, &peer).await?;

    let actual_node_id = node_id.unwrap_or(join_info.next_node_id);
    if actual_node_id == 0 {
        anyhow::bail!(
            "Cluster returned next_node_id 0 (the auto-assign sentinel); refusing to join \
             with an unset consensus identity"
        );
    }
    config.node_id = actual_node_id;
    // Persist the assigned identity before any cluster interaction: a service
    // restart re-runs `join`, and the restart fast-path above must resume
    // with this exact id even if the config file still holds a stale one.
    write_node_id_marker(&raft_dir, actual_node_id)?;
    info!(
        node_id = actual_node_id,
        peer = %peer,
        auto_assigned = node_id.is_none(),
        voter,
        "Joining cluster"
    );

    if config.peers.is_empty() {
        info!(
            count = join_info.peers.len(),
            "Auto-discovered peers from cluster"
        );
        for p in &join_info.peers {
            config.peers.push(crate::config::PeerConfig {
                id: p.id,
                raft_addr: p.raft_addr.parse().map_err(|_| {
                    anyhow::anyhow!("Invalid raft_addr from cluster: {}", p.raft_addr)
                })?,
                pg_addr: p
                    .pg_addr
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid pg_addr from cluster: {}", p.pg_addr))?,
                mgmt_addr: Some(p.mgmt_addr.parse().map_err(|_| {
                    anyhow::anyhow!("Invalid mgmt_addr from cluster: {}", p.mgmt_addr)
                })?),
                metrics_addr: Some(p.metrics_addr.parse().map_err(|_| {
                    anyhow::anyhow!("Invalid metrics_addr from cluster: {}", p.metrics_addr)
                })?),
            });
        }
    }

    // The join process handles everything:
    // 1. Discover leader from peer
    // 2. Register as learner with Raft
    // 3. Run pg_basebackup from leader
    // 4. Start the node
    // 5. Auto-promote to voter if --voter flag is set
    let app = crate::app::App::new(config);
    app.run_join_flow(peer, voter).await
}

async fn fetch_join_info(client: &reqwest::Client, peer: &str) -> Result<JoinInfoResponse> {
    let url = format!("http://{peer}/api/v1/cluster/join-info");
    client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{}\nError: {}", hints::connection_failed(peer), e))?
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse join info: {e}"))
}

/// Write a configuration file for joining a cluster.
fn write_join_config(output_path: &str, node_id: u64, join_info: &JoinInfoResponse) -> Result<()> {
    use colors::{BOLD, DIM, GREEN, RESET, YELLOW};

    // Check if file exists
    if Path::new(output_path).exists() {
        anyhow::bail!(
            "Config file '{output_path}' already exists. Remove it first or choose a different path."
        );
    }

    // Build peers list
    let peers: Vec<(u64, String, String, String, String)> = join_info
        .peers
        .iter()
        .map(|p| {
            (
                p.id,
                p.raft_addr.clone(),
                p.pg_addr.clone(),
                p.mgmt_addr.clone(),
                p.metrics_addr.clone(),
            )
        })
        .collect();

    // Use placeholder values - user needs to fill these in
    let listen_addr = "0.0.0.0:5432";
    let raft_addr = "0.0.0.0:5433";
    let metrics_addr = "0.0.0.0:9090";
    let pg_data_dir = "/var/lib/postgresql/data";
    let pg_bin_dir = super::init::detect_pg_bin_dir_silent()
        .unwrap_or_else(|| "/usr/lib/postgresql/16/bin".to_string());

    generate_join_config(JoinConfigParams {
        output_path,
        node_id,
        listen_addr,
        raft_addr,
        metrics_addr,
        pg_data_dir,
        pg_bin_dir: &pg_bin_dir,
        peers: &peers,
    })?;

    cprintln!("{GREEN}✓ Configuration written to: {output_path}{RESET}");
    cprintln!();
    cprintln!("{YELLOW}IMPORTANT:{RESET} Review and update the following in the config file:");
    cprintln!("  - listen_addr: Set to this node's client-facing address");
    cprintln!("  - raft_addr: Set to this node's Raft address (must be reachable by peers)");
    cprintln!("  - metrics_addr: Set to this node's metrics address");
    cprintln!("  - pg_data_dir: Set to your PostgreSQL data directory");
    cprintln!("  - pg_bin_dir: Set to your PostgreSQL binaries path");
    cprintln!();
    cprintln!("{BOLD}Next steps:{RESET}");
    cprintln!("  1. Edit {output_path} with your node's addresses");
    cprintln!("  2. Ensure pg_data_dir exists: mkdir -p <pg_data_dir>");
    cprintln!("  3. Start the node:");
    cprintln!("     {DIM}pgbattery --config {output_path} run{RESET}");
    cprintln!();

    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn marker_format_parse_roundtrip() {
        for id in [1, 2, 42, u64::MAX] {
            assert_eq!(
                parse_node_id_marker(&format_node_id_marker(id)).unwrap(),
                id
            );
        }
    }

    #[test]
    fn marker_parse_tolerates_whitespace() {
        assert_eq!(parse_node_id_marker(" 7\n").unwrap(), 7);
        assert_eq!(parse_node_id_marker("7").unwrap(), 7);
    }

    #[test]
    fn marker_parse_rejects_garbage_and_sentinel() {
        assert!(parse_node_id_marker("").is_err());
        assert!(parse_node_id_marker("abc").is_err());
        assert!(parse_node_id_marker("-1").is_err());
        assert!(parse_node_id_marker("0").is_err());
    }

    #[test]
    fn marker_read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_node_id_marker(dir.path()).unwrap(), None);
        write_node_id_marker(dir.path(), 3).unwrap();
        assert_eq!(read_node_id_marker(dir.path()).unwrap(), Some(3));
        // Overwrite is idempotent for restarts under the same id.
        write_node_id_marker(dir.path(), 3).unwrap();
        assert_eq!(read_node_id_marker(dir.path()).unwrap(), Some(3));
    }

    #[test]
    fn resume_id_prefers_marker_and_rejects_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        write_node_id_marker(dir.path(), 2).unwrap();

        // Marker overrides an unset (0) config id and a matching one.
        assert_eq!(resolve_resume_node_id(dir.path(), None, 0).unwrap(), 2);
        assert_eq!(resolve_resume_node_id(dir.path(), None, 2).unwrap(), 2);
        assert_eq!(resolve_resume_node_id(dir.path(), Some(2), 0).unwrap(), 2);

        // Conflicting explicit or config identity is a hard error.
        assert!(resolve_resume_node_id(dir.path(), Some(1), 0).is_err());
        assert!(resolve_resume_node_id(dir.path(), None, 1).is_err());
    }

    #[test]
    fn resume_id_without_marker_records_config_identity() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(resolve_resume_node_id(dir.path(), None, 5).unwrap(), 5);
        // The trusted identity is now persisted for future restarts.
        assert_eq!(read_node_id_marker(dir.path()).unwrap(), Some(5));
        // With neither marker nor configured id, resuming is refused.
        let empty = tempfile::tempdir().unwrap();
        assert!(resolve_resume_node_id(empty.path(), None, 0).is_err());
    }
}
