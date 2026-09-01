//! HTTP client for cluster management operations.
//!
//! Centralizes all HTTP communication with the cluster management API,
//! including proper address resolution and error handling.
//!
//! # Transport security
//!
//! All inter-node calls in this client are **plaintext HTTP**, and the bearer
//! token in `x-pgbattery-token` is forwarded in the clear. This is acceptable
//! **only** when pgbattery's management plane runs on a trusted, isolated
//! network (the docker-compose `raft_net` bridge in this repository, a
//! private VPC, or similar). On any network where an attacker can observe
//! TCP traffic to the management ports, the token can be stolen and used to
//! drive arbitrary membership / backup / leadership-transfer operations.
//! Switch to mTLS (Raft transport already uses it — see `src/governor/tls.rs`)
//! before exposing the management API across an untrusted segment.

use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

use crate::cluster::MemberRole;
use crate::config::{Config, PeerConfig, RedactedSecret};

const MANAGEMENT_API_TOKEN_HEADER: &str = "x-pgbattery-token";

/// Client for cluster management API operations.
///
/// Handles address resolution, HTTP requests, and provides a clean
/// interface for CLI commands.
#[derive(Debug, Clone)]
pub struct ClusterClient {
    client: Client,
    /// Our own management address (for local queries)
    local_mgmt_addr: Option<SocketAddr>,
    /// Known peer management addresses
    peer_mgmt_addrs: Vec<SocketAddr>,
    /// Optional token for protected mutating management API routes.
    management_api_token: Option<RedactedSecret>,
}

/// Response from leader discovery endpoint.
#[derive(Debug, Deserialize, Serialize)]
pub struct LeaderInfo {
    pub leader_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub leader_pg_addr: Option<String>,
    pub leader_mgmt_addr: Option<String>,
}

/// Response from membership operations.
#[derive(Debug, Deserialize, Serialize)]
pub struct MembershipResponse {
    pub success: bool,
    pub message: String,
    #[serde(default)]
    pub members: Vec<MemberInfo>,
}

/// Information about a cluster member.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MemberInfo {
    pub node_id: u64,
    pub addr: String,
    pub role: MemberRole,
}

/// Response from node discovery endpoint.
#[derive(Debug, Deserialize, Serialize)]
pub struct NodesResponse {
    pub nodes: Vec<NodeInfo>,
}

/// Information about a node (from discovery API).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeInfo {
    pub node_id: u64,
    pub mgmt_addr: String,
    pub raft_addr: String,
    pub pg_addr: String,
    pub metrics_addr: String,
    pub is_leader: bool,
}

impl ClusterClient {
    /// Create a new cluster client with the given timeout.
    ///
    /// # Errors
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn new(timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()?;

        Ok(Self {
            client,
            local_mgmt_addr: None,
            peer_mgmt_addrs: Vec::new(),
            management_api_token: None,
        })
    }

    /// Create a cluster client configured from the local config file.
    ///
    /// # Errors
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn from_config(config: &Config, timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()?;

        let local_mgmt_addr = Some(config.get_mgmt_addr());
        let peer_mgmt_addrs = config.peers.iter().map(PeerConfig::get_mgmt_addr).collect();
        let env_token = std::env::var("PGBATTERY_MANAGEMENT_API_TOKEN")
            .ok()
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty());
        let management_api_token = env_token
            .or_else(|| {
                config
                    .management_api_token
                    .as_ref()
                    .map(|s| s.as_str().trim().to_string())
                    .filter(|token| !token.is_empty())
            })
            .map(RedactedSecret::new);

        Ok(Self {
            client,
            local_mgmt_addr,
            peer_mgmt_addrs,
            management_api_token,
        })
    }

    /// Set explicit management address to use.
    #[must_use]
    pub const fn with_addr(mut self, addr: SocketAddr) -> Self {
        self.local_mgmt_addr = Some(addr);
        self
    }

    /// Override the management API token with one resolved by the caller
    /// (e.g. from `--token-file`, which outranks the env var and config).
    /// `None` keeps any env/config-derived token already set.
    #[must_use]
    pub fn with_token(mut self, token: Option<String>) -> Self {
        if let Some(token) = token {
            self.management_api_token = Some(RedactedSecret::new(token));
        }
        self
    }

    /// Get the underlying HTTP client for custom requests.
    #[must_use]
    pub const fn http_client(&self) -> &Client {
        &self.client
    }

    fn add_management_token(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = self.management_api_token.as_ref() {
            request.header(MANAGEMENT_API_TOKEN_HEADER, token.as_str())
        } else {
            request
        }
    }
}

/// Read a non-success response body for inclusion in an error message.
/// Replaces the previous `unwrap_or_default()` pattern that silently dropped
/// the decode error and rendered the body as an empty string — making the
/// resulting `bail!` indistinguishable from "server replied 500 with no body".
async fn read_error_body(resp: reqwest::Response) -> String {
    match resp.text().await {
        Ok(body) => body,
        Err(e) => format!("(response body unreadable: {e})"),
    }
}

/// Compose the operator-facing message for a non-success management API
/// response. On 401/403 it appends an authentication hint that states whether
/// a token was sent at all — the one diagnostic fact only the client side
/// knows — and where a token can come from.
pub(crate) fn management_api_failure(
    what: &str,
    status: reqwest::StatusCode,
    body: &str,
    token_sent: bool,
) -> String {
    let mut msg = format!("{what} failed ({status}): {body}");
    if matches!(
        status,
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
    ) {
        if token_sent {
            msg.push_str(
                "\nHint: the management API token was sent but rejected. Ensure it matches \
                 the cluster's management_api_token (sources, most preferred first: \
                 --token-file, PGBATTERY_MANAGEMENT_API_TOKEN, config file).",
            );
        } else {
            msg.push_str(
                "\nHint: no management API token was sent. This operation requires one: \
                 pass --token-file <path>, set PGBATTERY_MANAGEMENT_API_TOKEN, or set \
                 management_api_token in the config file.",
            );
        }
    }
    msg
}

impl ClusterClient {
    /// Discover the current cluster leader.
    ///
    /// Tries local node first, then peers, returning the leader's
    /// management address when found.
    ///
    /// # Errors
    /// Returns an error if no reachable node reports a leader management
    /// address; the message includes the per-address failure reasons so an
    /// unreachable node is distinguishable from a leaderless cluster.
    pub async fn discover_leader(&self) -> Result<String> {
        let mut addrs: Vec<SocketAddr> = Vec::new();

        // Try local node first
        if let Some(local) = self.local_mgmt_addr {
            addrs.push(local);
        }

        // Then peers
        addrs.extend(&self.peer_mgmt_addrs);

        if addrs.is_empty() {
            anyhow::bail!(
                "Could not discover leader: no management addresses configured. \
                 Use --leader to specify manually."
            );
        }

        let mut failures: Vec<String> = Vec::new();
        for addr in addrs {
            let url = format!("http://{addr}/api/v1/cluster/leader");
            match self.client.get(&url).send().await {
                Err(e) => failures.push(format!("{addr}: {e}")),
                Ok(resp) if !resp.status().is_success() => {
                    let status = resp.status();
                    let body = read_error_body(resp).await;
                    failures.push(format!("{addr}: leader request failed ({status}): {body}"));
                }
                Ok(resp) => match resp.json::<LeaderInfo>().await {
                    Err(e) => failures.push(format!("{addr}: invalid leader response: {e}")),
                    Ok(info) => match info.leader_mgmt_addr {
                        Some(leader_mgmt) => return Ok(leader_mgmt),
                        None => failures.push(format!("{addr}: reports no known leader")),
                    },
                },
            }
        }

        anyhow::bail!(
            "Could not discover leader ({}). Use --leader to specify manually.",
            failures.join("; ")
        )
    }

    /// Get leader info from a specific node.
    ///
    /// # Errors
    /// Returns an error if the node is unreachable or returns a non-success status.
    pub async fn get_leader_info(&self, addr: &str) -> Result<LeaderInfo> {
        let url = format!("http://{addr}/api/v1/cluster/leader");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Leader request",
                status,
                &body,
                false
            ));
        }

        Ok(resp.json().await?)
    }

    /// Get cluster members from a specific node.
    ///
    /// # Errors
    /// Returns an error if the node is unreachable or returns a non-success status.
    pub async fn get_members(&self, addr: &str) -> Result<MembershipResponse> {
        let url = format!("http://{addr}/api/v1/cluster/members");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Members request",
                status,
                &body,
                false
            ));
        }

        Ok(resp.json().await?)
    }

    /// Promote a learner to voter.
    ///
    /// # Errors
    /// Returns an error if the leader is unreachable or the promote request fails.
    pub async fn promote_node(
        &self,
        leader_addr: &str,
        node_id: u64,
    ) -> Result<MembershipResponse> {
        let url = format!("http://{leader_addr}/api/v1/cluster/promote/{node_id}");
        let resp = self
            .add_management_token(self.client.post(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact leader {leader_addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Promote request",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }

        Ok(resp.json().await?)
    }

    /// Remove a node from the cluster.
    ///
    /// # Errors
    /// Returns an error if the leader is unreachable or the remove request fails.
    pub async fn remove_node(&self, leader_addr: &str, node_id: u64) -> Result<MembershipResponse> {
        let url = format!("http://{leader_addr}/api/v1/cluster/remove/{node_id}");
        let resp = self
            .add_management_token(self.client.post(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact leader {leader_addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Remove request",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }

        Ok(resp.json().await?)
    }

    /// Transfer leadership to a specific node.
    ///
    /// # Errors
    /// Returns an error if the node is unreachable, the request fails, or the
    /// transfer does not complete successfully.
    pub async fn transfer_leadership(&self, mgmt_addr: &str, target_node: u64) -> Result<()> {
        #[derive(Debug, Deserialize)]
        struct TransferResponse {
            success: bool,
            message: String,
        }

        let url = format!("http://{mgmt_addr}/api/v1/cluster/transfer-leadership/{target_node}");
        let resp = self
            .add_management_token(self.client.post(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {mgmt_addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Transfer request",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }

        let transfer: TransferResponse = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse transfer response: {e}"))?;
        if !transfer.success {
            anyhow::bail!("Leadership transfer did not complete: {}", transfer.message);
        }

        Ok(())
    }

    /// Get all nodes from a specific management address.
    ///
    /// # Errors
    /// Returns an error if the node is unreachable or returns a non-success status.
    pub async fn get_nodes(&self, addr: &str) -> Result<NodesResponse> {
        let url = format!("http://{addr}/api/v1/cluster/nodes");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Nodes request",
                status,
                &body,
                false
            ));
        }

        Ok(resp.json().await?)
    }

    /// Fetch one node's binary / frame / store versions (`GET /api/v1/version`).
    ///
    /// Probe each member's own management address — versions are not mirrored
    /// through Raft, so a single `/nodes` call cannot answer uniformity.
    ///
    /// # Errors
    /// Returns an error if the node is unreachable or returns a non-success status.
    pub async fn get_version(
        &self,
        addr: &str,
    ) -> Result<crate::observability::management_api::VersionResponse> {
        let url = format!("http://{addr}/api/v1/version");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Version request",
                status,
                &body,
                false
            ));
        }

        Ok(resp.json().await?)
    }

    /// List backups on a node (`GET /api/v1/backup/list`, token required).
    ///
    /// # Errors
    /// Returns an error if the node is unreachable, unauthorized, or non-success.
    pub async fn list_backups(
        &self,
        addr: &str,
    ) -> Result<crate::observability::management_api::BackupListResponse> {
        let url = format!("http://{addr}/api/v1/backup/list");
        let resp = self
            .add_management_token(self.client.get(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Backup list request",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }

        Ok(resp.json().await?)
    }

    /// Dual-bin-dir inventory (`GET /api/v1/postgres/bins`).
    ///
    /// # Errors
    /// Unreachable node or non-success status.
    pub async fn get_postgres_bins(
        &self,
        addr: &str,
    ) -> Result<crate::observability::management_api::PostgresBinsResponse> {
        let url = format!("http://{addr}/api/v1/postgres/bins");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "PostgreSQL bins request",
                status,
                &body,
                false
            ));
        }

        Ok(resp.json().await?)
    }

    /// Primary-side Track B preflight (`POST /api/v1/admin/postgres-upgrade/preflight`).
    ///
    /// # Errors
    /// Unreachable, unauthorized, or preflight refusal.
    pub async fn postgres_upgrade_preflight(
        &self,
        addr: &str,
        body: &crate::observability::management_api::PostgresUpgradePreflightRequest,
    ) -> Result<crate::observability::management_api::PostgresUpgradePreflightResponse> {
        let url = format!("http://{addr}/api/v1/admin/postgres-upgrade/preflight");
        let resp = self
            .add_management_token(self.client.post(&url).json(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            // Prefer structured message when the body is our JSON response.
            if let Ok(parsed) = serde_json::from_str::<
                crate::observability::management_api::PostgresUpgradePreflightResponse,
            >(&err_body)
                && let Some(msg) = parsed.message
            {
                anyhow::bail!("PostgreSQL upgrade preflight at {addr}: {msg}");
            }
            anyhow::bail!(management_api_failure(
                "PostgreSQL upgrade preflight",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }

        Ok(resp.json().await?)
    }

    /// Track C catalog preflight (`POST .../postgres-upgrade/blue-green/preflight`).
    ///
    /// # Errors
    /// Unreachable, unauthorized, or catalog refusal.
    pub async fn blue_green_preflight(
        &self,
        addr: &str,
        body: &crate::observability::management_api::BlueGreenPreflightRequest,
    ) -> Result<crate::observability::management_api::BlueGreenPreflightReport> {
        let url = format!("http://{addr}/api/v1/admin/postgres-upgrade/blue-green/preflight");
        let resp = self
            .add_management_token(self.client.post(&url).json(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            if let Ok(parsed) = serde_json::from_str::<
                crate::observability::management_api::BlueGreenPreflightHttpResponse,
            >(&err_body)
                && let Some(msg) = parsed.message
            {
                anyhow::bail!("blue/green preflight at {addr}: {msg}");
            }
            anyhow::bail!(management_api_failure(
                "blue/green preflight",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }

        let parsed: crate::observability::management_api::BlueGreenPreflightHttpResponse =
            resp.json().await?;
        parsed.report.ok_or_else(|| {
            anyhow::anyhow!("blue/green preflight at {addr}: success without report")
        })
    }

    /// Track C cutover status (`GET /api/v1/cutover/status`).
    ///
    /// # Errors
    /// Unreachable node.
    pub async fn cutover_status(&self, addr: &str) -> Result<serde_json::Value> {
        let url = format!("http://{addr}/api/v1/cutover/status");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "cutover status",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Commit a cutover log entry (`POST /api/v1/admin/cutover/{action}`).
    ///
    /// # Errors
    /// Unreachable, unauthorized, or protocol refusal.
    pub async fn cutover_action(
        &self,
        addr: &str,
        action: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("http://{addr}/api/v1/admin/cutover/{action}");
        let resp = self
            .add_management_token(self.client.post(&url).json(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                &format!("cutover {action}"),
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Set or clear Track B upgrade freeze (`POST .../postgres-upgrade/freeze`).
    ///
    /// # Errors
    /// Unreachable or unauthorized.
    pub async fn set_upgrade_freeze(
        &self,
        addr: &str,
        frozen: bool,
    ) -> Result<crate::observability::management_api::UpgradeFreezeResponse> {
        let url = format!("http://{addr}/api/v1/admin/postgres-upgrade/freeze");
        let body = crate::observability::management_api::UpgradeFreezeRequest { frozen };
        let resp = self
            .add_management_token(self.client.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Upgrade freeze",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Stop `PostgreSQL` on a node (`POST /api/v1/admin/postgres/stop`).
    ///
    /// # Errors
    /// Unreachable, unauthorized, or stop failure.
    pub async fn stop_postgres(&self, addr: &str) -> Result<()> {
        let url = format!("http://{addr}/api/v1/admin/postgres/stop");
        let resp = self
            .add_management_token(self.client.post(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "PostgreSQL stop",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(())
    }

    /// Primary `pg_upgrade` apply (`POST .../postgres-upgrade/apply`).
    ///
    /// # Errors
    /// Unreachable, unauthorized, or apply failure.
    pub async fn postgres_upgrade_apply(
        &self,
        addr: &str,
        body: &crate::observability::management_api::PostgresUpgradeApplyRequest,
    ) -> Result<crate::observability::management_api::PostgresUpgradeApplyResponse> {
        let url = format!("http://{addr}/api/v1/admin/postgres-upgrade/apply");
        let resp = self
            .add_management_token(self.client.post(&url).json(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            if let Ok(parsed) = serde_json::from_str::<
                crate::observability::management_api::PostgresUpgradeApplyResponse,
            >(&err_body)
                && let Some(msg) = parsed.message
            {
                anyhow::bail!("PostgreSQL upgrade apply at {addr}: {msg}");
            }
            anyhow::bail!(management_api_failure(
                "PostgreSQL upgrade apply",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Standby re-basebackup after primary major upgrade.
    ///
    /// # Errors
    /// Unreachable, unauthorized, or rejoin failure.
    pub async fn postgres_upgrade_rejoin(
        &self,
        addr: &str,
        body: &crate::observability::management_api::PostgresUpgradeRejoinRequest,
    ) -> Result<crate::observability::management_api::PostgresUpgradeRejoinResponse> {
        let url = format!("http://{addr}/api/v1/admin/postgres-upgrade/rejoin");
        let resp = self
            .add_management_token(self.client.post(&url).json(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            if let Ok(parsed) = serde_json::from_str::<
                crate::observability::management_api::PostgresUpgradeRejoinResponse,
            >(&err_body)
                && let Some(msg) = parsed.message
            {
                anyhow::bail!("PostgreSQL upgrade rejoin at {addr}: {msg}");
            }
            anyhow::bail!(management_api_failure(
                "PostgreSQL upgrade rejoin",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Active `PostgreSQL` major / paths (`GET /api/v1/postgres/status`).
    ///
    /// # Errors
    /// Unreachable or non-success.
    pub async fn get_postgres_status(
        &self,
        addr: &str,
    ) -> Result<crate::observability::management_api::PostgresStatusResponse> {
        let url = format!("http://{addr}/api/v1/postgres/status");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "PostgreSQL status",
                status,
                &body,
                false
            ));
        }
        Ok(resp.json().await?)
    }

    /// Committed cutover authority on one node (`GET /api/v1/cutover/status`).
    ///
    /// # Errors
    /// Unreachable or non-success.
    pub async fn get_cutover_status(
        &self,
        addr: &str,
    ) -> Result<crate::observability::management_api::CutoverStatusResponse> {
        let url = format!("http://{addr}/api/v1/cutover/status");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "cutover status",
                status,
                &body,
                false
            ));
        }
        Ok(resp.json().await?)
    }

    /// Subscription shape on `addr`: enabled count, table sync states, and
    /// the applied origin minimum (observability only).
    ///
    /// # Errors
    /// Unreachable, non-success, or a node with no subscriptions — draining
    /// against a cluster that is not subscribed would wait out the budget and
    /// report a failure that names the wrong cause.
    pub async fn get_subscription_progress(
        &self,
        addr: &str,
    ) -> Result<crate::observability::management_api::SubscriptionProgressResponse> {
        let url = format!("http://{addr}/api/v1/cutover/subscription-progress");
        let resp = self
            .add_management_token(self.client.get(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "subscription progress",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }
        let body: crate::observability::management_api::SubscriptionProgressResponse =
            resp.json().await?;
        if body.subscription_count == 0 {
            anyhow::bail!(
                "{addr} has no logical subscriptions: the recipient must already be \
                 streaming from the donor before a cutover can drain it. Seed it with \
                 POST /api/v1/admin/cutover/seed (donor schema, then a copying \
                 subscription), and wait for the initial copy to finish."
            );
        }
        Ok(body)
    }

    /// WAL the donor is holding for its logical slots, and how far the
    /// subscriber has confirmed on them.
    ///
    /// # Errors
    /// The donor is unreachable, answers non-success, or could not read its
    /// own slots — an error, never zero bytes, because zero always passes
    /// the WAL-risk check.
    pub async fn get_donor_retention(
        &self,
        addr: &str,
    ) -> Result<crate::observability::management_api::DonorRetentionResponse> {
        let url = format!("http://{addr}/api/v1/cutover/donor-retention");
        let resp = self
            .add_management_token(self.client.get(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "donor retention",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Schema shape of the cluster at `addr`.
    ///
    /// # Errors
    /// The node is unreachable or answers with a non-success status.
    pub async fn get_schema_digest(&self, addr: &str) -> Result<String> {
        let url = format!("http://{addr}/api/v1/cutover/schema-digest");
        let resp = self
            .add_management_token(self.client.get(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "schema digest",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }
        let body: crate::observability::management_api::SchemaDigestResponse = resp.json().await?;
        Ok(body.digest)
    }

    /// The donor's user sequences with their last issued values (U3).
    ///
    /// # Errors
    /// Unreachable, unauthorized, or a donor that could not read its own
    /// sequences.
    pub async fn get_sequence_values(
        &self,
        addr: &str,
    ) -> Result<Vec<crate::observability::management_api::SequenceValue>> {
        let url = format!("http://{addr}/api/v1/cutover/sequence-values");
        let resp = self
            .add_management_token(self.client.get(&url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "sequence values",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }
        let body: crate::observability::management_api::SequenceValuesResponse =
            resp.json().await?;
        Ok(body.sequences)
    }

    /// Advance the recipient's sequences past the donor's values (U3).
    ///
    /// # Errors
    /// Unreachable, unauthorized, or any sequence missing on the recipient —
    /// the error names it; a silent skip is exactly the vacuity refused here.
    pub async fn advance_sequences(
        &self,
        addr: &str,
        body: &crate::observability::management_api::AdvanceSequencesRequest,
    ) -> Result<crate::observability::management_api::AdvanceSequencesResponse> {
        let url = format!("http://{addr}/api/v1/admin/cutover/advance-sequences");
        let resp = self
            .add_management_token(self.client.post(&url).json(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            if let Ok(parsed) = serde_json::from_str::<
                crate::observability::management_api::AdvanceSequencesResponse,
            >(&err_body)
                && let Some(msg) = parsed.message
            {
                anyhow::bail!("advance sequences at {addr}: {msg}");
            }
            anyhow::bail!(management_api_failure(
                "advance sequences",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Drive one step of the cutover protocol on `addr`.
    ///
    /// `step` is the path segment under `/api/v1/admin/cutover/`. A 409 means
    /// the entry committed but no phase accepted it, which is never success.
    ///
    /// # Errors
    /// Unreachable, unauthorized, or a refused step.
    pub async fn cutover_step<B: Serialize + Sync>(
        &self,
        addr: &str,
        step: &str,
        body: &B,
    ) -> Result<crate::observability::management_api::CutoverActionResponse> {
        let url = format!("http://{addr}/api/v1/admin/cutover/{step}");
        let resp = self
            .add_management_token(self.client.post(&url))
            .json(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                &format!("cutover {step}"),
                status,
                &detail,
                true
            ));
        }
        Ok(resp.json().await?)
    }

    /// Confirm upgrade — delete old datadir + anchor.
    ///
    /// # Errors
    /// Unreachable, unauthorized, or confirm failure.
    pub async fn postgres_upgrade_confirm(
        &self,
        addr: &str,
        to_major: u32,
    ) -> Result<crate::observability::management_api::PostgresUpgradeConfirmResponse> {
        let url = format!("http://{addr}/api/v1/admin/postgres-upgrade/confirm");
        let body = crate::observability::management_api::PostgresUpgradeConfirmRequest { to_major };
        let resp = self
            .add_management_token(self.client.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            if let Ok(parsed) = serde_json::from_str::<
                crate::observability::management_api::PostgresUpgradeConfirmResponse,
            >(&err_body)
                && let Some(msg) = parsed.message
            {
                anyhow::bail!("PostgreSQL upgrade confirm at {addr}: {msg}");
            }
            anyhow::bail!(management_api_failure(
                "PostgreSQL upgrade confirm",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Roll primary back to the pre-upgrade datadir.
    ///
    /// # Errors
    /// Unreachable, unauthorized, or rollback failure.
    pub async fn postgres_upgrade_rollback(
        &self,
        addr: &str,
        to_major: u32,
        accept_write_loss: bool,
    ) -> Result<crate::observability::management_api::PostgresUpgradeRollbackResponse> {
        let url = format!("http://{addr}/api/v1/admin/postgres-upgrade/rollback");
        let body = crate::observability::management_api::PostgresUpgradeRollbackRequest {
            to_major,
            accept_write_loss,
        };
        let resp = self
            .add_management_token(self.client.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = read_error_body(resp).await;
            if let Ok(parsed) = serde_json::from_str::<
                crate::observability::management_api::PostgresUpgradeRollbackResponse,
            >(&err_body)
                && let Some(msg) = parsed.message
            {
                anyhow::bail!("PostgreSQL upgrade rollback at {addr}: {msg}");
            }
            anyhow::bail!(management_api_failure(
                "PostgreSQL upgrade rollback",
                status,
                &err_body,
                self.management_api_token.is_some()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Trigger a remote self-upgrade (`POST /api/v1/admin/self-upgrade`).
    ///
    /// The target node replaces its binary and shuts down; the caller must
    /// wait for it to come back on the target version.
    ///
    /// # Errors
    /// Returns when the request fails or the node reports unsuccessful upgrade.
    pub async fn self_upgrade(
        &self,
        addr: &str,
        body: &crate::observability::management_api::SelfUpgradeHttpRequest,
    ) -> Result<crate::observability::management_api::SelfUpgradeResponse> {
        let url = format!("http://{addr}/api/v1/admin/self-upgrade");
        // Long timeout: download + cosign verify of a release binary.
        let client = Client::builder().timeout(Duration::from_mins(10)).build()?;
        let resp = self
            .add_management_token(client.post(&url).json(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure(
                "Self-upgrade request",
                status,
                &body,
                self.management_api_token.is_some()
            ));
        }

        Ok(resp.json().await?)
    }

    /// Query replication lag for a node (`GET /api/v1/cluster/node/{id}/lag`).
    ///
    /// # Errors
    /// Returns when the node is unreachable or returns a non-success status.
    pub async fn get_node_lag(
        &self,
        addr: &str,
        node_id: u64,
    ) -> Result<crate::observability::management_api::LagResponse> {
        let url = format!("http://{addr}/api/v1/cluster/node/{node_id}/lag");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to contact {addr}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_error_body(resp).await;
            anyhow::bail!(management_api_failure("Lag request", status, &body, false));
        }

        Ok(resp.json().await?)
    }

    /// Get local management address if configured.
    #[must_use]
    pub const fn local_mgmt_addr(&self) -> Option<SocketAddr> {
        self.local_mgmt_addr
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_client_new() {
        let client = ClusterClient::new(10);
        assert!(client.is_ok());
    }

    #[test]
    fn test_cluster_client_with_addr() {
        let client = ClusterClient::new(10)
            .unwrap()
            .with_addr("127.0.0.1:9091".parse().unwrap());

        assert_eq!(
            client.local_mgmt_addr(),
            Some("127.0.0.1:9091".parse().unwrap())
        );
    }

    #[test]
    fn test_failure_message_carries_status_and_body() {
        let msg = management_api_failure(
            "Promote request",
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "boom",
            true,
        );
        assert!(msg.contains("Promote request failed"), "{msg}");
        assert!(msg.contains("500"), "{msg}");
        assert!(msg.contains("boom"), "{msg}");
        assert!(
            !msg.contains("Hint"),
            "no auth hint on non-auth errors: {msg}"
        );
    }

    #[test]
    fn test_unauthorized_without_token_says_none_was_sent() {
        let msg = management_api_failure(
            "Promote request",
            reqwest::StatusCode::UNAUTHORIZED,
            "unauthorized",
            false,
        );
        assert!(msg.contains("no management API token was sent"), "{msg}");
        assert!(msg.contains("--token-file"), "{msg}");
        assert!(msg.contains("PGBATTERY_MANAGEMENT_API_TOKEN"), "{msg}");
    }

    #[test]
    fn test_unauthorized_with_token_says_it_was_rejected() {
        for status in [
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::FORBIDDEN,
        ] {
            let msg = management_api_failure("Remove request", status, "denied", true);
            assert!(msg.contains("rejected"), "{msg}");
            assert!(msg.contains("management_api_token"), "{msg}");
        }
    }

    /// Serve one canned HTTP response and hand back the raw request bytes.
    async fn capture_one_request(
        response_body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0_u8; 8192];
            let n = sock.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(buf.get(..n).unwrap()).to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                response_body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
            request
        });
        (addr, handle)
    }

    /// The cutover probe routes are token-gated on the server; a client that
    /// sends no token gets 401s from every donor and recipient it must read.
    #[tokio::test]
    async fn cutover_probe_gets_carry_the_management_token() {
        let client = ClusterClient::new(5)
            .unwrap()
            .with_token(Some("sekrit".into()));

        let (addr, handle) = capture_one_request(r#"{"retained_bytes":0,"slot_count":0}"#).await;
        client.get_donor_retention(&addr).await.unwrap();
        let req = handle.await.unwrap().to_lowercase();
        assert!(req.contains("x-pgbattery-token: sekrit"), "{req}");

        let (addr, handle) = capture_one_request(
            r#"{"applied_lsn":0,"subscription_count":1,"enabled_subscription_count":1,"tables_total":1,"tables_ready":1}"#,
        )
        .await;
        client.get_subscription_progress(&addr).await.unwrap();
        let req = handle.await.unwrap().to_lowercase();
        assert!(req.contains("x-pgbattery-token: sekrit"), "{req}");

        let (addr, handle) = capture_one_request(r#"{"digest":"abc"}"#).await;
        client.get_schema_digest(&addr).await.unwrap();
        let req = handle.await.unwrap().to_lowercase();
        assert!(req.contains("x-pgbattery-token: sekrit"), "{req}");

        let (addr, handle) = capture_one_request(r#"{"sequences":[]}"#).await;
        client.get_sequence_values(&addr).await.unwrap();
        let req = handle.await.unwrap().to_lowercase();
        assert!(req.contains("x-pgbattery-token: sekrit"), "{req}");
    }
}
