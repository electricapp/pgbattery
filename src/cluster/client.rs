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
#[derive(Debug)]
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
            anyhow::bail!("Leader request failed ({status}): {body}");
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
            anyhow::bail!("Members request failed ({status}): {body}");
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
            anyhow::bail!("Promote request failed ({status}): {body}");
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
            anyhow::bail!("Remove request failed ({status}): {body}");
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
            anyhow::bail!("Transfer request failed ({status}): {body}");
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
            anyhow::bail!("Nodes request failed ({status}): {body}");
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
}
