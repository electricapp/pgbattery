//! Cluster membership and address management.
//!
//! Centralizes all node address handling to avoid scattered port derivation
//! logic and ensure consistent address resolution across the codebase.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::config::Config;

/// Advertised addresses for a cluster node.
///
/// All addresses a node advertises to the cluster. Using explicit addresses
/// rather than deriving from port math ensures nodes can use non-standard
/// port configurations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdvertisedAddresses {
    /// `PostgreSQL` address for replication connections
    pub pg_addr: SocketAddr,
    /// Raft RPC address for consensus
    pub raft_addr: SocketAddr,
    /// Management API address for cluster operations
    pub mgmt_addr: SocketAddr,
    /// Metrics/Prometheus endpoint address
    pub metrics_addr: SocketAddr,
}

/// Cluster membership role for a node.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MemberRole {
    Voter,
    Learner,
    #[serde(other)]
    Unknown,
}

impl MemberRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Voter => "voter",
            Self::Learner => "learner",
            Self::Unknown => "unknown",
        }
    }
}

impl AdvertisedAddresses {
    /// Create advertised addresses from config.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            pg_addr: config.get_advertise_pg_addr(),
            raft_addr: config.get_advertise_raft_addr(),
            mgmt_addr: config.get_advertise_mgmt_addr(),
            metrics_addr: config.get_advertise_metrics_addr(),
        }
    }
}

/// Join request payload for cluster membership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Unique node identifier
    pub node_id: u64,
    /// Raft RPC address
    pub raft_addr: String,
    /// `PostgreSQL` address for replication
    pub pg_addr: String,
    /// Management API address
    pub mgmt_addr: String,
    /// Metrics endpoint address
    pub metrics_addr: String,
}

impl JoinRequest {
    /// Create a join request from config.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        let addrs = AdvertisedAddresses::from_config(config);
        Self {
            node_id: config.node_id,
            raft_addr: addrs.raft_addr.to_string(),
            pg_addr: addrs.pg_addr.to_string(),
            mgmt_addr: addrs.mgmt_addr.to_string(),
            metrics_addr: addrs.metrics_addr.to_string(),
        }
    }

    /// Convert to [`AdvertisedAddresses`].
    #[must_use]
    pub fn to_advertised(&self) -> Option<AdvertisedAddresses> {
        Some(AdvertisedAddresses {
            pg_addr: self.pg_addr.parse().ok()?,
            raft_addr: self.raft_addr.parse().ok()?,
            mgmt_addr: self.mgmt_addr.parse().ok()?,
            metrics_addr: self.metrics_addr.parse().ok()?,
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn test_join_request_roundtrip() {
        let req = JoinRequest {
            node_id: 1,
            raft_addr: "10.0.0.1:5433".to_string(),
            pg_addr: "10.0.0.1:5434".to_string(),
            mgmt_addr: "10.0.0.1:9091".to_string(),
            metrics_addr: "10.0.0.1:9090".to_string(),
        };

        let addrs = req.to_advertised();
        assert!(addrs.is_some());
        if let Some(addrs) = addrs {
            assert_eq!(addrs.raft_addr.port(), 5433);
            assert_eq!(addrs.pg_addr.port(), 5434);
            assert_eq!(addrs.mgmt_addr.port(), 9091);
            assert_eq!(addrs.metrics_addr.port(), 9090);
        }
    }

    #[test]
    fn test_join_request_invalid_addr() {
        let req = JoinRequest {
            node_id: 1,
            raft_addr: "not-valid".to_string(),
            pg_addr: "10.0.0.1:5434".to_string(),
            mgmt_addr: "10.0.0.1:9091".to_string(),
            metrics_addr: "10.0.0.1:9090".to_string(),
        };

        assert!(req.to_advertised().is_none());
    }

    #[test]
    fn test_member_role_deserialization() {
        let voter = serde_json::from_str::<MemberRole>("\"voter\"");
        let learner = serde_json::from_str::<MemberRole>("\"learner\"");
        let unknown = serde_json::from_str::<MemberRole>("\"arbiter\"");

        assert!(matches!(voter, Ok(MemberRole::Voter)));
        assert!(matches!(learner, Ok(MemberRole::Learner)));
        assert!(matches!(unknown, Ok(MemberRole::Unknown)));
    }

    #[test]
    fn test_member_role_as_str() {
        assert_eq!(MemberRole::Voter.as_str(), "voter");
        assert_eq!(MemberRole::Learner.as_str(), "learner");
        assert_eq!(MemberRole::Unknown.as_str(), "unknown");
    }

    #[test]
    fn test_member_role_serialization() {
        assert_eq!(
            serde_json::to_string(&MemberRole::Voter).unwrap(),
            "\"voter\""
        );
        assert_eq!(
            serde_json::to_string(&MemberRole::Learner).unwrap(),
            "\"learner\""
        );
    }

    #[test]
    fn test_advertised_addresses_serde_roundtrip() {
        let addrs = AdvertisedAddresses {
            pg_addr: "10.0.0.1:5434".parse().unwrap(),
            raft_addr: "10.0.0.1:5433".parse().unwrap(),
            mgmt_addr: "10.0.0.1:9091".parse().unwrap(),
            metrics_addr: "10.0.0.1:9090".parse().unwrap(),
        };
        let json = serde_json::to_string(&addrs).unwrap();
        let back: AdvertisedAddresses = serde_json::from_str(&json).unwrap();
        assert_eq!(addrs, back);
    }

    #[test]
    fn test_join_request_invalid_single_field() {
        // All fields valid except pg_addr — to_advertised should return None
        let req = JoinRequest {
            node_id: 2,
            raft_addr: "10.0.0.2:5433".to_string(),
            pg_addr: "not-an-address".to_string(),
            mgmt_addr: "10.0.0.2:9091".to_string(),
            metrics_addr: "10.0.0.2:9090".to_string(),
        };
        assert!(req.to_advertised().is_none());
    }
}
