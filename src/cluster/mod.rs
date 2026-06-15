//! Cluster management module.
//!
//! Provides centralized handling of:
//! - Node address management (`AdvertisedAddresses`)
//! - Cluster membership operations
//! - HTTP client for cluster communication

pub mod client;
pub mod membership;

pub use client::ClusterClient;
pub use membership::{AdvertisedAddresses, JoinRequest, MemberRole};
