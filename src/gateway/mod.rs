//! Gateway - Message-aware TCP proxy for `PostgreSQL`.
//!
//! The Gateway handles all client connections and provides:
//! - `PostgreSQL` wire protocol parsing
//! - Connection state tracking (`Idle`, `InTransaction`, etc.)
//! - COPY protocol handling
//! - LISTEN/NOTIFY subscription tracking
//! - SSL/TLS termination or passthrough
//! - Seamless failover for idle connections
//!
//! # Architecture
//!
//! ```text
//!   Client ──► Gateway ──► PostgreSQL (leader)
//!              │
//!              └─► Watches leader changes via Raft
//! ```
//!
//! # Connection Migration
//!
//! When a failover occurs, connections are handled based on their state:
//!
//! - **Idle**: Transparently reconnected to new leader
//! - **`InTransaction`**: Terminated with error (cannot migrate mid-transaction)
//! - **`InCopy`**: Terminated with error
//!
//! # Fencing
//!
//! During leadership transitions, the gateway enters a "fenced" state where
//! new writes are blocked until the new leader is confirmed. This prevents
//! split-brain writes during network partitions.
//!
//! # Modules
//!
//! - [`connection`]: Connection state tracking and registry
//! - [`protocol`]: `PostgreSQL` wire protocol parsing
//! - [`proxy`]: Main proxy loop and configuration
//! - [`ssl`]: TLS/SSL handling
//! - [`handlers`]: Per-connection request handling

pub mod connection;
pub mod handlers;
pub mod protocol;
pub mod proxy;
pub mod session_replay;
pub mod ssl;

pub use proxy::{Gateway, GatewayConfig};
