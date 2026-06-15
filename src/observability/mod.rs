//! Observability - Metrics, logging, and management API.
//!
//! This module provides three pillars of observability:
//!
//! # Metrics
//!
//! Prometheus-compatible metrics exposed at `/metrics`:
//! - `pgbattery_connections_active` - Current active connections
//! - `pgbattery_raft_leader` - Whether this node is the Raft leader
//! - `pgbattery_replication_lag_bytes` - Replication lag in bytes
//! - `pgbattery_failovers_total` - Total number of failovers
//!
//! # Logging
//!
//! Structured logging via `tracing` with optional JSON output.
//! Configure with `log_json = true` in config or `RUST_LOG` env var.
//!
//! # Management API
//!
//! REST API for cluster operations (default port: metrics + 1). Discovery
//! endpoints are unauthenticated; mutations require the
//! `x-pgbattery-token` header. See the routing table in
//! [`management_api::start_management_api`] for the authoritative list:
//! - `GET  /health`
//! - `GET  /api/v1/cluster/{leader,nodes,members}` and `/node/{id}/lag`
//! - `POST /api/v1/cluster/{transfer-leadership,promote,remove}/{id}`, `/join`
//! - `POST /api/v1/backup/create`, `/api/v1/backup/restore`
//! - `GET  /debug/*` (diagnostics)
//!
//! # Modules
//!
//! - [`logging`]: Tracing subscriber setup
//! - [`metrics`]: Prometheus metrics registration
//! - [`management_api`]: Axum-based REST API

pub mod debug_events;
pub mod logging;
pub mod management_api;
pub mod metrics;
