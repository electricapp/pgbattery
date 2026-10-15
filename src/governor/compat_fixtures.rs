//! Raft wire/store compatibility fixtures (`UPGRADE_CHECKLIST` step 1).
//!
//! Committed golden bytes under `tests/fixtures/raft_compat/` are the CI gate
//! that patch/minor releases do not change frame format or store schema:
//! [`RAFT_RPC_FRAME_VERSION`] / [`STORE_SCHEMA_VERSION`] must match the
//! fixture manifest, HEAD must decode/open every `supported/` artifact
//! (including migrating the populated schema-0 store), and HEAD must refuse
//! every `unsupported_newer/` artifact. The `.bin` pins commit the actual
//! postcard record encodings — the schema is the bytes, not the constant.
//!
//! Regenerate after an intentional version bump:
//! `cargo test -p pgbattery --lib governor::compat_fixtures::regenerate -- --ignored --nocapture`

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    unused_imports,
    reason = "test-only module: asserts and fixture I/O"
)]

use std::fs;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use redb::{ReadableDatabase, TableDefinition};
use sha2::{Digest, Sha256};

use super::network::{
    RAFT_RPC_FRAME_VERSION, RpcType, decode_rpc_frame_for_fuzz, encode_rpc_frame_for_fuzz,
};
use super::state_machine::{
    ClusterCommand, ClusterState, ClusterStateV0, CutoverAuthorityState, CutoverPhase, NodeId,
    NodeInfo, NodeRole,
};
use super::storage::{
    LastAppliedState, LocalStoredMembership, LogEntry, LogEntryPayload, RedbLogStorage,
    STORE_SCHEMA_VERSION, SnapshotMeta, Vote,
};

const LOGS_TABLE: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("raft_logs");
const META_TABLE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("raft_meta");
const SNAPSHOT_TABLE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("raft_snapshot");
const SCHEMA_VERSION_KEY: &str = "schema_version";

/// One node, one voter, single-entry maps throughout: postcard encodes maps in
/// iteration order, so every sample keeps collections at one element to make
/// the golden bytes deterministic.
fn sample_node(id: NodeId) -> NodeInfo {
    let addr = "127.0.0.1:5434".parse().unwrap();
    NodeInfo {
        id,
        pg_addr: addr,
        raft_addr: addr,
        mgmt_addr: addr,
        metrics_addr: addr,
        role: NodeRole::Follower,
        last_seen: 1_700_000_000,
    }
}

fn sample_state_v0() -> ClusterStateV0 {
    ClusterStateV0 {
        nodes: std::collections::HashMap::from([(1, sample_node(1))]),
        voter_ids: std::collections::HashSet::from([1]),
        learner_ids: std::collections::HashSet::new(),
        node_lsns: std::collections::HashMap::from([(1, (4_096, 1_700_000_000))]),
        max_cluster_lsn: 4_096,
        sync_replication_active: Some(true),
    }
}

fn sample_state_current() -> ClusterState {
    ClusterState {
        cutover: CutoverAuthorityState {
            epoch: 2,
            authority_cluster_id: Some("green".to_string()),
            phase: CutoverPhase::Transferred,
            sealed_final_lsn: Some(8_192),
            target_cluster_id: Some("green".to_string()),
            target_pg_addr: Some("172.28.1.11:5434".to_string()),
            transfer_forced: false,
            fenced_app_roles: vec!["app".to_string()],
        },
        upgrade_frozen: true,
        applied_index: Some(42),
        feature_level: 1,
        ..ClusterState::from(sample_state_v0())
    }
}

/// One entry per `ClusterCommand` variant, plus `Blank` and `Membership`
/// payloads — the full postcard surface a `raft.db` log can contain.
fn sample_log_entries() -> Vec<LogEntry> {
    let commands = vec![
        LogEntryPayload::Blank,
        LogEntryPayload::Membership(sample_membership()),
        LogEntryPayload::Normal(ClusterCommand::AddNode(sample_node(2))),
        LogEntryPayload::Normal(ClusterCommand::RemoveNode(2)),
        LogEntryPayload::Normal(ClusterCommand::UpdateNode {
            id: 1,
            role: NodeRole::Follower,
        }),
        LogEntryPayload::Normal(ClusterCommand::UpdateLsn {
            node_id: 1,
            lsn_bytes: 4_096,
            timestamp: 1_700_000_000,
        }),
        LogEntryPayload::Normal(ClusterCommand::SetSyncMode { active: true }),
        LogEntryPayload::Normal(ClusterCommand::CutoverArmed {
            target_cluster_id: "green".to_string(),
            local_cluster_id: "blue".to_string(),
        }),
        LogEntryPayload::Normal(ClusterCommand::CutoverSeal { final_lsn: 8_192 }),
        LogEntryPayload::Normal(ClusterCommand::SetUpgradeFreeze { frozen: true }),
        LogEntryPayload::Normal(ClusterCommand::CutoverDrainObserved),
        LogEntryPayload::Normal(ClusterCommand::CutoverTransfer {
            target_cluster_id: "green".to_string(),
            target_pg_addr: "172.28.1.11:5434".to_string(),
            forced: false,
        }),
        LogEntryPayload::Normal(ClusterCommand::CutoverAbortUnseal {
            local_cluster_id: "blue".to_string(),
        }),
        LogEntryPayload::Normal(ClusterCommand::CutoverAuthorityReceived {
            epoch: 2,
            from_cluster_id: "blue".to_string(),
            local_cluster_id: "green".to_string(),
        }),
        LogEntryPayload::Normal(ClusterCommand::SetFeatureLevel { level: 1 }),
        LogEntryPayload::Normal(ClusterCommand::CutoverSealing {
            app_roles: vec!["app".to_string()],
        }),
    ];
    commands
        .into_iter()
        .enumerate()
        .map(|(i, payload)| LogEntry {
            index: i as u64 + 1,
            term: 1,
            leader_node_id: 1,
            payload,
        })
        .collect()
}

fn sample_membership() -> LocalStoredMembership {
    LocalStoredMembership {
        log_id_index: Some(1),
        log_id_term: Some(1),
        log_id_leader_node_id: 1,
        configs: vec![vec![1]],
        nodes: vec![(1, "127.0.0.1:7000".to_string())],
    }
}

fn sample_snapshot_meta() -> SnapshotMeta {
    SnapshotMeta {
        last_applied: LastAppliedState {
            last_applied_term: Some(1),
            last_applied_index: Some(1),
            last_applied_leader_node_id: 1,
        },
        membership: sample_membership(),
    }
}

/// Write a `raft.db` exactly as a schema-0 binary left it: no schema key,
/// v0-layout snapshot, one old-era log entry, a vote. This is the store the
/// 0→1 migration must open.
fn write_schema0_store(path: &Path) {
    drop(fs::remove_file(path));
    let db = redb::Database::create(path).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut logs = txn.open_table(LOGS_TABLE).unwrap();
        let entry = LogEntry {
            index: 1,
            term: 1,
            leader_node_id: 1,
            payload: LogEntryPayload::Normal(ClusterCommand::UpdateLsn {
                node_id: 1,
                lsn_bytes: 4_096,
                timestamp: 1_700_000_000,
            }),
        };
        let bytes = postcard::to_allocvec(&entry).unwrap();
        logs.insert(1, bytes.as_slice()).unwrap();

        let mut meta = txn.open_table(META_TABLE).unwrap();
        let vote = Vote {
            term: 3,
            voted_for: Some(1),
            committed: true,
        };
        let vote_bytes = postcard::to_allocvec(&vote).unwrap();
        meta.insert("vote", vote_bytes.as_slice()).unwrap();

        let mut snap = txn.open_table(SNAPSHOT_TABLE).unwrap();
        let data = postcard::to_allocvec(&sample_state_v0()).unwrap();
        let digest = Sha256::digest(&data);
        snap.insert("data", data.as_slice()).unwrap();
        snap.insert("data_sha256", digest.as_slice()).unwrap();
        let meta_bytes = postcard::to_allocvec(&sample_snapshot_meta()).unwrap();
        snap.insert("meta", meta_bytes.as_slice()).unwrap();
    }
    txn.commit().unwrap();
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raft_compat")
}

fn read_u32_text(path: &Path) -> u32 {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    text.trim()
        .parse::<u32>()
        .unwrap_or_else(|e| panic!("{}: not a u32 ({e}): {text:?}", path.display()))
}

fn parse_manifest(text: &str) -> (u8, u32, &str) {
    let mut frame = None;
    let mut schema = None;
    let mut pkg = None;
    for raw in text.lines() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            panic!("manifest: bad line {raw:?}");
        };
        match k.trim() {
            "frame_version" => frame = Some(v.trim().parse::<u8>().expect("frame_version")),
            "store_schema_version" => {
                schema = Some(v.trim().parse::<u32>().expect("store_schema_version"));
            }
            "package_version" => pkg = Some(v.trim()),
            other => panic!("manifest: unknown key {other:?}"),
        }
    }
    (
        frame.expect("manifest missing frame_version"),
        schema.expect("manifest missing store_schema_version"),
        pkg.expect("manifest missing package_version"),
    )
}

/// Write golden fixtures used by the always-on compat tests.
///
/// Not run in CI — operators (and agents) run it when bumping frame/schema.
#[test]
#[ignore = "run explicitly to regenerate tests/fixtures/raft_compat"]
fn regenerate_raft_compat_fixtures() {
    let root = fixture_root();
    fs::create_dir_all(root.join("supported")).unwrap();
    fs::create_dir_all(root.join("unsupported_newer")).unwrap();

    // Supported: current emit frame (opaque body over the Vote type byte).
    let mut frame = BytesMut::new();
    encode_rpc_frame_for_fuzz(&mut frame, RpcType::Vote as u8, 0xdead_beef, b"compat")
        .expect("encode current frame");
    fs::write(root.join("supported/frame_v0_vote.bin"), &frame).unwrap();

    // Supported: a populated genuine schema-0 store (no schema key, v0
    // snapshot). HEAD must open it, run the 0→1 migration, and decode
    // every record.
    write_schema0_store(&root.join("supported/store_schema0.raft.db"));

    // Supported: a populated current-schema store written through the real
    // storage API. HEAD must open it and decode every record.
    let store1_path = root.join("supported/store_schema1.raft.db");
    drop(fs::remove_file(&store1_path));
    {
        let storage = RedbLogStorage::new(&store1_path).expect("create current-schema store");
        storage
            .save_vote(&Vote {
                term: 3,
                voted_for: Some(1),
                committed: true,
            })
            .expect("vote");
        storage
            .append_entries(&sample_log_entries())
            .expect("entries");
        let data = postcard::to_allocvec(&sample_state_current()).expect("encode state");
        storage
            .save_snapshot(&sample_snapshot_meta(), &data)
            .expect("snapshot");
        drop(storage);
    }

    // Golden postcard pins: the actual record encodings, not just header
    // bytes. Any field addition/removal/reorder in `ClusterState`,
    // `ClusterCommand`, or `LogEntry` turns these red — which is a store
    // schema change and needs a bump plus a migration.
    fs::write(
        root.join("supported/snapshot_state_schema1.bin"),
        postcard::to_allocvec(&sample_state_current()).unwrap(),
    )
    .unwrap();
    fs::write(
        root.join("supported/log_entries_schema1.bin"),
        postcard::to_allocvec(&sample_log_entries()).unwrap(),
    )
    .unwrap();

    // Unsupported newer: frame version N+1 (same shape, wrong version byte).
    let mut newer_frame = BytesMut::new();
    encode_rpc_frame_for_fuzz(
        &mut newer_frame,
        RpcType::Vote as u8,
        0xc0ff_ee00,
        b"too-new",
    )
    .expect("encode base");
    newer_frame[4] = RAFT_RPC_FRAME_VERSION.saturating_add(1);
    fs::write(
        root.join("unsupported_newer/frame_v_next.bin"),
        &newer_frame,
    )
    .unwrap();

    // Unsupported newer: valid redb with schema_version = STORE+1.
    let newer_store = root.join("unsupported_newer/store_schema_next.raft.db");
    drop(fs::remove_file(&newer_store));
    {
        let storage = RedbLogStorage::new(&newer_store).expect("create store for schema bump");
        let write = storage.database().begin_write().expect("begin write");
        {
            let mut meta = write.open_table(META_TABLE).expect("meta");
            let bytes = (STORE_SCHEMA_VERSION + 1).to_le_bytes();
            meta.insert(SCHEMA_VERSION_KEY, bytes.as_slice())
                .expect("insert newer schema");
        }
        write.commit().expect("commit");
        drop(storage);
    }

    let manifest = format!(
        "# Raft frame + redb store compatibility pin (UPGRADE_CHECKLIST step 1).\n\
         # Patch/minor releases must not change these values. Bumping either\n\
         # requires updating supported/ fixtures and keeping accept window N-1..=N.\n\
         frame_version={RAFT_RPC_FRAME_VERSION}\n\
         store_schema_version={STORE_SCHEMA_VERSION}\n\
         package_version={}\n",
        env!("CARGO_PKG_VERSION")
    );
    fs::write(root.join("manifest"), manifest).unwrap();

    // Pin files recording the unsupported versions (for human inspection).
    fs::write(
        root.join("unsupported_newer/frame_version"),
        format!("{}\n", u32::from(RAFT_RPC_FRAME_VERSION.saturating_add(1))),
    )
    .unwrap();
    fs::write(
        root.join("unsupported_newer/store_schema_version"),
        format!("{}\n", STORE_SCHEMA_VERSION + 1),
    )
    .unwrap();

    eprintln!("wrote raft compat fixtures under {}", root.display());
}

#[test]
fn manifest_pins_match_binary_constants() {
    let text = fs::read_to_string(fixture_root().join("manifest")).expect(
        "tests/fixtures/raft_compat/manifest missing — run \
         regenerate_raft_compat_fixtures --ignored",
    );
    let (frame, schema, _pkg) = parse_manifest(&text);
    assert_eq!(
        frame, RAFT_RPC_FRAME_VERSION,
        "frame_version in manifest drifted from RAFT_RPC_FRAME_VERSION — \
         patch/minor must not change the wire format; if this is an intentional \
         major bump, regenerate fixtures and keep accept window at N-1..=N"
    );
    assert_eq!(
        schema, STORE_SCHEMA_VERSION,
        "store_schema_version in manifest drifted from STORE_SCHEMA_VERSION — \
         patch/minor must not change the store schema; if this is an intentional \
         major bump, regenerate fixtures and add a forward migrate"
    );
}

#[test]
fn supported_frame_fixture_decodes_and_round_trips() {
    let bytes = fs::read(fixture_root().join("supported/frame_v0_vote.bin"))
        .expect("supported frame fixture missing — run regenerate_raft_compat_fixtures --ignored");
    assert_eq!(
        bytes.get(4).copied(),
        Some(RAFT_RPC_FRAME_VERSION),
        "supported frame fixture must carry the current frame version"
    );

    let mut buf = BytesMut::from(bytes.as_slice());
    let (type_byte, corr_id, body) = decode_rpc_frame_for_fuzz(&mut buf)
        .expect("supported frame must decode")
        .expect("supported frame must be complete");
    assert!(buf.is_empty());
    assert_eq!(type_byte, RpcType::Vote as u8);
    assert_eq!(corr_id, 0xdead_beef);
    assert_eq!(body, b"compat");

    let mut re = BytesMut::new();
    encode_rpc_frame_for_fuzz(&mut re, type_byte, corr_id, &body).unwrap();
    assert_eq!(
        &re[..],
        bytes.as_slice(),
        "HEAD encoder must still emit the committed supported frame bytes"
    );
}

/// Open a golden store on a scratch copy (open may migrate/write) and return
/// the live handle.
fn open_golden_copy(name: &str) -> (tempfile::TempDir, RedbLogStorage) {
    let src = fixture_root().join(name);
    assert!(
        src.is_file(),
        "{name} missing — run regenerate_raft_compat_fixtures --ignored"
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    fs::copy(&src, &path).unwrap();
    let storage = RedbLogStorage::new(&path).expect("supported store must open");
    (dir, storage)
}

fn assert_stamped_current(storage: &RedbLogStorage) {
    let read = storage.database().begin_read().unwrap();
    let meta = read.open_table(META_TABLE).unwrap();
    let value = meta
        .get(SCHEMA_VERSION_KEY)
        .unwrap()
        .expect("schema_version key after open");
    let raw: [u8; 4] = value.value().try_into().expect("schema version is u32 le");
    assert_eq!(u32::from_le_bytes(raw), STORE_SCHEMA_VERSION);
}

/// The schema-0 golden is a populated pre-versioning store: opening it must
/// run the 0→1 snapshot migration and leave every record decodable.
#[test]
fn supported_schema0_store_migrates_and_decodes() {
    let (_dir, storage) = open_golden_copy("supported/store_schema0.raft.db");
    assert_stamped_current(&storage);

    let data = storage
        .load_snapshot_verified()
        .expect("digest must verify after migration")
        .expect("snapshot must survive migration");
    let state: ClusterState =
        postcard::from_bytes(&data).expect("migrated snapshot must decode as current ClusterState");
    assert_eq!(state.max_cluster_lsn, 4_096);
    assert_eq!(state.sync_replication_active, Some(true));
    assert!(state.nodes.contains_key(&1));
    assert_eq!(state.cutover, CutoverAuthorityState::default());
    assert!(!state.upgrade_frozen);

    let vote = storage.load_vote().expect("vote must decode");
    assert_eq!(vote.term, 3);
    let entry = storage
        .get_entry(1)
        .expect("log read")
        .expect("log entry must survive");
    assert!(matches!(
        entry.payload,
        LogEntryPayload::Normal(ClusterCommand::UpdateLsn { node_id: 1, .. })
    ));
}

/// The current-schema golden must open with no migration and every record —
/// vote, all command variants, snapshot — must decode.
#[test]
fn supported_schema1_store_opens_and_decodes() {
    let (_dir, storage) = open_golden_copy("supported/store_schema1.raft.db");
    assert_stamped_current(&storage);

    let vote = storage.load_vote().expect("vote must decode");
    assert_eq!(vote.term, 3);

    let expected = sample_log_entries();
    let entries = storage
        .get_entries(1, expected.len() as u64 + 1)
        .expect("log entries must decode");
    assert_eq!(entries.len(), expected.len());

    let data = storage
        .load_snapshot_verified()
        .expect("snapshot digest must verify")
        .expect("snapshot present");
    let state: ClusterState =
        postcard::from_bytes(&data).expect("snapshot must decode as current ClusterState");
    assert_eq!(state.cutover.epoch, 2);
    assert_eq!(state.cutover.phase, CutoverPhase::Transferred);
    assert!(state.upgrade_frozen);
}

/// The committed snapshot bytes ARE the store schema: any field change in
/// `ClusterState` (or anything it contains) changes this encoding and must
/// come with a `STORE_SCHEMA_VERSION` bump plus a migration.
#[test]
fn snapshot_state_golden_pins_current_encoding() {
    let bytes = fs::read(fixture_root().join("supported/snapshot_state_schema1.bin"))
        .expect("snapshot golden missing — run regenerate_raft_compat_fixtures --ignored");
    let decoded: ClusterState =
        postcard::from_bytes(&bytes).expect("golden must decode as current ClusterState");
    assert_eq!(decoded.cutover.epoch, 2);
    assert_eq!(
        postcard::to_allocvec(&sample_state_current()).unwrap(),
        bytes,
        "ClusterState postcard encoding drifted from the committed golden — \
         this is a store schema change: bump STORE_SCHEMA_VERSION, add a \
         migration, then regenerate fixtures"
    );
}

/// Same pin for the log surface: every `ClusterCommand` variant and every
/// `LogEntryPayload` shape.
#[test]
fn log_entries_golden_pins_current_encoding() {
    let bytes = fs::read(fixture_root().join("supported/log_entries_schema1.bin"))
        .expect("log entries golden missing — run regenerate_raft_compat_fixtures --ignored");
    let decoded: Vec<LogEntry> =
        postcard::from_bytes(&bytes).expect("golden must decode as current LogEntry list");
    assert_eq!(decoded.len(), sample_log_entries().len());
    assert_eq!(
        postcard::to_allocvec(&sample_log_entries()).unwrap(),
        bytes,
        "LogEntry/ClusterCommand postcard encoding drifted from the committed \
         golden — this is a store schema change: bump STORE_SCHEMA_VERSION, \
         add a migration, then regenerate fixtures"
    );
}

#[test]
fn unsupported_newer_frame_fixture_is_refused() {
    let bytes = fs::read(fixture_root().join("unsupported_newer/frame_v_next.bin"))
        .expect("newer frame fixture missing — run regenerate_raft_compat_fixtures --ignored");
    let expected = read_u32_text(&fixture_root().join("unsupported_newer/frame_version")) as u8;
    assert_eq!(bytes.get(4).copied(), Some(expected));
    assert!(
        !super::network::raft_rpc_frame_version_accepted(expected),
        "fixture version {expected} must fall outside this binary's accept window"
    );

    let mut buf = BytesMut::from(bytes.as_slice());
    let err = decode_rpc_frame_for_fuzz(&mut buf).expect_err("newer frame must be refused");
    assert!(
        err.to_string().contains("not accepted"),
        "unexpected error: {err}"
    );
    assert_eq!(&buf[..], bytes.as_slice(), "refusal must not consume bytes");
}

#[test]
fn unsupported_newer_store_fixture_is_refused() {
    let src = fixture_root().join("unsupported_newer/store_schema_next.raft.db");
    assert!(
        src.is_file(),
        "newer store fixture missing — run regenerate_raft_compat_fixtures --ignored"
    );
    let expected = read_u32_text(&fixture_root().join("unsupported_newer/store_schema_version"));
    assert!(expected > STORE_SCHEMA_VERSION);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    fs::copy(&src, &path).unwrap();

    let err = RedbLogStorage::new(&path).expect_err("newer schema must refuse to open");
    let msg = err.to_string();
    assert!(
        msg.contains("newer than this binary") && msg.contains("Downgrade is not supported"),
        "unexpected error: {msg}"
    );
}

#[test]
fn frame_accept_window_is_exactly_n_minus_one_through_n() {
    // Major bumps may advance the spoken version by one; the accept window is
    // always adjacent releases only (UPGRADE_CHECKLIST / UPGRADE.md).
    let n = RAFT_RPC_FRAME_VERSION;
    let min = n.saturating_sub(1);
    for v in 0..=255u8 {
        let mut buf = BytesMut::new();
        encode_rpc_frame_for_fuzz(&mut buf, RpcType::Vote as u8, 1, b"").unwrap();
        buf[4] = v;
        let result = decode_rpc_frame_for_fuzz(&mut buf);
        let should_accept = (min..=n).contains(&v);
        if should_accept {
            assert!(
                result.as_ref().ok().and_then(|f| f.as_ref()).is_some(),
                "version {v} should be accepted in window {min}..={n}: {result:?}"
            );
        } else {
            assert!(
                result.is_err(),
                "version {v} should be refused outside window {min}..={n}"
            );
        }
    }
}
