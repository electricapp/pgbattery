//! Raft wire/store compatibility fixtures (`UPGRADE_CHECKLIST` step 1).
//!
//! Committed golden bytes under `tests/fixtures/raft_compat/` are the CI gate
//! that patch/minor releases do not change frame format or store schema:
//! [`RAFT_RPC_FRAME_VERSION`] / [`STORE_SCHEMA_VERSION`] must match the
//! fixture manifest, HEAD must decode/open every `supported/` artifact, and
//! HEAD must refuse every `unsupported_newer/` artifact.
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

use super::network::{
    RAFT_RPC_FRAME_VERSION, RpcType, decode_rpc_frame_for_fuzz, encode_rpc_frame_for_fuzz,
};
use super::storage::{RedbLogStorage, STORE_SCHEMA_VERSION};

const META_TABLE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("raft_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";

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

    // Supported: current emit frame (empty-body Vote) and a blank schema-0 store.
    let mut frame = BytesMut::new();
    encode_rpc_frame_for_fuzz(&mut frame, RpcType::Vote as u8, 0xdead_beef, b"compat")
        .expect("encode current frame");
    fs::write(root.join("supported/frame_v0_vote.bin"), &frame).unwrap();

    let store_path = root.join("supported/store_schema0.raft.db");
    drop(fs::remove_file(&store_path));
    {
        let storage = RedbLogStorage::new(&store_path).expect("create schema-0 store");
        drop(storage);
    }

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

#[test]
fn supported_store_fixture_opens_at_current_schema() {
    let src = fixture_root().join("supported/store_schema0.raft.db");
    assert!(
        src.is_file(),
        "supported store fixture missing — run regenerate_raft_compat_fixtures --ignored"
    );

    // Copy: RedbLogStorage may recover/write on open; keep the golden intact.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    fs::copy(&src, &path).unwrap();

    let storage = RedbLogStorage::new(&path).expect("supported store must open");
    let read = storage.database().begin_read().unwrap();
    let meta = read.open_table(META_TABLE).unwrap();
    let value = meta
        .get(SCHEMA_VERSION_KEY)
        .unwrap()
        .expect("schema_version key after open");
    let raw: [u8; 4] = value.value().try_into().expect("schema version is u32 le");
    assert_eq!(u32::from_le_bytes(raw), STORE_SCHEMA_VERSION);
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
