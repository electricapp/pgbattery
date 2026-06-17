# Releasing pgbattery

`pgbattery upgrade` downloads a binary, verifies a **SHA-256 checksum** (integrity)
and, when a signing key is configured, a **minisign signature** (authenticity).
This document is the publisher-side half: how to generate the key, sign
artifacts, and lay them out so the upgrade client can verify them.

> **Status:** signing is _scaffolded but not yet active_. Until a key is embedded
> (step 2), `upgrade` falls back to checksum + HTTPS and prints a warning that
> authenticity is unverified. Once the key is embedded, every upgrade requires a
> valid signature.

## Artifact layout

For a release `vX.Y.Z`, publish under the release base URL
(`https://pgbattery.io/releases` by default, overridable with `--url`):

```
<base>/latest                              # plain text: "X.Y.Z"
<base>/vX.Y.Z/pgbattery-<os>-<arch>        # the binary (e.g. pgbattery-linux-x86_64)
<base>/vX.Y.Z/pgbattery-<os>-<arch>.sha256 # hex SHA-256 of the binary
<base>/vX.Y.Z/pgbattery-<os>-<arch>.minisig# minisign signature of the binary
```

`<os>`/`<arch>` come from Rust's `std::env::consts` (`linux`/`macos`,
`x86_64`/`aarch64`) — see `platform_binary_name()` in `src/commands/upgrade.rs`.

## 1. Generate the signing keypair

Use a **password-less** key for CI (minisign prompts for a password otherwise):

```bash
# jedisct1/minisign — `brew install minisign` / `apt-get install minisign`
minisign -G -W -p pgbattery.pub -s pgbattery.key
```

- `pgbattery.pub` — public key (safe to commit/publish). Last line is the base64 key.
- `pgbattery.key` — **secret key**. Never commit. Store in CI secrets only.

## 2. Embed the public key in the client

Copy the base64 key line (the last line of `pgbattery.pub`, starting `RWQ…`) into
`RELEASE_PUBLIC_KEY` in `src/commands/upgrade.rs`:

```rust
const RELEASE_PUBLIC_KEY: Option<&str> = Some("RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3");
```

Rebuild and ship. From then on, clients require a valid `.minisig` for every upgrade.

Operators who want to enforce authenticity _before_ a key is embedded can pass
`--public-key pgbattery.pub` or set `PGBATTERY_RELEASE_PUBLIC_KEY` to the base64
key line.

## 3. Store the secret key in CI

Add repository secrets:

| Secret                | Value                            |
| --------------------- | -------------------------------- |
| `MINISIGN_SECRET_KEY` | full contents of `pgbattery.key` |

(A password-less key needs no `MINISIGN_PASSWORD`. If you used a password, add it
as a secret and feed it to `minisign` on stdin.)

## 4. Sign

Locally:

```bash
scripts/sign-release.sh dist/   # signs every pgbattery-* binary in dist/
```

In CI: `.github/workflows/release.yml` builds the matrix, then signs and uploads
on a `v*` tag. See that file for the exact steps.

## Verifying manually

```bash
minisign -V -p pgbattery.pub -m pgbattery-linux-x86_64
sha256sum -c <(echo "$(cat pgbattery-linux-x86_64.sha256)  pgbattery-linux-x86_64")
```

## Key rotation

1. Generate a new keypair.
2. Ship a client release embedding the **new** public key (signed with the **old**
   key so existing clients can still upgrade to it).
3. Once enough clients have upgraded, sign subsequent releases with the new key.

Signatures are prehashed (`minisign -H`); the client verifies with
`allow_legacy = false`.
