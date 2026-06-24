//! Sigstore **cosign keyless** verification of release artifacts.
//!
//! Releases are signed in CI with `cosign sign-blob` in keyless mode: GitHub
//! Actions presents its OIDC identity to Fulcio, which issues a short-lived
//! signing certificate whose Subject Alternative Name (SAN) encodes the exact
//! workflow + tag that ran. The signature, that certificate, and a **Sigstore
//! bundle** (`<binary>.bundle`) are published next to the binary. There is **no
//! signing key to manage** — trust is anchored in the Sigstore/Fulcio root, the
//! Rekor transparency-log public key, and the expected OIDC issuer + workflow
//! identity.
//!
//! The bundle is the primary verification path because it additionally carries
//! the **Rekor transparency-log inclusion proof** (a Signed Entry Timestamp /
//! SET over the log entry). The detached `.sig`/`.pem` are kept as a fallback
//! for the same signature + chain + identity checks, minus Rekor.
//!
//! [`CosignVerifier::verify_bundle`] enforces, for a downloaded binary:
//!
//! 1. **Rekor inclusion** — `SignedArtifactBundle::new_verified` validates the
//!    bundle's Rekor SET against the trust root's Rekor public key, proving the
//!    signing event was recorded in the public transparency log and yielding the
//!    log's **integrated time**. A signature that was never logged in Rekor (or
//!    whose SET does not verify) is rejected here.
//! 2. **Signature over the blob** — the bundle's signature is valid over the
//!    binary bytes under the bundle certificate's public key
//!    (`sigstore::cosign::Client::verify_blob`).
//! 3. **Certificate chains to Fulcio** — the bundle's leaf certificate chains to
//!    a Fulcio CA from the Sigstore trust root (fetched via TUF). The chain is
//!    verified *at the Rekor integrated time* — the transparency log's own
//!    attestation of when the signature existed — which is strictly better than
//!    the certificate's `notBefore`: Fulcio leaves live only ~10 minutes, so
//!    "now" would reject every release shortly after it is cut.
//! 4. **Identity + issuer** — the certificate's OIDC-issuer extension equals the
//!    expected issuer (GitHub Actions), and its SAN identity matches the
//!    expected release-workflow regex. This is what stops an attacker who can
//!    obtain *some* valid Fulcio certificate (e.g. for their own repo) from
//!    passing verification: the identity must be *this* repo's release workflow.
//!
//! [`CosignVerifier::verify_blob`] is the detached-`.sig`/`.pem` fallback: it
//! runs checks 2–4 but anchors the chain at the certificate's `notBefore` and
//! does **not** verify Rekor inclusion (the detached artifacts carry no SET, and
//! sigstore-rs 0.14 exposes Rekor verification only via the bundle path). See
//! `docs/RELEASING.md`.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use const_oid::ObjectIdentifier;
use regex::Regex;
use x509_cert::Certificate as X509Certificate;
use x509_cert::der::{Decode, DecodePem, Encode};

use sigstore::cosign::CosignCapabilities;
use sigstore::cosign::bundle::SignedArtifactBundle;
use sigstore::cosign::client::Client as CosignClient;
use sigstore::crypto::CosignVerificationKey;
use sigstore::trust::TrustRoot;
use sigstore::trust::sigstore::SigstoreTrustRoot;

/// Expected OIDC issuer baked into the binary: GitHub Actions' token issuer.
/// A release certificate must carry exactly this issuer in its Fulcio
/// OIDC-issuer extension. Overridable via [`ENV_ISSUER`] for forks/testing.
const DEFAULT_OIDC_ISSUER: &str = "https://token.actions.githubusercontent.com";

/// Expected SAN identity (regex) baked into the binary: this repository's
/// release workflow, signing a `v*` tag. The leaf certificate's SAN URI must
/// match. Overridable via [`ENV_IDENTITY`] or the `--identity` CLI flag.
const DEFAULT_IDENTITY_REGEX: &str =
    r"^https://github\.com/electricapp/pgbattery/\.github/workflows/release\.yml@refs/tags/v.*$";

/// Override for the expected OIDC issuer (exact match).
const ENV_ISSUER: &str = "PGBATTERY_RELEASE_OIDC_ISSUER";

/// Override for the expected SAN identity (regex). Also settable per-invocation
/// with `--identity`.
const ENV_IDENTITY: &str = "PGBATTERY_RELEASE_IDENTITY_REGEX";

/// Fulcio X.509 extension OID carrying the OIDC issuer (the original,
/// non-deprecated form). `1.3.6.1.4.1.57264.1.1`.
/// See <https://github.com/sigstore/fulcio/blob/main/docs/oid-info.md>.
const SIGSTORE_OIDC_ISSUER_OID: ObjectIdentifier =
    ObjectIdentifier::new_unwrap("1.3.6.1.4.1.57264.1.1");

/// `SubjectAltName` extension OID (`2.5.29.17`).
const SUBJECT_ALT_NAME_OID: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.17");

/// Code-signing extended-key-usage OID (`1.3.6.1.5.5.7.3.3`), required of Fulcio
/// leaves. `ObjectIdentifier::as_bytes` is not `const`, so the DER value is
/// materialised at use-site in [`verify_fulcio_chain_at`].
const EKU_CODE_SIGNING_OID: ObjectIdentifier = const_oid::db::rfc5912::ID_KP_CODE_SIGNING;

/// Verifies release artifacts against a Sigstore cosign keyless trust policy.
///
/// Holds the compiled identity policy (expected issuer + SAN regex). The
/// Sigstore trust root (Fulcio CA certs) is fetched lazily on first use so that
/// constructing a verifier (and thus the whole `--check`/argument-validation
/// path) never touches the network.
#[derive(Debug)]
pub(super) struct CosignVerifier {
    expected_issuer: String,
    identity_regex: Regex,
}

impl CosignVerifier {
    /// Build a verifier from the baked-in defaults, with optional overrides for
    /// the SAN identity regex (CLI flag value, then [`ENV_IDENTITY`]) and the
    /// OIDC issuer ([`ENV_ISSUER`]).
    ///
    /// # Errors
    /// Returns an error if the identity override is not a valid regex.
    pub(super) fn from_env(identity_override: Option<&str>) -> Result<Self> {
        let issuer = std::env::var(ENV_ISSUER)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_OIDC_ISSUER.to_string());

        let identity_pattern = identity_override
            .map(str::to_string)
            .or_else(|| {
                std::env::var(ENV_IDENTITY)
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            })
            .unwrap_or_else(|| DEFAULT_IDENTITY_REGEX.to_string());

        Self::new(&issuer, &identity_pattern)
    }

    /// Build a verifier from an explicit expected issuer and SAN identity regex.
    ///
    /// # Errors
    /// Returns an error if `identity_pattern` is not a valid regex.
    pub(super) fn new(expected_issuer: &str, identity_pattern: &str) -> Result<Self> {
        let identity_regex = Regex::new(identity_pattern).with_context(|| {
            format!("Invalid release identity regex: {identity_pattern:?} (override via --identity or {ENV_IDENTITY})")
        })?;
        Ok(Self {
            expected_issuer: expected_issuer.to_string(),
            identity_regex,
        })
    }

    /// Fetch the Sigstore trust material (Fulcio CA certificates **and** Rekor
    /// transparency-log public keys) via TUF.
    ///
    /// This is the only step that needs the network. It is async and may fetch
    /// and cache TUF metadata under the OS cache dir. The Rekor keys are parsed
    /// into [`CosignVerificationKey`]s exactly as sigstore-rs's own
    /// `ClientBuilder::build` does (`try_from_der`, hex `key_id` to key), so the
    /// SET-verification key lookup in [`SignedArtifactBundle::new_verified`]
    /// matches the trust root.
    async fn trust_material(&self) -> Result<TrustMaterial> {
        let trust_root = SigstoreTrustRoot::new(None)
            .await
            .context("Failed to fetch the Sigstore trust root (Fulcio CA + Rekor key) via TUF")?;

        let certs = trust_root
            .fulcio_certs()
            .context("Sigstore trust root contained no Fulcio CA certificates")?;
        let fulcio_roots: Vec<pki_types::CertificateDer<'static>> = certs
            .into_iter()
            .map(pki_types::CertificateDer::into_owned)
            .collect();

        let rekor_keys = build_rekor_keys(&trust_root)?;

        Ok(TrustMaterial {
            fulcio_roots,
            rekor_keys,
        })
    }

    /// Verify a downloaded blob against the cosign keyless trust policy.
    ///
    /// `signature_b64` is the base64 signature (`cosign sign-blob
    /// --output-signature`); `certificate_pem` is the Fulcio leaf certificate
    /// (`--output-certificate`).
    ///
    /// # Errors
    /// Returns an error if any of the four checks fail: signature-over-blob,
    /// Fulcio chain, OIDC issuer, or SAN identity.
    pub(super) async fn verify_blob(
        &self,
        blob: &[u8],
        signature_b64: &str,
        certificate_pem: &str,
    ) -> Result<()> {
        let trust = self.trust_material().await?;
        self.verify_blob_with_roots(blob, signature_b64, certificate_pem, &trust.fulcio_roots)
    }

    /// Verify a downloaded blob against the cosign keyless trust policy **using
    /// a Sigstore bundle** (`cosign sign-blob --bundle`).
    ///
    /// Unlike [`Self::verify_blob`], this additionally verifies the **Rekor
    /// transparency-log inclusion proof** carried in the bundle, and anchors the
    /// Fulcio chain check at the Rekor integrated time rather than the
    /// certificate's `notBefore`. `bundle_json` is the raw JSON written by
    /// `cosign sign-blob --bundle <file>`.
    ///
    /// # Errors
    /// Returns an error if any check fails: Rekor SET inclusion, signature over
    /// the blob, Fulcio chain, OIDC issuer, or SAN identity.
    pub(super) async fn verify_bundle(&self, blob: &[u8], bundle_json: &str) -> Result<()> {
        let trust = self.trust_material().await?;
        self.verify_bundle_with_trust(blob, bundle_json, &trust)
    }

    /// The synchronous, network-free core of [`Self::verify_bundle`], separated
    /// so it can be unit-tested with fixture trust material.
    fn verify_bundle_with_trust(
        &self,
        blob: &[u8],
        bundle_json: &str,
        trust: &TrustMaterial,
    ) -> Result<()> {
        // (1) Rekor transparency-log inclusion. `new_verified` parses the bundle
        // and validates its Signed Entry Timestamp (SET) against the trust
        // root's Rekor public key, proving the signing event was recorded in the
        // public log. It does NOT check the blob signature, the Fulcio chain, or
        // the signer identity — those are checks (2)–(4) below, which is what
        // makes the bundle path trustworthy rather than merely "logged".
        if trust.rekor_keys.is_empty() {
            bail!(
                "Sigstore trust root contained no Rekor public key; cannot verify \
                 transparency-log inclusion. Re-run after refreshing the trust root, or use the \
                 detached .sig/.pem path."
            );
        }
        let bundle =
            SignedArtifactBundle::new_verified(bundle_json, &trust.rekor_keys).map_err(|e| {
                anyhow::anyhow!("Rekor transparency-log inclusion does not verify: {e}")
            })?;

        // The Rekor log entry's integrated time: the transparency log's own
        // attestation of when the signature existed. We anchor the Fulcio chain
        // check here (see check (3)).
        let integrated_time = bundle.rekor_bundle.payload.integrated_time;

        // The bundle carries the Fulcio leaf certificate (base64-of-PEM in the
        // `cert` field) and the base64 signature over the blob.
        let certificate_pem = normalize_cert_to_pem(&bundle.cert)?;

        // (2) Signature over the blob, under the bundle certificate's public key.
        CosignClient::verify_blob(&certificate_pem, &bundle.base64_signature, blob)
            .map_err(|e| anyhow::anyhow!("signature does not verify over the binary: {e}"))?;

        // Parse the leaf once for the chain + identity checks.
        let leaf = parse_leaf_pem(&certificate_pem)?;

        // (3) The leaf chains to a Fulcio root, checked at the Rekor integrated
        // time. A leaf that does not chain to Fulcio is rejected here.
        let verification_time = integrated_time_to_unix(integrated_time)?;
        verify_fulcio_chain_at(&trust.fulcio_roots, &leaf, verification_time)?;

        // (4) OIDC issuer + SAN identity, identical to the detached path.
        self.check_issuer_and_identity(&leaf)?;

        Ok(())
    }

    /// The synchronous, network-free core of [`Self::verify_blob`], separated so
    /// it can be unit-tested with a fixture trust root.
    fn verify_blob_with_roots(
        &self,
        blob: &[u8],
        signature_b64: &str,
        certificate_pem: &str,
        fulcio_roots: &[pki_types::CertificateDer<'static>],
    ) -> Result<()> {
        // (1) Signature over the blob, under the certificate's public key.
        // verify_blob ONLY checks this — not the chain, identity, expiry, or
        // Rekor. The remaining checks below are what make this trustworthy.
        CosignClient::verify_blob(certificate_pem, signature_b64, blob)
            .map_err(|e| anyhow::anyhow!("signature does not verify over the binary: {e}"))?;

        // Parse the leaf once for the chain + identity checks.
        let leaf = parse_leaf_pem(certificate_pem)?;

        // (2) The leaf chains to a Fulcio root, checked at the leaf's notBefore
        // (cosign's "trusted forever, signed within window" model). A leaf that
        // does not chain to Fulcio — e.g. a self-signed cert an attacker minted
        // — is rejected here. The bundle path anchors at the Rekor integrated
        // time instead; here we have no transparency-log timestamp.
        let not_before = leaf.tbs_certificate.validity.not_before.to_unix_duration();
        verify_fulcio_chain_at(
            fulcio_roots,
            &leaf,
            pki_types::UnixTime::since_unix_epoch(not_before),
        )?;

        // (3) + (4) OIDC issuer + SAN identity.
        self.check_issuer_and_identity(&leaf)?;

        Ok(())
    }

    /// Enforce that the leaf's OIDC-issuer extension equals the expected issuer
    /// and its SAN identity matches the expected release-workflow regex. Shared
    /// by the detached-`.sig` and bundle paths.
    fn check_issuer_and_identity(&self, leaf: &X509Certificate) -> Result<()> {
        // OIDC issuer must be exactly the expected one.
        let issuer = extract_oidc_issuer(leaf)?;
        if issuer != self.expected_issuer {
            bail!(
                "certificate OIDC issuer {issuer:?} does not match expected {:?}",
                self.expected_issuer
            );
        }

        // SAN identity must match the expected release-workflow regex. This
        // pins the signer to THIS repo's release workflow + a vX.Y.Z tag, so a
        // valid Fulcio cert for any other identity is rejected.
        let identity = extract_san_identity(leaf)?;
        if !self.identity_regex.is_match(&identity) {
            bail!(
                "certificate identity {identity:?} does not match expected pattern {:?}",
                self.identity_regex.as_str()
            );
        }
        Ok(())
    }
}

/// Sigstore trust material needed for verification: Fulcio CA roots (for the
/// chain check) and Rekor transparency-log public keys keyed by hex `log_id`
/// (for the bundle SET check).
#[derive(Debug)]
pub(super) struct TrustMaterial {
    fulcio_roots: Vec<pki_types::CertificateDer<'static>>,
    rekor_keys: BTreeMap<String, CosignVerificationKey>,
}

/// Build the Rekor public-key map (`hex log_id` -> verification key) from a
/// trust root, mirroring sigstore-rs's own `ClientBuilder::build`: each raw key
/// is parsed with [`CosignVerificationKey::try_from_der`]. Keys that fail to
/// parse are skipped (rather than aborting) so a single malformed/legacy log key
/// does not break verification, exactly as upstream does.
fn build_rekor_keys<R: TrustRoot + ?Sized>(
    trust_root: &R,
) -> Result<BTreeMap<String, CosignVerificationKey>> {
    let raw = trust_root
        .rekor_keys()
        .context("Sigstore trust root did not expose any Rekor public keys")?;
    let keys: BTreeMap<String, CosignVerificationKey> = raw
        .into_iter()
        .filter_map(|(key_id, data)| {
            CosignVerificationKey::try_from_der(data)
                .ok()
                .map(|key| (key_id, key))
        })
        .collect();
    Ok(keys)
}

/// Normalise a bundle/`.pem` certificate field to a PEM string. The bundle's
/// `cert` field is base64-of-PEM; a `.pem` file is already PEM. Mirrors
/// [`parse_leaf_pem`]'s tolerance but returns the PEM text (needed by
/// `Client::verify_blob`, which re-parses PEM internally).
fn normalize_cert_to_pem(cert: &str) -> Result<String> {
    let trimmed = cert.trim();
    if trimmed.starts_with("-----BEGIN") {
        return Ok(trimmed.to_string());
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(trimmed.as_bytes())
        .context("bundle certificate is neither PEM nor base64-encoded PEM")?;
    String::from_utf8(decoded).context("base64-decoded bundle certificate is not valid UTF-8 PEM")
}

/// Convert a Rekor integrated time (Unix seconds, as an `i64`) into a
/// [`pki_types::UnixTime`]. Rejects negative times defensively.
fn integrated_time_to_unix(integrated_time: i64) -> Result<pki_types::UnixTime> {
    let secs = u64::try_from(integrated_time)
        .with_context(|| format!("Rekor integrated time {integrated_time} is negative"))?;
    Ok(pki_types::UnixTime::since_unix_epoch(
        std::time::Duration::from_secs(secs),
    ))
}

/// Parse a PEM-encoded leaf certificate. `cosign` sometimes emits a
/// double-base64-encoded PEM; handle both the plain PEM and that wrapping.
fn parse_leaf_pem(certificate_pem: &str) -> Result<X509Certificate> {
    let trimmed = certificate_pem.trim();
    // Plain PEM.
    if trimmed.starts_with("-----BEGIN") {
        return X509Certificate::from_pem(trimmed.as_bytes())
            .context("failed to parse leaf certificate PEM");
    }
    // Some cosign outputs are base64-of-PEM; decode one layer then parse.
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(trimmed.as_bytes())
        .context("certificate is neither PEM nor base64-encoded PEM")?;
    X509Certificate::from_pem(&decoded)
        .context("failed to parse base64-decoded leaf certificate PEM")
}

/// Verify the leaf chains to one of the Fulcio roots at an explicit
/// `verification_time`, requiring the code-signing EKU.
///
/// The detached-`.sig` path passes the leaf's own `notBefore` (Fulcio leaves
/// live ~10 minutes, so "now" would reject every release shortly after it is
/// cut); the bundle path passes the Rekor integrated time, the transparency
/// log's attestation of when the signature existed.
fn verify_fulcio_chain_at(
    fulcio_roots: &[pki_types::CertificateDer<'static>],
    leaf: &X509Certificate,
    verification_time: pki_types::UnixTime,
) -> Result<()> {
    let trust_anchors: Vec<pki_types::TrustAnchor<'_>> = fulcio_roots
        .iter()
        .map(|der| {
            webpki::anchor_from_trusted_cert(der)
                .context("Fulcio CA certificate is not a valid trust anchor")
        })
        .collect::<Result<_>>()?;

    // Re-encode the parsed leaf to DER for webpki. (We parsed from PEM above;
    // re-encoding the same TBS+sig round-trips the exact bytes.)
    let leaf_der = leaf
        .to_der()
        .context("failed to re-encode leaf certificate to DER")?;
    let leaf_der = pki_types::CertificateDer::from(leaf_der);
    let end_entity = webpki::EndEntityCert::try_from(&leaf_der)
        .context("leaf certificate is not a valid end-entity certificate")?;

    end_entity
        .verify_for_usage(
            webpki::ALL_VERIFICATION_ALGS,
            &trust_anchors,
            // No intermediates supplied separately: Fulcio's full CA chain
            // (root + intermediate) is provided as trust anchors by the trust
            // root, matching sigstore-rs's own CertificatePool construction.
            &[],
            verification_time,
            webpki::KeyUsage::required(EKU_CODE_SIGNING_OID.as_bytes()),
            None,
            None,
        )
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("leaf certificate does not chain to a Fulcio CA root: {e}"))
}

/// Extract the Sigstore OIDC-issuer extension value from the leaf certificate.
fn extract_oidc_issuer(leaf: &X509Certificate) -> Result<String> {
    let extensions = leaf
        .tbs_certificate
        .extensions
        .as_ref()
        .context("certificate has no extensions (missing OIDC issuer)")?;

    let ext = extensions
        .iter()
        .find(|e| e.extn_id == SIGSTORE_OIDC_ISSUER_OID)
        .context("certificate is missing the Sigstore OIDC-issuer extension")?;

    // The original issuer extension (1.1) is a raw UTF-8 string (not DER-wrapped
    // — that is the deprecated-vs-v2 distinction). Decode as UTF-8 directly.
    String::from_utf8(ext.extn_value.as_bytes().to_vec())
        .context("Sigstore OIDC-issuer extension is not valid UTF-8")
}

/// Extract the SAN identity (the URI general name GitHub Actions uses, or an
/// rfc822 email as a fallback) from the leaf certificate.
fn extract_san_identity(leaf: &X509Certificate) -> Result<String> {
    use x509_cert::ext::pkix::SubjectAltName;
    use x509_cert::ext::pkix::name::GeneralName;

    let extensions = leaf
        .tbs_certificate
        .extensions
        .as_ref()
        .context("certificate has no extensions (missing SAN)")?;

    let ext = extensions
        .iter()
        .find(|e| e.extn_id == SUBJECT_ALT_NAME_OID)
        .context("certificate is missing a Subject Alternative Name")?;

    let san = SubjectAltName::from_der(ext.extn_value.as_bytes())
        .context("failed to parse Subject Alternative Name extension")?;

    for name in &san.0 {
        match name {
            GeneralName::UniformResourceIdentifier(uri) => return Ok(uri.to_string()),
            GeneralName::Rfc822Name(email) => return Ok(email.to_string()),
            _ => {}
        }
    }
    bail!("Subject Alternative Name has no URI or email identity")
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        reason = "test assertions on fixtures; unwrap/expect pinpoint the failing case"
    )]

    use super::*;

    #[test]
    fn rejects_invalid_identity_regex() {
        let err = CosignVerifier::new("https://issuer", "(unclosed").unwrap_err();
        assert!(format!("{err:#}").contains("Invalid release identity regex"));
    }

    #[test]
    fn default_identity_regex_matches_release_workflow_tag() {
        let v = CosignVerifier::new(DEFAULT_OIDC_ISSUER, DEFAULT_IDENTITY_REGEX).unwrap();
        // The SAN a real GitHub Actions release-workflow run on a v* tag carries.
        let good = "https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/tags/v1.2.3";
        assert!(v.identity_regex.is_match(good), "should match {good}");
        let good_pre = "https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/tags/v1.2.3-rc.1";
        assert!(v.identity_regex.is_match(good_pre));
    }

    #[test]
    fn default_identity_regex_rejects_impostors() {
        let v = CosignVerifier::new(DEFAULT_OIDC_ISSUER, DEFAULT_IDENTITY_REGEX).unwrap();
        // Wrong repo.
        assert!(!v.identity_regex.is_match(
            "https://github.com/attacker/pgbattery/.github/workflows/release.yml@refs/tags/v1.0.0"
        ));
        // Wrong workflow.
        assert!(!v.identity_regex.is_match(
            "https://github.com/electricapp/pgbattery/.github/workflows/evil.yml@refs/tags/v1.0.0"
        ));
        // A branch ref rather than a tag.
        assert!(!v.identity_regex.is_match(
            "https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/heads/main"
        ));
        // Prefix-injection attempt (anchored regex must reject).
        assert!(!v.identity_regex.is_match(
            "https://evil.com/?x=https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/tags/v1.0.0"
        ));
    }

    #[test]
    fn issuer_override_via_env_is_picked_up() {
        // Construct directly to avoid global env races in parallel tests.
        let v = CosignVerifier::new("https://example.test/issuer", DEFAULT_IDENTITY_REGEX).unwrap();
        assert_eq!(v.expected_issuer, "https://example.test/issuer");
    }

    #[test]
    fn cli_identity_override_takes_precedence() {
        let v = CosignVerifier::from_env(Some(r"^https://github\.com/me/fork/.*$")).unwrap();
        assert!(
            v.identity_regex
                .is_match("https://github.com/me/fork/.github/workflows/release.yml@refs/tags/v1")
        );
        assert!(!v.identity_regex.is_match(
            "https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/tags/v1"
        ));
    }

    // ── Fixture-backed end-to-end parse/identity tests ──
    //
    // These exercise the cert-parsing + identity/issuer logic against a real
    // Fulcio-shaped certificate (URI SAN + OIDC-issuer extension) generated at
    // build time under `tests/fixtures/cosign/`. The chain check is covered by
    // a self-issued root in the same fixture set; see `verify_blob_with_roots`
    // integration in tests/ when fixtures are present.
    //
    // Fixtures are optional: when absent (e.g. a minimal checkout), these tests
    // skip rather than fail, so `cargo test` stays green without network/openssl.

    fn fixture_path(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/cosign")
            .join(name)
    }

    fn fixture(name: &str) -> Option<String> {
        std::fs::read_to_string(fixture_path(name)).ok()
    }

    fn fixture_bytes(name: &str) -> Option<Vec<u8>> {
        std::fs::read(fixture_path(name)).ok()
    }

    /// The fixture's self-signed CA, used in place of the real Fulcio roots so
    /// the chain check runs offline. Loads `root.pem` into a DER trust anchor.
    fn fixture_roots() -> Option<Vec<pki_types::CertificateDer<'static>>> {
        let pem = fixture("root.pem")?;
        let cert = X509Certificate::from_pem(pem.as_bytes()).expect("root.pem parses");
        let der = cert.to_der().expect("root re-encodes");
        Some(vec![pki_types::CertificateDer::from(der)])
    }

    /// Load the full fixture set (blob, signature, leaf cert, roots). Returns
    /// `None` (test skips) if any piece is missing.
    fn full_fixture() -> Option<(
        Vec<u8>,
        String,
        String,
        Vec<pki_types::CertificateDer<'static>>,
    )> {
        Some((
            fixture_bytes("blob.bin")?,
            fixture("blob.sig")?,
            fixture("leaf.pem")?,
            fixture_roots()?,
        ))
    }

    #[test]
    fn extracts_issuer_and_san_from_fixture_cert() {
        let Some(pem) = fixture("leaf.pem") else {
            eprintln!("skipping: tests/fixtures/cosign/leaf.pem not present");
            return;
        };
        let leaf = parse_leaf_pem(&pem).expect("fixture leaf parses");
        let issuer = extract_oidc_issuer(&leaf).expect("fixture has issuer ext");
        assert_eq!(issuer, "https://token.actions.githubusercontent.com");
        let san = extract_san_identity(&leaf).expect("fixture has SAN");
        assert_eq!(
            san,
            "https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/tags/v9.9.9"
        );
    }

    #[test]
    fn verifies_full_fixture_signature_chain_and_identity() {
        let Some((blob, sig, cert, roots)) = full_fixture() else {
            eprintln!("skipping: cosign fixtures not present");
            return;
        };
        let v = CosignVerifier::new(
            "https://token.actions.githubusercontent.com",
            DEFAULT_IDENTITY_REGEX,
        )
        .unwrap();
        v.verify_blob_with_roots(&blob, sig.trim(), &cert, &roots)
            .expect("valid fixture should verify (signature + chain + identity + issuer)");
    }

    #[test]
    fn rejects_tampered_blob() {
        let Some((mut blob, sig, cert, roots)) = full_fixture() else {
            return;
        };
        blob.push(b'!'); // flip the payload
        let v = CosignVerifier::new(
            "https://token.actions.githubusercontent.com",
            DEFAULT_IDENTITY_REGEX,
        )
        .unwrap();
        let err = v
            .verify_blob_with_roots(&blob, sig.trim(), &cert, &roots)
            .expect_err("tampered blob must fail");
        assert!(format!("{err:#}").contains("signature"), "got: {err:#}");
    }

    #[test]
    fn rejects_wrong_issuer() {
        let Some((blob, sig, cert, roots)) = full_fixture() else {
            return;
        };
        let v = CosignVerifier::new("https://accounts.google.com", DEFAULT_IDENTITY_REGEX).unwrap();
        let err = v
            .verify_blob_with_roots(&blob, sig.trim(), &cert, &roots)
            .expect_err("wrong issuer must fail");
        assert!(format!("{err:#}").contains("issuer"), "got: {err:#}");
    }

    #[test]
    fn rejects_wrong_identity() {
        let Some((blob, sig, cert, roots)) = full_fixture() else {
            return;
        };
        let v = CosignVerifier::new(
            "https://token.actions.githubusercontent.com",
            r"^https://github\.com/someone/else/.*$",
        )
        .unwrap();
        let err = v
            .verify_blob_with_roots(&blob, sig.trim(), &cert, &roots)
            .expect_err("wrong identity must fail");
        assert!(format!("{err:#}").contains("identity"), "got: {err:#}");
    }

    #[test]
    fn rejects_untrusted_root() {
        // Verify against an empty root set: the leaf cannot chain to Fulcio.
        let Some((blob, sig, cert, _roots)) = full_fixture() else {
            return;
        };
        let v = CosignVerifier::new(
            "https://token.actions.githubusercontent.com",
            DEFAULT_IDENTITY_REGEX,
        )
        .unwrap();
        let err = v
            .verify_blob_with_roots(&blob, sig.trim(), &cert, &[])
            .expect_err("no trusted root must fail the chain check");
        assert!(format!("{err:#}").contains("chain"), "got: {err:#}");
    }

    // ── Bundle / Rekor transparency-log verification ──

    #[test]
    fn normalize_cert_accepts_plain_pem_and_base64_pem() {
        let pem = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----";
        assert_eq!(normalize_cert_to_pem(pem).unwrap(), pem);
        // Base64-of-PEM (what a cosign bundle's `cert` field carries) decodes
        // back to the same PEM text.
        let b64 = base64::engine::general_purpose::STANDARD.encode(pem.as_bytes());
        assert_eq!(normalize_cert_to_pem(&b64).unwrap(), pem);
        // Garbage that is neither PEM nor base64 is rejected.
        assert!(normalize_cert_to_pem("@@not base64@@").is_err());
    }

    #[test]
    fn integrated_time_rejects_negative_and_accepts_real_time() {
        assert!(integrated_time_to_unix(-1).is_err());
        // A real Rekor integrated time (2022-11-25) converts cleanly.
        assert!(integrated_time_to_unix(1_669_361_833).is_ok());
    }

    /// Build the Rekor key map from the real Rekor public-key fixture
    /// (`rekor_pub.pem` + `rekor_key_id.txt`). This is the *actual* Sigstore
    /// Rekor key used to sign the bundle fixture's SET, captured from upstream
    /// sigstore-rs test vectors so the SET check runs offline + deterministically.
    fn fixture_rekor_keys() -> Option<BTreeMap<String, CosignVerificationKey>> {
        let pem = fixture("rekor_pub.pem")?;
        let key_id = fixture("rekor_key_id.txt")?.trim().to_string();
        // try_from_pem mirrors how the trust root's DER key is parsed; from_pem
        // here matches the PEM fixture form.
        let key = CosignVerificationKey::from_pem(
            pem.as_bytes(),
            &sigstore::crypto::SigningScheme::default(),
        )
        .expect("rekor_pub.pem parses");
        Some(BTreeMap::from([(key_id, key)]))
    }

    /// The Rekor inclusion check is REAL, not a no-op: a bundle whose SET
    /// verifies against the real Rekor key passes [`SignedArtifactBundle::new_verified`],
    /// and the same bundle FAILS when the Rekor key is wrong/absent. This is the
    /// load-bearing evidence that transparency-log inclusion is actually verified.
    ///
    /// `rekor_valid.bundle` is a real public cosign bundle (from upstream
    /// sigstore-rs test vectors, log index 7810348) whose SET is signed by the
    /// production Rekor key in `rekor_pub.pem`. We assert the SET verifies (not
    /// the full release path: its cert is a different identity that does not
    /// chain to our fixture Fulcio root and signs a blob we do not ship).
    #[test]
    fn rekor_set_inclusion_is_actually_verified() {
        let (Some(bundle_json), Some(good_keys)) =
            (fixture("rekor_valid.bundle"), fixture_rekor_keys())
        else {
            eprintln!("skipping: rekor_valid.bundle / rekor_pub.pem not present");
            return;
        };

        // Valid SET against the real Rekor key: new_verified succeeds and the
        // parsed bundle reports the expected log index.
        let bundle = SignedArtifactBundle::new_verified(bundle_json.trim(), &good_keys)
            .expect("valid Rekor SET must verify against the real Rekor key");
        assert_eq!(bundle.rekor_bundle.payload.log_index, 7_810_348);
        assert_eq!(bundle.rekor_bundle.payload.integrated_time, 1_669_361_833);

        // Empty key map: the Rekor key for this log id is unknown, so the SET
        // cannot be checked and verification is refused. (`expect_err` itself
        // proves rejection; any error variant is a rejection.)
        SignedArtifactBundle::new_verified(bundle_json.trim(), &BTreeMap::new())
            .expect_err("an unknown Rekor key must fail SET verification");

        // Wrong key under the right key id: the SET signature does not verify.
        let wrong_pem = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAENptdY/l3nB0yqkXLBWkZWQwo6+cu\nOSWS1X9vPavpiQOoTTGC0xX57OojUadxF1cdQmrsiReWg2Wn4FneJfa8xw==\n-----END PUBLIC KEY-----";
        let wrong_key = CosignVerificationKey::from_pem(
            wrong_pem.as_bytes(),
            &sigstore::crypto::SigningScheme::default(),
        )
        .expect("wrong key parses");
        let key_id = fixture("rekor_key_id.txt").unwrap().trim().to_string();
        let wrong_keys = BTreeMap::from([(key_id, wrong_key)]);
        assert!(
            SignedArtifactBundle::new_verified(bundle_json.trim(), &wrong_keys).is_err(),
            "a different key under the right key id must fail SET verification"
        );
    }

    /// `verify_bundle_with_trust` refuses outright when no Rekor key is present
    /// in the trust material (otherwise the SET could not be checked at all).
    #[test]
    fn verify_bundle_refuses_without_rekor_key() {
        let Some(bundle_json) = fixture("rekor_valid.bundle") else {
            return;
        };
        let v = CosignVerifier::new(
            "https://token.actions.githubusercontent.com",
            DEFAULT_IDENTITY_REGEX,
        )
        .unwrap();
        let trust = TrustMaterial {
            fulcio_roots: vec![],
            rekor_keys: BTreeMap::new(),
        };
        let err = v
            .verify_bundle_with_trust(b"blob", bundle_json.trim(), &trust)
            .expect_err("no Rekor key must refuse bundle verification");
        assert!(format!("{err:#}").contains("Rekor"), "got: {err:#}");
    }

    /// Full offline bundle path against a *self-consistent* release-shaped
    /// fixture: `release.bundle` (a bundle whose cert is our fixture leaf and
    /// whose SET verifies against `rekor_pub.pem`), `blob.bin` it signs, our
    /// fixture Fulcio root, and the real Rekor key. Skips if absent — a real
    /// Rekor SET is signed by Rekor's private key and cannot be synthesised
    /// offline, so this fixture must be captured from a genuine signing run.
    /// See `tests/fixtures/cosign/README` for how to (re)generate it.
    #[test]
    fn verifies_full_release_bundle_when_fixture_present() {
        let (Some(blob), Some(bundle_json), Some(roots), Some(rekor_keys)) = (
            fixture_bytes("blob.bin"),
            fixture("release.bundle"),
            fixture_roots(),
            fixture_rekor_keys(),
        ) else {
            eprintln!("skipping: release.bundle e2e fixture not present (see fixtures README)");
            return;
        };
        let v = CosignVerifier::new(
            "https://token.actions.githubusercontent.com",
            DEFAULT_IDENTITY_REGEX,
        )
        .unwrap();
        let trust = TrustMaterial {
            fulcio_roots: roots,
            rekor_keys,
        };
        v.verify_bundle_with_trust(&blob, bundle_json.trim(), &trust)
            .expect("a self-consistent release bundle must verify end-to-end");
    }
}
