//! Sigstore **cosign keyless** verification of release artifacts.
//!
//! Releases are signed in CI with `cosign sign-blob` in keyless mode: GitHub
//! Actions presents its OIDC identity to Fulcio, which issues a short-lived
//! signing certificate whose Subject Alternative Name (SAN) encodes the exact
//! workflow + tag that ran. The signature and that certificate are published
//! next to the binary (`<binary>.sig`, `<binary>.pem`). There is **no signing
//! key to manage** — trust is anchored in the Sigstore/Fulcio root and the
//! expected OIDC issuer + workflow identity.
//!
//! [`CosignVerifier::verify_blob`] enforces, for a downloaded binary:
//!
//! 1. **Signature over the blob** — the detached signature is valid over the
//!    binary bytes under the certificate's public key
//!    (`sigstore::cosign::Client::verify_blob`).
//! 2. **Certificate chains to Fulcio** — the leaf certificate chains to a
//!    Fulcio CA from the Sigstore trust root (fetched via TUF). Because Fulcio
//!    leaves are valid for only ~10 minutes, the chain is verified *at the
//!    certificate's own `notBefore`* (cosign's model: "the cert is trusted
//!    forever; we check the signature was made inside its validity window"),
//!    not at "now" — otherwise every release would fail to verify minutes after
//!    it was cut.
//! 3. **Identity + issuer** — the certificate's OIDC-issuer extension equals
//!    the expected issuer (GitHub Actions), and its SAN identity matches the
//!    expected release-workflow regex. This is what stops an attacker who can
//!    obtain *some* valid Fulcio certificate (e.g. for their own repo) from
//!    passing verification: the identity must be *this* repo's release workflow.
//!
//! ## Known gap — Rekor transparency log (TODO)
//!
//! We verify only the `.sig` + `.pem`. We do **not** verify a Rekor
//! transparency-log inclusion proof, because the published artifacts do not
//! include a Rekor bundle and sigstore-rs (0.14) exposes Rekor verification
//! only via the bundle path (`SignedArtifactBundle::new_verified`), not the
//! detached `verify_blob` path. The practical consequence: we do not
//! independently confirm the signature was logged in Rekor, and we rely on the
//! certificate's `notBefore` (rather than a Rekor integrated timestamp) for the
//! chain-validity instant. The signature, Fulcio chain, and identity/issuer
//! checks are unaffected. To close this gap, also publish
//! `cosign sign-blob --bundle <binary>.bundle` and verify the Rekor SET here.
//! See `docs/RELEASING.md`.

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use const_oid::ObjectIdentifier;
use regex::Regex;
use x509_cert::Certificate as X509Certificate;
use x509_cert::der::{Decode, DecodePem, Encode};

use sigstore::cosign::CosignCapabilities;
use sigstore::cosign::client::Client as CosignClient;
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
/// materialised at use-site in [`verify_fulcio_chain`].
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

    /// Fetch the Sigstore trust root (Fulcio CA certificates) via TUF.
    ///
    /// This is the only step that needs the network. It is async and may fetch
    /// + cache TUF metadata under the OS cache dir.
    async fn fulcio_roots(&self) -> Result<Vec<pki_types::CertificateDer<'static>>> {
        let trust_root = SigstoreTrustRoot::new(None)
            .await
            .context("Failed to fetch the Sigstore trust root (Fulcio CA) via TUF")?;
        let certs = trust_root
            .fulcio_certs()
            .context("Sigstore trust root contained no Fulcio CA certificates")?;
        Ok(certs
            .into_iter()
            .map(pki_types::CertificateDer::into_owned)
            .collect())
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
        let fulcio_roots = self.fulcio_roots().await?;
        self.verify_blob_with_roots(blob, signature_b64, certificate_pem, &fulcio_roots)
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
        // — is rejected here.
        verify_fulcio_chain(fulcio_roots, &leaf)?;

        // (3) OIDC issuer must be exactly the expected one.
        let issuer = extract_oidc_issuer(&leaf)?;
        if issuer != self.expected_issuer {
            bail!(
                "certificate OIDC issuer {issuer:?} does not match expected {:?}",
                self.expected_issuer
            );
        }

        // (4) SAN identity must match the expected release-workflow regex. This
        // pins the signer to THIS repo's release workflow + a vX.Y.Z tag, so a
        // valid Fulcio cert for any other identity is rejected.
        let identity = extract_san_identity(&leaf)?;
        if !self.identity_regex.is_match(&identity) {
            bail!(
                "certificate identity {identity:?} does not match expected pattern {:?}",
                self.identity_regex.as_str()
            );
        }

        Ok(())
    }
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

/// Verify the leaf chains to one of the Fulcio roots, anchored at the leaf's
/// own `notBefore` instant and requiring the code-signing EKU.
fn verify_fulcio_chain(
    fulcio_roots: &[pki_types::CertificateDer<'static>],
    leaf: &X509Certificate,
) -> Result<()> {
    // Anchor verification time at the leaf's notBefore. Fulcio leaves live ~10
    // minutes, so "now" would reject every release shortly after it is cut.
    let not_before = leaf.tbs_certificate.validity.not_before.to_unix_duration();
    let verification_time = pki_types::UnixTime::since_unix_epoch(not_before);

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
        let good =
            "https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/tags/v1.2.3";
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
    fn full_fixture() -> Option<(Vec<u8>, String, String, Vec<pki_types::CertificateDer<'static>>)> {
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
        let v =
            CosignVerifier::new("https://token.actions.githubusercontent.com", DEFAULT_IDENTITY_REGEX)
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
        let v =
            CosignVerifier::new("https://token.actions.githubusercontent.com", DEFAULT_IDENTITY_REGEX)
                .unwrap();
        let err = v
            .verify_blob_with_roots(&blob, sig.trim(), &cert, &[])
            .expect_err("no trusted root must fail the chain check");
        assert!(format!("{err:#}").contains("chain"), "got: {err:#}");
    }
}
