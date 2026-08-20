# TLA+ model checking — reproducible, scripted.
#
#   make check          # download (if needed) + check ALL specs AND the
#                       # counterexample models; fails on violation
#   make check-large    # the nightly models: more nodes, wider bounds
#   make check-counterexamples  # the models that MUST fail (see below)
#   make check-lease_fencing   # check one spec (full TLC output)
#   make tools          # just fetch + verify the pinned TLC jar
#   make clean          # remove the downloaded jar + TLC state
#
# Requires a JDK >= 11. Auto-detected in this order: a working `java` on PATH,
# else Homebrew `openjdk@21` (macOS, keg-only). Override with `make JAVA=/path/to/java`.
# Install locally on macOS (no sudo):  brew install openjdk@21
#
# The model checker (tla2tools.jar) is PINNED by version + SHA-256, so every run
# uses an identical, verified binary. CI uses the same target (see
# ../.github/workflows/tla.yml).
#
# Pin a TAGGED release only. tlaplus ships its nightly master builds by
# replacing the assets on the v1.8.0 pre-release in place, so that tag's jar
# changes without notice and a hash pinned to it goes stale on the next
# upstream build — the pin then fails as a mismatch rather than as an
# out-of-date version, which reads like a supply-chain event. A tagged build
# records its tag in the jar manifest (`X-Git-Tag`); a nightly leaves it empty.

SHELL := bash

TLA_VERSION    := 1.7.4
TLA_JAR_SHA256 := 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
TLA_JAR_URL    := https://github.com/tlaplus/tlaplus/releases/download/v$(TLA_VERSION)/tla2tools.jar

TOOLS_DIR := .tools
JAR       := $(TOOLS_DIR)/tla2tools.jar
STATES    := $(TOOLS_DIR)/states

SPECS := lease_fencing raft_lsn commit_probing timeline_verification

# Models that MUST fail. Each `<spec>.inv-<name>.cfg` disables one mechanism the
# spec's safety argument rests on and names, in an `EXPECT-VIOLATION:` header,
# the invariant TLC then has to report. This is the same rule the Python
# harnesses live under: an assertion nobody has watched fail is an assertion
# whose passing means nothing.
INV_CFGS := $(wildcard *.inv-*.cfg)

# Prefer a `java` that actually runs (the macOS /usr/bin/java stub fails
# `java -version`), else Homebrew openjdk@21, else bare `java` for a clear error.
JAVA ?= $(shell \
  if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then command -v java; \
  elif [ -x "$$(brew --prefix openjdk@21 2>/dev/null)/bin/java" ]; then echo "$$(brew --prefix openjdk@21)/bin/java"; \
  else echo java; fi)

TLC := "$(JAVA)" -XX:+UseParallelGC -cp "$(JAR)" tlc2.TLC

# Per-spec wall-clock cap for the nightly models. Every large config was
# measured well inside this; a spec that grows past it has changed shape and
# should be re-measured rather than left to run until the job's own timeout,
# where it looks like a stuck runner instead of an intractable model.
LARGE_TIMEOUT_SECS := 900

# macOS ships no `timeout`; coreutils installs it as `gtimeout`. Resolved once
# so a missing binary is a clear message rather than rc=127 mid-run.
TIMEOUT_BIN := $(shell \
  if command -v timeout >/dev/null 2>&1; then echo timeout; \
  elif command -v gtimeout >/dev/null 2>&1; then echo gtimeout; \
  else echo ""; fi)

# `check-<spec>` is deliberately absent from .PHONY: GNU make does not apply
# pattern rules to phony targets, so listing it there turned `make
# check-lease_fencing` — the documented way to check one spec — into a no-op
# that printed "Nothing to be done" and exited 0.
.PHONY: check check-specs check-large check-counterexamples tools clean

check: check-specs check-counterexamples

check-specs: tools
	@mkdir -p $(STATES)
	@fail=0; for s in $(SPECS); do \
	  echo "=== $$s ==="; \
	  if $(TLC) -config $$s.cfg -workers auto -metadir $(STATES)/$$s $$s.tla > $(STATES)/$$s.log 2>&1; then \
	    grep -E "Model checking completed|distinct states found" $(STATES)/$$s.log | sed 's/^/  /'; \
	  else \
	    echo "  FAILED — invariant violated or error:"; tail -40 $(STATES)/$$s.log | sed 's/^/  /'; fail=1; \
	  fi; \
	done; \
	if [ $$fail -ne 0 ]; then echo "TLA validation FAILED"; exit 1; fi; \
	echo "All TLA specs passed."

# The nightly models: more nodes and wider bounds than a PR can afford. Each
# spec's `<spec>.large.cfg` says what it widens and why, and carries the
# measurement that justified the choice.
#
# A spec with no large config is an error, not a skip: the whole point of this
# target is that it covers every spec, and silently checking three of four
# would read as coverage the next time somebody adds one.
check-large: tools
	@if [ -z "$(TIMEOUT_BIN)" ]; then \
	  echo "no timeout(1) on PATH. Install GNU coreutils (brew install coreutils)."; exit 2; \
	fi
	@mkdir -p $(STATES)
	@missing=""; for s in $(SPECS); do \
	  [ -f $$s.large.cfg ] || missing="$$missing $$s"; \
	done; \
	if [ -n "$$missing" ]; then \
	  echo "no large config for:$$missing — add <spec>.large.cfg or this target checks less than it claims"; \
	  exit 1; \
	fi
	@fail=0; for s in $(SPECS); do \
	  echo "=== $$s (large) ==="; \
	  if $(TIMEOUT_BIN) $(LARGE_TIMEOUT_SECS) $(TLC) -config $$s.large.cfg -workers auto \
	       -metadir $(STATES)/$$s-large $$s.tla > $(STATES)/$$s-large.log 2>&1; then \
	    grep -E "Model checking completed|distinct states found" $(STATES)/$$s-large.log | sed 's/^/  /'; \
	  elif [ $$? -eq 124 ]; then \
	    echo "  TIMED OUT after $(LARGE_TIMEOUT_SECS)s — the model outgrew its budget:"; \
	    tail -5 $(STATES)/$$s-large.log | sed 's/^/  /'; fail=1; \
	  else \
	    echo "  FAILED — invariant violated or error:"; tail -40 $(STATES)/$$s-large.log | sed 's/^/  /'; fail=1; \
	  fi; \
	done; \
	if [ $$fail -ne 0 ]; then echo "TLA nightly validation FAILED"; exit 1; fi; \
	echo "All large TLA models passed."

# The counterexample models. A run that SUCCEEDS here is the failure: it means
# the defect the config injects no longer breaks anything, so either the spec
# stopped modelling the mechanism or the invariant went vacuous. Failing for
# some other reason — a parse error, a violated ASSUME — is also a failure,
# which is why each config names the invariant it expects.
check-counterexamples: tools
	@if [ -z "$(INV_CFGS)" ]; then \
	  echo "no *.inv-*.cfg found — this target would check nothing"; exit 1; \
	fi
	@mkdir -p $(STATES)
	@fail=0; for cfg in $(INV_CFGS); do \
	  spec=$${cfg%%.inv-*}; name=$${cfg%.cfg}; \
	  want=$$(grep -m1 'EXPECT-VIOLATION:' $$cfg | sed 's/.*EXPECT-VIOLATION: *//'); \
	  echo "=== $$cfg (must fail) ==="; \
	  if [ -z "$$want" ]; then \
	    echo "  no EXPECT-VIOLATION header — cannot tell a real counterexample from a broken model"; \
	    fail=1; continue; \
	  fi; \
	  if $(TLC) -config $$cfg -workers auto -metadir $(STATES)/$$name $$spec.tla \
	       > $(STATES)/$$name.log 2>&1; then \
	    echo "  PASSED, which is the failure: $$want survived the injected defect"; fail=1; \
	  elif grep -q "Invariant $$want is violated" $(STATES)/$$name.log; then \
	    echo "  $$want violated, as required"; \
	  else \
	    echo "  failed for the wrong reason (expected $$want):"; \
	    tail -20 $(STATES)/$$name.log | sed 's/^/    /'; fail=1; \
	  fi; \
	done; \
	if [ $$fail -ne 0 ]; then echo "TLA counterexample validation FAILED"; exit 1; fi; \
	echo "All TLA counterexample models failed as required."

# Single spec, full TLC output (e.g. `make check-lease_fencing`).
check-%: tools
	@mkdir -p $(STATES)
	$(TLC) -config $*.cfg -workers auto -metadir $(STATES)/$* $*.tla

tools: $(JAR)

$(JAR):
	@mkdir -p $(TOOLS_DIR)
	@echo "Fetching tla2tools.jar v$(TLA_VERSION) ..."
	@curl -fsSL -o $(JAR) "$(TLA_JAR_URL)"
	@printf '%s  %s\n' "$(TLA_JAR_SHA256)" "$(JAR)" > $(JAR).sha256
	@if command -v sha256sum >/dev/null 2>&1; then sha256sum -c $(JAR).sha256; \
	 elif command -v shasum >/dev/null 2>&1; then shasum -a 256 -c $(JAR).sha256; \
	 else echo "no sha256 tool found"; exit 1; fi \
	  || { echo "CHECKSUM MISMATCH — refusing to use $(JAR)"; rm -f $(JAR); exit 1; }

clean:
	rm -rf $(TOOLS_DIR)
