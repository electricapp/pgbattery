# TLA+ model checking — reproducible, scripted.
#
#   make check          # download (if needed) + check ALL specs; fails on violation
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

SHELL := bash

TLA_VERSION    := 1.7.4
TLA_JAR_SHA256 := 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
TLA_JAR_URL    := https://github.com/tlaplus/tlaplus/releases/download/v$(TLA_VERSION)/tla2tools.jar

TOOLS_DIR := .tools
JAR       := $(TOOLS_DIR)/tla2tools.jar
STATES    := $(TOOLS_DIR)/states

SPECS := lease_fencing raft_lsn commit_probing timeline_verification

# Prefer a `java` that actually runs (the macOS /usr/bin/java stub fails
# `java -version`), else Homebrew openjdk@21, else bare `java` for a clear error.
JAVA ?= $(shell \
  if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then command -v java; \
  elif [ -x "$$(brew --prefix openjdk@21 2>/dev/null)/bin/java" ]; then echo "$$(brew --prefix openjdk@21)/bin/java"; \
  else echo java; fi)

TLC := "$(JAVA)" -XX:+UseParallelGC -cp "$(JAR)" tlc2.TLC

.PHONY: check tools clean $(addprefix check-,$(SPECS))

check: tools
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
