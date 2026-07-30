"""Internals of the linearizability harness.

`linearizability_register.py` stays the entrypoint and re-exports these, so
every CLI path and every existing import keeps working.

Nothing here reads module-level configuration: `WorkloadConfig` is passed in.
A `global` rebind would only take effect in the module that declares it, so a
worker or checker living here would otherwise silently ignore the CLI.
"""

from __future__ import annotations
