#!/bin/sh
# Build a PostgreSQL bindir whose pg_upgrade is the holding wrapper beside this
# script and whose every other binary is the real one, by symlink.
#
# Usage: install.sh <real bindir> <shim bindir>
#
# PostgreSQL binaries resolve symlinks to find their share and lib dirs, so a
# symlinked postgres, initdb or pg_ctl behaves exactly like the original.
set -eu

real_bin="$1"
shim_bin="$2"
here="$(cd "$(dirname "$0")" && pwd)"

test -x "$real_bin/pg_upgrade"
test -x "$here/pg_upgrade"

rm -rf "$shim_bin"
mkdir -p "$shim_bin"
for path in "$real_bin"/*; do
  ln -s "$path" "$shim_bin/$(basename "$path")"
done
rm "$shim_bin/pg_upgrade"
ln -s "$real_bin/pg_upgrade" "$shim_bin/pg_upgrade.real"
ln -s "$here/pg_upgrade" "$shim_bin/pg_upgrade"

test -x "$shim_bin/postgres"
test -x "$shim_bin/pg_upgrade.real"
test "$(readlink "$shim_bin/pg_upgrade")" = "$here/pg_upgrade"
