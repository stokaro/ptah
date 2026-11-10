# shellcheck shell=bash
# The upstream Go change the Windows jobs apply to their toolchain, sourced by
# apply.sh and watch.sh so both read one declaration.
#
# golang/go#81238: on Intel hosts with AMX, the frame Windows pushes to dispatch
# a hardware exception is about 11.7 KiB, and the runtime reserves 4 KiB below
# each goroutine stack for it. A recovered nil dereference then overwrites the
# heap span below the stack, and the process dies later in the collector
# (stokaro/ptah#2365). CL 828724 sizes the reserve from the host at startup.
#
# cl828724.patch is the patch set below byte for byte, as Gerrit serves it from
# /changes/<change>/revisions/<revision>/patch. watch.sh compares the two.

# shellcheck disable=SC2034 # read by the scripts that source this file
readonly GO_RUNTIME_PATCH_ISSUE=81238
readonly GO_RUNTIME_PATCH_CHANGE=828724
readonly GO_RUNTIME_PATCH_PATCHSET=3
readonly GO_RUNTIME_PATCH_REVISION=5ec5e90237a6f364d60ad44d2f2d9c08597b8a8e
readonly GO_RUNTIME_PATCH_FILE=cl828724.patch

# A toolchain whose src/runtime/stack.go defines this function carries the fix.
# apply.sh skips patching such a toolchain, and watch.sh reports that the patch
# can go. A later patch set that renames the function changes this line when
# the patch is refreshed; until then watch.sh reports the newer patch set.
readonly GO_RUNTIME_PATCH_MARKER='func setStackSystemReserve('
