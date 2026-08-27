// Package featureinventory measures the surfaces Ptah ships and holds the
// contributor-facing feature inventory to them.
//
// The inventory in docs/feature-inventory.md is the hand-written half of a
// comparison. Every other half is discovered here: the command paths come from
// the cobra trees the binaries are built out of, the flag sets come from those
// same trees, the public Go packages come from the ledger the API gates already
// parse, the runnable programs come from `git ls-files`, and the format lists
// come from the code that validates them.
//
// Nothing in this package reads `--help`. Help output is a rendering of the
// tree, and it is a lossy one: seven ptah-compat commands print no flag block
// at all while registering four flags each, and `--help` exits 0 on every
// command strict mode removes. A census built on help agrees with a help string
// that has drifted from the code beside it (stokaro/ptah#2065 is the same
// failure one layer down).
package featureinventory
