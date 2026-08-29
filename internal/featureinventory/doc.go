// Package featureinventory derives Ptah's feature register from the product's
// own declarations and checks the committed artifact against that derivation.
//
// Nothing here is authored. Every row comes from calling a function in process
// or reading a manifest the product already maintains -- the walked command
// tree, the stable-embedder ledger, the release configuration, the renderer's
// dialect list -- so a row cannot claim something no source states. The one
// hand-written datum in the whole system is the `owns:` frontmatter line by
// which a documentation page claims a feature, and it is compared to a derived
// identifier by string equality rather than searched for in prose.
//
// The package holds the derivation and the rules. It imports neither command
// tree: the caller walks and hands the leaves in, which is what lets the rules
// be driven against fixtures by [SelfTest].
package featureinventory
