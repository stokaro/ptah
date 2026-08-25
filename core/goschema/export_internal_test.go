package goschema

// White-box testing required: deduplicate is unexported, and the black-box
// order-preservation and identity suites must call it in isolation -- driving
// it through Finalize would fold in embedded-field processing and dependency
// sorting, which is exactly the reordering those suites assert never happens.
// This file only re-exports the function for them; it declares no tests.

// Deduplicate hands the unexported deduplicate to the package's black-box test
// suites.
var Deduplicate = deduplicate
