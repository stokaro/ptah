// Package blankimportguard hosts the test that makes every blank import say why
// it is there.
//
// A blank import is the one import whose removal the compiler does not notice.
// It is present for a side effect -- a driver registering itself with
// database/sql, a package populating a registry from its own init -- and the
// symbol that would break is never named. Delete it and the build stays green;
// what changes is somewhere else, at run time.
//
// revive's `blank-imports` rule already states this, and `.golangci.yml`
// enables it. It exempts main packages and `_test.go` files by design, which is
// where every unjustified blank import in this repository lives: measured when
// this package was written, a bare blank import planted in
// internal/atlashclrender/render.go is reported and the identical one in
// dialect_scope_loss_test.go is not.
//
// The exemption is wrong for the tests here. Several of them blank-import
// Ptah's own packages to populate a registry the test then reads back --
// cmd/internal/envboolguard and internal/atlascompatpolicy both do -- and there
// the failure of a missing import is not a broken test but a quieter one: the
// registry holds less, the enumeration checks less, and the assertion passes.
// That is the shape AGENTS.md calls a gate that reports without running.
//
// So the rule this package enforces is revive's, applied to the files revive
// leaves out: every blank import carries a comment of its own, on its line or
// directly above it. A comment heading a group does not count for the imports
// below it, because gofmt sorts a group alphabetically and will interleave a
// non-blank import through it -- the group comment and the import it explains
// drift apart on the next `goimports` run, with nothing to say they have.
//
// The package carries no runtime code of its own.
package blankimportguard
