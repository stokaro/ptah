// Package rowserrguard hosts the test that keeps rowserrcheck's reach in step
// with the tree it is meant to police.
//
// rowserrcheck reports a `for rows.Next()` loop whose function never asks
// [database/sql.Rows.Err]. A result set that ends early -- a dropped
// connection, a server-side cancellation, a statement killed mid-stream --
// ends that loop exactly as an exhausted one does, so the reader returns the
// rows that arrived and reports success.
//
// What the linter can see is CONFIGURED. It tracks rows by the package that
// DECLARED the Query returning them, and its default list is `database/sql`
// alone. Ptah's schema readers hold a [go.5x5.cz/ptah/internal/sqlrunner.Runner]
// rather than a *sql.DB, so under the default list the linter ran over the
// PostgreSQL reader and reported nothing while eleven of its reads were missing
// the terminal check -- measured on the tree stokaro/ptah#2720 was filed
// against, where it found the two MySQL sites and none of the eleven
// PostgreSQL ones.
//
// So `.golangci.yml` names every package here that declares one. That list is
// hand-written, and a hand-written list is a claim that was true when it was
// written: a new interface with a `QueryContext(...) (*sql.Rows, error)` does
// not fail anything by going unlisted, it makes the linter silent about the
// package holding it. Silence is the state the reported defect was already in,
// which is the whole reason this test exists rather than a comment asking
// people to remember.
//
// It asserts set equality in both directions. A declaring package that is not
// listed is missing coverage; a listed package that no longer declares one is a
// line whose reason has gone, and leaving it reads as coverage of something.
// The test also asserts that the linter is ENABLED, because a complete list
// under a disabled linter is the same silence with more evidence of care.
//
// The package carries no runtime code of its own.
package rowserrguard
