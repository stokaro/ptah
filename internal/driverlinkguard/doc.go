// Package driverlinkguard hosts the test that keeps a database driver out of
// the paths that turn Go structs into SQL.
//
// Rendering a schema and planning a migration are decisions about text. They
// read no catalog and open no connection, so an embedder who only wants DDL
// out of struct tags should link no driver to get it. That is not something the
// compiler enforces on its own: one import of a package that parses a DSN puts
// the ClickHouse, pgx and go-mssqldb wire protocols under every caller of the
// rendering path, and nothing about the build goes red.
//
// The test measures the invariant from the resolved import graph rather than
// from the source text, the way [ptah.run/internal/cmd/boundaries] does:
// an import edge the compiler resolved cannot be talked out of, while a search
// for driver spellings answers wrongly in both directions -- a doc comment
// naming a driver looks like an import, and an indirect edge three packages
// down looks like nothing at all.
//
// It answers in two ways on purpose. The named-driver check states the
// invariant §6 of stokaro/ptah#2246 asks for and names the module in its
// failure. The pinned-module-set check catches the driver nobody thought to
// name, and any other dependency that reaches a pure path: a driver added to
// go.mod tomorrow is not in the first list, but it is not in the second either.
//
// The package carries no runtime code of its own.
package driverlinkguard
