// Package config provides configuration options for the Ptah schema migration system.
//
// This package provides a simple, programmatic API for configuring schema comparison
// and migration behavior when using Ptah as a library. It focuses on providing
// clean Go APIs rather than external configuration file management.
package config

import (
	"slices"

	"go.5x5.cz/ptah/core/platform/identifier"
)

// CompareOptions contains configuration options for schema comparison operations.
// These options control how schema differences are calculated and what elements
// should be ignored during comparison.
type CompareOptions struct {
	// IgnoredExtensions is a list of PostgreSQL extension names that should be
	// ignored during schema migrations. These extensions will:
	// - Never be deleted, even if missing from the target schema
	// - Be excluded from schema diff calculations
	// - Be treated as if they don't exist for comparison purposes
	//
	// Common extensions to ignore include:
	// - plpgsql: Default procedural language, usually pre-installed
	// - adminpack: Administrative functions, often pre-installed
	IgnoredExtensions []string

	// Dialect is the target database dialect ("postgres", "mysql", "mariadb",
	// "clickhouse"). It is optional; when empty the comparison uses
	// dialect-neutral rules. It is currently consulted only to fold
	// referential-action reporting quirks: MariaDB reports an unspecified
	// ON DELETE/ON UPDATE as RESTRICT (MySQL and PostgreSQL report NO ACTION),
	// and InnoDB treats RESTRICT and NO ACTION identically, so for MySQL/MariaDB
	// RESTRICT is folded to NO ACTION to avoid a perpetual drop+add loop on an
	// unchanged foreign key. PostgreSQL distinguishes the two at DDL, so the
	// fold is deliberately NOT applied there.
	Dialect string

	// IdentifierSemantics overrides the dialect's offline identifier rules.
	// Catalog-sensitive callers must provide a complete resolved snapshot;
	// first-party live operations obtain one through schemadiff.
	IdentifierSemantics *identifier.Semantics

	// SkipTableDrops reports that the caller removes every table drop from the
	// diff before it is planned: `diff.skip: [drop_table]` in ptah.yaml, and
	// `diff { skip { drop_table = true } }` in an Atlas project file.
	//
	// It changes NO comparison result. Nothing here reads it except the SQLite
	// virtual-table guard, which refuses a comparison for the statements it
	// predicts -- and a table drop the caller filters out again is a statement
	// nothing will run. Without this, a `schema apply` configured to skip table
	// drops was refused on an fts4 database whose plan the policy had already
	// emptied, and the operator was sent to two dangerous opt-ins to obtain
	// `Schema is synced, no changes to be made.` (stokaro/ptah#1028).
	//
	// It is deliberately narrow. `skip drop_table` does not filter a
	// modification, so the guard's post-comparison half keeps refusing a
	// rebuild of a table Ptah could not classify; only the removal input is
	// discounted.
	SkipTableDrops bool
}

// DefaultCompareOptions returns the default comparison options with sensible defaults.
// The default configuration includes commonly pre-installed PostgreSQL
// extensions that should typically be ignored during migrations.
func DefaultCompareOptions() *CompareOptions {
	return &CompareOptions{
		IgnoredExtensions: []string{
			"plpgsql", // PostgreSQL procedural language - usually pre-installed
		},
	}
}

// WithIgnoredExtensions returns a new CompareOptions with the specified ignored extensions.
// This completely replaces the default ignored extensions list.
//
// Example:
//
//	opts := config.WithIgnoredExtensions("plpgsql", "adminpack", "pg_stat_statements")
func WithIgnoredExtensions(extensions ...string) *CompareOptions {
	return &CompareOptions{
		IgnoredExtensions: extensions,
	}
}

// WithAdditionalIgnoredExtensions returns a new CompareOptions that includes the default
// ignored extensions plus the additional ones specified.
//
// Example:
//
//	opts := config.WithAdditionalIgnoredExtensions("adminpack", "pg_stat_statements")
//	// Result: ["plpgsql", "adminpack", "pg_stat_statements"]
func WithAdditionalIgnoredExtensions(extensions ...string) *CompareOptions {
	defaults := DefaultCompareOptions()
	allExtensions := make([]string, len(defaults.IgnoredExtensions)+len(extensions))
	copy(allExtensions, defaults.IgnoredExtensions)
	copy(allExtensions[len(defaults.IgnoredExtensions):], extensions)

	return &CompareOptions{
		IgnoredExtensions: allExtensions,
	}
}

// IsExtensionIgnored checks if the given extension name should be ignored
// during schema migrations based on the current configuration.
func (c *CompareOptions) IsExtensionIgnored(extensionName string) bool {
	return slices.Contains(c.IgnoredExtensions, extensionName)
}

// FilterIgnoredExtensions removes ignored extensions from the provided slice
// and returns a new slice containing only non-ignored extensions.
// This is useful for filtering extension lists before comparison.
func (c *CompareOptions) FilterIgnoredExtensions(extensions []string) []string {
	filtered := make([]string, 0)
	for _, ext := range extensions {
		if !c.IsExtensionIgnored(ext) {
			filtered = append(filtered, ext)
		}
	}
	return filtered
}
