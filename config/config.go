// Package config provides configuration options for the Ptah schema migration system.
//
// This package provides a simple, programmatic API for configuring schema comparison
// and migration behavior when using Ptah as a library. It focuses on providing
// clean Go APIs rather than external configuration file management.
package config

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
	for _, ignored := range c.IgnoredExtensions {
		if ignored == extensionName {
			return true
		}
	}
	return false
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
