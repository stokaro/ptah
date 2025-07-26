package config_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config"
)

func TestDefaultCompareOptions(t *testing.T) {
	c := qt.New(t)

	opts := config.DefaultCompareOptions()

	c.Assert(opts, qt.IsNotNil)
	c.Assert(opts.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql"})
}

func TestWithIgnoredExtensions(t *testing.T) {
	tests := []struct {
		name       string
		extensions []string
		expected   []string
	}{
		{
			name:       "single extension",
			extensions: []string{"plpgsql"},
			expected:   []string{"plpgsql"},
		},
		{
			name:       "multiple extensions",
			extensions: []string{"plpgsql", "adminpack", "pg_stat_statements"},
			expected:   []string{"plpgsql", "adminpack", "pg_stat_statements"},
		},
		{
			name:       "empty list",
			extensions: []string{},
			expected:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			opts := config.WithIgnoredExtensions(tt.extensions...)
			c.Assert(opts.IgnoredExtensions, qt.DeepEquals, tt.expected)
		})
	}
}

func TestWithAdditionalIgnoredExtensions(t *testing.T) {
	tests := []struct {
		name       string
		additional []string
		expected   []string
	}{
		{
			name:       "add single extension",
			additional: []string{"adminpack"},
			expected:   []string{"plpgsql", "adminpack"},
		},
		{
			name:       "add multiple extensions",
			additional: []string{"adminpack", "pg_stat_statements"},
			expected:   []string{"plpgsql", "adminpack", "pg_stat_statements"},
		},
		{
			name:       "add no extensions",
			additional: []string{},
			expected:   []string{"plpgsql"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			opts := config.WithAdditionalIgnoredExtensions(tt.additional...)
			c.Assert(opts.IgnoredExtensions, qt.DeepEquals, tt.expected)
		})
	}
}

func TestCompareOptions_IsExtensionIgnored(t *testing.T) {
	tests := []struct {
		name              string
		ignoredExtensions []string
		extensionName     string
		expected          bool
	}{
		{
			name:              "extension is ignored",
			ignoredExtensions: []string{"plpgsql", "adminpack"},
			extensionName:     "plpgsql",
			expected:          true,
		},
		{
			name:              "extension is not ignored",
			ignoredExtensions: []string{"plpgsql", "adminpack"},
			extensionName:     "pg_trgm",
			expected:          false,
		},
		{
			name:              "empty ignore list",
			ignoredExtensions: []string{},
			extensionName:     "plpgsql",
			expected:          false,
		},
		{
			name:              "case sensitive matching",
			ignoredExtensions: []string{"plpgsql"},
			extensionName:     "PLPGSQL",
			expected:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			opts := &config.CompareOptions{
				IgnoredExtensions: tt.ignoredExtensions,
			}

			result := opts.IsExtensionIgnored(tt.extensionName)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestCompareOptions_FilterIgnoredExtensions(t *testing.T) {
	tests := []struct {
		name              string
		ignoredExtensions []string
		inputExtensions   []string
		expected          []string
	}{
		{
			name:              "filter some extensions",
			ignoredExtensions: []string{"plpgsql", "adminpack"},
			inputExtensions:   []string{"plpgsql", "pg_trgm", "adminpack", "btree_gin"},
			expected:          []string{"pg_trgm", "btree_gin"},
		},
		{
			name:              "filter all extensions",
			ignoredExtensions: []string{"plpgsql", "pg_trgm"},
			inputExtensions:   []string{"plpgsql", "pg_trgm"},
			expected:          []string{},
		},
		{
			name:              "filter no extensions",
			ignoredExtensions: []string{"adminpack"},
			inputExtensions:   []string{"plpgsql", "pg_trgm", "btree_gin"},
			expected:          []string{"plpgsql", "pg_trgm", "btree_gin"},
		},
		{
			name:              "empty input list",
			ignoredExtensions: []string{"plpgsql"},
			inputExtensions:   []string{},
			expected:          []string{},
		},
		{
			name:              "empty ignore list",
			ignoredExtensions: []string{},
			inputExtensions:   []string{"plpgsql", "pg_trgm"},
			expected:          []string{"plpgsql", "pg_trgm"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			opts := &config.CompareOptions{
				IgnoredExtensions: tt.ignoredExtensions,
			}

			result := opts.FilterIgnoredExtensions(tt.inputExtensions)
			c.Assert(result, qt.DeepEquals, tt.expected)
		})
	}
}

func TestLibraryUsageExamples(t *testing.T) {
	c := qt.New(t)

	t.Run("default usage", func(t *testing.T) {
		// User wants default behavior (ignore plpgsql)
		opts := config.DefaultCompareOptions()
		c.Assert(opts.IsExtensionIgnored("plpgsql"), qt.IsTrue)
		c.Assert(opts.IsExtensionIgnored("pg_trgm"), qt.IsFalse)
	})

	t.Run("custom ignore list", func(t *testing.T) {
		// User wants to ignore specific extensions only
		opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
		c.Assert(opts.IsExtensionIgnored("plpgsql"), qt.IsTrue)
		c.Assert(opts.IsExtensionIgnored("adminpack"), qt.IsTrue)
		c.Assert(opts.IsExtensionIgnored("pg_trgm"), qt.IsFalse)
	})

	t.Run("additional ignored extensions", func(t *testing.T) {
		// User wants defaults plus additional extensions
		opts := config.WithAdditionalIgnoredExtensions("adminpack", "pg_stat_statements")
		c.Assert(opts.IsExtensionIgnored("plpgsql"), qt.IsTrue)            // default
		c.Assert(opts.IsExtensionIgnored("adminpack"), qt.IsTrue)          // additional
		c.Assert(opts.IsExtensionIgnored("pg_stat_statements"), qt.IsTrue) // additional
		c.Assert(opts.IsExtensionIgnored("pg_trgm"), qt.IsFalse)           // not ignored
	})

	t.Run("no ignored extensions", func(t *testing.T) {
		// User wants to manage all extensions
		opts := config.WithIgnoredExtensions()
		c.Assert(opts.IsExtensionIgnored("plpgsql"), qt.IsFalse)
		c.Assert(opts.IsExtensionIgnored("pg_trgm"), qt.IsFalse)
	})
}
