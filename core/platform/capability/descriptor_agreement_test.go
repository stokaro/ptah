package capability_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// agreementSources are the test files whose string literals join the version
// corpus: every banner, version and unreadable string a test of version
// resolution has pinned.
var agreementSources = []string{
	"*_test.go",
	"../../../internal/servertarget/*_test.go",
	"../../../internal/capabilityprobe/*.go",
	"../../../internal/serverprofile/*_test.go",
	"../../../dbschema/*_test.go",
}

// agreementPrefixes wrap a generated version in each shape a resolver reads.
var agreementPrefixes = []string{
	"%s",
	"PostgreSQL %s (Debian)",
	"CockroachDB CCL v%s (x86_64-pc-linux-gnu)",
	"PostgreSQL 15.2-YB-%s-b0 on x86_64-pc-linux-gnu",
	"PostgreSQL 11.2-YB-%s-b0",
	"%s-YB-2025.1.0.0",
	"Cloud Spanner PostgreSQL %s",
	"%s-MariaDB-1:10.11.6+maria~ubu2204",
	"5.5.5-%s-MariaDB",
	"Microsoft SQL Server 2022 (RTM-CU12) - %s (X64)",
	"Microsoft SQL Server %s",
	"Oracle Database 21c Express Edition Release %s - Production",
	"Oracle AI Database 26ai Free Release %s",
	"ClickHouse %s",
	"%s-log",
	"v%s",
	" %s ",
}

// agreementVersions are generated around every step and measured line of
// every ladder.
func agreementVersions() []string {
	majors := []int{0, 2, 3, 4, 5, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 99, 2023, 2024, 2025, 2026, 2027}
	minors := []int{0, 1, 2, 3, 4, 5, 9, 10, 11, 12, 24, 25, 26, 52, 53}
	patches := []int{0, 12, 13, 16, 19, 20}
	versions := make([]string, 0, len(majors)*len(minors)*(len(patches)+1)+len(majors))
	for _, major := range majors {
		versions = append(versions, strconv.Itoa(major))
		for _, minor := range minors {
			versions = append(versions, fmt.Sprintf("%d.%d", major, minor))
			for _, patch := range patches {
				versions = append(versions, fmt.Sprintf("%d.%d.%d", major, minor, patch))
			}
		}
	}
	return versions
}

// agreementLiterals returns every string literal of the agreement sources.
func agreementLiterals(c *qt.C) []string {
	c.Helper()
	var literals []string
	for _, pattern := range agreementSources {
		files, err := filepath.Glob(pattern)
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.Not(qt.HasLen), 0, qt.Commentf("pattern %s", pattern))
		for _, name := range files {
			parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
			c.Assert(err, qt.IsNil)
			ast.Inspect(parsed, func(node ast.Node) bool {
				literal, isLiteral := node.(*ast.BasicLit)
				text, err := strconv.Unquote(literalValue(literal, isLiteral))
				literals = appendWhen(literals, text, isLiteral && literal.Kind == token.STRING && err == nil && len(text) < 400)
				return true
			})
		}
	}
	slices.Sort(literals)
	return slices.Compact(literals)
}

func literalValue(literal *ast.BasicLit, isLiteral bool) string {
	if !isLiteral {
		return `""`
	}
	return literal.Value
}

func appendWhen(values []string, value string, when bool) []string {
	if !when {
		return values
	}
	return append(values, value)
}

// bannerTokens are the product tokens the switch tried, in its order.
var bannerTokens = []string{"cockroachdb", "yugabytedb", "yugabyte", "-yb-", "spanner", "postgres", "mariadb", "sql server", "clickhouse", "oracle"}

// tokenPairs names two products in one string, in both orders: the only
// strings on which the two orders of trying the tokens can differ.
func tokenPairs() []string {
	var pairs []string
	for _, first := range bannerTokens {
		for _, second := range bannerTokens {
			pairs = append(pairs, "x "+first+" 1.2 "+second+" 3.4")
		}
	}
	return pairs
}

// unorderedPairs are the token pairs the two answer differently. The switch
// tried SQL Server before ClickHouse; the resolver tries families in registry
// order, and ClickHouse is registered first. No server's banner names two
// unrelated products.
var unorderedPairs = []string{"x sql server 1.2 clickhouse 3.4", "x clickhouse 1.2 sql server 3.4"}

// TestResolver_AgreesWithTheSwitchItReplaces compares the descriptor-driven
// resolver with the dialect switch it replaces, over every built-in dialect
// and a corpus of version strings: every string literal the resolution tests
// pin, and versions generated around every ladder step in each shape a
// resolver reads.
func TestResolver_AgreesWithTheSwitchItReplaces(t *testing.T) {
	c := qt.New(t)

	resolver, err := capability.NewResolver(platform.Dialects())
	c.Assert(err, qt.IsNil)

	versions := agreementLiterals(c)
	for _, version := range agreementVersions() {
		for _, prefix := range agreementPrefixes {
			versions = append(versions, fmt.Sprintf(prefix, version))
		}
	}
	versions = append(versions, "", "who knows", "trunk", "stable-25-4-1", "stable-26-2", "stable-x-y", "stable-25-4-1-9")
	versions = append(versions, tokenPairs()...)
	slices.Sort(versions)
	versions = slices.Compact(versions)
	dialects := append(capability.DefaultDialects(), "", "unknown", " Postgres ", "MYSQL", "pgx", "mssql", "crdb", "ydbs")
	c.Logf("comparing %d dialect spellings over %d version strings", len(dialects), len(versions))

	var disagreements []string
	for _, dialect := range dialects {
		c.Check(resolver.ForDialect(dialect), qt.DeepEquals, capability.ForDialect(dialect), qt.Commentf("dialect %q", dialect))
		c.Check(resolver.Identifiers(dialect), qt.Equals, capability.Identifiers(dialect), qt.Commentf("dialect %q", dialect))
		for _, version := range versions {
			disagreements = append(disagreements, disagreement(dialect, version,
				resolver.ResolveServerVersion(dialect, version), capability.ResolveServerVersion(dialect, version))...)
		}
	}
	var bannerDisagreements []string
	for _, version := range versions {
		bannerDisagreements = appendWhen(bannerDisagreements, version,
			resolver.BannerPlatform(version) != capability.BannerPlatform(version))
	}
	c.Assert(resolver.Dialects(), qt.DeepEquals, capability.DefaultDialects())
	c.Assert(bannerDisagreements, qt.ContentEquals, unorderedPairs)
	c.Assert(disagreements, qt.HasLen, len(dialects)*len(unorderedPairs), qt.Commentf("%s", strings.Join(disagreements, "\n")))
	c.Assert(len(versions) > 10000, qt.IsTrue, qt.Commentf("the corpus holds %d versions", len(versions)))
}

// disagreement describes a pair the two resolve differently, and returns
// nothing for a pair they agree on. A pair of unordered products reports
// itself without a description.
func disagreement(dialect, version string, got, want capability.VersionResolution) []string {
	if reflect.DeepEqual(got, want) {
		return nil
	}
	if slices.Contains(unorderedPairs, version) {
		return []string{"known"}
	}
	return []string{fmt.Sprintf("dialect %q version %q:\n got %s\nwant %s",
		dialect, version, describeResolution(got), describeResolution(want))}
}

func describeResolution(resolution capability.VersionResolution) string {
	name := "?"
	for _, preset := range capability.NamedPresets() {
		name = presetName(name, preset, resolution.Capabilities)
	}
	return fmt.Sprintf("{preset %s specific %t saturated %t newest %q recognized %t dialect %q}",
		name, resolution.VersionSpecific, resolution.Saturated, resolution.NewestMeasured,
		resolution.Recognized, resolution.ResolvedDialect)
}

func presetName(current string, preset capability.NamedPreset, caps capability.Capabilities) string {
	if current != "?" || !preset.Capabilities.Equal(caps) {
		return current
	}
	return preset.Name
}
