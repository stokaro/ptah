//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/testutils"
)

// sqlDocumentSpec builds the fixture command whose engine reads its SQL schema
// documents whole, from the SDK version the host pins.
func sqlDocumentSpec(c *qt.C) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "sql-document")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
}

func writeSQLSchema(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	for name, text := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600), qt.IsNil)
	}
}

// TestNativeCLIAppliesASQLSchemaTheProviderReads plans and applies a schema
// directory the provider reads as one load, where the second file alters the
// table the first one creates, and reads the table back.
func TestNativeCLIAppliesASQLSchemaTheProviderReads(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, sqlDocumentSpec(c))
	dir := filepath.Join(filepath.Dir(f.schemaPath), "schema")
	writeSQLSchema(c, dir, map[string]string{
		"01_items.sql": "CREATE TABLE items (id INTEGER NOT NULL);\n",
		"02_label.sql": "ALTER TABLE items ADD label TEXT;\n",
	})

	planned := f.success(c, "plan", "--schema-file", dir, "--output", f.planPath, "--json")
	c.Assert(planned, qt.Contains, `"outcome": "changes"`)
	applied := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
	c.Assert(applied, qt.Contains, `"outcome": "applied"`)

	after := f.success(c, "inspect", "--format", "json")
	c.Assert(after, qt.Contains, `"Name": "items"`)
	c.Assert(after, qt.Contains, `"Name": "label"`)
	c.Assert(f.success(c, "plan", "--schema-file", dir, "--output", f.planPath, "--json"), qt.Contains, `"outcome": "no-changes"`)
}

// TestNativeCLIRefusesASQLSchemaTheProviderRefuses shows the provider deciding:
// the shared grammar reads a GRANT, and this provider's grammar refuses it. The
// first error is printed with its position, the JSON report carries every
// diagnostic, and nothing is planned or written.
func TestNativeCLIRefusesASQLSchemaTheProviderRefuses(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, sqlDocumentSpec(c))
	schema := filepath.Join(filepath.Dir(f.schemaPath), "schema.sql")
	writeSQLSchema(c, filepath.Dir(schema), map[string]string{"schema.sql": "CREATE TABLE items (id INTEGER NOT NULL);\nGRANT SELECT ON items TO reader;\n"})

	out, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", schema, "--output", f.planPath, "--json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Contains, "read SQL schema file "+schema+": unsupported statement GRANT at position 42 (line 2, column 1)")
	var report struct {
		Outcome           string                                `json:"outcome"`
		SchemaDiagnostics []providerdiagnostic.SchemaDiagnostic `json:"schema_diagnostics"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(report.Outcome, qt.Equals, "failed")
	c.Assert(report.SchemaDiagnostics, qt.DeepEquals, []providerdiagnostic.SchemaDiagnostic{{
		Code: "unsupported", Severity: "error", Path: schema, Message: "unsupported statement GRANT",
		Start: providerdiagnostic.SchemaPosition{Offset: 42, Line: 2, Column: 1}, End: providerdiagnostic.SchemaPosition{Offset: 47, Line: 2, Column: 6},
	}})
	_, err := os.Stat(f.planPath)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "items")
}

// TestNativeCLIRefusesAReadingItCannotReportWhole refuses a reading the host
// could only use by dropping part of it: a reply the SDK's validation refuses,
// and a complete reading with a warning, which the host has no channel for.
func TestNativeCLIRefusesAReadingItCannotReportWhole(t *testing.T) {
	spec := sqlDocumentSpec(qt.New(t))
	for _, test := range []struct {
		name, text, want string
	}{
		{"reply without a trace", "CREATE TABLE malformed (id INTEGER NOT NULL);\n", "provider read SQL schema failed"},
		{"reading with a warning", "CREATE TABLE warned (id INTEGER NOT NULL);\n", "the provider reported a warning, which Ptah does not report for a SQL schema reading yet; the first: fixture warns about table warned at position 0 (line 1, column 1)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, spec)
			schema := filepath.Join(filepath.Dir(f.schemaPath), "schema.sql")
			writeSQLSchema(c, filepath.Dir(schema), map[string]string{"schema.sql": test.text})

			_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", schema, "--output", f.planPath, "--json")

			c.Assert(code, qt.Equals, 2)
			c.Assert(diagnostics, qt.Contains, test.want)
			_, err := os.Stat(f.planPath)
			c.Assert(os.IsNotExist(err), qt.IsTrue)
		})
	}
}

// TestProviderReadsASQLLoadIntoTheHostModel reads a load through the host's
// SQL reader and the provider's grammar: the provider's tables, its own
// retention object normalized by the provider, each file's declarations, the
// claim the load makes, which the header limits, and the common header kinds
// the reader adds itself. A document read alone, with no path, is a load of
// one.
func TestProviderReadsASQLLoadIntoTheHostModel(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "sql-document")
	c.Assert(sqlschema.ReadsAsLoad("fixture", runtime.SQL()), qt.IsTrue)
	files := []sqlext.SourceFile{
		{Path: "one.sql", Text: "-- ptah:not-described retention \"kept\"\n-- ptah:not-described view\nCREATE TABLE items (id INTEGER PRIMARY KEY);\nCREATE RETENTION hourly SECONDS 3600;\n"},
		{Path: "two.sql", Text: "ALTER TABLE items ADD label TEXT NOT NULL;\n"},
	}

	load, err := sqlschema.ReadLoad(c.Context(), sqlext.GrammarRequest{Target: "fixture", Files: files},
		sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary()})

	c.Assert(err, qt.IsNil)
	c.Assert(load.Database.Tables, qt.HasLen, 1)
	c.Assert(load.Database.Tables[0].Name, qt.Equals, "items")
	c.Assert(load.Database.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(load.Database.Fields, qt.HasLen, 2)
	c.Assert(load.Database.Fields[1].Name, qt.Equals, "label")
	c.Assert(load.Database.Fields[1].Nullable, qt.IsFalse)
	c.Assert(load.Files, qt.DeepEquals, []sqlext.FileDeclarations{
		{Path: "one.sql", Objects: []sqlext.Declaration{{Kind: "table", Name: "items"}, {Kind: "retention", Name: "hourly"}}, Tables: []sqlext.TableStatement{{Name: "items"}}},
		{Path: "two.sql"},
	})
	refs := load.Database.FeatureObjects.Refs()
	c.Assert(refs, qt.HasLen, 1)
	c.Assert(refs[0].Name.Source, qt.Equals, "hourly")
	claim := load.Database.FeatureCoverage
	c.Assert(claim.Representation(), qt.Equals, schemaext.Desired)
	c.Assert(claim.KindRecords(), qt.HasLen, 1)
	c.Assert(claim.SubjectRecords(), qt.HasLen, 1)
	c.Assert(claim.SubjectRecords()[0].Subject.Name.Source, qt.Equals, "kept")
	c.Assert(claim.SubjectRecords()[0].Knowledge.State, qt.Equals, schemaext.Uninspected)
	c.Assert(load.Database.NotDescribed.Describes(coverage.View), qt.IsFalse)
	alone, statements, err := sqlschema.ReadWith(c.Context(), []byte("CREATE TABLE items (id INTEGER);\n"), "fixture", nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.IsNil)
	c.Assert(alone.Tables, qt.HasLen, 1)
}

// TestProviderSQLLoadRefusalNamesTheDocument refuses a load the provider
// refuses: the refusal names the second document, classifies an unsupported
// construct, and carries the provider's diagnostics.
func TestProviderSQLLoadRefusalNamesTheDocument(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "sql-document")
	files := []sqlext.SourceFile{{Path: "one.sql", Text: "CREATE TABLE items (id INTEGER);\n"}, {Path: "two.sql", Text: "CREATE INDEX i ON items (id);\n"}}

	load, err := sqlschema.ReadLoad(c.Context(), sqlext.GrammarRequest{Target: "fixture", Files: files}, sqlschema.Options{Owners: runtime.SQL()})

	c.Assert(err, qt.ErrorMatches, `two.sql: unsupported statement CREATE at position 0 \(line 1, column 1\)`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	refusal, ok := errors.AsType[*providerdiagnostic.SchemaRefusal](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(refusal.Diagnostics, qt.HasLen, 1)
	c.Assert(refusal.Diagnostics[0].Path, qt.Equals, "two.sql")
	c.Assert(load, qt.DeepEquals, sqlschema.Load{})
}

// TestMissingProviderRefusesASQLSchemaBeforeReadingIt refuses a SQL desired
// schema for an official engine without its provider with provider_missing,
// and never reads the file with another grammar.
func TestMissingProviderRefusesASQLSchemaBeforeReadingIt(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		for _, command := range []struct {
			verb string
			args []string
		}{
			{"plan", []string{"--json"}}, {"apply", []string{"--auto-approve", "--json"}},
		} {
			t.Run(entry.EngineID+"/"+command.verb, func(t *testing.T) {
				c := qt.New(t)
				schema := filepath.Join(c.TempDir(), "schema.sql")
				writeSQLSchema(c, filepath.Dir(schema), map[string]string{"schema.sql": "$prefix = '/app';\nUPSERT INTO items (id) VALUES (1);\n"})

				args := append([]string{"schema", command.verb, "--provider-engine", entry.EngineID, "--schema-file", schema}, command.args...)
				out, stderr, code := executeProviderCLI(c.Context(), args)

				c.Assert(code, qt.Equals, 2, qt.Commentf("%s", stderr))
				var report struct {
					Diagnostic providerdiagnostic.Diagnostic `json:"provider_diagnostic"`
				}
				c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
				c.Assert(report.Diagnostic.Code, qt.Equals, "provider_missing")
				c.Assert(report.Diagnostic.EngineID, qt.Equals, entry.EngineID)
				c.Assert(stderr, qt.Not(qt.Contains), "read SQL schema file")
			})
		}
	}
}
