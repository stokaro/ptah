package providerinstall_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
)

func goodRecord() providerinstall.Record {
	return providerinstall.Record{
		Schema: providerinstall.RecordSchema, ProviderID: providerID, Version: "0.2.0",
		MinContract: providerrelease.Contract{Major: 25}, Engines: []string{"ydb"},
		Executable: "0.2.0/linux_amd64/ptah-plugin-ydb", SHA256: strings.Repeat("a", 64),
	}
}

func TestRecord_HappyPath(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	c.Assert(providerinstall.WriteRecord(root, linux, goodRecord()), qt.IsNil)
	record, executable, err := providerinstall.ReadRecord(root, providerID, linux)
	c.Assert(err, qt.IsNil)
	c.Assert(record, qt.DeepEquals, goodRecord())
	c.Assert(executable, qt.Equals, filepath.Join(root, "github.com", "stokaro", "ptah-ydb-plugin", "0.2.0", "linux_amd64", "ptah-plugin-ydb"))
	c.Assert(providerinstall.RecordPath(root, providerID, linux), qt.Equals,
		filepath.Join(root, "github.com", "stokaro", "ptah-ydb-plugin", "installed-linux_amd64.json"))
	// Another platform has its own record, and none was written for it.
	_, _, err = providerinstall.ReadRecord(root, providerID, windows)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	// The write leaves no temporary file behind.
	c.Assert(names(providerinstall.ProviderDir(root, providerID)), qt.DeepEquals, []string{"installed-linux_amd64.json"})
}

func TestReadRecord_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
		wantErr string
	}{
		{"not JSON", "{", `.*unexpected EOF`},
		{"unknown field", `{"schema":1,"extra":true}`, `.*unknown field "extra"`},
		{"another schema", strings.Replace(recordJSON(), `"schema": 1`, `"schema": 2`, 1), `.*schema 2 is not 1.*`},
		{"another provider", strings.Replace(recordJSON(), providerID, "example.org/other", 1), `.*provider_id "example.org/other" is not "github.com/stokaro/ptah-ydb-plugin".*`},
		{"executable outside the provider directory", strings.Replace(recordJSON(), "0.2.0/linux_amd64/ptah-plugin-ydb", "../../../evil", 1), `.*executable "../../../evil" is not a clean path below the provider directory.*`},
		{"absolute executable", strings.Replace(recordJSON(), "0.2.0/linux_amd64/ptah-plugin-ydb", "/usr/bin/evil", 1), `.*is not a clean path below the provider directory.*`},
		{"executable with a backslash", strings.Replace(recordJSON(), "0.2.0/linux_amd64/ptah-plugin-ydb", `0.2.0\\\\evil`, 1), `.*is not a clean path below the provider directory.*`},
		{"digest that is not a SHA-256", strings.Replace(recordJSON(), strings.Repeat("a", 64), "abc", 1), `.*sha256 "abc" is not a SHA-256 digest.*`},
		{"no minimum contract", strings.Replace(recordJSON(), `"major": 25`, `"major": 0`, 1), `.*min_contract.major must be at least 1.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			path := providerinstall.RecordPath(root, providerID, linux)
			c.Assert(os.MkdirAll(filepath.Dir(path), 0o700), qt.IsNil)
			c.Assert(os.WriteFile(path, []byte(test.content), 0o600), qt.IsNil)
			record, executable, err := providerinstall.ReadRecord(root, providerID, linux)
			c.Assert(err, qt.ErrorIs, providerinstall.ErrInvalidRecord)
			c.Assert(err, qt.ErrorMatches, `(?s)`+test.wantErr)
			c.Assert(record, qt.DeepEquals, providerinstall.Record{})
			c.Assert(executable, qt.Equals, "")
		})
	}
}

func TestWriteRecord_RefusesAnInvalidRecord(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	record := goodRecord()
	record.Executable = "../escape"
	c.Assert(providerinstall.WriteRecord(root, linux, record), qt.ErrorIs, providerinstall.ErrInvalidRecord)
	_, _, err := providerinstall.ReadRecord(root, providerID, linux)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

// recordJSON is goodRecord as WriteRecord writes it.
func recordJSON() string {
	return `{
  "schema": 1,
  "provider_id": "github.com/stokaro/ptah-ydb-plugin",
  "version": "0.2.0",
  "min_contract": {"major": 25, "minor": 0},
  "engines": ["ydb"],
  "executable": "0.2.0/linux_amd64/ptah-plugin-ydb",
  "sha256": "` + strings.Repeat("a", 64) + `"
}`
}
