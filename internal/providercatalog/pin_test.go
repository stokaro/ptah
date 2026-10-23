package providercatalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providercatalog"
)

func TestPinsCoverOnlyEnginesWithARelease(t *testing.T) {
	c := qt.New(t)
	ydb, found := providercatalog.LookupPin("ydb")
	c.Assert(found, qt.IsTrue)
	c.Assert(ydb.Validate(), qt.IsNil)
	c.Assert(ydb.Artifacts, qt.HasLen, len(providercatalog.Platforms()))
	for _, engine := range []string{"duckdb", "spanner", "unknown"} {
		_, found := providercatalog.LookupPin(engine)
		c.Assert(found, qt.IsFalse, qt.Commentf("%s", engine))
	}
}

func TestPinsAreCopies(t *testing.T) {
	c := qt.New(t)
	first, _ := providercatalog.LookupPin("ydb")
	first.Artifacts[0].Archive = "changed"
	second, _ := providercatalog.LookupPin("ydb")
	c.Assert(second.Artifacts[0].Archive, qt.Not(qt.Equals), "changed")
}

func TestPinArchiveURL(t *testing.T) {
	c := qt.New(t)
	entry, _ := providercatalog.Lookup("ydb")
	pin, _ := providercatalog.LookupPin("ydb")
	artifact, found := pin.Artifact(providercatalog.Platform{OS: "linux", Arch: "amd64"})
	c.Assert(found, qt.IsTrue)
	official, err := pin.ArchiveURL(entry, "", artifact)
	c.Assert(err, qt.IsNil)
	c.Assert(official, qt.Equals, "https://github.com/stokaro/ptah-ydb-plugin/releases/download/v"+pin.Version+"/"+artifact.Archive)
	mirror, err := pin.ArchiveURL(entry, "https://mirror.example.org/ptah/", artifact)
	c.Assert(err, qt.IsNil)
	c.Assert(mirror, qt.Equals, "https://mirror.example.org/ptah/"+artifact.Archive)
	_, found = pin.Artifact(providercatalog.Platform{OS: "plan9", Arch: "386"})
	c.Assert(found, qt.IsFalse)
}

func TestPinValidate_FailurePath(t *testing.T) {
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	linux := providercatalog.Platform{OS: "linux", Arch: "amd64"}
	valid := func() providercatalog.Pin {
		return providercatalog.Pin{EngineID: "ydb", Version: "1.0.0", Tag: "v1.0.0", Artifacts: []providercatalog.Artifact{{
			Platform: linux, Archive: providercatalog.ArchiveName("ydb", "1.0.0", linux), ArchiveSHA256: digest,
			Executable: "ptah-plugin-ydb", ExecutableSHA256: digest,
		}}}
	}
	c := qt.New(t)
	c.Assert(valid().Validate(), qt.IsNil)
	for _, test := range []struct {
		name   string
		mutate func(*providercatalog.Pin)
		want   string
	}{
		{"unknown engine", func(p *providercatalog.Pin) { p.EngineID = "oracle" }, `unknown engine "oracle"`},
		{"version", func(p *providercatalog.Pin) { p.Version = "latest" }, `not a semantic version`},
		{"tag", func(p *providercatalog.Pin) { p.Tag = "1.0.0" }, `tag "1.0.0" must be "v1.0.0"`},
		{"no artifacts", func(p *providercatalog.Pin) { p.Artifacts = nil }, `no artifacts`},
		{"platform", func(p *providercatalog.Pin) { p.Artifacts[0].Platform.Arch = "mips" }, `unsupported platform linux/mips`},
		{"duplicate platform", func(p *providercatalog.Pin) { p.Artifacts = append(p.Artifacts, p.Artifacts[0]) }, `platform linux/amd64 twice`},
		{"archive name", func(p *providercatalog.Pin) { p.Artifacts[0].Archive = "x.tar.gz" }, `archive "x.tar.gz" must be`},
		{"executable name", func(p *providercatalog.Pin) { p.Artifacts[0].Executable = "x" }, `executable "x" must be`},
		{"archive digest", func(p *providercatalog.Pin) { p.Artifacts[0].ArchiveSHA256 = "ABC" }, `archive digest "ABC"`},
		{"executable digest", func(p *providercatalog.Pin) { p.Artifacts[0].ExecutableSHA256 = digest[1:] }, `executable digest`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			pin := valid()
			test.mutate(&pin)
			c.Assert(pin.Validate(), qt.ErrorMatches, `(?s).*`+test.want+`.*`)
		})
	}
}
