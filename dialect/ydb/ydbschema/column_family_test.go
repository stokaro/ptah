package ydbschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// A value encodes its families sorted by name and each family's columns
// sorted, so two values that list the same families in another order encode
// to the same bytes and compare equal, and decoding gives the value back.
func TestColumnFamilies_EncodeCanonically(t *testing.T) {
	c := qt.New(t)
	written := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
		{Name: "warm", Columns: []string{"b", "a"}},
		{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory"},
	}}
	sorted := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
		{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory"},
		{Name: "warm", Columns: []string{"a", "b"}},
	}}
	codec := ydbschema.ColumnFamiliesCodecs()[0]

	encoded := must.Must(codec.Encode(written))
	decoded := must.Must(codec.Decode(encoded))

	c.Assert(string(encoded), qt.Equals,
		`{"families":[{"name":"cold","data":"hdd","compression":"lz4","cache_mode":"in_memory"},{"name":"warm","columns":["a","b"]}]}`)
	c.Assert(string(must.Must(codec.Encode(sorted))), qt.Equals, string(encoded))
	c.Assert(written.Equal(sorted), qt.IsTrue)
	c.Assert(decoded.(*ydbschema.DesiredColumnFamilies).Equal(written), qt.IsTrue)
	c.Assert(string(must.Must(codec.Encode(&ydbschema.DesiredColumnFamilies{}))), qt.Equals, `{"families":[]}`)
}

// keep_in_memory is a read's: an observation encodes it, and a declaration
// adopted from an observation keeps it.
func TestColumnFamilies_KeepInMemoryTravelsWithTheRead(t *testing.T) {
	c := qt.New(t)
	observed := &ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}}}

	encoded := must.Must(ydbschema.ColumnFamiliesCodecs()[1].Encode(observed))
	declared := observed.Desired()

	c.Assert(string(encoded), qt.Equals, `{"families":[{"name":"default","compression":"lz4","keep_in_memory":true}]}`)
	c.Assert(declared.Families, qt.DeepEquals, observed.Families)
	c.Assert(must.Must(declared.Observed()).Equal(observed), qt.IsTrue)
	c.Assert(observed.Equal(&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}}}), qt.IsFalse)
	c.Assert(declared.Equal(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}}}), qt.IsFalse)
}

// The validators refuse families YDB cannot hold, in both representations.
func TestValidateColumnFamilies_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		families []ydbschema.ColumnFamily
		wantErr  string
	}{
		{"no name", []ydbschema.ColumnFamily{{Name: " "}}, `.*a column family needs a name`},
		{"a name twice", []ydbschema.ColumnFamily{{Name: "cold"}, {Name: "cold"}}, `.*column family "cold" is listed twice`},
		{"zstd", []ydbschema.ColumnFamily{{Name: "cold", Compression: "zstd"}}, `.*has compression "zstd", which is not off or lz4`},
		{"a compression in capitals", []ydbschema.ColumnFamily{{Name: "cold", Compression: "LZ4"}}, `.*has compression "LZ4".*`},
		{"an unknown cache mode", []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "ram"}}, `.*has cache mode "ram", which is not regular or in_memory`},
		{"a blank pool", []ydbschema.ColumnFamily{{Name: "cold", Data: " "}}, `.*names an empty storage pool kind`},
		{"columns in the default family", []ydbschema.ColumnFamily{{Name: "default", Columns: []string{"a"}}}, `.*the default column family lists columns.*`},
		{"an empty column", []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{""}}}, `.*names an empty column`},
		{"a column twice", []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a", "a"}}}, `.*names column "a" twice`},
		{"a column in two families", []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}, {Name: "warm", Columns: []string{"a"}}},
			`.*column "a" is in two column families, "cold" and "warm"`},
		{"a NUL in a name", []ydbschema.ColumnFamily{{Name: "co\x00ld"}}, `.*column family name contains a NUL byte`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			desired := ydbschema.ValidateDesiredColumnFamilies(&ydbschema.DesiredColumnFamilies{Families: test.families})
			observed := ydbschema.ValidateObservedColumnFamilies(&ydbschema.ObservedColumnFamilies{Families: test.families})

			c.Assert(desired, qt.ErrorMatches, test.wantErr)
			c.Assert(desired, qt.ErrorIs, schemaext.ErrInvalidValue)
			var invalid *schemaext.InvalidModelError
			c.Assert(desired, qt.ErrorAs, &invalid)
			c.Assert(invalid.Kind, qt.Equals, ydbschema.ColumnFamiliesKind)
			c.Assert(observed, qt.ErrorMatches, test.wantErr)
		})
	}
	c := qt.New(t)
	c.Assert(ydbschema.ValidateDesiredColumnFamilies(nil), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(ydbschema.ValidateObservedColumnFamilies(nil), qt.ErrorIs, schemaext.ErrInvalidValue)
}

// The codecs accept only the spelling the encoder writes: a null, a key in
// another case, an unknown key, an omitted value written out and a value the
// validators refuse are all refused with a typed error.
func TestColumnFamiliesCodecs_RefuseMalformedValues(t *testing.T) {
	for _, wire := range []string{
		`null`,
		`{}`,
		`{"families":null}`,
		`{"Families":[]}`,
		`{"families":[],"extra":1}`,
		`{"families":[{"name":"cold","data":""}]}`,
		`{"families":[{"name":"cold","columns":[]}]}`,
		`{"families":[{"name":"cold","keep_in_memory":false}]}`,
		`{"families":[{"name":"cold","size":1}]}`,
		`{"families":[{"name":"cold","compression":"zstd"}]}`,
	} {
		t.Run(wire, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := ydbschema.ColumnFamiliesCodecs()[0].Decode(json.RawMessage(wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			var invalid *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &invalid)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestColumnFamiliesCoverage_RecordsTheClaimItIsGiven pins coverage: the claim
// for every table, a subject's own knowledge over it, and no coverage for a
// representation that is not a schema's.
func TestColumnFamiliesCoverage_RecordsTheClaimItIsGiven(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))

	coverage, err := ydbschema.ColumnFamiliesCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.ColumnFamiliesKind, Subject: builder.TableParts("app", "events"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
	_, refused := ydbschema.ColumnFamiliesCoverage(schemaext.Change, schemaext.Knowledge{State: schemaext.Complete}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Representation(), qt.Equals, schemaext.Observed)
	c.Assert(coverage.Lookup(ydbschema.ColumnFamiliesKind, builder.TableParts("app", "events")).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(ydbschema.ColumnFamiliesKind, builder.TableParts("app", "other")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(refused, qt.ErrorIs, schemaext.ErrInvalidValue)
}
