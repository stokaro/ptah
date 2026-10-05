package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/internal/schemafile"
)

// Only `.sql` has CREATE VIRTUAL TABLE, so silence about a live SQLite virtual
// table is intent there and is not intent in HCL, where the document could not
// have named it (stokaro/ptah#1028).
//
// The record now carries why. "Unsupported, derived from another fact Ptah
// holds" is the document's FORMAT speaking, not a read that failed and not a
// selection the user wrote, and a user told only that virtual tables were "not
// described" cannot tell those three apart (stokaro/ptah#1346).
func TestAFormatThatCannotExpressAKindSaysSoAndSaysWhy(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		contents string
		want     []coverage.Object
	}{
		{
			// HCL has the synonym and extended_property blocks
			// (stokaro/ptah#1031), so it records neither -- and it still cannot
			// name a virtual table, a table's row deletion policy, a changefeed,
			// a YDB topic, a column family, or a YDB async replication or
			// transfer, secret, external object, resource pool or classifier.
			name: "HCL cannot name a virtual table, a TTL, a changefeed, a topic, a column family, a " +
				"replication, transfer, secret, external object, resource pool or classifier",
			file:     "schema.hcl",
			contents: "schema \"main\" {\n}\n",
			want: unsupportedRecords(coverage.Changefeed, coverage.ColumnFamily, coverage.ColumnTable, coverage.ExternalDataSource, coverage.ExternalTable,
				coverage.Replication, coverage.ResourcePool, coverage.ResourcePoolClassifier, coverage.Secret, coverage.Topic,
				coverage.Transfer, coverage.TTL, coverage.VirtualTable),
		},
		{
			// The control on the virtual table. A `.sql` document CAN name one,
			// so it carries no record for that kind -- without this row a
			// loader that recorded the limit for every format would pass the
			// row above. It is also the control on the two SQL Server kinds
			// going the other way: `.sql` still expresses neither, so a loader
			// that dropped the record along with HCL's would fail here.
			name:     "SQL can",
			file:     "schema.sql",
			contents: "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
			want: unsupportedRecords(
				coverage.ContinuousAggregate, coverage.ExtendedProperty, coverage.ExternalDataSource, coverage.ExternalTable,
				coverage.Hypertable, coverage.Replication, coverage.ResourcePool, coverage.ResourcePoolClassifier, coverage.Secret, coverage.Synonym, coverage.Topic, coverage.Transfer),
		},
		{
			// YAML expresses the fewest families of the three, and the row is
			// what keeps the HCL narrowing from being read as "the loader no
			// longer records these kinds anywhere". It has keys for a row
			// deletion policy, a changefeed and a column family, as `.sql` has
			// Spanner's policy clause, so both rows are the control on the TTL,
			// changefeed and column family records HCL and DBML carry. It is
			// also the control on the topic, the replication and the
			// transfer, secret and external objects: YAML has a key for each, so a loader that recorded
			// them for every format fails here.
			name:     "YAML cannot name nine families",
			file:     "schema.yaml",
			contents: "tables:\n  users:\n    fields:\n      id:\n        type: INTEGER\n",
			want: unsupportedRecords(
				coverage.Composite, coverage.ContinuousAggregate, coverage.Domain,
				coverage.ExtendedProperty, coverage.Hypertable, coverage.Range,
				coverage.Sequence, coverage.Synonym, coverage.VirtualTable),
		},
		{
			// DBML declares the widest boundary of any format here, and that is
			// the point rather than an accident: it describes tables, columns,
			// enums, indexes and references and has no syntax for anything
			// else. Extension, Policy and Role appear in no other row, which is
			// what makes this one the exhaustive boundary #2065 asks for --
			// and coverage.Schema is absent from it because DBML qualifies a
			// name with a schema.
			name:     "DBML cannot name twenty-three families",
			file:     "schema.dbml",
			contents: "Table users {\n  id integer [pk]\n}\n",
			want: unsupportedRecords(
				coverage.Changefeed, coverage.ColumnFamily, coverage.ColumnTable, coverage.Composite, coverage.ContinuousAggregate,
				coverage.Domain, coverage.ExtendedProperty, coverage.Extension, coverage.ExternalDataSource, coverage.ExternalTable, coverage.Hypertable,
				coverage.Policy, coverage.Range, coverage.Replication, coverage.ResourcePool, coverage.ResourcePoolClassifier, coverage.Role, coverage.Secret, coverage.Sequence,
				coverage.Synonym, coverage.Topic, coverage.Transfer, coverage.TTL, coverage.VirtualTable),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.contents), 0o600), qt.IsNil)

			database, err := schemafile.LoadPath(path, schemafile.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(database.NotDescribed.Objects, qt.DeepEquals, test.want)
		})
	}
}

// unsupportedRecords is the shape every limit this loader records has: the
// format cannot express the family, which follows from which format it is.
// Listing the kinds rather than asserting a length is what makes a limit added
// to the wrong branch visible.
func unsupportedRecords(kinds ...coverage.Kind) []coverage.Object {
	records := make([]coverage.Object, 0, len(kinds))
	for _, kind := range kinds {
		records = append(records, coverage.Object{
			Kind:       kind,
			Reason:     coverage.Unsupported,
			Provenance: coverage.DerivedFromFact,
		})
	}
	return records
}
