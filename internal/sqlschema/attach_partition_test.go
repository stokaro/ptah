package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
)

// dumpOfPartitions is what pg_dump 18 writes for a range-partitioned table with a
// primary key and an index, and two partitions: the restrict lines, each
// partition's own CREATE TABLE, the attachment, each partition's copy of the key
// and of the index, and the attachment of each copy.
const dumpOfPartitions = `\restrict bOMhEXnexOW3tvDg5NTTwP1FGM9GPjnB7sgGYWvMBgICQ0H5YUab8jmZmEQvvvb

CREATE TABLE public.events (
    id bigint NOT NULL,
    at date NOT NULL,
    note text DEFAULT 'x'::text
)
PARTITION BY RANGE (at);
CREATE TABLE public.events_2026 (
    id bigint CONSTRAINT events_id_not_null NOT NULL,
    at date CONSTRAINT events_at_not_null NOT NULL,
    note text DEFAULT 'x'::text
);
CREATE TABLE public.events_rest (
    id bigint CONSTRAINT events_id_not_null NOT NULL,
    at date CONSTRAINT events_at_not_null NOT NULL,
    note text DEFAULT 'x'::text
);
ALTER TABLE ONLY public.events ATTACH PARTITION public.events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ALTER TABLE ONLY public.events ATTACH PARTITION public.events_rest DEFAULT;
ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (id, at);
ALTER TABLE ONLY public.events_2026
    ADD CONSTRAINT events_2026_pkey PRIMARY KEY (id, at);
ALTER TABLE ONLY public.events_rest
    ADD CONSTRAINT events_rest_pkey PRIMARY KEY (id, at);
CREATE INDEX events_note ON ONLY public.events USING btree (note);
CREATE INDEX events_2026_note_idx ON public.events_2026 USING btree (note);
CREATE INDEX events_rest_note_idx ON public.events_rest USING btree (note);
ALTER INDEX public.events_note ATTACH PARTITION public.events_2026_note_idx;
ALTER INDEX public.events_pkey ATTACH PARTITION public.events_2026_pkey;
ALTER INDEX public.events_note ATTACH PARTITION public.events_rest_note_idx;
ALTER INDEX public.events_pkey ATTACH PARTITION public.events_rest_pkey;

\unrestrict bOMhEXnexOW3tvDg5NTTwP1FGM9GPjnB7sgGYWvMBgICQ0H5YUab8jmZmEQvvvb
`

// A dump reads as the declaration PARTITION OF makes: each partition has its parent
// and bound and no columns, keys or indexes of its own, and the parent keeps its key
// and index.
func TestReadWith_AttachPartition_HappyPath(t *testing.T) {
	c := qt.New(t)

	database, err := readWithBuiltinOwners(c, dumpOfPartitions, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(*memberOfTable(c, database, "events_2026"), qt.DeepEquals,
		pgtable.PartitionMember{ParentSchema: "public", Parent: "events", Bound: "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"})
	c.Assert(*memberOfTable(c, database, "events_rest"), qt.DeepEquals,
		pgtable.PartitionMember{ParentSchema: "public", Parent: "events", Bound: "DEFAULT"})
	c.Assert(attachedColumnsOf(database, "events_2026"), qt.HasLen, 0)
	c.Assert(attachedColumnsOf(database, "events_rest"), qt.HasLen, 0)
	c.Assert(attachedColumnsOf(database, "events"), qt.DeepEquals, []string{"id", "at", "note"})
	c.Assert(attachedIndexNames(database), qt.DeepEquals, []string{"events_note"})
	events := database.Tables[slicesIndexTable(database.Tables, "events")]
	c.Assert(events.PrimaryKeyName, qt.Equals, "events_pkey")
	c.Assert(database.Tables[slicesIndexTable(database.Tables, "events_2026")].PrimaryKey, qt.HasLen, 0)
}

// A dump in the shape a PostgreSQL 15 release writes, with no restrict lines and
// no NOT NULL names, reads on YugabyteDB as the PostgreSQL dump reads.
func TestReadWith_AttachPartitionOnYugabyteDB_HappyPath(t *testing.T) {
	c := qt.New(t)
	dump := strings.NewReplacer("CONSTRAINT events_id_not_null ", "", "CONSTRAINT events_at_not_null ", "").Replace(dumpOfPartitions)
	dump = strings.Join(slices.DeleteFunc(strings.Split(dump, "\n"), func(line string) bool { return strings.HasPrefix(line, `\`) }), "\n")

	database, err := readWithBuiltinOwners(c, dump, "yugabytedb")

	c.Assert(err, qt.IsNil)
	c.Assert(memberOfTable(c, database, "events_2026").Parent, qt.Equals, "events")
	c.Assert(attachedColumnsOf(database, "events_2026"), qt.HasLen, 0)
	c.Assert(attachedIndexNames(database), qt.DeepEquals, []string{"events_note"})
}

// A partition attached by itself, with no index of the parent to copy, reads the
// same way on both targets that take the statement.
func TestReadWith_AttachPartitionAlone_HappyPath(t *testing.T) {
	for _, target := range []string{"postgres", "yugabytedb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			database, err := readWithBuiltinOwners(c, `CREATE TABLE regions (code text NOT NULL) PARTITION BY LIST (code);
CREATE TABLE "Regions EU" (code text NOT NULL);
ALTER TABLE regions ATTACH PARTITION "Regions EU" FOR VALUES IN ('de', 'fr');`, target)

			c.Assert(err, qt.IsNil)
			c.Assert(*memberOfTable(c, database, "Regions EU"), qt.DeepEquals,
				pgtable.PartitionMember{Parent: "regions", Bound: "FOR VALUES IN ('de', 'fr')"})
			c.Assert(attachedColumnsOf(database, "Regions EU"), qt.HasLen, 0)
		})
	}
}

// Each refusal names what the declarations could not hold.
func TestReadWith_AttachPartition_FailurePath(t *testing.T) {
	const parent = "CREATE TABLE p (id int NOT NULL, note text) PARTITION BY RANGE (id);\n"
	for _, test := range []struct {
		name, source, wantErr string
	}{
		{
			name:    "a column the parent lacks",
			source:  parent + "CREATE TABLE c (id int NOT NULL, note text, extra int);\nALTER TABLE p ATTACH PARTITION c DEFAULT;",
			wantErr: `the schema model has no place for this statement: partition c of p declares column extra, which its parent does not; Ptah takes a partition's columns from its parent`,
		},
		{
			name:    "a column of another type",
			source:  parent + "CREATE TABLE c (id bigint NOT NULL, note text);\nALTER TABLE p ATTACH PARTITION c DEFAULT;",
			wantErr: `the schema model has no place for this statement: partition c of p declares column id as bigint, where its parent declares int; .*`,
		},
		{
			name:    "a column of the parent left out",
			source:  parent + "CREATE TABLE c (id int NOT NULL);\nALTER TABLE p ATTACH PARTITION c DEFAULT;",
			wantErr: `the schema model has no place for this statement: partition c of p does not declare column note of its parent; .*`,
		},
		{
			name:    "another default",
			source:  parent + "CREATE TABLE c (id int NOT NULL, note text DEFAULT 'x');\nALTER TABLE p ATTACH PARTITION c DEFAULT;",
			wantErr: `the schema model has no place for this statement: partition c of p declares column note with another default than its parent; .*`,
		},
		{
			name:    "a partition nothing declares",
			source:  parent + "ALTER TABLE p ATTACH PARTITION c DEFAULT;",
			wantErr: `line 2, column 1: ALTER TABLE p ATTACH PARTITION c DEFAULT: table "c" is not declared in this schema`,
		},
		{
			name:    "a table already a partition",
			source:  parent + "CREATE TABLE c PARTITION OF p DEFAULT;\nALTER TABLE p ATTACH PARTITION c FOR VALUES FROM (1) TO (2);",
			wantErr: `line 3, column 1: ALTER TABLE p ATTACH PARTITION c FOR VALUES FROM \(1\) TO \(2\): duplicate feature identity: facet "ptah.run/postgres/table-partition-member"`,
		},
		{
			name:    "something after the bound",
			source:  parent + "CREATE TABLE c (id int NOT NULL, note text);\nALTER TABLE p ATTACH PARTITION c DEFAULT, ADD COLUMN x int;",
			wantErr: `line 3, column 1: invalid feature value: PostgreSQL partition: , ADD COLUMN x int after the bound of ATTACH PARTITION is not read`,
		},
		{
			name: "an index copy of a table that is not a partition",
			source: parent + "CREATE INDEX p_note ON p (note);\nCREATE TABLE other (id int NOT NULL, note text);\n" +
				"CREATE INDEX other_note ON other (note);\nALTER INDEX p_note ATTACH PARTITION other_note;",
			wantErr: `line 5, column 1: ALTER INDEX p_note ATTACH PARTITION other_note: table other is not a partition of p`,
		},
		{
			name: "an index ON ONLY a partition leaves without a copy",
			source: parent + "CREATE TABLE c PARTITION OF p DEFAULT;\n" +
				"CREATE INDEX p_note ON ONLY p (note);",
			wantErr: `the schema model has no place for this statement: CREATE INDEX p_note ON ONLY p: partition c attaches no copy of the index, so the server leaves it invalid; .*`,
		},
		{
			name:    "an index copy nothing declares",
			source:  parent + "CREATE INDEX p_note ON p (note);\nALTER INDEX p_note ATTACH PARTITION c_note;",
			wantErr: `line 3, column 1: ALTER INDEX p_note ATTACH PARTITION c_note names an index this schema does not declare`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := readWithBuiltinOwners(c, test.source, "postgres")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

func attachedColumnsOf(database schemamodel.Database, table string) []string {
	position := slicesIndexTable(database.Tables, table)
	var names []string
	for _, field := range database.Fields {
		if position >= 0 && field.StructName == database.Tables[position].StructName {
			names = append(names, field.Name)
		}
	}
	return names
}

func attachedIndexNames(database schemamodel.Database) []string {
	var names []string
	for _, index := range database.Indexes {
		names = append(names, index.Name)
	}
	return names
}
