package ydb_test

import (
	"context"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// describedSequence is what DescribeTable reports for the sequence of Serial
// column column of an Int64 type nobody altered, measured on 25.1.4.7 and
// 26.2.1.14, data type included: field 9, which the pinned protocol buffers
// do not model, holding Int64 for every width.
func describedSequence(column string, maximum int64) *Ydb_Table.SequenceDescription {
	sequence := &Ydb_Table.SequenceDescription{
		Name: new("_serial_column_" + column), MinValue: new(int64(1)), MaxValue: new(maximum),
		StartValue: new(int64(1)), Cache: new(uint64(1)), Increment: new(int64(1)), Cycle: new(false),
	}
	return withDataType(sequence, Ydb.Type_INT64)
}

// withDataType appends field 9, the sequence's data type, as the server sends
// it.
func withDataType(sequence *Ydb_Table.SequenceDescription, id Ydb.Type_PrimitiveTypeId) *Ydb_Table.SequenceDescription {
	encoded := must.Must(proto.Marshal(&Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: id}}))
	unknown := protowire.AppendTag(nil, 9, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, encoded)
	sequence.ProtoReflect().SetUnknown(append(sequence.ProtoReflect().GetUnknown(), unknown...))
	return sequence
}

// serialTable is a table whose key id is a Serial column of type id, filled
// from sequence.
func serialTable(id Ydb.Type_PrimitiveTypeId, sequence *Ydb_Table.SequenceDescription) fakeSource {
	described := plainTable()
	described.Columns[0] = &Ydb_Table.ColumnMeta{Name: "id", Type: primitive(id),
		DefaultValue: &Ydb_Table.ColumnMeta_FromSequence{FromSequence: sequence}}
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
	}
}

// TestReader_Sequence_HappyPath reads a Serial's sequence onto its column: the
// start and the increment where either is not 1, and the value of a restart,
// which DescribeTable reports only for a sequence some ALTER SEQUENCE
// restarted. A maximum is the column type's, or the Int64 maximum every ALTER
// SEQUENCE leaves on a 16-bit or 32-bit Serial.
func TestReader_Sequence_HappyPath(t *testing.T) {
	altered := describedSequence("id", math.MaxInt64)
	altered.StartValue, altered.Increment = new(int64(100)), new(int64(5))
	restarted := describedSequence("id", math.MaxInt64)
	restarted.StartValue = new(int64(100))
	restarted.SetVal = &Ydb_Table.SequenceDescription_SetVal{NextValue: new(int64(100)), NextUsed: new(false)}
	for _, test := range []struct {
		name   string
		source fakeSource
		want   catalog.Column
	}{
		{
			name:   "a BigSerial nobody altered",
			source: serialTable(Ydb.Type_INT64, describedSequence("id", math.MaxInt64)),
			want:   catalog.Column{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true, IsAutoIncrement: true},
		},
		{
			name:   "a Serial nobody altered",
			source: serialTable(Ydb.Type_INT32, describedSequence("id", math.MaxInt32)),
			want:   catalog.Column{Name: "id", DataType: "Int32", ColumnType: "Int32", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true, IsAutoIncrement: true},
		},
		{
			name:   "a SmallSerial whose maximum an ALTER SEQUENCE raised",
			source: serialTable(Ydb.Type_INT16, describedSequence("id", math.MaxInt64)),
			want:   catalog.Column{Name: "id", DataType: "Int16", ColumnType: "Int16", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true, IsAutoIncrement: true},
		},
		{
			name:   "a sequence given a start and an increment",
			source: serialTable(Ydb.Type_INT64, altered),
			want: catalog.Column{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true, IsAutoIncrement: true,
				IdentityStart: "100", IdentityIncrement: "5"},
		},
		{
			name:   "a restarted sequence",
			source: serialTable(Ydb.Type_INT64, restarted),
			want: catalog.Column{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true, IsAutoIncrement: true,
				IdentityStart: "100", SequenceRestart: "100"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := readFrom(c, test.source)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].Columns, qt.DeepEquals, []catalog.Column{test.want})
		})
	}
}

// TestReader_RecordsTheDatabasePath carries the database's own path on the
// read, which a plan names a Serial's sequence under.
func TestReader_RecordsTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	for _, database := range []string{"/local", "local", "/local/"} {
		db, err := ydbschema.NewReaderFromSource(serialTable(Ydb.Type_INT64, describedSequence("id", math.MaxInt64)),
			database, capability.YDB262()).ReadSchemaContext(context.Background())
		c.Assert(err, qt.IsNil)
		c.Assert(db.DatabasePath, qt.Equals, "/local")
	}
}

// TestReader_Sequence_FailurePath refuses a sequence the declaration cannot
// state and a plan cannot reproduce, naming what it found. No YQL statement
// makes any of these on 25.1.4.7 or 26.2.1.14: ALTER SEQUENCE takes no
// MINVALUE, MAXVALUE, CACHE or CYCLE, and YDB names the sequence itself.
func TestReader_Sequence_FailurePath(t *testing.T) {
	sequence := func(change func(*Ydb_Table.SequenceDescription)) *Ydb_Table.SequenceDescription {
		described := describedSequence("id", math.MaxInt64)
		change(described)
		return described
	}
	for _, test := range []struct {
		name    string
		source  fakeSource
		wantErr string
	}{
		{
			name: "a sequence under another name",
			source: serialTable(Ydb.Type_INT64, sequence(func(s *Ydb_Table.SequenceDescription) {
				s.Name = new("seq_id")
			})),
			wantErr: `YDB table /local/t: column "id": its sequence is named "seq_id" rather than "_serial_column_id", .*`,
		},
		{
			name: "a least value other than 1",
			source: serialTable(Ydb.Type_INT64, sequence(func(s *Ydb_Table.SequenceDescription) {
				s.MinValue = new(int64(5))
			})),
			wantErr: `YDB table /local/t: column "id": its sequence has the least value 5, .*`,
		},
		{
			name: "a cache other than 1",
			source: serialTable(Ydb.Type_INT64, sequence(func(s *Ydb_Table.SequenceDescription) {
				s.Cache = new(uint64(10))
			})),
			wantErr: `YDB table /local/t: column "id": its sequence caches 10 values, .*`,
		},
		{
			name: "a sequence that cycles",
			source: serialTable(Ydb.Type_INT64, sequence(func(s *Ydb_Table.SequenceDescription) {
				s.Cycle = new(true)
			})),
			wantErr: `YDB table /local/t: column "id": its sequence cycles, .*`,
		},
		{
			name:    "a maximum that is neither the column's nor the Int64 maximum",
			source:  serialTable(Ydb.Type_INT32, describedSequence("id", 1000)),
			wantErr: `YDB table /local/t: column "id": its sequence has the maximum 1000, which is neither the maximum of Int32 .*`,
		},
		{
			name: "a data type other than Int64",
			source: serialTable(Ydb.Type_INT32, withDataType(&Ydb_Table.SequenceDescription{
				Name: new("_serial_column_id"),
			}, Ydb.Type_INT32)),
			wantErr: `YDB table /local/t: column "id": its sequence's data type is .*INT32.*, and Ptah reads a sequence of Int64`,
		},
		{
			name: "a field of the description the reader does not know",
			source: serialTable(Ydb.Type_INT64, func() *Ydb_Table.SequenceDescription {
				described := describedSequence("id", math.MaxInt64)
				unknown := protowire.AppendTag(nil, 12, protowire.VarintType)
				unknown = protowire.AppendVarint(unknown, 1)
				described.ProtoReflect().SetUnknown(append(described.ProtoReflect().GetUnknown(), unknown...))
				return described
			}()),
			wantErr: `YDB table /local/t: column "id": its sequence carries field 12 of its description, which this build of Ptah does not read`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := ydbschema.NewReaderFromSource(test.source, "/local", capability.YDB262()).ReadSchemaContext(context.Background())
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
