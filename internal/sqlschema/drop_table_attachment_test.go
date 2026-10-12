package sqlschema_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

const badgeKind schemaext.Kind = "example.org/badges/badge"

// badge is an object of the owner example.org/badges, declared by `CREATE
// BADGE name ON table` and attached to that table without being its child.
type badge struct {
	On string `json:"on"`
}

func (*badge) Kind() schemaext.Kind { return badgeKind }

func (b *badge) Clone() schemaext.Value { return new(*b) }

func (b *badge) Equal(other schemaext.Value) bool {
	o, ok := other.(*badge)
	return ok && *o == *b
}

func badgeRef(name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(badgeKind), Name: objectidentity.Part{Source: name, Normalized: name}}
}

// badgeParser reads `CREATE BADGE name ON table` as the owner's object.
type badgeParser struct{}

func (badgeParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(strings.TrimSuffix(claim.Text, ";"))
		values = append(values, sqlext.Value{Statement: &widgetStatement{Text: claim.Text},
			Objects: []schemaext.Object{{Ref: badgeRef(words[2]), Value: &badge{On: words[4]}}}})
	}
	return sqlext.Result{Values: values}, nil
}

// badgeRelations reports the table a badge is on as its attachment.
type badgeRelations struct{}

func (badgeRelations) DescribeRelations(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	builder := objectidentity.NewBuilder(request.Identifiers)
	result := schemaext.RelationResult{Complete: true}
	for _, value := range request.Values {
		result.Values = append(result.Values, schemaext.ValueRelations{Subject: value.Subject, Complete: true,
			Attachment: builder.Table(value.Value.(*badge).On)})
	}
	return result, nil
}

// badgeRuntime registers target "badges" with the badge owner's statement and
// its relation discovery.
func badgeRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/badges",
		Targets: []engine.Target{{Name: "badges"}},
		Codecs: []schemaext.Codec{schemaext.ModelCodec[*badge]{Prototype: &badge{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(`{"type":"object"}`)}.Codec()},
		SQL: []sqlext.Extension{{
			Owner:    "example.org/badges",
			Targets:  []string{"badges"},
			Kinds:    []schemaext.Kind{badgeKind},
			Patterns: []sqlext.Pattern{{Name: "create", Site: sqlext.Statement, Words: []string{"CREATE", "BADGE"}}},
			Parser:   badgeParser{},
		}},
		Relations: []engine.RelationDiscovery{{Target: "badges", Representation: schemaext.Desired,
			Kinds: []schemaext.Kind{badgeKind}, Service: badgeRelations{}}},
	}))
}

const badgedTables = "CREATE TABLE a (id int);\nCREATE TABLE b (id int);\nCREATE BADGE x ON a;\nCREATE BADGE y ON b;\n"

// TestReadWith_DropTableDetachesWhatHangsOffIt pins that a DROP TABLE removes
// the objects attached to the table, as the server drops them with it, and
// keeps the ones attached to another table.
func TestReadWith_DropTableDetachesWhatHangsOffIt(t *testing.T) {
	c := qt.New(t)
	runtime := badgeRuntime()

	database, _, err := sqlschema.ReadWith(t.Context(), []byte(badgedTables+"DROP TABLE a;\n"), "badges", nil,
		sqlschema.Options{Owners: runtime.SQL(), Relations: runtime})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{badgeRef("y")})
}

// TestReadWith_DropTableWithoutRelationsIsRefused pins that a read that
// cannot tell which table a standalone object hangs off refuses a DROP TABLE
// rather than keep an object the server drops.
func TestReadWith_DropTableWithoutRelationsIsRefused(t *testing.T) {
	c := qt.New(t)

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(badgedTables+"DROP TABLE a;\n"), "badges", nil,
		sqlschema.Options{Owners: badgeRuntime().SQL()})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `DROP TABLE a: .*no runtime can say which tables the standalone feature objects bind or are attached to`)
	c.Assert(database.Tables, qt.HasLen, 0)
	c.Assert(statements, qt.IsNil)
}

// TestReadWith_WithoutRelationsReadsADocumentThatDropsNothing is the control:
// a read without relation discovery reads standalone objects until a table is
// dropped.
func TestReadWith_WithoutRelationsReadsADocumentThatDropsNothing(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.ReadWith(t.Context(), []byte(badgedTables), "badges", nil,
		sqlschema.Options{Owners: badgeRuntime().SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(database.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{badgeRef("x"), badgeRef("y")})
}

// TestReadWith_ADocumentThatNamesNoTargetDetachesNothing pins that relation
// discovery, registered per target, is not asked about a document that names
// none: its DROP TABLE reads beside the standalone objects an untargeted
// statement declared, and keeps them.
func TestReadWith_ADocumentThatNamesNoTargetDetachesNothing(t *testing.T) {
	c := qt.New(t)
	runtime := badgeRuntime()

	database, _, err := sqlschema.ReadWith(t.Context(), []byte(badgedTables+"DROP TABLE a;\n"), "", nil,
		sqlschema.Options{Owners: runtime.SQL(), Relations: runtime})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{badgeRef("x"), badgeRef("y")})
}
