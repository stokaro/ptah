package backend_test

import (
	"testing"

	"example.com/ptah-tutorial-provider/backend"
	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func table(name string) *protocol.Reference {
	return &protocol.Reference{Kind: "table", Name: name}
}

func TestRender_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		change *protocol.Change
		want   string
	}{{
		name: "create a table",
		change: &protocol.Change{Id: "1", Object: table("notes"), Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "body", Type: "TEXT", Nullable: true},
		}}}},
		want: `CREATE TABLE "notes" ("id" INTEGER NOT NULL PRIMARY KEY, "body" TEXT);`,
	}, {
		name:   "drop a table",
		change: &protocol.Change{Id: "1", Object: table("notes"), Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}},
		want:   `DROP TABLE "notes";`,
	}, {
		name:   "add a column",
		change: &protocol.Change{Id: "1", Object: table("notes"), Operation: &protocol.Change_AddColumn{AddColumn: &protocol.Column{Name: "created_at", Type: "INTEGER", Nullable: true}}},
		want:   `ALTER TABLE "notes" ADD COLUMN "created_at" INTEGER;`,
	}, {
		name:   "drop a column",
		change: &protocol.Change{Id: "1", Object: table("notes"), Operation: &protocol.Change_DropColumn{DropColumn: "created_at"}},
		want:   `ALTER TABLE "notes" DROP COLUMN "created_at";`,
	}, {
		name:   "quote a name that holds a quote",
		change: &protocol.Change{Id: "1", Object: table(`odd"name`), Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}},
		want:   `DROP TABLE "odd""name";`,
	}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{test.change}}
			result, err := (&backend.Provider{}).Render(c.Context(), request)
			c.Assert(err, qt.IsNil)
			// The validator the SDK runs on both sides of the process boundary.
			c.Assert(provider.ValidateRenderedBatch(backend.Description(), request, result), qt.IsNil)
			c.Assert(result.GetUnits(), qt.HasLen, 1)
			c.Assert(result.GetUnits()[0].GetChangeId(), qt.Equals, "1")
			c.Assert(result.GetUnits()[0].GetSql(), qt.Equals, test.want)
		})
	}
}

func TestRender_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		change *protocol.Change
		reason string
	}{{
		name: "a type the engine does not store",
		change: &protocol.Change{Id: "1", Object: table("notes"), Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{
			{Name: "title", Type: "VARCHAR(80)", Nullable: true},
		}}}},
		reason: "the tutorial engine stores the column types INTEGER, TEXT, REAL and BLOB only",
	}, {
		name: "a nullable primary key",
		change: &protocol.Change{Id: "1", Object: table("notes"), Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true, Nullable: true},
		}}}},
		reason: "a primary key column of the tutorial engine is declared NOT NULL",
	}, {
		name:   "a default",
		change: &protocol.Change{Id: "1", Object: table("notes"), Operation: &protocol.Change_AddColumn{AddColumn: &protocol.Column{Name: "n", Type: "INTEGER", DefaultExpression: new("0")}}},
		reason: "a tutorial column has no default and no engine settings",
	}, {
		name:   "a schema qualifier",
		change: &protocol.Change{Id: "1", Object: &protocol.Reference{Kind: "table", Schema: "other", Name: "notes"}, Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}},
		reason: "a tutorial table has a name and no schema qualifier",
	}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{test.change}}
			result, err := (&backend.Provider{}).Render(c.Context(), request)
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(status.Convert(err).Message(), qt.Equals, test.reason)
			c.Assert(result, qt.IsNil)
		})
	}
}
