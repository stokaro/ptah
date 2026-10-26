package atlashcl_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

// ruleDecoder declares a gadget for each `rule` claim, named after the rule
// and sized by the roles it applies to, and a gadget facet on the table of
// each `switch` claim. It refuses a rule named "refused" on its own, and two
// rules of one name on one table together, naming neither block, as an owner
// refuses two blocks that declare one object. It records the requests.
type ruleDecoder struct {
	requests *[]hclext.Request
}

func (d ruleDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, withoutRanges(request))
	for i, claim := range request.Claims {
		if claim.Name == "refused" {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("rule %q is refused", claim.Name)}
		}
	}
	declarations := make([]hclext.Declaration, len(request.Claims))
	seen := make(map[string]string)
	for i, claim := range request.Claims {
		if claim.Block == "switch" {
			declarations[i].Facets = must.Must(schemaext.NewFacets(&gadget{Size: "switched"}))
			continue
		}
		on := claim.Attributes["on"].Reference
		key := on.Schema + "." + on.Name + "/" + claim.Name
		if earlier, found := seen[key]; found {
			return hclext.Result{}, fmt.Errorf("rules at %s and %s both declare %s", earlier, claim.At, key)
		}
		seen[key] = claim.At
		size := ""
		for _, role := range claim.Attributes["to"].References {
			size += "[" + role.Schema + "|" + role.Name + "]"
		}
		ref := objectidentity.ID{Kind: objectidentity.Kind(gadgetKind), Name: objectidentity.Part{Source: key, Normalized: key}}
		declarations[i].Objects = []schemaext.Object{{Ref: ref, Value: &gadget{Size: size}}}
	}
	return hclext.Result{Declarations: declarations}, nil
}

// ruleOwners registers the owner example.org/custom, whose top-level `rule`
// block is named by its labels on the table its `on` refers to and applies
// to the roles its `to` lists, and whose `switch` block stands in a table.
func ruleOwners(requests *[]hclext.Request) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner: "example.org/custom",
			Kinds: []schemaext.Kind{gadgetKind},
			Blocks: []hclext.Block{
				{Site: hclext.TopLevel, Type: "rule", Naming: hclext.JoinedName, Attributes: []hclext.Attribute{
					{Name: "on", Kind: hclext.Ref, Refers: "table"}, {Name: "to", Kind: hclext.Refs, Refers: "role"}}},
				{Site: hclext.InTable, Type: "switch", Attributes: []hclext.Attribute{{Name: "on", Kind: hclext.Bool}}},
			},
			Decoder: ruleDecoder{requests: requests},
		}},
	})).HCL()
}

// parseRules parses source with the rule owner, and returns the requests the
// owner received.
func parseRules(ctx context.Context, source string) (*schemamodel.Database, []hclext.Request, error) {
	var requests []hclext.Request
	db, err := atlashcl.ParseWithOptions(ctx, []byte(source), "schema.hcl", atlashcl.Options{HCL: ruleOwners(&requests)})
	return db, requests, err
}

const ruleTables = `variable "t" {
  type    = string
  default = "users"
}
schema "app" {}
table "users" {
  schema = schema.app
  column "id" {
    type = int
  }
}
table "orders" {
  column "id" {
    type = int
  }
}
`

// TestParseWithOptions_ResolvesAReference hands the owner what a reference
// refers to: a table the document declares carries that table's schema,
// whatever spelling names it, and one the document does not declare keeps
// the schema and name it writes.
func TestParseWithOptions_ResolvesAReference(t *testing.T) {
	tests := []struct {
		name string
		on   string
		want hclext.Reference
	}{
		{name: "a reference to a declared table", on: `table.users`, want: hclext.Reference{Kind: "table", Schema: "app", Name: "users"}},
		{name: "a qualified reference", on: `table.app.users`, want: hclext.Reference{Kind: "table", Schema: "app", Name: "users"}},
		{name: "an index", on: `table["users"]`, want: hclext.Reference{Kind: "table", Schema: "app", Name: "users"}},
		{name: "a string", on: `"users"`, want: hclext.Reference{Kind: "table", Schema: "app", Name: "users"}},
		{name: "an identifier", on: `orders`, want: hclext.Reference{Kind: "table", Name: "orders"}},
		{name: "a variable", on: `var.t`, want: hclext.Reference{Kind: "table", Schema: "app", Name: "users"}},
		{name: "an undeclared table", on: `table.ghost`, want: hclext.Reference{Kind: "table", Name: "ghost"}},
		{name: "an undeclared qualified string", on: `"x.ghost"`, want: hclext.Reference{Kind: "table", Schema: "x", Name: "ghost"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, requests, err := parseRules(t.Context(), ruleTables+"rule \"r\" {\n  on = "+test.on+"\n}\n")

			c.Assert(err, qt.IsNil)
			c.Assert(requests, qt.HasLen, 1)
			c.Assert(*requests[0].Claims[0].Attributes["on"].Reference, qt.DeepEquals, test.want)
		})
	}
}

// TestParseWithOptions_RefusesAReferenceItCannotResolve refuses a block
// whose reference is absent or refers to another type of object at the
// block, as this package refuses its own, and a reference several declared
// tables match naming the block and their schemas.
func TestParseWithOptions_RefusesAReferenceItCannotResolve(t *testing.T) {
	twoOrders := "schema \"a\" {}\nschema \"b\" {}\ntable \"orders\" {\n  schema = schema.a\n  column \"id\" {\n    type = int\n  }\n}\n" +
		"table \"orders\" {\n  schema = schema.b\n  column \"id\" {\n    type = int\n  }\n}\n"
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{name: "an absent reference", source: "rule \"r\" {}\n",
			wantErr: `parse HCL schema at schema\.hcl:1,1-5: rule "r" requires on`},
		{name: "a reference to a view", source: "rule \"r\" {\n  on = view.v\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:1,1-5: rule "r" requires on`},
		{name: "a reference with too many parts", source: "rule \"r\" {\n  on = table.a.b.c\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:1,1-5: rule "r" requires on`},
		{name: "a reference two tables match", source: twoOrders + "rule \"r\" {\n  on = table.orders\n}\n",
			wantErr: `parse HCL schema schema\.hcl: rule "r" at schema\.hcl:15: invalid annotation attribute value: ` +
				`table "orders" is declared in schemas "a" and "b"; name the schema`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := parseRules(t.Context(), test.source)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_ReadsAListOfReferences hands the owner each element
// of a list of references: a reference with or without a schema, an index,
// a string or an identifier naming it as written, and a variable naming it
// with its value. A single element reads as a list of one.
func TestParseWithOptions_ReadsAListOfReferences(t *testing.T) {
	tests := []struct {
		name string
		to   string
		want []hclext.Reference
	}{
		{name: "a list", to: `[role.app, role["a b"], role.s.x, "PUBLIC", reader, var.r]`, want: []hclext.Reference{
			{Kind: "role", Name: "app"}, {Kind: "role", Name: "a b"}, {Kind: "role", Schema: "s", Name: "x"},
			{Kind: "role", Name: "PUBLIC"}, {Kind: "role", Name: "reader"}, {Kind: "role", Name: "writer"}}},
		{name: "one reference", to: `role.app`, want: []hclext.Reference{{Kind: "role", Name: "app"}}},
		{name: "an empty list", to: `[]`, want: make([]hclext.Reference, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "variable \"r\" {\n  type    = string\n  default = \"writer\"\n}\n" + ruleTables +
				"rule \"r\" {\n  on = table.users\n  to = " + test.to + "\n}\n"

			_, requests, err := parseRules(t.Context(), source)

			c.Assert(err, qt.IsNil)
			c.Assert(requests, qt.HasLen, 1)
			c.Assert(requests[0].Claims[0].Attributes["to"].References, qt.DeepEquals, test.want)
		})
	}
}

// TestParseWithOptions_RefusesAListElementThatIsNoReference refuses an
// element that refers to another type of object, or is no reference at all,
// at the attribute, and a string that spells a reference at the element: the
// string's meaning would otherwise depend on who reads it. The remedy names
// the unquoted reference only where that is one this package reads.
func TestParseWithOptions_RefusesAListElementThatIsNoReference(t *testing.T) {
	tests := []struct {
		name    string
		to      string
		wantErr string
	}{
		{name: "a view", to: `[view.x]`,
			wantErr: `parse HCL schema at schema\.hcl:3,3-5: rule attribute "to": view\.x is not a role reference`},
		{name: "a number", to: `[1]`,
			wantErr: `parse HCL schema at schema\.hcl:3,3-5: rule attribute "to": 1 is not a role reference`},
		{name: "a string that spells a reference", to: `[role.app, "role.x"]`,
			wantErr: `parse HCL schema at schema\.hcl:3,19-27: rule attribute "to": "role\.x" is ambiguous; ` +
				`write role\.x unquoted for the role x, or name the role without the role\. prefix`},
		{name: "a string that spells an index", to: `["role[\"x\"]"]`,
			wantErr: `parse HCL schema at schema\.hcl:3,9-22: rule attribute "to": "role\[\\"x\\"\]" is ambiguous; ` +
				`write role\["x"\] unquoted for the role x, or name the role without the role\[ prefix`},
		{name: "a string that spells no reference this package reads", to: `["role.a.b.c"]`,
			wantErr: `parse HCL schema at schema\.hcl:3,9-21: rule attribute "to": "role\.a\.b\.c" is ambiguous; ` +
				`name the role without the role\. prefix`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := parseRules(t.Context(), "rule \"r\" {\n  on = table.users\n  to = "+test.to+"\n}\n")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_JoinsTwoLabelsIntoOneName names a block named by its
// labels alone with both labels joined by a dot, in no schema.
func TestParseWithOptions_JoinsTwoLabelsIntoOneName(t *testing.T) {
	c := qt.New(t)

	_, requests, err := parseRules(t.Context(), ruleTables+"rule \"a\" \"b\" {\n  on = table.users\n}\n")

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Claims[0].Schema, qt.Equals, "")
	c.Assert(requests[0].Claims[0].Name, qt.Equals, "a.b")
}

// TestParseWithOptions_RefusesASchemaOfAJoinedName refuses a `schema`
// attribute on a block named by its labels alone, which names the object
// inside the one its reference names.
func TestParseWithOptions_RefusesASchemaOfAJoinedName(t *testing.T) {
	c := qt.New(t)

	db, _, err := parseRules(t.Context(), "rule \"r\" {\n  schema = schema.app\n  on     = table.users\n}\n")

	c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema\.hcl:1,1-5: unsupported rule attribute "schema"`)
	c.Assert(db, qt.IsNil)
}

const switchedTables = `schema "app" {}
table "users" {
  schema = schema.app
  column "id" {
    type = int
  }
  switch {
    on = true
  }
}
table "orders" {
  column "id" {
    type = int
  }
}
`

// TestParseWithOptions_ClaimsATableBlockForTheOwner hands the owner a block
// inside a table with the table it stands in, and adds the facets it returns
// to that table alone.
func TestParseWithOptions_ClaimsATableBlockForTheOwner(t *testing.T) {
	c := qt.New(t)

	db, requests, err := parseRules(t.Context(), switchedTables)

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{{Block: "switch",
		Attributes: map[string]hclext.Value{"on": {Kind: hclext.Bool, Bool: true}}, At: "schema.hcl:7",
		Table: &hclext.Table{Schema: "app", Name: "users", Struct: "app.users"}}}}})
	c.Assert(db.Tables, qt.HasLen, 2)
	switched, _, err := schemaext.FacetAs[*gadget](db.Tables[0].Facets, gadgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(switched.Size, qt.Equals, "switched")
	c.Assert(db.Tables[1].Facets.Len(), qt.Equals, 0)
}

// TestParseWithOptions_RefusesATableBlockAsItsOwn refuses a table block with
// labels, and refuses the block outright without its owner.
func TestParseWithOptions_RefusesATableBlockAsItsOwn(t *testing.T) {
	tests := []struct {
		name    string
		owners  hclext.Set
		source  string
		wantErr string
	}{
		{name: "a label", owners: ruleOwners(new([]hclext.Request)),
			source:  "table \"t\" {\n  column \"id\" {\n    type = int\n  }\n  switch \"x\" {}\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:5,3-9: switch block does not accept labels`},
		{name: "no owner", owners: hclext.None(), source: switchedTables,
			wantErr: `parse HCL schema at schema\.hcl:7,3-9: unsupported table block "switch"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.ParseWithOptions(t.Context(), []byte(test.source), "schema.hcl", atlashcl.Options{HCL: test.owners})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_ReportsARefusalOfTheDocumentLast reports an owner's
// refusal of two blocks together, and a reference several tables match, once
// this package's own checks of the whole document pass: a redeclared table
// is reported first, and a refusal of one block by position.
func TestParseWithOptions_ReportsARefusalOfTheDocumentLast(t *testing.T) {
	repeated := "rule \"r\" {\n  on = table.users\n}\nrule \"r\" {\n  on = table.app.users\n}\n"
	usersAgain := "table \"users\" {\n  schema = schema.app\n  column \"id\" {\n    type = int\n  }\n}\n"
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{name: "the owner's refusal of the document", source: ruleTables + repeated,
			wantErr: `parse HCL schema schema\.hcl: rules at schema\.hcl:17 and schema\.hcl:20 both declare app\.users/r`},
		{name: "after a redeclared table", source: ruleTables + repeated + usersAgain,
			wantErr: `parse HCL schema schema\.hcl: table "app\.users" is declared more than once;.*`},
		{name: "after a refusal of one block", source: ruleTables + repeated + "rule \"refused\" {\n  on = table.users\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:23,1-5: rule "refused" is refused`},
		{name: "a reference several tables match, after a refusal of one block",
			source: ruleTables + "table \"users\" {\n  column \"id\" {\n    type = int\n  }\n}\n" +
				"rule \"r\" {\n  on = table.users\n}\nrule \"refused\" {\n  on = table.users\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:25,1-5: rule "refused" is refused`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := parseRules(t.Context(), test.source)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_RefusesAnAmbiguousReferenceAsInvalid keeps the class
// of a reference several tables match.
func TestParseWithOptions_RefusesAnAmbiguousReferenceAsInvalid(t *testing.T) {
	c := qt.New(t)
	source := ruleTables + "table \"users\" {\n  column \"id\" {\n    type = int\n  }\n}\nrule \"r\" {\n  on = table.users\n}\n"

	db, _, err := parseRules(t.Context(), source)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(db, qt.IsNil)
}

// TestParseWithOptions_KeepsNothingAnOwnerDeclaredForARefusedDocument
// reports the reader's refusal of a table after the owner read a block inside
// it, and adds nothing the owner declared: the table it would go on was never
// read.
func TestParseWithOptions_KeepsNothingAnOwnerDeclaredForARefusedDocument(t *testing.T) {
	c := qt.New(t)
	source := "table \"t\" {\n  switch {\n    on = true\n  }\n  column \"id\" {\n    type = int\n    null = \"x\"\n  }\n}\n"

	db, _, err := parseRules(t.Context(), source)

	c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema\.hcl:7,5-9: column attribute "null" must be a bool`)
	c.Assert(db, qt.IsNil)
}
