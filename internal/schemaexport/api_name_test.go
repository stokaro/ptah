package schemaexport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

// A column's storage name and its published name are separate identities, and
// the separation only earns its keep if leaving it undeclared changes nothing:
// every schema that does not use it must export exactly as before
// (stokaro/ptah#905).
func TestFieldAPIName(t *testing.T) {
	tests := []struct {
		name  string
		field goschema.Field
		want  string
	}{
		{
			name:  "an undeclared API name is the column name",
			field: goschema.Field{Name: "billing_amount_minor"},
			want:  "billing_amount_minor",
		},
		{
			name:  "a declared one replaces it",
			field: goschema.Field{Name: "billing_amount_minor", APIName: "amount"},
			want:  "amount",
		},
		{
			// Empty is "not declared", not "declared as empty": there is no way
			// to publish a field under no name, and treating the empty string
			// as a request to would produce a document with an unnamed member.
			name:  "an empty declaration is not a declaration",
			field: goschema.Field{Name: "id", APIName: ""},
			want:  "id",
		},
		{
			name:  "a declaration that equals the column name is not special",
			field: goschema.Field{Name: "id", APIName: "id"},
			want:  "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaexport.FieldAPIName(tt.field), qt.Equals, tt.want)
		})
	}
}

// Distinct API names are what the exporters are entitled to assume, so this is
// the shape that must stay quiet.
func TestValidateFieldAPINamesAcceptsDistinctNames(t *testing.T) {
	table := goschema.Table{Name: "invoices"}

	tests := []struct {
		name   string
		fields []goschema.Field
	}{
		{
			name: "columns and an alias that do not overlap",
			fields: []goschema.Field{
				{Name: "id"},
				{Name: "billing_amount_minor", APIName: "amount"},
			},
		},
		{
			name:   "no fields at all",
			fields: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaexport.ValidateFieldAPINames(table, tt.fields), qt.IsNil)
		})
	}
}

// The refusal is what makes the alias safe to have. Two columns published under
// one name means one of them is absent from the exported schema, and nothing in
// that schema records that it was dropped -- so the error names both sources
// rather than picking a winner.
func TestValidateFieldAPINamesRefusesACollision(t *testing.T) {
	table := goschema.Table{Name: "invoices"}

	tests := []struct {
		name    string
		fields  []goschema.Field
		wantErr string
	}{
		{
			name: "an alias colliding with another column",
			fields: []goschema.Field{
				{Name: "amount"},
				{Name: "billing_amount_minor", APIName: "amount"},
			},
			wantErr: `table "invoices" exports two columns as "amount": "amount" and "billing_amount_minor"; ` +
				`give one of them a distinct api_name`,
		},
		{
			name: "two aliases colliding with each other",
			fields: []goschema.Field{
				{Name: "net_minor", APIName: "amount"},
				{Name: "gross_minor", APIName: "amount"},
			},
			wantErr: `table "invoices" exports two columns as "amount": "net_minor" and "gross_minor"; ` +
				`give one of them a distinct api_name`,
		},
		{
			// The alias moved a name onto a column that did not have it, and
			// the column that did have it comes later. Order must not decide
			// whether this is caught.
			name: "the alias declared before the column it collides with",
			fields: []goschema.Field{
				{Name: "billing_amount_minor", APIName: "amount"},
				{Name: "amount"},
			},
			wantErr: `table "invoices" exports two columns as "amount": "billing_amount_minor" and "amount"; ` +
				`give one of them a distinct api_name`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaexport.ValidateFieldAPINames(table, tt.fields)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, tt.wantErr)
		})
	}
}
