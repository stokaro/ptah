package viz

// White-box testing required: securityAnnotations and higherSeverity are
// unexported, and the sorting they do -- which finding a node can carry and
// which it cannot -- has no other observation point than the rendered diagram.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaviz"
)

// TestSecurityAnnotations sorts findings into the ones a node can carry and the
// ones it cannot.
//
// An entity diagram has a node per table and none for a routine or a schema, so
// a finding about either has nowhere to attach. Those are returned separately
// and drawn as a comment: a diagram showing three of five findings without
// saying so is worse than one showing three and naming the other two
// (stokaro/ptah#1035).
func TestSecurityAnnotations(t *testing.T) {
	tests := []struct {
		name     string
		opts     options
		database *schemamodel.Database
		// wantAnnotated maps a table to the severity its node is drawn in.
		wantAnnotated map[string]string
		// wantUnattached is what no node could carry, in order.
		wantUnattached []string
	}{
		{
			name: "off, nothing is analyzed",
			opts: options{security: false, dialect: "postgres"},
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "audit_log"}},
			},
			wantAnnotated:  make(map[string]string),
			wantUnattached: make([]string, 0),
		},
		{
			name: "a table finding marks its node",
			opts: options{security: true, dialect: "postgres"},
			database: &schemamodel.Database{
				Grants:           []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "audit_log"}},
				RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "audit_log"}},
			},
			wantAnnotated: map[string]string{"audit_log": "warning"},
			wantUnattached: []string{
				"OWN01 not checked here: object ownership was not read for this source",
				"ROL01 not checked here: no role usage data was supplied; a privilege is not use, and no catalog records which roles read which objects",
				"ROL03 not checked here: role membership was not read for this source",
				"ROL04 not checked here: role membership was not read for this source",
			},
		},
		{
			name: "a routine finding has no node and is named anyway",
			opts: options{security: true, dialect: "postgres"},
			database: &schemamodel.Database{
				Functions: []schemamodel.Function{{Name: "escalate", Security: "DEFINER", Language: "plpgsql"}},
			},
			wantAnnotated: make(map[string]string),
			wantUnattached: []string{
				"routine escalate: info PRV02",
				"OWN01 not checked here: object ownership was not read for this source",
				"ROL01 not checked here: no role usage data was supplied; a privilege is not use, and no catalog records which roles read which objects",
				"ROL03 not checked here: role membership was not read for this source",
				"ROL04 not checked here: role membership was not read for this source",
			},
		},
		{
			// The worst severity decides the color, so a node found by two
			// rules is not drawn in whichever one happened to sort first.
			name: "two findings on one table keep the worse severity",
			opts: options{security: true, dialect: "postgres"},
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "audit_log"}},
			},
			wantAnnotated: map[string]string{"audit_log": "warning"},
			wantUnattached: []string{
				"OWN01 not checked here: object ownership was not read for this source",
				"ROL01 not checked here: no role usage data was supplied; a privilege is not use, and no catalog records which roles read which objects",
				"ROL03 not checked here: role membership was not read for this source",
				"ROL04 not checked here: role membership was not read for this source",
			},
		},
		{
			name: "a rule that cannot run here is reported, not skipped silently",
			opts: options{security: true, dialect: "mysql"},
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"}},
			},
			wantAnnotated: make(map[string]string),
			wantUnattached: []string{
				"PRV01 not checked here: the target does not model row-level security",
				"OWN01 not checked here: object ownership was not read for this source",
				"ROL01 not checked here: no role usage data was supplied; a privilege is not use, and no catalog records which roles read which objects",
				"ROL03 not checked here: role membership was not read for this source",
				"ROL04 not checked here: role membership was not read for this source",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			annotations, unattached, err := securityAnnotations(test.database, test.opts)

			c.Assert(err, qt.IsNil)

			severities := make(map[string]string, len(annotations))
			for table, annotation := range annotations {
				severities[table] = annotation.Severity
				c.Assert(len(annotation.Labels) > 0, qt.IsTrue)
			}
			c.Assert(severities, qt.DeepEquals, test.wantAnnotated)
			c.Assert(orEmptySlice(unattached), qt.DeepEquals, test.wantUnattached)
		})
	}
}

// TestHigherSeverity pins the ordering the node color is chosen by.
//
// The empty string is "nothing seen yet" rather than a severity, and risk.Rank
// scores it and info alike at 0 -- so a rank comparison alone would leave the
// first info finding with no severity at all, and the node would render with a
// label that opens with a colon.
func TestHigherSeverity(t *testing.T) {
	tests := []struct {
		name      string
		current   string
		candidate string
		want      string
	}{
		{name: "the first finding wins over nothing", current: "", candidate: "info", want: "info"},
		{name: "warning beats info", current: "info", candidate: "warning", want: "warning"},
		{name: "info does not beat warning", current: "warning", candidate: "info", want: "warning"},
		{name: "error beats warning", current: "warning", candidate: "error", want: "error"},
		{name: "warning does not beat error", current: "error", candidate: "warning", want: "error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(higherSeverity(test.current, test.candidate), qt.Equals, test.want)
		})
	}
}

// TestSecurityAnnotations_RenderThroughTheDiagram is the end-to-end shape: the
// annotation reaches the drawing rather than only the map.
func TestSecurityAnnotations_RenderThroughTheDiagram(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "AuditLog", Name: "audit_log"}},
		Grants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "audit_log"}},
	}

	annotations, unattached, analyzeErr := securityAnnotations(database, options{security: true, dialect: "postgres"})
	c.Assert(analyzeErr, qt.IsNil)
	rendered, err := schemaviz.Render(database, schemaviz.Options{
		Format:      schemaviz.FormatDOT,
		Annotations: annotations,
		Unattached:  unattached,
	})

	c.Assert(err, qt.IsNil)
	// One row, both codes, drawn in the worse of the two severities: the table
	// is granted to PUBLIC (warning) and has no row-level security (info).
	c.Assert(string(rendered), qt.Contains, "warning: PRV01 PRV03")
	c.Assert(string(rendered), qt.Contains, `CELLPADDING="6" COLOR="#b45309"`)
}

// orEmptySlice turns a nil slice into an empty one, so a row states its
// expectation as a value rather than the test branching on which it got.
func orEmptySlice(values []string) []string {
	if values == nil {
		return make([]string, 0)
	}
	return values
}
