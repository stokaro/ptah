package schemadiff_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemacensus"
	"ptah.run/migration/schemadiff"
)

// A document has to compare equal to itself. [schemadiff.CompareSchemas] sends
// the CURRENT side through internal/convert/goschematodb and compares the result
// against a desired side that did not make that hop, so a property the converter
// drops cannot be seen to agree with itself: it is on one side of the comparison
// and absent from the other. The plan that comes out never converges, and where
// the shape is an add paired with a removal it is destructive -- dropping an
// enum takes every column typed on it (stokaro/ptah#3214).
//
// The sweep is driven by reflection over the model rather than by a fixture.
// Two spot checks already assert the self-diff is empty, and each pins one
// hand-written table of three columns; seven declarable properties failed the
// very assertion those tests make, and a sweep over the census corpus found
// twenty more. A hand-written list is what went stale, so this walks every
// fixture internal/schemacensus declares and, when one is dirty, blames the
// failure on a field by ablating each one the census can name.

// selfDiffDialects are the targets the sweep compares under.
//
// The dialect is not decoration here. It decides how the converter unpacks the
// one model field carrying two concepts, whether an enum column is a type of its
// own or an inline model, and which objects a dialect scope keeps -- so a
// property can agree with itself on one target and disagree on another. One
// target per renderer family covers those splits; the whole release matrix would
// spend time on presets that answer identically.
var selfDiffDialects = []string{
	platform.Postgres,
	platform.MySQL,
	platform.SQLServer,
	platform.SQLite,
	platform.ClickHouse,
}

// TestCompareSchemas_EveryDeclaredDocumentEqualsItself is the gate.
//
// Every failure here is a plan that runs forever against a schema nobody
// changed. The failure message names the fields whose removal makes the
// comparison clean, which is what turns a diff into a line to fix.
func TestCompareSchemas_EveryDeclaredDocumentEqualsItself(t *testing.T) {
	c := qt.New(t)

	fixtures := schemacensus.Fixtures()

	// The corpus floor. A sweep whose fixture list stopped matching would
	// report nothing and read exactly like a clean tree.
	c.Assert(len(fixtures) >= 90, qt.IsTrue,
		qt.Commentf("the census corpus shrank to %d fixtures", len(fixtures)))
	c.Assert(len(schemacensus.Fields()) >= 300, qt.IsTrue,
		qt.Commentf("the field census shrank to %d fields", len(schemacensus.Fields())))

	for _, fixture := range fixtures {
		for _, dialect := range selfDiffDialects {
			t.Run(fixture.Name+"/"+dialect, func(t *testing.T) {
				c := qt.New(t)
				diff := selfDiffOf(fixture.Schema, dialect)
				c.Assert(diff, qt.Equals, "",
					qt.Commentf("blame: %s", strings.Join(blameSelfDiff(fixture.Schema, dialect), ", ")))
			})
		}
	}
}

// blameSelfDiff names the fields whose removal makes the comparison clean.
//
// It runs only for a failure message, so it is allowed to be slow and allowed to
// name more than one field: a container and the leaf inside it both clean the
// comparison, and a reader wants to see both rather than have the test pick.
func blameSelfDiff(schema schemamodel.Database, dialect string) []string {
	var blamed []string
	for _, field := range schemacensus.Fields() {
		if !schemacensus.Populated(schema, field) {
			continue
		}
		if selfDiffOf(schemacensus.Ablate(schema, field), dialect) == "" {
			blamed = append(blamed, field)
		}
	}
	if len(blamed) == 0 {
		return []string{"no single field explains it; two contribute at once"}
	}
	return blamed
}

// selfDiffOf finalizes a copy of the schema and compares it against itself,
// answering with the rendered diff or the empty string when there is none.
//
// The copy is what makes the sweep repeatable: [schemamodel.Finalize] writes
// into the slices it is handed, so finalizing a fixture in place would leave the
// next dialect comparing a schema the previous one derived.
func selfDiffOf(schema schemamodel.Database, dialect string) string {
	finalized := schemacensus.Copy(schema)
	schemamodel.Finalize(&finalized)
	diff := schemadiff.CompareSchemas(&finalized, &finalized, dialect)
	if !diff.HasChanges() {
		return ""
	}
	encoded, err := json.Marshal(diff)
	if err != nil {
		return fmt.Sprintf("%+v", diff)
	}
	return string(encoded)
}
