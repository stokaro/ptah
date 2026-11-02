package featureselect_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/featureselect"
)

// TestTablesAsksAboutTheSchemaTheSourceWrote passes keepTable the schema each
// policy's source wrote: the empty string where the identity's builder filled
// in PostgreSQL's offline default, public, so a caller resolves it with the
// session's default instead.
func TestTablesAsksAboutTheSchemaTheSourceWrote(t *testing.T) {
	c := qt.New(t)
	objects := must.Must(schemaext.NewObjects(
		must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "audit", "unqualified"), pgpolicy.DesiredPolicy{})),
		must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("public", "orders", "qualified"), pgpolicy.DesiredPolicy{})),
	))
	var asked []string

	kept, _ := featureselect.Tables(objects, schemaext.Coverage{}, func(schema, table string) bool {
		asked = append(asked, schema+"|"+table)
		return true
	})

	c.Assert(asked, qt.ContentEquals, []string{"|audit", "public|orders"})
	c.Assert(kept.Refs(), qt.DeepEquals, objects.Refs())
}
