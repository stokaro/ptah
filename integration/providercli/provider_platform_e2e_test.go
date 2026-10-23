//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/engine"
)

func platformProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderCommand(c, "platform-claims")
	return runtime
}

// platformGoSource writes the retention of a table, a column and an index as
// platform properties the provider claimed, beside a platform property of
// another engine that nobody claimed.
const platformGoSource = `package p

//ptah:schema:table name="records" platform.fixture.seconds=30 platform.mysql.engine="InnoDB"
type Record struct {
	//ptah:schema:field name="id" type="INTEGER" platform.fixture.minutes=2
	ID int
	//ptah:schema:field name="body" type="TEXT" platform.mysql.type="TEXT"
	Body string

	//ptah:schema:index name="by_body" fields="body" platform.fixture.seconds=9
	_ int
}
`

func TestProviderClaimedPlatformAttributes_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", platformGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, db), qt.DeepEquals, []string{
		`table records {"seconds":30}`,
		`column id {"seconds":120}`,
		`index by_body {"seconds":9}`,
	})
}

// TestProviderClaimedPlatformAttributes_KeepsUnclaimed pins that only the
// claimed properties leave the common model: the provider holds them, and a
// property nobody claimed is still the common model's.
func TestProviderClaimedPlatformAttributes_KeepsUnclaimed(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", platformGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"engine": "InnoDB"}})
	c.Assert(db.Fields[0].Overrides, qt.HasLen, 0)
	c.Assert(db.Fields[1].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"type": "TEXT"}})
	c.Assert(db.Indexes[0].Overrides, qt.HasLen, 0)
}

// TestProviderPlatformAttributesWithoutTheProvider_KeepsThem is the control: a
// run that did not select the provider keeps every platform property in the
// common model, so the removal above is the claim's doing.
func TestProviderPlatformAttributesWithoutTheProvider_KeepsThem(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New()
	c.Assert(err, qt.IsNil)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", platformGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].Overrides["fixture"], qt.DeepEquals, map[string]string{"seconds": "30"})
}
