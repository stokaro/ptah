package crdbsource_test

import (
	"context"
	"fmt"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
)

// ExampleService shows a Go annotation declaring CockroachDB row-level TTL
// through platform.cockroachdb properties, decoded by an application-selected
// provider into the owner's declaration. The bundled engine runtime is not
// needed.
func ExampleService() {
	database, err := goschema.ParseSource(builtintest.Annotations(), "sessions.go", `package entities

//ptah:schema:table name="sessions" platform.cockroachdb.ttl_expire_after="3 days" platform.cockroachdb.ttl_job_cron="@daily"
type Session struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
}
`)
	if err != nil {
		fmt.Println(err)
		return
	}
	runtime, err := engine.New(engine.Provider{
		ID: crdbschema.Owner, Targets: []engine.Target{{Name: "cockroachdb"}}, Codecs: crdbschema.Codecs(),
		Properties: []engine.PropertySource{{Target: "cockroachdb", Format: schemaext.TablePlatformProperties,
			Definitions: crdbsource.Definitions(), Service: crdbsource.Service{}}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	decoded, err := schemaproperties.DecodeTables(context.Background(), &database, "cockroachdb", runtime)
	if err != nil {
		fmt.Println(err)
		return
	}
	declared, _, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](decoded.Tables[0].Facets, crdbschema.RowTTLKind)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, parameter := range declared.Policy.Parameters() {
		fmt.Printf("%s = %s\n", parameter.Name, parameter.Value)
	}
	// Output:
	// ttl_expire_after = 3 days
	// ttl_job_cron = @daily
}
