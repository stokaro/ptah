package mysqlprepare_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/mysql/mysqlprepare"
)

// ExampleService_PrepareTables resolves the collation a column that names
// none takes from its table, while the declaration keeps naming none.
func ExampleService_PrepareTables() {
	request := schemapreparation.Request{
		Target: "mysql",
		Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "users", Collate: "utf8mb4_unicode_ci"},
			Fields: []schemamodel.Field{{Name: "id", Type: "int"}, {Name: "email", Type: "varchar(255)"}},
		}}},
		CharsetDefaultCollations: map[string]string{"utf8mb4": "utf8mb4_0900_ai_ci"},
	}
	result, err := (mysqlprepare.Service{}).PrepareTables(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	table := result.Tables[0]
	fmt.Println("table default:", table.DefaultCollation)
	fmt.Println("email inherits:", table.InheritedCollations["email"])
	fmt.Printf("email declares: %q\n", table.Desired.Fields[1].Collate)
	// Output:
	// table default: utf8mb4_unicode_ci
	// email inherits: utf8mb4_unicode_ci
	// email declares: ""
}
