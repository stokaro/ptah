package oratype_test

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/feature/oratype"
)

// ExampleHandlers shows the statements the owner writes for a declared domain
// and a declared object type on Oracle 23: the domain's comment as a SQL
// comment line, CREATE DOMAIN with the existence guard the release line has,
// and CREATE OR REPLACE TYPE with the OBJECT keyword Oracle needs.
func ExampleHandlers() {
	registry, err := renderer.NewExtensions(oratype.Handlers()...)
	if err != nil {
		panic(err)
	}
	context := renderer.ExtensionContext{Target: platform.Oracle, Capabilities: capability.Oracle23()}
	email := oratype.Ref(oratype.DomainKind, "", "email_d")
	address := oratype.Ref(oratype.ObjectTypeKind, "", "addr_t")
	for _, payload := range []ast.ExtensionPayload{
		oratype.CreateDomainOf(email, &oratype.DesiredDomain{BaseType: "VARCHAR2(320)", NotNull: true, Comment: "mail"}),
		oratype.ReplaceObjectTypeOf(address, &oratype.DesiredObjectType{Attributes: []oratype.Attribute{
			{Name: "street", Type: "VARCHAR2(80)"}, {Name: "zip", Type: "VARCHAR2(10)"},
		}}, false),
	} {
		statements, err := registry.Render(context, ast.StatementExtension, payload)
		if err != nil {
			panic(err)
		}
		for _, statement := range statements {
			fmt.Println(statement)
		}
	}

	// Output:
	// -- mail
	// CREATE DOMAIN IF NOT EXISTS email_d AS VARCHAR2(320) NOT NULL;
	// CREATE OR REPLACE TYPE addr_t AS OBJECT (street VARCHAR2(80), zip VARCHAR2(10));
}
