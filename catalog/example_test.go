package catalog_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/dbschema"
)

// ExampleQualifyTableName shows the keying convention both sides of a schema
// comparison build object names through: an empty schema yields the bare
// name, a set schema joins with a dot, and a name carrying a literal dot is
// quoted so it cannot collide with a schema-qualified name. Every
// QualifiedName method in this package delegates here.
func ExampleQualifyTableName() {
	fmt.Println(catalog.QualifyTableName("", "users"))
	fmt.Println(catalog.QualifyTableName("tenant", "data"))
	fmt.Println(catalog.QualifyTableName("", "tenant.data"))

	// Output:
	// users
	// tenant.data
	// "tenant.data"
}

// ExampleColumn_RawType shows the one type spelling a comparator holds a
// desired schema against. The catalog keeps a varchar's width and a decimal's
// precision in fields of their own, and RawType folds them back in;
// FormattedType, the server's own spelling, takes precedence wherever a reader
// filled it, which is how an array or a domain keeps the type the server
// actually reports.
func ExampleColumn_RawType() {
	varchar := catalog.Column{
		DataType:           "character varying",
		CharacterMaxLength: new(50),
	}
	array := catalog.Column{
		DataType:      "ARRAY",
		UDTName:       "_varchar",
		FormattedType: "character varying(100)[]",
	}
	decimal := catalog.Column{
		DataType:         "numeric",
		NumericPrecision: new(10),
		NumericScale:     new(2),
	}

	fmt.Println(varchar.RawType())
	fmt.Println(array.RawType())
	fmt.Println(decimal.RawType())

	// Output:
	// character varying(50)
	// character varying(100)[]
	// numeric(10,2)
}

// ExampleConstraint_ColumnNamesOrDefault shows the read-through-the-accessor
// contract: a reader may fill the legacy single-column field or the
// multi-column slice, and only the accessors merge the two. A consumer that
// reads one field directly misses every constraint spelled the other way.
func ExampleConstraint_ColumnNamesOrDefault() {
	legacy := catalog.Constraint{
		Name:       "fk_orders_user",
		TableName:  "orders",
		Type:       "FOREIGN KEY",
		ColumnName: "user_id",
	}
	composite := catalog.Constraint{
		Name:        "pk_order_items",
		TableName:   "order_items",
		Type:        "PRIMARY KEY",
		ColumnNames: []string{"order_id", "sku"},
	}

	fmt.Println(legacy.ColumnNamesOrDefault())
	fmt.Println(composite.ColumnNamesOrDefault())

	// Output:
	// [user_id]
	// [order_id sku]
}

// ExampleSynonym_DeclaredTarget shows the two spellings of a SQL Server
// synonym's target. Target is base_object_name exactly as the catalog
// records it, brackets included; DeclaredTarget rebuilds the dot-separated
// declaration spelling from the parsed parts. A three-part target names an
// object in another database, so IsExternal reports true and
// TargetQualifiedName declines to produce a local join key for it.
func ExampleSynonym_DeclaredTarget() {
	local := catalog.Synonym{
		Name:         "orders_alias",
		Schema:       "dbo",
		Target:       "[app].[orders]",
		TargetSchema: "app",
		TargetObject: "orders",
	}
	remote := catalog.Synonym{
		Name:           "gauge",
		Schema:         "dbo",
		Target:         "[other].[dbo].[gauge]",
		TargetDatabase: "other",
		TargetSchema:   "dbo",
		TargetObject:   "gauge",
	}

	for _, synonym := range []catalog.Synonym{local, remote} {
		fmt.Printf("%s -> %s external=%t local-key=%q\n",
			synonym.Name, synonym.DeclaredTarget(), synonym.IsExternal(), synonym.TargetQualifiedName())
	}

	// Output:
	// orders_alias -> app.orders external=false local-key="app.orders"
	// gauge -> other.dbo.gauge external=true local-key=""
}

// ExampleSchemaReader reads a live database into the catalog model, which is
// the CURRENT side of a schema comparison. The reader comes from dbschema,
// which picks the dialect implementation from the URL scheme; an in-memory
// SQLite database keeps the example self-contained. Note the blank-Schema
// convention: the table is in the connection's default schema, so
// QualifiedName returns the bare name.
func ExampleSchemaReader() {
	ctx := context.Background()

	conn := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:"))
	defer dbschema.CloseAndWarn(conn)

	writer := conn.SchemaWriter()
	must.Assert(writer.ExecuteSQL(ctx,
		"CREATE TABLE products (id INTEGER PRIMARY KEY, code VARCHAR(50) NOT NULL)"))

	db, err := conn.Reader().ReadSchemaContext(ctx)
	if err != nil {
		fmt.Println("read:", err)
		return
	}

	info := conn.Info()
	fmt.Printf("dialect=%s schema=%s\n", info.Dialect, info.Schema)
	for _, table := range db.Tables {
		fmt.Println("table:", table.QualifiedName())
		for _, column := range table.Columns {
			fmt.Printf("  %s %s nullable=%s\n", column.Name, column.RawType(), column.IsNullable)
		}
	}

	// Output:
	// dialect=sqlite schema=main
	// table: products
	//   id INTEGER nullable=YES
	//   code VARCHAR(50) nullable=NO
}
