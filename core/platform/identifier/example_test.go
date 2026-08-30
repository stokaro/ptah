package identifier_test

import (
	"fmt"

	"go.5x5.cz/ptah/core/platform/identifier"
)

// ExampleForDialect contrasts two dialects' offline rules: SQLite folds ASCII
// case, so Users and users are one table, while PostgreSQL keeps them apart.
// A key is a token to compare against another key, not a value to read, which
// is why the question is asked as an equality. DefaultSchema is part of the
// answer too — it is what unqualified names are resolved against.
func ExampleForDialect() {
	sqlite := identifier.ForDialect("sqlite")
	postgres := identifier.ForDialect("postgres")

	fmt.Println("sqlite Users is users:",
		sqlite.TableIdentityKey("Users") == sqlite.TableIdentityKey("users"))
	fmt.Println("postgres Users is users:",
		postgres.TableIdentityKey("Users") == postgres.TableIdentityKey("users"))
	fmt.Println("default schema:", sqlite.DefaultSchema, postgres.DefaultSchema)

	// Output:
	// sqlite Users is users: true
	// postgres Users is users: false
	// default schema: main public
}

// ExampleSemantics_QualifiedTableIdentityKey shows why comparators key tables
// through the qualified entry point: an unqualified name is resolved against
// DefaultSchema, so users and public.users are the same object under
// PostgreSQL semantics even though their spellings differ, while the same
// table name in another schema stays a different object.
func ExampleSemantics_QualifiedTableIdentityKey() {
	semantics := identifier.ForDialect("postgres")

	unqualified := semantics.QualifiedTableIdentityKey("users")

	fmt.Println("users is public.users:",
		unqualified == semantics.QualifiedTableIdentityKey("public.users"))
	fmt.Println("users is audit.users:",
		unqualified == semantics.QualifiedTableIdentityKey("audit.users"))

	// Output:
	// users is public.users: true
	// users is audit.users: false
}

// ExampleForSQLServerCatalog walks the live-catalog flow: seed the semantics
// with the equivalence classes the target resolved under its collation, then
// ask the two questions the package separates. Users and users share a
// resolved class, so they are one object. Orders is a name the catalog did
// not resolve — a table the database does not have yet — so it keeps a
// distinct identity while its conflict key stays conservative: two
// unresolved spellings are different objects that may still collide.
func ExampleForSQLServerCatalog() {
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "Users", Key: "Users"},
			{Name: "users", Key: "Users"},
		})

	fmt.Println("Users same object as users:", semantics.TableIdentityKey("Users") == semantics.TableIdentityKey("users"))
	fmt.Println("Orders resolved:", semantics.Resolves("Orders"))
	fmt.Println("Orders same object as ORDERS:", semantics.TableIdentityKey("Orders") == semantics.TableIdentityKey("ORDERS"))
	fmt.Println("Orders may collide with ORDERS:", semantics.TableConflictKey("Orders") == semantics.TableConflictKey("ORDERS"))

	// Output:
	// Users same object as users: true
	// Orders resolved: false
	// Orders same object as ORDERS: false
	// Orders may collide with ORDERS: true
}

// ExampleSemantics_Normalize is the safety valve for semantics that crossed a
// serialization boundary. A partial value — here one naming only a comparison
// mode — cannot be trusted, so Normalize discards it for the conservative
// ForDialect rules; a complete value survives untouched, and the dialect
// argument is only the fallback, not an override.
func ExampleSemantics_Normalize() {
	partial := identifier.Semantics{TableNames: identifier.ComparisonExact}
	fmt.Println("partial falls back:", partial.Normalize("postgres").Equal(identifier.ForDialect("postgres")))

	complete := identifier.ForDialect("sqlite")
	fmt.Println("complete survives:", complete.Normalize("postgres").Equal(complete))

	// Output:
	// partial falls back: true
	// complete survives: true
}
