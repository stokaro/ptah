// Package main demonstrates the PostgreSQL extension ignore functionality in Ptah.
//
// This example shows how to use the new configuration API to control which
// PostgreSQL extensions should be ignored during schema migrations.
package main

import (
	"fmt"
	"log"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func main() {
	fmt.Println("PostgreSQL Extension Ignore Functionality Demo")
	fmt.Println("==============================================")
	fmt.Println()

	// Create sample data for demonstration
	generated := createSampleGeneratedSchema()
	database := createSampleDatabaseSchema()

	fmt.Println("Sample Data:")
	fmt.Printf("Generated schema extensions: %v\n", getExtensionNames(generated.Extensions))
	fmt.Printf("Database schema extensions: %v\n", getDatabaseExtensionNames(database.Extensions))
	fmt.Println()

	// Demonstrate different configuration options
	demonstrateDefaultBehavior(generated, database)
	demonstrateCustomIgnoreList(generated, database)
	demonstrateAdditionalIgnoredExtensions(generated, database)
	demonstrateManageAllExtensions(generated, database)
}

func createSampleGeneratedSchema() *goschema.Database {
	return &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true, Comment: "Trigram similarity search"},
			{Name: "btree_gin", IfNotExists: true, Comment: "GIN indexes for btree types"},
		},
	}
}

func createSampleDatabaseSchema() *types.DBSchema {
	return &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
		},
	}
}

func getExtensionNames(extensions []goschema.Extension) []string {
	names := make([]string, len(extensions))
	for i, ext := range extensions {
		names[i] = ext.Name
	}
	return names
}

func getDatabaseExtensionNames(extensions []types.DBExtension) []string {
	names := make([]string, len(extensions))
	for i, ext := range extensions {
		names[i] = ext.Name
	}
	return names
}

func demonstrateDefaultBehavior(generated *goschema.Database, database *types.DBSchema) {
	fmt.Println("1. Default Behavior (ignores 'plpgsql'):")
	fmt.Println("   Code: schemadiff.Compare(generated, database)")

	diff := schemadiff.Compare(generated, database)

	fmt.Printf("   Extensions to add: %v\n", diff.ExtensionsAdded)
	fmt.Printf("   Extensions to remove: %v\n", diff.ExtensionsRemoved)
	fmt.Println("   Note: 'plpgsql' is ignored by default, so it won't be removed")
	fmt.Println()
}

func demonstrateCustomIgnoreList(generated *goschema.Database, database *types.DBSchema) {
	fmt.Println("2. Custom Ignore List (ignore 'adminpack' only):")
	fmt.Println("   Code: config.WithIgnoredExtensions(\"adminpack\")")

	opts := config.WithIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	fmt.Printf("   Extensions to add: %v\n", diff.ExtensionsAdded)
	fmt.Printf("   Extensions to remove: %v\n", diff.ExtensionsRemoved)
	fmt.Println("   Note: 'adminpack' is ignored, but 'plpgsql' will be removed")
	fmt.Println()
}

func demonstrateAdditionalIgnoredExtensions(generated *goschema.Database, database *types.DBSchema) {
	fmt.Println("3. Additional Ignored Extensions (default + 'adminpack'):")
	fmt.Println("   Code: config.WithAdditionalIgnoredExtensions(\"adminpack\")")

	opts := config.WithAdditionalIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	fmt.Printf("   Extensions to add: %v\n", diff.ExtensionsAdded)
	fmt.Printf("   Extensions to remove: %v\n", diff.ExtensionsRemoved)
	fmt.Println("   Note: Both 'plpgsql' and 'adminpack' are ignored")
	fmt.Println()
}

func demonstrateManageAllExtensions(generated *goschema.Database, database *types.DBSchema) {
	fmt.Println("4. Manage All Extensions (no ignoring):")
	fmt.Println("   Code: config.WithIgnoredExtensions() // empty list")

	opts := config.WithIgnoredExtensions() // Empty list - manage everything
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	fmt.Printf("   Extensions to add: %v\n", diff.ExtensionsAdded)
	fmt.Printf("   Extensions to remove: %v\n", diff.ExtensionsRemoved)
	fmt.Println("   Note: All extensions are managed, including 'plpgsql'")
	fmt.Println()
}

// Example of how you might use this in a real application
func realWorldExample() {
	// This function demonstrates how you might integrate the extension ignore
	// functionality into a real application

	fmt.Println("Real-World Integration Example:")
	fmt.Println("==============================")

	// Parse your Go entities
	generated, err := goschema.ParseDir("./models")
	if err != nil {
		log.Fatalf("Failed to parse Go entities: %v", err)
	}

	// Connect to database (this would be a real connection)
	// conn, err := dbschema.ConnectToDatabase("postgres://user:pass@localhost/db")
	// if err != nil {
	//     log.Fatalf("Failed to connect to database: %v", err)
	// }
	// defer conn.Close()

	// database, err := conn.ReadSchema()
	// if err != nil {
	//     log.Fatalf("Failed to read database schema: %v", err)
	// }

	// For demo purposes, create a mock database schema
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Different environments might have different ignore strategies
	var opts *config.CompareOptions

	environment := "production" // This could come from env var or config
	switch environment {
	case "development":
		// Development: ignore common pre-installed extensions
		opts = config.WithIgnoredExtensions("plpgsql", "adminpack")
	case "production":
		// Production: be conservative, ignore monitoring extensions too
		opts = config.WithAdditionalIgnoredExtensions("adminpack", "pg_stat_statements")
	case "testing":
		// Testing: manage all extensions for complete control
		opts = config.WithIgnoredExtensions()
	default:
		// Default: use Ptah defaults
		opts = nil
	}

	// Compare schemas with environment-specific configuration
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	fmt.Printf("Environment: %s\n", environment)
	fmt.Printf("Extensions to add: %v\n", diff.ExtensionsAdded)
	fmt.Printf("Extensions to remove: %v\n", diff.ExtensionsRemoved)

	// You could then use this diff to generate migrations
	// files, err := generator.GenerateMigration(generator.GenerateMigrationOptions{
	//     RootDir:       "./models",
	//     DatabaseURL:   "postgres://user:pass@localhost/db",
	//     MigrationName: "update_extensions",
	//     OutputDir:     "./migrations",
	// })
}
