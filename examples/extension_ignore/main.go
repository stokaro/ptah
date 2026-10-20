// Package main demonstrates the PostgreSQL extension ignore functionality in Ptah.
//
// This example shows how to use the new configuration API to control which
// PostgreSQL extensions should be ignored during schema migrations.
//
// Example of integrating extension ignore functionality into a real application:
//
//	// Parse your Go entities
//	generated, err := goschema.ParseDir("./models")
//	if err != nil {
//		log.Fatalf("Failed to parse Go entities: %v", err)
//	}
//
//	// Connect to database (supply a context so the call can be canceled).
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	conn, err := dbschema.ConnectToDatabase(ctx, "postgres://user:pass@localhost/db")
//	if err != nil {
//		log.Fatalf("Failed to connect to database: %v", err)
//	}
//	defer dbschema.CloseAndWarn(conn)
//
//	database, err := conn.ReadSchemaContext(ctx)
//	if err != nil {
//		log.Fatalf("Failed to read database schema: %v", err)
//	}
//
//	// Configure extension ignore strategy based on environment
//	var opts *config.CompareOptions
//	switch environment {
//	case "development":
//		opts = config.WithIgnoredExtensions("plpgsql", "adminpack")
//	case "production":
//		opts = config.WithAdditionalIgnoredExtensions("adminpack", "pg_stat_statements")
//	case "testing":
//		opts = config.WithIgnoredExtensions() // manage all extensions
//	default:
//		opts = nil // use Ptah defaults
//	}
//
//	// Compare schemas with environment-specific configuration
//	runtime, err := builtin.New()
//	if err != nil { return err }
//	diff, err := schemadiff.CompareWithDatabase(ctx, conn, generated, database, opts, runtime)
//	if err != nil { return err }
//
//	// Generate migrations based on the differences
//	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
//		Runtime: runtime,
//		CompareOptions: opts,
//		GoEntitiesDir: "./models",
//		DatabaseURL:   "postgres://user:pass@localhost/db",
//		MigrationName: "update_extensions",
//		OutputDir:     "./migrations",
//	})
package main

import (
	"context"
	"fmt"
	"log"
	"slices"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/migration/schemadiff"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	runtime, err := builtin.New()
	if err != nil {
		return err
	}
	fmt.Println("PostgreSQL Extension Ignore Functionality Demo")
	fmt.Println("==============================================")
	fmt.Println()

	// Create sample data for demonstration
	desired, err := createSampleGeneratedSchema()
	if err != nil {
		return err
	}
	database, err := createSampleDatabaseSchema()
	if err != nil {
		return err
	}

	fmt.Println("Sample Data:")
	fmt.Printf("Generated schema extensions: %v\n", pgextension.Names(desired.FeatureObjects))
	fmt.Printf("Database schema extensions: %v\n", pgextension.Names(database.FeatureObjects))
	fmt.Println()

	// Demonstrate different configuration options
	for _, demonstrate := range []func(context.Context, *schemamodel.Database, *catalog.Database, schemapreparation.Runtime) error{
		demonstrateDefaultBehavior, demonstrateCustomIgnoreList,
		demonstrateAdditionalIgnoredExtensions, demonstrateManageAllExtensions,
	} {
		if err := demonstrate(ctx, desired, database, runtime); err != nil {
			return err
		}
	}
	return nil
}

// createSampleGeneratedSchema declares two extensions to the extension owner,
// as a Go //ptah:schema:extension annotation does, and claims that it
// declares every extension.
func createSampleGeneratedSchema() (*schemamodel.Database, error) {
	var objects []schemaext.Object
	for name, comment := range map[string]string{"pg_trgm": "Trigram similarity search", "btree_gin": "GIN indexes for btree types"} {
		object, err := pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{IfNotExists: true, Comment: comment})
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	declared, err := schemaext.NewObjects(objects...)
	if err != nil {
		return nil, err
	}
	known, err := pgextension.CompleteCoverage(schemaext.Desired)
	if err != nil {
		return nil, err
	}
	return &schemamodel.Database{FeatureObjects: declared, FeatureCoverage: known}, nil
}

// createSampleDatabaseSchema holds three installed extensions, as the
// PostgreSQL reader hands them to the extension owner.
func createSampleDatabaseSchema() (*catalog.Database, error) {
	var objects []schemaext.Object
	for _, installed := range []struct{ name, version, schema string }{
		{"plpgsql", "1.0", "pg_catalog"}, {"adminpack", "2.1", "public"}, {"pg_trgm", "1.6", "public"},
	} {
		object, err := pgextension.ObservedObject(pgextension.Ref(installed.name),
			pgextension.ObservedExtension{Version: installed.version, Schema: installed.schema})
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	read, err := schemaext.NewObjects(objects...)
	if err != nil {
		return nil, err
	}
	known, err := pgextension.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return nil, err
	}
	return &catalog.Database{FeatureObjects: read, FeatureCoverage: known}, nil
}

// extensionChanges names the extensions a diff installs and drops, from the
// extension owner's changes.
func extensionChanges(diff *difftypes.SchemaDiff) (added, removed []string) {
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*pgextension.Change)
		if !ok {
			continue
		}
		switch {
		case change.Before == nil:
			added = append(added, pgextension.Name(record.Subject))
		case change.After == nil:
			removed = append(removed, pgextension.Name(record.Subject))
		}
	}
	slices.Sort(added)
	slices.Sort(removed)
	return added, removed
}

func printChanges(diff *difftypes.SchemaDiff) {
	added, removed := extensionChanges(diff)
	fmt.Printf("   Extensions to add: %v\n", added)
	fmt.Printf("   Extensions to remove: %v\n", removed)
}

func demonstrateDefaultBehavior(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, runtime schemapreparation.Runtime) error {
	fmt.Println("1. Default Behavior (ignores 'plpgsql'):")
	fmt.Println(`   Code: schemadiff.CompareWithDialect(ctx, generated, database, "postgres", runtime)`)

	diff, err := schemadiff.CompareWithDialect(ctx, desired, current, "postgres", runtime)
	if err != nil {
		return err
	}

	printChanges(diff)
	fmt.Println("   Note: 'plpgsql' is ignored by default, so it won't be removed")
	fmt.Println()
	return nil
}

func demonstrateCustomIgnoreList(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, runtime schemapreparation.Runtime) error {
	fmt.Println("2. Custom Ignore List (ignore 'adminpack' only):")
	fmt.Println("   Code: config.WithIgnoredExtensions(\"adminpack\")")

	opts := config.WithIgnoredExtensions("adminpack")
	opts.Dialect = "postgres"
	diff, err := schemadiff.CompareWithOptions(ctx, desired, current, opts, runtime)
	if err != nil {
		return err
	}

	printChanges(diff)
	fmt.Println("   Note: 'adminpack' is ignored, but 'plpgsql' will be removed")
	fmt.Println()
	return nil
}

func demonstrateAdditionalIgnoredExtensions(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, runtime schemapreparation.Runtime) error {
	fmt.Println("3. Additional Ignored Extensions (default + 'adminpack'):")
	fmt.Println("   Code: config.WithAdditionalIgnoredExtensions(\"adminpack\")")

	opts := config.WithAdditionalIgnoredExtensions("adminpack")
	opts.Dialect = "postgres"
	diff, err := schemadiff.CompareWithOptions(ctx, desired, current, opts, runtime)
	if err != nil {
		return err
	}

	printChanges(diff)
	fmt.Println("   Note: Both 'plpgsql' and 'adminpack' are ignored")
	fmt.Println()
	return nil
}

func demonstrateManageAllExtensions(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, runtime schemapreparation.Runtime) error {
	fmt.Println("4. Manage All Extensions (no ignoring):")
	fmt.Println("   Code: config.WithIgnoredExtensions() // empty list")

	opts := config.WithIgnoredExtensions() // Empty list - manage everything
	opts.Dialect = "postgres"
	diff, err := schemadiff.CompareWithOptions(ctx, desired, current, opts, runtime)
	if err != nil {
		return err
	}

	printChanges(diff)
	fmt.Println("   Note: All extensions are managed, including 'plpgsql'")
	fmt.Println()
	return nil
}
