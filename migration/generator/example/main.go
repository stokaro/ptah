// Command example demonstrates embedding go.5x5.cz/ptah/migration/generator
// in a standalone program: it compares annotated Go entities against a live
// database, publishes the migration pair, and prints what was written. The
// package's testable examples are the reference for the API contracts; this
// program shows the same call wired to command-line arguments.
package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"go.5x5.cz/ptah/migration/generator"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run main.go <entities_dir> <database_url> <output_dir> [migration_name]")
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println("  go run main.go ./entities sqlite:///path/to/app.db ./migrations")
		fmt.Println("  go run main.go ./entities postgres://user:pass@localhost/db ./migrations add_users_table")
		fmt.Println("")
		fmt.Println("The database URL takes any scheme dbschema.ConnectToDatabase accepts,")
		fmt.Println("for example sqlite://, postgres://, mysql://, or mariadb://.")
		os.Exit(1)
	}

	entitiesDir := os.Args[1]
	databaseURL := os.Args[2]
	outputDir := os.Args[3]

	migrationName := "migration" // default
	if len(os.Args) > 4 {
		migrationName = os.Args[4]
	}

	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   databaseURL,
		MigrationName: migrationName,
		OutputDir:     outputDir,
	}

	fmt.Printf("Generating migration...\n")
	fmt.Printf("  Entities directory: %s\n", entitiesDir)
	fmt.Printf("  Database URL: %s\n", maskPassword(databaseURL))
	fmt.Printf("  Output directory: %s\n", outputDir)
	fmt.Printf("  Migration name: %s\n", migrationName)
	fmt.Println()

	// Bound database access, planning, lock acquisition, and publication.
	//
	// We cancel synchronously rather than via `defer` because the failure
	// path below uses log.Fatalf, which calls os.Exit and skips deferreds.
	// The OS reclaims the timer when the process exits, so this is safe.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	files, err := generator.GenerateMigration(ctx, opts)
	cancel()
	if err != nil {
		log.Fatalf("Error generating migration: %v", err)
	}

	// Nil files with a nil error means the comparison found no changes.
	if files == nil {
		fmt.Println("No schema changes detected - no migration needed.")
		fmt.Println("The database schema is already in sync with your Go entities.")
		return
	}

	fmt.Println("Migration generated successfully.")
	for _, pair := range files.Files {
		fmt.Printf("  Version: %d\n", pair.Version)
		fmt.Printf("  UP file:   %s\n", pair.UpFile)
		fmt.Printf("  DOWN file: %s\n", pair.DownFile)
		if pair.ReportFile != "" {
			fmt.Printf("  Report:    %s\n", pair.ReportFile)
		}
		if pair.NoTransaction {
			fmt.Println("  Transaction: disabled")
		}
	}
	fmt.Println()

	for _, pair := range files.Files {
		fmt.Printf("=== UP MIGRATION %d ===\n", pair.Version)
		upContent, err := os.ReadFile(pair.UpFile)
		if err != nil {
			log.Printf("Warning: Could not read UP migration file: %v", err)
		} else {
			fmt.Println(string(upContent))
		}

		fmt.Printf("=== DOWN MIGRATION %d ===\n", pair.Version)
		downContent, err := os.ReadFile(pair.DownFile)
		if err != nil {
			log.Printf("Warning: Could not read DOWN migration file: %v", err)
		} else {
			fmt.Println(string(downContent))
		}
	}

	fmt.Println("Review the generated SQL carefully before applying the migration.")
	fmt.Println()
	fmt.Println("To apply the migration:")
	fmt.Printf("  ptah migrations up --db-url %s --migrations-dir %s\n", maskPassword(databaseURL), outputDir)
	fmt.Println()
	fmt.Println("To roll back the migration:")
	fmt.Printf("  ptah migrations down --db-url %s --migrations-dir %s --target <previous_version>\n", maskPassword(databaseURL), outputDir)
}

// maskPassword masks the password component of a database URL for display, so
// the program never echoes a credential into a terminal or a log.
func maskPassword(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.User == nil {
		return rawURL
	}
	if _, hasPassword := parsed.User.Password(); !hasPassword {
		return rawURL
	}
	parsed.User = url.UserPassword(parsed.User.Username(), "****")
	return parsed.String()
}
