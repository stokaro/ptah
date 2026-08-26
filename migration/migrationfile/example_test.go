package migrationfile_test

import (
	"fmt"

	"go.5x5.cz/ptah/migration/migrationfile"
)

// ExampleParseFileName demonstrates parsing migration file names
func ExampleParseFileName() {
	filenames := []string{
		"0000000001_create_users_table.up.sql",
		"0000000002_add_email_index.down.sql",
		"invalid_filename.sql",
	}

	for _, filename := range filenames {
		migrationFile, err := migrationfile.ParseFileName(filename)
		if err != nil {
			fmt.Printf("Invalid filename: %s\n", filename)
			continue
		}

		fmt.Printf("File: %s\n", filename)
		fmt.Printf("  Version: %d\n", migrationFile.Version)
		fmt.Printf("  Name: %s\n", migrationFile.Name)
		fmt.Printf("  Direction: %s\n", migrationFile.Direction)
	}

	// Output:
	// File: 0000000001_create_users_table.up.sql
	//   Version: 1
	//   Name: Create Users Table
	//   Direction: up
	// File: 0000000002_add_email_index.down.sql
	//   Version: 2
	//   Name: Add Email Index
	//   Direction: down
	// Invalid filename: invalid_filename.sql
}

// ExampleFileName demonstrates generating migration file names
func ExampleFileName() {
	// Generate filenames for a new migration
	var version int64 = 20240101120000
	description := "Add User Preferences Table"

	upFilename := migrationfile.FileName(version, description, "up")
	downFilename := migrationfile.FileName(version, description, "down")

	fmt.Printf("Up migration file: %s\n", upFilename)
	fmt.Printf("Down migration file: %s\n", downFilename)

	// Output:
	// Up migration file: 20240101120000_add_user_preferences_table.up.sql
	// Down migration file: 20240101120000_add_user_preferences_table.down.sql
}
