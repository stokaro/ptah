# PostgreSQL Extension Ignore Functionality

This document describes the PostgreSQL extension ignore functionality in Ptah, which allows you to configure which extensions should be ignored during schema migrations.

## Overview

Some PostgreSQL extensions (like `plpgsql`) are installed by default in databases and should not be managed by migration systems. The ignore functionality provides:

- **Configurable ignore list**: Specify which extensions to ignore
- **Default behavior**: `plpgsql` is ignored by default
- **Programmatic API**: Clean Go API for library usage
- **Flexible configuration**: Multiple ways to configure the ignore list

## Key Behaviors

When an extension is ignored:
- ✅ **Can be created** during migrations if defined in target schema
- ❌ **Never deleted** even if missing from target schema  
- 🚫 **Excluded from diff calculations** (treated as if it doesn't exist)
- 🔍 **Filtered out** before schema comparison

## Library Usage

### Basic Usage (Default Behavior)

```go
package main

import (
    "github.com/stokaro/ptah/core/goschema"
    "github.com/stokaro/ptah/dbschema"
    "github.com/stokaro/ptah/migration/schemadiff"
)

func main() {
    // Parse your Go entities
    generated, err := goschema.ParseDir("./models")
    if err != nil {
        panic(err)
    }

    // Connect to database and read current schema
    conn, err := dbschema.ConnectToDatabase("postgres://user:pass@localhost/db")
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    database, err := conn.ReadSchema()
    if err != nil {
        panic(err)
    }

    // Compare schemas with default options (ignores "plpgsql")
    diff := schemadiff.Compare(generated, database)
    
    // plpgsql in database will NOT be marked for removal
    fmt.Printf("Extensions to add: %v\n", diff.ExtensionsAdded)
    fmt.Printf("Extensions to remove: %v\n", diff.ExtensionsRemoved)
}
```

### Custom Ignore List

```go
import "github.com/stokaro/ptah/config"

// Ignore specific extensions only
opts := config.WithIgnoredExtensions("plpgsql", "adminpack", "pg_stat_statements")
diff := schemadiff.CompareWithOptions(generated, database, opts)
```

### Add to Default Ignore List

```go
// Keep default (plpgsql) and add more
opts := config.WithAdditionalIgnoredExtensions("adminpack", "pg_stat_statements")
diff := schemadiff.CompareWithOptions(generated, database, opts)
// Result: ignores ["plpgsql", "adminpack", "pg_stat_statements"]
```

### Manage All Extensions

```go
// Don't ignore any extensions (manage everything)
opts := config.WithIgnoredExtensions() // Empty list
diff := schemadiff.CompareWithOptions(generated, database, opts)
// Result: even plpgsql will be managed
```

## Configuration API

### CompareOptions

```go
type CompareOptions struct {
    IgnoredExtensions []string
}
```

### Factory Functions

```go
// Default options (ignores "plpgsql")
opts := config.DefaultCompareOptions()

// Custom ignore list (replaces defaults)
opts := config.WithIgnoredExtensions("plpgsql", "adminpack")

// Add to defaults (keeps "plpgsql" + adds more)
opts := config.WithAdditionalIgnoredExtensions("adminpack")
```

### Utility Methods

```go
// Check if extension is ignored
if opts.IsExtensionIgnored("plpgsql") {
    // Extension will be ignored
}

// Filter a list of extensions
filtered := opts.FilterIgnoredExtensions([]string{"plpgsql", "pg_trgm", "adminpack"})
// Returns only non-ignored extensions
```

## Real-World Examples

### Development Environment

```go
// Development: ignore common pre-installed extensions
opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
diff := schemadiff.CompareWithOptions(generated, database, opts)
```

### Production Environment

```go
// Production: be more conservative, ignore more extensions
opts := config.WithAdditionalIgnoredExtensions(
    "adminpack",
    "pg_stat_statements", 
    "pg_buffercache",
)
diff := schemadiff.CompareWithOptions(generated, database, opts)
```

### Testing Environment

```go
// Testing: manage all extensions for complete control
opts := config.WithIgnoredExtensions() // Empty - manage everything
diff := schemadiff.CompareWithOptions(generated, database, opts)
```

## Migration Generation

The ignore functionality automatically integrates with migration generation:

```go
import "github.com/stokaro/ptah/migration/generator"

// Generate migration with custom extension ignore options
opts := generator.GenerateMigrationOptions{
    GoEntitiesDir: "./models",
    DatabaseURL:   "postgres://user:pass@localhost/db",
    MigrationName: "update_schema",
    OutputDir:     "./migrations",
    // Extension ignore options will be supported in future versions
}

files, err := generator.GenerateMigration(opts)
```

## Common Extensions to Ignore

- **`plpgsql`**: PostgreSQL procedural language (usually pre-installed)
- **`adminpack`**: Administrative functions (often pre-installed)
- **`pg_stat_statements`**: Query statistics (monitoring extension)
- **`pg_buffercache`**: Buffer cache inspection (monitoring extension)

## Best Practices

1. **Start with defaults**: Use `schemadiff.Compare()` for most cases
2. **Be explicit in production**: Use `CompareWithOptions()` with explicit ignore lists
3. **Document your choices**: Comment why specific extensions are ignored
4. **Test thoroughly**: Verify ignore behavior in your test environment
5. **Review regularly**: Periodically review your ignore list as your system evolves

## Backward Compatibility

- Existing code using `schemadiff.Compare()` continues to work unchanged
- Default behavior ignores `plpgsql` (safe for most PostgreSQL installations)
- New `CompareWithOptions()` function provides full control when needed
