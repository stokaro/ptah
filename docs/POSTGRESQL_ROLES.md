# PostgreSQL Role Management with Ptah

This document describes how to use Ptah's PostgreSQL role management features to create and manage database roles through Go struct annotations.

## Overview

Ptah supports PostgreSQL role creation and management through the `//migrator:schema:role` annotation. This allows you to define database roles alongside your entity definitions, ensuring that roles are created and managed as part of your schema migrations.

## Basic Role Definition

Define roles using the `//migrator:schema:role` annotation:

```go
package entities

// Define basic application roles
//migrator:schema:role name="app_user" login="true" comment="Application user role"
//migrator:schema:role name="admin_user" login="true" superuser="true" comment="Administrator role"
//migrator:schema:role name="readonly_user" login="true" comment="Read-only user role"
type UserRoles struct {
    // This struct serves as a container for role annotations
}
```

## Role Attributes

The following attributes are supported for PostgreSQL roles:

### Required Attributes

- `name`: The name of the role (required)

### Optional Attributes

- `login`: Whether the role can login (default: `false`)
- `password`: Encrypted password for the role (optional)
- `superuser`: Whether the role has superuser privileges (default: `false`)
- `createdb` or `create_db`: Whether the role can create databases (default: `false`)
- `createrole` or `create_role`: Whether the role can create other roles (default: `false`)
- `inherit`: Whether the role inherits privileges from granted roles (default: `true`)
- `replication`: Whether the role can initiate streaming replication (default: `false`)
- `comment`: Optional comment describing the role

## Advanced Role Configurations

### Service Roles

```go
// Service role with database creation privileges
//migrator:schema:role name="service_user" login="true" createdb="true" comment="Service user for automated processes"

// Backup role with replication privileges
//migrator:schema:role name="backup_user" login="true" replication="true" comment="Backup user for replication"

// API role with restricted inheritance
//migrator:schema:role name="api_user" login="true" inherit="false" comment="API user with restricted privileges"
```

### Administrative Roles

```go
// Database administrator with full privileges
//migrator:schema:role name="dba_user" login="true" superuser="true" createdb="true" createrole="true" comment="Database administrator"

// Schema manager with role creation privileges
//migrator:schema:role name="schema_manager" login="true" createrole="true" comment="Schema management role"
```

## Generated SQL

Ptah generates appropriate PostgreSQL SQL statements for role management:

### CREATE ROLE Statements

```sql
-- Application user role
CREATE ROLE app_user WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION;

-- Administrator role with full privileges
CREATE ROLE admin_user WITH LOGIN SUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION;

-- Service user for automated processes
CREATE ROLE service_user WITH LOGIN NOSUPERUSER CREATEDB NOCREATEROLE INHERIT NOREPLICATION;
```

### Role Modifications

When role attributes change between migrations, Ptah generates ALTER ROLE statements:

```sql
-- Enable database creation for existing role
ALTER ROLE service_user CREATEDB;

-- Change login capability
ALTER ROLE api_user NOLOGIN;

-- Update password
ALTER ROLE app_user PASSWORD 'new_encrypted_password';
```

### Role Removal

When roles are removed from the schema, Ptah generates DROP ROLE statements:

```sql
-- Remove unused role (with safety check)
DROP ROLE IF EXISTS old_service_role;
```

## Integration with RLS Policies

Roles can be referenced in Row-Level Security policies:

```go
// Define roles first
//migrator:schema:role name="tenant_user" login="true" comment="Multi-tenant user role"
//migrator:schema:role name="admin_user" login="true" superuser="true" comment="Administrator role"

// Use roles in RLS policies
//migrator:schema:rls:policy name="tenant_isolation" table="users" for="ALL" to="tenant_user" using="tenant_id = current_user_tenant_id()" comment="Tenant isolation policy"
//migrator:schema:rls:policy name="admin_access" table="users" for="ALL" to="admin_user" using="true" comment="Admin full access policy"
```

## Best Practices

### 1. Role Naming

Use descriptive names that clearly indicate the role's purpose:

```go
//migrator:schema:role name="app_readonly" login="true" comment="Application read-only access"
//migrator:schema:role name="app_readwrite" login="true" comment="Application read-write access"
//migrator:schema:role name="app_admin" login="true" superuser="true" comment="Application administrator"
```

### 2. Principle of Least Privilege

Grant only the minimum privileges required:

```go
// Good: Specific privileges for specific purposes
//migrator:schema:role name="backup_service" login="true" replication="true" comment="Backup service role"

// Avoid: Unnecessary superuser privileges
//migrator:schema:role name="backup_service" login="true" superuser="true" comment="Backup service role"
```

### 3. Documentation

Always include meaningful comments:

```go
//migrator:schema:role name="analytics_reader" login="true" comment="Read-only access for analytics and reporting"
//migrator:schema:role name="etl_processor" login="true" createdb="true" comment="ETL process role with database creation for temporary schemas"
```

### 4. Password Management

Avoid hardcoding passwords in annotations. Use environment variables or external secret management:

```go
// Don't include actual passwords in code
//migrator:schema:role name="app_user" login="true" comment="Application user - password set externally"
```

## Cross-Database Compatibility

Role annotations are PostgreSQL-specific and will be ignored when using MySQL or MariaDB dialects. This allows you to maintain the same entity definitions across different database backends.

## Migration Behavior

### Role Creation Order

Roles are created early in the migration process, before functions and RLS policies that might reference them.

### Role Modification

When role attributes change, Ptah generates appropriate ALTER ROLE statements to update the existing roles.

### Role Removal

**Important**: Ptah does NOT automatically remove roles that exist in the database but are not defined in your schema. This is a safety feature to prevent accidental removal of roles that may have been created by DBAs, other applications, or infrastructure setup.

If you need to remove a role, you must do so manually:

```sql
-- Manual role removal (only when you're certain it's safe)
DROP ROLE IF EXISTS old_service_role;
```

### Dependency Management

Ptah automatically handles dependencies between roles and other database objects:

1. Roles are created before functions and policies that reference them
2. Role attributes are modified when changes are detected
3. Roles are never automatically dropped (manual removal required for safety)

## Password Security

**Important**: Passwords in role definitions should be properly encrypted/hashed before being stored in your schema files. Ptah does not automatically encrypt passwords - this should be done at the application level or using PostgreSQL's built-in password encryption.

### Password Best Practices

1. **Never use plaintext passwords** in your schema definitions
2. **Use PostgreSQL's password encryption** when creating roles:
   ```sql
   -- Good: Use encrypted password
   CREATE ROLE app_user WITH LOGIN PASSWORD 'md5a1b2c3d4e5f6789012345678901234';

   -- Bad: Plaintext password (security risk)
   CREATE ROLE app_user WITH LOGIN PASSWORD 'mypassword123';
   ```

3. **Use environment variables** for sensitive passwords in development:
   ```go
   // Use environment variables for passwords
   password := os.Getenv("APP_USER_PASSWORD_HASH")
   ```

4. **Password Detection**: Ptah includes heuristic checks to detect potential plaintext passwords and will add warning comments to generated SQL when suspicious passwords are detected.

### Supported Password Formats

Ptah recognizes these encrypted password formats:
- MD5 hashes: `md5a1b2c3d4e5f6789012345678901234`
- SCRAM-SHA-256: `SCRAM-SHA-256$4096:salt$hash:signature`
- bcrypt: `$2a$10$...`, `$2b$10$...`, `$2y$10$...`
- SHA-256/SHA-512: `$5$...`, `$6$...`

## Testing

Use the integration test framework to verify role functionality:

```bash
# Test PostgreSQL role scenarios
docker-compose --profile test run --rm ptah-tester --scenarios=dynamic_roles_basic,dynamic_roles_advanced --databases=postgres

# Test cross-database compatibility
docker-compose --profile test run --rm ptah-tester --scenarios=dynamic_roles_cross_database
```

## Troubleshooting

### Common Issues

1. **Role already exists**: Ptah handles existing roles gracefully during migrations
2. **Permission denied**: Ensure the migration user has sufficient privileges to create roles
3. **Role dependencies**: Check that roles are not referenced by other database objects before removal

### Debugging

Enable verbose logging to see generated SQL statements:

```bash
# Run with verbose output to see role creation SQL
docker-compose --profile test run --rm ptah-tester --verbose --scenarios=dynamic_roles_basic
```

## Examples

See the integration test fixtures for complete examples:

- `integration/fixtures/entities/016-roles/`: Basic role definitions
- `integration/fixtures/entities/017-roles-advanced/`: Advanced role configurations

These examples demonstrate real-world usage patterns and can serve as templates for your own role definitions.
