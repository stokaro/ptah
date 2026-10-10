package sqlschema

import (
	"fmt"
	"path"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/privilegefold"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbacl"
)

func appendYDBPrivilege(database *schemamodel.Database, document *Document, statement ast.Node) (bool, error) {
	var role, kind, name, verb string
	var privileges []string
	switch node := statement.(type) {
	case *ast.GrantPrivilegeNode:
		role, kind, name, privileges, verb = node.Role, node.ObjectType, node.ObjectName, slices.Clone(node.Privileges), "GRANT"
		if node.WithOption {
			privileges = append(privileges, ydbacl.GrantPermission)
		}
		if len(node.Columns) != 0 || node.Arguments != "" {
			return true, fmt.Errorf("%w: YDB grants cannot name columns or routines", ErrUnmodeledStatement)
		}
	case *ast.RevokePrivilegeNode:
		role, kind, name, privileges, verb = node.Role, node.ObjectType, node.ObjectName, slices.Clone(node.Privileges), "REVOKE"
		if node.GrantOptionFor {
			privileges = append(privileges, ydbacl.GrantPermission)
		}
		if len(node.Columns) != 0 || node.Arguments != "" {
			return true, fmt.Errorf("%w: YDB revokes cannot name columns or routines", ErrUnmodeledStatement)
		}
	default:
		return false, nil
	}
	if role == "" || role != strings.TrimSpace(role) {
		return true, fmt.Errorf("%w: a YDB permission subject cannot be empty or have surrounding whitespace", ErrUnmodeledStatement)
	}
	grant, err := ydbPrivilegeTarget(database, document, kind, name)
	if err != nil {
		return true, err
	}
	// The YQL parser already decoded the subject. Decoding again would turn
	// a literal backtick in a principal name into identifier quoting.
	grant.Role = role
	for _, privilege := range privileges {
		permission, ok := ydbacl.Permission(privilege)
		if !ok {
			return true, fmt.Errorf("%w: unknown YDB permission %q", ErrUnmodeledStatement, privilege)
		}
		grant.Privileges = append(grant.Privileges, permission)
	}
	grant.Canonicalize()
	if verb == "GRANT" {
		privilegefold.Merge(database, &schemamodel.Database{Grants: []schemamodel.Grant{grant}})
	} else {
		privilegefold.Merge(database, &schemamodel.Database{RevokedGrants: []schemamodel.Grant{grant}})
	}
	return true, nil
}

func ydbPrivilegeTarget(database *schemamodel.Database, document *Document, kind, name string) (schemamodel.Grant, error) {
	if kind != ydbacl.ObjectPath {
		grant := schemamodel.Grant{}
		switch kind {
		case ydbacl.ObjectDatabase:
			grant.OnDatabase = true
		case ydbacl.ObjectTable, ydbacl.ObjectDirectory:
			setGrantTarget(&grant, kind, name, "", platform.YDB)
		default:
			return grant, fmt.Errorf("%w: unsupported YDB permission object kind %q", ErrUnmodeledStatement, kind)
		}
		return grant, nil
	}
	relative, err := relativeYDBSourcePath(name, document.YDBDatabasePath)
	if err != nil {
		return schemamodel.Grant{}, err
	}
	if relative == "" {
		return schemamodel.Grant{OnDatabase: true}, nil
	}
	for _, source := range []*schemamodel.Database{database, document.base} {
		if source == nil {
			continue
		}
		for _, table := range source.Tables {
			if path.Join(table.Schema, table.Name) == relative {
				return schemamodel.Grant{OnTable: normalizeSQLTableReference(platform.YDB, sqlident.Quote(platform.YDB, relative))}, nil
			}
		}
		if declaredYDBDirectory(source, relative) {
			return schemamodel.Grant{OnSchema: relative}, nil
		}
	}
	return schemamodel.Grant{}, fmt.Errorf("%w: YDB permission path %q must name a table or directory declared earlier in this desired schema", ErrUnmodeledStatement, name)
}

// An absolute source path is meaningful only with an explicit database root.
// Suffix matching would let a path in a different database bind to a local table.
func relativeYDBSourcePath(name, databasePath string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: empty YDB permission path", ErrUnmodeledStatement)
	}
	for part := range strings.SplitSeq(name, "/") {
		if part == "." || part == ".." {
			return "", fmt.Errorf("%w: YDB source paths cannot traverse directories", ErrUnmodeledStatement)
		}
	}
	name = path.Clean(name)
	if !strings.HasPrefix(name, "/") {
		return name, nil
	}
	if databasePath == "" {
		return "", fmt.Errorf("%w: an absolute YDB source path needs a database URL to identify its database root", ErrUnmodeledStatement)
	}
	root := strings.TrimRight(databasePath, "/")
	if name == root {
		return "", nil
	}
	relative, ok := strings.CutPrefix(name, root+"/")
	if !ok {
		return "", fmt.Errorf("%w: YDB source path %q is outside database %q", ErrUnmodeledStatement, name, root)
	}
	return relative, nil
}

// Every path-bearing YDB family can create its parent directories. The leaf
// object is not a directory: a topic or view cannot become a table grant here.
func declaredYDBDirectory(database *schemamodel.Database, name string) bool {
	var directories []string
	for _, schema := range database.Schemas {
		directories = append(directories, schema.Name)
	}
	for _, object := range database.Tables {
		directories = append(directories, object.Schema)
	}
	for _, object := range database.Views {
		if ref, ok := tableref.Parse(object.Name); ok {
			directories = append(directories, ref.Schema)
		}
	}
	for _, ref := range database.FeatureObjects.Refs() {
		directories = append(directories, ref.Schema.Source)
	}
	for _, object := range database.AsyncReplications {
		directories = append(directories, object.Schema)
	}
	for _, object := range database.Transfers {
		directories = append(directories, object.Schema)
	}
	return slices.ContainsFunc(directories, func(directory string) bool {
		return directory == name || strings.HasPrefix(directory, name+"/")
	})
}
