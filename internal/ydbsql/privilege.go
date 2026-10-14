package ydbsql

import (
	"fmt"
	"path"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/privilegefold"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbacl"
)

// appendPrivilege folds a GRANT or a REVOKE of YDB permissions into the
// grants the load declares.
func appendPrivilege(scope sqlschema.Scope, statement ast.Node) (bool, error) {
	var role, kind, name, verb string
	var privileges []string
	switch node := statement.(type) {
	case *ast.GrantPrivilegeNode:
		role, kind, name, privileges, verb = node.Role, node.ObjectType, node.ObjectName, slices.Clone(node.Privileges), "GRANT"
		if node.WithOption {
			privileges = append(privileges, ydbacl.GrantPermission)
		}
		if len(node.Columns) != 0 || node.Arguments != "" {
			return true, fmt.Errorf("%w: YDB grants cannot name columns or routines", sqlschema.ErrUnmodeledStatement)
		}
	case *ast.RevokePrivilegeNode:
		role, kind, name, privileges, verb = node.Role, node.ObjectType, node.ObjectName, slices.Clone(node.Privileges), "REVOKE"
		if node.GrantOptionFor {
			privileges = append(privileges, ydbacl.GrantPermission)
		}
		if len(node.Columns) != 0 || node.Arguments != "" {
			return true, fmt.Errorf("%w: YDB revokes cannot name columns or routines", sqlschema.ErrUnmodeledStatement)
		}
	default:
		return false, nil
	}
	if role == "" || role != strings.TrimSpace(role) {
		return true, fmt.Errorf("%w: a YDB permission subject cannot be empty or have surrounding whitespace", sqlschema.ErrUnmodeledStatement)
	}
	grant, err := privilegeTarget(scope, kind, name)
	if err != nil {
		return true, err
	}
	// The YQL parser already decoded the subject. Decoding again would turn
	// a literal backtick in a principal name into identifier quoting.
	grant.Role = role
	for _, privilege := range privileges {
		permission, ok := ydbacl.Permission(privilege)
		if !ok {
			return true, fmt.Errorf("%w: unknown YDB permission %q", sqlschema.ErrUnmodeledStatement, privilege)
		}
		grant.Privileges = append(grant.Privileges, permission)
	}
	grant.Canonicalize()
	database := scope.Database()
	if verb == "GRANT" {
		privilegefold.Merge(database, &schemamodel.Database{Grants: []schemamodel.Grant{grant}})
	} else {
		privilegefold.Merge(database, &schemamodel.Database{RevokedGrants: []schemamodel.Grant{grant}})
	}
	return true, nil
}

func privilegeTarget(scope sqlschema.Scope, kind, name string) (schemamodel.Grant, error) {
	if kind != ydbacl.ObjectPath {
		grant := schemamodel.Grant{}
		switch kind {
		case ydbacl.ObjectDatabase:
			grant.OnDatabase = true
		case ydbacl.ObjectDirectory:
			grant.OnSchema = scope.Name(name)
		case ydbacl.ObjectTable:
			grant.OnTable = scope.TableName(name)
		case ydbacl.ObjectResourcePool:
			grant.OnObject, grant.ObjectKind = name, kind
		default:
			if !ydbacl.IsSchemeObject(kind) {
				return grant, fmt.Errorf("%w: unsupported YDB permission object kind %q", sqlschema.ErrUnmodeledStatement, kind)
			}
			grant.OnObject, grant.ObjectKind = scope.TableName(name), kind
		}
		return grant, nil
	}
	relative, err := relativeSourcePath(name, scope.DatabasePath())
	if err != nil {
		return schemamodel.Grant{}, err
	}
	if relative == "" {
		return schemamodel.Grant{OnDatabase: true}, nil
	}
	for _, source := range []*schemamodel.Database{scope.Database(), scope.Base()} {
		if source == nil {
			continue
		}
		for _, table := range source.Tables {
			if path.Join(table.Schema, table.Name) == relative {
				return schemamodel.Grant{OnTable: scope.TableName(sqlident.Quote(platform.YDB, relative))}, nil
			}
		}
		// A grant on a view is a grant on a table, as the reader reports it.
		for _, view := range source.Views {
			if viewPath, err := ydbacl.RelativePath(ydbacl.ObjectTable, view.Name); err == nil && viewPath == relative {
				return schemamodel.Grant{OnTable: view.Name}, nil
			}
		}
		if grant, ok := declaredSchemeObject(source, relative); ok {
			return grant, nil
		}
		if declaredDirectory(source, relative) {
			return schemamodel.Grant{OnSchema: relative}, nil
		}
	}
	return schemamodel.Grant{}, undeclared(fmt.Errorf("%w: YDB permission path %q must name a table, a view, a directory "+
		"or another scheme object, such as a topic or a resource pool, declared earlier in this desired schema",
		sqlschema.ErrUnmodeledStatement, name))
}

// declaredSchemeObject is a grant on the scheme object a feature owner
// declares at relative, of a kind whose permissions the reader reads, and
// false when database declares none there. A resource pool's path is under
// [ydbacl.ResourcePoolDirectory], not at its name.
func declaredSchemeObject(database *schemamodel.Database, relative string) (schemamodel.Grant, bool) {
	for _, ref := range database.FeatureObjects.Refs() {
		objectType, objectName, ok := ydbacl.ObjectName(ref)
		if !ok {
			continue
		}
		if objectPath, err := ydbacl.RelativePath(objectType, objectName); err == nil && objectPath == relative {
			return schemamodel.Grant{OnObject: objectName, ObjectKind: objectType}, true
		}
	}
	return schemamodel.Grant{}, false
}

// An absolute source path is meaningful only with an explicit database root.
// Suffix matching would let a path in a different database bind to a local table.
func relativeSourcePath(name, databasePath string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: empty YDB permission path", sqlschema.ErrUnmodeledStatement)
	}
	for part := range strings.SplitSeq(name, "/") {
		if part == "." || part == ".." {
			return "", fmt.Errorf("%w: YDB source paths cannot traverse directories", sqlschema.ErrUnmodeledStatement)
		}
	}
	name = path.Clean(name)
	if !strings.HasPrefix(name, "/") {
		return name, nil
	}
	if databasePath == "" {
		return "", fmt.Errorf("%w: an absolute YDB source path needs a database URL to identify its database root", sqlschema.ErrUnmodeledStatement)
	}
	root := strings.TrimRight(databasePath, "/")
	if name == root {
		return "", nil
	}
	relative, ok := strings.CutPrefix(name, root+"/")
	if !ok {
		return "", fmt.Errorf("%w: YDB source path %q is outside database %q", sqlschema.ErrUnmodeledStatement, name, root)
	}
	return relative, nil
}

// Every path-bearing YDB family can create its parent directories. The leaf
// object is not a directory: a topic or a view cannot become a directory grant
// here.
func declaredDirectory(database *schemamodel.Database, name string) bool {
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
	return slices.ContainsFunc(directories, func(directory string) bool {
		return directory == name || strings.HasPrefix(directory, name+"/")
	})
}

// undeclared marks err, the refusal of a statement that changes an object the
// load does not declare, as [sqlext.ErrUndeclared], keeping its text.
func undeclared(err error) error {
	return undeclaredError{err: err}
}

// undeclaredError is a refusal [undeclared] marked.
type undeclaredError struct {
	err error
}

func (e undeclaredError) Error() string { return e.err.Error() }

func (e undeclaredError) Unwrap() []error { return []error{e.err, sqlext.ErrUndeclared} }
