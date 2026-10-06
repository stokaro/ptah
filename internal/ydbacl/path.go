package ydbacl

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/tableref"
)

// The object kinds a YDB grant names, as [ptah.run/core/ast.GrantPrivilegeNode]
// and [ptah.run/catalog.Grant] spell them. A Ptah schema is a directory on YDB,
// so a grant on a directory is a SCHEMA grant, and the database root is the
// directory every schema lives under.
const (
	// ObjectPath is an untyped YQL path. The source reader resolves its object
	// kind before building the schema model; only YDB may render it directly.
	ObjectPath = "PATH"
	// ObjectTable is a row table.
	ObjectTable = "TABLE"
	// ObjectDirectory is a directory: a Ptah schema.
	ObjectDirectory = "SCHEMA"
	// ObjectDatabase is the database itself, the root of its directory tree.
	ObjectDatabase = "DATABASE"
)

// ErrNeedsDatabasePath reports a grant whose object YDB takes only by its
// absolute path, which begins with the database's own and which a render with
// no database connection cannot know.
var ErrNeedsDatabasePath = errors.New("YDB takes this object only by its absolute path, " +
	"which begins with the database's own path")

// RelativePath is the path of the object a grant names, relative to the
// database root, and "" for the database itself.
//
// objectName is what the grant carries: a table reference in Ptah's
// schema-qualified spelling, such as `shop.orders` (the table orders in the
// directory shop), a directory, such as `shop/eu`, or nothing for the
// database. A name that begins with a slash is already an absolute path and is
// returned as it is.
func RelativePath(objectType, objectName string) (string, error) {
	if strings.HasPrefix(objectName, "/") && strings.Trim(objectName, "/") != "" {
		return objectName, nil
	}
	switch strings.ToUpper(strings.TrimSpace(objectType)) {
	case ObjectPath:
		if objectName == "" {
			return "", errors.New("a YDB permission names an empty path")
		}
		return objectName, nil
	case ObjectDatabase:
		if strings.TrimSpace(objectName) != "" {
			return "", fmt.Errorf("a grant on the database names the database %q, "+
				"which is the database the connection opens", objectName)
		}
		return "", nil
	case ObjectDirectory:
		directory := strings.Trim(objectName, "/")
		if directory == "" {
			return "", errors.New("a grant on a directory names no directory; " +
				"the database root is the database itself")
		}
		return directory, nil
	case ObjectTable:
		ref, ok := tableref.Parse(objectName)
		if !ok || ref.Name == "" {
			return "", fmt.Errorf("a grant on a table names %q, which is not a table", objectName)
		}
		if schema := strings.Trim(ref.Schema, "/"); schema != "" {
			return schema + "/" + ref.Name, nil
		}
		return ref.Name, nil
	default:
		return "", fmt.Errorf("a YDB grant is on the database, a directory or a table, not a %s", objectType)
	}
}

// StatementPath is the path a GRANT or REVOKE writes for the object a grant
// names: relative to the database root where the server resolves it, and the
// absolute path under databasePath where it does not.
//
// Measured: every line from 25.1.4.7 to 26.2.1.14 resolves a relative path
// with a directory part against the database root, and none applies `PRAGMA
// TablePathPrefix` to a grant. 26.1.1.22 and 26.2.1.14 resolve a single name
// too; 25.1 to 25.4 answer `wrong path format 'name'`.
// [capability.RelativeGrantPaths] in caps says which the target does. No line resolves a relative spelling of the
// database itself (`.` answers `Path does not exist`, and the empty name
// `Empty basename`).
//
// A relative path is preferred because it names the same object in every
// database: a migration file planned against one database applies to another
// of a different name. An empty databasePath is a render with no database
// connection, and an object only an absolute path reaches is then refused
// with [ErrNeedsDatabasePath].
func StatementPath(objectType, objectName, databasePath string, caps capability.Capabilities) (string, error) {
	relative, err := RelativePath(objectType, objectName)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(relative, "/") {
		return relative, nil
	}
	if relative != "" && (caps.Has(capability.RelativeGrantPaths) || strings.Contains(relative, "/")) {
		return relative, nil
	}
	database := strings.TrimRight(databasePath, "/")
	if database == "" {
		return "", ErrNeedsDatabasePath
	}
	if relative == "" {
		return database, nil
	}
	return database + "/" + relative, nil
}

// Describe names the object a grant is about, for a message a person reads:
// the database, a directory, or a table.
func Describe(objectType, objectName string) string {
	switch strings.ToUpper(strings.TrimSpace(objectType)) {
	case ObjectDatabase:
		return "the database"
	case ObjectDirectory:
		return "directory " + objectName
	default:
		return strings.ToLower(strings.TrimSpace(objectType)) + " " + objectName
	}
}
