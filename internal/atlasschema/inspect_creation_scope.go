package atlasschema

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// databaseObjects are the objects a read of some schemas holds that belong to
// the database rather than to any of those schemas, which the creation plan
// of the read leaves out; see [inspectCreationPlan].
type databaseObjects struct {
	// grants are the grants on the database itself.
	grants []catalog.Grant
	// objects are the objects of the whole database installed in a schema
	// the read does not cover, and installed names each of them.
	objects   schemaext.Objects
	installed []installedObject
}

// installedObject names an object of the whole database and the schema the
// database installed it in.
type installedObject struct {
	kind, name, schema string
}

// databaseObjectsOutsideScope collects the objects of read that belong to the
// database rather than to a schema read covers.
//
// A grant on the database names no schema at all. An object installed into a
// schema, as PostgreSQL installs an extension, belongs to the whole database
// and is kept by every schema scope; it is outside this one when the schema it
// is installed in is not a schema the read covers. One installed in a schema
// the read covers is created with that schema. An extension the comparison
// ignores is planned by no scope, so it is not named.
func databaseObjectsOutsideScope(read *catalog.Database, options *config.CompareOptions) (databaseObjects, error) {
	var found databaseObjects
	for _, grant := range read.Grants {
		if strings.EqualFold(grant.ObjectType, "DATABASE") {
			found.grants = append(found.grants, grant)
		}
	}
	covered := make(map[string]bool, len(read.Schemas))
	for _, schema := range read.Schemas {
		covered[schema.Name] = true
	}
	objects, err := read.FeatureObjects.All()
	if err != nil {
		return databaseObjects{}, err
	}
	outside := make(map[objectidentity.Key]bool)
	for _, object := range objects {
		installation, ok := object.Value.(schemaext.SchemaInstallation)
		if !ok {
			continue
		}
		schema := installation.InstalledSchema()
		if schema == "" || covered[schema] {
			continue
		}
		outside[object.Ref.Key()] = true
		if !ignoredExtension(object.Ref, options) {
			found.installed = append(found.installed, installedObject{kind: kindNoun(object.Ref.Kind), name: object.Ref.Name.Source, schema: schema})
		}
	}
	found.objects = read.FeatureObjects.Select(func(ref objectidentity.ID) bool { return outside[ref.Key()] })
	return found, nil
}

// ignoredExtension reports whether ref is an extension options tells the
// comparison to leave alone.
func ignoredExtension(ref objectidentity.ID, options *config.CompareOptions) bool {
	return ref.Kind == objectidentity.Kind(pgextension.ExtensionKind) && slices.Contains(options.IgnoredExtensions, pgextension.Name(ref))
}

// kindNoun is the last path segment of a feature kind, the noun a reader
// knows the object by: "extension" for ptah.run/pgextension/extension.
func kindNoun(kind objectidentity.Kind) string {
	noun := string(kind)
	if slash := strings.LastIndex(noun, "/"); slash >= 0 {
		noun = noun[slash+1:]
	}
	return noun
}

// report writes one note to w naming the objects the plan left out, and
// nothing when there are none or w is nil.
func (d databaseObjects) report(w io.Writer) {
	if w == nil {
		return
	}
	var items []string
	var roles []string
	for _, grant := range d.grants {
		if role := strconv.Quote(grant.Role); !slices.Contains(roles, role) {
			roles = append(roles, role)
		}
	}
	if len(roles) > 0 {
		slices.Sort(roles)
		items = append(items, "the grants on the database to "+strings.Join(roles, ", "))
	}
	for _, object := range d.installed {
		items = append(items, fmt.Sprintf("%s %q in schema %q", object.kind, object.name, object.schema))
	}
	if len(items) == 0 {
		return
	}
	fmt.Fprintf(w, "note: the read covers some schemas of the database, so the plan creates what they hold and leaves out "+
		"what belongs to the database: %s; inspect the whole database to plan them\n", strings.Join(items, "; "))
}
