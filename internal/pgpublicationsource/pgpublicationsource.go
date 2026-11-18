// Package pgpublicationsource reads PostgreSQL logical replication
// publication declarations for the owner of package pgpublication from the
// sources that can state one: CREATE PUBLICATION and the ALTER PUBLICATION
// statements pg_dump writes after it in SQL, and the publication block in
// HCL. Each frontend hands it the declarations; it returns the publications
// they declare, and claims that the document describes every publication:
// [DocumentCoverage]. Which ones a plan may drop is the comparison's
// decision, made over the whole desired schema rather than one document.
//
// A Go annotation and a YAML document have no spelling for a publication and
// make no claim about one, so a plan from either neither creates nor drops a
// publication.
package pgpublicationsource

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
)

// Targets are the targets a publication declaration is read for: PostgreSQL,
// the one target whose publications the owner models.
func Targets() []string { return []string{platform.Postgres} }

// Declare returns the declaration of the publication name as the owner's
// object. Every source reads a publication through it, so one declaration
// decodes to one object from either of them. A blank name, and a row filter
// or a column list, which no source parses and no writer writes, are refused.
func Declare(name string, publication pgpublication.DesiredPublication) (schemaext.Object, error) {
	if strings.TrimSpace(name) == "" {
		return schemaext.Object{}, fmt.Errorf("%w: a publication declaration requires a name", schemaext.ErrInvalidValue)
	}
	if err := pgpublication.RefuseFiltered(name, &publication); err != nil {
		return schemaext.Object{}, err
	}
	return pgpublication.DesiredObject(pgpublication.Ref(name), publication)
}

// DocumentCoverage is the claim an SQL or HCL document makes about
// publications: it describes every one, so one it leaves out is absent.
//
// The claim does not depend on what the document declares. A schema split
// across files merges the claims of its files conservatively, so a claim that
// held only where a file declares a publication would leave a split schema
// claiming nothing. The comparison reads the merged schema instead and drops
// no publication from one that declares none
// (publicationcompare.Service).
func DocumentCoverage() (schemaext.Coverage, error) {
	return pgpublication.CompleteCoverage(schemaext.Desired)
}
