package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// declaredTargetKind returns the object kind a grant or revoke on objectName
// targets: SEQUENCE where objectType is TABLE and the name reaches a sequence
// the schema declares so far, on a dialect whose GRANT names a sequence as it
// names a table, and objectType otherwise. statement is GRANT or REVOKE, for
// the refusal of [grantOnSequence].
//
// PostgreSQL takes `GRANT SELECT ON s1` and `GRANT SELECT ON TABLE s1` on a
// sequence, and Oracle's GRANT has no object kind at all, while the read of
// either reports the grant under SEQUENCE. Left on the table name, the
// declared grant does not match the one read: the plan grants it and revokes
// the sequence grant in the same run, and the role ends up holding nothing on
// the sequence.
//
// The kind is settled as the statement is read, so a later REVOKE in either
// spelling takes back an earlier GRANT. A GRANT before its CREATE SEQUENCE is
// moved once the file is converted; see [resolveSequenceGrants].
func declaredTargetKind(
	database, base *schemamodel.Database, statement, objectType, objectName string, columns []string, sourcePlatform string,
) (string, error) {
	if !strings.EqualFold(objectType, "TABLE") || len(columns) > 0 || !factsOf(sourcePlatform).sequenceGrantsByName {
		return objectType, nil
	}
	onSequence, err := grantOnSequence(modelsSoFar(database, base), statement, normalizeSQLTableReference(sourcePlatform, objectName), sourcePlatform)
	if err != nil || !onSequence {
		return objectType, err
	}
	return "SEQUENCE", nil
}

// resolveSequenceGrants moves each grant and revoke on a table name that
// reaches a sequence the file declares after it onto the sequence, by the rule
// of [declaredTargetKind].
func resolveSequenceGrants(database, base *schemamodel.Database, sourcePlatform string) error {
	if !factsOf(sourcePlatform).sequenceGrantsByName {
		return nil
	}
	databases := modelsSoFar(database, base)
	declared := []struct {
		statement string
		grants    []schemamodel.Grant
	}{{"GRANT", database.Grants}, {"REVOKE", database.RevokedGrants}}
	for _, of := range declared {
		for i := range of.grants {
			grant := &of.grants[i]
			if grant.OnTable == "" || len(grant.Columns) > 0 {
				continue
			}
			onSequence, err := grantOnSequence(databases, of.statement, grant.OnTable, sourcePlatform)
			if err != nil {
				return err
			}
			if onSequence {
				grant.OnSequence, grant.OnTable = grant.OnTable, ""
			}
		}
	}
	return nil
}

// grantTarget is a declared object a grant on a table name can reach.
type grantTarget struct {
	// kind is "sequence", "table" or "view".
	kind string
	// name is the qualified name the declaration writes.
	name string
}

// grantOnSequence reports whether a grant on the table name qualified reaches
// a declared sequence rather than a declared table or view.
//
// A name that matches declarations as written, compared as
// [resolveDeclaredName] compares names and with a name in the dialect's
// default schema read as unqualified, reaches those: a table or a view among
// them keeps the grant on the table.
//
// Where the connection decides the schema, as on Oracle, an unqualified name
// is in the schema of whoever runs the file. So a name that matches nothing as
// written reaches an object of that name in a schema the other spelling writes
// out: `ptah db read` writes `CREATE SEQUENCE S1` beside `GRANT SELECT ON
// APP.S1`. Where more than one object matches that way and one of them is a
// sequence, the account decides which the grant reaches, and the grant is
// refused.
func grantOnSequence(databases []*schemamodel.Database, statement, qualified, sourcePlatform string) (bool, error) {
	targets := grantTargets(databases)
	written := unqualified(qualified, sourcePlatform)
	var exact []grantTarget
	for _, target := range targets {
		if resolveDeclaredName(sourcePlatform, written, []string{unqualified(target.name, sourcePlatform)}) == 0 {
			exact = append(exact, target)
		}
	}
	if len(exact) > 0 {
		return !slices.ContainsFunc(exact, isRelation), nil
	}
	loose := connectionSchemaMatches(targets, qualified, sourcePlatform)
	if !slices.ContainsFunc(loose, isSequence) {
		return false, nil
	}
	if len(loose) == 1 {
		return true, nil
	}
	described := make([]string, 0, len(loose))
	for _, target := range loose {
		described = append(described, "the "+target.kind+" "+target.name)
	}
	return false, fmt.Errorf("%s ON %s can reach %s, by the schema of the account that runs the file: write the schema in the name",
		statement, qualified, strings.Join(described, " or "))
}

// connectionSchemaMatches returns the targets that qualified reaches by its
// last part where one of the two names writes a schema and the other leaves it
// to the connection, on a dialect with no default schema. A dialect with one
// reads an unqualified name in that schema, which [unqualified] covers.
func connectionSchemaMatches(targets []grantTarget, qualified, sourcePlatform string) []grantTarget {
	written, parsed := tableref.Parse(qualified)
	if !parsed || factsOf(sourcePlatform).defaultSchema != "" {
		return nil
	}
	var matches []grantTarget
	for _, target := range targets {
		ref, ok := tableref.Parse(target.name)
		if ok && ref.Qualified != written.Qualified && resolveDeclaredName(sourcePlatform, written.Name, []string{ref.Name}) == 0 {
			matches = append(matches, target)
		}
	}
	return matches
}

// grantTargets lists the sequences, tables and views databases declare.
func grantTargets(databases []*schemamodel.Database) []grantTarget {
	var targets []grantTarget
	for _, database := range databases {
		for _, sequence := range database.Sequences {
			targets = append(targets, grantTarget{kind: "sequence", name: sequence.QualifiedName()})
		}
		for _, table := range database.Tables {
			targets = append(targets, grantTarget{kind: "table", name: table.QualifiedName()})
		}
		for _, view := range database.Views {
			targets = append(targets, grantTarget{kind: "view", name: view.Name})
		}
	}
	return targets
}

func isSequence(target grantTarget) bool { return target.kind == "sequence" }

func isRelation(target grantTarget) bool { return target.kind != "sequence" }

// modelsSoFar is the models a name may resolve in: the file being read, and the
// files before it.
func modelsSoFar(database, base *schemamodel.Database) []*schemamodel.Database {
	if base == nil {
		return []*schemamodel.Database{database}
	}
	return []*schemamodel.Database{database, base}
}
