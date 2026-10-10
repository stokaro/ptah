package schemadiff

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

// renamedIndexOwners is database as the feature comparison reads it: each
// index the plan renames carries its attached settings, and the knowledge the
// read recorded about them, under the name the declaration gives it. Under
// its old name a renamed index's settings would read as one index dropping
// them and another declaring them, so a changed setting would be compared
// against nothing. The plan renames the index before any owner step changes
// it, since an owner step names the index by its new name.
//
// The common children are copied before a name changes; database is not
// modified.
func renamedIndexOwners(database *catalog.Database, renames []difftypes.IndexRename, semantics identifier.Semantics) (*catalog.Database, error) {
	if len(renames) == 0 {
		return database, nil
	}
	builder := objectidentity.NewBuilder(semantics)
	renamed := *database
	renamed.Indexes = slices.Clone(database.Indexes)
	subjects := make(map[objectidentity.Key]objectidentity.ID)
	for i := range renamed.Indexes {
		index := &renamed.Indexes[i]
		position := slices.IndexFunc(renames, func(rename difftypes.IndexRename) bool {
			return rename.From == index.Name &&
				semantics.TableIdentityKey(rename.TableName) == semantics.TableIdentityKey(index.QualifiedTableName())
		})
		if position < 0 {
			continue
		}
		from := builder.IndexParts(index.Schema, index.TableName, index.Name)
		index.Name = renames[position].To
		subjects[from.Key()] = builder.IndexParts(index.Schema, index.TableName, index.Name)
	}
	if len(subjects) == 0 {
		return database, nil
	}
	records := database.FeatureCoverage.SubjectRecords()
	for i, record := range records {
		if to, found := subjects[record.Subject.Key()]; found {
			records[i].Subject = to
		}
	}
	coverage, err := schemaext.NewCoverage(database.FeatureCoverage.Representation(), database.FeatureCoverage.KindRecords(), records)
	if err != nil {
		return nil, err
	}
	renamed.FeatureCoverage = coverage
	return &renamed, nil
}
