package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpublication"
)

// ownedPublicationFixture declares a publication of one table and one schema,
// with a publish set, both settings and an owner, every declared field set
// that a membership of named tables can hold, so the census measures each
// one. The table is outside the default schema, so the member's schema is
// one a statement has to write.
func ownedPublicationFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t", Schema: "app"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgpublication.DesiredObject(pgpublication.Ref("feed"),
		pgpublication.DesiredPublication{
			Tables: []pgpublication.Member{{Schema: "app", Table: "t"}}, Schemas: []string{"audit"},
			Publish: []string{"insert"}, ViaPartitionRoot: true, GeneratedColumns: pgpublication.GeneratedColumnsStored, Owner: "replicator",
		}))))
	db.FeatureCoverage = must.Must(pgpublication.CompleteCoverage(schemaext.Desired))
	return db
}

// allTablesPublicationFixture declares the membership form the fixture above
// cannot hold beside its tables: every table of the database.
func allTablesPublicationFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgpublication.DesiredObject(pgpublication.Ref("everything"),
		pgpublication.DesiredPublication{AllTables: true}))))
	db.FeatureCoverage = must.Must(pgpublication.CompleteCoverage(schemaext.Desired))
	return db
}
