package builtindialects_test

import (
	"cmp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// The tables below write out the maps and switches of internal/atlasurl,
// internal/atlashcl, internal/atlashclrender, internal/atlasreport and
// internal/atlassource that named engines, as they stood when the descriptor
// replaced them. A descriptor that stops agreeing with them fails here, by
// name, rather than as a dev URL, an HCL document or a report that changed.

// switchDockerImages is dockerEngineDialects and dockerExtensionEngines of
// internal/atlasurl: the engine a docker:// URL names, and the dialect of the
// server it starts.
var switchDockerImages = map[string]string{
	"postgres": "postgres", "postgis": "postgres", "pgvector": "postgres",
	"mysql": "mysql", "maria": "mariadb", "mariadb": "mariadb",
	"ydb": "ydb",
}

// switchExtensionImages is dockerExtensionEngines alone, the images strict
// compatibility refused. The dialect behind them is the one dialect
// attachYDBJSONLoss of internal/atlasreport warned for.
var switchExtensionImages = []string{"ydb"}

// switchDockerDrivers is dockerImageDrivers of internal/atlasurl, whose
// dialect DialectFromURL read through platform.NormalizeDialect.
var switchDockerDrivers = []string{"postgres", "mysql", "maria", "mariadb", "clickhouse", "sqlserver"}

// switchDefaultPorts is defaultPorts of internal/atlasurl.
var switchDefaultPorts = map[string]uint16{
	"postgres": 5432, "cockroachdb": 26257, "yugabytedb": 5433,
	"mysql": 3306, "mariadb": 3306, "sqlserver": 1433, "clickhouse": 9000,
}

// switchFileSchemes is fileSchemes of internal/atlasurl.
var switchFileSchemes = []string{"sqlite", "sqlite3", "file"}

// switchHCLProperties is what mysqlColumnSettings, mysqlProperties and
// sqliteProperties of internal/atlashcl stated, by block, attribute and the
// targets that received it.
var switchHCLProperties = map[string]map[string][]string{
	"table": {
		"auto_increment": {"mysql", "mariadb"}, "charset": {"mysql", "mariadb"},
		"strict": {"sqlite"}, "without_rowid": {"sqlite"},
	},
	"column": {"charset": {"mysql", "mariadb"}, "on_update": {"mysql", "mariadb"}},
	"index":  {"parser": {"mysql", "mariadb"}},
}

// switchRefusedBlocks is atlasRefusedBlockTypes of internal/atlashclrender.
var switchRefusedBlocks = map[string][]string{"postgres": {"extension", "policy", "sequence"}}

// switchColumnTypes is modeledColumnTypes of internal/atlashclrender.
var switchColumnTypes = map[string][]string{
	"sqlite": {
		"bigint", "blob", "bool", "boolean", "character", "date", "datetime", "decimal", "double", "float",
		"int", "integer", "json", "jsonb", "numeric", "real", "smallint", "text", "uuid", "varchar",
	},
	"postgres": {
		"bigint", "bigserial", "bit", "bool", "boolean", "box", "bytea", "char", "character", "cidr",
		"circle", "date", "daterange", "decimal", "float", "float4", "float8", "inet", "int", "int2",
		"int4", "int4range", "int8", "int8range", "integer", "interval", "json", "jsonb", "line", "lseg",
		"macaddr", "macaddr8", "money", "name", "numeric", "numrange", "oid", "path", "point", "polygon",
		"real", "regclass", "serial", "smallint", "smallserial", "text", "time", "timestamp", "timestamptz", "timetz",
		"tsquery", "tsrange", "tstzrange", "tsvector", "uuid", "varchar", "xml",
	},
}

// switchComparedAs is identityDialect of internal/atlasurl for a dialect it
// knows: the MySQL family is one, and every other dialect is itself.
func switchComparedAs(dialect string) string {
	switch dialect {
	case "mysql", "mariadb":
		return "mysql"
	default:
		return dialect
	}
}

// switchGoSQLDriver is cutMySQLScheme of internal/atlasurl: the dialects whose
// schemes it read as MySQL-family URLs.
var switchGoSQLDriver = []string{"mysql", "mariadb"}

// switchSQLite is the dialect internal/atlasurl compared by file,
// internal/atlassource took an opaque URL of, internal/atlasreport named the
// schema "main" for, and internal/atlashclrender read key nullability from the
// table's shape on.
const switchSQLite = "sqlite"

func TestDescriptors_URLsAgreeWithTheSwitchesTheyReplace(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Dialects().Dialects(), qt.HasLen, len(builtindialects.Descriptors()))
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(spelling)
			c.Assert(found, qt.IsTrue)

			c.Assert(described.URLs.DefaultPort, qt.Equals, switchDefaultPorts[described.Name])
			c.Assert(cmp.Or(described.URLs.ComparedAs, described.Name), qt.Equals, switchComparedAs(described.Name))
			c.Assert(described.URLs.Grammar == descriptor.URLGrammarGoSQLDriver, qt.Equals,
				slices.Contains(switchGoSQLDriver, described.Name))
			c.Assert(len(described.URLs.FileSchemes) > 0, qt.Equals, described.Name == switchSQLite)
			c.Assert(described.Comparison.RowidTables, qt.Equals, described.Name == switchSQLite)
			c.Assert(described.Comparison.PrimaryKeyNullable, qt.Equals, described.Name == switchSQLite)
		})
	}
}

func TestDescriptors_FileSchemesAgreeWithTheListTheyReplace(t *testing.T) {
	c := qt.New(t)
	var stated []string
	for _, described := range platform.Dialects().Dialects() {
		stated = append(stated, described.URLs.FileSchemes...)
	}
	c.Assert(stated, qt.DeepEquals, switchFileSchemes)
	described, found := platform.Lookup(switchSQLite)
	c.Assert(found, qt.IsTrue)
	c.Assert(described.URLs.FileSchemes, qt.DeepEquals, switchFileSchemes)
}

func TestDescriptors_DockerNamesAgreeWithTheMapsTheyReplace(t *testing.T) {
	c := qt.New(t)
	images, extension, drivers := statedDockerNames()
	c.Assert(images, qt.DeepEquals, switchDockerImages)
	c.Assert(extension, qt.DeepEquals, switchExtensionImages)
	c.Assert(drivers, qt.DeepEquals, slices.Sorted(slices.Values(switchDockerDrivers)))
	for _, driver := range drivers {
		described, found := platform.Lookup(platform.NormalizeDialect(driver))
		c.Assert(found, qt.IsTrue, qt.Commentf("docker driver %q", driver))
		c.Assert(described.Atlas.DockerDrivers, qt.Contains, driver)
	}
}

// statedDockerNames collects the docker names every built-in descriptor
// states: each image with the dialect that claims it, the images of an
// extension engine, and every driver, sorted.
func statedDockerNames() (images map[string]string, extension, drivers []string) {
	images = make(map[string]string)
	for _, described := range platform.Dialects().Dialects() {
		for _, image := range described.Atlas.DockerImages {
			images[image] = described.Name
			if described.Atlas.Extension {
				extension = append(extension, image)
			}
		}
		drivers = append(drivers, described.Atlas.DockerDrivers...)
	}
	slices.Sort(drivers)
	return images, extension, drivers
}

func TestDescriptors_AtlasSchemaAgreesWithTheSwitchesTheyReplace(t *testing.T) {
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(spelling)
			c.Assert(found, qt.IsTrue)

			// schema_inspect.go named "main" for the dialect, and common.go
			// for a URL whose scheme was "sqlite" exactly.
			c.Assert(described.Atlas.Schema != "", qt.Equals, described.Name == switchSQLite)
			c.Assert(described.Atlas.Schema != "" && described.Name == spelling, qt.Equals, spelling == "sqlite")
			c.Assert(described.Atlas.Extension, qt.Equals, slices.Contains(switchExtensionImages, described.Name))
		})
	}
}

func TestDescriptors_AtlasHCLAgreesWithTheMapsTheyReplace(t *testing.T) {
	c := qt.New(t)
	properties, refused, columnTypes := statedAtlasHCL()
	c.Assert(properties, qt.DeepEquals, switchHCLProperties)
	c.Assert(refused, qt.DeepEquals, switchRefusedBlocks)
	c.Assert(columnTypes, qt.DeepEquals, switchColumnTypes)
}

// statedAtlasHCL collects what every built-in descriptor states of an Atlas
// HCL schema: the targets of each attribute by block, and the refused blocks
// and modeled column types of each dialect that states any.
func statedAtlasHCL() (properties map[string]map[string][]string, refused, columnTypes map[string][]string) {
	properties = map[string]map[string][]string{"table": {}, "column": {}, "index": {}}
	refused = make(map[string][]string)
	columnTypes = make(map[string][]string)
	for _, described := range platform.Dialects().Dialects() {
		hcl := described.Atlas.HCL
		for kind, names := range map[string][]string{
			"table": hcl.TableProperties, "column": hcl.ColumnProperties, "index": hcl.IndexProperties,
		} {
			for _, name := range names {
				properties[kind][name] = append(properties[kind][name], described.Name)
			}
		}
		if len(hcl.RefusedBlocks) > 0 {
			refused[described.Name] = hcl.RefusedBlocks
		}
		if len(hcl.ColumnTypes) > 0 {
			columnTypes[described.Name] = hcl.ColumnTypes
		}
	}
	return properties, refused, columnTypes
}
