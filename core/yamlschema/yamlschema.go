// Package yamlschema reads a desired schema written in Ptah's YAML format.
//
// # One of the authoring formats
//
// A desired schema reaches Ptah as a *schemamodel.Database, and YAML is one of the
// formats that produce it. Go annotations, YAML, HCL, SQL, and DBML each have a
// reader of their own, each returns the same model, and everything downstream —
// rendering, diffing, planning, linting, migration generation — sees only that
// model and cannot tell which reader filled it. A schema authored in YAML is
// therefore as complete an input as one authored in Go: it is a peer of the
// other four, not a convenience wrapper over one of them.
//
// This package is the YAML reader. Parse takes the document as bytes, ParseFile
// reads it from a path; both return the model:
//
//	db, err := yamlschema.ParseFile(context.Background(), runtime.YAML(), "schema.yaml")
//	if err != nil {
//		return err
//	}
//	statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
//
// ptah/core/schemasource covers the other direction, running an external
// program that writes YAML, HCL, or SQL to its standard output. It parses that
// output through this package. Reach for it when the schema is produced by
// another tool; reach for this package when the YAML is already in hand.
//
// # What the document holds
//
// The top level is a set of object collections, each keyed by name: tables,
// indexes, constraints, enums, extensions, functions, rls_policies,
// rls_enabled_tables (also accepted as rls_enabled), roles, grants, revokes,
// default_privileges, views, matviews and triggers. A selected feature owner
// adds keys of its own, such as YDB's topics and secrets and ClickHouse's
// row_policies, and keys to a table or an index entry, such as a YDB table's
// changefeeds; a key no selected owner reads is unknown. A table carries
// its columns in declaration order, along with its primary key, checks, engine,
// comment, and per-platform overrides. A column carries the type, its
// nullability, key and uniqueness flags, defaults, generated and identity
// expressions, a foreign key with its referential actions, character set and
// collation, and its own per-platform overrides. Tables and columns can also
// declare shared and per-target API names; columns additionally carry API-only
// type and exposure metadata.
//
// A grant or revoke names its target with on_table, on_schema, on_sequence,
// on_database, on_function and on_procedure, whose value carries the
// argument types: purge(uuid), or, on YDB, on_topic, on_coordination_node,
// on_secret, on_external_data_source, on_external_table,
// on_async_replication, on_transfer or on_streaming_query, each spelled as
// on_table is, or on_resource_pool, which takes the pool's name alone. A revokes entry says the role must not hold the privileges,
// and a privilege both granted and revoked to one role on one object is
// refused.
//
// A default_privileges key is a label rather than a default name, because a
// default privilege has no name. The role whose new objects it covers, the
// schema, the object type and the grantee identify it. An entry without a
// schema is the global default, ALTER DEFAULT PRIVILEGES without IN SCHEMA.
//
// # Strictness
//
// Parsing is strict in two ways that a permissive YAML reader is not. An
// unknown key is an error rather than a silent drop, so a misspelled attribute
// cannot pass as an intentional setting. A second YAML document in the same
// stream is refused rather than ignored, so a schema split across a `---`
// separator cannot half-apply.
//
// Errors are returned, never printed, and a decoding failure carries the parse
// position the YAML decoder reported.
package yamlschema

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/matviewrefresh"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/routinesetting"
	"ptah.run/internal/yamlvalue"
)

// ParseFile reads a YAML schema file and parses it with Parse, returning the
// same *schemamodel.Database that Go annotations, HCL, SQL, and DBML produce.
// A read failure is wrapped but keeps the underlying filesystem error, so
// errors.Is(err, fs.ErrNotExist) still answers for a missing file and
// errors.As reaches the *fs.PathError naming the path.
//
// owners selects the feature owners whose models the document can declare, as
// it does for Parse.
func ParseFile(ctx context.Context, owners yamlext.Set, path string) (*schemamodel.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !owners.Selected() {
		return nil, fmt.Errorf("parse YAML schema: %w", yamlext.ErrUnselected)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema file: %w", err)
	}

	return parseDocument(ctx, owners, data, path)
}

// Parse parses a YAML schema document into the same *schemamodel.Database
// that Go annotations, HCL, SQL, and DBML produce.
//
// The input is exactly one YAML document. An unknown key is an error rather
// than a silent drop, and a second document after a `---` separator is
// refused. A document that decodes but declares an incomplete object — an
// index without fields, a top-level constraint without a table, a duplicate
// column — is refused as well. Every refusal is returned as an error naming
// the document position or the object key it concerns, so the author can find
// it; none of the message text is contract.
//
// The model does not depend on YAML map order: the same bytes always produce
// the same model, whatever order the decoder walked the maps in. Within a
// table, columns keep the order they were declared in, because column order
// is part of the schema the author wrote. Tables and functions come back
// ordered by their dependencies.
//
// owners selects the feature owners whose models the document can declare,
// usually the YAML set of the runtime the caller renders and compares with.
// The document claims knowledge of an owner's models only when the parse
// selects the owner; otherwise they stay unknown rather than absent. The zero
// set is refused with [yamlext.ErrUnselected]; [yamlext.None] selects no
// owner on purpose.
func Parse(ctx context.Context, owners yamlext.Set, data []byte) (*schemamodel.Database, error) {
	return parseDocument(ctx, owners, data, "")
}

func parseDocument(ctx context.Context, owners yamlext.Set, data []byte, filename string) (*schemamodel.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !owners.Selected() {
		return nil, fmt.Errorf("parse YAML schema: %w", yamlext.ErrUnselected)
	}
	var doc document
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}
	var extraDoc document
	if err := decoder.Decode(&extraDoc); err == nil {
		return nil, fmt.Errorf("parse YAML schema: multiple YAML documents are not supported")
	} else if err != io.EOF {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}
	if err := checkOwnedKeys(owners, data, doc.Owned); err != nil {
		return nil, err
	}

	db, err := doc.toDatabase(ctx, owners, data, filename)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return db, nil
}

type document struct {
	Tables            map[string]tableSpec      `yaml:"tables"`
	Indexes           map[string]indexSpec      `yaml:"indexes"`
	Constraints       map[string]constraintSpec `yaml:"constraints"`
	Enums             map[string]enumSpec       `yaml:"enums"`
	Functions         map[string]functionSpec   `yaml:"functions"`
	RLSPolicies       map[string]rlsPolicySpec  `yaml:"rls_policies"`
	RLSEnabledTables  map[string]rlsEnableSpec  `yaml:"rls_enabled_tables"`
	RLSEnabled        map[string]rlsEnableSpec  `yaml:"rls_enabled"`
	Roles             map[string]roleSpec       `yaml:"roles"`
	Grants            map[string]grantSpec      `yaml:"grants"`
	Revokes           map[string]revokeSpec     `yaml:"revokes"`
	Views             map[string]viewSpec       `yaml:"views"`
	MaterializedViews map[string]matViewSpec    `yaml:"matviews"`
	Triggers          map[string]triggerSpec    `yaml:"triggers"`

	// Owned holds the top-level keys the frontend does not read itself. A
	// selected owner's section is handed to the owner; any other key is
	// refused as unknown.
	Owned map[string]yaml.Node `yaml:",inline"`
}

type tableSpec struct {
	Schema      stringScalar               `yaml:"schema"`
	StructName  stringScalar               `yaml:"struct_name"`
	Name        stringScalar               `yaml:"name"`
	APIName     stringScalar               `yaml:"api_name"`
	OpenAPIName stringScalar               `yaml:"openapi_name"`
	GraphQLName stringScalar               `yaml:"graphql_name"`
	ProtoName   stringScalar               `yaml:"proto_name"`
	Engine      stringScalar               `yaml:"engine"`
	Comment     stringScalar               `yaml:"comment"`
	PrimaryKey  stringList                 `yaml:"primary_key"`
	Checks      stringList                 `yaml:"checks"`
	CustomSQL   stringScalar               `yaml:"custom_sql"`
	Columns     orderedMap[fieldSpec]      `yaml:"columns"`
	Fields      orderedMap[fieldSpec]      `yaml:"fields"`
	Indexes     orderedMap[indexSpec]      `yaml:"indexes"`
	Constraints orderedMap[constraintSpec] `yaml:"constraints"`
	RLSEnabled  bool                       `yaml:"rls_enabled"`

	Platform  platformSpec `yaml:"platform"`
	Overrides platformSpec `yaml:"overrides"`

	// Owned holds the keys the frontend does not read itself: the ones a
	// selected owner adds to a table, such as a YDB table's changefeeds, and
	// anything else, which is refused as unknown.
	Owned map[string]yaml.Node `yaml:",inline"`
}

type fieldSpec struct {
	FieldName          stringScalar  `yaml:"field_name"`
	Name               stringScalar  `yaml:"name"`
	APIName            stringScalar  `yaml:"api_name"`
	OpenAPIName        stringScalar  `yaml:"openapi_name"`
	GraphQLName        stringScalar  `yaml:"graphql_name"`
	ProtoName          stringScalar  `yaml:"proto_name"`
	APIType            stringScalar  `yaml:"api_type"`
	APIExpose          stringScalar  `yaml:"api_expose"`
	Type               stringScalar  `yaml:"type"`
	Nullable           *bool         `yaml:"nullable"`
	NotNull            bool          `yaml:"not_null"`
	Primary            bool          `yaml:"primary"`
	AutoIncrement      bool          `yaml:"auto_increment"`
	AutoInc            bool          `yaml:"auto_inc"`
	IdentityGeneration stringScalar  `yaml:"identity_generation"`
	IdentityStart      stringScalar  `yaml:"identity_start"`
	IdentityIncrement  stringScalar  `yaml:"identity_increment"`
	IdentityOptions    stringScalar  `yaml:"identity_options"`
	Unique             bool          `yaml:"unique"`
	UniqueExpr         stringScalar  `yaml:"unique_expr"`
	Index              bool          `yaml:"index"`
	Generated          stringScalar  `yaml:"generated"`
	GeneratedKind      stringScalar  `yaml:"generated_kind"`
	Stored             bool          `yaml:"stored"`
	Default            *stringScalar `yaml:"default"`
	DefaultExpr        stringScalar  `yaml:"default_expr"`
	Foreign            stringScalar  `yaml:"foreign"`
	ForeignKeyName     stringScalar  `yaml:"foreign_key_name"`
	OnDelete           stringScalar  `yaml:"on_delete"`
	OnUpdate           stringScalar  `yaml:"on_update"`
	Enum               stringList    `yaml:"enum"`
	Check              stringScalar  `yaml:"check"`
	CheckName          stringScalar  `yaml:"check_name"`
	Collate            stringScalar  `yaml:"collate"`
	Comment            stringScalar  `yaml:"comment"`
	Platform           platformSpec  `yaml:"platform"`
	Overrides          platformSpec  `yaml:"overrides"`

	// Owned holds the keys the frontend does not read itself: the ones a
	// selected document owner adds to a column, and anything else, which is
	// refused as unknown.
	Owned map[string]yaml.Node `yaml:",inline"`
}

type indexSpec struct {
	Name      stringScalar `yaml:"name"`
	Fields    stringList   `yaml:"fields"`
	Columns   stringList   `yaml:"columns"`
	Include   stringList   `yaml:"include"`
	Unique    bool         `yaml:"unique"`
	Comment   stringScalar `yaml:"comment"`
	Type      stringScalar `yaml:"type"`
	Condition stringScalar `yaml:"condition"`
	Where     stringScalar `yaml:"where"`
	Operator  stringScalar `yaml:"ops"`
	TableName stringScalar `yaml:"table"`
	Platform  platformSpec `yaml:"platform"`

	// Owned holds the keys the frontend does not read itself: the ones a
	// selected owner adds to an index, such as a YDB index's partitioning,
	// and anything else, which is refused as unknown.
	Owned map[string]yaml.Node `yaml:",inline"`
}

type constraintSpec struct {
	Name            stringScalar `yaml:"name"`
	Type            stringScalar `yaml:"type"`
	Table           stringScalar `yaml:"table"`
	UsingMethod     stringScalar `yaml:"using"`
	CheckExpression stringScalar `yaml:"check"`
	Columns         stringList   `yaml:"columns"`
	ForeignTable    stringScalar `yaml:"foreign_table"`
	ForeignColumn   stringScalar `yaml:"foreign_column"`
	OnDelete        stringScalar `yaml:"on_delete"`
	OnUpdate        stringScalar `yaml:"on_update"`
	Comment         stringScalar `yaml:"comment"`
}

type enumSpec struct {
	Values  stringList   `yaml:"values"`
	Comment stringScalar `yaml:"comment"`
}

func (s *enumSpec) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode || value.Kind == yaml.ScalarNode {
		var values stringList
		if err := value.Decode(&values); err != nil {
			return err
		}
		s.Values = values
		return nil
	}

	type rawEnumSpec enumSpec
	var raw rawEnumSpec
	if err := decodeKnownFields(value, &raw); err != nil {
		return err
	}
	*s = enumSpec(raw)
	return nil
}

type functionSpec struct {
	StructName stringScalar         `yaml:"struct_name"`
	Name       stringScalar         `yaml:"name"`
	Parameters stringScalar         `yaml:"parameters"`
	Params     stringScalar         `yaml:"params"`
	Returns    stringScalar         `yaml:"returns"`
	Language   stringScalar         `yaml:"language"`
	Security   stringScalar         `yaml:"security"`
	Volatility stringScalar         `yaml:"volatility"`
	Owned      map[string]yaml.Node `yaml:",inline"`
	// Settings are the routine's own configuration settings, each `name=value`.
	Settings []stringScalar `yaml:"settings"`
	Body     stringScalar   `yaml:"body"`
	Comment  stringScalar   `yaml:"comment"`
}

type viewSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Body       stringScalar `yaml:"body"`
	WithCheck  bool         `yaml:"with_check"`
	Comment    stringScalar `yaml:"comment"`
}

type matViewSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Body       stringScalar `yaml:"body"`
	// RefreshStrategy is retired and kept only so a document that declares it
	// is refused by name. Deleting the field would make the strict decoder
	// answer `field refresh_strategy not found`, which reads as a typo and says
	// nothing about why a correctly spelled key stopped working
	// (stokaro/ptah#1625). It is a yaml.Node so that `refresh_strategy:` with
	// no value, and an empty string, are refused as well as a real value.
	RefreshStrategy yaml.Node            `yaml:"refresh_strategy"`
	Comment         stringScalar         `yaml:"comment"`
	Owned           map[string]yaml.Node `yaml:",inline"`
}

type triggerSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Table      stringScalar `yaml:"table"`
	Timing     stringScalar `yaml:"timing"`
	Event      stringScalar `yaml:"event"`
	ForEach    stringScalar `yaml:"for"`
	When       stringScalar `yaml:"when"`
	OldTable   stringScalar `yaml:"old_table"`
	NewTable   stringScalar `yaml:"new_table"`
	Body       stringScalar `yaml:"body"`
	Comment    stringScalar `yaml:"comment"`
}

type rlsPolicySpec struct {
	StructName          stringScalar `yaml:"struct_name"`
	Name                stringScalar `yaml:"name"`
	Table               stringScalar `yaml:"table"`
	PolicyFor           stringScalar `yaml:"for"`
	ToRoles             stringScalar `yaml:"to"`
	UsingExpression     stringScalar `yaml:"using"`
	WithCheckExpression stringScalar `yaml:"with_check"`
	// As is PERMISSIVE or RESTRICTIVE, as the annotation's `as` is.
	As      stringScalar `yaml:"as"`
	Comment stringScalar `yaml:"comment"`
	// Dialects scopes the policy, as it scopes a default privilege. A scope
	// naming ClickHouse is refused, and one naming only targets no selected
	// owner reads keeps it a shared declaration.
	Dialects yaml.Node `yaml:"dialects"`
}

type rlsEnableSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Table      stringScalar `yaml:"table"`
	Comment    stringScalar `yaml:"comment"`
	Dialects   yaml.Node    `yaml:"dialects"`
}

type roleSpec struct {
	StructName  stringScalar `yaml:"struct_name"`
	Name        stringScalar `yaml:"name"`
	Login       bool         `yaml:"login"`
	Password    stringScalar `yaml:"password"`
	Superuser   bool         `yaml:"superuser"`
	CreateDB    bool         `yaml:"create_db"`
	CreateRole  bool         `yaml:"create_role"`
	Inherit     *bool        `yaml:"inherit"`
	Replication bool         `yaml:"replication"`
	Comment     stringScalar `yaml:"comment"`
	// Group declares a YDB group, and MemberOf the groups the role is a
	// member of; see [schemamodel.Role].
	Group    bool       `yaml:"group"`
	MemberOf stringList `yaml:"member_of"`
}

type grantSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Role       stringScalar `yaml:"role"`
	Privilege  stringList   `yaml:"privilege"`
	Privileges stringList   `yaml:"privileges"`
	OnTable    stringScalar `yaml:"on_table"`
	OnSchema   stringScalar `yaml:"on_schema"`
	OnSequence stringScalar `yaml:"on_sequence"`
	// OnFunction and OnProcedure name a routine with its argument types,
	// `purge(uuid)`, as the Go annotations do: PostgreSQL overloads a name by
	// them.
	OnFunction    stringScalar `yaml:"on_function"`
	OnProcedure   stringScalar `yaml:"on_procedure"`
	OnDatabase    bool         `yaml:"on_database"`
	objectTargets `yaml:",inline"`
	WithOption    bool         `yaml:"with_option"`
	Comment       stringScalar `yaml:"comment"`
}

// objectTargets are the keys a grant and a revoke name a YDB scheme object
// by, one per kind [schemamodel.GrantObjectKinds] lists.
type objectTargets struct {
	OnTopic              stringScalar `yaml:"on_topic"`
	OnCoordinationNode   stringScalar `yaml:"on_coordination_node"`
	OnSecret             stringScalar `yaml:"on_secret"`
	OnExternalDataSource stringScalar `yaml:"on_external_data_source"`
	OnExternalTable      stringScalar `yaml:"on_external_table"`
	OnAsyncReplication   stringScalar `yaml:"on_async_replication"`
	OnTransfer           stringScalar `yaml:"on_transfer"`
	OnStreamingQuery     stringScalar `yaml:"on_streaming_query"`
	OnResourcePool       stringScalar `yaml:"on_resource_pool"`
}

// byAttribute is each target named, by the attribute that names it.
func (t objectTargets) byAttribute() map[string]string {
	return map[string]string{
		"on_topic":                string(t.OnTopic),
		"on_coordination_node":    string(t.OnCoordinationNode),
		"on_secret":               string(t.OnSecret),
		"on_external_data_source": string(t.OnExternalDataSource),
		"on_external_table":       string(t.OnExternalTable),
		"on_async_replication":    string(t.OnAsyncReplication),
		"on_transfer":             string(t.OnTransfer),
		"on_streaming_query":      string(t.OnStreamingQuery),
		"on_resource_pool":        string(t.OnResourcePool),
	}
}

// revokeSpec is one entry under `revokes`: privileges a role is declared not
// to hold on an object, the YAML spelling of a schema file's REVOKE. It names
// its target as a grant does and has no grant option.
type revokeSpec struct {
	StructName    stringScalar `yaml:"struct_name"`
	Role          stringScalar `yaml:"role"`
	Privilege     stringList   `yaml:"privilege"`
	Privileges    stringList   `yaml:"privileges"`
	OnTable       stringScalar `yaml:"on_table"`
	OnSchema      stringScalar `yaml:"on_schema"`
	OnSequence    stringScalar `yaml:"on_sequence"`
	OnFunction    stringScalar `yaml:"on_function"`
	OnProcedure   stringScalar `yaml:"on_procedure"`
	OnDatabase    bool         `yaml:"on_database"`
	objectTargets `yaml:",inline"`
	Comment       stringScalar `yaml:"comment"`
}

type platformSpec map[string]map[string]stringScalar

func (d document) toDatabase(ctx context.Context, owners yamlext.Set, data []byte, filename string) (*schemamodel.Database, error) {
	db := &schemamodel.Database{
		Dependencies:               make(map[string][]string),
		FunctionDependencies:       make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
	}

	d.addEnums(db)
	if err := d.addTables(db, owners, data); err != nil {
		return nil, err
	}
	if err := d.addIndexes(db, owners); err != nil {
		return nil, err
	}
	if err := d.addConstraints(db); err != nil {
		return nil, err
	}
	if err := d.addFunctions(db, owners); err != nil {
		return nil, err
	}
	if err := d.addViews(db); err != nil {
		return nil, err
	}
	if err := d.addMaterializedViews(db, owners); err != nil {
		return nil, err
	}
	if err := d.addTriggers(db); err != nil {
		return nil, err
	}
	if err := d.addRLS(db, owners); err != nil {
		return nil, err
	}
	if err := d.addOwnerSections(db, owners, data); err != nil {
		return nil, err
	}
	if err := d.addOwnerDocuments(ctx, db, owners, data, filename); err != nil {
		return nil, err
	}
	// Once every owner has declared what it reads, each narrows the claim
	// by what the document declares of its models.
	coverage, err := owners.Cover(db.FeatureCoverage, db.FeatureObjects)
	if err != nil {
		return nil, err
	}
	db.FeatureCoverage = coverage
	d.addRoles(db)
	if err := d.addGrants(db); err != nil {
		return nil, err
	}
	if err := d.addRevokes(db); err != nil {
		return nil, err
	}

	schemamodel.Finalize(db)
	// After Finalize, which merges the entries of one identity and resolves the
	// table a grant or revoke names: a document has no statement order, so a
	// privilege one entry grants and another revokes cannot be resolved.
	if err := schemamodel.ValidateRevokedGrants(db); err != nil {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}
	return db, nil
}

func (d document) addEnums(db *schemamodel.Database) {
	for _, name := range sortedKeys(d.Enums) {
		db.Enums = append(db.Enums, schemamodel.Enum{
			Name:    name,
			Values:  cleanStrings(d.Enums[name].Values),
			Comment: strings.TrimSpace(string(d.Enums[name].Comment)),
		})
	}
}

func (d document) addTables(db *schemamodel.Database, owners yamlext.Set, data []byte) error {
	// The selected owners claim what a YAML document can declare of their
	// models, such as CockroachDB row-level TTL and the Spanner row deletion
	// policy in a table's cockroachdb and spanner platform groups, so a table
	// without one requests none.
	claims, err := owners.Coverage()
	if err != nil {
		return err
	}
	db.FeatureCoverage = claims

	for _, tableKey := range sortedKeys(d.Tables) {
		table := d.Tables[tableKey]
		structName := valueOrDefault(table.StructName, tableKey)
		tableName := valueOrDefault(table.Name, tableKey)

		scalars, sections, err := ownedKeys(owners, yamlext.EntryTable, fmt.Sprintf("table %q", tableKey), table.Owned)
		if err != nil {
			return err
		}
		facets, err := owners.DecodeEntryAttributes(yamlext.EntryTable, scalars)
		if err != nil {
			return fmt.Errorf("table %q: %w", tableKey, err)
		}
		db.Tables = append(db.Tables, schemamodel.Table{
			StructName: structName,
			Name:       tableName,
			Schema:     string(table.Schema),
			APIName:    string(table.APIName),
			APINames: schemamodel.TargetNames{
				OpenAPI:  string(table.OpenAPIName),
				GraphQL:  string(table.GraphQLName),
				Protobuf: string(table.ProtoName),
			},
			Engine:     string(table.Engine),
			Comment:    string(table.Comment),
			PrimaryKey: cleanStrings(table.PrimaryKey),
			Checks:     cleanStrings(table.Checks),
			CustomSQL:  string(table.CustomSQL),
			Overrides:  mergePlatform(withoutGroups(table.Platform, owners.PlatformGroups()), table.Overrides),

			Facets: facets,
		})
		owned := yamlext.Table{Key: tableKey, Schema: string(table.Schema), Name: tableName, Struct: structName}
		if err := addTableSections(db, owners, data, owned, sections); err != nil {
			return err
		}

		if err := addFields(db, owners, data, structName, tableKey, table.Columns, table.Fields); err != nil {
			return err
		}
		if err := addTableIndexes(db, owners, structName, table.Indexes); err != nil {
			return err
		}
		if err := addTableConstraints(db, structName, tableName, table.Constraints); err != nil {
			return err
		}
	}

	return nil
}

// addFields adds the columns of a table entry. A column entry holds no key
// the frontend does not read but the ones a document owner reads, which
// addOwnerDocuments hands to it.
func addFields(db *schemamodel.Database, owners yamlext.Set, data []byte, structName, tableKey string, columns, fields orderedMap[fieldSpec]) error {
	seen := make(map[string]bool)
	for _, column := range columns {
		seen[column.Name] = true
		if err := checkColumnKeys(owners, data, tableKey, yamlext.EntryColumn, column); err != nil {
			return err
		}
		field, err := buildField(structName, column.Name, column.Value, db)
		if err != nil {
			return err
		}
		db.Fields = append(db.Fields, field)
	}

	for _, field := range fields {
		if seen[field.Name] {
			return fmt.Errorf("duplicate column %q in columns and fields", field.Name)
		}
		if err := checkColumnKeys(owners, data, tableKey, "fields", field); err != nil {
			return err
		}
		parsedField, err := buildField(structName, field.Name, field.Value, db)
		if err != nil {
			return err
		}
		db.Fields = append(db.Fields, parsedField)
	}

	return nil
}

// checkColumnKeys refuses a key of a column entry that neither the frontend
// nor a selected owner reads, naming the line the author wrote it on. The
// entry's Owned nodes come from a re-encoded copy, whose lines are not the
// document's.
func checkColumnKeys(owners yamlext.Set, data []byte, tableKey, group string, column yamlvalue.Entry[fieldSpec]) error {
	for _, key := range sortedKeys(column.Value.Owned) {
		if found, _ := owners.EntryKey(yamlext.EntryColumn, key); found {
			continue
		}
		line := 0
		var root yaml.Node
		if yaml.Unmarshal(data, &root) == nil {
			if node := entryNode(&root, yamlext.EntryTable, tableKey, group, column.Name); node != nil {
				line = keyNodeLine(node, key)
			}
		}
		return fmt.Errorf("parse YAML schema: line %d: unknown key %q of column %q of table %q", line, key, column.Name, tableKey)
	}
	return nil
}

func buildField(structName, key string, spec fieldSpec, db *schemamodel.Database) (schemamodel.Field, error) {
	fieldName := valueOrDefault(spec.FieldName, key)
	columnName := valueOrDefault(spec.Name, key)
	fieldType := string(spec.Type)
	enumValues := cleanStrings(spec.Enum)
	if len(enumValues) > 0 && (fieldType == "" || fieldType == "ENUM") {
		enumName := "enum_" + strings.ToLower(structName) + "_" + strings.ToLower(fieldName)
		db.Enums = append(db.Enums, schemamodel.Enum{Name: enumName, Values: enumValues})
		fieldType = enumName
	}

	nullable := true
	if spec.Nullable != nil {
		nullable = *spec.Nullable
	}
	if spec.NotNull {
		nullable = false
	}
	identityGeneration := normalizeIdentityGeneration(string(spec.IdentityGeneration))
	if spec.IdentityGeneration != "" && identityGeneration == "" {
		return schemamodel.Field{}, fmt.Errorf("column %q has unsupported identity_generation %q", key, spec.IdentityGeneration)
	}
	if identityGeneration == "" && hasIdentitySettings(spec) {
		identityGeneration = "BY_DEFAULT"
	}

	var defaultValue string
	if spec.Default != nil {
		defaultValue = string(*spec.Default)
	}

	return schemamodel.Field{
		StructName: structName,
		FieldName:  fieldName,
		Name:       columnName,
		APIName:    string(spec.APIName),
		APINames: schemamodel.TargetNames{
			OpenAPI:  string(spec.OpenAPIName),
			GraphQL:  string(spec.GraphQLName),
			Protobuf: string(spec.ProtoName),
		},
		APIType:             string(spec.APIType),
		APIExpose:           string(spec.APIExpose),
		Type:                fieldType,
		Nullable:            nullable,
		Primary:             spec.Primary,
		AutoInc:             spec.AutoIncrement || spec.AutoInc || identityGeneration != "",
		IdentityGeneration:  identityGeneration,
		IdentityStart:       string(spec.IdentityStart),
		IdentityIncrement:   string(spec.IdentityIncrement),
		IdentityOptions:     string(spec.IdentityOptions),
		Unique:              spec.Unique,
		UniqueExpr:          string(spec.UniqueExpr),
		Default:             defaultValue,
		DefaultSet:          spec.Default != nil,
		DefaultExpr:         string(spec.DefaultExpr),
		Foreign:             string(spec.Foreign),
		ForeignKeyName:      string(spec.ForeignKeyName),
		OnDelete:            string(spec.OnDelete),
		OnUpdate:            string(spec.OnUpdate),
		Enum:                enumValues,
		Check:               string(spec.Check),
		CheckName:           string(spec.CheckName),
		GeneratedExpression: string(spec.Generated),
		GeneratedKind:       yamlGeneratedColumnKind(spec),
		Collate:             string(spec.Collate),
		Comment:             string(spec.Comment),
		Overrides:           mergePlatform(spec.Platform, spec.Overrides),
	}, nil
}

func hasIdentitySettings(spec fieldSpec) bool {
	return spec.IdentityStart != "" || spec.IdentityIncrement != "" || spec.IdentityOptions != ""
}

func yamlGeneratedColumnKind(spec fieldSpec) string {
	if strings.TrimSpace(string(spec.Generated)) == "" {
		return ""
	}
	if kind := strings.TrimSpace(string(spec.GeneratedKind)); kind != "" {
		return strings.ToUpper(kind)
	}
	if spec.Stored {
		return "STORED"
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func normalizeIdentityGeneration(value string) string {
	switch strings.ToUpper(strings.ReplaceAll(value, " ", "_")) {
	case "ALWAYS":
		return "ALWAYS"
	case "BY_DEFAULT":
		return "BY_DEFAULT"
	default:
		return ""
	}
}

func addTableIndexes(db *schemamodel.Database, owners yamlext.Set, structName string, indexes orderedMap[indexSpec]) error {
	for _, index := range indexes {
		value, err := buildIndex(owners, index.Name, structName, index.Value)
		if err != nil {
			return err
		}
		db.Indexes = append(db.Indexes, value)
	}
	return nil
}

func (d document) addIndexes(db *schemamodel.Database, owners yamlext.Set) error {
	for _, key := range sortedKeys(d.Indexes) {
		index, err := buildIndex(owners, key, "", d.Indexes[key])
		if err != nil {
			return err
		}
		db.Indexes = append(db.Indexes, index)
	}
	return nil
}

func buildIndex(owners yamlext.Set, key, structName string, spec indexSpec) (schemamodel.Index, error) {
	fields := cleanStrings(spec.Fields)
	if len(fields) == 0 {
		fields = cleanStrings(spec.Columns)
	}
	if len(fields) == 0 {
		return schemamodel.Index{}, fmt.Errorf("index %q requires fields", key)
	}
	if structName == "" && string(spec.TableName) == "" {
		return schemamodel.Index{}, fmt.Errorf("top-level index %q requires table", key)
	}
	var include []string
	if len(spec.Include) > 0 {
		include = cleanStrings(spec.Include)
		if len(include) != len(spec.Include) {
			return schemamodel.Index{}, fmt.Errorf("index %q names an empty include column", key)
		}
	}
	// The keys owners add to an index, such as YDB's partitioning and
	// full-text options, are read by their owners into settings and options
	// of the index; an owner reads the index's type and operator class too.
	scalars, _, err := ownedKeys(owners, yamlext.EntryIndex, fmt.Sprintf("index %q", key), spec.Owned)
	if err != nil {
		return schemamodel.Index{}, err
	}
	scalars["type"] = string(spec.Type)
	if spec.Operator != "" {
		scalars["ops"] = string(spec.Operator)
	}
	facets, err := owners.DecodeEntryAttributes(yamlext.EntryIndex, scalars)
	if err != nil {
		return schemamodel.Index{}, fmt.Errorf("index %q: %w", key, err)
	}
	options, err := owners.DecodeEntryParameters(yamlext.EntryIndex, scalars)
	if err != nil {
		return schemamodel.Index{}, fmt.Errorf("index %q: %w", key, err)
	}

	return schemamodel.Index{
		Facets:         facets,
		StructName:     structName,
		Name:           valueOrDefault(spec.Name, key),
		Fields:         fields,
		IncludeColumns: include,
		Unique:         spec.Unique,
		Comment:        string(spec.Comment),
		Type:           string(spec.Type),
		Condition:      firstNonEmpty(string(spec.Where), string(spec.Condition)),
		Operator:       string(spec.Operator),
		TableName:      string(spec.TableName),
		Overrides:      mergePlatform(spec.Platform, nil),
		StorageParams:  options,
	}, nil
}

func addTableConstraints(db *schemamodel.Database, structName, tableName string, constraints orderedMap[constraintSpec]) error {
	for _, constraint := range constraints {
		value, err := buildConstraint(constraint.Name, structName, tableName, constraint.Value)
		if err != nil {
			return err
		}
		db.Constraints = append(db.Constraints, value)
	}
	return nil
}

func (d document) addConstraints(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Constraints) {
		constraint, err := buildConstraint(key, "", "", d.Constraints[key])
		if err != nil {
			return err
		}
		db.Constraints = append(db.Constraints, constraint)
	}
	return nil
}

func buildConstraint(key, structName, tableName string, spec constraintSpec) (schemamodel.Constraint, error) {
	constraintTable := string(spec.Table)
	if constraintTable == "" && structName == "" {
		constraintTable = tableName
	}
	constraintType := strings.ToUpper(string(spec.Type))
	columns := cleanStrings(spec.Columns)
	if err := validateConstraint(key, structName, constraintTable, constraintType, columns, spec); err != nil {
		return schemamodel.Constraint{}, err
	}

	return schemamodel.Constraint{
		StructName:      structName,
		Name:            valueOrDefault(spec.Name, key),
		Type:            constraintType,
		Table:           constraintTable,
		UsingMethod:     string(spec.UsingMethod),
		CheckExpression: string(spec.CheckExpression),
		Columns:         columns,
		ForeignTable:    string(spec.ForeignTable),
		ForeignColumn:   string(spec.ForeignColumn),
		OnDelete:        string(spec.OnDelete),
		OnUpdate:        string(spec.OnUpdate),
		Comment:         string(spec.Comment),
	}, nil
}

func validateConstraint(key, structName, tableName, constraintType string, columns []string, spec constraintSpec) error {
	if structName == "" && tableName == "" {
		return fmt.Errorf("top-level constraint %q requires table", key)
	}

	switch constraintType {
	case "PRIMARY KEY", "UNIQUE":
		if len(columns) == 0 {
			return fmt.Errorf("constraint %q requires columns", key)
		}
	case "FOREIGN KEY":
		if len(columns) == 0 {
			return fmt.Errorf("constraint %q requires columns", key)
		}
		if spec.ForeignTable == "" || spec.ForeignColumn == "" {
			return fmt.Errorf("constraint %q requires foreign_table and foreign_column", key)
		}
	case "CHECK":
		if spec.CheckExpression == "" {
			return fmt.Errorf("constraint %q requires check", key)
		}
	case "":
		return fmt.Errorf("constraint %q requires type", key)
	default:
		return fmt.Errorf("constraint %q has unsupported type %q", key, constraintType)
	}
	return nil
}

func (d document) addFunctions(db *schemamodel.Database, owners yamlext.Set) error {
	for _, key := range sortedKeys(d.Functions) {
		spec := d.Functions[key]
		parameters := string(spec.Parameters)
		if parameters == "" {
			parameters = string(spec.Params)
		}
		attributes, _, err := ownedKeys(owners, yamlext.EntryFunction, fmt.Sprintf("function %q", key), spec.Owned)
		if err != nil {
			return err
		}
		facets, err := owners.DecodeEntryAttributes(yamlext.EntryFunction, attributes)
		if err != nil {
			return fmt.Errorf("function %q: %w", key, err)
		}
		settings := make([]string, 0, len(spec.Settings))
		for _, setting := range spec.Settings {
			settings = append(settings, string(setting))
		}

		fn := schemamodel.Function{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Parameters: parameters,
			Returns:    string(spec.Returns),
			Language:   string(spec.Language),
			Security:   string(spec.Security),
			Volatility: string(spec.Volatility),
			// The settings key was accepted and never read, so a YAML
			// document's search_path reached nothing (stokaro/ptah#3630).
			Settings: routinesetting.NormalizeAll(settings),
			Facets:   facets,
			Body:     string(spec.Body),
			Comment:  string(spec.Comment),
		}
		fn.Canonicalize()
		db.Functions = append(db.Functions, fn)
	}
	return nil
}

func (d document) addViews(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Views) {
		spec := d.Views[key]
		if string(spec.Body) == "" {
			return fmt.Errorf("view %q requires body", key)
		}
		db.Views = append(db.Views, schemamodel.View{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Body:       string(spec.Body),
			WithCheck:  spec.WithCheck,
			Comment:    string(spec.Comment),
		})
	}
	return nil
}

// addMaterializedViews reads each view and its owner attributes in key order.
func (d document) addMaterializedViews(db *schemamodel.Database, owners yamlext.Set) error {
	for _, key := range sortedKeys(d.MaterializedViews) {
		spec := d.MaterializedViews[key]
		if string(spec.Body) == "" {
			return fmt.Errorf("materialized view %q requires body", key)
		}
		if spec.RefreshStrategy.Kind != 0 {
			return matviewrefresh.Refuse(valueOrDefault(spec.Name, key))
		}
		attributes, _, err := ownedKeys(owners, yamlext.EntryMaterializedView, fmt.Sprintf("materialized view %q", key), spec.Owned)
		if err != nil {
			return err
		}
		facets, err := owners.DecodeEntryAttributes(yamlext.EntryMaterializedView, attributes)
		if err != nil {
			return fmt.Errorf("materialized view %q: %w", key, err)
		}
		db.MaterializedViews = append(db.MaterializedViews, schemamodel.MaterializedView{
			Facets:     facets,
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Body:       string(spec.Body),
			Comment:    string(spec.Comment),
		})
	}
	return nil
}

func (d document) addTriggers(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Triggers) {
		spec := d.Triggers[key]
		if string(spec.Table) == "" {
			return fmt.Errorf("trigger %q requires table", key)
		}
		if string(spec.Timing) == "" {
			return fmt.Errorf("trigger %q requires timing", key)
		}
		if string(spec.Event) == "" {
			return fmt.Errorf("trigger %q requires event", key)
		}
		if string(spec.Body) == "" {
			return fmt.Errorf("trigger %q requires body", key)
		}
		trigger := schemamodel.Trigger{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Table:      string(spec.Table),
			Timing:     string(spec.Timing),
			Event:      string(spec.Event),
			ForEach:    string(spec.ForEach),
			When:       string(spec.When),
			OldTable:   string(spec.OldTable),
			NewTable:   string(spec.NewTable),
			Body:       string(spec.Body),
			Comment:    string(spec.Comment),
		}
		trigger.Canonicalize()
		db.Triggers = append(db.Triggers, trigger)
	}
	return nil
}

// addRLS hands the document's row-level security to its owners. An entry a
// selected owner reads by its target scope goes to that owner: without
// `dialects`, or scoped to the PostgreSQL family, it is the row-security
// owner's, a policy becoming an object and an enablement the switches facet
// of its table, which the document must declare. An entry scoped to
// ClickHouse is refused: a ClickHouse row policy is its owner's, declared
// under the owner's row_policies key, and ClickHouse has no switch. An entry
// scoped to other targets stays shared. [yamlext.Set.TargetOwner] is the one
// place that routes an entry by its scope.
func (d document) addRLS(db *schemamodel.Database, owners yamlext.Set) error {
	var entries []yamlext.Entry
	for _, key := range sortedKeys(d.Tables) {
		if d.Tables[key].RLSEnabled {
			entries = append(entries, yamlext.Entry{Key: rlsEnabledTablesKey, Origin: fmt.Sprintf("tables.%s.rls_enabled", key),
				Attributes: map[string]string{"struct_name": valueOrDefault(d.Tables[key].StructName, key)}})
		}
	}
	for _, group := range []struct {
		name  string
		specs map[string]rlsEnableSpec
	}{{"rls_enabled_tables", d.RLSEnabledTables}, {"rls_enabled", d.RLSEnabled}} {
		for _, key := range sortedKeys(group.specs) {
			spec := group.specs[key]
			origin := fmt.Sprintf("%s.%s", group.name, key)
			attributes := presentAttributes(map[string]string{"struct_name": string(spec.StructName),
				"table": valueOrDefault(spec.Table, key), "comment": string(spec.Comment)})
			entry, owned, err := routeRowSecurity(owners, rlsEnabledTablesKey, origin, spec.Dialects, attributes)
			if err != nil {
				return err
			}
			if owned {
				entries = append(entries, entry)
				continue
			}
			if scopesClickHouse(entry.Targets) {
				return fmt.Errorf("%s: %w: ClickHouse has no row-level security switch: a row policy filters rows once it exists; "+
					"leave clickhouse out of the enablement's dialects", origin, ptaherr.ErrInvalidAttributeValue)
			}
			db.RLSEnabledTables = append(db.RLSEnabledTables, schemamodel.RLSEnabledTable{StructName: string(spec.StructName),
				Table: valueOrDefault(spec.Table, key), Comment: string(spec.Comment), Dialects: entry.Targets})
		}
	}
	for _, key := range sortedKeys(d.RLSPolicies) {
		spec := d.RLSPolicies[key]
		origin := "rls_policies." + key
		restrictive, err := policyRestrictive(origin, string(spec.As))
		if err != nil {
			return err
		}
		attributes := presentAttributes(map[string]string{"struct_name": string(spec.StructName), "name": valueOrDefault(spec.Name, key),
			"table": string(spec.Table), "for": string(spec.PolicyFor), "to": string(spec.ToRoles), "as": string(spec.As),
			"using": string(spec.UsingExpression), "with_check": string(spec.WithCheckExpression), "comment": string(spec.Comment)})
		entry, owned, err := routeRowSecurity(owners, rlsPoliciesKey, origin, spec.Dialects, attributes)
		if err != nil {
			return err
		}
		if owned {
			entries = append(entries, entry)
			continue
		}
		if scopesClickHouse(entry.Targets) {
			return fmt.Errorf("%s: %w: a ClickHouse row policy is not a row-level security policy; declare it under "+
				"row_policies instead", origin, ptaherr.ErrInvalidAttributeValue)
		}
		db.RLSPolicies = append(db.RLSPolicies, schemamodel.RLSPolicy{
			StructName: string(spec.StructName), Name: valueOrDefault(spec.Name, key), Table: string(spec.Table),
			PolicyFor: string(spec.PolicyFor), ToRoles: string(spec.ToRoles), UsingExpression: string(spec.UsingExpression),
			WithCheckExpression: string(spec.WithCheckExpression), Comment: string(spec.Comment), Restrictive: restrictive,
			Dialects: entry.Targets,
		})
	}
	contributions, err := owners.ReadEntries(entries, d.documentTables(db))
	if err != nil {
		return err
	}
	for _, contribution := range contributions {
		if err := contribute(db, "row-level security", contribution); err != nil {
			return err
		}
	}
	return nil
}

// policyRestrictive reads a policy's `as`, which decides how the policy
// combines with the table's others. An unrecognized value is refused rather
// than read as the default: PERMISSIVE is the weaker of the two, so folding a
// misspelled RESTRICTIVE into it would grant the access the policy was
// written to withhold, as the annotation refuses it (stokaro/ptah#3121).
func policyRestrictive(origin, written string) (bool, error) {
	switch strings.ToUpper(strings.TrimSpace(written)) {
	case "", "PERMISSIVE":
		return false, nil
	case "RESTRICTIVE":
		return true, nil
	default:
		return false, fmt.Errorf("%s: %w: as must be PERMISSIVE or RESTRICTIVE, got %q", origin, ptaherr.ErrInvalidAttributeValue, written)
	}
}

// The frontend keys whose row-level security entries an owner may read by
// their target scope. rls_enabled and a table's rls_enabled route as
// rls_enabled_tables.
const (
	rlsPoliciesKey      = "rls_policies"
	rlsEnabledTablesKey = "rls_enabled_tables"
)

// routeRowSecurity reads a row-level security entry's `dialects` and asks
// the owners whether one reads the entry. It returns the entry as an owner
// reads it, with the scope it was written with either way.
func routeRowSecurity(owners yamlext.Set, key, origin string, dialects yaml.Node, attributes map[string]string) (yamlext.Entry, bool, error) {
	scope, err := rowSecurityScope(origin, dialects)
	if err != nil {
		return yamlext.Entry{}, false, err
	}
	entry := yamlext.Entry{Key: key, Origin: origin, Attributes: attributes, Targets: scope}
	_, owned, err := owners.TargetOwner(key, scope)
	if err != nil {
		return yamlext.Entry{}, false, fmt.Errorf("%s: %w", origin, err)
	}
	return entry, owned, nil
}

// presentAttributes drops the attributes an entry leaves empty, so an owner
// reads a field the entry leaves out as absent, as it does from a Go
// annotation.
func presentAttributes(attributes map[string]string) map[string]string {
	for name, value := range attributes {
		if value == "" {
			delete(attributes, name)
		}
	}
	return attributes
}

// scopesClickHouse reports whether a row-level security entry's scope names a
// target whose row policy is its owner's and not row-level security, as its
// descriptor says (descriptor.Conversion.RowPolicyOwned): ClickHouse.
func scopesClickHouse(scope []string) bool {
	return slices.ContainsFunc(scope, func(target string) bool {
		described, _ := platform.Lookup(target)
		return described.Conversion.RowPolicyOwned
	})
}

// rowSecurityScope reads a row-level security entry's `dialects`. An entry
// without the key names no target.
func rowSecurityScope(origin string, node yaml.Node) ([]string, error) {
	if node.Kind == 0 {
		return nil, nil
	}
	var written stringList
	if err := node.Decode(&written); err != nil {
		return nil, fmt.Errorf("%s has an invalid dialects value: %w", origin, err)
	}
	scope, err := dialectscope.Parse(strings.Join(cleanStrings(written), ","))
	if err != nil {
		return nil, fmt.Errorf("%s dialects: %w", origin, err)
	}
	return scope, nil
}

func (d document) addRoles(db *schemamodel.Database) {
	for _, key := range sortedKeys(d.Roles) {
		spec := d.Roles[key]
		inherit := true
		if spec.Inherit != nil {
			inherit = *spec.Inherit
		}
		// Nil for none, as the annotation parser leaves it.
		var memberOf []string
		if groups := cleanStrings(spec.MemberOf); len(groups) > 0 {
			memberOf = groups
		}
		db.Roles = append(db.Roles, schemamodel.Role{
			StructName:  string(spec.StructName),
			Name:        valueOrDefault(spec.Name, key),
			Login:       spec.Login,
			Password:    string(spec.Password),
			Superuser:   spec.Superuser,
			CreateDB:    spec.CreateDB,
			CreateRole:  spec.CreateRole,
			Inherit:     inherit,
			Replication: spec.Replication,
			Comment:     string(spec.Comment),
			Group:       spec.Group,
			MemberOf:    memberOf,
		})
	}
}

func (d document) addGrants(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Grants) {
		spec := d.Grants[key]
		grant, err := buildGrant("grant", key, revokeSpec{
			StructName: spec.StructName, Role: spec.Role, Privilege: spec.Privilege, Privileges: spec.Privileges,
			OnTable: spec.OnTable, OnSchema: spec.OnSchema, OnSequence: spec.OnSequence,
			OnFunction: spec.OnFunction, OnProcedure: spec.OnProcedure, OnDatabase: spec.OnDatabase,
			objectTargets: spec.objectTargets, Comment: spec.Comment,
		})
		if err != nil {
			return err
		}
		grant.WithOption = spec.WithOption
		db.Grants = append(db.Grants, grant)
	}
	return nil
}

// addRevokes reads the `revokes` entries into the model's revoked grants.
func (d document) addRevokes(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Revokes) {
		revoked, err := buildGrant("revoke", key, d.Revokes[key])
		if err != nil {
			return err
		}
		if revoked.Role == "" || len(revoked.Privileges) == 0 {
			return fmt.Errorf("revoke %q requires role and privileges", key)
		}
		db.RevokedGrants = append(db.RevokedGrants, revoked)
	}
	return nil
}

// buildGrant reads the part a grant and a revoke share: the role, the
// privileges and the target.
func buildGrant(kind, key string, spec revokeSpec) (schemamodel.Grant, error) {
	privileges := cleanStrings(spec.Privilege)
	if len(privileges) == 0 {
		privileges = cleanStrings(spec.Privileges)
	}
	grant := schemamodel.Grant{
		StructName: string(spec.StructName),
		Role:       string(spec.Role),
		Privileges: privileges,
		OnTable:    string(spec.OnTable),
		OnSchema:   string(spec.OnSchema),
		OnSequence: string(spec.OnSequence),
		OnDatabase: spec.OnDatabase,
		Comment:    string(spec.Comment),
	}
	for _, routine := range []struct {
		attribute, kind, value string
	}{
		{"on_function", "FUNCTION", string(spec.OnFunction)},
		{"on_procedure", "PROCEDURE", string(spec.OnProcedure)},
	} {
		if strings.TrimSpace(routine.value) == "" {
			continue
		}
		name, arguments, ok := routineargs.SplitTarget(routine.value)
		if !ok {
			return schemamodel.Grant{}, fmt.Errorf(
				"%s %q %s %q needs the argument types in parentheses, as in purge(uuid): "+
					"PostgreSQL tells overloaded routines apart by them", kind, key, routine.attribute, routine.value)
		}
		grant.OnRoutine, grant.RoutineArguments, grant.RoutineKind = name, arguments, routine.kind
	}
	targets := spec.byAttribute()
	var named string
	for _, object := range schemamodel.GrantObjectKinds() {
		value := strings.TrimSpace(targets[object.Attribute])
		if value == "" {
			continue
		}
		if named != "" {
			return schemamodel.Grant{}, fmt.Errorf("%s %q names a target with both %s and %s; a %s names one object",
				kind, key, named, object.Attribute, kind)
		}
		named = object.Attribute
		grant.OnObject, grant.ObjectKind = value, object.Kind
	}
	grant.Canonicalize()
	return grant, nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func valueOrDefault(value stringScalar, fallback string) string {
	if string(value) != "" {
		return string(value)
	}
	return fallback
}

func cleanStrings(values stringList) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// withoutGroups returns spec without the groups a document owner claims.
// The value of such a group is the owner's, and a copy in the table's overrides
// would reach a consumer that refuses overrides it cannot project. Only the
// platform key is read this way: a group written under overrides stays, so it
// is never dropped without reaching the owner.
func withoutGroups(spec platformSpec, groups []string) platformSpec {
	if !slices.ContainsFunc(groups, func(group string) bool { _, written := spec[group]; return written }) {
		return spec
	}
	kept := maps.Clone(spec)
	for _, group := range groups {
		delete(kept, group)
	}
	return kept
}

func mergePlatform(primary, secondary platformSpec) map[string]map[string]string {
	if len(primary) == 0 && len(secondary) == 0 {
		return nil
	}

	result := make(map[string]map[string]string)
	copyPlatform(result, primary)
	copyPlatform(result, secondary)
	return result
}

func copyPlatform(target map[string]map[string]string, source platformSpec) {
	for group, values := range source {
		if target[group] == nil {
			target[group] = make(map[string]string)
		}
		for key, value := range values {
			target[group][key] = string(value)
		}
	}
}

// The value types the frontend reads its keys with, shared with the owners
// that read keys of their own.
type (
	stringScalar      = yamlvalue.Scalar
	stringList        = yamlvalue.List
	orderedMap[V any] = yamlvalue.OrderedMap[V]
)

func decodeKnownFields[V any](node *yaml.Node, target *V) error {
	return yamlvalue.DecodeKnownFields(node, target)
}
