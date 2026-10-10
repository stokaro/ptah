package ydbindex

import (
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// Resolve reads a declaration against the settings an index's own table
// holds: each setting the declaration names, and the held value of each it
// leaves out (see [ydbpartition.Declared.Over]). A nil declaration resolves to
// held, so removing a declaration changes nothing. A declaration YDB would
// refuse is an error saying why.
//
// The settings are read, compared and changed as a table's are, through
// [ydbpartition]: the index's settings are those of the table YDB keeps it in.
func Resolve(spec *ydbschema.IndexPartitioning, held ydbpartition.Settings) (ydbpartition.Settings, error) {
	if spec == nil || spec.IsZero() {
		return held, nil
	}
	return declaredOf(spec).Over(held)
}

// Held reads a reader's report of what an index holds, written by [Spec] as
// the settings that differ from [ydbpartition.DefaultSettings]. A nil report
// is an index holding the defaults, which is what YDB gives a new index
// whatever its table holds.
func Held(spec *ydbschema.IndexPartitioning) (ydbpartition.Settings, error) {
	return Resolve(spec, ydbpartition.DefaultSettings())
}

// Spec writes settings as what differs from [ydbpartition.DefaultSettings],
// which is what a reader reports for an index: nil for an index holding the
// defaults. [Held] gives the settings back.
func Spec(settings ydbpartition.Settings) *ydbschema.IndexPartitioning {
	spec := specOf(settings.Declared())
	if spec == nil || spec.IsZero() {
		return nil
	}
	return spec
}

// Explicit writes settings as an index declaration that names every setting,
// so it resolves to them over whatever the index holds; see
// [ydbpartition.Settings.Explicit].
func Explicit(settings ydbpartition.Settings) *ydbschema.IndexPartitioning {
	return specOf(settings.Explicit())
}

// CreateClause writes the settings the ALTER INDEX that follows a new index
// names: each one the declaration names, and nothing for one it leaves out,
// which the index takes from YDB (see [ydbpartition.Declared.CreateClause]).
func CreateClause(spec *ydbschema.IndexPartitioning) []string {
	if spec == nil || spec.IsZero() {
		return nil
	}
	return declaredOf(spec).CreateClause()
}

// declaredOf reads an index declaration as the settings a table and an index
// share.
func declaredOf(spec *ydbschema.IndexPartitioning) ydbpartition.Declared {
	return ydbpartition.Declared{
		BySize:          spec.BySize,
		PartitionSizeMB: spec.PartitionSizeMB,
		ByLoad:          spec.ByLoad,
		MinPartitions:   spec.MinPartitions,
		MaxPartitions:   spec.MaxPartitions,
		ReadReplicas:    spec.ReadReplicas,
	}
}

// specOf writes the shared settings as an index declaration.
func specOf(declared ydbpartition.Declared) *ydbschema.IndexPartitioning {
	return &ydbschema.IndexPartitioning{
		BySize:          declared.BySize,
		PartitionSizeMB: declared.PartitionSizeMB,
		ByLoad:          declared.ByLoad,
		MinPartitions:   declared.MinPartitions,
		MaxPartitions:   declared.MaxPartitions,
		ReadReplicas:    declared.ReadReplicas,
	}
}

// RenameKeeps reports whether renaming an index in place carries what its
// facets state: an index's partitioning is carried, since the YDB owner
// compares it under the new name and changes it in place. Any other facet,
// such as a vector index's settings, which are fixed when the index is built,
// is not, and a drop and a create of such an index are not a rename.
func RenameKeeps(facets schemaext.Facets) bool {
	return !slices.ContainsFunc(facets.DeclaredKinds(), func(kind schemaext.Kind) bool {
		return kind != ydbschema.IndexPartitioningKind
	})
}
