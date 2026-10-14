// Package schemamerge merges the desired-schema descriptions of several files
// of one load into one, in the order the load reads them: every object family
// is appended, grants compose so a later REVOKE takes back an earlier GRANT,
// and the coverage of the whole is the union of what each file leaves
// undescribed. The schema file loader and an owner that reads a load of its
// own language both merge through it, so the two cannot disagree.
package schemamerge

import (
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/privilegefold"
)

// Append merges one parsed file, src, into dst, the description being built
// from all of them.
//
// EVERY object family of [schemamodel.Database] belongs here. Two did not, and the
// consequence was not a compile error: `ptah schema apply --to file://x.hcl`
// silently ignored every declared SQL Server synonym and extended property and
// reported `Schema is synced, no changes to be made`, while the same file
// through schemafile.Load planned all of them. Nothing else could see it,
// because schemafile.Load returns the parsed database directly and every unit
// test goes that way (stokaro/ptah#1999).
//
// TestAppend_MergesEveryObjectFamily is what keeps the next family
// from being missed the same way: it reflects over the struct rather than
// trusting this list.
//
// An owner's object both describe is combined as a declaration: an extension
// two files declare alike is one extension, and two that differ are refused;
// see [schemaext.Objects.MergeDeclarations].
func Append(dst, src *schemamodel.Database) error {
	objects, err := dst.FeatureObjects.MergeDeclarations(src.FeatureObjects)
	if err != nil {
		return err
	}
	facets, err := dst.Facets.Merge(src.Facets)
	if err != nil {
		return err
	}
	// This private destination starts as a fresh file accumulator. Capture a
	// direction even for an unknown first source so a later known source can
	// never reinterpret that first source as an empty accumulator.
	sourceCoverage := src.FeatureCoverage
	if sourceCoverage.Representation() == "" {
		sourceCoverage, err = schemaext.NewCoverage(schemaext.Desired, nil, nil)
		if err != nil {
			return err
		}
	}
	mergedCoverage := sourceCoverage
	if dst.FeatureCoverage.Representation() != "" {
		mergedCoverage, err = dst.FeatureCoverage.Merge(sourceCoverage)
		if err != nil {
			return err
		}
	}
	dst.FeatureObjects, dst.Facets, dst.FeatureCoverage = objects, facets, mergedCoverage
	dst.Schemas = append(dst.Schemas, src.Schemas...)
	dst.Tables = append(dst.Tables, src.Tables...)
	dst.Fields = append(dst.Fields, src.Fields...)
	dst.Indexes = append(dst.Indexes, src.Indexes...)
	dst.Constraints = append(dst.Constraints, src.Constraints...)
	dst.Enums = append(dst.Enums, src.Enums...)
	dst.EmbeddedFields = append(dst.EmbeddedFields, src.EmbeddedFields...)
	dst.Sequences = append(dst.Sequences, src.Sequences...)
	dst.Domains = append(dst.Domains, src.Domains...)
	dst.CompositeTypes = append(dst.CompositeTypes, src.CompositeTypes...)
	dst.Ranges = append(dst.Ranges, src.Ranges...)
	dst.Functions = append(dst.Functions, src.Functions...)
	dst.Views = append(dst.Views, src.Views...)
	dst.MaterializedViews = append(dst.MaterializedViews, src.MaterializedViews...)
	dst.Triggers = append(dst.Triggers, src.Triggers...)
	dst.RLSPolicies = append(dst.RLSPolicies, src.RLSPolicies...)
	dst.RLSEnabledTables = append(dst.RLSEnabledTables, src.RLSEnabledTables...)
	dst.Roles = append(dst.Roles, src.Roles...)
	// Grants and revoked grants compose in file order: a later file's REVOKE
	// takes back an earlier file's GRANT, as it would in one file.
	privilegefold.Merge(dst, src)
	dst.ManagedData = append(dst.ManagedData, src.ManagedData...)
	// Several files loaded together are one description, and it describes only
	// what all of them together describe. Union, never intersection: a limit
	// one file declares is a limit of the whole (stokaro/ptah#1276).
	dst.NotDescribed = dst.NotDescribed.Merge(src.NotDescribed)
	return nil
}
