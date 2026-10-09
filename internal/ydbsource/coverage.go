// Package ydbsource records feature knowledge shared by the Go, YAML, and YQL
// schema readers. Each format explicitly supports these declaration namespaces.
package ydbsource

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
)

// Coverage captures the namespaces these formats can declare, including an
// empty document. HCL enrolls coordination nodes separately because it cannot
// declare changefeeds. Runtime registration never expands source knowledge.
// Each optional coordination limit names a path the author leaves unmanaged;
// an empty name leaves the whole namespace unmanaged.
func Coverage(coordinationLimits ...string) (schemaext.Coverage, error) {
	feeds, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	nodes, err := coordinationCoverage(coordinationLimits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return feeds.Combine(nodes)
}

func coordinationCoverage(limits []string) (schemaext.Coverage, error) {
	namespace := schemaext.Knowledge{State: schemaext.Complete}
	limit := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source declares this coordination node unmanaged"}
	var subjects []schemaext.SubjectCoverage
	seen := make(map[objectidentity.Key]bool)
	for _, name := range limits {
		if name == "" {
			namespace = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source declares coordination nodes unmanaged"}
			continue
		}
		// A source path is already decoded. Dots within its directory or leaf
		// are literal; only names without slashes use SQL qualification.
		physical := name
		if !strings.Contains(name, "/") {
			physical = ydbscheme.ObjectPath(name)
		}
		schema, leaf := "", physical
		if slash := strings.LastIndex(physical, "/"); slash >= 0 {
			schema, leaf = physical[:slash], physical[slash+1:]
		}
		ref := ydbcoordination.Ref(schema, leaf)
		if err := ydbcoordination.ValidateIdentity(ref); err != nil {
			return schemaext.Coverage{}, err
		}
		if !seen[ref.Key()] {
			subjects = append(subjects, schemaext.SubjectCoverage{Kind: ydbcoordination.Kind, Subject: ref, Knowledge: limit})
			seen[ref.Key()] = true
		}
	}
	return ydbcoordination.Coverage(schemaext.Desired, namespace, subjects)
}
