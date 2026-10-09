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
	"ptah.run/dialect/ydb/ydbstreaming"
)

// Limits records source declarations that leave standalone objects unmanaged.
// An empty name leaves its entire namespace unmanaged.
type Limits struct {
	Coordination []string
	Streaming    []string
}

// Coverage enrolls only namespaces these source formats can declare. HCL
// enrolls coordination separately; it cannot declare changefeeds or streaming
// queries. Runtime registration never expands a source's vocabulary.
func Coverage(limits Limits) (schemaext.Coverage, error) {
	feeds, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	nodes, err := namespaceCoverage(limits.Coordination, ydbcoordination.Kind, "coordination nodes", ydbcoordination.Ref, ydbcoordination.ValidateIdentity, ydbcoordination.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	queries, err := namespaceCoverage(limits.Streaming, ydbstreaming.Kind, "streaming queries", ydbstreaming.Ref, ydbstreaming.ValidateIdentity, ydbstreaming.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	combined, err := feeds.Combine(nodes)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return combined.Combine(queries)
}

func namespaceCoverage(limits []string, kind schemaext.Kind, label string,
	identity func(string, string) objectidentity.ID, validate func(objectidentity.ID) error,
	enroll func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error),
) (schemaext.Coverage, error) {
	namespace := schemaext.Knowledge{State: schemaext.Complete}
	limit := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source leaves this object unmanaged"}
	var subjects []schemaext.SubjectCoverage
	seen := make(map[objectidentity.Key]bool)
	for _, name := range limits {
		if name == "" {
			namespace = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source leaves " + label + " unmanaged"}
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
		ref := identity(schema, leaf)
		if err := validate(ref); err != nil {
			return schemaext.Coverage{}, err
		}
		if !seen[ref.Key()] {
			subjects = append(subjects, schemaext.SubjectCoverage{Kind: kind, Subject: ref, Knowledge: limit})
			seen[ref.Key()] = true
		}
	}
	return enroll(schemaext.Desired, namespace, subjects)
}
