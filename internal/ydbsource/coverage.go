// Package ydbsource records feature knowledge shared by the Go, YAML, and YQL
// schema readers. Each format explicitly supports these declaration namespaces.
package ydbsource

import (
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
)

// Limits records source declarations that leave standalone objects unmanaged.
// An empty name leaves its entire namespace unmanaged.
type Limits struct {
	Coordination []string
	Streaming    []string
	Pools        []string
	Classifiers  []string
}

// sourceKinds is shared by limit decoding and export. A new spelling must be
// recognized in both directions or exporting unknown coverage would grant
// authority that the input never held.
var sourceKinds = []struct {
	kind  schemaext.Kind
	token string
}{
	{ydbcoordination.Kind, "coordination_node"},
	{ydbstreaming.Kind, "streaming_query"},
	{ydbworkload.PoolKind, "resource_pool"},
	{ydbworkload.ClassifierKind, "resource_pool_classifier"},
}

func sourceKind(token string) schemaext.Kind {
	for _, family := range sourceKinds {
		if family.token == strings.ToLower(strings.TrimSpace(token)) {
			return family.kind
		}
	}
	return ""
}

// UnenrolledNamespaces returns source kind tokens whose knowledge was never
// captured, in deterministic declaration order. A Go export must explicitly
// leave them unmanaged; otherwise reading it grants complete source coverage.
// Explicitly enrolled limits are handled by the export validators.
func UnenrolledNamespaces(known schemaext.Coverage) []string {
	var tokens []string
	records := known.KindRecords()
	for _, family := range sourceKinds {
		if !slices.ContainsFunc(records, func(record schemaext.KindCoverage) bool { return record.Model.Kind == family.kind }) {
			tokens = append(tokens, family.token)
		}
	}
	return tokens
}

// Add records a source-level unmanaged-object directive if this adapter owns
// its spelling. Go annotations and SQL headers share this recognition so one
// format cannot accidentally turn the other's workload limit into absence.
func (l *Limits) Add(kind, name string) bool {
	switch sourceKind(kind) {
	case ydbcoordination.Kind:
		l.Coordination = append(l.Coordination, name)
	case ydbstreaming.Kind:
		l.Streaming = append(l.Streaming, name)
	case ydbworkload.PoolKind:
		l.Pools = append(l.Pools, name)
	case ydbworkload.ClassifierKind:
		l.Classifiers = append(l.Classifiers, name)
	default:
		return false
	}
	return true
}

// ConsumeDirective records a supported source-header limit for the owner
// coverage. The common header decoder validates its syntax and attributes.
func (l *Limits) ConsumeDirective(object coverage.Object) (bool, error) {
	return l.Add(string(object.Kind), object.Name), nil
}

// Coverage enrolls only namespaces these source formats can declare. HCL
// enrolls coordination separately. Runtime registration never expands a
// source's vocabulary.
func Coverage(limits Limits) (schemaext.Coverage, error) {
	feeds, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	nodes, err := namespaceCoverage(limits.Coordination, ydbcoordination.Kind, "coordination nodes", schemeIdentity(ydbcoordination.Ref), ydbcoordination.ValidateIdentity, ydbcoordination.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	queries, err := namespaceCoverage(limits.Streaming, ydbstreaming.Kind, "streaming queries", schemeIdentity(ydbstreaming.Ref), ydbstreaming.ValidateIdentity, ydbstreaming.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	combined, err := feeds.Combine(nodes)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	combined, err = combined.Combine(queries)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, family := range []struct {
		limits   []string
		kind     schemaext.Kind
		label    string
		identity func(string) objectidentity.ID
	}{
		{limits.Pools, ydbworkload.PoolKind, "resource pools", ydbworkload.PoolRef},
		{limits.Classifiers, ydbworkload.ClassifierKind, "resource pool classifiers", ydbworkload.ClassifierRef},
	} {
		known, err := namespaceCoverage(family.limits, family.kind, family.label, family.identity,
			func(ref objectidentity.ID) error { return ydbworkload.ValidateIdentity(ref, family.kind) },
			func(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
				return ydbworkload.Coverage(family.kind, representation, knowledge, subjects)
			})
		if err != nil {
			return schemaext.Coverage{}, err
		}
		combined, err = combined.Combine(known)
		if err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return combined, nil
}

func namespaceCoverage(limits []string, kind schemaext.Kind, label string,
	identity func(string) objectidentity.ID, validate func(objectidentity.ID) error,
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
		ref := identity(name)
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

// Scheme paths and database-wide workload names have different grammars. A
// pool called batch.jobs keeps its dot; only scheme objects use qualification.
func schemeIdentity(identity func(string, string) objectidentity.ID) func(string) objectidentity.ID {
	return func(name string) objectidentity.ID {
		physical := name
		if !strings.Contains(name, "/") {
			physical = ydbscheme.ObjectPath(name)
		}
		schema, leaf := "", physical
		if slash := strings.LastIndex(physical, "/"); slash >= 0 {
			schema, leaf = physical[:slash], physical[slash+1:]
		}
		return identity(schema, leaf)
	}
}
