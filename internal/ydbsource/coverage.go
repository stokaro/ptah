// Package ydbsource records feature knowledge for schema source adapters. Each
// format explicitly enrolls the declaration namespaces it supports.
package ydbsource

import (
	"fmt"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
)

// Limits records source declarations that leave standalone objects unmanaged.
// An empty name leaves its entire namespace unmanaged.
type Limits struct {
	Coordination []string
	Streaming    []string
	Pools        []string
	Classifiers  []string
	Secrets      []string
	Topics       []string
	// ExternalDataSources and ExternalTables name external objects by their
	// paths, as Secrets and Topics do.
	ExternalDataSources []string
	ExternalTables      []string
}

const unmanagedObjectReason = "the source leaves this object unmanaged"

func unmanagedNamespaceReason(label string) string {
	return "the source leaves " + label + " unmanaged"
}

type sourceFamily struct {
	kind  schemaext.Kind
	token string
	label string
	// unmanaged are the reasons a database read gives an object it leaves
	// unmanaged, which a source writes as an unmanaged object.
	unmanaged []string
}

// sourceKinds is shared by limit decoding and export. A new spelling must be
// recognized in both directions or exporting unknown coverage would grant
// authority that the input never held.
var sourceKinds = []sourceFamily{
	{ydbcoordination.Kind, "coordination_node", "coordination nodes", nil},
	{ydbstreaming.Kind, "streaming_query", "streaming queries", nil},
	{ydbworkload.PoolKind, "resource_pool", "resource pools", nil},
	{ydbworkload.ClassifierKind, "resource_pool_classifier", "resource pool classifiers", nil},
	{ydbsecret.Kind, "secret", "secrets", []string{ydbsecret.UnsupportedReason}},
	{ydbtopic.Kind, "topic", "topics", []string{ydbtopic.UnsupportedReason, ydbtopic.QueueGroupReason}},
	{ydbexternal.SourceKind, "external_data_source", "external data sources", []string{ydbexternal.UnsupportedReason}},
	{ydbexternal.TableKind, "external_table", "external tables", []string{ydbexternal.UnsupportedReason}},
}

func sourceKind(token string) schemaext.Kind {
	for _, family := range sourceKinds {
		if family.token == strings.ToLower(strings.TrimSpace(token)) {
			return family.kind
		}
	}
	return ""
}

func sourceLabel(kind schemaext.Kind) string {
	for _, family := range sourceKinds {
		if family.kind == kind {
			return family.label
		}
	}
	return ""
}

// RecognizesLimit reports whether this adapter owns a source limit's spelling.
// Document splitting uses it to preserve owner directives without interpreting
// them as common coverage or admitting unknown kinds.
func RecognizesLimit(kind coverage.Kind) bool { return sourceKind(string(kind)) != "" }

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
	case ydbsecret.Kind:
		l.Secrets = append(l.Secrets, name)
	case ydbtopic.Kind:
		l.Topics = append(l.Topics, name)
	case ydbexternal.SourceKind:
		l.ExternalDataSources = append(l.ExternalDataSources, name)
	case ydbexternal.TableKind:
		l.ExternalTables = append(l.ExternalTables, name)
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

// ConsumeHCLDirective records only limits on the namespace HCL can declare.
// Workload and streaming namespaces remain unenrolled in an HCL source.
func (l *Limits) ConsumeHCLDirective(object coverage.Object) (bool, error) {
	if sourceKind(string(object.Kind)) != ydbcoordination.Kind {
		return false, nil
	}
	return l.ConsumeDirective(object)
}

// HCLCoverage records an explicit source limit on coordination nodes. An HCL
// document without an owned directive makes no namespace claim: ordinary Atlas
// files must not authorize removal of objects the format did not name.
func HCLCoverage(limits Limits) (schemaext.Coverage, error) {
	if len(limits.Coordination) == 0 {
		return schemaext.NewCoverage(schemaext.Desired, nil, nil)
	}
	return namespaceCoverage(limits.Coordination, ydbcoordination.Kind, schemeIdentity(ydbcoordination.Ref), ydbcoordination.ValidateIdentity, ydbcoordination.Coverage)
}

// Coverage enrolls only namespaces these source formats can declare,
// including a table's TTL, which each of them can write. HCL enrolls
// coordination separately and leaves the TTL unmanaged. Runtime registration
// never expands a source's vocabulary.
func Coverage(limits Limits) (schemaext.Coverage, error) {
	feeds, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	ttl, err := ydbschema.TTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	if feeds, err = feeds.Combine(ttl); err != nil {
		return schemaext.Coverage{}, err
	}
	nodes, err := namespaceCoverage(limits.Coordination, ydbcoordination.Kind, schemeIdentity(ydbcoordination.Ref), ydbcoordination.ValidateIdentity, ydbcoordination.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	queries, err := namespaceCoverage(limits.Streaming, ydbstreaming.Kind, schemeIdentity(ydbstreaming.Ref), ydbstreaming.ValidateIdentity, ydbstreaming.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	secrets, err := pathNamespaceCoverage(limits.Secrets, "secret", ydbsecret.Kind, ydbsecret.ParsePath, ydbsecret.ValidateIdentity, ydbsecret.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	topics, err := pathNamespaceCoverage(limits.Topics, "topic", ydbtopic.Kind, ydbtopic.ParsePath, ydbtopic.ValidateIdentity, ydbtopic.Coverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	sources, err := pathNamespaceCoverage(limits.ExternalDataSources, "external data source", ydbexternal.SourceKind,
		externalPath(ydbexternal.SourceKind), ydbexternal.ValidateIdentity, ydbexternal.SourceCoverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	tables, err := pathNamespaceCoverage(limits.ExternalTables, "external table", ydbexternal.TableKind,
		externalPath(ydbexternal.TableKind), ydbexternal.ValidateIdentity, ydbexternal.TableCoverage)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	combined, err := feeds.Combine(nodes)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, known := range []schemaext.Coverage{queries, secrets, topics, sources, tables} {
		combined, err = combined.Combine(known)
		if err != nil {
			return schemaext.Coverage{}, err
		}
	}
	for _, family := range []struct {
		limits   []string
		kind     schemaext.Kind
		identity func(string) objectidentity.ID
	}{
		{limits.Pools, ydbworkload.PoolKind, ydbworkload.PoolRef},
		{limits.Classifiers, ydbworkload.ClassifierKind, ydbworkload.ClassifierRef},
	} {
		known, err := namespaceCoverage(family.limits, family.kind, family.identity,
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

func namespaceCoverage(limits []string, kind schemaext.Kind,
	identity func(string) objectidentity.ID, validate func(objectidentity.ID) error,
	enroll func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error),
) (schemaext.Coverage, error) {
	namespace := schemaext.Knowledge{State: schemaext.Complete}
	limit := schemaext.Knowledge{State: schemaext.Uninspected, Reason: unmanagedObjectReason}
	var subjects []schemaext.SubjectCoverage
	seen := make(map[objectidentity.Key]bool)
	for _, name := range limits {
		if name == "" {
			namespace = schemaext.Knowledge{State: schemaext.Uninspected, Reason: unmanagedNamespaceReason(sourceLabel(kind))}
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

// pathNamespaceCoverage records the limits of a family whose limit names the
// object by its path, as every other spelling of a secret, a topic or an
// external object does: a
// slash separates directories and a dot stays in its segment, so `pg.pw` is
// one object at the root, never pw in a directory pg. A limit parse refuses,
// an absolute path included, is reported with parse's reason.
func pathNamespaceCoverage(limits []string, label string, kind schemaext.Kind,
	parse func(string) (objectidentity.ID, error), validate func(objectidentity.ID) error,
	enroll func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error),
) (schemaext.Coverage, error) {
	for _, name := range limits {
		if _, err := parse(name); name != "" && err != nil {
			return schemaext.Coverage{}, fmt.Errorf("%w: %s limit: %w", schemaext.ErrInvalidValue, label, err)
		}
	}
	identity := func(name string) objectidentity.ID {
		ref, _ := parse(name)
		return ref
	}
	return namespaceCoverage(limits, kind, identity, validate, enroll)
}

// externalPath reads a limit on an external object of kind as its path.
func externalPath(kind schemaext.Kind) func(string) (objectidentity.ID, error) {
	return func(written string) (objectidentity.ID, error) { return ydbexternal.ParsePath(kind, written) }
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
