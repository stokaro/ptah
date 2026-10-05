// Package ydbpool reads, checks, compares and writes YDB's workload
// management objects: resource pools, which limit the queries that run in
// them, and resource pool classifiers, which send a user's or a group's
// queries to a pool.
//
// The annotation parser, the YAML loader, the renderer, the planner and the
// comparison all go through it, so a declaration is checked the same way
// wherever it was written, and the statement a plan writes is the one a render
// writes.
//
// Every fact here was measured on local-ydb 25.1.4.7 to 26.2.1.14 with the
// EnableResourcePools flag on, which is off by default on each of them.
package ydbpool

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// DefaultPool is the pool YDB creates for a database, which runs every query a
// classifier does not send elsewhere. Measured on 25.1.4.7 and 26.2.1.14:
// `DROP RESOURCE POOL default` is accepted, and every later query of the
// database, `CREATE RESOURCE POOL default` included, answers `Resource pool
// default not found or you don't have access permissions`. So Ptah changes
// its settings where a declaration names it and never creates or drops it.
const DefaultPool = "default"

// The attributes a resource pool declaration takes: the pool's name and the
// WITH options of CREATE RESOURCE POOL, spelled as YDB spells them in lower
// case.
const (
	AttributeName                           = "name"
	AttributeConcurrentQueryLimit           = "concurrent_query_limit"
	AttributeQueueSize                      = "queue_size"
	AttributeDatabaseLoadCPUThreshold       = "database_load_cpu_threshold"
	AttributeQueryMemoryLimitPercentPerNode = "query_memory_limit_percent_per_node"
	AttributeQueryCPULimitPercentPerNode    = "query_cpu_limit_percent_per_node"
	AttributeTotalCPULimitPercentPerNode    = "total_cpu_limit_percent_per_node"
	AttributeResourceWeight                 = "resource_weight"
)

// The attributes a classifier declaration takes besides its name.
const (
	AttributeResourcePool = "resource_pool"
	AttributeMemberName   = "member_name"
	AttributeRank         = "rank"
)

// PoolAttributes returns the attributes a resource pool declaration takes, in
// the order a statement names them.
func PoolAttributes() []string {
	return []string{
		AttributeName,
		AttributeConcurrentQueryLimit, AttributeQueueSize, AttributeDatabaseLoadCPUThreshold,
		AttributeQueryMemoryLimitPercentPerNode, AttributeQueryCPULimitPercentPerNode,
		AttributeTotalCPULimitPercentPerNode, AttributeResourceWeight,
	}
}

// ClassifierAttributes returns the attributes a classifier declaration takes.
func ClassifierAttributes() []string {
	return []string{AttributeName, AttributeResourcePool, AttributeMemberName, AttributeRank}
}

// DeclarationError is an attribute whose value a declaration cannot carry.
type DeclarationError struct {
	// Attribute is the attribute's name.
	Attribute string
	// Value is the value it was given.
	Value string
	// Reason says what the attribute takes.
	Reason string
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Attribute, e.Value, e.Reason)
}

// ParsePool reads one resource pool out of values, keyed by attribute name,
// and ignores every key it does not name.
//
// A whole-number setting takes a whole number from 0, and a percentage a
// number from 0 to 100 with a fraction allowed, the ranges YDB keeps
// (`Invalid percent value 150, it is should be between 0 and 100 or -1`). YDB
// spells "no limit" -1, which YQL can write only as a string; a declaration
// leaves the attribute out instead, so a negative number is refused. A queue
// needs a limit to wait for: YDB refuses `queue_size` without
// `concurrent_query_limit` or `database_load_cpu_threshold` (`queue_size
// unsupported without concurrent_query_limit or database_load_cpu_threshold`).
// The pool `default` takes neither, so it takes no queue either; see
// [CheckPool].
func ParsePool(values map[string]string) (string, ast.ResourcePoolSpec, error) {
	name := strings.TrimSpace(values[AttributeName])
	if err := checkName("a resource pool", name); err != nil {
		return "", ast.ResourcePoolSpec{}, err
	}
	var spec ast.ResourcePoolSpec
	var err error
	if spec.ConcurrentQueryLimit, err = count(values, AttributeConcurrentQueryLimit); err != nil {
		return "", ast.ResourcePoolSpec{}, err
	}
	if spec.QueueSize, err = count(values, AttributeQueueSize); err != nil {
		return "", ast.ResourcePoolSpec{}, err
	}
	percentages := []struct {
		attribute string
		target    **float64
	}{
		{AttributeDatabaseLoadCPUThreshold, &spec.DatabaseLoadCPUThreshold},
		{AttributeQueryMemoryLimitPercentPerNode, &spec.QueryMemoryLimitPercentPerNode},
		{AttributeQueryCPULimitPercentPerNode, &spec.QueryCPULimitPercentPerNode},
		{AttributeTotalCPULimitPercentPerNode, &spec.TotalCPULimitPercentPerNode},
		{AttributeResourceWeight, &spec.ResourceWeight},
	}
	for _, percentage := range percentages {
		if *percentage.target, err = percent(values, percentage.attribute); err != nil {
			return "", ast.ResourcePoolSpec{}, err
		}
	}
	if attribute, reason := defaultPoolRefusal(name, spec); reason != "" {
		return "", ast.ResourcePoolSpec{}, &DeclarationError{
			Attribute: attribute, Value: values[attribute], Reason: reason,
		}
	}
	if reason := poolShapeRefusal(spec); reason != "" {
		return "", ast.ResourcePoolSpec{}, &DeclarationError{
			Attribute: AttributeQueueSize, Value: values[AttributeQueueSize], Reason: reason,
		}
	}
	return name, spec, nil
}

// ParseClassifier reads one classifier out of values, keyed by attribute name,
// and ignores every key it does not name.
//
// The pool is required, as YDB requires it (`Missing required property
// resource_pool`). The rank is required too, although YDB takes a classifier
// without one: it then gives it the highest rank in the database plus 1000
// (measured: 1010 beside a classifier of rank 10, 2010 beside both), so the
// rank of an undeclared one depends on what the database held when it was
// created, and a declaration could not be compared with it. A rank is a whole
// number from 0; a negative one is a parse error in YQL.
func ParseClassifier(values map[string]string) (string, ast.ResourcePoolClassifierSpec, error) {
	name := strings.TrimSpace(values[AttributeName])
	if err := checkName("a resource pool classifier", name); err != nil {
		return "", ast.ResourcePoolClassifierSpec{}, err
	}
	spec := ast.ResourcePoolClassifierSpec{
		ResourcePool: strings.TrimSpace(values[AttributeResourcePool]),
		MemberName:   strings.TrimSpace(values[AttributeMemberName]),
	}
	if spec.ResourcePool == "" {
		return "", ast.ResourcePoolClassifierSpec{}, &DeclarationError{Attribute: AttributeResourcePool,
			Reason: "a classifier names the pool it sends queries to (`Missing required property resource_pool`)"}
	}
	raw, ok := values[AttributeRank]
	if !ok || strings.TrimSpace(raw) == "" {
		return "", ast.ResourcePoolClassifierSpec{}, &DeclarationError{Attribute: AttributeRank,
			Reason: "a classifier needs a rank; YDB ranks one declared without it after every classifier the " +
				"database holds, so the rank would depend on the database rather than on the declaration"}
	}
	rank, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || rank < 0 {
		return "", ast.ResourcePoolClassifierSpec{}, &DeclarationError{Attribute: AttributeRank, Value: raw,
			Reason: "takes a whole number from 0"}
	}
	spec.Rank = rank
	return name, spec, nil
}

// count reads a whole-number setting, nil when values does not name it.
func count(values map[string]string, attribute string) (*int32, error) {
	raw, ok := values[attribute]
	if !ok {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || parsed < 0 {
		return nil, &DeclarationError{Attribute: attribute, Value: raw,
			Reason: "takes a whole number from 0; leave it out for no limit"}
	}
	return new(int32(parsed)), nil
}

// percent reads a percentage setting, nil when values does not name it.
func percent(values map[string]string, attribute string) (*float64, error) {
	raw, ok := values[attribute]
	if !ok {
		return nil, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(parsed) || parsed < 0 || parsed > 100 {
		return nil, &DeclarationError{Attribute: attribute, Value: raw,
			Reason: "takes a percentage from 0 to 100; leave it out for no limit"}
	}
	return new(parsed), nil
}

// checkName refuses a name YDB does not take for what, measured on both
// lines: an empty one, one holding a slash (`Resource pool id should not
// contain '/' symbol`, `Symbol '/' is not allowed in the resource pool
// classifier name`), and one holding a space or a character outside ASCII
// (`symbol ' ' is not allowed in the path part`, the same for `пул`). A quote
// and a backtick are taken, and the statements escape them.
func checkName(what, name string) error {
	if name == "" {
		return &DeclarationError{Attribute: AttributeName, Reason: what + " needs a name"}
	}
	for _, r := range name {
		if r == '/' || r <= ' ' || r > '~' {
			return &DeclarationError{Attribute: AttributeName, Value: name,
				Reason: "a name holds printable ASCII characters other than a slash and a space, as YDB requires"}
		}
	}
	return nil
}

// defaultPoolRefusal names the setting the pool `default` cannot take and
// says why, or returns "" for another pool and for a default that names
// neither. YDB keeps every query of the database that no classifier sends
// elsewhere in `default`, and will not let it limit them: on 25.1.4.7 and
// 26.2.1.14 a SET or a RESET of either answers `Can not change property
// concurrent_query_limit for default pool`, and the same for the threshold.
// A queue needs one of the two, so `default` has no queue either; the shape
// rule refuses that one.
func defaultPoolRefusal(name string, spec ast.ResourcePoolSpec) (attribute, reason string) {
	if name != DefaultPool {
		return "", ""
	}
	for _, setting := range []struct {
		attribute string
		set       bool
	}{
		{AttributeConcurrentQueryLimit, spec.ConcurrentQueryLimit != nil},
		{AttributeDatabaseLoadCPUThreshold, spec.DatabaseLoadCPUThreshold != nil},
	} {
		if setting.set {
			return setting.attribute, fmt.Sprintf("the pool %s takes no %s: YDB keeps it unlimited "+
				"(`Can not change property %s for default pool`)", DefaultPool, setting.attribute, setting.attribute)
		}
	}
	return "", ""
}

// poolShapeRefusal says why YDB refuses a pool on every line, or "".
func poolShapeRefusal(spec ast.ResourcePoolSpec) string {
	if spec.QueueSize != nil && spec.ConcurrentQueryLimit == nil && spec.DatabaseLoadCPUThreshold == nil {
		return "a queue needs concurrent_query_limit or database_load_cpu_threshold beside it " +
			"(`queue_size unsupported without concurrent_query_limit or database_load_cpu_threshold`)"
	}
	return ""
}

// FlagHint is what a refusal through [capability.ResourcePools] on a YDB
// target adds: every YDB preset says false, because the flag is off by
// default, and the cluster's own flags are what turns the key on.
const FlagHint = "YDB keeps resource pools behind its EnableResourcePools feature flag, off by default: " +
	"turn the flag on, and name the cluster's monitoring endpoint in the URL " +
	"(monitoring=http://host:8765) so Ptah reads the flags before it plans"

// Refusal says why a pool or a classifier cannot be written on a target: Key
// is the capability it needs and the target lacks, or empty when YDB refuses
// it whatever the target, and Reason then says why.
type Refusal struct {
	// Subject names what is refused.
	Subject string
	// Key is the capability the declaration needs, empty for a refusal YDB
	// makes on every line.
	Key capability.Capability
	// Reason is why it is refused: for a refusal without a key, the reason
	// YDB gives on every line, and with one, how to turn the key on.
	Reason string
}

// CheckPool reports why the pool name with spec cannot be written on a target
// holding caps, or nil when it can. A spec built by hand is held to the
// parse's rules too, because nothing else checks it.
func CheckPool(name string, spec ast.ResourcePoolSpec, caps capability.Capabilities) *Refusal {
	subject := fmt.Sprintf("resource pool %q", name)
	if !caps.Has(capability.ResourcePools) {
		return &Refusal{Subject: subject, Key: capability.ResourcePools, Reason: FlagHint}
	}
	if err := checkName("a resource pool", name); err != nil {
		return &Refusal{Subject: subject, Reason: err.Error()}
	}
	if reason := poolValueRefusal(spec); reason != "" {
		return &Refusal{Subject: subject, Reason: reason}
	}
	if _, reason := defaultPoolRefusal(name, spec); reason != "" {
		return &Refusal{Subject: subject, Reason: reason}
	}
	if reason := poolShapeRefusal(spec); reason != "" {
		return &Refusal{Subject: subject, Reason: reason}
	}
	return nil
}

// CheckPoolDrop reports why the pool name cannot be dropped: it is the
// database's own pool `default`. See [DefaultPool].
func CheckPoolDrop(name string, caps capability.Capabilities) *Refusal {
	if refusal := CheckPool(name, ast.ResourcePoolSpec{}, caps); refusal != nil {
		return refusal
	}
	if name == DefaultPool {
		return &Refusal{Subject: "DROP RESOURCE POOL " + DefaultPool, Reason: "it is the pool YDB runs every " +
			"query in that no classifier sends elsewhere, and after it is dropped every query of the database " +
			"fails with `Resource pool default not found`"}
	}
	return nil
}

// poolValueRefusal says which setting of spec is out of the range YDB keeps,
// or "".
func poolValueRefusal(spec ast.ResourcePoolSpec) string {
	for _, setting := range settingsOf(spec) {
		switch {
		case setting.integer != nil && *setting.integer < 0:
			return setting.option + " takes a whole number from 0"
		case setting.fraction != nil && (math.IsNaN(*setting.fraction) || *setting.fraction < 0 || *setting.fraction > 100):
			return setting.option + " takes a percentage from 0 to 100"
		}
	}
	return ""
}

// CheckClassifier reports why the classifier name with spec cannot be written
// on a target holding caps, or nil when it can.
func CheckClassifier(name string, spec ast.ResourcePoolClassifierSpec, caps capability.Capabilities) *Refusal {
	subject := fmt.Sprintf("resource pool classifier %q", name)
	if !caps.Has(capability.ResourcePools) {
		return &Refusal{Subject: subject, Key: capability.ResourcePools, Reason: FlagHint}
	}
	if err := checkName("a resource pool classifier", name); err != nil {
		return &Refusal{Subject: subject, Reason: err.Error()}
	}
	switch {
	case spec.ResourcePool == "":
		return &Refusal{Subject: subject, Reason: "it names no pool (`Missing required property resource_pool`)"}
	case spec.Rank < 0:
		return &Refusal{Subject: subject, Reason: "its rank is below 0, which YQL cannot write"}
	}
	return nil
}

// Classifier is a classifier and its name, as [CheckRouting] reads one.
type Classifier struct {
	Name string
	Spec ast.ResourcePoolClassifierSpec
}

// CheckRouting reports why a set of declared classifiers cannot stand beside
// the declared pools, or nil when it can:
//
//   - two classifiers share a rank, which YDB refuses for the second
//     (`Classifier with rank 10 already exists, its name c1`);
//   - a classifier names a pool that is neither declared nor `default`. YDB
//     takes one and runs the member's queries in `default` without a word
//     (measured: the member's query succeeded, and the server logged `Failed
//     to fetch pool: ghost`), so the misspelled pool would route nothing.
func CheckRouting(pools []string, classifiers []Classifier) *Refusal {
	declared := make(map[string]bool, len(pools)+1)
	declared[DefaultPool] = true
	for _, pool := range pools {
		declared[pool] = true
	}
	ranks := make(map[int64]string, len(classifiers))
	for _, classifier := range classifiers {
		subject := fmt.Sprintf("resource pool classifier %q", classifier.Name)
		if other, taken := ranks[classifier.Spec.Rank]; taken && other != classifier.Name {
			return &Refusal{Subject: subject, Reason: fmt.Sprintf("its rank %d is the rank of classifier %q, and YDB "+
				"keeps one classifier per rank", classifier.Spec.Rank, other)}
		}
		ranks[classifier.Spec.Rank] = classifier.Name
		if !declared[classifier.Spec.ResourcePool] {
			return &Refusal{Subject: subject, Reason: fmt.Sprintf("it names resource pool %q, which is not declared; "+
				"YDB runs the queries of a classifier whose pool does not exist in the pool %q without a word",
				classifier.Spec.ResourcePool, DefaultPool)}
		}
	}
	return nil
}
