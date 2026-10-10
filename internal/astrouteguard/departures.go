package astrouteguard

import (
	"slices"
	"strings"
)

// Unmarked is the marker of a node kind that declares none: a statement, or a
// part of one that no fragment interface groups.
const Unmarked Marker = ""

// BaselineKind is one node kind core/ast declared when the extraction of
// dialect features into owner packages began (stokaro/ptah#4140).
type BaselineKind struct {
	// Name is the type name without a package qualifier.
	Name string
	// Marker is the fragment marker the kind declared, or [Unmarked].
	Marker Marker
}

// Departure records where the operations of one baseline node kind went when
// it left core/ast.
type Departure struct {
	// Node is the baseline node kind.
	Node string
	// Marker is the node kind's marker in the baseline. [Account] fills it;
	// the recorded departures leave it empty.
	Marker Marker
	// Successors are the owner payloads that carry the kind's operations now,
	// spelled as [ExtensionKinds] spells them.
	Successors []ExtensionKind
}

// Accounting is the node corpus measured against the extraction baseline.
//
// A floor on the number of node kinds can say that the corpus shrank. It cannot
// say where the kinds went, and lowering it is how a gate would agree that a
// kind is handled by being gone. Every baseline kind is therefore accounted for
// by name: either core/ast still declares it, or a departure names the owner
// payloads that carry its operations. A broken enumerator reports every kind
// unaccounted rather than an empty corpus that passes.
type Accounting struct {
	// Departed are the baseline kinds core/ast no longer declares, each with
	// its recorded successors.
	Departed []Departure
	// Pending are recorded departures of kinds core/ast still declares. The
	// successor exists ahead of the move, so the move needs no second record.
	Pending []Departure
	// Unaccounted are the baseline kinds core/ast no longer declares and no
	// departure records.
	Unaccounted []BaselineKind
	// Invalid are recorded departures the baseline cannot hold: a kind it never
	// declared, a kind recorded twice, or a departure naming no successor.
	Invalid []Departure
}

// Successors returns every payload a departed or pending kind names, once
// each, sorted by package and name.
func (a Accounting) Successors() []ExtensionKind {
	var successors []ExtensionKind
	for _, departure := range slices.Concat(a.Departed, a.Pending) {
		successors = append(successors, departure.Successors...)
	}
	slices.SortFunc(successors, compareExtensionKinds)
	return slices.Compact(successors)
}

// Account measures nodes, the corpus [NodeKinds] reads, against baseline and
// the recorded departures. Every result list is sorted by node name.
func Account(baseline []BaselineKind, departures []Departure, nodes []NodeKind) Accounting {
	declared := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		declared[node.Name] = true
	}
	markers := make(map[string]Marker, len(baseline))
	for _, kind := range baseline {
		markers[kind.Name] = kind.Marker
	}

	var result Accounting
	recorded := make(map[string]bool, len(departures))
	for _, departure := range departures {
		marker, known := markers[departure.Node]
		departure = Departure{Node: departure.Node, Marker: marker, Successors: slices.Clone(departure.Successors)}
		switch {
		case !known || recorded[departure.Node] || len(departure.Successors) == 0:
			result.Invalid = append(result.Invalid, departure)
		case declared[departure.Node]:
			result.Pending = append(result.Pending, departure)
		default:
			result.Departed = append(result.Departed, departure)
		}
		recorded[departure.Node] = true
	}
	for _, kind := range baseline {
		if !declared[kind.Name] && !recorded[kind.Name] {
			result.Unaccounted = append(result.Unaccounted, kind)
		}
	}

	byNode := func(a, b Departure) int { return strings.Compare(a.Node, b.Node) }
	slices.SortStableFunc(result.Departed, byNode)
	slices.SortStableFunc(result.Pending, byNode)
	slices.SortStableFunc(result.Invalid, byNode)
	slices.SortFunc(result.Unaccounted, func(a, b BaselineKind) int { return strings.Compare(a.Name, b.Name) })
	return result
}

// Baseline returns the node kinds core/ast declared when the extraction began,
// sorted by name.
//
// It is a record, not a corpus: the kinds to route are still derived from the
// source, and a kind added since needs no row here. What the record catches is
// a kind that disappears. It is a source constant for the reason a floor is
// one: an input the checked tree can rewrite cannot bound it.
func Baseline() []BaselineKind {
	return slices.Clone(extractionBaseline)
}

// Departures returns the recorded departures, sorted by node name.
//
// A change that moves a node kind out of core/ast adds its row here, naming
// the payloads that now carry its operations. A row may land before the node
// leaves, when the successor already exists; [Account] reports it as pending.
func Departures() []Departure {
	result := make([]Departure, 0, len(recordedDepartures))
	for _, departure := range recordedDepartures {
		result = append(result, Departure{Node: departure.Node, Successors: slices.Clone(departure.Successors)})
	}
	return result
}

func compareExtensionKinds(a, b ExtensionKind) int {
	if order := strings.Compare(a.Package, b.Package); order != 0 {
		return order
	}
	return strings.Compare(a.Name, b.Name)
}

// The owner payload packages the departures name.
const (
	clickHouseAST = "ptah.run/dialect/clickhouse/chast"
	cockroachAST  = "ptah.run/dialect/cockroachdb/crdbast"
	mssqlProperty = "ptah.run/dialect/mssql/mssqlproperty"
	spannerAST    = "ptah.run/dialect/spanner/spannerast"
	synonymOwner  = "ptah.run/feature/synonym"
	timescaleAST  = "ptah.run/dialect/timescaledb/tsast"
	ydbAST        = "ptah.run/dialect/ydb/ydbast"
)

func payload(pkg, name string) ExtensionKind { return ExtensionKind{Package: pkg, Name: name} }

// recordedDepartures is sorted by node name.
var recordedDepartures = []Departure{
	{Node: "AddChangefeedOperation", Successors: []ExtensionKind{payload(ydbAST, "AddChangefeed")}},
	{Node: "AddSkippingIndexOperation", Successors: []ExtensionKind{payload(clickHouseAST, "AddSkippingIndex")}},
	{Node: "AddTopicConsumerNode", Successors: []ExtensionKind{payload(ydbAST, "TopicConsumer")}},
	{Node: "AlterAsyncReplicationNode", Successors: []ExtensionKind{payload(ydbAST, "AsyncReplication")}},
	{Node: "AlterChangefeedTopicOperation", Successors: []ExtensionKind{payload(ydbAST, "AlterChangefeedTopic")}},
	{Node: "AlterCoordinationNodeNode", Successors: []ExtensionKind{payload(ydbAST, "CoordinationNode")}},
	{Node: "AlterMaterializedViewRefreshNode", Successors: []ExtensionKind{payload(clickHouseAST, "ModifyRefresh")}},
	{Node: "AlterResourcePoolClassifierNode", Successors: []ExtensionKind{payload(ydbAST, "ResourcePoolClassifier")}},
	{Node: "AlterResourcePoolNode", Successors: []ExtensionKind{payload(ydbAST, "ResourcePool")}},
	{Node: "AlterSecretNode", Successors: []ExtensionKind{payload(ydbAST, "Secret")}},
	{Node: "AlterTopicNode", Successors: []ExtensionKind{payload(ydbAST, "Topic")}},
	{Node: "AlterTransferNode", Successors: []ExtensionKind{payload(ydbAST, "Transfer")}},
	{Node: "CreateAsyncReplicationNode", Successors: []ExtensionKind{payload(ydbAST, "AsyncReplication")}},
	{Node: "CreateContinuousAggregateNode", Successors: []ExtensionKind{payload(timescaleAST, "ContinuousAggregate")}},
	{Node: "CreateCoordinationNodeNode", Successors: []ExtensionKind{payload(ydbAST, "CoordinationNode")}},
	{Node: "CreateExternalDataSourceNode", Successors: []ExtensionKind{payload(ydbAST, "ExternalDataSource")}},
	{Node: "CreateExternalTableNode", Successors: []ExtensionKind{payload(ydbAST, "ExternalTable")}},
	{Node: "CreateHypertableNode", Successors: []ExtensionKind{payload(timescaleAST, "CreateHypertable")}},
	{Node: "CreateResourcePoolClassifierNode", Successors: []ExtensionKind{payload(ydbAST, "ResourcePoolClassifier")}},
	{Node: "CreateResourcePoolNode", Successors: []ExtensionKind{payload(ydbAST, "ResourcePool")}},
	{Node: "CreateSecretNode", Successors: []ExtensionKind{payload(ydbAST, "Secret")}},
	{Node: "CreateSynonymNode", Successors: []ExtensionKind{payload(synonymOwner, "Operation")}},
	{Node: "CreateTopicNode", Successors: []ExtensionKind{payload(ydbAST, "Topic")}},
	{Node: "CreateTransferNode", Successors: []ExtensionKind{payload(ydbAST, "Transfer")}},
	{Node: "DropAsyncReplicationNode", Successors: []ExtensionKind{payload(ydbAST, "AsyncReplication")}},
	{Node: "DropChangefeedOperation", Successors: []ExtensionKind{payload(ydbAST, "DropChangefeed")}},
	{Node: "DropContinuousAggregateNode", Successors: []ExtensionKind{payload(timescaleAST, "ContinuousAggregate")}},
	{Node: "DropCoordinationNodeNode", Successors: []ExtensionKind{payload(ydbAST, "CoordinationNode")}},
	{Node: "DropExternalDataSourceNode", Successors: []ExtensionKind{payload(ydbAST, "ExternalDataSource")}},
	{Node: "DropExternalTableNode", Successors: []ExtensionKind{payload(ydbAST, "ExternalTable")}},
	{Node: "DropResourcePoolClassifierNode", Successors: []ExtensionKind{payload(ydbAST, "ResourcePoolClassifier")}},
	{Node: "DropResourcePoolNode", Successors: []ExtensionKind{payload(ydbAST, "ResourcePool")}},
	{Node: "DropRowDeletionPolicyOperation", Successors: []ExtensionKind{payload(spannerAST, "AlterRowDeletion"), payload(ydbAST, "AlterTTL")}},
	{Node: "DropSecretNode", Successors: []ExtensionKind{payload(ydbAST, "Secret")}},
	{Node: "DropSynonymNode", Successors: []ExtensionKind{payload(synonymOwner, "Operation")}},
	{Node: "DropTopicNode", Successors: []ExtensionKind{payload(ydbAST, "Topic")}},
	{Node: "DropTransferNode", Successors: []ExtensionKind{payload(ydbAST, "Transfer")}},
	{Node: "ExtendedPropertyNode", Successors: []ExtensionKind{payload(mssqlProperty, "Operation")}},
	{Node: "ModifyTTLOperation", Successors: []ExtensionKind{payload(clickHouseAST, "AlterTTL")}},
	{Node: "ResetRowTTLOperation", Successors: []ExtensionKind{payload(cockroachAST, "AlterRowTTL")}},
	{Node: "SetIndexPartitioningOperation", Successors: []ExtensionKind{payload(ydbAST, "AlterIndexPartitioning")}},
	{Node: "SetRowDeletionPolicyOperation", Successors: []ExtensionKind{payload(spannerAST, "AlterRowDeletion"), payload(ydbAST, "AlterTTL")}},
	{Node: "SetRowTTLOperation", Successors: []ExtensionKind{payload(cockroachAST, "AlterRowTTL")}},
	{Node: "SetYDBColumnFamiliesOperation", Successors: []ExtensionKind{payload(ydbAST, "AlterColumnFamilies")}},
	{Node: "SetYDBTablePartitioningOperation", Successors: []ExtensionKind{payload(ydbAST, "AlterTablePartitioning")}},
}

// extractionBaseline is what [NodeKinds] and [MarkedKinds] read from core/ast
// at the parent of the commit that began the extraction (a221591f9, #4215).
var extractionBaseline = []BaselineKind{
	{Name: "AddChangefeedOperation", Marker: AlterOperationMarker},
	{Name: "AddColumnOperation", Marker: AlterOperationMarker},
	{Name: "AddConstraintOperation", Marker: AlterOperationMarker},
	{Name: "AddEnumValueOperation", Marker: TypeOperationMarker},
	{Name: "AddIndexOperation", Marker: AlterOperationMarker},
	{Name: "AddSkippingIndexOperation", Marker: AlterOperationMarker},
	{Name: "AddTopicConsumerNode", Marker: Unmarked},
	{Name: "AlterAsyncReplicationNode", Marker: Unmarked},
	{Name: "AlterChangefeedTopicOperation", Marker: AlterOperationMarker},
	{Name: "AlterColumnOperation", Marker: AlterOperationMarker},
	{Name: "AlterCoordinationNodeNode", Marker: Unmarked},
	{Name: "AlterGeneratedColumnExpressionOperation", Marker: AlterOperationMarker},
	{Name: "AlterIndexNode", Marker: Unmarked},
	{Name: "AlterIndexVisibilityOperation", Marker: AlterOperationMarker},
	{Name: "AlterMaterializedViewRefreshNode", Marker: Unmarked},
	{Name: "AlterResourcePoolClassifierNode", Marker: Unmarked},
	{Name: "AlterResourcePoolNode", Marker: Unmarked},
	{Name: "AlterRoleNode", Marker: Unmarked},
	{Name: "AlterSecretNode", Marker: Unmarked},
	{Name: "AlterSequenceNode", Marker: Unmarked},
	{Name: "AlterSerialSequenceNode", Marker: Unmarked},
	{Name: "AlterTableDisableRLSNode", Marker: Unmarked},
	{Name: "AlterTableEnableRLSNode", Marker: Unmarked},
	{Name: "AlterTableForceRLSNode", Marker: Unmarked},
	{Name: "AlterTableNode", Marker: Unmarked},
	{Name: "AlterTopicNode", Marker: Unmarked},
	{Name: "AlterTransferNode", Marker: Unmarked},
	{Name: "AlterTypeNode", Marker: Unmarked},
	{Name: "ColumnNode", Marker: Unmarked},
	{Name: "CommentNode", Marker: Unmarked},
	{Name: "CompositeAttributeOperation", Marker: TypeOperationMarker},
	{Name: "CompositeTypeDef", Marker: TypeDefinitionMarker},
	{Name: "ConstraintNode", Marker: Unmarked},
	{Name: "CreateAsyncReplicationNode", Marker: Unmarked},
	{Name: "CreateContinuousAggregateNode", Marker: Unmarked},
	{Name: "CreateCoordinationNodeNode", Marker: Unmarked},
	{Name: "CreateDatabaseNode", Marker: Unmarked},
	{Name: "CreateExternalDataSourceNode", Marker: Unmarked},
	{Name: "CreateExternalTableNode", Marker: Unmarked},
	{Name: "CreateFunctionNode", Marker: Unmarked},
	{Name: "CreateHypertableNode", Marker: Unmarked},
	{Name: "CreateMaterializedViewNode", Marker: Unmarked},
	{Name: "CreatePolicyNode", Marker: Unmarked},
	{Name: "CreateResourcePoolClassifierNode", Marker: Unmarked},
	{Name: "CreateResourcePoolNode", Marker: Unmarked},
	{Name: "CreateRoleNode", Marker: Unmarked},
	{Name: "CreateSchemaNode", Marker: Unmarked},
	{Name: "CreateSecretNode", Marker: Unmarked},
	{Name: "CreateSequenceNode", Marker: Unmarked},
	{Name: "CreateSynonymNode", Marker: Unmarked},
	{Name: "CreateTableNode", Marker: Unmarked},
	{Name: "CreateTopicNode", Marker: Unmarked},
	{Name: "CreateTransferNode", Marker: Unmarked},
	{Name: "CreateTriggerNode", Marker: Unmarked},
	{Name: "CreateTypeNode", Marker: Unmarked},
	{Name: "CreateViewNode", Marker: Unmarked},
	{Name: "DefaultPrivilegeNode", Marker: Unmarked},
	{Name: "DomainConstraintOperation", Marker: TypeOperationMarker},
	{Name: "DomainDefaultOperation", Marker: TypeOperationMarker},
	{Name: "DomainNotNullOperation", Marker: TypeOperationMarker},
	{Name: "DomainTypeDef", Marker: TypeDefinitionMarker},
	{Name: "DropAsyncReplicationNode", Marker: Unmarked},
	{Name: "DropChangefeedOperation", Marker: AlterOperationMarker},
	{Name: "DropColumnOperation", Marker: AlterOperationMarker},
	{Name: "DropConstraintOperation", Marker: AlterOperationMarker},
	{Name: "DropContinuousAggregateNode", Marker: Unmarked},
	{Name: "DropCoordinationNodeNode", Marker: Unmarked},
	{Name: "DropExtensionNode", Marker: Unmarked},
	{Name: "DropExternalDataSourceNode", Marker: Unmarked},
	{Name: "DropExternalTableNode", Marker: Unmarked},
	{Name: "DropFunctionNode", Marker: Unmarked},
	{Name: "DropIndexNode", Marker: Unmarked},
	{Name: "DropMaterializedViewNode", Marker: Unmarked},
	{Name: "DropPolicyNode", Marker: Unmarked},
	{Name: "DropResourcePoolClassifierNode", Marker: Unmarked},
	{Name: "DropResourcePoolNode", Marker: Unmarked},
	{Name: "DropRoleNode", Marker: Unmarked},
	{Name: "DropRowDeletionPolicyOperation", Marker: AlterOperationMarker},
	{Name: "DropSecretNode", Marker: Unmarked},
	{Name: "DropSequenceNode", Marker: Unmarked},
	{Name: "DropSynonymNode", Marker: Unmarked},
	{Name: "DropTableNode", Marker: Unmarked},
	{Name: "DropTopicNode", Marker: Unmarked},
	{Name: "DropTransferNode", Marker: Unmarked},
	{Name: "DropTriggerNode", Marker: Unmarked},
	{Name: "DropTypeNode", Marker: Unmarked},
	{Name: "DropViewNode", Marker: Unmarked},
	{Name: "EnumNode", Marker: Unmarked},
	{Name: "EnumTypeDef", Marker: TypeDefinitionMarker},
	{Name: "ExtendedPropertyNode", Marker: Unmarked},
	{Name: "ExtensionNode", Marker: Unmarked},
	{Name: "GrantPrivilegeNode", Marker: Unmarked},
	{Name: "GrantRoleMembershipNode", Marker: Unmarked},
	{Name: "IndexNode", Marker: Unmarked},
	{Name: "ModifyColumnOperation", Marker: AlterOperationMarker},
	{Name: "ModifyTTLOperation", Marker: AlterOperationMarker},
	{Name: "MySQLRoutineNode", Marker: Unmarked},
	{Name: "ObjectCommentNode", Marker: Unmarked},
	{Name: "OpaqueRoutineNode", Marker: Unmarked},
	{Name: "PostgresDoBlockNode", Marker: Unmarked},
	{Name: "PostgresRoutineNode", Marker: Unmarked},
	{Name: "RangeTypeDef", Marker: TypeDefinitionMarker},
	{Name: "RawSQLNode", Marker: Unmarked},
	{Name: "RefreshMaterializedViewNode", Marker: Unmarked},
	{Name: "RenameColumnOperation", Marker: AlterOperationMarker},
	{Name: "RenameConstraintOperation", Marker: AlterOperationMarker},
	{Name: "RenameEnumValueOperation", Marker: TypeOperationMarker},
	{Name: "RenameIndexOperation", Marker: AlterOperationMarker},
	{Name: "RenameTableOperation", Marker: AlterOperationMarker},
	{Name: "RenameTypeOperation", Marker: TypeOperationMarker},
	{Name: "ReplaceIndexOperation", Marker: AlterOperationMarker},
	{Name: "ResetRowTTLOperation", Marker: AlterOperationMarker},
	{Name: "RevokeDefaultPrivilegeNode", Marker: Unmarked},
	{Name: "RevokePrivilegeNode", Marker: Unmarked},
	{Name: "RevokeRoleMembershipNode", Marker: Unmarked},
	{Name: "SQLServerRoutineNode", Marker: Unmarked},
	{Name: "SetCommentOperation", Marker: AlterOperationMarker},
	{Name: "SetConstraintCommentOperation", Marker: AlterOperationMarker},
	{Name: "SetIndexPartitioningOperation", Marker: AlterOperationMarker},
	{Name: "SetRowDeletionPolicyOperation", Marker: AlterOperationMarker},
	{Name: "SetRowTTLOperation", Marker: AlterOperationMarker},
	{Name: "SetYDBColumnFamiliesOperation", Marker: AlterOperationMarker},
	{Name: "SetYDBTablePartitioningOperation", Marker: AlterOperationMarker},
	{Name: "StatementList", Marker: Unmarked},
	{Name: "UpsertNode", Marker: Unmarked},
	{Name: "ValidateConstraintOperation", Marker: AlterOperationMarker},
}
