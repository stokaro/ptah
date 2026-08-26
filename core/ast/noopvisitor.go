package ast

// NoopVisitor implements [Visitor] with a no-op method for every node kind: each
// one ignores the node it is handed and returns nil.
//
// Embed it in a visitor that only cares about part of the AST -- an analysis
// pass, a linter, a consumer outside this repository -- and that visitor keeps
// compiling when Ptah adds a node kind. The forward compatibility is bought
// with silence: an unhandled node produces no output and no error, so a visitor
// that renders SQL this way emits a statement list missing whatever it did not
// recognize, and nothing says so.
//
// Ptah's own dialect renderers therefore do not embed it. Every dialect answers
// for every node kind, and the compiler is what holds them to it: adding a
// method to [Visitor] breaks each renderer's build until that dialect has said
// what the new kind means -- render it, or refuse it with a diagnostic naming
// the feature. Embedding NoopVisitor there would turn that build failure into
// silently dropped DDL.
//
// The zero value is ready to use. Its methods take a value receiver, so
// embedding it by value or by pointer both satisfy [Visitor].
type NoopVisitor struct{}

var _ Visitor = NoopVisitor{}

func (NoopVisitor) VisitCreateTable(*CreateTableNode) error                             { return nil }
func (NoopVisitor) VisitCreateSchema(*CreateSchemaNode) error                           { return nil }
func (NoopVisitor) VisitCreateDatabase(*CreateDatabaseNode) error                       { return nil }
func (NoopVisitor) VisitAlterTable(*AlterTableNode) error                               { return nil }
func (NoopVisitor) VisitColumn(*ColumnNode) error                                       { return nil }
func (NoopVisitor) VisitConstraint(*ConstraintNode) error                               { return nil }
func (NoopVisitor) VisitIndex(*IndexNode) error                                         { return nil }
func (NoopVisitor) VisitDropIndex(*DropIndexNode) error                                 { return nil }
func (NoopVisitor) VisitEnum(*EnumNode) error                                           { return nil }
func (NoopVisitor) VisitCreateType(*CreateTypeNode) error                               { return nil }
func (NoopVisitor) VisitAlterType(*AlterTypeNode) error                                 { return nil }
func (NoopVisitor) VisitComment(*CommentNode) error                                     { return nil }
func (NoopVisitor) VisitDropTable(*DropTableNode) error                                 { return nil }
func (NoopVisitor) VisitDropType(*DropTypeNode) error                                   { return nil }
func (NoopVisitor) VisitExtension(*ExtensionNode) error                                 { return nil }
func (NoopVisitor) VisitDropExtension(*DropExtensionNode) error                         { return nil }
func (NoopVisitor) VisitCreateFunction(*CreateFunctionNode) error                       { return nil }
func (NoopVisitor) VisitDropFunction(*DropFunctionNode) error                           { return nil }
func (NoopVisitor) VisitCreateSequence(*CreateSequenceNode) error                       { return nil }
func (NoopVisitor) VisitAlterSequence(*AlterSequenceNode) error                         { return nil }
func (NoopVisitor) VisitDropSequence(*DropSequenceNode) error                           { return nil }
func (NoopVisitor) VisitCreateView(*CreateViewNode) error                               { return nil }
func (NoopVisitor) VisitDropView(*DropViewNode) error                                   { return nil }
func (NoopVisitor) VisitCreateSynonym(*CreateSynonymNode) error                         { return nil }
func (NoopVisitor) VisitCreateHypertable(*CreateHypertableNode) error                   { return nil }
func (NoopVisitor) VisitCreateContinuousAggregate(*CreateContinuousAggregateNode) error { return nil }
func (NoopVisitor) VisitDropContinuousAggregate(*DropContinuousAggregateNode) error     { return nil }
func (NoopVisitor) VisitDropSynonym(*DropSynonymNode) error                             { return nil }
func (NoopVisitor) VisitExtendedProperty(*ExtendedPropertyNode) error                   { return nil }
func (NoopVisitor) VisitCreateMaterializedView(*CreateMaterializedViewNode) error       { return nil }
func (NoopVisitor) VisitDropMaterializedView(*DropMaterializedViewNode) error           { return nil }
func (NoopVisitor) VisitAlterMaterializedViewRefresh(*AlterMaterializedViewRefreshNode) error {
	return nil
}
func (NoopVisitor) VisitRefreshMaterializedView(*RefreshMaterializedViewNode) error { return nil }
func (NoopVisitor) VisitCreateTrigger(*CreateTriggerNode) error                     { return nil }
func (NoopVisitor) VisitDropTrigger(*DropTriggerNode) error                         { return nil }
func (NoopVisitor) VisitCreatePolicy(*CreatePolicyNode) error                       { return nil }
func (NoopVisitor) VisitDropPolicy(*DropPolicyNode) error                           { return nil }
func (NoopVisitor) VisitAlterTableEnableRLS(*AlterTableEnableRLSNode) error         { return nil }
func (NoopVisitor) VisitAlterTableDisableRLS(*AlterTableDisableRLSNode) error       { return nil }
func (NoopVisitor) VisitCreateRole(*CreateRoleNode) error                           { return nil }
func (NoopVisitor) VisitDropRole(*DropRoleNode) error                               { return nil }
func (NoopVisitor) VisitAlterRole(*AlterRoleNode) error                             { return nil }
func (NoopVisitor) VisitGrantPrivilege(*GrantPrivilegeNode) error                   { return nil }
func (NoopVisitor) VisitRevokePrivilege(*RevokePrivilegeNode) error                 { return nil }
func (NoopVisitor) VisitRawSQL(*RawSQLNode) error                                   { return nil }
func (NoopVisitor) VisitUpsert(*UpsertNode) error                                   { return nil }
