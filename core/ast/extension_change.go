package ast

// ExtensionChange describes the logical schema action of one ALTER payload.
// Name identifies the affected object within the real ALTER TABLE parent.
// It is reporting data, not a dependency or execution authorization.
type ExtensionChange struct {
	Action ExtensionChangeAction
	Name   string
}

// ExtensionChangeAction distinguishes logical changes independently of safety.
// An additive object may still have a costly or disruptive physical operation.
type ExtensionChangeAction string

const (
	// ExtensionAdd creates an object owned by the ALTER TABLE parent.
	ExtensionAdd ExtensionChangeAction = "add"
	// ExtensionDrop removes an object owned by the ALTER TABLE parent.
	ExtensionDrop ExtensionChangeAction = "drop"
	// ExtensionModify changes an existing object owned by the parent.
	ExtensionModify ExtensionChangeAction = "modify"
	// ExtensionRename changes the name of an object; Name is its previous name.
	ExtensionRename ExtensionChangeAction = "rename"
)

// ExtensionChangeReporter exposes a single logical action for schema-change
// reports without importing its concrete owner. The result must be immutable
// data. Composite or unclassified operations need not implement this interface;
// consumers retain their conservative parent-level modification report.
// This does not replace schemaext.EffectSource or a plan's full footprint.
type ExtensionChangeReporter interface {
	SchemaChange() ExtensionChange
}
