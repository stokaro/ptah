package ast

// CreateSecretNode creates a YDB secret: `CREATE SECRET <path> WITH (value =
// $<variable>)`. The YDB renderer writes it; every other renderer refuses it,
// because no other engine has a secret Ptah models.
//
// The node never holds the secret's value. ValueEnv names the environment
// variable that holds it, the statement refers to that variable as a named
// expression, and the YDB connection defines the expression from the
// environment when the statement runs, so the value is in no rendered text.
type CreateSecretNode struct {
	// Name is the secret's canonical reference: the directory and the name,
	// as [ptah.run/internal/tableref.Canonical] joins a table's.
	Name string
	// ValueEnv names the environment variable that holds the value.
	ValueEnv string
}

// NewCreateSecret creates a CREATE SECRET node for the secret name, whose value
// the environment variable valueEnv holds.
func NewCreateSecret(name, valueEnv string) *CreateSecretNode {
	return &CreateSecretNode{Name: name, ValueEnv: valueEnv}
}

// Accept implements the Node interface for CreateSecretNode.
func (n *CreateSecretNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AlterSecretNode gives a YDB secret a new value, the one the environment
// variable ValueEnv holds when the statement runs: `ALTER SECRET <path> WITH
// (value = $<variable>)`. Ptah plans it only when asked to rotate the secret,
// because a value the server never returns cannot be compared. The YDB
// renderer writes it; every other renderer refuses it.
type AlterSecretNode struct {
	// Name is the secret's canonical reference.
	Name string
	// ValueEnv names the environment variable that holds the new value.
	ValueEnv string
}

// NewAlterSecret creates an ALTER SECRET node that gives the secret name the
// value the environment variable valueEnv holds.
func NewAlterSecret(name, valueEnv string) *AlterSecretNode {
	return &AlterSecretNode{Name: name, ValueEnv: valueEnv}
}

// Accept implements the Node interface for AlterSecretNode.
func (n *AlterSecretNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropSecretNode drops a YDB secret and its value, which nothing can read back:
// `DROP SECRET <path>`. The YDB renderer writes it; every other renderer
// refuses it.
type DropSecretNode struct {
	// Name is the secret's canonical reference.
	Name string
}

// NewDropSecret creates a DROP SECRET node for the secret name.
func NewDropSecret(name string) *DropSecretNode {
	return &DropSecretNode{Name: name}
}

// Accept implements the Node interface for DropSecretNode.
func (n *DropSecretNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
