package ast

// ExternalColumn is one column of a YDB external table: a name, a YQL type and
// whether it is NOT NULL. It is YDB's, and only a [CreateExternalTableNode]
// holds one; it is part of that node rather than a node of its own.
type ExternalColumn struct {
	// Name is the column's name.
	Name string
	// Type is the column's YQL type, as declared.
	Type string
	// NotNull says the column is NOT NULL.
	NotNull bool
}

// CreateExternalDataSourceNode creates a YDB external data source, or
// replaces one with `CREATE OR REPLACE` when Replace is set: `CREATE EXTERNAL
// DATA SOURCE <path> WITH (SOURCE_TYPE = ..., LOCATION = ..., AUTH_METHOD =
// ..., ...)`. The YDB renderer writes it; every other renderer refuses it,
// because no other engine has an external data source Ptah models.
//
// A credential is never one of Options: an option names a secret that holds
// it, by path or by the name of a deprecated secret object.
type CreateExternalDataSourceNode struct {
	// Name is the data source's canonical reference: the directory and the
	// name, as [ptah.run/internal/tableref.Canonical] joins a table's.
	Name string
	// SourceType is SOURCE_TYPE.
	SourceType string
	// Location is LOCATION, or empty where the source type takes none.
	Location string
	// AuthMethod is AUTH_METHOD.
	AuthMethod string
	// Options are every other option, keyed by upper-case name.
	Options map[string]string
	// Replace writes CREATE OR REPLACE, which replaces a data source that
	// exists at the path.
	Replace bool
}

// Accept implements the Node interface for CreateExternalDataSourceNode.
func (n *CreateExternalDataSourceNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropExternalDataSourceNode drops a YDB external data source: `DROP EXTERNAL
// DATA SOURCE <path>`. YDB refuses it while an external table reads from the
// source. The YDB renderer writes it; every other renderer refuses it.
type DropExternalDataSourceNode struct {
	// Name is the data source's canonical reference.
	Name string
}

// NewDropExternalDataSource creates a DROP EXTERNAL DATA SOURCE node for the
// data source name.
func NewDropExternalDataSource(name string) *DropExternalDataSourceNode {
	return &DropExternalDataSourceNode{Name: name}
}

// Accept implements the Node interface for DropExternalDataSourceNode.
func (n *DropExternalDataSourceNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// CreateExternalTableNode creates a YDB external table, or replaces one with
// `CREATE OR REPLACE` when Replace is set: `CREATE EXTERNAL TABLE <path>
// (<columns>) WITH (DATA_SOURCE = ..., LOCATION = ..., ...)`. The YDB renderer
// writes it; every other renderer refuses it.
type CreateExternalTableNode struct {
	// Name is the external table's canonical reference.
	Name string
	// DataSource is the path of the data source the table reads, as a query
	// names it.
	DataSource string
	// Location is LOCATION, the files' path under the data source.
	Location string
	// Columns are the table's columns, in order.
	Columns []ExternalColumn
	// Options are every other option, such as FORMAT, keyed by upper-case
	// name.
	Options map[string]string
	// Replace writes CREATE OR REPLACE, which replaces an external table that
	// exists at the path.
	Replace bool
}

// Accept implements the Node interface for CreateExternalTableNode.
func (n *CreateExternalTableNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropExternalTableNode drops a YDB external table, which drops no data: the
// files stay where the data source keeps them. `DROP EXTERNAL TABLE <path>`.
// The YDB renderer writes it; every other renderer refuses it.
type DropExternalTableNode struct {
	// Name is the external table's canonical reference.
	Name string
}

// NewDropExternalTable creates a DROP EXTERNAL TABLE node for the external
// table name.
func NewDropExternalTable(name string) *DropExternalTableNode {
	return &DropExternalTableNode{Name: name}
}

// Accept implements the Node interface for DropExternalTableNode.
func (n *DropExternalTableNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
