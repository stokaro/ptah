package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/internal/ydbexternal"
)

// renderCreateExternalDataSource writes one CREATE EXTERNAL DATA SOURCE, or
// CREATE OR REPLACE when the node replaces one. A credential is never in the
// text: an option names the secret that holds it.
func (r *Renderer) renderCreateExternalDataSource(node *ast.CreateExternalDataSourceNode) error {
	source := externalDataSource(node)
	if err := externalRefusal(ydbexternal.CheckDataSource(node.Name, source, r.caps)); err != nil {
		return err
	}
	if err := r.refuseReplace(node.Replace, "external data source "+node.Name); err != nil {
		return err
	}
	r.w.WriteLine(ydbexternal.CreateDataSourceStatement(node.Name, source, node.Replace))
	return nil
}

// renderCreateExternalTable writes one CREATE EXTERNAL TABLE, or CREATE OR
// REPLACE when the node replaces one.
func (r *Renderer) renderCreateExternalTable(node *ast.CreateExternalTableNode) error {
	table := externalTable(node)
	if err := externalRefusal(ydbexternal.CheckTable(node.Name, table, r.caps)); err != nil {
		return err
	}
	if err := r.refuseReplace(node.Replace, "external table "+node.Name); err != nil {
		return err
	}
	r.w.WriteLine(ydbexternal.CreateTableStatement(node.Name, table, node.Replace))
	return nil
}

// renderDropExternalDataSource writes one DROP EXTERNAL DATA SOURCE.
func (r *Renderer) renderDropExternalDataSource(node *ast.DropExternalDataSourceNode) error {
	if err := r.refuseExternal("DROP EXTERNAL DATA SOURCE " + node.Name); err != nil {
		return err
	}
	r.w.WriteLine(ydbexternal.DropDataSourceStatement(node.Name))
	return nil
}

// renderDropExternalTable writes one DROP EXTERNAL TABLE, which leaves the
// files it reads where they are.
func (r *Renderer) renderDropExternalTable(node *ast.DropExternalTableNode) error {
	if err := r.refuseExternal("DROP EXTERNAL TABLE " + node.Name); err != nil {
		return err
	}
	r.w.WriteLine(ydbexternal.DropTableStatement(node.Name))
	return nil
}

// refuseExternal refuses subject on a target without external data sources.
func (r *Renderer) refuseExternal(subject string) error {
	return externalRefusal(ydbexternal.CheckDrop(subject, r.caps))
}

// refuseReplace refuses a CREATE OR REPLACE on a target that does not take
// one.
func (r *Renderer) refuseReplace(replace bool, subject string) error {
	if !replace {
		return nil
	}
	return externalRefusal(ydbexternal.CheckReplace(subject, r.caps))
}

// externalDataSource is what node declares, in the form ydbexternal writes.
func externalDataSource(node *ast.CreateExternalDataSourceNode) ydbexternal.DataSource {
	return ydbexternal.DataSource{
		SourceType: node.SourceType,
		Location:   node.Location,
		AuthMethod: node.AuthMethod,
		Options:    node.Options,
	}
}

// externalTable is what node declares, in the form ydbexternal writes.
func externalTable(node *ast.CreateExternalTableNode) ydbexternal.Table {
	table := ydbexternal.Table{DataSource: node.DataSource, Location: node.Location, Options: node.Options}
	for _, column := range node.Columns {
		table.Columns = append(table.Columns, ydbexternal.Column{
			Name: column.Name, Type: column.Type, NotNull: column.NotNull,
		})
	}
	return table
}

// externalRefusal turns a refusal into the renderer's error: by the
// capability key it names, or by the reason the declaration is wrong.
func externalRefusal(refusal *ydbexternal.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
