package ydb

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/ydbpool"
)

// renderCreateResourcePool writes one CREATE RESOURCE POOL with the settings
// the declaration names. A pool the line cannot hold, or one YDB refuses on
// every line, is refused through [ydbpool.CheckPool].
func (r *Renderer) renderCreateResourcePool(node *ast.CreateResourcePoolNode) error {
	if err := poolRefusal(ydbpool.CheckPool(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbpool.CreatePoolStatement(node.Name, node.Spec))
	return nil
}

// renderAlterResourcePool writes the ALTER RESOURCE POOL that moves a pool
// from what the database holds to its declaration, and nothing when the two
// hold the same settings.
func (r *Renderer) renderAlterResourcePool(node *ast.AlterResourcePoolNode) error {
	if err := poolRefusal(ydbpool.CheckPool(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	if statement := ydbpool.AlterPoolStatement(node.Name, node.Spec, node.Previous); statement != "" {
		r.w.WriteLine(statement)
	}
	return nil
}

// renderDropResourcePool writes one DROP RESOURCE POOL. The pool `default` is
// refused: YDB takes the statement and then runs no query of the database.
func (r *Renderer) renderDropResourcePool(node *ast.DropResourcePoolNode) error {
	if err := poolRefusal(ydbpool.CheckPoolDrop(node.Name, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbpool.DropPoolStatement(node.Name))
	return nil
}

// renderCreateResourcePoolClassifier writes one CREATE RESOURCE POOL
// CLASSIFIER.
func (r *Renderer) renderCreateResourcePoolClassifier(node *ast.CreateResourcePoolClassifierNode) error {
	if err := poolRefusal(ydbpool.CheckClassifier(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbpool.CreateClassifierStatement(node.Name, node.Spec))
	return nil
}

// renderAlterResourcePoolClassifier writes the ALTER RESOURCE POOL CLASSIFIER
// that moves a classifier from what the database holds to its declaration.
func (r *Renderer) renderAlterResourcePoolClassifier(node *ast.AlterResourcePoolClassifierNode) error {
	if err := poolRefusal(ydbpool.CheckClassifier(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	if statement := ydbpool.AlterClassifierStatement(node.Name, node.Spec, node.Previous); statement != "" {
		r.w.WriteLine(statement)
	}
	return nil
}

// renderDropResourcePoolClassifier writes one DROP RESOURCE POOL CLASSIFIER.
func (r *Renderer) renderDropResourcePoolClassifier(node *ast.DropResourcePoolClassifierNode) error {
	if err := poolRefusal(ydbpool.CheckClassifier(node.Name,
		ast.ResourcePoolClassifierSpec{ResourcePool: ydbpool.DefaultPool}, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbpool.DropClassifierStatement(node.Name))
	return nil
}

// poolRefusal turns a pool or classifier refusal into the renderer's error: by
// the capability key it names, or by the reason YDB refuses it on every line.
func poolRefusal(refusal *ydbpool.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return &ptaherr.CapabilityError{
			Dialect: DialectName,
			Feature: string(refusal.Key),
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target; %s",
				refusal.Subject, refusal.Key, DialectName, refusal.Reason),
		}
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
