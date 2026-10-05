package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/internal/ydbsecret"
)

// renderSecretNode routes each secret statement to its renderer.
func (r *Renderer) renderSecretNode(node ast.Node) error {
	switch typed := node.(type) {
	case *ast.CreateSecretNode:
		return r.renderCreateSecret(typed)
	case *ast.AlterSecretNode:
		return r.renderAlterSecret(typed)
	default:
		return r.renderDropSecret(node.(*ast.DropSecretNode))
	}
}

// renderCreateSecret writes one CREATE SECRET whose value is the named
// expression for the node's environment variable. The text holds the
// variable's name and never its value: the YDB connection defines the
// expression from the environment when the statement runs.
func (r *Renderer) renderCreateSecret(node *ast.CreateSecretNode) error {
	if err := secretRefusal(ydbsecret.Check(node.Name, node.ValueEnv, r.caps), node.ValueEnv == ""); err != nil {
		return err
	}
	r.w.WriteLine(ydbsecret.CreateStatement(node.Name, node.ValueEnv))
	return nil
}

// renderAlterSecret writes one ALTER SECRET that gives the secret the value
// its environment variable holds when the statement runs.
func (r *Renderer) renderAlterSecret(node *ast.AlterSecretNode) error {
	if err := secretRefusal(ydbsecret.Check(node.Name, node.ValueEnv, r.caps), node.ValueEnv == ""); err != nil {
		return err
	}
	r.w.WriteLine(ydbsecret.AlterStatement(node.Name, node.ValueEnv))
	return nil
}

// renderDropSecret writes one DROP SECRET, which drops the secret and its
// value; nothing can read the value back to create the secret again.
func (r *Renderer) renderDropSecret(node *ast.DropSecretNode) error {
	if err := secretRefusal(ydbsecret.Check(node.Name, "", r.caps), false); err != nil {
		return err
	}
	r.w.WriteLine(ydbsecret.DropStatement(node.Name))
	return nil
}

// secretRefusal turns a secret refusal into the renderer's error: by the
// capability key it names, or by the reason the declaration is wrong. A
// statement that sets a value without naming its variable is refused too,
// because it would have nothing to define the value from.
func secretRefusal(refusal *ydbsecret.Refusal, missingValue bool) error {
	switch {
	case refusal != nil && refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	case refusal != nil:
		return refuseFact(refusal.Subject, refusal.Reason)
	case missingValue:
		return refuseFact("a secret", "it names no environment variable to take its value from")
	default:
		return nil
	}
}
