package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/internal/ydbsecret"
	"ptah.run/migration/schemadiff/difftypes"
)

// refuseTopicsAndSecrets refuses a topic or a secret the diff declares that
// this server cannot take, before the plan emits anything.
func (p *Planner) refuseTopicsAndSecrets(diff *difftypes.SchemaDiff) error {
	if err := p.refuseTopics(diff); err != nil {
		return err
	}
	return p.refuseSecrets(diff)
}

// refuseSecrets refuses a secret change the target cannot make, before any
// node is returned: every change on a target without the secrets key, and a
// created or rotated secret whose variable is not one a value may come from.
func (p *Planner) refuseSecrets(diff *difftypes.SchemaDiff) error {
	for _, secret := range diff.SecretsAdded {
		if err := planSecretRefusal(ydbsecret.Check(secret.QualifiedName(), secret.ValueEnv, p.caps)); err != nil {
			return err
		}
	}
	for _, secret := range diff.SecretsRotated {
		if err := planSecretRefusal(ydbsecret.Check(secret.QualifiedName(), secret.ValueEnv, p.caps)); err != nil {
			return err
		}
	}
	for _, secret := range diff.SecretsRemoved {
		if err := planSecretRefusal(ydbsecret.Check(secret.QualifiedName(), "", p.caps)); err != nil {
			return err
		}
	}
	return nil
}

// dropSecrets drops every secret the plan removes. They go early, so a table
// the plan creates under a dropped secret's path finds the path free; YDB
// records no dependency of an external data source on a secret it names
// (measured on 26.2.1.14: DROP SECRET succeeds while a data source's
// PASSWORD_SECRET_PATH names the secret).
func dropSecrets(diff *difftypes.SchemaDiff) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.SecretsRemoved))
	for _, secret := range diff.SecretsRemoved {
		nodes = append(nodes, ast.NewDropSecret(secret.QualifiedName()))
	}
	return nodes
}

// changeSecrets creates every secret the plan adds and rotates every one the
// caller asked to rotate. They come after the tables are dropped, so a secret
// created under a dropped table's path finds the path free, and before
// anything that names a secret: an external data source refuses a secret
// path that does not exist yet (`secret ... not found`).
func changeSecrets(diff *difftypes.SchemaDiff) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.SecretsAdded)+len(diff.SecretsRotated))
	for _, secret := range diff.SecretsAdded {
		nodes = append(nodes, ast.NewCreateSecret(secret.QualifiedName(), secret.ValueEnv))
	}
	for _, secret := range diff.SecretsRotated {
		nodes = append(nodes, ast.NewAlterSecret(secret.QualifiedName(), secret.ValueEnv))
	}
	return nodes
}

// planSecretRefusal turns a secret refusal into the planner's error.
func planSecretRefusal(refusal *ydbsecret.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
