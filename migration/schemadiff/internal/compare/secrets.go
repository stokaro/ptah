package compare

import (
	"sort"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// Secrets compares declared YDB secrets against the ones the database holds,
// by directory and name, and carries every declared secret in the diff for a
// rotation request to find.
//
// A secret is equal on both sides by its presence alone. The server never
// returns a secret's value and a declaration never holds one, so there is
// nothing else to compare; a changed value is planned only when the caller
// asks for it through [difftypes.SchemaDiff.RotateSecrets].
//
// A secret only the database holds is a removal only where the desired state
// claims to describe secrets, and one only the declaration holds is a
// creation only where the read looked: CREATE SECRET carries no guard on the
// lines Ptah measured, so an undecided creation is withheld and recorded
// rather than planned.
func Secrets(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	declared := make(map[string]schemamodel.Secret, len(desired.Secrets))
	for _, secret := range desired.Secrets {
		declared[secret.QualifiedName()] = secret
		diff.DeclaredSecrets = append(diff.DeclaredSecrets, secret)
	}
	held := make(map[string]catalog.Secret, len(database.Secrets))
	for _, secret := range database.Secrets {
		held[secret.QualifiedName()] = secret
	}

	var added difftypes.SecretChanges
	for name, secret := range declared {
		if _, exists := held[name]; !exists {
			added = append(added, secret)
		}
	}
	for name, secret := range held {
		if _, ok := declared[name]; ok {
			continue
		}
		if !cov.PlansRemoval(coverage.Secret, secret.Schema, secret.Name, name) {
			continue
		}
		diff.SecretsRemoved = append(diff.SecretsRemoved, schemamodel.Secret{
			Name:   secret.Name,
			Schema: secret.Schema,
		})
	}

	kept, withheld := keepPlannedAdditions(cov, coverage.Secret, added,
		func(secret schemamodel.Secret) (string, []string) {
			return secret.Schema, []string{secret.Name, secret.QualifiedName()}
		},
		func(secret schemamodel.Secret) string { return secret.QualifiedName() },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.SecretsAdded = kept

	sortSecrets(diff.SecretsAdded)
	sortSecrets(diff.SecretsRemoved)
	sortSecrets(diff.DeclaredSecrets)
}

// sortSecrets orders secrets by their canonical reference.
func sortSecrets(secrets []schemamodel.Secret) {
	sort.Slice(secrets, func(i, j int) bool {
		return secrets[i].QualifiedName() < secrets[j].QualifiedName()
	})
}
