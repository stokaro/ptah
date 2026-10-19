package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/doblockext"
	"ptah.run/internal/doblockext/builtin"
)

// appendRoleBootstrap reads the roles a DO block declares with the reader of
// the language the dialect's descriptor names, and appends them.
func appendRoleBootstrap(
	database *schemamodel.Database, document *Document, block *ast.PostgresDoBlockNode, dialect string,
) error {
	// The block is read as the source dialect's, or for a read with no
	// dialect as the dialect whose descriptor names a DO-block language.
	registry := factsOfRegistry()
	reader := factsOf(dialect)
	if dialect == "" {
		reader = factsOf(registry.doBlockReader)
	}
	services := doBlockServices(reader.statements.DoBlocks)
	if services.Roles == nil {
		return fmt.Errorf("%w: procedural role declarations require one of the dialects %q", ErrUnmodeledStatement, registry.doBlockReaders)
	}
	if block.Language != "" && !strings.EqualFold(block.Language, reader.statements.DoBlocks) {
		return fmt.Errorf("%w: desired-state DO blocks require the supported %s role-bootstrap form", ErrUnmodeledStatement, services.Label)
	}
	request := doblockext.Request{
		Dialect: reader.name,
		Body:    block.Body.SQL,
		Role:    func(node *ast.CreateRoleNode) schemamodel.Role { return toRole(node, reader.name) },
	}
	for _, role := range database.Roles {
		request.Declared = append(request.Declared, role.Name)
	}
	if document.base != nil {
		for _, role := range document.base.Roles {
			request.Declared = append(request.Declared, role.Name)
		}
	}
	roles, err := services.Roles.Roles(request)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnmodeledStatement, err)
	}
	database.Roles = append(database.Roles, roles...)
	return nil
}

// doBlockServices returns the services of a DO-block language. It is the one
// answer to whether a dialect reads DO blocks: the dialect a read with no
// dialect reads a block as, the dialects a refusal names, and the reader a
// block is read with all come from it.
func doBlockServices(language string) doblockext.Services {
	return builtin.Lookup(language)
}
