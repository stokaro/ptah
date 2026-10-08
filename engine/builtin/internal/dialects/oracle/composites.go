package oracle

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// visitCreateComposite renders Oracle's composite type.
//
// The spelling is the whole difference, and getting it wrong is silent.
// Measured on 23.26.2.0.0 through go-ora:
//
//	CREATE TYPE t AS (a NUMBER, b VARCHAR2(10))         -> err=nil
//	CREATE TYPE t AS OBJECT (a NUMBER, b VARCHAR2(10))  -> err=nil
//
// and the catalog separates them: the first leaves USER_TYPES reporting
// ATTRIBUTES=0 with INCOMPLETE=YES and USER_OBJECTS reporting STATUS=INVALID,
// while the second reports two attributes and a valid object. The driver
// answers nil for both, so a renderer that emitted PostgreSQL's spelling would
// report success and create a type nothing can use -- which is why
// CompositeTypes read false here rather than being written true untested
// (stokaro/ptah#1920).
//
// A trailing semicolon is accepted and so is its absence, measured the same
// way; the statement is written with one, like every other statement here.
func (r *Renderer) visitCreateComposite(node *ast.CreateTypeNode, typeDef *ast.CompositeTypeDef) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	fields := make([]string, len(typeDef.Fields))
	for i, field := range typeDef.Fields {
		fields[i] = fmt.Sprintf("%s %s", escapeIdentifier(field.Name), strings.TrimSpace(field.Type))
	}
	// CREATE OR REPLACE rather than the existence guard, and the guard is
	// available: `CREATE TYPE IF NOT EXISTS` is accepted on 23 and creates a
	// type literally named IF on 21 -- the same trap the routine header has --
	// so ObjectExistenceGuards would keep it off that line correctly. The
	// replacing form is chosen anyway because it serves BOTH halves: it creates
	// a type that is not there and it is what a changed attribute list needs,
	// so one statement covers an addition and a modification.
	//
	// It is not a way around a dependency. Measured on 23.26.2.0.0, replacing a
	// type a table column uses answers ORA-02303 and changes nothing, which is
	// the server refusing to leave that column naming a shape it no longer has.
	// That refusal is kept rather than worked around.
	r.w.WriteLinef("CREATE OR REPLACE TYPE %s AS OBJECT (%s);",
		escapeQualifiedIdentifier(node.Name), strings.Join(fields, ", "))
	return nil
}

// visitDropComposite renders DROP TYPE for a composite.
//
// No FORCE, for the reason the domain drop records: a type a column still uses
// answers ORA-02303, and dropping it out from under that column is not what a
// plan that no longer declares the type asked for. FORCE would leave the column
// naming a type the server has no definition of.
func (r *Renderer) visitDropComposite(node *ast.DropTypeNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	// No guard: `DROP TYPE IF EXISTS` is ORA-00933 on 21 and the 23 preset's
	// object-existence key does not reach this statement, so the drop is
	// written bare on both lines.
	r.w.WriteLinef("DROP TYPE %s;", escapeQualifiedIdentifier(node.Name))
	return nil
}

// compositesRendered reports whether this target's preset says composite types
// are rendered, read back and planned here.
func (r *Renderer) compositesRendered() bool {
	return r.capabilities().Has(capability.CompositeTypes)
}
