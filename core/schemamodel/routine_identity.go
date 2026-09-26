package schemamodel

import (
	"ptah.run/internal/routineargs"
)

// routineIdentity is what makes two routine declarations one routine: its
// kind, its name and its input argument types.
//
// PostgreSQL tells overloads apart by their input argument types, so `f(a
// int)` and `f(a int, b text)` are two functions, and a key of the name alone
// kept the first and dropped the second without a word. A schema file that
// declares both, which is what pg_dump writes for a schema that overloads,
// then described a database holding one, and a plan against a database
// holding both dropped the second (stokaro/ptah#3672). The types are compared
// the way [routineargs.InputTypes] reduces them, as GRANT and COMMENT ON name
// a routine: parameter names, defaults, modes and OUT arguments are not part
// of it, and `int` and `integer` are one type. A repeated declaration of one
// overload still folds into one.
//
// The kind is part of it because a function and a procedure are two objects on
// the engines that keep them apart; on PostgreSQL, where one argument list
// cannot name both, the server refuses the second rather than a key dropping
// it.
type routineIdentity struct {
	procedure  bool
	name       string
	inputTypes string
}

// routineIdentityOf answers the identity of one declared routine.
func routineIdentityOf(function Function) routineIdentity {
	return routineIdentity{
		procedure:  function.IsProcedure(),
		name:       function.Name,
		inputTypes: routineargs.InputTypes(function.Parameters),
	}
}

// kind names the routine's kind the way a message says it.
func (r routineIdentity) kind() string {
	if r.procedure {
		return FunctionKindProcedure
	}
	return FunctionKindFunction
}

// signature spells the identity the way a statement names the routine:
// `app.f(integer, text)`.
func (r routineIdentity) signature() string {
	return r.name + "(" + r.inputTypes + ")"
}
