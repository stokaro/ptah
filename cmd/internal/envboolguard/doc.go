// Package envboolguard hosts the repository guard for the boolean `PTAH_*`
// environment-variable contract.
//
// The contract itself lives in [go.5x5.cz/ptah/internal/envbool]: absence
// selects the documented default, a present value must parse as a boolean, and
// anything else refuses the command before it does work. A contract that only
// exists in the packages that happen to follow it today is not a contract; the
// pattern it replaced
//
//	value, err := strconv.ParseBool(os.Getenv(name))
//	return err == nil && value
//
// was copied from one package to the next five times precisely because nothing
// refused the sixth copy.
//
// Two guards, and they fail for different reasons on purpose:
//
//   - a SOURCE scan refuses a new direct boolean environment parse anywhere
//     outside the shared package. It works on the shape of the code, so it
//     catches the pattern before any variable is named;
//   - an ENUMERATION check derives every `PTAH_*` name that appears in the
//     tree's non-test Go source and requires each one to be either declared
//     through the shared package or listed here as non-boolean. It works on the
//     names, so it catches a boolean variable that was added with a hand-rolled
//     reader the source scan's shape rules happened not to match.
//
// Neither list is hand-maintained on the boolean side. The boolean set is read
// back from [envbool.Registered] at run time, which is why the test imports the
// packages that own the variables: the registration happens in their package
// initialization, and an owner nobody imports contributes nothing. Only the
// NON-boolean classification is written down, because a new name there is a
// deliberate statement that the variable is not a boolean and deserves the
// friction of saying so.
//
// It lives under cmd/internal rather than beside the shared package because
// `cmd/internal/editor` owns one of the variables, and Go's internal-import rule
// lets only packages under `cmd/` import it. A guard that could not see one
// owner's registrations would report that owner's variables as unclassified.
//
// The package carries no runtime code of its own.
package envboolguard
