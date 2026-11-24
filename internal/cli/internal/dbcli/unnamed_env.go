package dbcli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/envbinding"
)

// MissingProjectValue completes the refusal of a command that lacks a value a
// project env could have supplied. When the run named no env and the project
// files declare some, the refusal says so and names them:
//
//	database URL is required: no --env given; ptah.yaml declares env "prod"
//
// An env applies only to a run whose --env names it, so a lone env is never
// taken for the project default. A refusal that left the env unmentioned would
// read like a project file that is not being read; this one teaches the rule
// at the moment it matters. Otherwise err is returned unchanged, and the
// result wraps err either way.
func MissingProjectValue(config projectconfig.Config, err error) error {
	if config.EnvName != "" || len(config.DeclaredEnvs) == 0 {
		return err
	}
	return fmt.Errorf("%w: no --%s given; %s", err, EnvFlagName, DescribeDeclaredEnvs(config.DeclaredEnvs))
}

// RefuseEmptyProjectEnv refuses an empty or blank PTAH_ENV beside a project
// file that declares envs; config is the project config the run loaded, which
// such a variable leaves with no env selected. The binding reads the value as
// unset, since the env flag reads a blank value as empty, so none of the envs
// applies, and a run meant for one of them reads what the files say outside
// every env. An env that names its revision table is the costly case:
// `migrations up` reads the default table, finds nothing applied and runs
// every migration again. Where no file declares an env, the empty value
// selects what any value would, and nil is returned. A typed --env decides
// the run however the variable is set.
func RefuseEmptyProjectEnv(flags *pflag.FlagSet, config projectconfig.Config) error {
	envName, value, empty := cmdflags.EmptyEnvName(flags, EnvFlagName)
	if !empty || len(config.DeclaredEnvs) == 0 {
		return nil
	}
	return EmptyProjectEnvError(envName, value, config.DeclaredEnvs)
}

// EmptyProjectEnvError is the refusal of envName, the variable that selects a
// project env, set to value, which is empty or only white space, beside files
// that declare the envs declared. It wraps [envbinding.ErrEmptyValue], as the
// refusal of every other empty variable does.
func EmptyProjectEnvError(envName, value string, declared []projectconfig.DeclaredEnv) error {
	return &emptyProjectEnvError{text: fmt.Sprintf(
		"%s is %s, and %s: name the env to read, or unset it and leave --%s out to select no env",
		envName, emptyVariableState(value), DescribeDeclaredEnvs(declared), EnvFlagName)}
}

// UnreadableProjectEnvError is the refusal of envName set to value, which is
// empty or only white space, beside the project file file, whose envs cannot
// be listed because reading it failed with err. Whether the file declares an
// env decides whether the value is refused, so a file that cannot be read is
// refused with it rather than read as declaring none. It wraps err and
// [envbinding.ErrEmptyValue].
func UnreadableProjectEnvError(envName, value, file string, err error) error {
	return &unreadableProjectEnvError{
		text: fmt.Sprintf("%s is %s, and %s cannot be read to list its envs: %v; name the env or unset the variable",
			envName, emptyVariableState(value), file, err),
		err: err,
	}
}

// unreadableProjectEnvError is the refusal [UnreadableProjectEnvError] returns.
type unreadableProjectEnvError struct {
	text string
	err  error
}

func (e *unreadableProjectEnvError) Error() string { return e.text }

func (e *unreadableProjectEnvError) Unwrap() []error { return []error{e.err, envbinding.ErrEmptyValue} }

// emptyVariableState is how a refusal describes value, a variable's value that
// names nothing.
func emptyVariableState(value string) string {
	if value == "" {
		return "set but empty"
	}
	return fmt.Sprintf("set to %q, which names nothing", value)
}

// emptyProjectEnvError is the refusal [EmptyProjectEnvError] returns.
type emptyProjectEnvError struct{ text string }

func (e *emptyProjectEnvError) Error() string { return e.text }

func (e *emptyProjectEnvError) Unwrap() error { return envbinding.ErrEmptyValue }

// DescribeDeclaredEnvs names the envs of each file, in the order the files
// were loaded: `ptah.yaml declares envs "dev" and "prod"; atlas.hcl declares
// env "local"`. It returns "" for an empty list.
func DescribeDeclaredEnvs(declared []projectconfig.DeclaredEnv) string {
	var files []string
	byFile := make(map[string][]projectconfig.DeclaredEnv)
	for _, env := range declared {
		if _, seen := byFile[env.File]; !seen {
			files = append(files, env.File)
		}
		byFile[env.File] = append(byFile[env.File], env)
	}
	clauses := make([]string, 0, len(files))
	for _, file := range files {
		clauses = append(clauses, file+" declares "+declaredEnvList(byFile[file]))
	}
	return strings.Join(clauses, "; ")
}

// declaredEnvList names the envs one file declares. An atlas.hcl env block with
// no label takes its name from an expression, so it is described rather than
// named.
func declaredEnvList(envs []projectconfig.DeclaredEnv) string {
	const unlabeledBlock = "an env block with no label"
	quoted := make([]string, 0, len(envs))
	for _, env := range envs {
		if env.Name != "" {
			quoted = append(quoted, strconv.Quote(env.Name))
		}
	}
	if len(quoted) == 0 {
		return unlabeledBlock
	}
	noun := "env "
	if len(quoted) > 1 {
		noun = "envs "
	}
	list := noun + joinWithAnd(quoted)
	if len(quoted) < len(envs) {
		list += " and " + unlabeledBlock
	}
	return list
}

func joinWithAnd(items []string) string {
	if len(items) == 1 {
		return items[0]
	}
	return strings.Join(items[:len(items)-1], ", ") + " and " + items[len(items)-1]
}
