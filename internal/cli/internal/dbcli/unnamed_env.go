package dbcli

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/config/projectconfig"
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
