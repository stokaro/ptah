package migrationlintreport

import (
	"io/fs"
	"os"
	"strings"

	yaml "go.yaml.in/yaml/v3"

	"ptah.run/config/projectconfig"
	"ptah.run/migration/lint"
)

// PolicyDialect returns the dialect the lint policy of a run names: the
// dialect of the explicit --config file, or of the .ptah-lint.yaml in the
// linted directory, and otherwise the lint dialect of the project file. It
// reads nothing else and validates nothing. A command reads it to resolve a
// runtime that serves the dialect before [Build] loads the policy, since an
// engine an executable provider serves is a dialect only that runtime knows,
// and Build validates the policy against it. A policy that cannot be read
// names no dialect here, and Build reports why.
func PolicyDialect(opts Options, projectCfg projectconfig.Config) string {
	var raw []byte
	var err error
	if opts.ConfigPath != "" {
		raw, err = os.ReadFile(opts.ConfigPath)
	} else {
		sourceFS := opts.FS
		if sourceFS == nil {
			sourceFS = os.DirFS(opts.effectiveDir(projectCfg))
		}
		raw, err = fs.ReadFile(sourceFS, lint.ConfigFileName)
	}
	// The key is lint.Config's dialect, decoded alone, so a policy that is
	// invalid elsewhere still names its dialect and Build gives the refusal.
	var policy struct {
		Dialect string `yaml:"dialect"`
	}
	if err == nil && yaml.Unmarshal(raw, &policy) == nil && strings.TrimSpace(policy.Dialect) != "" {
		return strings.TrimSpace(policy.Dialect)
	}
	return strings.TrimSpace(projectCfg.Lint.Dialect)
}
