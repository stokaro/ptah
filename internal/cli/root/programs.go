package root

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/execpolicy"
)

// AllowConfigExecFlagName is the native opt-in for running a program that a
// project file or a test file names. Its environment twin is
// PTAH_ALLOW_CONFIG_EXEC.
const AllowConfigExecFlagName = "allow-config-exec"

// allowConfigExecOptIn is how a refusal tells the operator to allow it.
const allowConfigExecOptIn = "pass --" + AllowConfigExecFlagName

// installProgramPolicy gives native ptah its program policy: a program named
// in ptah.yaml, in atlas.hcl or in a .test.hcl case does not run unless the
// operator passed --allow-config-exec.
//
// The policy is attached to the command's context once flags are parsed, in
// the root's persistent pre-run, and every place that starts such a program
// asks it there; see [execpolicy]. No native subcommand defines a persistent
// pre-run of its own, which would replace this one, and a test holds that.
//
// --allow-external-schema, which commands that read an external_schema
// program register, is kept as an older spelling of the same permission, so
// a script that passes it, or sets PTAH_ALLOW_EXTERNAL_SCHEMA, keeps working.
// It allows no more than --allow-config-exec does.
func installProgramPolicy(root *cobra.Command) {
	root.PersistentFlags().Bool(
		AllowConfigExecFlagName,
		false,
		"Allow running programs a project or test file names: an atlas.hcl data \"external\" or data \"external_schema\", "+
			"a ptah.yaml external_schema, pre_up_hook or pre_down_hook, and a .test.hcl external step",
	)
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		allowed, err := configExecAllowed(cmd)
		if err != nil {
			return err
		}
		policy := execpolicy.Policy{Mode: execpolicy.Refuse, OptIn: allowConfigExecOptIn}
		if allowed {
			policy.Mode = execpolicy.RunSelected
		}
		cmd.SetContext(execpolicy.With(cmd.Context(), policy))
		return nil
	}
}

// configExecAllowed reads the opt-in and its older spelling from the command
// being run.
func configExecAllowed(cmd *cobra.Command) (bool, error) {
	allowed, err := cmd.Flags().GetBool(AllowConfigExecFlagName)
	if err != nil || allowed {
		return allowed, err
	}
	if cmd.Flags().Lookup(dbcli.AllowExternalSchemaFlagName) == nil {
		return false, nil
	}
	return cmd.Flags().GetBool(dbcli.AllowExternalSchemaFlagName)
}
