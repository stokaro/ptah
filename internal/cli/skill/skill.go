// Package skill implements `ptah skill`, which lists the agent skills Ptah
// ships and installs them where a coding agent reads skills. It writes files
// and touches no database; internal/skills does the work.
package skill

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/skills"
)

// NewCommand returns the `ptah skill` namespace.
func NewCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "skill",
		Short: "List and install the agent skills Ptah ships",
		Long: `List and install the agent skills Ptah ships.

A skill tells a coding agent how to change a schema with Ptah: edit the desired
schema, then let Ptah validate it, plan and generate the migration, lint it and
apply it to a disposable database. The skill is a SKILL.md file the agent loads
when a task matches its description.`,
	}
	command.AddCommand(newListCommand(), newInstallCommand())
	cmdutil.ConfigureCommandArgs(command, cmdutil.NoPositionalArgs)
	return command
}

func newListCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List the skills Ptah ships, with what each is for",
		RunE: func(cmd *cobra.Command, _ []string) error {
			shipped, err := skills.List()
			if err != nil {
				return err
			}
			for _, skill := range shipped {
				fmt.Fprintf(cmd.OutOrStdout(), "%s\n  %s\n", skill.Name, skill.Description)
			}
			return nil
		},
	}
	cmdutil.ConfigureCommandArgs(command, cmdutil.NoPositionalArgs)
	return command
}

type installOptions struct {
	agents    []string
	dir       string
	user      bool
	overwrite bool
}

func newInstallCommand() *cobra.Command {
	opts := new(installOptions)
	command := &cobra.Command{
		Use:   "install [skill...]",
		Short: "Install skills where an agent reads them",
		Long: `Install skills where an agent reads them. Without a skill name, every skill
Ptah ships is installed.

--agent names the agent, and is repeatable. Claude Code reads .claude/skills;
Codex, Cursor and GitHub Copilot read .agents/skills. The directory is under the
current directory, which should be the project root, or under your home
directory with --user. --skills-dir names any other skills directory instead.

A file already there with the skill's content is left as it is. A file with
other content, in any of the directories, stops the install before anything is
written anywhere, unless --overwrite is given. Nothing is deleted.`,
		Example: `  ptah skill install --agent claude
  ptah skill install --agent codex --agent cursor --user
  ptah skill install ptah-schema --skills-dir ./tools/skills`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInstall(cmd, args, opts)
		},
	}
	flags := command.Flags()
	flags.StringArrayVar(&opts.agents, "agent", nil, "Agent to install for: claude, codex, cursor or copilot (repeatable)")
	flags.StringVar(&opts.dir, "skills-dir", "", "Skills directory to install into, in place of the agents' own")
	flags.BoolVar(&opts.user, "user", false, "Install under your home directory rather than the current directory")
	flags.BoolVar(&opts.overwrite, "overwrite", false, "Replace a file whose content differs from the skill's")
	for _, name := range []string{"agent", "skills-dir", "user", "overwrite"} {
		if err := cmdflags.DisableEnvBinding(flags, name); err != nil {
			panic(err)
		}
	}
	command.MarkFlagsMutuallyExclusive("agent", "skills-dir")
	command.MarkFlagsMutuallyExclusive("user", "skills-dir")
	cmdutil.ConfigureCommandArgs(command, cobra.ArbitraryArgs)
	return command
}

func runInstall(cmd *cobra.Command, args []string, opts *installOptions) error {
	dirs, err := installDirs(opts)
	if err != nil {
		return err
	}
	names := args
	if len(names) == 0 {
		shipped, err := skills.List()
		if err != nil {
			return err
		}
		for _, skill := range shipped {
			names = append(names, skill.Name)
		}
	}
	var targets []skills.Target
	for _, dir := range dirs {
		for _, name := range names {
			targets = append(targets, skills.Target{Name: name, Dir: dir})
		}
	}
	installed, err := skills.Install(targets, skills.InstallOptions{Overwrite: opts.overwrite})
	for _, file := range installed {
		fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", file.Status, file.Path)
	}
	return err
}

// installDirs resolves the flags to the skills directories to install into,
// each once, in the order the agents were named.
func installDirs(opts *installOptions) ([]string, error) {
	if opts.dir != "" {
		return []string{opts.dir}, nil
	}
	if len(opts.agents) == 0 {
		return nil, errors.New("name the agent with --agent, or a skills directory with --skills-dir")
	}
	base := "."
	if opts.user {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("find your home directory for --user: %w", err)
		}
		base = home
	}
	var dirs []string
	for _, name := range opts.agents {
		agent, err := skills.ParseAgent(name)
		if err != nil {
			return nil, err
		}
		dir := filepath.Join(base, skills.SkillsDir(agent))
		if !slices.Contains(dirs, dir) {
			dirs = append(dirs, dir)
		}
	}
	return dirs, nil
}
