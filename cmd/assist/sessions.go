package assist

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/assistsession"
)

// olderThanFlag names the prune window.
const olderThanFlag = "older-than"

// sessionsOptions is what the sessions commands take.
type sessionsOptions struct {
	workspace string
	format    string
	olderThan time.Duration
}

// registerWorkspaceFlag points the sessions commands at a project.
//
// The same `--workspace` the other surfaces take, because a session belongs to
// a project and a person asking about their sessions means the ones for the
// project they are in.
func registerWorkspaceFlag(cmd *cobra.Command, opts *sessionsOptions) {
	cmd.Flags().StringVar(&opts.workspace, agentflags.WorkspaceFlag, "",
		"Project whose sessions to work with; the working directory when omitted")
	cmd.Flags().StringVar(&opts.format, formatFlag, formatText, "Output format: text or json")
}

// newSessionsCommand returns the sessions namespace.
func newSessionsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sessions",
		Short: "List, read and remove saved Ptah Assist conversations",
		Long: `Ptah Assist saves each conversation under .ptah/sessions in the project it was
about, one JSON object per line.

A session file holds the conversation AND what Ptah read on the model's behalf:
migration text, schema files, database object names. It is written so only you
can read it, and it belongs in .gitignore. Run with --ephemeral to keep no
record at all.

No credential is ever stored. The provider profile's name is, because a session
that could not say which model answered cannot be read later.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newSessionsListCommand(),
		newSessionsShowCommand(),
		newSessionsDeleteCommand(),
		newSessionsPruneCommand(),
	)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// openStore resolves the project and its session directory.
func openStore(opts *sessionsOptions) (*assistsession.Store, error) {
	root := opts.workspace
	if root == "" {
		working, err := workingDirectory()
		if err != nil {
			return nil, err
		}
		root = working
	}
	return assistsession.Open(assistsession.Options{Root: root})
}

// newSessionsListCommand returns "sessions list".
func newSessionsListCommand() *cobra.Command {
	opts := &sessionsOptions{}
	cmd := &cobra.Command{
		Use:           "list",
		Short:         "List the saved conversations for this project",
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSessionsList(cmd, opts)
		},
	}
	registerWorkspaceFlag(cmd, opts)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// runSessionsList prints the summaries.
func runSessionsList(cmd *cobra.Command, opts *sessionsOptions) error {
	if err := validateFormat(cmd, opts.format); err != nil {
		return err
	}
	store, err := openStore(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	summaries, err := store.List()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if opts.format == formatJSON {
		return writeJSON(cmd.OutOrStdout(), map[string]any{
			"directory": store.Dir(),
			"sessions":  summaries,
		})
	}

	out := cmd.OutOrStdout()
	if len(summaries) == 0 {
		fmt.Fprintf(out, "No saved conversations in %s.\n", store.Dir())
		return nil
	}
	for _, summary := range summaries {
		fmt.Fprintf(out, "%s  %s  %s\n", summary.ID,
			summary.UpdatedAt.Local().Format(time.RFC3339), summary.Model)
		fmt.Fprintf(out, "    %d request(s), %d tool call(s) via %s\n",
			summary.Requests, summary.ToolCalls, summary.Provider)
		if summary.First != "" {
			fmt.Fprintf(out, "    %s\n", summary.First)
		}
	}
	fmt.Fprintf(out, "\nContinue one with: ptah assist --resume <id>\n")
	return nil
}

// newSessionsShowCommand returns "sessions show".
func newSessionsShowCommand() *cobra.Command {
	opts := &sessionsOptions{}
	cmd := &cobra.Command{
		Use:   "show <id>",
		Short: "Print one saved conversation, including what Ptah did",
		Long: `Print one conversation: what was asked, which tools Ptah ran and what they
answered, and what the model replied.

An id or a unique prefix of one. The tool records are the evidence half: an
answer with no tool behind it was never checked against the project, and this is
where that is visible.`,
		Args:          cobra.ExactArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSessionsShow(cmd, opts, args[0])
		},
	}
	registerWorkspaceFlag(cmd, opts)
	cmdutil.ConfigureCommandArgs(cmd, cobra.ExactArgs(1))
	return cmd
}

// runSessionsShow prints one conversation.
func runSessionsShow(cmd *cobra.Command, opts *sessionsOptions, id string) error {
	if err := validateFormat(cmd, opts.format); err != nil {
		return err
	}
	store, err := openStore(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	records, err := store.Read(id)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if opts.format == formatJSON {
		return writeJSON(cmd.OutOrStdout(), records)
	}
	writeSessionRecords(cmd.OutOrStdout(), records)
	return nil
}

// writeSessionRecords renders a conversation for a person.
func writeSessionRecords(out io.Writer, records []assistsession.Record) {
	for _, record := range records {
		stamp := record.At.Local().Format("15:04:05")
		switch record.Kind {
		case assistsession.KindHeader:
			fmt.Fprintf(out, "session %s\n", record.SessionID)
			fmt.Fprintf(out, "  started  %s\n", record.At.Local().Format(time.RFC3339))
			fmt.Fprintf(out, "  model    %s via %s\n", record.Model, record.Provider)
			fmt.Fprintf(out, "  project  %s\n", record.ProjectRoot)
			fmt.Fprintf(out, "  ptah     %s\n\n", record.PtahVersion)
		case assistsession.KindRequest:
			fmt.Fprintf(out, "%s  > %s\n", stamp, record.Text)
		case assistsession.KindTool:
			fmt.Fprintf(out, "%s  %s %s\n", stamp, outcomeWord[record.Failed], record.Tool)
			fmt.Fprintf(out, "              %s\n", firstResultLine(record.Result))
		case assistsession.KindAnswer:
			fmt.Fprintf(out, "%s  %s\n", stamp, strings.TrimSpace(record.Text))
			fmt.Fprintf(out, "              %d turn(s), %s, %s\n\n",
				record.Turns, record.StopReason, verifiedWord[record.Verified])
		}
	}
}

// verifiedWord says whether Ptah checked anything during a turn.
var verifiedWord = map[bool]string{
	true:  "Ptah tools answered",
	false: "no Ptah tool answered, so nothing was checked",
}

// newSessionsDeleteCommand returns "sessions delete".
func newSessionsDeleteCommand() *cobra.Command {
	opts := &sessionsOptions{}
	cmd := &cobra.Command{
		Use:           "delete <id>",
		Short:         "Remove one saved conversation",
		Args:          cobra.ExactArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openStore(opts)
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			path, err := store.Delete(args[0])
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted %s\n", path)
			return nil
		},
	}
	registerWorkspaceFlag(cmd, opts)
	cmdutil.ConfigureCommandArgs(cmd, cobra.ExactArgs(1))
	return cmd
}

// newSessionsPruneCommand returns "sessions prune".
func newSessionsPruneCommand() *cobra.Command {
	opts := &sessionsOptions{}
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Remove conversations older than a given age",
		Long: `Remove saved conversations that have not been touched for a while.

There is no automatic retention. A conversation is removed when somebody asks
for it to be, because a tool that quietly deleted a record of what it changed
would be the wrong kind of tidy.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSessionsPrune(cmd, opts)
		},
	}
	registerWorkspaceFlag(cmd, opts)
	cmd.Flags().DurationVar(&opts.olderThan, olderThanFlag, 30*24*time.Hour,
		"Remove conversations untouched for longer than this")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// runSessionsPrune removes the old ones.
func runSessionsPrune(cmd *cobra.Command, opts *sessionsOptions) error {
	if err := validateFormat(cmd, opts.format); err != nil {
		return err
	}
	if opts.olderThan <= 0 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"--%s must be positive: %s would remove every conversation, which delete does one at a time",
			olderThanFlag, opts.olderThan))
	}
	store, err := openStore(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	removed, err := store.Prune(opts.olderThan)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if opts.format == formatJSON {
		return writeJSON(cmd.OutOrStdout(), map[string]any{"removed": removed})
	}
	out := cmd.OutOrStdout()
	if len(removed) == 0 {
		fmt.Fprintf(out, "Nothing older than %s in %s.\n", opts.olderThan, store.Dir())
		return nil
	}
	for _, id := range removed {
		fmt.Fprintf(out, "Removed %s\n", id)
	}
	return nil
}
