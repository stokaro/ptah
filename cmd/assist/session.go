package assist

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/agentflags"
	"ptah.run/internal/aiprovider"
	"ptah.run/internal/assistloop"
	"ptah.run/internal/assistsession"
	"ptah.run/internal/buildinfo"
)

// Session flags shared by the surfaces that hold a conversation.
const (
	ephemeralFlag = "ephemeral"
	resumeFlag    = "resume"
)

// sessionOptions is the session half of a command's flags.
type sessionOptions struct {
	ephemeral bool
	resume    string
}

// registerSessionFlags adds them.
func registerSessionFlags(cmd *cobra.Command, opts *sessionOptions) {
	flags := cmd.Flags()
	flags.BoolVar(&opts.ephemeral, ephemeralFlag, false,
		"Keep no record of this conversation on disk")
	flags.StringVar(&opts.resume, resumeFlag, "",
		"Continue a saved session, by id or a unique prefix of one")
}

// conversation is a session being written and the history it continues.
type conversation struct {
	recorder assistsession.Recorder
	history  []aiprovider.Message
	// notice is what to tell the person, empty when there is nothing to say.
	notice string
	// writeErr is the first failure to record something, kept until the run
	// ends. Records are written as the run happens, and a tool hook has nowhere
	// to return an error to; losing the answer over a failed write would be the
	// wrong trade, so it is reported beside the answer instead.
	writeErr error
}

// openConversation resolves --ephemeral and --resume into a recorder and a
// history.
//
// The project root is the workspace when one was configured and the working
// directory otherwise, because a conversation is about a repository and reading
// it later means reading it beside that repository.
func openConversation(
	agent *agentflags.Options,
	opts *sessionOptions,
	provider aiprovider.Provider,
	mirror assistsession.Recorder,
) (*conversation, error) {
	header := assistsession.Record{
		PtahVersion: buildinfo.Resolve().Version,
		Provider:    provider.Profile(),
		Model:       provider.Model(),
	}

	if opts.ephemeral {
		if opts.resume != "" {
			return nil, fmt.Errorf("--%s and --%s ask for opposite things: one keeps no record, "+
				"the other continues one", ephemeralFlag, resumeFlag)
		}
		if mirror == nil {
			return &conversation{recorder: assistsession.Discard{}}, nil
		}
		if err := assistsession.Begin(mirror, header); err != nil {
			return nil, err
		}
		return &conversation{recorder: mirror}, nil
	}

	root := agent.Workspace
	if root == "" {
		working, err := workingDirectory()
		if err != nil {
			return nil, err
		}
		root = working
	}
	store, err := assistsession.Open(assistsession.Options{Root: root})
	if err != nil {
		return nil, err
	}

	history := make([]aiprovider.Message, 0)
	if opts.resume != "" {
		records, readErr := store.Read(opts.resume)
		if readErr != nil {
			return nil, readErr
		}
		history = assistsession.Messages(records)
	}

	id, err := assistsession.NewID(time.Now(), rand.Reader)
	if err != nil {
		return nil, err
	}
	header.ProjectRoot = root
	mirrors := make([]assistsession.Recorder, 0, 1)
	if mirror != nil {
		mirrors = append(mirrors, mirror)
	}
	writer, err := store.Create(id, header, mirrors...)
	if err != nil {
		return nil, err
	}
	return &conversation{
		recorder: writer,
		history:  history,
		notice:   assistsession.Notice,
	}, nil
}

// begin records the request, before the model is asked.
//
// Records are written as the run happens rather than gathered and written at
// the end. A process killed mid-run used to leave nothing at all behind for the
// turn it was in; now it leaves the question and every tool that had answered,
// which is what an append-only file is for.
func (c *conversation) begin(request string) {
	c.fail(c.recorder.Append(assistsession.Record{
		Kind: assistsession.KindRequest,
		Text: request,
	}))
}

// tool records one call as it completes, before the model is shown the result.
func (c *conversation) tool(record assistloop.ToolRecord) {
	c.fail(c.recorder.Append(assistsession.Record{
		Kind:      assistsession.KindTool,
		Tool:      record.Name,
		Arguments: record.Args,
		Failed:    record.Failed,
		Result:    record.Result,
		Truncated: record.Truncated,
	}))
}

// finish records the answer and carries the exchange into the history.
//
// runErr is recorded rather than only returned: a run that hit a limit or lost
// the endpoint still produces an answer record, and one without the reason
// reads exactly like a model that had nothing to say.
func (c *conversation) finish(request string, result *assistloop.Result, runErr error) {
	record := assistsession.Record{
		Kind:       assistsession.KindAnswer,
		Text:       result.Answer,
		Turns:      result.Turns,
		StopReason: string(result.StopReason),
		Usage:      result.Usage,
		Verified:   result.UsedTools(),
	}
	if runErr != nil {
		record.Error = runErr.Error()
	}
	c.fail(c.recorder.Append(record))
	c.continueWith(request, result.Answer)
}

// fail keeps the first write failure of a run.
func (c *conversation) fail(err error) {
	if err != nil && c.writeErr == nil {
		c.writeErr = err
	}
}

// saved reports the first write failure and clears it for the next request.
func (c *conversation) saved() error {
	err := c.writeErr
	c.writeErr = nil
	return err
}

// continueWith folds one exchange into the history a later turn continues from.
func (c *conversation) continueWith(request, answer string) {
	c.history = append(c.history,
		aiprovider.Message{Role: aiprovider.RoleUser, Content: request},
		aiprovider.Message{Role: aiprovider.RoleAssistant, Content: answer},
	)
}

// announce tells the person where the conversation is being kept, once.
func (c *conversation) announce(out io.Writer) {
	if c.notice == "" {
		return
	}
	fmt.Fprintf(out, "%s\n", c.notice)
	if path := c.recorder.Path(); path != "" {
		fmt.Fprintf(out, "Session %s: %s\n", c.recorder.ID(), path)
	}
	c.notice = ""
}

// workingDirectory is where a conversation without a workspace keeps its
// record.
func workingDirectory() (string, error) {
	working, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve the working directory: %w", err)
	}
	return working, nil
}
