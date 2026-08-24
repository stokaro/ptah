package assist

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistloop"
	"go.5x5.cz/ptah/internal/assistsession"
	"go.5x5.cz/ptah/internal/buildinfo"
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
) (*conversation, error) {
	if opts.ephemeral {
		if opts.resume != "" {
			return nil, fmt.Errorf("--%s and --%s ask for opposite things: one keeps no record, "+
				"the other continues one", ephemeralFlag, resumeFlag)
		}
		return &conversation{recorder: assistsession.Discard{}}, nil
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
	writer, err := store.Create(id, assistsession.Record{
		PtahVersion: buildinfo.Resolve().Version,
		ProjectRoot: root,
		Provider:    provider.Profile(),
		Model:       provider.Model(),
	})
	if err != nil {
		return nil, err
	}
	return &conversation{
		recorder: writer,
		history:  history,
		notice:   assistsession.Notice,
	}, nil
}

// record writes one exchange: what was asked, what Ptah did, and what the model
// answered.
//
// Written after the turn rather than during it, because the tool records are
// what the loop returns and writing them twice -- once as they happen and once
// from the result -- is how the two come to disagree.
func (c *conversation) record(request string, result *assistloop.Result) error {
	if err := c.recorder.Append(assistsession.Record{
		Kind: assistsession.KindRequest,
		Text: request,
	}); err != nil {
		return err
	}
	for _, tool := range result.Tools {
		if err := c.recorder.Append(assistsession.Record{
			Kind:      assistsession.KindTool,
			Tool:      tool.Name,
			Arguments: tool.Args,
			Failed:    tool.Failed,
			Result:    tool.Result,
			Truncated: tool.Truncated,
		}); err != nil {
			return err
		}
	}
	return c.recorder.Append(assistsession.Record{
		Kind:       assistsession.KindAnswer,
		Text:       result.Answer,
		Turns:      result.Turns,
		StopReason: string(result.StopReason),
		Usage:      result.Usage,
		Verified:   result.UsedTools(),
	})
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
