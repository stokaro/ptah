package assist_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// refusing is an endpoint that fails the test if anything reaches it.
func refusing(c *qt.C) http.HandlerFunc {
	return func(http.ResponseWriter, *http.Request) {
		c.Errorf("assist context reached the model provider; it must send nothing")
	}
}

func TestContext_SendsNothing(t *testing.T) {
	// The claim the command exists to make. An endpoint that is never called is
	// the only way to check it, because a report saying "nothing was sent" is
	// exactly what a command that sent something would also print.
	c := qt.New(t)
	home := configHome(c, profileFor(endpoint(c, refusing(c))))
	root := workspace(c)

	out, err := execute(c, home, "context", "what is here?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Nothing below has been sent.")
	c.Assert(out, qt.Contains, "what is here?")
	c.Assert(out, qt.Contains, "read_artifact")
}

func TestContext_LeavesNothingInTheProject(t *testing.T) {
	// A command reporting what a question would send, while dropping a file
	// into the project it is describing, would contradict itself in the
	// directory a person was just told to go and look at.
	c := qt.New(t)
	home := configHome(c, profileFor(endpoint(c, refusing(c))))
	root := workspace(c)

	_, err := execute(c, home, "context", "what is here?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")

	c.Assert(err, qt.IsNil)
	_, statErr := os.Stat(filepath.Join(root, ".ptah"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue,
		qt.Commentf("assist context runs no tool and makes no decision, so it opens no audit log"))
}

func TestContext_JSONCarriesTheRequestItself(t *testing.T) {
	// The summary is checkable against the thing it summarizes, rather than
	// something to take on trust.
	c := qt.New(t)
	home := configHome(c, profileFor(endpoint(c, refusing(c))))
	root := workspace(c)

	out, err := execute(c, home, "context", "what is here?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "json")

	c.Assert(err, qt.IsNil)
	report := struct {
		Model   string `json:"model"`
		Request struct {
			System   string `json:"system"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"request"`
		Sizes struct {
			System    int `json:"system"`
			ToolCount int `json:"tool_count"`
			Total     int `json:"total"`
		} `json:"sizes"`
		ProjectContent bool `json:"project_content"`
	}{}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	c.Assert(report.Model, qt.Equals, "a-model")
	c.Assert(report.Request.Messages, qt.HasLen, 1)
	c.Assert(report.Request.Messages[0].Content, qt.Equals, "what is here?")
	c.Assert(report.Request.Tools, qt.HasLen, report.Sizes.ToolCount)
	c.Assert(report.Sizes.System, qt.Equals, len(report.Request.System))
	c.Assert(report.ProjectContent, qt.IsFalse)
}

func TestContext_ResumeShowsTheEarlierConversation(t *testing.T) {
	// A resumed session is the one case where a first request already carries
	// something about the project, and saying otherwise would be the reassuring
	// answer rather than the true one.
	c := qt.New(t)
	root := workspace(c)
	id := recordOne(c, root, "what is the table called?", "It is called users.")

	home := configHome(c, profileFor(endpoint(c, refusing(c))))
	out, err := execute(c, home, "context", "say it again",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--resume", id)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "what is the table called?")
	c.Assert(out, qt.Contains, "It is called users.")
	c.Assert(out, qt.Contains, "carries earlier answers, which may describe this")
}

func TestContext_ResumeDoesNotStartASession(t *testing.T) {
	// Reporting on a conversation must not add one to the list.
	c := qt.New(t)
	root := workspace(c)
	id := recordOne(c, root, "a question", "an answer")
	before := sessionFiles(c, root)

	home := configHome(c, profileFor(endpoint(c, refusing(c))))
	_, err := execute(c, home, "context", "another",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--resume", id)

	c.Assert(err, qt.IsNil)
	c.Assert(sessionFiles(c, root), qt.DeepEquals, before)
}

func TestExplain_SaysWhyARunEndedBadly(t *testing.T) {
	// A run that lost its endpoint or hit a limit exits non-zero. Without this
	// it printed an empty answer and an empty stop reason and said nothing at
	// all, which reads exactly like a model that had nothing to say -- measured
	// against a real endpoint that timed out mid-run.
	c := qt.New(t)
	stub := &scripted{bodies: []string{toolAnswer("read_artifact", `{"artifact":"migrations"}`)}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	_, errOut, err := executeStreams(c, home, "explain", "loop please",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")

	c.Assert(err, qt.ErrorMatches, "the run hit a limit: repeated tool call")
	c.Assert(errOut, qt.Contains, "ptah: the run hit a limit: repeated tool call")
}
