//go:build integration

package atlas_test

import (
	"bytes"

	"go.5x5.cz/ptah/cmd/atlas"
)

func runCompatInspect(args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(append([]string{"schema", "inspect"}, args...))
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}
