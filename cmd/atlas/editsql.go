package atlas

import (
	"context"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/stokaro/ptah/cmd/internal/editor"
)

// editAtlasSQL round-trips SQL text through the operator's editor ($VISUAL,
// then $EDITOR) via a temporary file and returns the edited text. command
// names the compat verb doing the editing; it appears in the temporary file
// name and in every diagnostic, so a failure says which verb was editing.
//
// The temporary file is removed on every path. A non-zero exit from the editor
// is returned as an error with the file already discarded, so a caller that
// bails on the error writes nothing.
func editAtlasSQL(ctx context.Context, command, sqlText string) (string, error) {
	file, err := os.CreateTemp("", "ptah-"+strings.ReplaceAll(command, " ", "-")+"-*.sql")
	if err != nil {
		return "", fmt.Errorf("create %s edit file: %w", command, err)
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err := file.WriteString(sqlText); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("write %s edit file: %w", command, err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close %s edit file: %w", command, err)
	}
	if err := editor.Open(ctx, "", path); err != nil {
		return "", fmt.Errorf("edit %s SQL: %w", command, err)
	}
	edited, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read edited %s SQL: %w", command, err)
	}
	if !utf8.Valid(edited) {
		return "", fmt.Errorf("edited %s SQL is not valid UTF-8", command)
	}
	return string(edited), nil
}
