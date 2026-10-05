package ydb

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
)

// PathExists checks the scheme tree for a path below the writer's root. It
// lists each parent rather than interpreting a failed DescribePath as absence:
// permission and transport failures must remain errors. Unlike partition
// statistics, the scheme tree stops listing a column table when DROP completes.
func (w *Writer) PathExists(ctx context.Context, name string) (bool, error) {
	relative, err := w.droppableDirectory(name)
	if err != nil {
		return false, err
	}
	if w.scheme == nil {
		return false, fmt.Errorf("no YDB scheme connection")
	}
	parent := w.root
	parts := strings.Split(relative, "/")
	for i, part := range parts {
		entries, err := w.scheme.ListDirectory(ctx, parent)
		if err != nil {
			return false, fmt.Errorf("ydb: list %s: %w", parent, err)
		}
		var found *Ydb_Scheme.Entry
		for _, entry := range entries {
			if entry.GetName() == part {
				found = entry
				break
			}
		}
		if found == nil {
			return false, nil
		}
		if i == len(parts)-1 {
			return true, nil
		}
		if found.GetType() != Ydb_Scheme.Entry_DIRECTORY {
			return false, fmt.Errorf("ydb: parent %s is not a directory", path.Join(parent, part))
		}
		parent = path.Join(parent, part)
	}
	return false, nil
}
