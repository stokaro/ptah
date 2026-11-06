// Package unmodelednote reports the objects a read found of a kind Ptah does not
// model, such as a MySQL event, on every surface that reads a database.
//
// No schema source can declare such an object and no comparison reads one, so a
// report of no differences, or a document of what the database holds, says
// nothing about it. Without a word from the surface, that silence reads as a
// statement that the object is absent or matches. A surface whose document has a
// place for the objects, such as `schema drift`, prints [Entries] there and says
// [Summary]; one whose output has no place for them says [Report] on its
// diagnostics stream.
package unmodelednote

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/rundiag"
)

// Summary says what a comparison left out, as a clause a sentence can carry: "1
// event was not compared", "2 events were not compared", or "events were not
// compared" where a read could not list them. It is empty for no object.
func Summary(objects []catalog.UnmodeledObject) string {
	var kinds []string
	named := make(map[string]int)
	whole := make(map[string]bool)
	for _, object := range objects {
		if named[object.Kind] == 0 && !whole[object.Kind] {
			kinds = append(kinds, object.Kind)
		}
		if object.Name == "" {
			whole[object.Kind] = true
			continue
		}
		named[object.Kind]++
	}
	if len(kinds) == 0 {
		return ""
	}
	parts := make([]string, 0, len(kinds))
	verb := "were"
	for _, kind := range kinds {
		switch {
		case whole[kind]:
			parts = append(parts, kind+"s")
		case named[kind] == 1:
			parts = append(parts, "1 "+kind)
			if len(kinds) == 1 {
				verb = "was"
			}
		default:
			parts = append(parts, fmt.Sprintf("%d %ss", named[kind], kind))
		}
	}
	return strings.Join(parts, " and ") + " " + verb + " not compared"
}

// Entry is one display-only row of a report: what was left out and why.
type Entry struct {
	Subject string
	Reason  string
}

// Entries formats objects for a text or an HTML report, in the order given.
func Entries(objects []catalog.UnmodeledObject) []Entry {
	entries := make([]Entry, 0, len(objects))
	for _, object := range objects {
		entries = append(entries, Entry{Subject: subject(object), Reason: object.Reason})
	}
	return entries
}

// subject names an object in a sentence, qualified where the read covered a
// whole server.
func subject(object catalog.UnmodeledObject) string {
	switch {
	case object.Name == "" && object.Schema == "":
		return object.Kind + "s"
	case object.Name == "":
		return fmt.Sprintf("%ss in %q", object.Kind, object.Schema)
	case object.Schema == "":
		return fmt.Sprintf("%s %q", object.Kind, object.Name)
	default:
		return fmt.Sprintf("%s %q", object.Kind, object.Schema+"."+object.Name)
	}
}

// Report raises each object schema records as [rundiag.UnmodeledObjects] for the
// run ctx carries, on a surface whose output has no place for them. description
// names the database in prose: "the database", or "--from" on a command with two
// sides. A nil diagnostics stream or a nil schema drops the report.
func Report(ctx context.Context, diagnostics io.Writer, description string, schema *catalog.Database) {
	if diagnostics == nil || schema == nil {
		return
	}
	for _, object := range schema.UnmodeledObjects {
		named := subject(object) + " in " + description
		verb := "is"
		if object.Name == "" {
			verb = "are"
		}
		line := fmt.Sprintf("%s %s left out: %s", named, verb, object.Reason)
		_ = rundiag.Warn(ctx, diagnostics, rundiag.UnmodeledObjects, rundiag.Record{
			Key: line, Line: line, Attrs: []slog.Attr{slog.String("object", named), slog.String("cause", object.Reason)},
		})
	}
}
