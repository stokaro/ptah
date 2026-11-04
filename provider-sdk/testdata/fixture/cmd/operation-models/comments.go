package main

import (
	"context"
	"fmt"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// PrepareCommonObjects keeps a declaration as it is: the fixture spells
// comments as they are declared.
func (p *owner) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &protocol.PreparedCommonObjects{Complete: true, Prepared: proto.CloneOf(request.GetDeclared())}, nil
}

// renderComment writes the comment a change sets as one statement: `COMMENT
// ON TABLE "t"`, `COMMENT ON COLUMN "t"."c"` or `COMMENT ON INDEX "t"."i"`,
// then `IS 'text'`, or `IS NULL` for none. The fixture renders these for the
// plan to be read and runs none of them.
func renderComment(comment *protocol.ObjectComment) (string, error) {
	object := comment.GetObject()
	var target string
	switch object.GetKind() {
	case "table":
		target = "TABLE " + quote(object.GetName())
	case "column", "index":
		if len(object.GetPath()) != 1 {
			return "", fmt.Errorf("a %s comment names its table", object.GetKind())
		}
		target = strings.ToUpper(object.GetKind()) + " " + quote(object.GetPath()[0]) + "." + quote(object.GetName())
	default:
		return "", fmt.Errorf("the fixture comments tables, columns and indexes only")
	}
	text := "NULL"
	if comment.GetText() != "" {
		text = "'" + strings.ReplaceAll(comment.GetText(), "'", "''") + "'"
	}
	return "COMMENT ON " + target + " IS " + text + ";", nil
}
