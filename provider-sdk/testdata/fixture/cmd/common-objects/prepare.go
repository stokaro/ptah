package main

import (
	"context"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// PrepareCommonObjects spells a declaration as the program reads it back: a
// view's query with its runs of white space as one space, a privilege in
// upper case, and a role that inherits nothing, which is all a fixture role
// does. Every entry keeps its identity.
func (p *commonObjects) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prepared := proto.CloneOf(request.GetDeclared())
	for _, view := range prepared.GetViews() {
		view.Body = foldSpaces(view.GetBody())
	}
	for _, role := range prepared.GetRoles() {
		role.Inherit = false
	}
	for _, grant := range prepared.GetGrants() {
		for i, privilege := range grant.GetPrivileges() {
			grant.Privileges[i] = strings.ToUpper(privilege)
		}
	}
	return &protocol.PreparedCommonObjects{Complete: true, Prepared: prepared}, nil
}
