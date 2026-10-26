package main

import (
	"os"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// encodeGroups places all declarations in one ordered group, so host tests
// exercise parts with distinct subjects and table bindings in the same group.
func encodeGroups(result *protocol.EncodedSource) *protocol.EncodedSource {
	if os.Getenv("FIXTURE_SOURCE_GROUPS") != "1" || len(result.GetFragments()) == 0 {
		return result
	}
	group := &protocol.EncodedSourceGroup{}
	for _, fragment := range result.GetFragments() {
		group.ValueIds = append(group.ValueIds, fragment.GetValueIds()...)
		group.Parts = append(group.Parts, &protocol.EncodedSourcePart{Fragment: fragment.GetFragment(), Placement: fragment.GetPlacement()})
	}
	result.Fragments = nil
	result.Groups = []*protocol.EncodedSourceGroup{group}
	return result
}
