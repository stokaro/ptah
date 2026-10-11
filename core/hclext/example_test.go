package hclext_test

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
)

// noteReader reads `note` blocks, and declares nothing: the example shows the
// claims an owner receives.
type noteReader struct{}

func (noteReader) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	for _, claim := range request.Claims {
		fmt.Printf("%s %s.%s: %q\n", claim.Block, claim.Schema, claim.Name, claim.Attributes["text"].Text)
	}
	return hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}, nil
}

// ExampleNewSet shows an owner registering a block as data: the frontend asks
// the set for the shape of a block it meets, reads the block against it, and
// hands the owner every claim of a document in one call.
func ExampleNewSet() {
	set, err := hclext.NewSet(hclext.Extension{
		Owner: "example.org/notes",
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "note", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "text", Kind: hclext.String}}}},
		Decoder: noteReader{},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	shape, found := set.Lookup(hclext.TopLevel, "note")
	fmt.Println(found)

	_, err = set.Decode(context.Background(), []hclext.Match{{Shape: shape, Claim: hclext.Claim{
		Block: "note", Schema: "app", Name: "welcome",
		Attributes: map[string]hclext.Value{"text": {Kind: hclext.String, Text: "hello"}},
	}}})
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// true
	// note app.welcome: "hello"
}
