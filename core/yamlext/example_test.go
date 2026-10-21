package yamlext_test

import (
	"context"
	"fmt"

	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// ExampleExtension_document shows a document callback examining all of its
// present keys together. It inspects syntax only and claims no model coverage.
func ExampleExtension_document() {
	owners, err := yamlext.NewSet(yamlext.Extension{
		Owner:    "example.org/syntax",
		Sections: []yamlext.Section{{Key: "limits"}, {Key: "options"}},
		Document: func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
			fmt.Println("one document, fragments:", len(document.Fragments))
			for _, fragment := range document.Fragments {
				fmt.Printf("%s: %s %s\n", fragment.Path[0], fragment.Node.Tag, fragment.Node.Value)
			}
			return yamlext.DocumentResult{}, nil
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = yamlschema.Parse(context.Background(), owners, []byte("limits: 9007199254740993\noptions: null\n"))
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// one document, fragments: 2
	// limits: !!int 9007199254740993
	// options: !!null null
}

// ExampleSourceRange shows how byte offsets preserve authored quoting and
// Unicode while the decoded value remains available separately.
func ExampleSourceRange() {
	owners, err := yamlext.NewSet(yamlext.Extension{
		Owner: "example.org/syntax", Sections: []yamlext.Section{{Key: "color"}},
		Document: func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
			node := document.Fragments[0].Node
			span := node.Range
			fmt.Println(string(document.Source[span.Start.ByteOffset:span.End.ByteOffset]))
			fmt.Println(node.Value)
			return yamlext.DocumentResult{}, nil
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = yamlschema.Parse(context.Background(), owners, []byte("color: \"blå\"\n"))
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// "blå"
	// blå
}
