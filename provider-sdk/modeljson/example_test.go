package modeljson_test

import (
	"fmt"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
)

// ExampleCanonical normalizes object order without rounding exact numbers or
// reordering arrays. Owners define any additional semantic normalization.
func ExampleCanonical() {
	data, err := modeljson.Canonical([]byte(`{"z":[2,1],"n":9007199254740993}`))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	// Output: {"n":9007199254740993,"z":[2,1]}
}
