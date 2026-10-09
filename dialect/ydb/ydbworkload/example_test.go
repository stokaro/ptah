package ydbworkload_test

import (
	"fmt"

	"ptah.run/dialect/ydb/ydbworkload"
)

// ExamplePoolCodecs shows how a versioned model keeps an explicit zero limit
// distinct from an unlimited pool. The codec performs no database access.
func ExamplePoolCodecs() {
	codec := ydbworkload.PoolCodecs()[0]
	value := &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}}
	data, err := codec.Encode(value)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	fmt.Println(value.Equal(&ydbworkload.DesiredPool{}))
	// Output:
	// {"spec":{"concurrent_query_limit":0}}
	// false
}
