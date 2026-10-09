package crdbschema_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine"
)

// ExampleCodecs registers the model with an application-selected provider and
// encodes a declaration under the storage parameter names, without importing
// the bundled engine runtime.
func ExampleCodecs() {
	runtime, err := engine.New(engine.Provider{ID: crdbschema.Owner, Codecs: crdbschema.Codecs()})
	if err != nil {
		fmt.Println(err)
		return
	}
	value := &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "3 days", SelectBatchSize: new(int64(500))}}
	envelopes, err := runtime.Codecs().Encode(context.Background(), schemaext.Desired, []schemaext.Payload{value})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(envelopes[0].Kind)
	fmt.Println(string(envelopes[0].Payload))
	// Output:
	// ptah.run/cockroachdb/row-ttl
	// {"ttl_expire_after":"3 days","ttl_select_batch_size":500}
}

// ExampleDecodeDeclared reads declared parameters strictly: a count must be an
// integer, and a parameter that needs an expiry is refused without one.
func ExampleDecodeDeclared() {
	declared, err := crdbschema.DecodeDeclared(map[string]string{"ttl_expiration_expression": "expires_at", "ttl_pause": "false"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(len(declared.Policy.Parameters()), declared.Policy.Pause)
	_, err = crdbschema.DecodeDeclared(map[string]string{"ttl_job_cron": "@daily"})
	fmt.Println(err != nil)
	// Output:
	// 1 false
	// true
}
