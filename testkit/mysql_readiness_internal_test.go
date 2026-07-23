package testkit

// White-box testing required: mysqlReadinessDSN is an unexported readiness
// helper whose output is consumed by testcontainers before Ptah can observe the
// database through exported testkit APIs.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/moby/moby/api/types/network"
)

func Test_mysqlReadinessDSN(t *testing.T) {
	c := qt.New(t)

	got := mysqlReadinessDSN("root", "secret", "ptah_test")("127.0.0.1", network.MustParsePort("3306/tcp"))

	c.Assert(got, qt.Equals, "root:secret@tcp(127.0.0.1:3306)/ptah_test?multiStatements=true&parseTime=true")
}
