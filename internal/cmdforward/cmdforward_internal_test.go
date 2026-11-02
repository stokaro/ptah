package cmdforward

// White-box testing required: whether the registry lets go of a collected
// forwarder is the size of an unexported map. Build answers only for a
// forwarder the caller still holds, so no exported call can observe an entry
// that outlived its forwarder.

import (
	"runtime"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
)

// A test binary builds hundreds of compatibility trees, each registering its
// forwarders. An entry that kept its forwarder reachable would keep the whole
// tree, so the registry must empty once the forwarders are gone.
func TestRegistryLetsGoOfCollectedForwarders(t *testing.T) {
	c := qt.New(t)
	registerUnreachable(200)
	c.Assert(entriesAfterCollection(), qt.Equals, 0)
}

func registerUnreachable(count int) {
	for range count {
		Register(&cobra.Command{Use: "down"}, "migrations down", func() *cobra.Command { return &cobra.Command{Use: "down"} })
	}
}

// entriesAfterCollection collects garbage until the registry is empty or ten
// seconds pass, and returns how many entries it holds. A cleanup runs on its
// own goroutine some time after the collection that frees its object.
func entriesAfterCollection() int {
	deadline := time.Now().Add(10 * time.Second)
	for {
		runtime.GC()
		entries := 0
		factories.Range(func(any, any) bool {
			entries++
			return true
		})
		if entries == 0 || time.Now().After(deadline) {
			return entries
		}
		time.Sleep(10 * time.Millisecond)
	}
}
