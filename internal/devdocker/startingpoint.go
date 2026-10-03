package devdocker

import "sync"

// A dev database an atlas.hcl docker block provisions starts from the state
// the provisioning leaves: the image, built if the block builds one, and then
// the block's baseline SQL. That state is the dev database's starting point.
// The claim records it whole, every schema and object in it, and every reset
// returns the database to it, where a dev database named by URL alone has to
// be empty except for the environment the claim already keeps
// (stokaro/ptah#4056).
//
// An image URL with no block keeps the community binary's rule: the same
// image through `docker+postgres://` is refused when it holds a schema, as the
// binary refuses it.

// startingPoints records the connectable URL of every dev database a docker
// block provisioned and this process has not yet removed. A URL is counted, as
// [runOwned] counts one.
var startingPoints = struct {
	sync.Mutex
	urls map[string]int
}{urls: make(map[string]int)}

func recordStartingPoint(rawURL string) {
	startingPoints.Lock()
	defer startingPoints.Unlock()
	startingPoints.urls[rawURL]++
}

func forgetStartingPoint(rawURL string) {
	startingPoints.Lock()
	defer startingPoints.Unlock()
	startingPoints.urls[rawURL]--
	if startingPoints.urls[rawURL] <= 0 {
		delete(startingPoints.urls, rawURL)
	}
}

// StartingPointDeclared reports whether rawURL connects to a dev database an
// atlas.hcl docker block provisioned, whose state after the provisioning is
// its starting point. It answers for the URL [Provision] returned, until the
// instance is removed.
func StartingPointDeclared(rawURL string) bool {
	startingPoints.Lock()
	defer startingPoints.Unlock()
	return startingPoints.urls[rawURL] > 0
}
