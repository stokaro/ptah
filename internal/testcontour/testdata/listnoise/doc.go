// Package listnoise holds a TestMain that prints a test-shaped line, which go
// test -list output cannot tell from a listed test. The source declares no
// such test, so a sharded run refuses the listing.
package listnoise
