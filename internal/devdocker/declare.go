package devdocker

import (
	"sync"
	"time"
)

// An atlas.hcl `docker` block declares a dev database the image URL alone
// cannot describe: an image built from a Dockerfile, SQL that runs once the
// server is ready, and container environment (stokaro/ptah#4041). The project
// configuration evaluates a reference to the block to a `docker+<driver>://`
// URL whose fragment names the declaration, and records the declaration here
// under that name. [Parse] reads it back, so every consumer that resolves a dev
// URL provisions the block without knowing it came from one.
//
// The record lives for the process. A fragment no declaration was recorded for
// is part of an ordinary image URL, and is ignored as the pinned community
// binary ignores it.

// Declaration is what a `docker` block adds to the image its URL names.
type Declaration struct {
	// Build builds the image before the container starts. Nil starts the image
	// as it is.
	Build *Build
	// Baseline is SQL that runs on the dev database once the server is ready,
	// before any consumer claims it, so the claim judges the state it leaves.
	Baseline string
	// Env is container environment, in `NAME=value` form. The variables the
	// provisioner sets itself -- the superuser password and the database --
	// are applied after it and win.
	Env []string
	// ReadyTimeout bounds the readiness wait instead of [Options.ReadyTimeout]
	// when it is positive.
	ReadyTimeout time.Duration
}

// Build describes a `docker build` of the image the URL names.
type Build struct {
	// Context is the build context directory.
	Context string
	// Dockerfile is the Dockerfile's path, relative to Context. Empty uses
	// Context/Dockerfile.
	Dockerfile string
	// DockerfileInline is the Dockerfile's text, used instead of Dockerfile
	// when it is not empty.
	DockerfileInline string
	// Target is the build stage to stop at. Empty builds the last stage.
	Target string
	// Args are build arguments.
	Args map[string]string
	// Platform is the platform to build for. Empty is the daemon's own.
	Platform string
}

var declarations = struct {
	sync.Mutex
	byName map[string]Declaration
}{byName: make(map[string]Declaration)}

// Declare records declaration under name, the fragment of the URL a `docker`
// block's reference evaluates to. A later declaration under the same name
// replaces the earlier one; the project configuration derives the name from
// the declaration's content, so two different blocks never share one.
func Declare(name string, declaration Declaration) {
	declarations.Lock()
	defer declarations.Unlock()
	declarations.byName[name] = declaration
}

// declared returns the declaration recorded under name.
func declared(name string) (Declaration, bool) {
	if name == "" {
		return Declaration{}, false
	}
	declarations.Lock()
	defer declarations.Unlock()
	declaration, ok := declarations.byName[name]
	return declaration, ok
}
