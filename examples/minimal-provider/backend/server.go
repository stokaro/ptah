package backend

import "github.com/stokaro/ptah/provider-sdk/runtime"

// Server joins the description with the services that implement it. Each
// service the description offers needs an implementation, and each
// implementation needs its feature in the description: runtime.Serve refuses
// a server where the two differ.
func Server() runtime.Server {
	tutorial := &Provider{}
	return runtime.Server{
		Description: Description(),
		Sessions:    tutorial,
		Inspector:   tutorial,
		Renderer:    tutorial,
		Executor:    tutorial,
		SQLAnalyzer: tutorial,
		Locks:       tutorial,
	}
}
