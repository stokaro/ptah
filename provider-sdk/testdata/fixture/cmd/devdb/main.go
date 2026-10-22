// Package main exercises dev database provisioning through an independent process.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-devdb"
	description.Features = append(description.Features, provider.DevDatabaseFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.DevDatabaseFeature)
	engine.ReleaseLines = []*protocol.ReleaseLine{
		{Line: "2.0", Label: "Fixture 2", Support: protocol.ReleaseLine_CERTIFIED, Image: "example.org/fixture:2.0"},
		{Line: "1.0", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "the vendor ended support", Image: "example.org/fixture:1.0"},
	}
	owner := &provisioner{Provider: &backend.Provider{}, leases: make(map[string]bool)}
	err := runtime.Serve(runtime.Server{
		Description:     description,
		Sessions:        owner,
		Inspector:       owner,
		Renderer:        owner,
		Executor:        owner,
		SQLAnalyzer:     owner,
		ValueNormalizer: owner,
		SourceDecoder:   owner,
		ValueConverter:  owner,
		DevProvisioner:  owner,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// provisioner hands out numbered leases and remembers which are live. The
// spec "docker://none" is refused as unsupported and "docker://offline" as
// unavailable.
type provisioner struct {
	*backend.Provider
	mu     sync.Mutex
	next   int
	leases map[string]bool
}

func (p *provisioner) ProvisionDevDatabase(_ context.Context, request *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error) {
	if strings.HasSuffix(request.Spec, "none") {
		return &protocol.DevDatabase{Refusal: &protocol.DevDatabaseRefusal{
			Kind:   protocol.DevDatabaseRefusal_UNSUPPORTED,
			Reason: "the fixture provisions no database for " + request.Spec,
		}}, nil
	}
	if strings.HasSuffix(request.Spec, "offline") {
		return &protocol.DevDatabase{Refusal: &protocol.DevDatabaseRefusal{
			Kind:   protocol.DevDatabaseRefusal_UNAVAILABLE,
			Reason: "the fixture runtime is not running",
		}}, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	lease := fmt.Sprintf("lease-%d", p.next)
	p.leases[lease] = true
	return &protocol.DevDatabase{Lease: lease, Url: "fixture://localhost/" + lease}, nil
}

func (p *provisioner) ReleaseDevDatabase(_ context.Context, request *protocol.ReleaseDevRequest) (*protocol.Empty, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.leases, request.Lease)
	return &protocol.Empty{}, nil
}
