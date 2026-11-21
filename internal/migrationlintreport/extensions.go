package migrationlintreport

import (
	"context"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/vulnerability"
	"ptah.run/migration/lint"
)

// extensionCollector reads, once the replay has run every migration, what
// the dev database offers to install for the extensions the analysis asked
// about, and the server it runs on. The advisory data is matched afterwards,
// in [extensionCollector.assess], so a slow advisory source does not hold the
// dev database.
//
// A nil collector asks for nothing, so a directory that installs no extension
// pays no read and asks the advisory source nothing.
type extensionCollector struct {
	requests []lint.ExtensionRequest
	// read reports that the replay reached the observer.
	read bool
	// identity is the dev database's server, with its version, and catalog
	// what it can install; both are zero on a server whose product has no
	// reviewed advisory matcher.
	identity vulnerability.Inventory
	catalog  vulnerability.ExtensionCatalog
}

// newExtensionCollector returns the collector for requests, or nil when there
// are none.
func newExtensionCollector(requests []lint.ExtensionRequest) *extensionCollector {
	if len(requests) == 0 {
		return nil
	}
	return &extensionCollector{requests: requests}
}

// observer returns the replay hook, or nil for a nil collector.
func (c *extensionCollector) observer(ctx context.Context) func(*dbschema.DatabaseConnection) error {
	if c == nil {
		return nil
	}
	return func(conn *dbschema.DatabaseConnection) error {
		return c.observe(ctx, conn)
	}
}

// observe reads the server and its extension catalog. A product that has no
// reviewed advisory matcher, or a build that does not declare vulnerability
// scanning, is recorded by identity alone: the matcher reports it as
// unassessed.
func (c *extensionCollector) observe(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	c.read = true
	identity, err := vulnerability.Collect(ctx, conn)
	if err != nil {
		return err
	}
	c.identity = identity
	if identity.Product != "postgres" || identity.ScanCapability != "supported" {
		return nil
	}
	c.catalog, err = vulnerability.ReadExtensionCatalog(ctx, conn)
	return err
}

// assess matches what each request installs against the advisory data opts
// selects, and returns nil when the replay never reached the observer.
func (c *extensionCollector) assess(ctx context.Context, opts Options) (*lint.ExtensionAssessments, error) {
	if c == nil || !c.read {
		return nil, nil
	}
	matcher, err := vulnerability.NewPostgresMatcher(ctx, vulnerability.ScanInput{
		Snapshots:     opts.Advisories,
		RequireSource: opts.RequireAdvisorySource,
		Now:           time.Now().UTC(),
		Deployment:    "self-managed",
	})
	if err != nil {
		return nil, err
	}
	assessments := &lint.ExtensionAssessments{Server: vulnerability.ServerName(c.identity)}
	for _, request := range c.requests {
		answer := lint.ExtensionAnswer{Request: request}
		installs, _ := c.catalog.Installs(vulnerability.ExtensionStatement{Name: request.Extension, Version: request.Version, Cascade: request.Cascade})
		for _, install := range installs {
			inventory := c.identity
			inventory.Extensions = map[string]string{install.Name: install.Version}
			found := matcher.Match(inventory).ForExtension(install.Name)
			installed := lint.InstalledExtension{Extension: install.Name, Version: install.Version, Unassessed: found.Unassessed}
			for _, assessment := range found.Affected {
				installed.Advisories = append(installed.Advisories, lint.ExtensionAdvisory{
					ID: assessment.ID, Summary: assessment.Description, FixedIn: assessment.FixedVersion, URL: assessment.SourceURL,
				})
			}
			answer.Installs = append(answer.Installs, installed)
		}
		assessments.Answers = append(assessments.Answers, answer)
	}
	return assessments, nil
}

// replayedObservers runs each observer that is set, in order, as the one hook
// the replay takes, or returns nil when none is.
func replayedObservers(observers ...func(*dbschema.DatabaseConnection) error) func(*dbschema.DatabaseConnection) error {
	var set []func(*dbschema.DatabaseConnection) error
	for _, observe := range observers {
		if observe != nil {
			set = append(set, observe)
		}
	}
	if len(set) == 0 {
		return nil
	}
	return func(conn *dbschema.DatabaseConnection) error {
		for _, observe := range set {
			if err := observe(conn); err != nil {
				return err
			}
		}
		return nil
	}
}
