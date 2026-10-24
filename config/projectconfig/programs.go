package projectconfig

import (
	"context"

	"ptah.run/internal/execpolicy"
)

// A project file can name a program: an atlas.hcl `data "external"` or
// `data "external_schema"`, a ptah.yaml `external_schema` block, a
// `migration.pre_up_hook` or `pre_down_hook`. The context a file is read and
// used under decides whether such a program runs. Under a context that
// neither function below prepared, it does not: reading the file refuses a
// `data "external"` the selected env reaches, and a value nothing selected
// reaches is not evaluated at all.

// AtlasCompatible returns ctx under which a project file is read the way Atlas
// reads it. Every local is evaluated, and a program the file names runs, as
// ptah-compat runs it.
func AtlasCompatible(ctx context.Context) context.Context {
	return execpolicy.With(ctx, execpolicy.Policy{Mode: execpolicy.RunAsAtlas})
}

// AllowConfiguredPrograms returns ctx under which a program the file names
// runs when the selected env needs it, as native ptah runs it with
// --allow-config-exec. A value nothing selected reaches is still not
// evaluated, so a program only another env needs does not run.
func AllowConfiguredPrograms(ctx context.Context) context.Context {
	return execpolicy.With(ctx, execpolicy.Policy{Mode: execpolicy.RunSelected})
}
