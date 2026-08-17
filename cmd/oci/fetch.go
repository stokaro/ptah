package oci

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/ocireferrers"
)

const (
	digestFlag = "digest"
	fileFlag   = "file"
	outputFlag = "output"
)

type fetchOptions struct {
	filter    string
	digest    string
	file      string
	output    string
	plainHTTP bool
}

func newFetchCommand() *cobra.Command {
	opts := fetchOptions{}
	cmd := &cobra.Command{
		Use:   "fetch <oci-reference>",
		Short: "Download the payload of metadata attached to an OCI artifact",
		Long: `Download the payload of metadata attached to an OCI artifact.

` + "`ptah oci referrers`" + ` lists what is attached; this returns the bytes. Ptah
publishes its lint, plan and deployment reports as referrers, so this is how
Ptah reads back a report Ptah wrote, without oras or a raw registry call.

Selection never guesses. With one candidate the command fetches it; with
several it refuses and prints them, so a pipeline that would have silently
taken "the latest" fails while its author is watching instead. Narrow with
--` + typeFlag + `, or name one exactly with --` + digestFlag + `:

    ptah oci fetch ghcr.io/acme/db:latest --` + typeFlag + ` deployment
    ptah oci fetch ghcr.io/acme/db:latest --` + digestFlag + ` sha256:...

The same rule applies to the files inside the chosen referrer: one file is
written, several require --` + fileFlag + `.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFetch(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.filter, typeFlag, ocireferrers.FilterAll, "Referrer type: all, lint, plan, or deployment")
	flags.StringVar(&opts.digest, digestFlag, "", "Fetch the referrer with this exact digest")
	flags.StringVar(&opts.file, fileFlag, "", "File to write when the referrer carries several")
	flags.StringVarP(&opts.output, outputFlag, "o", "", "Write to this path instead of standard output")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runFetch(cmd *cobra.Command, reference string, opts fetchOptions) error {
	artifactType, err := ocireferrers.ArtifactType(opts.filter)
	if err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	_, discovered, err := client.DiscoverReferrers(cmd.Context(), reference, artifactType)
	if err != nil {
		return err
	}
	selected, err := selectReferrer(reference, discovered, opts)
	if err != nil {
		return err
	}
	artifact, err := client.FetchReferrer(cmd.Context(), reference, selected)
	if err != nil {
		return err
	}
	payload, err := selectPayload(artifact.FileSystem, selected, opts.file)
	if err != nil {
		return err
	}
	return writePayload(cmd.OutOrStdout(), opts.output, payload)
}

// selectReferrer picks the one referrer to fetch, or explains why it cannot.
//
// stokaro/ptah#1143 names the failure mode this exists to avoid: with several
// deployment or lint referrers on one subject, silently taking "the latest" is
// wrong, because a caller cannot tell a deliberate choice from an accident of
// ordering. So there is no ordering rule here at all — either the selection is
// unambiguous or the command refuses and shows what it was choosing between.
func selectReferrer(reference string, discovered []ociartifact.DiscoveredReferrer, opts fetchOptions) (string, error) {
	if digest := strings.TrimSpace(opts.digest); digest != "" {
		if !slices.ContainsFunc(discovered, func(item ociartifact.DiscoveredReferrer) bool {
			return item.Descriptor.Digest.String() == digest
		}) {
			return "", fmt.Errorf("%s has no %s referrer with digest %s",
				reference, describeFilter(opts.filter), digest)
		}
		return digest, nil
	}
	switch len(discovered) {
	case 0:
		return "", fmt.Errorf("%s has no %s referrer to fetch", reference, describeFilter(opts.filter))
	case 1:
		return discovered[0].Descriptor.Digest.String(), nil
	default:
		var builder strings.Builder
		fmt.Fprintf(&builder, "%s has %d %s referrers and this command does not choose between them; "+
			"narrow with --%s or name one with --%s:",
			reference, len(discovered), describeFilter(opts.filter), typeFlag, digestFlag)
		for _, item := range discovered {
			fmt.Fprintf(&builder, "\n  %s  %s", item.Descriptor.Digest, item.Descriptor.ArtifactType)
		}
		return "", fmt.Errorf("%s", builder.String())
	}
}

func describeFilter(filter string) string {
	if normalized := strings.ToLower(strings.TrimSpace(filter)); normalized != ocireferrers.FilterAll {
		return normalized
	}
	return "attached"
}

// selectPayload picks the one file to write, under the same rule.
func selectPayload(fsys fs.FS, digest, requested string) ([]byte, error) {
	if fsys == nil {
		return nil, fmt.Errorf("referrer %s carries no files", digest)
	}
	names, err := payloadNames(fsys)
	if err != nil {
		return nil, err
	}
	if requested = strings.TrimSpace(requested); requested != "" {
		if !slices.Contains(names, requested) {
			return nil, fmt.Errorf("referrer %s carries no file %q; it carries %s",
				digest, requested, strings.Join(names, ", "))
		}
		return fs.ReadFile(fsys, requested)
	}
	switch len(names) {
	case 0:
		return nil, fmt.Errorf("referrer %s carries no files", digest)
	case 1:
		return fs.ReadFile(fsys, names[0])
	default:
		return nil, fmt.Errorf("referrer %s carries %d files and this command does not choose between them; "+
			"name one with --%s: %s", digest, len(names), fileFlag, strings.Join(names, ", "))
	}
}

func payloadNames(fsys fs.FS) ([]string, error) {
	var names []string
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			names = append(names, name)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read referrer payload: %w", err)
	}
	slices.Sort(names)
	return names, nil
}

func writePayload(stdout io.Writer, output string, payload []byte) error {
	if output = strings.TrimSpace(output); output != "" {
		if err := os.WriteFile(output, payload, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", output, err)
		}
		return nil
	}
	if _, err := stdout.Write(payload); err != nil {
		return err
	}
	if len(payload) > 0 && payload[len(payload)-1] != '\n' {
		_, err := fmt.Fprintln(stdout)
		return err
	}
	return nil
}
