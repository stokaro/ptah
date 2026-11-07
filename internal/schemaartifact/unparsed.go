package schemaartifact

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/ociartifact"
)

// unparsedOutputExtension is the only extension an unparsed pull writes to.
const unparsedOutputExtension = ".hcl"

// Unparsed is a schema artifact retrieved from OCI storage whose envelope was
// checked and whose content nobody read.
//
// The envelope is everything [ociartifact.PullFrom] and [checkLayout] refuse:
// the manifest, the artifact type, the format, the layer media types, sizes,
// digests and file names. The content is what [Artifact] adds to it: whether
// schema.hcl parses, and whether the row layer agrees with its `data` blocks.
// An Unparsed value carries no model, because no model was built.
type Unparsed struct {
	FileSystem fs.FS
	Descriptor ocispec.Descriptor
	Reference  ociartifact.Reference
}

// PullUnparsedFrom retrieves a schema artifact from target and checks its
// envelope without reading its content.
func PullUnparsedFrom(ctx context.Context, target oras.ReadOnlyTarget, selector string) (Unparsed, error) {
	pulled, err := ociartifact.PullFrom(ctx, target, selector, pullOptions())
	if err != nil {
		return Unparsed{}, err
	}
	return unparsedFrom(pulled)
}

// PullUnparsedToFile retrieves reference, checks its envelope, and writes it at
// output through [MaterializeUnparsed] without reading its content. It returns
// the paths it created, the schema first.
//
// It exists for a process that holds registry credentials and must not report
// what an artifact says. An artifact it cannot read fails in the next command
// that reads the file, which holds no registry credentials.
//
// output has to end in .hcl. A default pull proves the bytes are HCL, so the
// extension of the file it writes selects nothing an author controls. Here the
// bytes are unproven, and the reader chooses its parser by that extension: under
// another one, a published layer reaches a parser the artifact format does not
// offer.
func PullUnparsedToFile(ctx context.Context, reference, output string, plainHTTP bool) (Unparsed, []string, error) {
	resolved, err := absentOutput(output)
	if err != nil {
		return Unparsed{}, nil, err
	}
	if !strings.EqualFold(filepath.Ext(resolved), unparsedOutputExtension) {
		return Unparsed{}, nil, fmt.Errorf(
			"an unparsed schema artifact is written to a %s file, because the extension selects the parser that reads it next: %s",
			unparsedOutputExtension, resolved,
		)
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: plainHTTP})
	if err != nil {
		return Unparsed{}, nil, err
	}
	pulled, err := client.Pull(ctx, reference, pullOptions())
	if err != nil {
		return Unparsed{}, nil, err
	}
	artifact, err := unparsedFrom(pulled)
	if err != nil {
		return Unparsed{}, nil, err
	}
	written, err := MaterializeUnparsed(artifact, resolved)
	if err != nil {
		return Unparsed{}, nil, err
	}
	return artifact, written, nil
}

// MaterializeUnparsed writes the files of artifact to disk, as [Materialize]
// does: the schema at output and the row layer beside it, each as the bytes the
// digest covered, or neither. It returns the paths it created, output first.
//
// An artifact with no row layer gets an empty one, which is the one thing it
// writes that the artifact does not hold, and it is what keeps a published `data`
// block from opening a file on this machine.
//
// The reader of a schema file treats a `data` block with no layer beside it as a
// working copy and reads the file the block names. A default pull refuses an
// artifact in that shape, so a block published in one never gets there. This
// function cannot see the block, because seeing it is parsing. The empty layer
// states what the artifact does carry, no rows, and the reader then refuses such
// a block in the words a default pull uses. A schema with no `data` block never
// reads the layer.
func MaterializeUnparsed(artifact Unparsed, output string) ([]string, error) {
	schema, err := fs.ReadFile(artifact.FileSystem, FileName)
	if err != nil {
		return nil, fmt.Errorf("read schema artifact: %w", err)
	}
	managed, err := fs.ReadFile(artifact.FileSystem, ManagedDataFileName)
	if errors.Is(err, fs.ErrNotExist) {
		managed, err = encodeManagedData(&schemamodel.Database{})
	}
	if err != nil {
		return nil, fmt.Errorf("read managed data layer: %w", err)
	}
	return writeSchemaFiles(output, schema, managed)
}

func unparsedFrom(pulled ociartifact.Artifact) (Unparsed, error) {
	if _, _, err := checkLayout(pulled); err != nil {
		return Unparsed{}, err
	}
	return Unparsed{
		FileSystem: pulled.FileSystem,
		Descriptor: pulled.Descriptor,
		Reference:  pulled.Reference,
	}, nil
}
