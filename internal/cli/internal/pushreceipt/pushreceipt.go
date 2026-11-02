// Package pushreceipt writes what `ptah schema push` and `ptah migrations push`
// published, in the format the caller chose with --format.
//
// The text format is the receipt a person reads, and each command words it. The
// machine formats are shared, so the two commands cannot disagree about them:
// `reference` prints the pinned reference alone, the way `ptah oci resolve`
// prints it, and `json` prints one document whose first fields carry the names
// and meanings of `ptah oci resolve --format json`.
//
// The pinned reference is built from the descriptor of the manifest the push
// wrote. It is never read back through a tag: a tag is movable, and another
// writer can move it between the push and the read, so a reference resolved
// from it can name an artifact this command did not publish.
package pushreceipt

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/pflag"

	"ptah.run/internal/ociartifact"
)

// FormatFlag is the flag that selects the output format. It is the flag
// `ptah oci resolve` and the other `ptah oci` verbs use.
const FormatFlag = "format"

// The values --format accepts.
const (
	// FormatText is the receipt a person reads. It is the default.
	FormatText Format = "text"
	// FormatJSON is one JSON document describing the published manifest.
	FormatJSON Format = "json"
	// FormatReference is the pinned reference alone, followed by a newline.
	FormatReference Format = "reference"
)

// Format is a validated --format value.
type Format string

// RegisterFormatFlag registers --format on flags, bound to target, with the
// text format as its default.
func RegisterFormatFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, FormatFlag, string(FormatText), "Output format: text, json, or reference")
}

// ParseFormat validates a --format value for a push to destination, ignoring
// case and surrounding space as `ptah oci resolve` does.
//
// A command calls it before it does any work, so a refused value leaves nothing
// published. A machine format is refused for an oci-layout:// destination: a
// layout reference selects by tag only, so there is no pinned reference that
// names what the push wrote, and printing none would hand a caller an empty
// value where it expects one.
func ParseFormat(value, destination string) (Format, error) {
	format := Format(strings.ToLower(strings.TrimSpace(value)))
	switch format {
	case FormatText:
		return format, nil
	case FormatJSON, FormatReference:
		if ociartifact.IsLayoutRef(destination) {
			return "", fmt.Errorf(
				"--%s %s prints a pinned oci:// reference, and an %s destination has none; push to a registry, or use --%s %s",
				FormatFlag, format, ociartifact.LayoutScheme, FormatFlag, FormatText,
			)
		}
		return format, nil
	default:
		return "", unsupportedFormat(value)
	}
}

func unsupportedFormat(value string) error {
	return fmt.Errorf("unsupported output format %q: expected text, json, or reference", value)
}

// record is the document --format json prints.
//
// The first five fields are the fields of `ptah oci resolve --format json`, with
// the same names and meanings, and artifact_type is named as `ptah oci inspect`
// names it. Every field is always present: version is empty when the push wrote
// no version tag, and tags is never null.
type record struct {
	// Reference is the reference the push wrote to, in canonical form.
	Reference string `json:"reference"`
	// PinnedReference selects the pushed manifest by digest.
	PinnedReference string `json:"pinned_reference"`
	// Digest is the digest of the pushed manifest.
	Digest string `json:"digest"`
	// MediaType is the media type of the pushed manifest.
	MediaType string `json:"media_type"`
	// Size is the size of the pushed manifest in bytes.
	Size int64 `json:"size"`
	// ArtifactType is the artifact type the manifest declares.
	ArtifactType string `json:"artifact_type"`
	// Tags are the tags the push applied, in the order it applied them.
	Tags []string `json:"tags"`
	// Version is the write-once version tag, empty when the push wrote none.
	Version string `json:"version"`
}

// Write writes the receipt of a completed push to w.
//
// text is the receipt the text format prints, worded by the command; the
// machine formats never print it. version is the write-once version tag the
// push applied, empty when it applied none.
//
// A caller writes nothing to w before the push succeeds, so a failed push leaves
// w empty in every format. That is the contract a script relies on: a reference
// on standard output means the manifest it names was published.
func Write(w io.Writer, format Format, result ociartifact.PushResult, version, text string) error {
	switch format {
	case FormatText:
		_, err := io.WriteString(w, text)
		return err
	case FormatReference:
		pinned, err := pinnedReference(result)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(w, pinned)
		return err
	case FormatJSON:
		pinned, err := pinnedReference(result)
		if err != nil {
			return err
		}
		tags := slices.Clone(result.Tags)
		if tags == nil {
			tags = make([]string, 0)
		}
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(record{
			Reference:       result.Reference.String(),
			PinnedReference: pinned,
			Digest:          result.Descriptor.Digest.String(),
			MediaType:       result.Descriptor.MediaType,
			Size:            result.Descriptor.Size,
			ArtifactType:    result.Descriptor.ArtifactType,
			Tags:            tags,
			Version:         version,
		})
	default:
		return unsupportedFormat(string(format))
	}
}

// pinnedReference names the manifest result describes by its digest. It is
// built from the descriptor the push wrote, never from a read of a tag.
func pinnedReference(result ociartifact.PushResult) (string, error) {
	if result.Reference.IsLayout() {
		return "", fmt.Errorf("an %s destination has no pinned reference to print", ociartifact.LayoutScheme)
	}
	return result.Reference.PinnedString(result.Descriptor.Digest.String()), nil
}
