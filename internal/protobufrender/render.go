package protobufrender

import (
	"fmt"
	"strconv"
	"strings"
)

// render serializes the model deterministically. The layout is a fixed point of
// buf format, so a `buf format -w` pre-commit hook - routine in exactly the
// buf-using repositories this target serves - cannot rewrite the file, break
// the content digest and make every later export refuse to run.
func render(f file) string {
	var sb strings.Builder

	sb.WriteString("edition = \"2023\";\n\n")
	sb.WriteString("package " + f.Package + ";\n\n")

	for _, imp := range f.Imports {
		sb.WriteString("import " + quoteProtoString(imp) + ";\n")
	}
	if len(f.Imports) > 0 {
		sb.WriteString("\n")
	}

	if f.GoPackage != "" {
		sb.WriteString("option go_package = " + quoteProtoString(f.GoPackage) + ";\n\n")
	}

	for i, msg := range f.Messages {
		if i > 0 {
			sb.WriteString("\n")
		}
		renderMessage(&sb, msg)
	}
	for i, en := range f.Enums {
		if i > 0 || len(f.Messages) > 0 {
			sb.WriteString("\n")
		}
		renderEnum(&sb, en)
	}
	// Exactly one blank line between the body and the header, and exactly one
	// trailing newline. A schema that selects no tables would otherwise carry the
	// blank line that follows the package statement, which `buf format` strips -
	// rewriting the file, invalidating the content digest and making every later
	// export refuse to run.
	body := strings.TrimRight(sb.String(), "\n")
	return body + "\n\n" + renderHeader()
}

// renderHeader writes the generated header block. It is the LAST thing in the
// file: as the leading comment it would be the file's leading detached comment,
// which protoc-gen-go copies to the top of every .pb.go, putting Ptah's content
// digest into consumers' Go source. The digest line is stamped with its real
// value by stampDigest once the rest of the file is complete.
func renderHeader() string {
	return generatedMarker + "\n" +
		versionPrefix + strconv.Itoa(exportVersion) + "\n" +
		digestPrefix + "\n"
}

func renderMessage(sb *strings.Builder, msg message) {
	writeComment(sb, "", msg.Comment)
	// buf format collapses an empty body to "{}", so emitting "{\n}" would make
	// the file a non-fixed point: one `buf format -w` would rewrite it and
	// invalidate the content digest. Reachable through a tombstone whose type
	// never had any fields.
	if len(msg.Fields) == 0 && !hasReservations(msg.Reserved) {
		sb.WriteString("message " + msg.Name + " {}\n")
		return
	}
	sb.WriteString("message " + msg.Name + " {\n")
	for _, fld := range msg.Fields {
		writeComment(sb, "  ", fld.Comment)
		sb.WriteString("  ")
		if fld.Repeated {
			sb.WriteString("repeated ")
		}
		sb.WriteString(fld.Type + " " + fld.Name + " = " + strconv.FormatInt(int64(fld.Number), 10) + ";\n")
	}
	if len(msg.Fields) > 0 && hasReservations(msg.Reserved) {
		sb.WriteString("\n")
	}
	writeReservations(sb, msg.Reserved)
	sb.WriteString("}\n")
}

func renderEnum(sb *strings.Builder, en enum) {
	writeComment(sb, "", en.Comment)
	sb.WriteString("enum " + en.Name + " {\n")
	for _, value := range en.Values {
		writeComment(sb, "  ", value.Comment)
		sb.WriteString("  " + value.Name + " = " + strconv.FormatInt(int64(value.Number), 10) + ";\n")
	}
	if len(en.Values) > 0 && hasReservations(en.Reserved) {
		sb.WriteString("\n")
	}
	writeReservations(sb, en.Reserved)
	sb.WriteString("}\n")
}

func hasReservations(res reservations) bool {
	return res.hasNumbers() || len(res.Names) > 0
}

// writeReservations emits numbers as one ascending statement, each range
// written the way it is held, then names as one statement in ascending order.
// Under editions the names are bare identifiers: quoting them is a hard parse
// error, which is the inverse of proto2/proto3.
func writeReservations(sb *strings.Builder, res reservations) {
	if !hasReservations(res) {
		return
	}
	if res.hasNumbers() {
		parts := make([]string, 0, len(res.Ranges))
		for _, r := range res.Ranges {
			if r.Start == r.End {
				parts = append(parts, strconv.FormatInt(int64(r.Start), 10))
				continue
			}
			parts = append(parts, fmt.Sprintf("%d to %d", r.Start, r.End))
		}
		sb.WriteString("  reserved " + strings.Join(parts, ", ") + ";\n")
	}
	if len(res.Names) > 0 {
		sb.WriteString("  reserved " + strings.Join(res.Names, ", ") + ";\n")
	}
}

// writeComment renders a source comment as // lines. Embedded newlines are
// re-prefixed and control characters stripped so a comment can never break out
// of its context and inject .proto syntax.
func writeComment(sb *strings.Builder, indent, comment string) {
	if comment == "" {
		return
	}
	for line := range strings.SplitSeq(strings.ReplaceAll(comment, "\r\n", "\n"), "\n") {
		sb.WriteString(indent + "//")
		clean := stripControl(line)
		// A top-level comment reproducing a header marker would give the file a
		// second "// ptah:content-sha256=" line and break the digest invariant.
		// One extra space keeps the text readable and out of that namespace.
		if indent == "" && strings.HasPrefix(clean, "ptah:") {
			clean = " " + clean
		}
		if clean != "" {
			sb.WriteString(" " + clean)
		}
		sb.WriteString("\n")
	}
}

func stripControl(s string) string {
	var out strings.Builder
	for _, r := range s {
		if r == '\t' {
			out.WriteRune(' ')
			continue
		}
		if r < 0x20 || r == 0x7f {
			continue
		}
		out.WriteRune(r)
	}
	return strings.TrimRight(out.String(), " ")
}

// quoteProtoString escapes a value for a protobuf string literal. Option
// payloads such as go_package are the only free-form strings that reach the
// file body, and the post-render re-parse is explicitly not an injection
// backstop: a balanced injection parses and links cleanly.
func quoteProtoString(s string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			out.WriteString("\\\"")
		case '\\':
			out.WriteString("\\\\")
		case '\n':
			out.WriteString("\\n")
		case '\r':
			out.WriteString("\\r")
		case '\t':
			out.WriteString("\\t")
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&out, "\\x%02x", r)
				continue
			}
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
	return out.String()
}
