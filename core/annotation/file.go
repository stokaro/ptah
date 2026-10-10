package annotation

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
)

// FileDecoder reads one owner's declarations of one Go file, for an owner
// whose declarations refer to each other or to the file's tables: a consumer
// of a changefeed the file declares after it, or a setting of a table the
// file declares after the directive. [Extension.File] starts one for each
// file in which the owner's directives appear, and the frontend drops it once
// Finish returns. A FileDecoder may keep the declarations it is handed.
type FileDecoder interface {
	// Decode reads one declaration of the owner's directives, in the order
	// the file writes them, and returns what the declaration declares on its
	// own. A declaration that depends on the rest of the file returns nothing
	// here and contributes from Finish.
	Decode(Declaration) ([]Contribution, error)
	// Finish returns what the file's declarations declare together, once the
	// frontend has read the whole file. tables are the tables the file
	// declares. Each contribution names its [Contribution.Source], and a
	// refusal names its declaration with a [DeclarationError].
	Finish(tables Tables) ([]Contribution, error)
}

// DeclarationError is an owner's refusal that names the attribute it is
// about, or a declaration other than the one being decoded. The frontend
// reports Attribute with the refusal and places it at Declaration. Any other
// error an owner returns refuses the declaration being decoded and names no
// attribute.
type DeclarationError struct {
	// Declaration is the declaration refused. The zero value means the one
	// being decoded. A refusal [FileDecoder.Finish] returns sets it, since no
	// declaration is being decoded then.
	Declaration Declaration
	// Attribute names the attribute the refusal is about, or is empty.
	Attribute string
	// Err is the refusal.
	Err error
}

// Error returns the refusal's text, which names neither the declaration nor
// the attribute: the frontend adds where the declaration is written.
func (e *DeclarationError) Error() string { return e.Err.Error() }

// Unwrap returns the refusal.
func (e *DeclarationError) Unwrap() error { return e.Err }

// Table is a table one Go file declares, as the frontend read it.
type Table struct {
	// Schema is the schema the table directive names, or empty where it
	// names none.
	Schema string
	// Name is the table's name.
	Name string
	// Struct is the Go struct the table directive is attached to.
	Struct string
}

// Tables are the tables one Go file declares, in the order the file writes
// them.
type Tables []Table

// Owning returns the position in t of the table a declaration of one of a
// table's parts belongs to: the one table names, optionally
// schema-qualified, or, where table is empty, the one the declaration's
// struct maps to. part names what the declaration declares, such as "a
// changefeed", as the refusal reads it.
//
// A name without a schema matches a table of that name in any schema of the
// file, and is refused when the file declares it in more than one: attaching
// the part to whichever came first would act on a table the author may not
// have meant. A table the file does not declare is refused too, because a
// part of a table nobody declares has no effect.
func (t Tables) Owning(structName, table, part string) (int, error) {
	if table == "" {
		for index, declared := range t {
			if declared.Struct == structName {
				return index, nil
			}
		}
		return -1, fmt.Errorf("struct %s maps to no table in this file; name the table with the table attribute", structName)
	}
	schemaName, tableName := QualifiedName("", table)
	var matches []int
	for index, declared := range t {
		if declared.Name == tableName && (schemaName == "" || declared.Schema == schemaName) {
			matches = append(matches, index)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return -1, fmt.Errorf("table %q is not declared in this file, and %s is declared beside its table", table, part)
	}
	schemas := make([]string, 0, len(matches))
	for _, index := range matches {
		schemas = append(schemas, strconv.Quote(cmp.Or(t[index].Schema, "(default)")))
	}
	return -1, fmt.Errorf("table %q is declared in schemas %s; name the schema in the table attribute", table, strings.Join(schemas, " and "))
}

// Reader reads the owner declarations of one Go file for the frontend.
// [Set.Reader] starts one for each file. A Reader is not safe for concurrent
// use.
type Reader struct {
	set   Set
	files map[int]FileDecoder
	order []int
}

// Reader starts reading the owner declarations of one file.
func (s Set) Reader() *Reader {
	return &Reader{set: s, files: make(map[int]FileDecoder)}
}

// Decode hands declaration to the owner of its directive: to its Decode, or
// to the [FileDecoder] it started for this file. A directive no extension of
// the set declares is an error, and so is a contribution of a model the owner
// does not declare.
func (r *Reader) Decode(declaration Declaration) ([]Contribution, error) {
	index, found := r.set.owners[declaration.Directive]
	if !found {
		return nil, fmt.Errorf("no selected owner declares directive %q", declaration.Directive)
	}
	extension := r.set.extensions[index]
	declaration.Attributes = cloneAttributes(declaration.Attributes)
	var (
		contributions []Contribution
		err           error
	)
	if extension.File == nil {
		contributions, err = extension.Decode(declaration)
	} else {
		var decoder FileDecoder
		if decoder, err = r.file(index); err == nil {
			contributions, err = decoder.Decode(declaration)
		}
	}
	if err != nil {
		return nil, err
	}
	if err := checkContributions(extension, declaration.Directive, contributions); err != nil {
		return nil, err
	}
	return contributions, nil
}

// Finish asks each owner that read a declaration of the file through a
// [FileDecoder] for what the file's declarations declare together, in the
// order the owners first appeared in the file. tables are the tables the file
// declares.
func (r *Reader) Finish(tables Tables) ([]Contribution, error) {
	var joined []Contribution
	for _, index := range r.order {
		extension := r.set.extensions[index]
		contributions, err := r.files[index].Finish(tables)
		if err != nil {
			return nil, err
		}
		for _, contribution := range contributions {
			if contribution.Source.Directive == "" {
				return nil, fmt.Errorf("%w: %s finished a contribution that names no declaration", schemaext.ErrInvalidValue, extension.Owner)
			}
		}
		if err := checkContributions(extension, "", contributions); err != nil {
			return nil, err
		}
		joined = append(joined, contributions...)
	}
	return joined, nil
}

func (r *Reader) file(index int) (FileDecoder, error) {
	if decoder, started := r.files[index]; started {
		return decoder, nil
	}
	decoder := r.set.extensions[index].File()
	if decoder == nil {
		return nil, fmt.Errorf("%w: %s started no file decoder", schemaext.ErrInvalidValue, r.set.extensions[index].Owner)
	}
	r.files[index] = decoder
	r.order = append(r.order, index)
	return decoder, nil
}
