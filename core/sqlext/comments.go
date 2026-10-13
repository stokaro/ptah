package sqlext

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// CommentEdit sets the comment of an already resolved object. Empty Text
// removes a comment. The host resolves source order and object lifetimes
// before handing the final edits to the owner.
type CommentEdit struct {
	Object schemaext.Object
	Text   string
}

// CommentRequest is the complete comment batch for one owner in one file.
type CommentRequest struct {
	Target string
	Edits  []CommentEdit
}

// CommentEditor applies source comments to typed values. The owner receives
// one batch after the host resolves names, renames and removals. It returns
// one independent object per edit, in order, with identical identity and scope.
type CommentEditor interface {
	ApplySQLComments(context.Context, CommentRequest) ([]schemaext.Object, error)
}

// ApplyComments routes final source comments by kind, one call per owner.
// It validates every result before returning any changes. A missing editor,
// malformed result, canceled context or provider failure returns no objects.
func (s Set) ApplyComments(ctx context.Context, target string, edits []CommentEdit) ([]schemaext.Object, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: SQL comments require a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inputs := make([]schemaext.Object, 0, len(edits))
	groups := make(map[int][]int)
	var order []int
	for i, edit := range edits {
		owner, err := s.commentOwner(target, edit.Object)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, edit.Object)
		if _, found := groups[owner]; !found {
			order = append(order, owner)
		}
		groups[owner] = append(groups[owner], i)
	}
	before, err := schemaext.NewObjects(inputs...)
	if err != nil {
		return nil, err
	}
	result := make([]schemaext.Object, len(edits))
	for _, owner := range order {
		positions := groups[owner]
		request, err := capturedComments(target, edits, positions, before)
		if err != nil {
			return nil, err
		}
		objects, err := s.extensions[owner].Comments.ApplySQLComments(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateCommentResults(objects, edits, positions, before); err != nil {
			return nil, err
		}
		for i, object := range objects {
			result[positions[i]] = object
		}
	}
	return captureCommentResults(result)
}

func capturedComments(target string, edits []CommentEdit, positions []int, before schemaext.Objects) (CommentRequest, error) {
	request := CommentRequest{Target: target}
	for _, position := range positions {
		edit := edits[position]
		object, _, err := before.Get(edit.Object.Ref)
		if err != nil {
			return CommentRequest{}, err
		}
		request.Edits = append(request.Edits, CommentEdit{Object: object, Text: edit.Text})
	}
	return request, nil
}

func validateCommentResults(objects []schemaext.Object, edits []CommentEdit, positions []int, before schemaext.Objects) error {
	if len(objects) != len(positions) {
		return fmt.Errorf("%w: SQL comment editor returned %d objects for %d edits", schemaext.ErrInvalidValue, len(objects), len(positions))
	}
	for i, object := range objects {
		original, _, err := before.Get(edits[positions[i]].Object.Ref)
		if err != nil {
			return err
		}
		if object.Ref != original.Ref || !slices.Equal(object.Targets, original.Targets) {
			return fmt.Errorf("%w: SQL comment editor changes an object's identity or scope", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

func captureCommentResults(objects []schemaext.Object) ([]schemaext.Object, error) {
	captured, err := schemaext.NewObjects(objects...)
	if err != nil {
		return nil, err
	}
	for i := range objects {
		object, _, err := captured.Get(objects[i].Ref)
		if err != nil {
			return nil, err
		}
		objects[i] = object
	}
	return objects, nil
}

func (s Set) commentOwner(target string, object schemaext.Object) (int, error) {
	owner := -1
	for i, extension := range s.extensions {
		if extension.Comments == nil || (target != "" && !slices.Contains(extension.Targets, target)) || !slices.Contains(extension.Kinds, schemaext.Kind(object.Ref.Kind)) {
			continue
		}
		if owner >= 0 {
			return 0, fmt.Errorf("%w: several SQL comment editors own %s", schemaext.ErrDuplicate, object.Ref.Kind)
		}
		owner = i
	}
	if owner < 0 {
		return 0, fmt.Errorf("%w: no SQL comment editor owns %s", schemaext.ErrInvalidValue, object.Ref.Kind)
	}
	return owner, nil
}
