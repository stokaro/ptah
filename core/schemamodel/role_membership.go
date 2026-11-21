package schemamodel

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// RoleMembershipOptions are the options one membership of a role in another
// carries: whether the member may grant the role onward (Admin), whether the
// member uses the role's privileges without switching to it (Inherit), and
// whether the member may switch to the role (Set).
//
// A nil option is not stated. A target holds the option's default for it:
// Admin false, Set true, and Inherit as the member role's own Inherit
// attribute. A comparison treats a stated option equal to the default as equal
// to a missing one, so a declaration that states no option leaves a membership
// that holds the defaults alone.
type RoleMembershipOptions struct {
	// Admin is WITH ADMIN OPTION.
	Admin *bool `json:"admin,omitempty"`
	// Inherit is WITH INHERIT, which PostgreSQL 16 and later hold per
	// membership.
	Inherit *bool `json:"inherit,omitempty"`
	// Set is WITH SET, which PostgreSQL 16 and later hold per membership.
	Set *bool `json:"set,omitempty"`
}

// IsZero reports whether no option is stated.
func (o RoleMembershipOptions) IsZero() bool {
	return o.Admin == nil && o.Inherit == nil && o.Set == nil
}

// Clone returns a copy that shares no pointer with o.
func (o RoleMembershipOptions) Clone() RoleMembershipOptions {
	return RoleMembershipOptions{Admin: cloneBool(o.Admin), Inherit: cloneBool(o.Inherit), Set: cloneBool(o.Set)}
}

// Equal reports whether o and other state the same options.
func (o RoleMembershipOptions) Equal(other RoleMembershipOptions) bool {
	return equalBoolPointers(o.Admin, other.Admin) &&
		equalBoolPointers(o.Inherit, other.Inherit) &&
		equalBoolPointers(o.Set, other.Set)
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

func equalBoolPointers(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// CloneMemberOptions returns a copy of options that shares no pointer with it,
// or nil when options is empty.
func CloneMemberOptions(options map[string]RoleMembershipOptions) map[string]RoleMembershipOptions {
	if len(options) == 0 {
		return nil
	}
	cloned := make(map[string]RoleMembershipOptions, len(options))
	for group, option := range options {
		cloned[group] = option.Clone()
	}
	return cloned
}

// ParseMemberOf reads the entries of a member_of list. An entry is a group
// name, optionally followed by its membership options in parentheses,
// separated by semicolons: `readers`, `readers(admin)` or
// `readers(admin;inherit=false;set=true)`. An option written without a value
// is true. The returned groups keep the order the entries were written in, and
// the options map is nil when no entry states one.
//
// A malformed entry, an option name other than admin, inherit and set, an
// option value other than exactly true or false, an option stated twice, and a
// group written twice with different options are refused, each naming the
// entry.
func ParseMemberOf(entries []string) ([]string, map[string]RoleMembershipOptions, error) {
	var groups []string
	var options map[string]RoleMembershipOptions
	for _, entry := range entries {
		group, parsed, err := parseMemberOfEntry(entry)
		if err != nil {
			return nil, nil, err
		}
		if group == "" {
			continue
		}
		if slices.Contains(groups, group) {
			if !options[group].Equal(parsed) {
				return nil, nil, fmt.Errorf("member_of states membership in %q twice with different options", group)
			}
			continue
		}
		groups = append(groups, group)
		if !parsed.IsZero() {
			if options == nil {
				options = make(map[string]RoleMembershipOptions)
			}
			options[group] = parsed
		}
	}
	return groups, options, nil
}

func parseMemberOfEntry(entry string) (string, RoleMembershipOptions, error) {
	entry = strings.TrimSpace(entry)
	open := strings.IndexByte(entry, '(')
	if open < 0 {
		if strings.ContainsAny(entry, ");") {
			return "", RoleMembershipOptions{}, fmt.Errorf("member_of entry %q: expected a group name or group(option;option)", entry)
		}
		return entry, RoleMembershipOptions{}, nil
	}
	if !strings.HasSuffix(entry, ")") {
		return "", RoleMembershipOptions{}, fmt.Errorf("member_of entry %q: the option list is not closed with )", entry)
	}
	group := strings.TrimSpace(entry[:open])
	if group == "" {
		return "", RoleMembershipOptions{}, fmt.Errorf("member_of entry %q: the group name is missing", entry)
	}
	body := strings.TrimSpace(entry[open+1 : len(entry)-1])
	if body == "" {
		return "", RoleMembershipOptions{}, fmt.Errorf("member_of entry %q: the option list is empty", entry)
	}
	var options RoleMembershipOptions
	for option := range strings.SplitSeq(body, ";") {
		name, value, hasValue := strings.Cut(strings.TrimSpace(option), "=")
		name = strings.ToLower(strings.TrimSpace(name))
		enabled := true
		if hasValue {
			switch written := strings.TrimSpace(value); written {
			case "true":
			case "false":
				enabled = false
			default:
				return "", RoleMembershipOptions{}, fmt.Errorf(
					"member_of entry %q: option %s takes true or false, got %q", entry, name, written)
			}
		}
		var target **bool
		switch name {
		case "admin":
			target = &options.Admin
		case "inherit":
			target = &options.Inherit
		case "set":
			target = &options.Set
		default:
			return "", RoleMembershipOptions{}, fmt.Errorf(
				"member_of entry %q: unknown membership option %q, expected admin, inherit or set", entry, name)
		}
		if *target != nil {
			return "", RoleMembershipOptions{}, fmt.Errorf("member_of entry %q: option %s is stated twice", entry, name)
		}
		*target = &enabled
	}
	return group, options, nil
}

// FormatMemberOf writes a role's member_of list in the spelling
// [ParseMemberOf] reads, one entry per group in declaration order.
func FormatMemberOf(role Role) []string {
	entries := make([]string, 0, len(role.MemberOf))
	for _, group := range role.MemberOf {
		options := role.MemberOptions[group]
		var stated []string
		for _, option := range []struct {
			name  string
			value *bool
		}{{"admin", options.Admin}, {"inherit", options.Inherit}, {"set", options.Set}} {
			if option.value != nil {
				stated = append(stated, option.name+"="+strconv.FormatBool(*option.value))
			}
		}
		if len(stated) == 0 {
			entries = append(entries, group)
			continue
		}
		entries = append(entries, group+"("+strings.Join(stated, ";")+")")
	}
	return entries
}
