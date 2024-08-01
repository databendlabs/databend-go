package godatabend

import "fmt"

// TypeDesc describes a (possibly nested) data type returned by Databend.
type TypeDesc struct {
	Name     string
	Nullable bool
	Args     []*TypeDesc
}

func ParseTypeDesc(s string) (*TypeDesc, error) {
	var (
		name     = ""
		args     = []*TypeDesc{}
		depth    = 0
		start    = 0
		nullable = false
	)

	for i, c := range s {
		switch c {
		case '(':
			if depth == 0 {
				name = s[start:i]
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 {
				s := s[start:i]
				if s != "" {
					desc, err := ParseTypeDesc(s)
					if err != nil {
						return nil, err
					}
					args = append(args, desc)
				}
				start = i + 1
			}
		case ',':
			if depth == 1 {
				s := s[start:i]
				if s != "" {
					desc, err := ParseTypeDesc(s)
					if err != nil {
						return nil, err
					}
					args = append(args, desc)
				}
				start = i + 1
			}
		case ' ':
			if depth == 0 {
				s := s[start:i]
				if s != "" {
					name = s
				}
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("invalid type desc: %s", s)
	}
	if start < len(s) {
		s := s[start:]
		if s != "" {
			if name == "" {
				name = s
			} else if s == "NULL" {
				nullable = true
			} else {
				return nil, fmt.Errorf("invalid type arg for %s: %s", name, s)
			}
		}
	}
	return &TypeDesc{Name: name, Nullable: nullable, Args: args}, nil
}
