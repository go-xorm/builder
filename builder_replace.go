// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"fmt"
	"strings"
)

func (b *Builder) replaceWriteTo(w Writer) error {
	if len(b.tableName) == 0 {
		return ErrNoTableName
	}

	if len(b.replacements) == 0 && b.subQuery == nil {
		return fmt.Errorf("empty replacements")
	}

	var cols []string
	var conditions []Eq
	if len(b.replacements) > 0 {
		switch b.replacements[0].(type) {
		case string:
			cols = make([]string, 0, len(b.replacements))
			for e := range b.replacements {
				if val, ok := b.replacements[e].(string); ok {
					cols = append(cols, val)
				} else {
					return fmt.Errorf("non-uniform types")
				}
			}

			if b.subQuery == nil {
				return fmt.Errorf("derived table should not be empty")
			}
		case Eq:
			conditions = make([]Eq, 0, len(b.replacements))
			for e := range b.replacements {
				if val, ok := b.replacements[e].(Eq); ok {
					conditions = append(conditions, val)
				} else {
					return fmt.Errorf("non-uniform types in replacements")
				}
			}
		default:
			return fmt.Errorf("unsupported replacement type, supposed to be string or Eq cond")
		}
	}

	switch b.dialect {
	case MYSQL:
		if len(conditions) > 0 {
			fmt.Fprintf(w, "REPLACE INTO %v SET ", b.tableName)

			for e := range conditions {
				if err := conditions[e].WriteTo(w); err != nil {
					return err
				}

				if e != len(conditions)-1 {
					fmt.Fprintf(w, ",")
				}
			}
		} else {
			if len(cols) == 0 {
				fmt.Fprintf(w, "REPLACE INTO %v ", b.tableName)
			} else {
				fmt.Fprintf(w, "REPLACE INTO %v(%v) ", b.tableName, strings.Join(cols, ","))
			}

			return b.subQuery.WriteTo(w)
		}
	default:
		return ErrNotSupportType
	}

	return nil
}
