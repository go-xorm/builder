// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"errors"
	"fmt"
	"strings"
)

func (b *Builder) unionWriteTo(w Writer) error {
	if b.limitation != nil || b.cond != NewCond() ||
		b.orderBy != "" || b.having != "" || b.groupBy != "" {
		return errors.New("builder in unionType should not have any conditional fields in it(like Where or Limit)")
	}

	for idx, u := range b.unions {
		current := u.builder
		if current.optype != selectType {
			return errors.New("UNION is only allowed among SELECT operations")
		}

		if len(b.unions) == 1 {
			if err := current.selectWriteTo(w); err != nil {
				return err
			}
		} else {
			if idx != 0 {
				fmt.Fprint(w, fmt.Sprintf(" UNION %v ", strings.ToUpper(u.unionType)))
			}
			fmt.Fprint(w, "(")

			// switch b.dialect {
			// case ORACLE, MSSQL:
			// 	// if dialect is Oracle or Mssql, we need to make a copy of current context
			// 	// coz the result of current will turn into a sub-query when building LIMIT query
			// 	tw := NewWriter()
			// 	if err := current.selectWriteTo(tw); err != nil {
			// 		return err
			// 	}
			//
			// 	fmt.Fprintf(w, tw.writer.String())
			// 	w.(*BytesWriter).args = append(w.(*BytesWriter).args, tw.args...)
			// case MYSQL, SQLITE, POSTGRES:
			// 	if err := current.selectWriteTo(w); err != nil {
			// 		return err
			// 	}
			// default:
			// 	return ErrNotSupportType
			// }

			if err := current.selectWriteTo(w); err != nil {
				return err
			}

			fmt.Fprint(w, ")")
		}
	}

	return nil
}
