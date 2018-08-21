// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"errors"
	"fmt"
	"strings"
)

func (b *Builder) limitWriteTo(w Writer) error {
	if strings.TrimSpace(b.dialect) == "" {
		return errors.New("field `dialect` must be set up when performing LIMIT, try use `Dialect(dbType)` at first")
	}

	if b.limitation != nil {
		limit := b.limitation
		if limit.offset < 0 || limit.limitN <= 0 {
			return errors.New("unexpected offset/limitN")
		}
		// erase limit condition
		b.limitation = nil
		ow := w.(*BytesWriter)

		switch strings.ToLower(strings.TrimSpace(b.dialect)) {
		case ORACLE:
			if len(b.selects) == 0 {
				b.selects = append(b.selects, "*")
			}

			var final *Builder
			selects := b.selects
			b.selects = append(selects, "ROWNUM RN")

			var wb *Builder
			if b.optype == unionType {
				wb = Dialect(b.dialect).Select("at.*", "ROWNUM RN").
					From("at", b)
			} else {
				wb = b
			}

			if limit.offset == 0 {
				final = Dialect(b.dialect).Select(selects...).From("at", wb).
					Where(Lte{"at.RN": limit.limitN})
			} else {
				sub := Dialect(b.dialect).Select("*").
					From("at", b).Where(Lte{"at.RN": limit.offset + limit.limitN})

				final = Dialect(b.dialect).Select(selects...).From("att", sub).
					Where(Gt{"att.RN": limit.offset})
			}

			return final.WriteTo(ow)
		case SQLITE, MYSQL, POSTGRES:
			// if type UNION, we need to write previous content back to current writer
			if b.optype == unionType {
				b.WriteTo(ow)
			}

			if limit.offset == 0 {
				fmt.Fprint(ow, " LIMIT ", limit.limitN)
			} else {
				fmt.Fprintf(ow, " LIMIT %v OFFSET %v", limit.limitN, limit.offset)
			}
		case MSSQL:
			if len(b.selects) == 0 {
				b.selects = append(b.selects, "*")
			}

			var final *Builder
			selects := b.selects
			b.selects = append(append([]string{fmt.Sprintf("TOP %d %v", limit.limitN+limit.offset, b.selects[0])},
				b.selects[1:]...), "ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN")

			var wb *Builder
			if b.optype == unionType {
				wb = Dialect(b.dialect).Select("*", "ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN").
					From("at", b)
			} else {
				wb = b
			}

			if limit.offset == 0 {
				final = Dialect(b.dialect).Select(selects...).From("at", wb)
			} else {
				final = Dialect(b.dialect).Select(selects...).From("at", wb).Where(Gt{"at.RN": limit.offset})
			}

			return final.WriteTo(ow)
		default:
			return ErrNotSupportType
		}
	}

	return nil
}
