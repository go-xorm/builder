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

		var final *Builder

		switch strings.ToLower(strings.TrimSpace(b.dialect)) {
		case ORACLE:
			if ow.writer.Len() > 0 {
				// flush writer, both buffer & args
				ow.writer.Reset()
				ow.args = nil
			}

			selects := b.selects
			b.selects = append(selects, "ROWNUM RN")
			if limit.offset == 0 {
				if len(selects) == 0 {
					selects = append(selects, "*")
				} else {
					selects = append(selects)
				}

				final = Dialect(b.dialect).Select(selects...).From("at", b).
					Where(Lte{"at.ROWNUM": limit.limitN})
			} else {
				sub := Dialect(b.dialect).Select("*").
					From("at", b).Where(Lte{"at.ROWNUM": limit.offset + limit.limitN})

				final = Dialect(b.dialect).Select("*").From("att", sub).
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
			if ow.writer.Len() > 0 {
				// flush writer, both buffer & args
				ow.writer.Reset()
				ow.args = nil
			}

			selects := b.selects
			if limit.offset == 0 {
				if len(selects) == 0 {
					selects = append(selects, "*")
				}

				final = Dialect(b.dialect).
					Select(fmt.Sprintf("TOP %d %v", limit.limitN, strings.Join(selects, ","))).
					From("at", b)
			} else {
				sub := Dialect(b.dialect).Select(
					fmt.Sprintf("TOP %d %v,%v", limit.limitN+limit.offset,
						strings.Join(selects, ","), "ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN")).
					From(b.tableName).Where(b.cond)

				if len(selects) == 0 {
					return ErrNotSupportType
				}

				final = Dialect(b.dialect).Select(
					fmt.Sprintf("TOP %d %v", limit.limitN, strings.Join(append(selects, "RN"), ","))).
					From("at", sub).Where(Gt{"at.RN": limit.limitN})
			}

			return final.WriteTo(ow)
		default:
			return ErrNotSupportType
		}
	}

	return nil
}
