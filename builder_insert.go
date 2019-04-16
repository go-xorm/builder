// Copyright 2016 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"bytes"
	"fmt"
)

// Insert creates an insert Builder
func Insert(eq Eq) *Builder {
	builder := &Builder{cond: NewCond()}
	return builder.Insert(eq)
}

func (b *Builder) insertWriteTo(w Writer) error {
	if len(b.tableName) <= 0 {
		return ErrNoTableName
	}
	if len(b.inserts) <= 0 {
		return ErrNoColumnToInsert
	}

	if _, err := fmt.Fprintf(w, "INSERT INTO %s (", b.tableName); err != nil {
		return err
	}

	var args = make([]interface{}, 0)
	var bs []byte
	var valBuffer = bytes.NewBuffer(bs)
	var i = 0

	for _, col := range b.inserts.sortedKeys() {
		value := b.inserts[col]
		fmt.Fprint(w, col)
		if e, ok := value.(expr); ok {
			fmt.Fprintf(valBuffer, "(%s)", e.sql)
			args = append(args, e.args...)
		} else {
			fmt.Fprint(valBuffer, "?")
			args = append(args, value)
		}

		if i != len(b.inserts)-1 {
			if _, err := fmt.Fprint(w, ","); err != nil {
				return err
			}
			if _, err := fmt.Fprint(valBuffer, ","); err != nil {
				return err
			}
		}
		i = i + 1
	}

	if _, err := fmt.Fprint(w, ") Values ("); err != nil {
		return err
	}

	if _, err := w.Write(valBuffer.Bytes()); err != nil {
		return err
	}
	if _, err := fmt.Fprint(w, ")"); err != nil {
		return err
	}

	w.Append(args...)

	return nil
}
