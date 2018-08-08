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

			if current.limitation != nil {
				tw := NewWriter()
				if err := current.selectWriteTo(tw); err != nil {
					return err
				}

				fmt.Fprintf(w, tw.writer.String())
				w.(*BytesWriter).args = append(w.(*BytesWriter).args, tw.args...)
			} else {
				if err := current.selectWriteTo(w); err != nil {
					return err
				}
			}

			fmt.Fprint(w, ")")
		}
	}

	return nil
}
