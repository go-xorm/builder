// Copyright 2016 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import "fmt"

// Iflike defines like condition
type Iflike [3]string

var _ Cond = Iflike{"", "", ""}

// WriteTo write SQL to Writer
func (iflike Iflike) WriteTo(w Writer) error {
	if _, err := fmt.Fprintf(w, "%s LIKE ?", iflike[0]); err != nil {
		return err
	}
	// FIXME: if use other regular express, this will be failed. but for compatible, keep this
	if iflike[1][0] == '%' || iflike[1][len(iflike[1])-1] == '%' {
		w.Append(iflike[1])
	} else {
		w.Append("%" + iflike[1] + "%")
	}
	return nil
}

// And implements And with other conditions
func (iflike Iflike) And(conds ...Cond) Cond {
	return And(iflike, And(conds...))
}

// Or implements Or with other conditions
func (iflike Iflike) Or(conds ...Cond) Cond {
	return Or(iflike, Or(conds...))
}

// IsValid tests if this condition is valid
func (iflike Iflike) IsValid() bool {
	if len(iflike[0]) > 0 && len(iflike[1]) > 0 {
		if iflike[1] == iflike[2] {
			return false
		}
		return true
	}
	return false
}
