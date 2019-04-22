// Copyright 2016 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"fmt"
)

// Ifeq defines equals conditions
type Ifeq struct {
	Col     string
	Val     interface{}
	CondVal interface{}
}

var _ Cond = Ifeq{}

// WriteTo writes SQL to Writer
func (ifeq Ifeq) WriteTo(w Writer) error {
	if _, err := fmt.Fprintf(w, "%s = ?", ifeq.Col); err != nil {
		return err
	}
	w.Append(ifeq.Val)
	return nil
}

// And implements And with other conditions
func (ifeq Ifeq) And(conds ...Cond) Cond {
	return And(ifeq, And(conds...))
}

// Or implements Or with other conditions
func (ifeq Ifeq) Or(conds ...Cond) Cond {
	return Or(ifeq, Or(conds...))
}

// IsValid tests if this Ifeq is valid
func (ifeq Ifeq) IsValid() bool {
	return len(ifeq.Col) > 0 && ifeq.CondVal != ifeq.Val
}
