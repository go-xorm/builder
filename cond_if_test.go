// Copyright 2019 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCond_If(t *testing.T) {
	var cond = If(1 > 0, Eq{"a": 1}, Eq{"b": 1})
	sql, err := ToBoundSQL(cond)
	assert.NoError(t, err)
	assert.EqualValues(t, "a=1", sql)

	cond = If(1 < 0, Eq{"a": 1}, Eq{"b": 1})
	sql, err = ToBoundSQL(cond)
	assert.NoError(t, err)
	assert.EqualValues(t, "b=1", sql)

	cond = If(1 > 0, cond, Eq{"c": 1})
	sql, err = ToBoundSQL(cond)
	assert.NoError(t, err)
	assert.EqualValues(t, "b=1", sql)
}
