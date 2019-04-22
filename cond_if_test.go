// Copyright 2019 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCond_If(t *testing.T) {
	cond1 := If(1 > 0, Eq{"a": 1}, Eq{"b": 1})
	sql, err := ToBoundSQL(cond1)
	assert.NoError(t, err)
	assert.EqualValues(t, "a=1", sql)

	cond2 := If(1 < 0, Eq{"a": 1}, Eq{"b": 1})
	sql, err = ToBoundSQL(cond2)
	assert.NoError(t, err)
	assert.EqualValues(t, "b=1", sql)

	cond3 := If(1 > 0, cond2, Eq{"c": 1})
	sql, err = ToBoundSQL(cond3)
	assert.NoError(t, err)
	assert.EqualValues(t, "b=1", sql)

	cond4 := If(2 < 0, Eq{"d": "a"})
	sql, err = ToBoundSQL(cond4)
	assert.NoError(t, err)
	assert.EqualValues(t, "", sql)

	cond5 := And(cond1, cond2, cond3, cond4)
	sql, err = ToBoundSQL(cond5)
	assert.NoError(t, err)
	assert.EqualValues(t, "a=1 AND b=1 AND b=1", sql)
}
