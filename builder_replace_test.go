// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Replace(t *testing.T) {
	sql, err := MySQL().Replace(Eq{"a": 1}, Eq{"b": 2}, Eq{"c": "3"}).Into("table1").ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "REPLACE INTO table1 SET a=1,b=2,c='3'", sql)

	sql, err = MySQL().Replace().Into("table1").From(
		Select("a", "b", "c").From("table2")).ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "REPLACE INTO table1 SELECT a,b,c FROM table2", sql)

	sql, err = MySQL().Replace("a", "b", "c").Into("table1").From(
		Select("a", "b", "c").From("table2")).ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "REPLACE INTO table1(a,b,c) SELECT a,b,c FROM table2", sql)

	sql, err = MySQL().Replace("a", "b", "c").Into("table1").From(
		Select("a", "b", "c").From("table2").Where(Neq{"a": 1})).ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "REPLACE INTO table1(a,b,c) SELECT a,b,c FROM table2 WHERE a<>1", sql)
}
