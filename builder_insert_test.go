// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilderInsert(t *testing.T) {
	sql, err := Insert(Eq{"c": 1, "d": 2}).Into("table1").ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "INSERT INTO table1 (c,d) Values (1,2)", sql)

	sql, err = Insert(Eq{"e": 3}, Eq{"c": 1}, Eq{"d": 2}).Into("table1").ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "INSERT INTO table1 (c,d,e) Values (1,2,3)", sql)

	sql, err = Insert(Eq{"c": 1, "d": Expr("SELECT b FROM t WHERE d=? LIMIT 1", 2)}).Into("table1").ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "INSERT INTO table1 (c,d) Values (1,(SELECT b FROM t WHERE d=2 LIMIT 1))", sql)

	sql, err = Insert(Eq{"c": 1, "d": 2}).ToBoundSQL()
	assert.Error(t, err)
	assert.EqualValues(t, ErrNoTableName, err)
	assert.EqualValues(t, "", sql)

	sql, err = Insert(Eq{}).Into("table1").ToBoundSQL()
	assert.Error(t, err)
	assert.EqualValues(t, ErrNoColumnToInsert, err)
	assert.EqualValues(t, "", sql)
}

func TestBuidlerInsert_Select(t *testing.T) {
	sql, err := Insert().Into("table1").Select().From("table2").ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "INSERT INTO table1 SELECT * FROM table2", sql)

	sql, err = Insert("a, b").Into("table1").Select("b, c").From("table2").ToBoundSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "INSERT INTO table1 (a, b) SELECT b, c FROM table2", sql)
}
