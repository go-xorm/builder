// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilderSelect(t *testing.T) {
	sql, args, err := Select("c, d").From("table1").ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	sql, args, err = Select("c, d").From("table1").Where(Eq{"a": 1}).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	sql, args, err = Select("c, d").From("table1").LeftJoin("table2", Eq{"table1.id": 1}.And(Lt{"table2.id": 3})).
		RightJoin("table3", "table2.id = table3.tid").Where(Eq{"a": 1}).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)
}

func TestBuilderSelectGroupBy(t *testing.T) {
	sql, args, err := Select("c").From("table1").GroupBy("c").Having("count(c)=1").ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT c FROM table1 GROUP BY c HAVING count(c)=1", sql)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)
}

func TestBuilderSelectOrderBy(t *testing.T) {
	sql, args, err := Select("c").From("table1").OrderBy("c DESC").ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT c FROM table1 ORDER BY c DESC", sql)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)
}

func TestBuilder_From(t *testing.T) {
	// simple one
	sql, args, err := Select("c").From("table1").ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)

	// from sub
	sql, args, err = Select("sub.id").From("sub",
		Select("id").From("table1").Where(Eq{"a": 1})).Where(Eq{"b": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// from union
	sql, args, err = Select("sub.id").From("sub",
		Select("id").From("table1").Where(Eq{"a": 1}).
			Union("all", Select("id").From("table1").Where(Eq{"a": 2}))).Where(Eq{"b": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(args))
	fmt.Println(sql, args)

	// will raise error
	sql, args, err = Select("c").From("table1", Insert(Eq{"a": 1}).From("table1")).ToSQL()
	assert.Error(t, err)
	fmt.Println(err)
}

func TestBuilder_Limit(t *testing.T) {
	// simple -- OracleSQL style
	sql, args, err := Select("a", "b", "c").From("table1").OrderBy("a ASC").
		OracleLimit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// simple -- OracleSQL style
	sql, args, err = Select("a", "b", "c").From("table1").OrderBy("a ASC").OracleLimit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(args))
	fmt.Println(sql, args)

	// simple with where -- OracleSQL style
	sql, args, err = Select("a", "b", "c").From("table1").Where(Eq{"f1": "v1", "f2": "v2"}).
		OrderBy("a ASC").OracleLimit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 4, len(args))
	fmt.Println(sql, args)

	// simple -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Select("a", "b", "c").From("table1").OrderBy("a ASC").
		MySQLLimit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)

	// simple -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Select("a", "b", "c").From("table1").OrderBy("a ASC").MySQLLimit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)

	// simple with where -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Select("a", "b", "c").From("table1").
		Where(Eq{"f1": "v1", "f2": "v2"}).OrderBy("a ASC").MySQLLimit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// simple -- MsSQL style
	sql, args, err = Select("a", "b", "c").From("table1").
		OrderBy("a ASC").MsSQLLimit("id", 5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)

	// raise error
	sql, args, err = Select("a", "b", "c").From("table1").
		OrderBy("a ASC").MsSQLLimit("", 5, 10).ToSQL()
	assert.Error(t, err)
	fmt.Println(err)

	// union with limit -- OracleSQL style
	sql, args, err = Select("a", "b", "c").From("table1").Where(Eq{"a": 1}).OrderBy("a ASC").
		OracleLimit(5, 10).Union("ALL",
		Select("a", "b", "c").From("table1").Where(Eq{"a": 2}).OrderBy("a DESC").OracleLimit(10)).
		OracleLimit(3).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 6, len(args))
	assert.EqualValues(t, "[1 15 10 2 10 3]", fmt.Sprintf("%v", args))
	fmt.Println(sql, args)

	// union -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Select("a", "b", "c").From("table1").Where(Eq{"a": 1}).
		OrderBy("a ASC").MySQLLimit(5, 9).Union("ALL",
		Select("a", "b", "c").From("table1").Where(Eq{"a": 2}).OrderBy("a DESC").MySQLLimit(10)).
		MySQLLimit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// union with limit -- MsSQL style
	sql, args, err = Select("a", "b", "c").From("table1").Where(Eq{"a": 1}).
		OrderBy("a ASC").MsSQLLimit("id", 5, 6).Union("ALL",
		Select("a", "b").From("table1").Where(Eq{"a": 2}).OrderBy("a DESC").MsSQLLimit("id", 10)).
		OrderBy("b DESC").MsSQLLimit("id", 7).ToSQL()
	assert.NoError(t, err)
	// assert.EqualValues(t, 6, len(args))
	fmt.Println(sql, args)
}
