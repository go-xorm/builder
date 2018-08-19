// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Limit(t *testing.T) {
	// simple -- OracleSQL style
	sql, args, err := Dialect(ORACLE).Select("a", "b", "c").From("table1").OrderBy("a ASC").
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, []interface{}{15, 10}, args)

	// simple with join -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c", "d").From("table1 t1").
		InnerJoin("table2 t2", "t1.id = t2.ref_id").OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c,d FROM (SELECT * FROM (SELECT a,b,c,d,ROWNUM RN FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.ref_id ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, []interface{}{15, 10}, args)

	// simple -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").
		OrderBy("a ASC").Limit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT a,b,c,ROWNUM RN FROM table1 ORDER BY a ASC) at WHERE at.RN<=?", sql)
	assert.EqualValues(t, []interface{}{5}, args)

	// simple with where -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").Where(Neq{"a": "10", "b": "20"}).
		OrderBy("a ASC").Limit(5, 1).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE a<>? AND b<>? ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, []interface{}{"10", "20", 6, 1}, args)

	// simple -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").OrderBy("a ASC").
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM table1 ORDER BY a ASC LIMIT 5 OFFSET 10", sql)
	assert.EqualValues(t, 0, len(args))

	// simple -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").
		OrderBy("a ASC").Limit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM table1 ORDER BY a ASC LIMIT 5", sql)
	assert.EqualValues(t, 0, len(args))

	// simple with where -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").
		Where(Eq{"f1": "v1", "f2": "v2"}).OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM table1 WHERE f1=? AND f2=? ORDER BY a ASC LIMIT 5 OFFSET 10", sql)
	assert.EqualValues(t, []interface{}{"v1", "v2"}, args)

	// simple -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").From("table1").
		OrderBy("a ASC").Limit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT TOP 5 a,b,c,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN FROM table1 ORDER BY a ASC) at", sql)
	assert.EqualValues(t, []interface{}([]interface{}(nil)), args)

	// simple with where -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").From("table1").
		Where(Neq{"a": "3"}).OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT TOP 15 a,b,c,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN FROM table1 WHERE a<>? ORDER BY a ASC) at WHERE at.RN>?", sql)
	assert.EqualValues(t, []interface{}{"3", 10}, args)
	// union with limit -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("at",
		Dialect(ORACLE).Select("a", "b", "c").From("table1").
			Where(Neq{"a": "0"}).OrderBy("a ASC").Limit(5, 10).Union("ALL",
			Select("a", "b", "c").From("table1").Where(Neq{"b": "48"}).OrderBy("a DESC").Limit(10))).Limit(3).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT a,b,c,ROWNUM RN FROM ((SELECT a,b,c FROM (SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE a<>? ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?) UNION ALL (SELECT a,b,c FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE b<>? ORDER BY a DESC) at WHERE at.RN<=?)) at) at WHERE at.RN<=?", sql)
	assert.EqualValues(t, []interface{}{"0", 15, 10, "48", 10, 3}, args)

	// union -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("at",
		Dialect(MYSQL).Select("a", "b", "c").From("table1").Where(Eq{"a": 1}).OrderBy("a ASC").
			Limit(5, 9).Union("ALL",
			Select("a", "b", "c").From("table1").Where(Eq{"a": 2}).OrderBy("a DESC").Limit(10))).
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM ((SELECT a,b,c FROM table1 WHERE a=? ORDER BY a ASC LIMIT 5 OFFSET 9) UNION ALL (SELECT a,b,c FROM table1 WHERE a=? ORDER BY a DESC LIMIT 10)) at LIMIT 5 OFFSET 10", sql)
	assert.EqualValues(t, []interface{}{1, 2}, args)

	// union with limit -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").From("at",
		Dialect(MSSQL).Select("a", "b", "c").From("table1").Where(Neq{"a": "1"}).
			OrderBy("a ASC").Limit(5, 6).Union("ALL",
			Select("a", "b", "c").From("table1").Where(Neq{"b": "2"}).OrderBy("a DESC").Limit(10))).
		OrderBy("b DESC").Limit(7, 9).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT TOP 16 a,b,c,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN FROM ((SELECT a,b,c FROM (SELECT TOP 11 a,b,c,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN FROM table1 WHERE a<>? ORDER BY a ASC) at WHERE at.RN>?) UNION ALL (SELECT a,b,c FROM (SELECT TOP 10 a,b,c,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN FROM table1 WHERE b<>? ORDER BY a DESC) at)) at ORDER BY b DESC) at WHERE at.RN>?", sql)
	assert.EqualValues(t, []interface{}{"1", 6, "2", 9}, args)
}
