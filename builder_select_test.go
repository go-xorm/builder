// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Select(t *testing.T) {
	sql, args, err := Select("c, d").From("table1").ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	sql, args, err = Select("c, d").From("table1").Where(Eq{"a": 1}).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	sql, args, err = Select("c, d").From("table1").LeftJoin("table2", Eq{"table1.id": 1}.And(Lt{"table2.id": 3})).
		RightJoin("table3", "table2.id = table3.tid").Where(Eq{"a": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT c, d FROM table1 LEFT JOIN table2 ON table1.id=? AND table2.id<? RIGHT JOIN table3 ON table2.id = table3.tid WHERE a=?",
		sql)
	assert.EqualValues(t, []interface{}{1, 3, 1}, args)

	sql, args, err = Select("c, d").From("table1").LeftJoin("table2", Eq{"table1.id": 1}.And(Lt{"table2.id": 3})).
		FullJoin("table3", "table2.id = table3.tid").Where(Eq{"a": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT c, d FROM table1 LEFT JOIN table2 ON table1.id=? AND table2.id<? FULL JOIN table3 ON table2.id = table3.tid WHERE a=?",
		sql)
	assert.EqualValues(t, []interface{}{1, 3, 1}, args)

	sql, args, err = Select("c, d").From("table1").LeftJoin("table2", Eq{"table1.id": 1}.And(Lt{"table2.id": 3})).
		CrossJoin("table3", "table2.id = table3.tid").Where(Eq{"a": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT c, d FROM table1 LEFT JOIN table2 ON table1.id=? AND table2.id<? CROSS JOIN table3 ON table2.id = table3.tid WHERE a=?",
		sql)
	assert.EqualValues(t, []interface{}{1, 3, 1}, args)

	sql, args, err = Select("c, d").From("table1").LeftJoin("table2", Eq{"table1.id": 1}.And(Lt{"table2.id": 3})).
		InnerJoin("table3", "table2.id = table3.tid").Where(Eq{"a": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT c, d FROM table1 LEFT JOIN table2 ON table1.id=? AND table2.id<? INNER JOIN table3 ON table2.id = table3.tid WHERE a=?",
		sql)
	assert.EqualValues(t, []interface{}{1, 3, 1}, args)
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
	assert.EqualValues(t, "SELECT c FROM table1", sql)
	assert.EqualValues(t, 0, len(args))

	// from sub
	sql, args, err = Select("sub.id").From("sub",
		Select("id").From("table1").Where(Eq{"a": 1})).Where(Eq{"b": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT sub.id FROM (SELECT id FROM table1 WHERE a=?) sub WHERE b=?", sql)
	assert.EqualValues(t, []interface{}{1, 1}, args)

	// from union
	sql, args, err = Select("sub.id").From("sub",
		Select("id").From("table1").Where(Eq{"a": 1}).
			Union("all", Select("id").From("table1").Where(Eq{"a": 2}))).Where(Eq{"b": 1}).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT sub.id FROM ((SELECT id FROM table1 WHERE a=?) UNION ALL (SELECT id FROM table1 WHERE a=?)) sub WHERE b=?", sql)
	assert.EqualValues(t, []interface{}{1, 2, 1}, args)

	// will raise error
	sql, args, err = Select("c").From("table1", Insert(Eq{"a": 1}).From("table1")).ToSQL()
	assert.Error(t, err)
	fmt.Println(err)
}

func TestBuilder_Limit(t *testing.T) {
	// simple -- OracleSQL style
	sql, args, err := Dialect(ORACLE).Select("a", "b", "c").From("table1").OrderBy("a ASC").
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT * FROM (SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, []interface{}{15, 10}, args)

	// simple with join -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c", "d").From("table1 t1").
		InnerJoin("table2 t2", "t1.id = t2.ref_id").OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT * FROM (SELECT * FROM (SELECT a,b,c,d,ROWNUM RN FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.ref_id ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, []interface{}{15, 10}, args)

	// simple -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").
		OrderBy("a ASC").Limit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 ORDER BY a ASC) at WHERE at.RN<=?", sql)
	assert.EqualValues(t, []interface{}{5}, args)

	// simple with where -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").Where(Eq{"a": "10", "b": "10"}).
		OrderBy("a ASC").Limit(5, 1).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT * FROM (SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE a=? AND b=? ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, []interface{}{"10", "10", 6, 1}, args)

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
	assert.EqualValues(t, "SELECT TOP 5 a,b,c FROM (SELECT a,b,c FROM table1 ORDER BY a ASC) at", sql)
	assert.EqualValues(t, 0, len(args))

	// simple with where -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").From("table1").
		Where(Eq{"a": "3"}).OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT TOP 5 a,b,c,RN FROM (SELECT TOP 15 a,b,c,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN FROM table1 WHERE a=?) at WHERE at.RN>?", sql)
	assert.EqualValues(t, []interface{}{"3", 5}, args)

	// union with limit -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").
		Where(Neq{"a": "0"}).OrderBy("a ASC").Limit(5, 10).Union("ALL",
		Select("a", "b", "c").From("table1").Where(Neq{"b": "48"}).OrderBy("a DESC").Limit(10)).
		Limit(3).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT * FROM ((SELECT * FROM (SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE a<>? ORDER BY a ASC) at WHERE at.RN<=?) att WHERE att.RN>?) UNION ALL (SELECT * FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE b<>? ORDER BY a DESC) at WHERE at.RN<=?)) at WHERE at.RN<=?", sql)
	assert.EqualValues(t, []interface{}{"0", 15, 10, "48", 10, 3}, args)

	// union -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").Where(Eq{"a": 1}).
		OrderBy("a ASC").Limit(5, 9).Union("ALL",
		Select("a", "b", "c").From("table1").Where(Eq{"a": 2}).OrderBy("a DESC").Limit(10)).
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "(SELECT a,b,c FROM table1 WHERE a=? ORDER BY a ASC LIMIT 5 OFFSET 9) UNION ALL (SELECT a,b,c FROM table1 WHERE a=? ORDER BY a DESC LIMIT 10) LIMIT 5 OFFSET 10", sql)
	assert.EqualValues(t, []interface{}{1, 2}, args)

	// union with limit -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").From("table1").
		Where(Eq{"a": 1}).OrderBy("a ASC").Limit(5, 6).Union("ALL",
		Select("a", "b", "c").From("table1").Where(Eq{"b": 2}).OrderBy("a DESC").Limit(10)).
		OrderBy("b DESC").Limit(7).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT TOP 7 * FROM ((SELECT TOP 5 a,b,c,RN FROM (SELECT TOP 11 a,b,c,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RN FROM table1 WHERE a=?) at WHERE at.RN>?) UNION ALL (SELECT TOP 10 a,b,c FROM (SELECT a,b,c FROM table1 WHERE b=? ORDER BY a DESC) at)) at", sql)
	assert.EqualValues(t, []interface{}{1, 5, 2}, args)
}
