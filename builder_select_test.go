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
	sql, args, err := Dialect(ORACLE).Select("a", "b", "c").From("table1").OrderBy("a ASC").
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT a,b,c,RN FROM (SELECT a,b,c,ROWNUM RN FROM table1 ORDER BY a ASC) at WHERE at.ROWNUM<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// simple with join -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1 t1").
		InnerJoin("table2 t2", "t1.id = t2.ref_id").OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT a,b,c,RN FROM (SELECT a,b,c,ROWNUM RN FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.ref_id ORDER BY a ASC) at WHERE at.ROWNUM<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// simple -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").
		OrderBy("a ASC").Limit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT a,b,c,ROWNUM RN FROM table1 ORDER BY a ASC) at WHERE at.ROWNUM<=?", sql)
	assert.EqualValues(t, 1, len(args))
	fmt.Println(sql, args)

	// simple with where -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").Where(Eq{"f1": "v1", "f2": "v2"}).
		OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM (SELECT a,b,c,RN FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE f1=? AND f2=? ORDER BY a ASC) at WHERE at.ROWNUM<=?) att WHERE att.RN>?", sql)
	assert.EqualValues(t, 4, len(args))
	fmt.Println(sql, args)

	// simple -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").OrderBy("a ASC").
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM table1 ORDER BY a ASC LIMIT 5 OFFSET 10", sql)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)

	// simple -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").
		OrderBy("a ASC").Limit(5).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM table1 ORDER BY a ASC LIMIT 5", sql)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)

	// simple with where -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").
		Where(Eq{"f1": "v1", "f2": "v2"}).OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT a,b,c FROM table1 WHERE f1=? AND f2=? ORDER BY a ASC LIMIT 5 OFFSET 10", sql)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// simple -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").PK("id").From("table1").
		OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT TOP 5 a,b,c FROM (SELECT TOP 15 a,b,c,id FROM (SELECT a,b,c,id FROM table1 ORDER BY a ASC)) WHERE id NOT IN (SELECT TOP 15 a,b,c,id FROM (SELECT a,b,c,id FROM table1 ORDER BY a ASC))", sql)
	assert.EqualValues(t, 0, len(args))
	fmt.Println(sql, args)

	// simple with where -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").PK("id").From("table1").
		Where(Eq{"a": "3"}).OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT TOP 5 a,b,c FROM (SELECT TOP 15 a,b,c,id FROM (SELECT a,b,c,id FROM table1 WHERE a=? ORDER BY a ASC)) WHERE id NOT IN (SELECT TOP 15 a,b,c,id FROM (SELECT a,b,c,id FROM table1 WHERE a=? ORDER BY a ASC))", sql)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// raise error
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").From("table1").
		OrderBy("a ASC").Limit(5, 10).ToSQL()
	assert.Error(t, err)
	fmt.Println(err)

	// union with limit -- OracleSQL style
	sql, args, err = Dialect(ORACLE).Select("a", "b", "c").From("table1").
		Where(Eq{"a": 1}).OrderBy("a ASC").Limit(5, 10).Union("ALL",
		Select("a", "b", "c").From("table1").Where(Eq{"a": 2}).OrderBy("a DESC").Limit(10)).
		Limit(3).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT * FROM ((SELECT a,b,c FROM (SELECT a,b,c,RN FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE a=? ORDER BY a ASC) at WHERE at.ROWNUM<=?) att WHERE att.RN>?) UNION ALL (SELECT a,b,c FROM (SELECT a,b,c,ROWNUM RN FROM table1 WHERE a=? ORDER BY a DESC) at WHERE at.ROWNUM<=?)) at WHERE at.ROWNUM<=?", sql)
	assert.EqualValues(t, 6, len(args))
	assert.EqualValues(t, "[1 15 10 2 10 3]", fmt.Sprintf("%v", args))
	fmt.Println(sql, args)

	// union -- MySQL/SQLite/PostgreSQL style
	sql, args, err = Dialect(MYSQL).Select("a", "b", "c").From("table1").Where(Eq{"a": 1}).
		OrderBy("a ASC").Limit(5, 9).Union("ALL",
		Select("a", "b", "c").From("table1").Where(Eq{"a": 2}).OrderBy("a DESC").Limit(10)).
		Limit(5, 10).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "(SELECT a,b,c FROM table1 WHERE a=? ORDER BY a ASC LIMIT 5 OFFSET 9) UNION ALL (SELECT a,b,c FROM table1 WHERE a=? ORDER BY a DESC LIMIT 10) LIMIT 5 OFFSET 10", sql)
	assert.EqualValues(t, 2, len(args))
	fmt.Println(sql, args)

	// union with limit -- MsSQL style
	sql, args, err = Dialect(MSSQL).Select("a", "b", "c").From("table1").
		PK("id1").Where(Eq{"a": 1}).OrderBy("a ASC").Limit(5, 6).Union("ALL",
		Select("a", "b").From("table1").Where(Eq{"b": 2}).OrderBy("a DESC").Limit(10)).
		OrderBy("b DESC").Limit(7).ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT TOP 7 * FROM ((SELECT TOP 5 a,b,c FROM (SELECT TOP 11 a,b,c,id1 FROM (SELECT a,b,c,id1 FROM table1 WHERE a=? ORDER BY a ASC)) WHERE id1 NOT IN (SELECT TOP 11 a,b,c,id1 FROM (SELECT a,b,c,id1 FROM table1 WHERE a=? ORDER BY a ASC))) UNION ALL (SELECT TOP 10 a,b FROM (SELECT a,b FROM table1 WHERE b=? ORDER BY a DESC)))", sql)
	assert.EqualValues(t, 3, len(args))
	fmt.Println(sql, args)
}
