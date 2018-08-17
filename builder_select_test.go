// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"fmt"
	"math/rand"
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

func BenchmarkBuilder_Limit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		builder := randQuery(rand.Intn(1000) >= 500, true)
		b.StartTimer()

		_, _, err := builder.ToSQL()
		assert.NoError(b, err)
	}
}

func TestRandQuery(t *testing.T) {
	sql, args, err := randQuery(false, true).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	sql, args, err = randQuery(false, false).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	sql, args, err = randQuery(true, false).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	sql, args, err = randQuery(true, true).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)
}

// randQuery Generate a basic query for benchmark test. But be careful it's not a executable SQL in real db.
func randQuery(allowUnion, allowLimit bool) *Builder {
	b := randSimpleQuery(allowLimit)
	if allowUnion {
		r := rand.Intn(3) + 1
		for i := r; i < r; i++ {
			b = b.Union("all", randSimpleQuery(allowLimit))
		}
	}

	if allowLimit {
		b = randLimit(b)
	}

	return b
}

func randSimpleQuery(allowLimit bool) *Builder {
	b := Dialect(randDialect()).Select(randSelects()...).From(randTableName(0)).PK("id")
	b = randJoin(b, 3)
	b = b.Where(randCond(b.selects, 3))
	if allowLimit {
		b = randLimit(b)
	}

	return b
}

func randDialect() string {
	dialects := []string{MYSQL, ORACLE, MSSQL, SQLITE, POSTGRES}

	return dialects[rand.Intn(len(dialects))]
}

func randSelects() []string {
	selects := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}

	if rand.Intn(1000) > 900 {
		return []string{"*"}
	}

	rdx := rand.Intn(len(selects) / 2)
	return selects[rdx:]
}

func randTableName(offset int) string {
	return fmt.Sprintf("table%v", rand.Intn(10)+offset)
}

func randJoin(b *Builder, lessThan int) *Builder {
	if lessThan <= 0 {
		return b
	}

	times := rand.Intn(lessThan)

	for i := 0; i < times; i++ {
		tableName := randTableName(i * 10)
		b = b.Join("", tableName, fmt.Sprintf("%v.id = %v.id", b.TableName(), tableName))
	}

	return b
}

func randCond(selects []string, lessThan int) Cond {
	if len(selects) <= 0 {
		return nil
	}

	cond := NewCond()

	times := rand.Intn(lessThan)
	for i := 0; i < times; i++ {
		cond = cond.And(Eq{selects[rand.Intn(len(selects))]: "expected"})
	}

	return cond
}

func randLimit(b *Builder) *Builder {
	r := rand.Intn(1000) + 1
	if r > 500 {
		return b.Limit(r, 1000)
	} else {
		return b.Limit(r)
	}
}
