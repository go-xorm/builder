package builder

import (
	"fmt"
	"reflect"
	"testing"
)

func TestBuilder1(t *testing.T) {
	var cases = []struct {
		cond Cond
		sql  string
		args []interface{}
	}{
		{
			Eq{"a": 1}.And(Like{"b", "c"}).Or(Eq{"a": 2}.And(Like{"b", "g"})),
			"(a=? AND b LIKE ?) OR (a=? AND b LIKE ?)",
			[]interface{}{1, "%c%", 2, "%g%"},
		},
		{
			Eq{"a": 1}.Or(Like{"b", "c"}).And(Eq{"a": 2}.Or(Like{"b", "g"})),
			"(a=? OR b LIKE ?) AND (a=? OR b LIKE ?)",
			[]interface{}{1, "%c%", 2, "%g%"},
		},
		{
			Eq{"d": []string{"e", "f"}},
			"d IN (?,?)",
			[]interface{}{"e", "f"},
		},
		{
			Neq{"d": []string{"e", "f"}},
			"d NOT IN (?,?)",
			[]interface{}{"e", "f"},
		},
		{
			Lt{"d": 3},
			"d<?",
			[]interface{}{3},
		},
		{
			Lte{"d": 3},
			"d<=?",
			[]interface{}{3},
		},
		{
			Gt{"d": 3},
			"d>?",
			[]interface{}{3},
		},
		{
			Gte{"d": 3},
			"d>=?",
			[]interface{}{3},
		},
		{
			Between{"d", 0, 2},
			"d BETWEEN ? AND ?",
			[]interface{}{0, 2},
		},
		{
			IsNull{"d"},
			"d IS NULL",
			[]interface{}{},
		},
		{
			NotIn("a", 1, 2).And(NotIn("b", "c", "d")),
			"a NOT IN (?,?) AND b NOT IN (?,?)",
			[]interface{}{1, 2, "c", "d"},
		},
		{
			In("a", 1, 2).Or(In("b", "c", "d")),
			"a IN (?,?) OR b IN (?,?)",
			[]interface{}{1, 2, "c", "d"},
		},
		{
			In("a", []int{1, 2}).Or(In("b", []string{"c", "d"})),
			"a IN (?,?) OR b IN (?,?)",
			[]interface{}{1, 2, "c", "d"},
		},
		{
			In("a", Expr("select id from x where name > ?", "b")),
			"a IN (select id from x where name > ?)",
			[]interface{}{"b"},
		},
		{
			NotIn("a", Expr("select id from x where name > ?", "b")),
			"a NOT IN (select id from x where name > ?)",
			[]interface{}{"b"},
		},
	}

	for _, k := range cases {
		sql, args, err := ToSQL(k.cond)
		if err != nil {
			t.Error(err)
			return
		}
		if sql != k.sql {
			t.Error("want", k.sql, "get", sql)
			return
		}
		fmt.Println(sql)

		if !(len(args) == 0 && len(k.args) == 0) {
			if !reflect.DeepEqual(args, k.args) {
				t.Error("want", k.args, "get", args)
				return
			}
		}
		fmt.Println(args)
	}
}
