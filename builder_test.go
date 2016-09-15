package builder

import "testing"
import "reflect"

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
	}

	for _, k := range cases {
		sql, args, err := k.cond.ToSQL()
		if err != nil {
			t.Error(err)
			return
		}
		if sql != k.sql {
			t.Error("want", k.sql, "get", sql)
			return
		}
		if !reflect.DeepEqual(args, k.args) {
			t.Error("want", k.args, "get", args)
			return
		}
	}
}
