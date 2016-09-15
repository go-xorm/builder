package builder

import (
	"fmt"
	"testing"
)

func TestBuilder1(t *testing.T) {
	cond1 := Eq{"a": 1}.And(Like{"b", "c"})
	cond2 := Eq{"a": 2}.And(Like{"b", "g"})
	sql, args, err := cond1.Or(cond2).ToSQL()
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(sql)
	fmt.Println(args)
}

func TestBuilder2(t *testing.T) {
	sql, args, err := Where(Eq{"id": 1}).And(Eq{"b": 2}).ToSQL()
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(sql)
	fmt.Println(args)
}
