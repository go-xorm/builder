# sql builder

```Go
cond1 := Eq{"a": 1}.And(Like{"b", "c"})
cond2 := Eq{"a": 2}.And(Like{"b", "g"})
sql, args, err := ToSQL(cond1.Or(cond2))
if err != nil {
	return err
}
```