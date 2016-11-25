package builder

import "fmt"

type Neq map[string]interface{}

var _ Cond = Neq{}

func (neq Neq) WriteTo(w Writer) error {
	var args = make([]interface{}, 0, len(neq))
	var i = 0
	for k, v := range neq {
		switch v.(type) {
		case []int, []int64, []string, []int32, []int16, []int8:
			if err := NotIn(k, v).WriteTo(w); err != nil {
				return err
			}
		case expr:
			if _, err := fmt.Fprintf(w, "`%s`<>(", k); err != nil {
				return err
			}

			if err := v.(expr).WriteTo(w); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(w, ")"); err != nil {
				return err
			}
		case *Builder:
			if _, err := fmt.Fprintf(w, "`%s`<>(", k); err != nil {
				return err
			}

			if err := v.(*Builder).WriteTo(w); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(w, ")"); err != nil {
				return err
			}
		default:
			if _, err := fmt.Fprintf(w, "`%s`<>?", k); err != nil {
				return err
			}
			args = append(args, v)
		}
		if i != len(neq)-1 {
			if _, err := fmt.Fprint(w, " AND "); err != nil {
				return err
			}
		}
		i = i + 1
	}
	w.Append(args...)
	return nil
}

func (neq Neq) And(conds ...Cond) Cond {
	return And(neq, And(conds...))
}

func (neq Neq) Or(conds ...Cond) Cond {
	return Or(neq, Or(conds...))
}

func (neq Neq) IsValid() bool {
	return len(neq) > 0
}
