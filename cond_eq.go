package builder

import "fmt"

type Eq map[string]interface{}

var _ Cond = Eq{}

func (eq Eq) WriteTo(w Writer) error {
	var args = make([]interface{}, 0, len(eq))
	var i = 0
	for k, v := range eq {
		switch v.(type) {
		case []int, []int64, []string, []int32, []int16, []int8:
			if err := In(k, v).WriteTo(w); err != nil {
				return err
			}
		case expr:
			if _, err := fmt.Fprintf(w, "%s=(", k); err != nil {
				return err
			}

			if err := v.(expr).WriteTo(w); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(w, ")"); err != nil {
				return err
			}

		default:
			if _, err := fmt.Fprintf(w, "%s=?", k); err != nil {
				return err
			}
			args = append(args, v)
		}
		if i != len(eq)-1 {
			if _, err := fmt.Fprint(w, " AND "); err != nil {
				return err
			}
		}
		i = i + 1
	}
	w.Append(args...)
	return nil
}

func (eq Eq) And(conds ...Cond) Cond {
	return And(eq, And(conds...))
}

func (eq Eq) Or(conds ...Cond) Cond {
	return Or(eq, Or(conds...))
}

func (eq Eq) IsValid() bool {
	return len(eq) > 0
}
