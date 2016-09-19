package builder

import (
	"errors"
	"fmt"
	"strings"
)

type condNotIn condIn

var _ Cond = condNotIn{}

func NotIn(col string, values ...interface{}) Cond {
	return condNotIn{col, values}
}

func (condNotIn condNotIn) WriteTo(w Writer) error {
	if len(condNotIn.vals) <= 0 {
		return errors.New("No in conditions")
	}

	switch condNotIn.vals[0].(type) {
	case []int8:
		vals := condNotIn.vals[0].([]int8)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s NOT IN (%s)", condNotIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int16:
		vals := condNotIn.vals[0].([]int16)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s NOT IN (%s)", condNotIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int:
		vals := condNotIn.vals[0].([]int)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s NOT IN (%s)", condNotIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int32:
		vals := condNotIn.vals[0].([]int32)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s NOT IN (%s)", condNotIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []string:
		vals := condNotIn.vals[0].([]string)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s NOT IN (%s)", condNotIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int64:
		vals := condNotIn.vals[0].([]int64)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s NOT IN (%s)", condNotIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case expr:
		val := condNotIn.vals[0].(expr)
		if _, err := fmt.Fprintf(w, "%s NOT IN (", condNotIn.col); err != nil {
			return err
		}
		if err := val.WriteTo(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, ")"); err != nil {
			return err
		}
	default:
		questionMark := strings.Repeat("?,", len(condNotIn.vals))
		if _, err := fmt.Fprintf(w, "%s NOT IN (%s)", condNotIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		w.Append(condNotIn.vals...)
	}
	return nil
}

func (condNotIn condNotIn) And(conds ...Cond) Cond {
	return And(condNotIn, And(conds...))
}

func (condNotIn condNotIn) Or(conds ...Cond) Cond {
	return Or(condNotIn, Or(conds...))
}

func (condNotIn condNotIn) IsValid() bool {
	return len(condNotIn.col) > 0 && len(condNotIn.vals) > 0
}
