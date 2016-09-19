package builder

import (
	"errors"
	"fmt"
	"strings"
)

type condIn struct {
	col  string
	vals []interface{}
}

var _ Cond = condIn{}

func In(col string, values ...interface{}) Cond {
	return condIn{col, values}
}

func (condIn condIn) WriteTo(w Writer) error {
	if len(condIn.vals) <= 0 {
		return errors.New("No in conditions")
	}

	switch condIn.vals[0].(type) {
	case []int8:
		vals := condIn.vals[0].([]int8)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s IN (%s)", condIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int16:
		vals := condIn.vals[0].([]int16)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s IN (%s)", condIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int:
		vals := condIn.vals[0].([]int)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s IN (%s)", condIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int32:
		vals := condIn.vals[0].([]int32)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s IN (%s)", condIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []string:
		vals := condIn.vals[0].([]string)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s IN (%s)", condIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case []int64:
		vals := condIn.vals[0].([]int64)
		questionMark := strings.Repeat("?,", len(vals))
		if _, err := fmt.Fprintf(w, "%s IN (%s)", condIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		for _, val := range vals {
			w.Append(val)
		}
	case expr:
		val := condIn.vals[0].(expr)
		if _, err := fmt.Fprintf(w, "%s IN (", condIn.col); err != nil {
			return err
		}
		if err := val.WriteTo(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, ")"); err != nil {
			return err
		}
	default:
		questionMark := strings.Repeat("?,", len(condIn.vals))
		if _, err := fmt.Fprintf(w, "%s IN (%s)", condIn.col, questionMark[:len(questionMark)-1]); err != nil {
			return err
		}
		w.Append(condIn.vals...)
	}
	return nil
}

func (condIn condIn) And(conds ...Cond) Cond {
	return And(condIn, And(conds...))
}

func (condIn condIn) Or(conds ...Cond) Cond {
	return Or(condIn, Or(conds...))
}

func (condIn condIn) IsValid() bool {
	return len(condIn.col) > 0 && len(condIn.vals) > 0
}
