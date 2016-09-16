package builder

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

type Writer interface {
	io.Writer
	Append(...interface{})
}

type stringWriter struct {
	writer *bytes.Buffer
	buffer []byte
	args   []interface{}
}

func NewWriter() *stringWriter {
	w := &stringWriter{}
	w.writer = bytes.NewBuffer(w.buffer)
	return w
}

func (s *stringWriter) Write(buf []byte) (int, error) {
	return s.writer.Write(buf)
}

func (s *stringWriter) Append(args ...interface{}) {
	s.args = append(s.args, args...)
}

func ToSQL(cond Cond) (string, []interface{}, error) {
	w := NewWriter()
	if err := cond.WriteTo(w); err != nil {
		return "", nil, err
	}
	return w.writer.String(), w.args, nil
}

type Cond interface {
	WriteTo(Writer) error
	And(...Cond) Cond
	Or(...Cond) Cond
}

type condAnd []Cond

func And(conds ...Cond) Cond {
	return condAnd(conds)
}

func (and condAnd) WriteTo(w Writer) error {
	for i, cond := range and {
		if _, ok := cond.(condOr); ok {
			fmt.Fprint(w, "(")
		}

		err := cond.WriteTo(w)
		if err != nil {
			return err
		}

		if _, ok := cond.(condOr); ok {
			fmt.Fprint(w, ")")
		}

		if i != len(and)-1 {
			fmt.Fprint(w, " AND ")
		}
	}

	return nil
}

func (and condAnd) And(conds ...Cond) Cond {
	return condAnd(append(and, conds...))
}

func (and condAnd) Or(conds ...Cond) Cond {
	return append(condOr{and}, conds...)
}

type condOr []Cond

func Or(conds ...Cond) Cond {
	return condOr(conds)
}

func (or condOr) WriteTo(w Writer) error {
	for i, cond := range or {
		if _, ok := cond.(condAnd); ok {
			fmt.Fprint(w, "(")
		}

		err := cond.WriteTo(w)
		if err != nil {
			return err
		}

		if _, ok := cond.(condAnd); ok {
			fmt.Fprint(w, ")")
		}

		if i != len(or)-1 {
			fmt.Fprint(w, " OR ")
		}
	}

	return nil
}

func (o condOr) And(conds ...Cond) Cond {
	return append(condAnd{o}, conds...)
}

func (o condOr) Or(conds ...Cond) Cond {
	return append(o, conds...)
}

type expr struct {
	sql  string
	args []interface{}
}

func Expr(sql string, args ...interface{}) Cond {
	return expr{sql, args}
}

func (expr expr) WriteTo(w Writer) error {
	if _, err := fmt.Fprint(w, expr.sql); err != nil {
		return err
	}
	w.Append(expr.args...)
	return nil
}

func (expr expr) And(conds ...Cond) Cond {
	return append(condAnd{expr}, conds...)
}

func (expr expr) Or(conds ...Cond) Cond {
	return append(condOr{expr}, conds...)
}

func WriteMap(w Writer, data map[string]interface{}, op string) error {
	var args = make([]interface{}, 0, len(data))
	var i = 0
	for k, v := range data {
		if _, err := fmt.Fprintf(w, "%s%s?", k, op); err != nil {
			return err
		}
		if i != len(data)-1 {
			if _, err := fmt.Fprint(w, " AND "); err != nil {
				return err
			}
		}
		args = append(args, v)
		i = i + 1
	}
	w.Append(args...)
	return nil
}

type Eq map[string]interface{}

func (eq Eq) WriteTo(w Writer) error {
	var args = make([]interface{}, 0, len(eq))
	var i = 0
	for k, v := range eq {
		switch v.(type) {
		case []int, []int64, []string, []int32, []int16, []int8:
			if err := In(k, v).WriteTo(w); err != nil {
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
	return append(condAnd{eq}, conds...)
}

func (eq Eq) Or(conds ...Cond) Cond {
	return append(condOr{eq}, conds...)
}

type Neq map[string]interface{}

func (neq Neq) WriteTo(w Writer) error {
	var args = make([]interface{}, 0, len(neq))
	var i = 0
	for k, v := range neq {
		switch v.(type) {
		case []int, []int64, []string, []int32, []int16, []int8:
			if err := NotIn(k, v).WriteTo(w); err != nil {
				return err
			}
		default:
			if _, err := fmt.Fprintf(w, "%s<>?", k); err != nil {
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
	return append(condAnd{neq}, conds...)
}

func (neq Neq) Or(conds ...Cond) Cond {
	return append(condOr{neq}, conds...)
}

type Lt map[string]interface{}

func (lt Lt) WriteTo(w Writer) error {
	return WriteMap(w, lt, "<")
}

func (lt Lt) And(conds ...Cond) Cond {
	return append(condAnd{lt}, conds...)
}

func (lt Lt) Or(conds ...Cond) Cond {
	return append(condOr{lt}, conds...)
}

type Lte map[string]interface{}

func (lte Lte) WriteTo(w Writer) error {
	return WriteMap(w, lte, "<=")
}

func (lte Lte) And(conds ...Cond) Cond {
	return append(condAnd{lte}, conds...)
}

func (lte Lte) Or(conds ...Cond) Cond {
	return append(condOr{lte}, conds...)
}

type Gt map[string]interface{}

func (gt Gt) WriteTo(w Writer) error {
	return WriteMap(w, gt, ">")
}

func (gt Gt) And(conds ...Cond) Cond {
	return append(condAnd{gt}, conds...)
}

func (gt Gt) Or(conds ...Cond) Cond {
	return append(condOr{gt}, conds...)
}

type Gte map[string]interface{}

func (gte Gte) WriteTo(w Writer) error {
	return WriteMap(w, gte, ">=")
}

func (gte Gte) And(conds ...Cond) Cond {
	return append(condAnd{gte}, conds...)
}

func (gte Gte) Or(conds ...Cond) Cond {
	return append(condOr{gte}, conds...)
}

type Between struct {
	col     string
	lessVal interface{}
	moreVal interface{}
}

func (between Between) WriteTo(w Writer) error {
	if _, err := fmt.Fprintf(w, "%s BETWEEN ? AND ?", between.col); err != nil {
		return err
	}
	w.Append(between.lessVal, between.moreVal)
	return nil
}

func (between Between) And(conds ...Cond) Cond {
	return append(condAnd{between}, conds...)
}

func (between Between) Or(conds ...Cond) Cond {
	return append(condOr{between}, conds...)
}

type condIn struct {
	col  string
	vals []interface{}
}

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
	return append(condAnd{condIn}, conds...)
}

func (condIn condIn) Or(conds ...Cond) Cond {
	return append(condOr{condIn}, conds...)
}

type condNotIn condIn

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
	return append(condAnd{condNotIn}, conds...)
}

func (condNotIn condNotIn) Or(conds ...Cond) Cond {
	return append(condOr{condNotIn}, conds...)
}

type Like [2]string

func (like Like) WriteTo(w Writer) error {
	if _, err := fmt.Fprintf(w, "%s LIKE ?", like[0]); err != nil {
		return err
	}
	w.Append("%" + like[1] + "%")
	return nil
}

func (like Like) And(conds ...Cond) Cond {
	return append(condAnd{like}, conds...)
}

func (like Like) Or(conds ...Cond) Cond {
	return append(condOr{like}, conds...)
}

type IsNull [1]string

func (isNull IsNull) WriteTo(w Writer) error {
	_, err := fmt.Fprintf(w, "%s IS NULL", isNull[0])
	return err
}

func (isNull IsNull) And(conds ...Cond) Cond {
	return append(condAnd{isNull}, conds...)
}

func (isNull IsNull) Or(conds ...Cond) Cond {
	return append(condOr{isNull}, conds...)
}

type NotNull [1]string

func (notNull NotNull) WriteTo(w Writer) error {
	_, err := fmt.Fprintf(w, "%s IS NOT NULL", notNull[0])
	return err
}

func (notNull NotNull) And(conds ...Cond) Cond {
	return append(condAnd{notNull}, conds...)
}

func (notNull NotNull) Or(conds ...Cond) Cond {
	return append(condOr{notNull}, conds...)
}

type Not [1]Cond

func (not Not) WriteTo(w Writer) error {
	if _, err := fmt.Fprint(w, "NOT "); err != nil {
		return err
	}
	switch not[0].(type) {
	case condAnd, condOr:
		if _, err := fmt.Fprint(w, "("); err != nil {
			return err
		}
	}

	if err := not[0].WriteTo(w); err != nil {
		return err
	}

	switch not[0].(type) {
	case condAnd, condOr:
		if _, err := fmt.Fprint(w, ")"); err != nil {
			return err
		}
	}

	return nil
}

func (not Not) And(conds ...Cond) Cond {
	return append(condAnd{not}, conds...)
}

func (not Not) Or(conds ...Cond) Cond {
	return append(condOr{not}, conds...)
}
