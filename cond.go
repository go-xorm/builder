package builder

import (
	"errors"
	"strings"
)

type Cond interface {
	ToSQL() (string, []interface{}, error)
	And(...Cond) Cond
	Or(...Cond) Cond
}

type condAnd []Cond

func And(conds ...Cond) Cond {
	return condAnd(conds)
}

func (and condAnd) ToSQL() (string, []interface{}, error) {
	var sqls = make([]string, 0, len(and))
	var args = make([]interface{}, 0, 2*len(and))

	for _, cond := range and {
		sql, args1, err := cond.ToSQL()
		if err != nil {
			return "", nil, err
		}

		switch cond.(type) {
		case condOr:
			sqls = append(sqls, "("+sql+")")
		default:
			sqls = append(sqls, sql)
		}

		args = append(args, args1...)
	}

	return strings.Join(sqls, " AND "), args, nil
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

func (or condOr) ToSQL() (string, []interface{}, error) {
	var sqls = make([]string, 0, len(or))
	var args = make([]interface{}, 0, 2*len(or))

	for _, cond := range or {
		sql, args1, err := cond.ToSQL()
		if err != nil {
			return "", nil, err
		}

		switch cond.(type) {
		case condAnd:
			sqls = append(sqls, "("+sql+")")
		default:
			sqls = append(sqls, sql)
		}

		args = append(args, args1...)
	}

	return strings.Join(sqls, " OR "), args, nil
}

func (o condOr) And(conds ...Cond) Cond {
	return append(condAnd{o}, conds...)
}

func (o condOr) Or(conds ...Cond) Cond {
	return append(o, conds...)
}

type Expr string

func (expr Expr) ToSQL() (string, []interface{}, error) {
	return string(expr), nil, nil
}

func (expr Expr) And(conds ...Cond) Cond {
	return append(condAnd{expr}, conds...)
}

func (expr Expr) Or(conds ...Cond) Cond {
	return append(condOr{expr}, conds...)
}

type Eq map[string]interface{}

func (eq Eq) ToSQL() (string, []interface{}, error) {
	var conds []string
	var args = make([]interface{}, 0, len(eq))
	for k, v := range eq {
		conds = append(conds, k+"=?")
		args = append(args, v)
	}
	return strings.Join(conds, " AND "), args, nil
}

func (eq Eq) And(conds ...Cond) Cond {
	return append(condAnd{eq}, conds...)
}

func (eq Eq) Or(conds ...Cond) Cond {
	return append(condOr{eq}, conds...)
}

type Neq map[string]interface{}

func (neq Neq) ToSQL() (string, []interface{}, error) {
	var conds []string
	var args = make([]interface{}, 0, len(neq))
	for k, v := range neq {
		conds = append(conds, k+"<>?")
		args = append(args, v)
	}
	return strings.Join(conds, " AND "), args, nil
}

type Lt map[string]interface{}

func (lt Lt) ToSQL() (string, []interface{}, error) {
	var conds []string
	var args = make([]interface{}, 0, len(lt))
	for k, v := range lt {
		conds = append(conds, k+"<?")
		args = append(args, v)
	}
	return strings.Join(conds, " AND "), args, nil
}

type Gt map[string]interface{}

func (gt Gt) ToSQL() (string, []interface{}, error) {
	var conds []string
	var args = make([]interface{}, 0, len(gt))
	for k, v := range gt {
		conds = append(conds, k+">?")
		args = append(args, v)
	}
	return strings.Join(conds, " AND "), args, nil
}

type condIn struct {
	col  string
	vals []interface{}
}

func In(col string, values ...interface{}) Cond {
	return condIn{col, values}
}

func (condIn condIn) ToSQL() (string, []interface{}, error) {
	if len(condIn.vals) <= 0 {
		return "", nil, errors.New("No in conditions")
	}

	questionMark := strings.Repeat("?,", len(condIn.vals))
	return condIn.col + "IN (" + questionMark[:len(questionMark)-1] + ")", condIn.vals, nil
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

func (condNotIn condNotIn) ToSQL() (string, []interface{}, error) {
	if len(condNotIn.vals) <= 0 {
		return "", nil, errors.New("No in conditions")
	}

	questionMark := strings.Repeat("?,", len(condNotIn.vals))
	return condNotIn.col + "NOT IN (" + questionMark[:len(questionMark)-1] + ")", condNotIn.vals, nil
}

func (condNotIn condNotIn) And(conds ...Cond) Cond {
	return append(condAnd{condNotIn}, conds...)
}

func (condNotIn condNotIn) Or(conds ...Cond) Cond {
	return append(condOr{condNotIn}, conds...)
}

type Like [2]string

func (like Like) ToSQL() (string, []interface{}, error) {
	return like[0] + " LIKE ?", []interface{}{"%" + like[1] + "%"}, nil
}

func (like Like) And(conds ...Cond) Cond {
	return append(condAnd{like}, conds...)
}

func (like Like) Or(conds ...Cond) Cond {
	return append(condOr{like}, conds...)
}

type IsNull [1]string

func (isNull IsNull) ToSQL() (string, []interface{}, error) {
	return string(isNull[0]) + " IS NULL", []interface{}{}, nil
}

func (isNull IsNull) And(conds ...Cond) Cond {
	return append(condAnd{isNull}, conds...)
}

func (isNull IsNull) Or(conds ...Cond) Cond {
	return append(condOr{isNull}, conds...)
}

type NotNull [1]string

func (notNull NotNull) ToSQL() (string, []interface{}, error) {
	return notNull[0] + " IS NOT NULL", []interface{}{}, nil
}

func (notNull NotNull) And(conds ...Cond) Cond {
	return append(condAnd{notNull}, conds...)
}

func (notNull NotNull) Or(conds ...Cond) Cond {
	return append(condOr{notNull}, conds...)
}

type Not [1]Cond

func (not Not) ToSQL() (string, []interface{}, error) {
	sql, args, err := not[0].ToSQL()
	if err != nil {
		return "", nil, err
	}
	switch not[0].(type) {
	case condAnd, condOr:
		return "NOT (" + sql + ")", args, nil
	}
	return " NOT " + sql, args, nil
}

func (not Not) And(conds ...Cond) Cond {
	return append(condAnd{not}, conds...)
}

func (not Not) Or(conds ...Cond) Cond {
	return append(condOr{not}, conds...)
}
