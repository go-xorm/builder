package builder

import (
	"errors"
	"strings"
)

type optype byte

const (
	condType   optype = iota // only conditions
	selectType               // select
	insertType               // insert
	updateType               // update
	deleteType               // delete
)

type Builder struct {
	optype
	tableName string
	cols      []string
	cond      Cond
}

func From(tableName string) *Builder {
	return &Builder{tableName: tableName}
}

func (b *Builder) Where(cond Cond) *Builder {
	b.cond = cond
	return b
}

func (b *Builder) Select(cols ...string) *Builder {
	b.cols = cols
	b.optype = selectType
	return b
}

func (b *Builder) And(cond Cond) *Builder {
	b.cond = And(b.cond, cond)
	return b
}

func (b *Builder) Or(cond Cond) *Builder {
	b.cond = Or(b.cond, cond)
	return b
}

func (b *Builder) ToSQL() (string, []interface{}, error) {
	switch b.optype {
	case condType:
		return ToSQL(b.cond)
	case selectType:
		if len(b.tableName) <= 0 {
			return "", nil, errors.New("no table indicated")
		}
		sql, args, err := ToSQL(b.cond)
		if err != nil {
			return "", nil, err
		}
		var colString = "*"
		if len(b.cols) > 0 {
			colString = strings.Join(b.cols, ",")
		}
		return "SELECT " + colString + " FROM " + sql, args, nil
	}

	return "", nil, errors.New("not supported SQL type")
}
