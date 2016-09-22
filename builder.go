package builder

type optype byte

const (
	condType   optype = iota // only conditions
	selectType               // select
	insertType               // insert
	updateType               // update
	deleteType               // delete
)

type join struct {
	joinType  string
	joinTable string
	joinCond  Cond
}

type Builder struct {
	optype
	tableName string
	cond      Cond
	selects   []string
	joins     []join
	inserts   Eq
	updates   []Set
}

func Select(cols ...string) *Builder {
	builder := &Builder{cond: NewCond()}
	return builder.Select(cols...)
}

func Insert(eq Eq) *Builder {
	builder := &Builder{cond: NewCond()}
	return builder.Insert(eq)
}

func Update(updates ...Set) *Builder {
	builder := &Builder{cond: NewCond()}
	return builder.Update(updates...)
}

func Delete(conds ...Cond) *Builder {
	builder := &Builder{cond: NewCond()}
	return builder.Delete(conds...)
}

func (b *Builder) Where(cond Cond) *Builder {
	b.cond = b.cond.And(cond)
	return b
}

func (b *Builder) From(tableName string) *Builder {
	b.tableName = tableName
	return b
}

func (b *Builder) Into(tableName string) *Builder {
	b.tableName = tableName
	return b
}

func (b *Builder) Join(joinType, joinTable string, joinCond interface{}) *Builder {
	switch joinCond.(type) {
	case Cond:
		b.joins = append(b.joins, join{joinType, joinTable, joinCond.(Cond)})
	case string:
		b.joins = append(b.joins, join{joinType, joinTable, Expr(joinCond.(string))})
	}

	return b
}

func (b *Builder) InnerJoin(joinTable string, joinCond interface{}) *Builder {
	return b.Join("INNER", joinTable, joinCond)
}

func (b *Builder) LeftJoin(joinTable string, joinCond interface{}) *Builder {
	return b.Join("LEFT", joinTable, joinCond)
}

func (b *Builder) RightJoin(joinTable string, joinCond interface{}) *Builder {
	return b.Join("RIGHT", joinTable, joinCond)
}

func (b *Builder) CrossJoin(joinTable string, joinCond interface{}) *Builder {
	return b.Join("CROSS", joinTable, joinCond)
}

func (b *Builder) FullJoin(joinTable string, joinCond interface{}) *Builder {
	return b.Join("FULL", joinTable, joinCond)
}

func (b *Builder) Select(cols ...string) *Builder {
	b.selects = cols
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

func (b *Builder) Insert(eq Eq) *Builder {
	b.inserts = eq
	b.optype = insertType
	return b
}

func (b *Builder) Update(updates ...Set) *Builder {
	b.updates = updates
	b.optype = updateType
	return b
}

func (b *Builder) Delete(conds ...Cond) *Builder {
	b.cond = b.cond.And(conds...)
	b.optype = deleteType
	return b
}

// ToSQL convert a builder to SQL and args
func (b *Builder) ToSQL() (string, []interface{}, error) {
	switch b.optype {
	case condType:
		return condToSQL(b.cond)
	case selectType:
		return b.selectToSQL()
	case insertType:
		return b.insertToSQL()
	case updateType:
		return b.updateToSQL()
	case deleteType:
		return b.deleteToSQL()
	}

	return "", nil, ErrNotSupportType
}

// ToSQL convert a builder or condtions to SQL and args
func ToSQL(cond interface{}) (string, []interface{}, error) {
	switch cond.(type) {
	case Cond:
		return condToSQL(cond.(Cond))
	case *Builder:
		return cond.(*Builder).ToSQL()
	}
	return "", nil, ErrNotSupportType
}
