package builder

import "fmt"

// Between
type Between struct {
	col     string
	lessVal interface{}
	moreVal interface{}
}

var _ Cond = Between{}

func (between Between) WriteTo(w Writer) error {
	if _, err := fmt.Fprintf(w, "%s BETWEEN ? AND ?", between.col); err != nil {
		return err
	}
	w.Append(between.lessVal, between.moreVal)
	return nil
}

func (between Between) And(conds ...Cond) Cond {
	return And(between, And(conds...))
}

func (between Between) Or(conds ...Cond) Cond {
	return Or(between, Or(conds...))
}

func (between Between) IsValid() bool {
	return len(between.col) > 0
}
