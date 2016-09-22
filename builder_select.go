package builder

import (
	"errors"
	"fmt"
)

func (b *Builder) selectToSQL() (string, []interface{}, error) {
	if len(b.tableName) <= 0 {
		return "", nil, errors.New("no table indicated")
	}

	w := NewWriter()
	if _, err := fmt.Fprint(w, "SELECT "); err != nil {
		return "", nil, err
	}
	if len(b.selects) > 0 {
		for i, s := range b.selects {
			if _, err := fmt.Fprint(w, s); err != nil {
				return "", nil, err
			}
			if i != len(b.selects)-1 {
				if _, err := fmt.Fprint(w, ","); err != nil {
					return "", nil, err
				}
			}
		}
	} else {
		if _, err := fmt.Fprint(w, "*"); err != nil {
			return "", nil, err
		}
	}

	if _, err := fmt.Fprintf(w, " FROM %s", b.tableName); err != nil {
		return "", nil, err
	}

	for _, v := range b.joins {
		fmt.Fprintf(w, " %s JOIN %s ON ", v.joinType, v.joinTable)
		if err := v.joinCond.WriteTo(w); err != nil {
			return "", nil, err
		}
	}

	if _, err := fmt.Fprint(w, " WHERE "); err != nil {
		return "", nil, err
	}

	err := b.cond.WriteTo(w)
	if err != nil {
		return "", nil, err
	}

	return w.writer.String(), w.args, nil
}
