package builder

import (
	"errors"
	"fmt"
)

func (b *Builder) updateToSQL() (string, []interface{}, error) {
	if len(b.tableName) <= 0 {
		return "", nil, errors.New("no table indicated")
	}
	if len(b.updates) <= 0 {
		return "", nil, errors.New("no column to be update")
	}

	w := NewWriter()
	if _, err := fmt.Fprintf(w, "UPDATE %s SET ", b.tableName); err != nil {
		return "", nil, err
	}

	for i, s := range b.updates {
		if err := s.WriteTo(w); err != nil {
			return "", nil, err
		}

		if i != len(b.updates)-1 {
			if _, err := fmt.Fprint(w, ","); err != nil {
				return "", nil, err
			}
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
