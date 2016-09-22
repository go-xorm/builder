package builder

import (
	"errors"
	"fmt"
)

func (b *Builder) deleteToSQL() (string, []interface{}, error) {
	if len(b.tableName) <= 0 {
		return "", nil, errors.New("no table indicated")
	}

	w := NewWriter()
	if _, err := fmt.Fprintf(w, "DELETE FROM %s WHERE ", b.tableName); err != nil {
		return "", nil, err
	}

	err := b.cond.WriteTo(w)
	if err != nil {
		return "", nil, err
	}

	return w.writer.String(), w.args, nil
}
