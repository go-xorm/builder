package builder

import (
	"errors"
	"fmt"
)

func (b *Builder) unionWriteTo(w Writer) error {
	for idx, v := range b.unions {
		if v.builder.optype != selectType {
			return errors.New("UNION is only allowed among SELECT operations")
		}
		if len(b.unions) == 1 {
			if err := v.builder.selectWriteTo(w); err != nil {
				return err
			}
		} else {
			if idx != 0 {
				fmt.Fprint(w, fmt.Sprintf(" union %v ", v.unionType))
			}
			fmt.Fprint(w, "(")
			if err := v.builder.selectWriteTo(w); err != nil {
				return err
			}
			fmt.Fprint(w, ")")
		}
	}
	return nil
}
