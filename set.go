package builder

type Set interface {
	Cond
	IsSet() bool
}

var (
	_ Set = Eq{}
	_ Set = expr{}
)

func (eq Eq) IsSet() bool {
	return true
}

func (expr expr) IsSet() bool {
	return true
}
