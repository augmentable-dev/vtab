package main

import (
	"fmt"
	"io"

	"github.com/augmentable-dev/vtab"
	"go.riyazali.net/sqlite"
)

type Iter struct { //implements iter and row interface
	current int
	total   int
}

func (i *Iter) Column(c int) (interface{}, error) {
	switch c {
	case 0:
		return fmt.Sprintf("hello, world x=%d", i.current), nil
	case 1:
		return i.total, nil
	}

	return nil, fmt.Errorf("unknown column")
}

func (i *Iter) Next() (vtab.Row, error) {
	i.current += 1
	if i.current > i.total {
		return nil, io.EOF
	}
	return i, nil
}

var cols = []vtab.Column{
	{"message", sqlite.SQLITE_TEXT, false, false, nil, vtab.NONE},
	{"times", sqlite.SQLITE_INTEGER, false, true, []sqlite.ConstraintOp{sqlite.INDEX_CONSTRAINT_EQ}, vtab.NONE}, //what does this constraint do
}

func init() {
	m := vtab.NewTableFunc("helloworld", cols, func(constraints []vtab.Constraint, order []*sqlite.OrderBy) (vtab.Iterator, error) {
		// defaults
		total := 10

		// override defaults based on any equality constraints (arguments to the table valued func) for user supplied parameters.
		for _, constraint := range constraints {
			if constraint.Op == sqlite.INDEX_CONSTRAINT_EQ {
				switch constraint.ColIndex {
				case 1:
					total = constraint.Value.Int()
				}
			}
		}

		return &Iter{0, total}, nil
	})

	sqlite.Register(func(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
		if err := api.CreateModule("helloworld", m,
			sqlite.EponymousOnly(true), sqlite.ReadOnly(true)); err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		return sqlite.SQLITE_OK, nil
	})
}

func main() {}
