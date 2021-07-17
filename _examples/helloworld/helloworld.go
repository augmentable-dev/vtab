package main

import (
	"fmt"
	"io"

	"github.com/augmentable-dev/vtab"
	"go.riyazali.net/sqlite"
)

type Iter struct {
	current int
	total   int
	name    string
}

func (i *Iter) Column(ctx *sqlite.Context, c int) error {
	switch c {
	case 0:
		ctx.ResultText(fmt.Sprintf("hello, world x=%d, name=%s", i.current, i.name))
	case 1:
		ctx.ResultInt(i.total)
	case 2:
		ctx.ResultText(i.name)
	default:
		return fmt.Errorf("unknown column")
	}
	return nil
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
	{"times", sqlite.SQLITE_INTEGER, false, true, []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}, vtab.NONE},
	{"name", sqlite.SQLITE_TEXT, false, true, []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}, vtab.NONE},
}

func init() {
	m := vtab.NewTableFunc("helloworld", cols, func(constraints []*vtab.Constraint, order []*sqlite.OrderBy) (vtab.Iterator, error) {
		// defaults
		total := 10
		name := ""

		// override defaults based on any equality constraints (arguments to the table valued func)
		for _, constraint := range constraints {
			if constraint.Op == sqlite.INDEX_CONSTRAINT_EQ {
				switch constraint.ColIndex {
				case 1:
					total = constraint.Value.Int()
				case 2:
					name = constraint.Value.Text()
				}
			}
		}

		return &Iter{0, total, name}, nil
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
