package vtab

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"text/template"

	"go.riyazali.net/sqlite"
)

func ColTypeSQLString(t sqlite.ColumnType) string {
	switch t {
	case sqlite.SQLITE_INTEGER:
		return "INTEGER"
	case sqlite.SQLITE_FLOAT:
		return "FLOAT"
	case sqlite.SQLITE_TEXT:
		return "TEXT"
	case sqlite.SQLITE_BLOB:
		return "BLOB"
	case sqlite.SQLITE_NULL:
		return "NULL"
	default:
		return "<unknown sqlite datatype>"
	}
}

type Orders uint8

const (
	NONE Orders = iota
	DESC
	ASC
)

type ColumnFilter struct {
	Op        sqlite.ConstraintOp
	Required  bool
	OmitCheck bool
}

type Column struct {
	Name    string
	Type    sqlite.ColumnType
	NotNull bool
	Hidden  bool
	Filters []*ColumnFilter
	OrderBy Orders
}

func (c Column) SQLType() string {
	return ColTypeSQLString(c.Type)
}

type Constraint struct {
	ColIndex int
	Op       sqlite.ConstraintOp
	Value    *sqlite.Value
}

type GetIteratorFunc func(constraints []*Constraint, order []*sqlite.OrderBy) (Iterator, error)

func NewTableFunc(name string, columns []Column, newIterator GetIteratorFunc) sqlite.Module {
	return &tableFuncModule{name, columns, newIterator}
}

type tableFuncModule struct {
	name        string
	columns     []Column
	getIterator GetIteratorFunc
}

type tableFuncTable struct {
	*tableFuncModule
}

type tableFuncCursor struct {
	*tableFuncTable
	iterator Iterator
	count    int
	current  Row
}

type Iterator interface {
	Next() (Row, error)
}

type Row interface {
	Column(ctx *sqlite.Context, col int) error
}

// createTableSQL produces the SQL to declare a new virtual table
func (m *tableFuncModule) createTableSQL() (string, error) {
	// TODO needs to support WITHOUT ROWID, PRIMARY KEY, NOT NULL
	const declare = `CREATE TABLE {{ .Name }} (
  {{- range $c, $col := .Columns }}
    {{ .Name }} {{ .SQLType }}{{ if .Hidden }} HIDDEN{{ end }}{{ if columnComma $c }},{{ end }}
  {{- end }}
)`

	// helper to determine whether we're on the last column (and therefore should avoid a comma ",") in the range
	fns := template.FuncMap{
		"columnComma": func(c int) bool {
			return c < len(m.columns)-1
		},
	}
	tmpl, err := template.New(fmt.Sprintf("declare_table_func_%s", m.name)).Funcs(fns).Parse(declare)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	err = tmpl.Execute(buf, struct {
		Name    string
		Columns []Column
	}{
		m.name,
		m.columns,
	})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (m *tableFuncModule) Connect(_ *sqlite.Conn, _ []string, declare func(string) error) (sqlite.VirtualTable, error) {
	str, err := m.createTableSQL()
	if err != nil {
		return nil, err
	}

	err = declare(str)
	if err != nil {
		return nil, err
	}

	return &tableFuncTable{m}, nil
}

func (m *tableFuncModule) Destroy() error {
	return nil
}

func (t *tableFuncTable) Open() (sqlite.VirtualCursor, error) {
	return &tableFuncCursor{t, nil, 0, nil}, nil
}

type index struct {
	Constraints []*Constraint
	Orders      []*sqlite.OrderBy
}

func (t *tableFuncTable) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	// start with a relatively high cost
	cost := 1000.0
	usage := make([]*sqlite.ConstraintUsage, len(input.Constraints))
	idx := &index{
		Constraints: make([]*Constraint, 0, len(input.Constraints)),
		Orders:      make([]*sqlite.OrderBy, 0),
	}

	orderByUsed := true
	for _, order := range input.OrderBy {
		col := t.columns[order.ColumnIndex]
		if col.OrderBy&ASC != 0 && !order.Desc {
			idx.Orders = append(idx.Orders, order)
			continue
		}
		if col.OrderBy&DESC != 0 && order.Desc {
			idx.Orders = append(idx.Orders, order)
			continue
		}
		orderByUsed = false
	}

	// iterate over constraints
	for cst, constraint := range input.Constraints {
		usage[cst] = &sqlite.ConstraintUsage{}

		if !constraint.Usable {
			return nil, sqlite.SQLITE_CONSTRAINT
		}

		// iterate over the declared constraints the column supports
		col := t.columns[constraint.ColumnIndex]
		for _, filter := range col.Filters {
			// if there's a match, reduce the cost (to prefer usage of this constraint)
			if filter.Op == constraint.Op {
				cost -= 10
				usage[cst].ArgvIndex = len(idx.Constraints) + 1
				usage[cst].Omit = filter.OmitCheck
				idx.Constraints = append(idx.Constraints, &Constraint{
					ColIndex: constraint.ColumnIndex,
					Op:       filter.Op,
				})
			}
		}
	}

	idxStr, err := json.Marshal(idx)
	if err != nil {
		return nil, err
	}

	return &sqlite.IndexInfoOutput{
		EstimatedCost:   cost,
		IndexString:     string(idxStr),
		ConstraintUsage: usage,
		OrderByConsumed: orderByUsed,
	}, nil
}

func (t *tableFuncTable) Disconnect() error {
	return t.Destroy()
}

func (t *tableFuncTable) Destroy() error { return nil }

func (c *tableFuncCursor) Filter(idxNum int, idxName string, values ...sqlite.Value) error {
	var idx index
	err := json.Unmarshal([]byte(idxName), &idx)
	if err != nil {
		return err
	}

	for c := range idx.Constraints {
		idx.Constraints[c].Value = &values[c]
	}

	iter, err := c.getIterator(idx.Constraints, idx.Orders)
	if err != nil {
		return err
	}
	c.iterator = iter

	row, err := iter.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.current = nil
			return nil
		}
		return err
	}

	c.current = row
	return nil
}

func (c *tableFuncCursor) Next() error {
	defer func() { c.count++ }()
	row, err := c.iterator.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.current = nil
			return nil
		}
		return err
	}
	c.current = row
	return nil
}

func (c *tableFuncCursor) Column(ctx *sqlite.Context, col int) error {
	return c.current.Column(ctx, col)
}

func (c *tableFuncCursor) Eof() bool {
	return c.current == nil
}

func (c *tableFuncCursor) Rowid() (int64, error) {
	return int64(c.count), nil
}

func (c *tableFuncCursor) Close() error {
	return nil
}
