package vtab

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/template"

	"go.riyazali.net/sqlite"
)

type ColumnType int

const (
	NULL = iota
	INTEGER
	REAL
	TEXT
	BLOB
)

func (t ColumnType) String() string {
	return []string{"NULL", "INTEGER", "REAL", "TEXT", "BLOB"}[t]
}

type Column struct {
	Name    string
	Type    ColumnType
	NotNull bool
	Hidden  bool
	Filters []sqlite.ConstraintOp
	// OrderBy bool
}

type GetIteratorFunc func(arguments []sqlite.Value) (Iterator, error)

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
	Column(int) (interface{}, error)
}

// createTableSQL produces the SQL to declare a new virtual table
func (m *tableFuncModule) createTableSQL() (string, error) {
	// TODO needs to support WITHOUT ROWID, PRIMARY KEY, NOT NULL
	const declare = `CREATE TABLE {{ .Name }} (
  {{- range $c, $col := .Columns }}
    {{ .Name }} {{ .Type }}{{ if .Hidden }} HIDDEN{{ end }}{{ if columnComma $c }},{{ end }}
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

func (t *tableFuncTable) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	// start with a relatively high cost
	cost := 1000.0
	usage := make([]*sqlite.ConstraintUsage, len(input.Constraints))
	idxNameParts := make([]string, 0)

	// iterate over constraints
	usedCsts := 0
	for cst, constraint := range input.Constraints {
		usage[cst] = &sqlite.ConstraintUsage{}

		// ignore unusable
		if !constraint.Usable {
			continue
		}

		// iterate over the declared constraints the column supports
		col := t.columns[constraint.ColumnIndex]
		for f, filter := range col.Filters {
			// if there's a match, reduce the cost (to prefer usage of this constraint)
			if filter == constraint.Op {
				cost -= float64(100*f + 1)
				usedCsts++
				usage[cst].ArgvIndex = usedCsts
				idxNameParts = append(idxNameParts, fmt.Sprintf("%s-%v", col.Name, filter))
			}
		}
	}

	return &sqlite.IndexInfoOutput{
		EstimatedCost:   cost,
		IndexString:     strings.Join(idxNameParts, ","),
		ConstraintUsage: usage,
	}, nil
}

func (t *tableFuncTable) Disconnect() error {
	return t.Destroy()
}

func (t *tableFuncTable) Destroy() error { return nil }

func (c *tableFuncCursor) Filter(idxNum int, idxName string, values ...sqlite.Value) error {
	iter, err := c.getIterator(values)
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
	val, err := c.current.Column(col)
	if err != nil {
		return err
	}

	if col < len(c.columns) {
		if val == nil {
			ctx.ResultNull()
			return nil
		}
		switch c.columns[col].Type {
		case INTEGER:
			ctx.ResultInt(val.(int))
		}
	} else {
		return fmt.Errorf("unexpected column")
	}

	return nil
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
