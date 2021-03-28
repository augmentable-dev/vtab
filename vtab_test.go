package vtab

import (
	"testing"
)

func TestCreateTableSQL(t *testing.T) {
	m := &tableFuncModule{
		name: "test_table",
		columns: []Column{
			{"test_one", TEXT, false, false, nil},
			{"test_two", INTEGER, false, false, nil},
			{"test_three", BLOB, false, false, nil},
			{"test_arg", TEXT, false, true, nil},
		},
	}

	str, err := m.createTableSQL()
	if err != nil {
		t.Fatal(err)
	}

	want := `CREATE TABLE test_table (
    test_one TEXT,
    test_two INTEGER,
    test_three BLOB,
    test_arg TEXT HIDDEN
)`

	if str != want {
		t.Fatalf("wanted: %s, got: %s", want, str)
	}
}
