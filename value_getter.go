package vtab

import "go.riyazali.net/sqlite"

// valueGetter implements the Context interface as a (hacky?) way to store
// a value that's returned as a Column value
type valueGetter struct{ value interface{} }

func (vg *valueGetter) ResultInt(v int)               { vg.value = v }
func (vg *valueGetter) ResultInt64(v int64)           { vg.value = v }
func (vg *valueGetter) ResultFloat(v float64)         { vg.value = v }
func (vg *valueGetter) ResultNull()                   { vg.value = nil }
func (vg *valueGetter) ResultValue(v sqlite.Value)    { vg.value = v }
func (vg *valueGetter) ResultZeroBlob(n int64)        { vg.value = n }
func (vg *valueGetter) ResultText(v string)           { vg.value = v }
func (vg *valueGetter) ResultError(err error)         { vg.value = err }
func (vg *valueGetter) ResultPointer(val interface{}) { vg.value = val }
