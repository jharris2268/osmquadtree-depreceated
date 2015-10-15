// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package sqlselect

import (
	"fmt"
	//"errors"
	"strings"
)

type valuesTable struct {
	rows []simpleRow
	cols []Rower
}

func (vt *valuesTable) Len() int                      { return len(vt.rows) }
func (vt *valuesTable) Row(i int) Row                 { return vt.rows[i] }
func (vt *valuesTable) Result(Tables) (Result, error) { return vt, nil }
func (vt *valuesTable) Columns() []Rower              { return vt.cols }
func (vt *valuesTable) String() string {
	return fmt.Sprintf("Values{%d rows:\n%s %v}", vt.Len(), vt.rows[0].RowString(), []simpleRow(vt.rows))
}

type simpleRow []keyValuePair

func (sr simpleRow) Len() int          { return len(sr) }
func (sr simpleRow) Key(i int) string  { return sr[i].key }
func (sr simpleRow) Value(i int) Value { return sr[i].value }
func (sr simpleRow) Index(k string) int {
	for i, c := range sr {
		if k == c.key {
			return i
		}
	}
	return -1
}
func (sr simpleRow) RowString() string {
	ss := make([]string, len(sr))
	for i, v := range sr {
		ss[i] = v.key
	}
	return fmt.Sprintf("(%s)", strings.Join(ss, ", "))
}
func (sr simpleRow) String() string {
	ss := make([]string, len(sr))
	for i, v := range sr {
		ss[i] = v.value.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(ss, ", "))
}

type NullValue struct {
	F interface{}
}

func (nv NullValue) Type() string     { return "NULL" }
func (nv NullValue) String() string   { return "NULL" }
func (nv NullValue) AsString() string { return "" }
func (nv NullValue) AsInt() int64     { return 0 }
func (nv NullValue) AsFloat() float64 { return 0.0 }
func (nv NullValue) IsNull() bool     { return nv.F == nil }
func (nv NullValue) AsBool() bool     { return true }

type stringValue string

func (sv stringValue) Type() string   { return "STRING" }
func (sv stringValue) String() string { return fmt.Sprintf("STRING \"%s\"", sv.AsString()) }
func (sv stringValue) AsString() string {
	if len(sv) < 2 {
		return string(sv)
	}
	if sv[0] == '\'' && sv[len(sv)-1] == '\'' {
		return string(sv[1 : len(sv)-1])
	}
	if sv[0] == '\'' && strings.HasSuffix(string(sv), "'::text") {
		return string(sv)[1 : len(sv)-7]
	}
	return string(sv)
}
func (sv stringValue) AsInt() int64     { return 0 }
func (sv stringValue) AsFloat() float64 { return 0.0 }
func (sv stringValue) IsNull() bool     { return false }
func (sv stringValue) AsBool() bool     { return false }

type intValue int64

func (iv intValue) Type() string     { return "INTEGER" }
func (iv intValue) String() string   { return fmt.Sprintf("INTEGER %d", iv.AsInt()) }
func (iv intValue) AsString() string { return "" }
func (iv intValue) AsInt() int64     { return int64(iv) }
func (iv intValue) AsFloat() float64 { return 0.0 }
func (iv intValue) IsNull() bool     { return false }
func (iv intValue) AsBool() bool     { return false }

type floatValue float64

func (fv floatValue) Type() string     { return "FLOAT" }
func (fv floatValue) String() string   { return fmt.Sprintf("FLOAT %0.5f", fv.AsFloat()) }
func (fv floatValue) AsString() string { return "" }
func (fv floatValue) AsInt() int64     { return 0 }
func (fv floatValue) AsFloat() float64 { return float64(fv) }
func (fv floatValue) IsNull() bool     { return false }
func (fv floatValue) AsBool() bool     { return false }

type boolValue bool

func (fv boolValue) Type() string { return "BOOL" }
func (fv boolValue) String() string {
	if bool(fv) {
		return "BOOL: true"
	}
	return "BOOL: false"
}
func (fv boolValue) AsString() string { return "" }
func (fv boolValue) AsInt() int64     { return 0 }
func (fv boolValue) AsFloat() float64 { return 0.0 }
func (fv boolValue) IsNull() bool     { return false }
func (fv boolValue) AsBool() bool     { return bool(fv) }

type keyValuePair struct {
	key   string
	value Value
}

func StringValue(s string) Value    { return stringValue(s) }
func IntValue(s int64) Value        { return intValue(s) }
func FloatValue(s float64) Value    { return floatValue(s) }
func nullValue() Value              { return &NullValue{nil} }
func GeomValue(f interface{}) Value { return &NullValue{f} }

func (l intValue) Less(r Value) bool {
    if r.Type()=="INTEGER" { return l.AsInt() < r.AsInt() }
    return true
}

func (l floatValue) Less(r Value) bool {
    if r.Type()=="FLOAT" { return l.AsFloat() < r.AsFloat() }
    return true
}


func (l boolValue) Less(r Value) bool {
    if r.Type()=="BOOL" { return !l.AsBool() && r.AsBool() }
    return true
}

func (l stringValue) Less(r Value) bool {
    if r.Type()=="STRING" { return l.AsString() < r.AsString() }
    return true
}

func (*NullValue) Less(v Value) bool {
    return true
}

