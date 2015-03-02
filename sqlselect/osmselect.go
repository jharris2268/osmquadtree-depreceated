// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package sqlselect

import (

	"fmt"
	"strconv"
)

type Result interface {
	Len() int
	Row(int) Row
	Columns() []Rower
}

type Value interface {
	Type() string
	String() string
	AsString() string
	AsInt() int64
	AsFloat() float64
	AsBool() bool
	IsNull() bool
}

type Row interface {
	Len() int
	Key(int) string
	Value(int) Value
	Index(string) int
}

type Tables map[string]Result

type Tabler interface {
	String() string
	Result(Tables) (Result, error)
}
type Wherer interface {
	String() string
	Where(Row) bool
}

type Rower interface {
	String() string
	Key() string
	Value(row Row) Value
}

type Orderer interface {
	String() string
	Less(Row, Row) bool
	Columns() []Rower
}

type order struct {
	rower Rower
	direc string
}

func (oo *order) Less(r1, r2 Row) bool {
	v1 := oo.rower.Value(r1)
	v2 := oo.rower.Value(r2)

	//println(fmt.Sprintf("%s %s %t %s %s %s %t",oo.rower,r1,r1==r2,v1,v2,oo.direc,valCompOp(v1,v2,"<")))
	if oo.direc == "ASC" {
		return valCompOp(v1, v2, "<")
	}
	return valCompOp(v1, v2, ">")
}
func (oo *order) String() string {
	return fmt.Sprintf("Order(%s, %s)", oo.rower, oo.direc)
}
func (oo *order) Columns() []Rower {
	return []Rower{oo.rower}
}

type orderList []order

func (oo orderList) Less(r1, r2 Row) bool {
	for i, o := range oo {
		v1 := o.rower.Value(r1)
		v2 := o.rower.Value(r2)
		if i == len(oo)-1 || !valCompOp(v1, v2, "=") {
			return o.Less(r1, r2)
		}
	}
	return false
}
func (ol orderList) String() string {
	return fmt.Sprintf("%v", []order(ol))
}
func (ol orderList) Columns() []Rower {
	cl := make([]Rower, len(ol))
	for i, o := range ol {
		cl[i] = o.rower
	}
	return cl
}

type rowerList []Rower

func makeNumVal(vstr string) Value {
	//println("makeNumVal",vstr)
	ii, err := strconv.ParseInt(vstr, 10, 64)
	if err == nil {
		//println("isint",ii)
		return intValue(ii)
	}

	ff, err := strconv.ParseFloat(vstr, 64)
	if err == nil {
		//println("isfloat",ff)
		return floatValue(ff)
	}
	return nil

}

func makeSimpleRow(ir []Value, cols []Rower) simpleRow {
	res := make(simpleRow, len(ir))
	for i, v := range ir {
		res[i] = keyValuePair{cols[i].Key(), v}
	}
	return res
}

func makeValuesTable(vals [][]Value, cols []Rower) Tabler {
	vt := make([]simpleRow, len(vals))

	//var err error
	for i, v := range vals {
		vt[i] = makeSimpleRow(v, cols)
		/*vt[i],err = makeSimpleRow(v, cols)
		  if err!=nil { return nil ,err} */
	}
	return &valuesTable{vt, cols} //,nil
}
