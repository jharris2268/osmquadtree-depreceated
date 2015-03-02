// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package sqlselect

import (
	"fmt"
	"strings"
)

func coalesceFunc(exprs []Rower, row Row) Value {
	for _, ex := range exprs {
		vl := ex.Value(row)
		if !vl.IsNull() {
			return vl
		}
	}
	return nullValue()
}

func makeintegerFunc(exprs []Rower, row Row) Value {
	if len(exprs) != 1 {
		println("makeintegerFunc: len(exprs)!=1")
		return nullValue()
	}
	vl := exprs[0].Value(row)
	if vl.Type() == "STRING" {
		rs := makeNumVal(vl.AsString())
		if rs != nil && rs.Type() == "INTEGER" {
			return rs
		}
	}
	return intValue(0)
}

func charlengthFunc(exprs []Rower, row Row) Value {
	if len(exprs) != 1 {
		println("charlengthFunc: len(exprs)!=1")
		return nullValue()
	}
	vl := exprs[0].Value(row)
	if vl.Type() == "STRING" {
		return intValue(len(vl.AsString()))
	}
	return intValue(0)
}

func substrFunc(exprs []Rower, row Row) Value {
	if len(exprs) != 3 {
		println("charlengthFunc: len(exprs)!=1")
		return nullValue()
	}
	str := exprs[0].Value(row)
	fr := exprs[1].Value(row)
	to := exprs[2].Value(row)

	if str.Type() != "STRING" || fr.Type() != "INTEGER" || to.Type() != "INTEGER" {
		return nullValue()
	}

	strs := str.AsString()
	fri, toi := fr.AsInt(), to.AsInt()

	//println("substr",strs,fri,toi)
	stl := int64(len(strs))
	if fri == 0 {
		fri = 1
		toi -= 1
	}
	if fri < 1 || fri > stl || toi <= 0 || (fri+toi-1) > stl {
		return nullValue()
	}
	r := strs[fri-1 : fri+toi-1]
	//println("substr",strs,fri,toi,r)
	return stringValue(r)
}

type concatExpr struct {
	ll, rr Rower
}

func (ce *concatExpr) String() string {
	return fmt.Sprintf("Concat(%s,%s)", ce.ll, ce.rr)
}
func (ce *concatExpr) Key() string { return "" }
func (ce *concatExpr) Value(row Row) Value {
	lv := ce.ll.Value(row)
	if lv.IsNull() || lv.Type() != "STRING" {
		return nullValue()
	}
	rv := ce.rr.Value(row)
	if rv.IsNull() || rv.Type() != "STRING" {
		return nullValue()
	}
	return stringValue(lv.AsString() + rv.AsString())
}

type mathExpr struct {
	ll, rr Rower
	op     string
}

func (ce *mathExpr) String() string {
	return fmt.Sprintf("Math(%s,%s,%s)", ce.ll, ce.op, ce.rr)
}
func (me *mathExpr) Key() string { return "" }
func (me *mathExpr) Value(row Row) Value {
	lv := me.ll.Value(row)
	if lv.IsNull() || !(lv.Type() == "INTEGER" || lv.Type() == "FLOAT") {
		return nullValue()
	}
	rv := me.rr.Value(row)
	if rv.IsNull() || rv.Type() != lv.Type() {
		return nullValue()
	}
	switch lv.Type() {
	case "INTEGER":
		return intValue(intOp(lv.AsInt(), rv.AsInt(), me.op))
	case "FLOAT":
		return floatValue(floatOp(lv.AsFloat(), rv.AsFloat(), me.op))
	}
	return nullValue()
}

func intOp(l int64, r int64, op string) int64 {
	switch op {
	case "+":
		return l + r
	case "-":
		return l - r
	case "/":
		return l / r
	case "*":
		return l * r

	}
	return 0
}
func floatOp(l float64, r float64, op string) float64 {
	switch op {
	case "+":
		return l + r
	case "-":
		return l - r
	case "/":
		return l / r
	case "*":
		return l * r

	}
	return 0.0
}

type compExpr struct {
	ll, rr Rower
	op     string
}

func (co *compExpr) String() string {
	return fmt.Sprintf("CompOp(%s,%s,%s)", co.ll, co.rr, co.op)
}
func (co *compExpr) Where(row Row) bool {
	return valCompOp(co.ll.Value(row), co.rr.Value(row), co.op)
}

func valCompOp(lv Value, rv Value, op string) bool {

	if lv.IsNull() {
		return false
	}

	if rv.IsNull() || lv.Type() != rv.Type() {
		return false
	}
	switch lv.Type() {
	case "STRING":
		return stringCompOp(lv.AsString(), rv.AsString(), op)
	case "INTEGER":
		return intCompOp(lv.AsInt(), rv.AsInt(), op)
	case "FLOAT":
		return floatCompOp(lv.AsFloat(), rv.AsFloat(), op)
	}
	return false
}

func stringCompOp(l string, r string, op string) bool {
	switch op {
	case "=":
		return l == r
	case "!=":
		return l != r
	case ">":
		return l > r
	case "<":
		return l < r
	case ">=":
		return l >= r
	case "<=":
		return l <= r
	}
	println("unknown op", op)
	return false
}

func intCompOp(l int64, r int64, op string) bool {
	switch op {
	case "=":
		return l == r
	case "!=":
		return l != r
	case ">":
		return l > r
	case "<":
		return l < r
	case ">=":
		return l >= r
	case "<=":
		return l <= r
	}
	println("unknown op", op)
	return false
}

func floatCompOp(l float64, r float64, op string) bool {
	//println(l,r,op)
	switch op {
	case "=":
		return l == r
	case "!=":
		return l != r
	case ">":
		return l > r
	case "<":
		return l < r
	case ">=":
		return l >= r
	case "<=":
		return l <= r
	}
	println("unknown op", op)
	return false
}

type rangeExpr struct {
	test, min, max Rower
}

func (co *rangeExpr) String() string {
	return fmt.Sprintf("RangeExpr(%s,%s,%s)", co.test, co.min, co.max)
}
func (co *rangeExpr) Where(row Row) bool {
	tv, mnv, mxv := co.test.Value(row), co.min.Value(row), co.max.Value(row)
	return valCompOp(tv, mnv, ">=") && valCompOp(tv, mxv, "<=")
}

type funcExpr struct {
	name  string
	exprs []Rower
}

func (fe *funcExpr) String() string { return fmt.Sprintf("Function(%s,%v)", fe.name, fe.exprs) }
func (fe *funcExpr) Key() string    { return fe.name }
func (fe *funcExpr) Value(row Row) Value {
	switch strings.ToLower(fe.name) {
	case "coalesce":
		return coalesceFunc(fe.exprs, row)
	case "substr":
		return substrFunc(fe.exprs, row)
	case "char_length", "length":
		return charlengthFunc(fe.exprs, row)
	case "makeinteger":
		return makeintegerFunc(fe.exprs, row)
	default:
		println("unexpected function:", fe.name)
	}
	return nullValue()
}
