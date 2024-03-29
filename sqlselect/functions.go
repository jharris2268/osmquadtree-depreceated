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


func nullifFunc(exprs []Rower, row Row) Value {
    if len(exprs)!=2 { return nullValue(); }
    vl0 := exprs[0].Value(row)
    vl1 := exprs[1].Value(row)
    
    if valCompOp(vl0,vl1, "=") {
        return nullValue()
    }
    return vl0
}

func strreplaceFunc(exprs []Rower, row Row) Value {
    if len(exprs) != 3 {
		println("strreplaceFunc: len(exprs)!=3")
		return nullValue()
	}
    vl := exprs[0].Value(row)
    a  := exprs[1].Value(row)
    b  := exprs[2].Value(row)
    
    if (vl.Type()!="STRING") || (a.Type()!="STRING") || (b.Type()!="String") {
        return nullValue()
    }
    
    ns := strings.Replace(vl.AsString(), a.AsString(), b.AsString(),-1)
    return stringValue(ns)
}

func numcharFunc(exprs []Rower, row Row) Value {
    if len(exprs) != 2 {
		println("numcharFunc: len(exprs)!=3")
		return nullValue()
	}
    vl := exprs[0].Value(row)
    c  := exprs[1].Value(row)
    
    
    if (vl.Type()!="STRING") || (c.Type()!="STRING") {
        return nullValue()
    }
    
    ns := strings.Count(vl.AsString(), c.AsString())
    return intValue(ns)
}

func maxwidthFunc(exprs []Rower, row Row) Value {
    if len(exprs) ==0 || len(exprs)> 2 {
		println("maxwidthFunc: len(exprs)!=3")
		return nullValue()
	}
    vl := exprs[0].Value(row)
    c := "\n"
    if len(exprs)==2 {
        c = exprs[1].Value(row).AsString()
    }
    
    
    if (vl.Type()!="STRING") || c==""{
        return nullValue()
    }
    
    ns := strings.Split(vl.AsString(), c)
    ml := 0
    for _,s := range ns {
        if len(s)>ml { 
            ml = len(s)
        }
    }
    return intValue(ml)
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

func makefloatFunc(exprs []Rower, row Row) Value {
	if len(exprs) != 1 {
		println("makeintegerFunc: len(exprs)!=1")
		return nullValue()
	}
	vl := exprs[0].Value(row)
	if vl.Type() == "STRING" {
		rs := makeFloatVal(vl.AsString())
		if rs != nil && rs.Type() == "FLOAT" {
			return rs
		}
	}
	return floatValue(0)
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

func concatFunc(cols []Rower, row Row) Value {
	if len(cols) == 0 {
		return nullValue()
	}
	if len(cols) == 1 {
		v := cols[0].Value(row)
		if v.IsNull() || v.Type() != "STRING" {
			return nullValue()
		}
		return v
	}

	a := cols[0].Value(row)
	if a.IsNull() || a.Type() != "STRING" {
		return nullValue()
	}
	b := concatFunc(cols[1:], row)
	if b.IsNull() || b.Type() != "STRING" {
		return nullValue()
	}
	return stringValue(a.AsString() + b.AsString())
}

type mathExpr struct {
	ll, rr Rower
	op     string
}

func (ce *mathExpr) String() string {
	return fmt.Sprintf("Math(%s,%s,%s)", ce.ll, ce.op, ce.rr)
}
func (me *mathExpr) Key() string { return "" }


func asFloat(v Value) float64 {
    if v.Type() == "FLOAT" {
        return v.AsFloat()
    }
    return float64(v.AsInt())
}
    

func (me *mathExpr) Value(row Row) Value {
	lv := me.ll.Value(row)
	if lv.IsNull() || !(lv.Type() == "INTEGER" || lv.Type() == "FLOAT") {
		return nullValue()
	}
	rv := me.rr.Value(row)
	if rv.IsNull() || !(lv.Type() == "INTEGER" || lv.Type() == "FLOAT") {
		return nullValue()
	}
    
    if (lv.Type()=="INTEGER" && rv.Type()=="INTEGER") {
        return intValue(intOp(lv.AsInt(), rv.AsInt(), me.op))
    }
    
    return floatValue(floatOp(asFloat(lv), asFloat(rv), me.op))
	
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

func likeOp(l, r string) bool {
    if strings.HasSuffix(r, "%") {
        return strings.HasPrefix(l, r[:len(r)-1])
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
    case "LIKE":
        return likeOp(l,r)
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
    case "makefloat":
		return makefloatFunc(fe.exprs, row)
	case "concat":
		return concatFunc(fe.exprs, row)
    case "nullif":
        return nullifFunc(fe.exprs, row)
    case "strreplace":
        return strreplaceFunc(fe.exprs, row)
    case "numchar":
        return numcharFunc(fe.exprs, row)
    case "maxwidth":
        return maxwidthFunc(fe.exprs, row)
	default:
		println("unexpected function:", fe.name)
	}
	return nullValue()
}
