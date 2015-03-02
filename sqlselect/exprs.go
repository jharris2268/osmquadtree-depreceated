// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package sqlselect

import (
	"fmt"
)

type asExpr struct {
	ex Rower
	as string
}

func (ae *asExpr) String() string { return fmt.Sprintf("AS(%s, %s)", ae.ex, ae.as) }
func (ae *asExpr) Key() string {
	if len(ae.as) > 2 && ae.as[0] == '"' && ae.as[len(ae.as)-1] == '"' {
		return ae.as[1 : len(ae.as)-1]
	}
	return ae.as
}
func (ae *asExpr) Value(row Row) Value { return ae.ex.Value(row) }

type andExpr struct {
	left, right Wherer
}

func (ae *andExpr) String() string { return fmt.Sprintf("AND(%s, %s)", ae.left, ae.right) }

//func (ae *andExpr) Key()    string { return "" }
func (ae *andExpr) Where(row Row) bool { return ae.left.Where(row) && ae.right.Where(row) }

type orExpr struct {
	left, right Wherer
}

func (ae *orExpr) String() string { return fmt.Sprintf("OR(%s, %s)", ae.left, ae.right) }

//func (ae *orExpr) Key()    string { return "" }
func (ae *orExpr) Where(row Row) bool { return ae.left.Where(row) || ae.right.Where(row) }

type notExpr struct {
	rower Wherer
}

func (ae *notExpr) String() string { return fmt.Sprintf("NOT(%s)", ae.rower) }

//func (ae *notExpr) Key()    string { return "" }
func (ae *notExpr) Where(row Row) bool { return !ae.rower.Where(row) }

type valRow struct {
	key string
	val Value
}

func (vr *valRow) String() string {
	p := ""
	if vr.key != "" {
		p = vr.key + "="
	}
	return fmt.Sprintf("Value(%s%s)", p, vr.val.String())
}
func (vr *valRow) Key() string     { return vr.key }
func (vr *valRow) Value(Row) Value { return vr.val }

type pickRow string

func (pr pickRow) String() string { return fmt.Sprintf("Column(%s)", string(pr)) }
func (pr pickRow) Key() string {
	prs := string(pr)
	if prs[0] == '"' && prs[len(prs)-1] == '"' {
		prs = prs[1 : len(prs)-1]
	}
	return prs
}

func (pr pickRow) Value(r Row) Value {
    
	i := r.Index(pr.Key())
        
	if i < 0 {
		return nullValue()
	}
	return r.Value(i)
}

type whereRower struct {
	where Wherer
}

func (br *whereRower) String() string      { return br.where.String() }
func (br *whereRower) Key() string         { return "" }
func (br *whereRower) Value(row Row) Value { return boolValue(br.where.Where(row)) }

/*
func makeExpr(ex sqlparser.Expr) (Rower, error) {
    switch ex.(type) {
        case sqlparser.BoolExpr:
            be,err:=makeBoolExpr(ex.(sqlparser.BoolExpr))
            if err!=nil { return nil,err }
            return &whereRower{be},nil
        case sqlparser.ValExpr:
            return makeValExpr(ex.(sqlparser.ValExpr))
    }
    return nil, errors.New("Not a Expr??"+reflect.TypeOf(ex).String())
}
*/

type equalsOp struct {
	ll, rr Rower
}

func (ae *equalsOp) String() string { return fmt.Sprintf("EQUALS(%s,%s)", ae.ll, ae.rr) }

//func (ae *notExpr) Key()    string { return "" }
func (ae *equalsOp) Where(row Row) bool {
	lv := ae.ll.Value(row)
	rv := ae.rr.Value(row)
	return valueEquals(lv, rv)
}

func valueEquals(lv, rv Value) bool {

	if lv.Type() != rv.Type() || lv.IsNull() || rv.IsNull() {
		return false
	}
	switch lv.Type() {
	case "STRING":
		return lv.AsString() == rv.AsString()
	case "INT":
		return lv.AsInt() == rv.AsInt()
	case "FLOAT":
		return lv.AsFloat() == rv.AsFloat()
	}
	return false
}

type isNullExpr struct {
	ll Rower
}

func (ine *isNullExpr) String() string     { return fmt.Sprintf("IsNull(%s)", ine.ll) }
func (ine *isNullExpr) Where(row Row) bool { return ine.ll.Value(row).IsNull() }

type isInExpr struct {
	ll  Rower
	vls []Rower
}

func (iie *isInExpr) String() string { return fmt.Sprintf("IsIn(%s, %v)", iie.ll, iie.vls) }
func (iie *isInExpr) Where(row Row) bool {
	vl := iie.ll.Value(row)
	for _, v := range iie.vls {
		if valueEquals(vl, v.Value(row)) {
			return true
		}
	}
	return false
}

/*
func makeBoolExpr(ex sqlparser.BoolExpr) (Wherer, error) {

    switch ex.(type) {
        case *sqlparser.AndExpr:
            a:=ex.(*sqlparser.AndExpr)
            l,e := makeBoolExpr(a.Left)
            if e!=nil { return nil,e }
            r,e := makeBoolExpr(a.Right)
            if e!=nil { return nil,e }
            return &andExpr{l,r},nil
        case *sqlparser.OrExpr:
            a:=ex.(*sqlparser.OrExpr)
            l,e := makeBoolExpr(a.Left)
            if e!=nil { return nil,e }
            r,e := makeBoolExpr(a.Right)
            if e!=nil { return nil,e }
            return &orExpr{l,r},nil
        case *sqlparser.NotExpr:
            a:=ex.(*sqlparser.NotExpr)
            l,e := makeBoolExpr(a.Expr)
            if e!=nil { return nil,e }

            return &notExpr{l},nil
        case *sqlparser.ParenBoolExpr:
            return nil,nil
        case *sqlparser.ComparisonExpr:
            return nil,nil
        case *sqlparser.RangeCond:
            return nil,nil
        case *sqlparser.NullCheck:
            return nil,nil
        case *sqlparser.ExistsExpr:
            return nil,nil
    }

    return nil,errors.New("Not a BoolExpr "+reflect.TypeOf(ex).String())
}


func makeValExpr(ex sqlparser.ValExpr) (Rower, error) {

    switch ex.(type) {

        case sqlparser.StrVal:
            return &valRow{"",stringValue(string(ex.(sqlparser.StrVal)))},nil
        case sqlparser.NumVal:
            vl:=makeNumVal(string(ex.(sqlparser.NumVal)))

            return &valRow{"",vl},nil
        case sqlparser.ValArg:
            return nil,nil
        case *sqlparser.NullVal:
            return &valRow{"",nullValue{}},nil
        case *sqlparser.ColName:
            cn := ex.(*sqlparser.ColName)
            if cn.Qualifier != nil {
                println("NEED ColName Qualifers")
            }
            return pickRow(string(cn.Name)),nil

        case sqlparser.ValTuple:
            return nil,nil
        case *sqlparser.Subquery:
            return nil,nil
        case sqlparser.ListArg:
            return nil,nil
        case *sqlparser.BinaryExpr:
            return nil,nil
        case *sqlparser.UnaryExpr:
            return nil,nil
        case *sqlparser.FuncExpr:
            return nil,nil
        case *sqlparser.CaseExpr:
            return makeCaseExpr(ex.(*sqlparser.CaseExpr))
            //return nil,nil
    }
    return nil,errors.New("Not a ValExpr  "+reflect.TypeOf(ex).String())
}
*/
type caseRower struct {
	//expr    Rower
	whens []exPair
	def   Rower
}

func (cr *caseRower) String() string {
	return fmt.Sprintf("CASE [%s], %s)", cr.whens, cr.def)
}
func (cr *caseRower) Key() string { return "" }
func (cr *caseRower) Value(row Row) Value {
	//ex := cr.expr.Value(row)
	for _, w := range cr.whens {
		if w.cond.Where(row) {
			return w.val.Value(row)
		}
	}
	if cr.def == nil {
		return nullValue()
	}
	return cr.def.Value(row)
}

type exPair struct {
	cond Wherer
	val  Rower
}

func (ep exPair) String() string {
	return fmt.Sprintf("When(%s,%s)", ep.cond, ep.val)
}

/*
func makeCaseExpr(ce *sqlparser.CaseExpr) (Rower,error) {
    var err error
    wns := make([]exPair, len(ce.Whens))
    for i,w := range ce.Whens {
        wns[i].cond,err = makeBoolExpr(w.Cond)
        if err!=nil { return nil,err}
        wns[i].val,err = makeValExpr(w.Val)
        if err!=nil { return nil,err}
    }
    def,err := makeValExpr(ce.Else)
    if err!=nil { return nil,err}
    return &caseRower{wns,def},nil
}
*/

func isAsExpr(rower Rower, as string) Rower {
	if as != "" {
		return &asExpr{rower, as}
	}
	return rower
}
