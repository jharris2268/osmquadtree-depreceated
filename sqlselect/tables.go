// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package sqlselect

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type pickTable string

func (pt pickTable) Result(tables Tables) (Result, error) {
	pts := string(pt)
	t, ok := tables[pts]
	if !ok {
		return nil, errors.New("no such table " + pts)
	}
	return t, nil
}
func (pt pickTable) String() string {

	return string(pt)
}

//**********************************************************************
type asTable struct {
	table Tabler
	as    string
}

func (at *asTable) Result(tables Tables) (Result, error) {
	return at.table.Result(tables)
}
func (at *asTable) String() string {
	return fmt.Sprintf("AS(%s,%s)\n", at.table.String(), at.as)
}

func isAsTable(table Tabler, as string) Tabler {

	if as != "" {
		return &asTable{table, as}
	}
	return table
}

//**********************************************************************

type unionQuery struct {
	left, right   Tabler
	uniontype, as string
}

func (sq *unionQuery) Result(tables Tables) (Result, error) {
	lr, err := sq.left.Result(tables)
	if err != nil {
		return nil, err
	}
	rr, err := sq.right.Result(tables)
	if err != nil {
		return nil, err
	}
	//println(fmt.Sprintf("return concatResult: %d %d %d %d\n",lr.Len(),rr.Len(),len(lr.Columns()),len(rr.Columns())))
	return &concatResult{lr, rr, lr.Len()}, nil
}
func (sq *unionQuery) String() string {
	ls := strings.Replace(sq.left.String(), "\n", "\n\t", -1)
	rs := strings.Replace(sq.right.String(), "\n", "\n\t", -1)
	return fmt.Sprintf("UNION{\n\t%s,\n\t%s,\n\t%s,%s\n}", ls, rs, sq.uniontype, sq.as)
}

//**********************************************************************
type joinQuery struct {
	left, right Tabler
	cols        []Rower
}

func make_join_key(r Row, cols []Rower) (string, []keyValuePair, []keyValuePair) {
	cn := map[string]int{}

	k := make([]string, len(cols))

	for j, c := range cols {
		cn[c.Key()] = j
	}

	at := make([]keyValuePair, 0, len(cols))
	ot := make([]keyValuePair, 0, r.Len()-len(cols))
	tf := len(cn)
	for j := 0; j < r.Len(); j++ {
		jk := r.Key(j)
		s, ok := cn[jk]
		if ok {
			k[s] = r.Value(j).String()
			at = append(at, keyValuePair{jk, r.Value(j)})
			tf--
		} else {
			ot = append(ot, keyValuePair{jk, r.Value(j)})
		}

	}
	if tf == 0 {
		return strings.Join(k, "_"), at, ot
	}
	return "", nil, append(at, ot...)
}

func (jq *joinQuery) Result(tables Tables) (Result, error) {
	left, err := jq.left.Result(tables)
	if err != nil {
		return nil, err
	}
	right, err := jq.right.Result(tables)
	if err != nil {
		return nil, err
	}
	rm := map[string][]keyValuePair{}
	for i := 0; i < right.Len(); i++ {
		r := right.Row(i)
		k, _, ot := make_join_key(r, jq.cols)
		if k != "" {
			rm[k] = ot
		}
	}

	cns := map[string]bool{}
	for _, c := range jq.cols {
		cns[c.Key()] = true
	}
	res := make([]simpleRow, 0, left.Len())
	for i := 0; i < left.Len(); i++ {
		l := left.Row(i)
		k, at, ot := make_join_key(l, jq.cols)
		if k != "" {
			r, ok := rm[k]
			if ok {
				rr := append(append(ot, at...), r...)
				for _, ri := range rr {
					cns[ri.key] = true
				}
				//println("jr",len(ot),len(at),len(r))
				res = append(res, simpleRow(rr))
			}
		}
	}
	cols := make([]Rower, 0, len(cns))
	for c, _ := range cns {
		cols = append(cols, pickRow(c))
	}
	//println(fmt.Sprintf("return valuesTable: %d rows, %v",len(res),cols))
	return &valuesTable{res, cols}, nil
}

func (jq *joinQuery) String() string {
	return fmt.Sprintf("JOIN{%s,%s,%s}", jq.left, jq.right, jq.cols)
}

//**********************************************************************

type simpleSelect struct {
	Table   Tabler
	Columns []Rower
	Where   Wherer
	Order   Orderer
}

func makeSimpleSelect(tab Tabler, cols []Rower, where Wherer, order orderList) Tabler {
	if len(cols) == 0 && where == nil && len(order) == 0 {
		return tab
	}
	var cs []Rower
	if len(cols) != 0 {
		cs = cols
	}
	var oo Orderer
	if len(order) != 0 {
		oo = order
	}
	return &simpleSelect{tab, cs, where, oo}
}

func (ss *simpleSelect) String() string {
	wherestr := ""
	if ss.Where != nil {
		wherestr = "\n  where: " + ss.Where.String()
	}
	selstr := "*"
	if ss.Columns != nil {
		pp := make([]string, len(ss.Columns))
		for i, c := range ss.Columns {
			if c != nil {
				pp[i] = c.String()
			}
		}
		selstr = strings.Join(pp, ", ")
	}
	tstr := ""
	if ss.Table != nil {
		tstr = strings.Replace(ss.Table.String(), "\n", "\n\t\t", -1)

	}

	orderstr := ""
	if ss.Order != nil {
		/*ol,ok:=ss.Order.(orderList)
		  if ok && len(ol) == 0 {
		      ss.Order=nil
		  } else {
		      for _,o:=range ol {
		          f:=false
		          for _,c:=range ss.Columns {
		              if c.Key() == o.rower.Key() {
		                  f=true
		              }
		          }
		          if !f {
		              ss.Columns = append(ss.Columns, pickRow(o.rower.Key()))
		          }
		      }

		  }*/
		orderstr = "\n order: " + ss.Order.String()
	}

	return fmt.Sprintf("SELECT(\n  cols:  %s\n  from:  %s%s%s\n)", selstr, tstr, wherestr, orderstr)
}

func (ss *simpleSelect) Result(tables Tables) (Result, error) {
	rr, err := ss.Table.Result(tables)
	if err != nil {
		return nil, err
	}
	if ss.Where == nil && ss.Columns == nil && ss.Order == nil {
		return rr, nil
	}

	cols := ss.Columns
	if cols == nil {
		//println("nil cols, use parent",len(rr.Columns()))
		cols = rr.Columns()
	}

	var rrs []int
	if ss.Where != nil {
		rrs = make([]int, 0, rr.Len())
		for i := 0; i < rr.Len(); i++ {
			if ss.Where.Where(rr.Row(i)) {
				rrs = append(rrs, i)
			}
		}
		if len(rrs) == 0 {

			return &emptyResult{cols}, nil
		}

	}

	rs := &resultSet{rr, rrs, cols}
	//println(fmt.Sprintf("selected %d rows, %d cols",rr.Len(),len(cols)))

	if ss.Order != nil {
		//sl,ok := ss.Order.(orderList)
		//if !ok || len(sl)!=0 {
		sortResultSet(rs, ss.Order)
		/*if len(rs.rows)>5{
		    println(fmt.Sprintf("%v %v",rs.rows[:5],rs.rows[len(rs.rows)-5:]))
		}*/
		//}
	}

	return rs, nil

}

//**********************************************************************

type resultRow struct {
	cols  []Rower
	inrow Row
}

func (rr *resultRow) Len() int         { return len(rr.cols) }
func (rr *resultRow) Key(i int) string { return rr.cols[i].Key() }
func (rr *resultRow) Index(k string) int {
	for i, c := range rr.cols {
		if k == c.Key() {
			return i
		}
	}
	return -1
}
func (rr *resultRow) Value(i int) Value {
	return rr.cols[i].Value(rr.inrow)
}

//**********************************************************************

type simpleRower struct {
	key string
}

func (sr *simpleRower) Key() string { return sr.key }

func (sr *simpleRower) Value(inrow Row) Value {
	i := inrow.Index(sr.key)
	if i < 0 {
		return nullValue()
	}
	return inrow.Value(i)
}

//**********************************************************************

type resultSet struct {
	table Result
	rows  []int
	cols  []Rower
}

func (rs *resultSet) Columns() []Rower { return rs.cols }
func (rs *resultSet) Len() int {
	if rs.rows != nil {
		return len(rs.rows)
	}
	return rs.table.Len()
}
func (rs *resultSet) Row(i int) Row {
	if rs.rows != nil {

		i = rs.rows[i]

	}
	return &resultRow{rs.cols, rs.table.Row(i)}
}

//**********************************************************************

type concatResult struct {
	left, right Result
	lln         int
}

func (cr *concatResult) Columns() []Rower {
	return cr.left.Columns()
}

func (cr *concatResult) Len() int {
	if cr.lln < 0 {
		cr.lln = cr.left.Len()
	}
	return cr.lln + cr.right.Len()
}
func (cr *concatResult) Row(i int) Row {
	if cr.lln < 0 {
		cr.lln = cr.left.Len()
	}
	if i < cr.lln {
		return cr.left.Row(i)
	}
	return cr.right.Row(i - cr.lln)
}

//**********************************************************************
type emptyResult struct {
	cols []Rower
}

func (er *emptyResult) Len() int         { return 0 }
func (er *emptyResult) Row(int) Row      { return nil }
func (er *emptyResult) Columns() []Rower { return er.cols }

//**********************************************************************
func sortResultSet(rs *resultSet, order Orderer) {
	if rs.rows == nil {
		rs.rows = make([]int, rs.Len())
		for i, _ := range rs.rows {
			rs.rows[i] = i
		}
	}
	//println(fmt.Sprintf("sort: %d %d %s %v => %v",rs.Len(),len(rs.rows),order,rs.rows[:5],rs.rows[len(rs.rows)-5:]))
	trs := &tempRS{rs, order}
	sort.Sort(trs)
	//println(fmt.Sprintf("=> %d %d %v => %v",rs.Len(),len(rs.rows),rs.rows[:5],rs.rows[len(rs.rows)-5:]))
	/*ol:=order.(orderList)
	  r0:=rs.Row(0).(*resultRow).inrow
	  r1:=rs.Row(rs.Len()-1).(*resultRow).inrow
	  println(fmt.Sprintf("%s %s",ol[0].rower.Value(r0),ol[1].rower.Value(r0)))
	  println(fmt.Sprintf("%s %s",ol[0].rower.Value(r1),ol[1].rower.Value(r1)))*/
}

type tempRS struct {
	rs    *resultSet
	order Orderer
}

func (trs *tempRS) Len() int { return trs.rs.Len() }
func (trs *tempRS) Less(i, j int) bool {
	//println("Less(",i,",",j,")")
	a := trs.rs.Row(i).(*resultRow).inrow
	b := trs.rs.Row(j).(*resultRow).inrow

	return trs.order.Less(a, b)
}

func (trs *tempRS) Swap(i, j int) {
	trs.rs.rows[i], trs.rs.rows[j] = trs.rs.rows[j], trs.rs.rows[i]
}

//**********************************************************************
