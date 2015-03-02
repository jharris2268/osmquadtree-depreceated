// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package calcqts

import (
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/elements"

	"fmt"
	"sync"

	"bufio"
	"io/ioutil"
)

type QtTree interface {
	Iter() <-chan QtTreeEntry
	Len() int
	At(uint32) QtTreeEntry
	Find(quadtree.Quadtree) uint32
}

type QtTreeEntry struct {
	Quadtree quadtree.Quadtree
	Count    int
	Total    int
}

func (qte QtTreeEntry) String() string {
	return fmt.Sprintf("%-18s: %-6d %-10d", qte.Quadtree, qte.Count, qte.Total)
}

type qtTreeItem struct {
	quadTree quadtree.Quadtree
	count    int32
	total    int
	parent   uint32
	children [4]uint32
}

type qtTreeTile struct {
	b [65536]qtTreeItem
	p int
}

//type qtTree map[int]qtTreeTile
type qtTree struct {
	t []*qtTreeTile
}

func newQtTree(q int64, expln int) *qtTree {
	r := qtTree{make([]*qtTreeTile, 0, expln)}
	r.newQtTreeItem(0, 0)
	return &r
}

func (qtt *qtTree) newQtTreeItem(q quadtree.Quadtree, p uint32) uint32 {
	cl := len(qtt.t) - 1
	if cl < 0 || qtt.t[cl].p == len(qtt.t[cl].b) {
		cl++
		//println("add tile",cl)
		//qtt.t = append(qtt.t, qtTreeTile{make([]qtTreeItem, 1<<16), 0})
        qtt.t = append(qtt.t, &qtTreeTile{})

	}

	ni := (cl << 16) | qtt.t[cl].p
	qtt.t[cl].b[qtt.t[cl].p] = qtTreeItem{q, 0, 0, p, [4]uint32{0, 0, 0, 0}}
	qtt.t[cl].p++
	
	return uint32(ni)
}

func (qtt *qtTree) Add(qt quadtree.Quadtree) {
    if qt<0 {
        return
    }
	qtt.addint(0, qt, 1)
}

func (qtt *qtTree) AddMulti(qt quadtree.Quadtree, w int32) {
	qtt.addint(0, qt, w)
}

func (qtt *qtTree) Remove(i uint32) {
	if i == 0 {
		return
	}

	t := qtt.Get(i)
	v := t.total

	ci := (t.quadTree >> (63 - 2*uint(t.quadTree&31))) & 3
	//println("remove",i,osmread.QuadtreeString(t.quadTree),ci)

	if t.parent != i {
		i = t.parent
		t = qtt.Get(t.parent)
		t.children[ci] = 0
		t.total -= v
		for t.parent != i {
			i = t.parent
			t = qtt.Get(i)
			t.total -= v
		}

	}
}

type qtTreeItemI struct {
	t *qtTreeItem
	i uint32
}

func (ti qtTreeItemI) String() string {
	return fmt.Sprintf("%-18s: %-6d %-10d %d %d", ti.t.quadTree, ti.t.count, ti.t.total, ti.i, ti.t.parent)
}

func (qtt *qtTree) ItemString(i uint32) string {
	return qtTreeItemI{qtt.Get(i), i}.String()
}

func (qtt *qtTree) Get(i uint32) *qtTreeItem {
	return &(qtt.t[int(i>>16)].b[i&65535])
}

func (qtt *qtTree) At(i uint32) QtTreeEntry {
	if int(i) >= qtt.Len() {
		return QtTreeEntry{-1, -1, -1}
	}
	t := qtt.Get(i)
	return QtTreeEntry{t.quadTree, int(t.count), t.total}
}

func (qtt *qtTree) Len() int {
	cl := len(qtt.t) - 1
	if cl < 0 {
		return 0
	}
	return cl<<16 | qtt.t[cl].p
}

func (qtt *qtTree) Total(i uint32) int {
	if int(i) >= qtt.Len() {
		return -1
	}
	return qtt.Get(i).total
}
func (qtt *qtTree) QuadTree(i uint32) quadtree.Quadtree {
	if int(i) >= qtt.Len() {
		return -1
	}
	return qtt.Get(i).quadTree
}

func (qtt *qtTree) addint(i uint32, qt quadtree.Quadtree, w int32) {
	t := qtt.Get(i)

	t.total += int(w)
	if qt == t.quadTree {
		t.count += w
		return
	}

	d := uint(t.quadTree & 31)
	nv := qt >> (61 - 2*d) & 3

	if t.children[nv] == 0 {
		cq := qt.Round(d+1)
		t.children[nv] = qtt.newQtTreeItem(cq, i)
	}
	qtt.addint(t.children[nv], qt, w)
	
}

func (qtt *qtTree) Find(qt quadtree.Quadtree) uint32 {
	return qtt.findInt(qt, 0, 0)
}

func (qtt *qtTree) findInt(qt quadtree.Quadtree, lastrs uint32, idx uint32) uint32 {
	t := qtt.Get(idx)
	if qt == t.quadTree {
		if t.count != 0 {
			return idx
		} else {
			return lastrs
		}
	}

	if t.count != 0 {
		lastrs = idx
	}
	d := uint(t.quadTree & 31)
	nv := qt >> (61 - 2*d) & 3
	if t.children[nv] == 0 {
		return lastrs
	}
	return qtt.findInt(qt, lastrs, t.children[nv])
}

func (qtt *qtTree) Iter() <-chan QtTreeEntry {

	res := make(chan QtTreeEntry)
	go func() {
		qtTreeIterInt(qtt, 0, res)
		close(res)
	}()
	return res
}

func qtTreeIterInt(qtt *qtTree, i uint32, res chan QtTreeEntry) {

	t := qtt.Get(i)
	if t.count != 0 {
		res <- QtTreeEntry{t.quadTree, int(t.count), t.total}
	}
	for _, c := range t.children {
		if c != 0 && c != i {
			qtTreeIterInt(qtt, c, res)
		}
	}

}

func FindQtTree(inchans []chan elements.ExtendedBlock, maxLevel uint) QtTree {
	res := newQtTree(0, 2000)
	intr := make(chan map[quadtree.Quadtree]int32)
	
    go func() {
        wg:=sync.WaitGroup{}
        wg.Add(len(inchans))
        for _,inc := range inchans {
            go func (inc chan elements.ExtendedBlock) {
                for bl := range inc {
                    b := map[quadtree.Quadtree]int32{}
                    for i := 0; i < bl.Len(); i++ {

                        q := bl.Element(i).(interface{Quadtree() quadtree.Quadtree}).Quadtree()
                        b[q]++

                    }
                    intr <- b
                }
                wg.Done()
            }(inc)
        }
        wg.Wait()
        close(intr)
    }()

	for bl := range intr {
		for q, w := range bl {
			res.AddMulti(q.Round(maxLevel), w)
		}
	}
	return res
}
/*
func FindQtTreeSingle(incc <-chan simpleobj.SimpleObjBlock) QtTree {
	res := newQtTree(0, 2000)

	for bl := range incc {
		for i := 0; i < bl.Len(); i++ {
			q := bl.Object(i).Quadtree()
			res.Add(quadtree.RoundUp(q, 17))
		}
	}
	return res
}*/

func findGroupInt(qtt *qtTree, i uint32, mn int, mx int) []uint32 {
	//println(qtt.ItemString(i),mn,mx)
	t := qtt.Get(i)

	alls := true
	for _, c := range t.children {
		if c != 0 {
			cc := qtt.Get(c)
			if cc.total > 1000 {
				alls = false
			}
		}
	}

	if t.count != 0 && t.total >= mn && (t.total <= mx || (t.total == int(t.count)) || alls) {
		//println("ret",i)
		qtt.Remove(i)
		return []uint32{i}
	}

	r := []uint32{}

	if t.total < mn {
		return r
	}

	for _, c := range t.children {
		if c != 0 {
			r = append(r, findGroupInt(qtt, c, mn, mx)...)
		}
	}
	return r
}

func qttCount(qt QtTree) int {
	c := 0
	for range qt.Iter() {
		c++
	}
	return c
}

func FindQtGroups(qttin QtTree, target int) QtTree {
	//res:=make([]uint32,0,500000)

	qtt, ok := qttin.(*qtTree)
	if !ok {
		panic("qtt not a *qtTree")
	}

	mn := target - 500
	mx := target + 500
	foundzero := false

	nqtt := newQtTree(0, 1)

	for qtt.Get(0).total >= mn && !foundzero {

		cont := true
		for cont && !foundzero {
			//println(mn,mx,qtt.Get(0).total,len(res))
			r := findGroupInt(qtt, 0, mn, mx)
			for _, ri := range r {
				if ri == 0 {
					foundzero = true
					break
				}
			}
			cont = len(r) > 0
			for _, ri := range r {
				t := qtt.Get(ri)
				nqtt.AddMulti(t.quadTree, int32(t.total))
			}
			//res = append(res, r...)
		}
		if foundzero {
			break
		}
		mn -= 500
		if mn < 1000 {
			mn = 1000
		}
		mx += 500
		if mx > 50000 {
			println("mx=", mx)
			//panic(0)
			break
		}
	}

	t0 := qtt.Get(0)
	if !foundzero && t0.total > 0 {
		if qttCount(qtt) > 1 {
			println("foundzero=", foundzero, "t0.total", t0.total, "t0.quadTree", t0.quadTree, "qtt.Len()=", qtt.Len())
			of, _ := ioutil.TempFile("", "calcqts")
			println("zero tile remains: dump tree", of.Name())
			func() {

				w := bufio.NewWriter(of)

				for r := range qtt.Iter() {
					println(r.String())
					w.WriteString(r.String() + "\n")
				}
				w.Flush()
			}()

			of.Close()
			//res = append(res, 0)
            panic("")
		}

		nqtt.AddMulti(0, int32(t0.total))

	}

	return nqtt
}

func MakeQtTree(inqts []quadtree.Quadtree) QtTree {
	qttree := newQtTree(0, len(inqts)*3/2/65536)
	for _, q := range inqts {
        
		qttree.Add(q)
	}
	return qttree
}
