// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package calcqts

import (
	"fmt"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"

	"bufio"
	"io/ioutil"
	"os"
	"sync"
)

type QtTree interface {
	Iter() <-chan QtTreeEntry      //Iterate over active leaves, in order
	Len() int                      //Total number of leaves, including inactive ones
	At(uint32) QtTreeEntry         //Return leaf at index
	Find(quadtree.Quadtree) uint32 //Find the last parent node for a quadtree value
}

type QtTreeEntry struct {
	Quadtree quadtree.Quadtree
	Count    int64
	Total    int64
}

func (qte QtTreeEntry) String() string {
	return fmt.Sprintf("%-18s: %-6d %-10d", qte.Quadtree, qte.Count, qte.Total)
}

//nb. 44 bytes
type qtTreeItem struct {
	quadTree quadtree.Quadtree
	count    int64
	total    int64
	parent   uint32
	children [4]uint32
}

//use fixed array for memory efficiency: 2.75mb per tile
type qtTreeTile struct {
	b [65536]qtTreeItem
	p int
}

//i.e smaller number of pointers to fixed size large objects
type qtTree struct {
	t []*qtTreeTile
}

func newQtTree(q int64, expln int) *qtTree {
	r := qtTree{make([]*qtTreeTile, 0, expln)}
	//make sure we have the zero entry in place
	r.newQtTreeItem(0, 0)
	return &r
}

func (qtt *qtTree) newQtTreeItem(q quadtree.Quadtree, p uint32) uint32 {
	//add a new entry
	cl := len(qtt.t) - 1                           // current tile
	if cl < 0 || qtt.t[cl].p == len(qtt.t[cl].b) { //if current is full, add new tile
		cl++
		qtt.t = append(qtt.t, &qtTreeTile{})

	}

	ni := (cl << 16) | qtt.t[cl].p //construct index for new entry
	qtt.t[cl].b[qtt.t[cl].p] = qtTreeItem{q, 0, 0, p, [4]uint32{0, 0, 0, 0}}
	qtt.t[cl].p++

	return uint32(ni)
}

func (qtt *qtTree) Add(qt quadtree.Quadtree) {
	if qt < 0 {
		return
	}
	qtt.addint(0, qt, 1)
}

func (qtt *qtTree) AddMulti(qt quadtree.Quadtree, w int64) {
	qtt.addint(0, qt, w)
}

func (qtt *qtTree) Remove(i uint32) {

	removeQtt(qtt, i, true)
}

func removeQtt(qtt *qtTree, i uint32, clip bool) {

	if i == 0 {
		return
	}

	t := qtt.Get(i)
	v := t.total

	//which child are we of our parent
	ci := (t.quadTree >> (63 - 2*uint(t.quadTree&31))) & 3

	if t.parent != i {
		i = t.parent
		t = qtt.Get(t.parent)
		t.children[ci] = 0
		if clip {
			t.total -= v
			for t.parent != i { //keep going till we reach the root leaf
				i = t.parent
				t = qtt.Get(i)
				t.total -= v
			}
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
	return QtTreeEntry{t.quadTree, t.count, t.total}
}

func (qtt *qtTree) Len() int {
	cl := len(qtt.t) - 1
	if cl < 0 {
		return 0
	}
	return cl<<16 | qtt.t[cl].p
}

func (qtt *qtTree) Total(i uint32) int64 {
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

func (qtt *qtTree) addint(i uint32, qt quadtree.Quadtree, w int64) {
	t := qtt.Get(i)

	t.total += w
	if qt == t.quadTree {
		t.count += w
		return
	}

	d := uint(t.quadTree & 31)
	nv := qt >> (61 - 2*d) & 3

	if t.children[nv] == 0 {
		cq := qt.Round(d + 1)
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

func countChildren(qtt *qtTree, i uint32) int {
    t:=qtt.Get(i)
    ans:=0
    if t.count>0 {
        ans += 1
    }
    
    for _,c:=range t.children {
        if c > 0 {
            ans += countChildren(qtt,c)
        }
    }
    return ans
}

func (qtt *qtTree) TrimSmall(minsize int64) int {
    return trimQttTree(qtt,0,minsize)
}

func trimQttTree(qtt *qtTree, i uint32, minsize int64) int {
    
    t := qtt.Get(i)
    if t.total <= minsize {
        nc := countChildren(qtt,i)
        removeQtt(qtt,i,false)
        return nc
    }
    
    ans := 0
    for _,c:=range t.children {
        if c!=0 {
            ans += trimQttTree(qtt,c,minsize)
        }
    }
    return ans
}
    
func qtTreeIterInt(qtt *qtTree, i uint32, res chan QtTreeEntry) {

	t := qtt.Get(i)
	if t.count != 0 {
		//only return tiles with entries
		res <- QtTreeEntry{t.quadTree, t.count, t.total}
	}
	for _, c := range t.children {
		if c != 0 && c != i {
			qtTreeIterInt(qtt, c, res)
		}
	}
	//go back a level, and iter over the remaining children

}

func FindQtTree(inchans []chan elements.ExtendedBlock, maxLevel uint) QtTree {
	res := newQtTree(0, 2000)
	intr := make(chan map[quadtree.Quadtree]int64)

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(len(inchans))
		for _, inc := range inchans {
			go func(inc chan elements.ExtendedBlock) {
				for bl := range inc {
					//for each block, count number of objects with each quadtree value
					b := map[quadtree.Quadtree]int64{}
					for i := 0; i < bl.Len(); i++ {

						q := bl.Element(i).(interface {
							Quadtree() quadtree.Quadtree
						}).Quadtree()
						b[q]++

					}
					//order doesn't matter, so write to one intermediary chan
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
			//call AddMulti once for each quadtree value in each block
			res.AddMulti(q.Round(maxLevel), w)
		}
	}
	return res
}

func findGroupInt(qtt *qtTree, i uint32, absmin int64, mn int64, mx int64, target int64) []uint32 {
	t := qtt.Get(i)

    
    diff := t.total-target
    if diff<0 { diff *= -1 }
    
	//look to see if all children (if any) are small
	alls := true
    
	for _, c := range t.children {
		if c != 0 {
			cc := qtt.Get(c)
			if cc.total > absmin {
				alls = false
			}
            
		}
	}
	
    //return if:
	//1. has values itself
	//2. is bigger than the minimum
	//3. is either:
	//  i.  smaller than the maxmimum
	//  ii. has no children, or only small children
	if t.count != 0 && t.total >= mn && (t.total <= mx || (t.total == t.count) || alls) {

		qtt.Remove(i) //remove from tree: at end, the tree will have no entries left
		return []uint32{i}
	}

	r := []uint32{}

	//won't find any children either
	if t.total < mn {
		return r
	}

	for _, c := range t.children {

		if c != 0 {
			//return concat'ed results of the child nodes
			r = append(r, findGroupInt(qtt, c, absmin, mn, mx, target)...)
		}
	}
	return r
}

func findGroupIntFlat(qtt *qtTree, i uint32, absmin int64, mn int64, mx int64, target int64) []uint32 {
//size_t clip_within(std::shared_ptr<qttree> tree, std::set<size_t>& outs, int64 min, int64 max, int64 absmin) {
    
    var next_item func(uint32,*qtTreeItem,int) uint32
    
    next_item = func(j uint32, t *qtTreeItem, fc int) uint32 {
		
		if fc<4 {
			for _,b := range t.children[fc:] {
				if b!=0 {
					return b
				}
			}
		}
		
        if (t.parent==j) { return 0 }
        pc := int((t.quadTree>>(63-2*uint(t.quadTree&31)))&3)
        
        par := qtt.Get(t.parent)
                
        
        //std::cout << "t.qt=" << quadtree_string(t.qt) << " next=" << pc+1 << std::endl;
        return next_item(t.parent,par,pc+1)
	}
	
	
    res := []uint32{}
    
    atend:=false
    for !atend {
		
		t := qtt.Get(i)
        
        if t.total >= mn {
			alls := true
			
			for _, c := range t.children {
				if c != 0 {
					cc := qtt.Get(c)
					if cc.total > absmin {
						alls = false
					}
					
				}
			}
			
			
            if ((t.count!=0) && ((t.total==t.count) || (t.total <= mx) || alls)) {
                //std::cout << " add";
                /*std::cout << "add " << item_string(i,t) << std::endl;
                tree->rollup(i);*/
                j:=i
                
                res = append(res, i)
                i = next_item(i,t,4)
                
                qtt.Remove(j)
            } else {
                i = next_item(i,t,0)
            }
        } else {
            i = next_item(i,t,4)
            
        } 
        //std::cout <<  std::endl;
        atend = (i==0)
    }
    
    return res
}


func findClosestToTarget_int(qtt *qtTree, i uint32, absmin int64, mn int64, mx int64, target int64) []uint32 {
    res := []uint32{}
    t := qtt.Get(i)
    if t.total < mn {
        return res
    }
    
        
    if t.count > 0 && t.total < mx {
        res = append(res,i)
        
        
    }
    
    if t.total > (target+mn) {    
        for _,c := range t.children {
            if c!=0 {
                res=append(res, findClosestToTarget_int(qtt,c,absmin, mn,mx,target)...)
            }
        }
    }
    
    if len(res)<2 {
        return res
    }
    
    curr := res[0]
    tot:=qtt.Get(curr).total - target
    if tot<0 {
        tot *= -1
    }
    
    for _,m := range res[1:] {
        t:=qtt.Get(m).total-target
        if t<0 { t*=-1 }
        if t < tot {
            curr=m
            tot=t
        }
    }
    return []uint32{curr}
}

func findClosestToTarget(qtt *qtTree, i uint32, absmin int64, mn int64, mx int64, target int64) []uint32 {
    mm := findClosestToTarget_int(qtt,i,absmin, mn,mx,target)
    if len(mm)==0 {
        return mm
    }
    if len(mm)!=1 {
        panic(fmt.Sprintf("?? findClosestToTarget %s",mm))
    }
    qtt.Remove(mm[0])
    return mm
}
        
            

func qttCount(qt QtTree) int {
	c := 0
	for range qt.Iter() {
		c++
	}
	return c
}

// Construct a new QtTree by grouping leaves of qttin
func FindQtGroups(qttin QtTree, target int64, minimum int64) QtTree {

	qtt, ok := qttin.(*qtTree)
	if !ok {
		panic("qtt not a *qtTree")
	}
    origtotal := qtt.Get(0).total
    
    //rems := qtt.TrimSmall(minimum/2)
    //fmt.Printf("removed %d small\n", rems)

	mn := target - 50
	mx := target + 50
	foundzero := false

	//result qttree: this will be much smaller
	nqtt := newQtTree(0, 10)
    numf :=0
    nextm:=0
	for qtt.Get(0).total > 1000 && !foundzero {

		cont := true
		//repeat until we don't find a group between mn and mx, or have found the root
		for cont && !foundzero {

			//r := findGroupInt(qtt, 0, minimum, mn, mx,target)
			r := findGroupIntFlat(qtt, 0, minimum, mn, mx,target)
            //r := findClosestToTarget(qtt,0,mn,mx,target)
			for _, ri := range r {
				if ri == 0 {
					foundzero = true
					break
				}
			}
			cont = len(r) > 0
			//add found groups to result qttree
			for _, ri := range r {
				
                
                t := qtt.Get(ri)
				nqtt.AddMulti(t.quadTree, t.total)
			}
			
			
            numf += len(r)
            if numf > nextm || true {
                nt:=nqtt.Get(0).total
                fmt.Printf("\r%6d: %10d / %10d [%5.1f%%]",numf,nt,origtotal,float64(nt)*100.0/float64(origtotal))
                //fmt.Printf("%4d => %6d added %5d [%6d: %10d / %10d [%5.1f%%]]\n",mn,mx,len(r),numf,nt,origtotal,float64(nt)*100.0/float64(origtotal))
                
                nextm = numf+1000
            }
            

		}

        
		if foundzero {
			//finished
			break
		}
		mn -= 50
		if mn < minimum {
			mn = minimum
		}
		mx += 50
		if mx > 50000 {
			//failed
			//println("mx=", mx)
            if mx > 1000000 {
                break
            }
		}
	}
    nt:=nqtt.Get(0).total
    fmt.Printf("\r%6d: %10d / %10d [%5.1f%%]\n",numf,nt,origtotal,float64(nt)*100.0/float64(origtotal))
        
    if mx > 50000 {
        fmt.Println("maximum:",mx)
    }
    
    
	t0 := qtt.Get(0)
    
	if !foundzero && t0.total > 0 {
		//items left: dump to file and panic
		if qttCount(qtt) > 1 {
			println("foundzero=", foundzero, "t0.total", t0.total, "t0.quadTree", t0.quadTree, "qtt.Len()=", qtt.Len())
			of, _ := ioutil.TempFile(os.Getenv("GOPATH"), "calcqts")
			println("zero tile remains: dump tree", of.Name())
			func() {

				w := bufio.NewWriter(of)
				
				rc:=0
				for r := range qtt.Iter() {
					if rc==100 {
						println("too many")
					} else if rc <100 {
						println(r.String())
					}
					w.WriteString(r.String() + "\n")
					rc+=1
				}
				w.Flush()
			}()

			of.Close()
			panic("")
		}

		nqtt.AddMulti(0, t0.total)

	}
    if nqtt.Get(0).total != origtotal {
        panic(fmt.Sprintf("?? %s",nqtt))
    }
    
	return nqtt
}

// Construct QtTree from slice of quadtree values. This can be used to
// allocate a quadtree value to the correct tile of a QtTree
func MakeQtTree(inqts []quadtree.Quadtree) QtTree {
	qttree := newQtTree(0, len(inqts)*3/2/65536)
	for _, q := range inqts {

		qttree.Add(q)
	}
	return qttree
}
