package calcqts

import (
	"fmt"
	"math"
	"github.com/jharris2268/osmquadtree/read"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/elements"
	"sort"
	"sync"
    
    "runtime/debug"
)

type refSlice []elements.Ref

func (is refSlice) Len() int           { return len(is) }
func (is refSlice) Swap(i, j int)      { is[i], is[j] = is[j], is[i] }
func (is refSlice) Less(i, j int) bool { return is[i] < is[j] }
func (is refSlice) Sort()              { sort.Sort(is) }

var mxInt32 = int32(math.MaxInt32)
var mnInt32 = int32(math.MinInt32)

type objQt interface {
	Get(elements.Ref) quadtree.Quadtree
	Set(elements.Ref, quadtree.Quadtree)
	Expand(elements.Ref, quadtree.Quadtree)

	Len() int
	ObjsIter(elements.ElementType, int) <-chan elements.ByElementId
}

const tileLen = 1 << 14
var tileSz = elements.Ref(tileLen)

type qtTile struct {
    t [tileLen]int64
}

type objQtImpl struct {
	tt map[elements.Ref]*qtTile
	ln int
}

func (oqi *objQtImpl) Get(id elements.Ref) quadtree.Quadtree {
	k := id / tileSz
	if _, ok := oqi.tt[k]; !ok {
        //println("??",id,k,ok)
		return quadtree.Null
	}
    r:=oqi.tt[k].t[id%tileSz] - 1
    /*if r<0 {
        println("!!",id,k,id%tileSz)
    }*/
	return quadtree.Quadtree(r)
}

func (oqi *objQtImpl) Set(id elements.Ref, qt quadtree.Quadtree) {
	k := id / tileSz
	if _, ok := oqi.tt[k]; !ok {
		//oqi.tt[k] = make(qtTile, tileSz)
        oqi.tt[k] = &qtTile{}
	}
	c := oqi.tt[k].t[id%tileSz]
	if c == 0 {
		oqi.ln++
	}
	oqi.tt[k].t[id%tileSz] = int64(qt) + 1
}

func (oqi *objQtImpl) Expand(id elements.Ref, qt quadtree.Quadtree) {
	k := id / tileSz
	if _, ok := oqi.tt[k]; !ok {
		//oqi.tt[k] = make(qtTile, tileSz)
        oqi.tt[k] = &qtTile{}
	}
	c := oqi.tt[k].t[id%tileSz] - 1
	if c < 0 {
		if c == 0 {
			oqi.ln++
		}
		oqi.tt[k].t[id%tileSz] = int64(qt) + 1
	} else {
		oqi.tt[k].t[id%tileSz] = int64(qt.Common(quadtree.Quadtree(c)) + 1)
	}

}

func (oqi *objQtImpl) Len() int {
	return oqi.ln
}

func (oqi *objQtImpl) ObjsIter(objT elements.ElementType, blckSz int) <-chan elements.ByElementId {
	res := make(chan elements.ByElementId)
	
    kk := make(refSlice, 0, len(oqi.tt))
	for k, _ := range oqi.tt {
		kk = append(kk, k)
	}
	
    kk.Sort()
	nnuls := 0
	go func() {
		curr := make(elements.ByElementId, 0, blckSz)
		for _, k := range kk {
			t := oqi.tt[k].t
			ks := k * tileSz
			for i, q := range t {
				if q != 0 {
					if q < 0 {
						nnuls++
						q = 1
					}
					curr = append(curr, read.MakeObjQt(objT, elements.Ref(i)+ks, quadtree.Quadtree(q-1)))
					if len(curr) == blckSz {
						res <- curr
						curr = make(elements.ByElementId, 0, blckSz)
					}
				}
			}
		}
		if len(curr) > 0 {
			res <- curr
		}
		fmt.Printf("set %d nulls to 0\n", nnuls)
		close(res)
	}()
	return res
}

func newObjQtImpl() objQt {
	return &objQtImpl{map[elements.Ref]*qtTile{}, 0}
}

type wayBbox interface {
	Expand(elements.Ref, int64, int64)
	Get(elements.Ref) quadtree.Bbox
	Qts(objQt, uint, float64) objQt
	Len() int
    NumTiles() int
}

type wayBboxTile struct {
    n [tileLen*4]int32
}

func newTile() *wayBboxTile {
	//n := make(wayBboxTile, tileSz*4)
    n := wayBboxTile{}
    //println("newTile",tileLen*4,len(n.n))
	for i, _ := range n.n {
		switch i % 4 {
		case 0, 1:
			n.n[i] = mxInt32
		case 2, 3:
			n.n[i] = mnInt32
		}
	}

	return &n
}

type wayBboxImpl struct {
	ts map[elements.Ref]*wayBboxTile
	cc int
}

func (wbi *wayBboxImpl) Len() int { return wbi.cc }
func (wbi *wayBboxImpl) NumTiles() int { return len(wbi.ts) }
func (wbi *wayBboxImpl) Expand(id elements.Ref, ln, lt int64) {
	k := id / tileSz
	if _, ok := wbi.ts[k]; !ok {
		wbi.ts[k] = newTile()
	}

	j := (id % tileSz) * 4
	v := wbi.ts[k]
	if v.n[j] == mxInt32 {
		wbi.cc++
	}
	if int32(ln) < v.n[j+0] {
		v.n[j+0] = int32(ln)
	}
	if int32(lt) < v.n[j+1] {
		v.n[j+1] = int32(lt)
	}
	if int32(ln) > v.n[j+2] {
		v.n[j+2] = int32(ln)
	}
	if int32(lt) > v.n[j+3] {
		v.n[j+3] = int32(lt)
	}
	//wbi.ts[k] = v
}

func (wbi *wayBboxImpl) Get(id elements.Ref) quadtree.Bbox {
	k := id / tileSz
	if _, ok := wbi.ts[k]; !ok {
		return *quadtree.NullBbox()
	}
	v := wbi.ts[k].n
	j := (id % tileSz) * 4
	if v[j] == mxInt32 || v[j+1] == mxInt32 {
		return *quadtree.NullBbox()
	}
	return quadtree.Bbox{int64(v[j]), int64(v[j+1]), int64(v[j+2]), int64(v[j+3])}
}


type wbt struct {
    k elements.Ref
    t *wayBboxTile
}

type qtt struct {
    k elements.Ref
    t *qtTile
    l int
}

func (wbi *wayBboxImpl) Qts(rr objQt, md uint, buf float64) objQt {
    r := rr.(*objQtImpl)
    
    wbc := make(chan wbt)
    go func() {
        z := 0
        for k,v := range wbi.ts {
            wbc <- wbt{k,v}
            delete(wbi.ts, k)
            z++
            if (z%1271) == 0 {
                debug.FreeOSMemory()
            }
        }
        close(wbc)
    }()
    
    qtc := make(chan qtt)
    go func() {
        wg:=sync.WaitGroup{}
        wg.Add(4)    
        for s := 0; s < 4; s++ {
            go func() {
                
                for w := range wbc {
                    ll := 0
                    k := w.k
                    v := w.t.n
                    nv := &qtTile{}
                    for i, _ := range nv.t {
                        j := 4 * i
                        //var err error
                        if v[j] != mxInt32 {
                            bx := quadtree.Bbox{int64(v[j]), int64(v[j+1]), int64(v[j+2]), int64(v[j+3])}
                            q,err := quadtree.Calculate(bx, buf, md)
                            if err!=nil { panic(err.Error()) }
                            nv.t[i] = int64(q+1)
                            ll++
                        }
                    }
                    qtc <- qtt{k,nv,ll}

                }
                wg.Done()
            }()
        }
        wg.Wait()
        close(qtc)
    }()
	
    for q := range qtc {
        r.tt[q.k] = q.t
        r.ln += q.l
    }
        
    return r
}
            
    


func (wbi *wayBboxImpl) Qtsz(rr objQt, md uint, buf float64) objQt {
	r := rr.(*objQtImpl)

	qc := make(chan elements.Ref)
	go func() {
		for k, _ := range wbi.ts {
			qc <- k
		}
		close(qc)
	}()
	//println(len(r.tt))
	ss := sync.Mutex{}
	rc := make(chan int)
    zz:=0
	for s := 0; s < 4; s++ {
		go func() {
			ll := 0
			for k := range qc {
				v := wbi.ts[k].n
				nv := &qtTile{} //make(qtTile, tileSz)
				for i, _ := range nv.t {
					j := 4 * i
                    //var err error
					if v[j] != mxInt32 {
						bx := quadtree.Bbox{int64(v[j]), int64(v[j+1]), int64(v[j+2]), int64(v[j+3])}
						q,err := quadtree.Calculate(bx, buf, md)
                        if err!=nil { panic(err.Error()) }
                        nv.t[i] = int64(q)+1
						ll++
					}
				}
				ss.Lock()
				if _, ok := r.tt[k]; ok {
					panic(fmt.Sprintf("tile", k, "already present??"))
				}
				r.tt[k] = nv
				delete(wbi.ts, k)
                zz++
                if (zz%1371)==0 {
                    debug.FreeOSMemory()
                }
				ss.Unlock()

			}
			rc <- ll
            //println("added", ll)
		}()
	}
	for s := 0; s < 4; s++ {
		r.ln += <-rc
	}
	return r
}

func newWayBbox() wayBbox {
	return &wayBboxImpl{map[elements.Ref]*wayBboxTile{}, 0}
}
