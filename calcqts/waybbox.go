// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package calcqts

import (
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/read"
    "github.com/jharris2268/osmquadtree/utils"

    "sort"
    "math"
    //"runtime/debug"
	"fmt"
	"sync"
    "time"
    "runtime"
)


type refSlice []elements.Ref

func (is refSlice) Len() int           { return len(is) }
func (is refSlice) Swap(i, j int)      { is[i], is[j] = is[j], is[i] }
func (is refSlice) Less(i, j int) bool { return is[i] < is[j] }
func (is refSlice) Sort()              { sort.Sort(is) }

var mxInt32 = int32(math.MaxInt32)
var mnInt32 = int32(math.MinInt32)

type quadtreeStore interface {
	Get(elements.Ref) quadtree.Quadtree
	Set(elements.Ref, quadtree.Quadtree)
	Expand(elements.Ref, quadtree.Quadtree)

	Len() int
	ObjsIter(elements.ElementType, int) <-chan elements.ByElementId
    
}

type quadtreeTile interface {
    Get(int) quadtree.Quadtree
    Set(int, quadtree.Quadtree) bool
    Expand(int, quadtree.Quadtree) bool
    AsElements(elements.ElementType, elements.Ref,int,int) []elements.ByElementId
    Clear()
    Len() int
}


type quadtreeStoreTiled interface {
    SetTile(key elements.Ref, tile quadtreeTile)
}

type wayBbox interface {
	Expand(elements.Ref, int64, int64)
	Get(elements.Ref) quadtree.Bbox
	Qts(quadtreeStore, uint, float64) quadtreeStore
	Len() int
    NumTiles() int
}


type wayBboxTile interface {
    Expand(int,int64,int64) bool
    Get(int) quadtree.Bbox
    CalcQuadtree(float64,uint) (int,quadtreeTile)
    Len() int
}



const tileLen = 1 << 18
var tileSz = elements.Ref(tileLen)


type quadtreeTileImpl struct {
    values []int64
    length int
}
func newQuadtreeTile() quadtreeTile {
    return &quadtreeTileImpl{make([]int64,tileSz),0}
}

func (qtt *quadtreeTileImpl) Get(i int) quadtree.Quadtree {
    return quadtree.Quadtree(qtt.values[i]-1)
}

func (qtt *quadtreeTileImpl) Set(i int, q quadtree.Quadtree) bool {
    cv:=qtt.values[i]==0
    qtt.values[i] = int64(q+1)
    if cv {
        qtt.length++
    }
    return cv
}

func (qtt *quadtreeTileImpl) Expand(i int, q quadtree.Quadtree) bool {
    cv:=qtt.Get(i)
    qtt.Set(i, q.Common(cv))
    if cv==quadtree.Null {
        qtt.length++
    }
    return cv==quadtree.Null
}

func (qtt *quadtreeTileImpl) Clear() {
    qtt.values=nil
}

func (qtt *quadtreeTileImpl) AsElements(objT elements.ElementType, off elements.Ref, first int, blckSz int) []elements.ByElementId {
    ans:=make([]elements.ByElementId, 0, qtt.length/blckSz+1)
    curr:=make(elements.ByElementId, 0, first)
    for i,q:=range qtt.values {
        if q>0 {
            curr=append(curr, read.MakeObjQt(objT, elements.Ref(i)+off, quadtree.Quadtree(q-1)))
            if len(curr)==cap(curr) {
                ans=append(ans,curr)
                curr=make(elements.ByElementId, 0, blckSz)
            }
            
        }
    }
    if len(curr)>0 {
        ans=append(ans,curr)
    }
    return ans
}

func (qtt *quadtreeTileImpl) Len() int {
    return qtt.length
}

type quadtreeTileMap map[int]quadtree.Quadtree

func (qtt quadtreeTileMap) Get(i int) quadtree.Quadtree {
    a,ok := qtt[i]
    if !ok { return quadtree.Null}
    return a
}

func (qtt quadtreeTileMap) Set(i int, q quadtree.Quadtree) bool {
    a,ok := qtt[i]
    if !ok {
        qtt[i]=q
        return true
    }
    qtt[i]=a
    return false
}

func (qtt quadtreeTileMap) Expand(i int, q quadtree.Quadtree) bool {
    a,ok := qtt[i]
    if !ok {
        qtt[i]=q
        return true
    }
    qtt[i]=a.Common(q)
    return false
}

func (qtt quadtreeTileMap) Clear() {
    
}

func (qtt quadtreeTileMap) AsElements(objT elements.ElementType, off elements.Ref, first int, blckSz int) []elements.ByElementId {
    ans := make(elements.ByElementId, 0, len(qtt))
    for i,q := range qtt {
        ans=append(ans, read.MakeObjQt(objT, elements.Ref(i)+off, q))
    }
    ans.Sort()
    if len(ans) < first {
        return []elements.ByElementId{ans}
    }
    res := make([]elements.ByElementId, 0, 1+len(ans)/blckSz)
    res = append(res, ans[:first])
    for vv := first; vv < len(ans); vv += blckSz {
        lv := vv+blckSz
        if lv > len(ans) { lv = len(ans) }
        res=append(res, ans[vv:lv])
    }
    tl:=0
    for _,v:=range res { tl+=len(v) }
    if tl!=len(qtt) {
        fmt.Println(off>>18,"first=",first,"; ",len(qtt),"qts to ",len(res),"blocks [",len(res[0]),",",len(res[len(res)-1]),"]=",tl)
    }
    return res
}


func (qtt quadtreeTileMap) Len() int {
    return len(qtt)
}

type quadtreeStoreImpl struct {
	tiles  map[elements.Ref]quadtreeTile
	length int
}

func (oqi *quadtreeStoreImpl) Get(id elements.Ref) quadtree.Quadtree {
	k := id / tileSz
    tl,ok := oqi.tiles[k]
    if !ok { return quadtree.Null }
    return tl.Get(int(id % tileSz))
}

func (oqi *quadtreeStoreImpl) Set(id elements.Ref, qt quadtree.Quadtree) {
	k := id / tileSz
    tl,ok := oqi.tiles[k]
    if !ok {
        tl=newQuadtreeTile()
        oqi.tiles[k]=tl
    }
    if tl.Set(int(id % tileSz),qt) {
        oqi.length++
    }
}

func (oqi *quadtreeStoreImpl) Expand(id elements.Ref, qt quadtree.Quadtree) {
	k := id / tileSz
	tl,ok := oqi.tiles[k]
    if !ok {
        tl=newQuadtreeTile()
        oqi.tiles[k]=tl
    }
    if tl.Expand(int(id % tileSz),qt) {
        oqi.length++
    }

}

func (oqi *quadtreeStoreImpl) Clear() {
    
    for k,v:=range oqi.tiles {
        v.Clear()
        delete(oqi.tiles,k)
    }
}

func (oqi *quadtreeStoreImpl) Len() int {
	return oqi.length
}

func (oqi *quadtreeStoreImpl) ObjsIter(objT elements.ElementType, blckSz int) <-chan elements.ByElementId {
	res := make(chan elements.ByElementId)
	
    kk := make(refSlice, 0, len(oqi.tiles))
	for k, _ := range oqi.tiles {
		kk = append(kk, k)
	}
	
    kk.Sort()
	nnuls := 0
	go func() {
		curr := make(elements.ByElementId, 0, blckSz)
		for _, k := range kk {
            
			t := oqi.tiles[k]
			ks := k * tileSz
            
            for _,o:=range t.AsElements(objT, ks, blckSz-len(curr),blckSz) {
                if len(curr)==0 && len(o)==blckSz {
                    res <- o
                } else {
                    curr = append(curr, o...)
                    if len(curr) == blckSz {
                        res <- curr
                        curr = make(elements.ByElementId, 0, blckSz)
                    }
                }
            }
            t.Clear()
            
		}
		if len(curr) > 0 {
			res <- curr
		}
        if (nnuls > 0) {
            fmt.Printf("set %d nulls to 0\n", nnuls)
        }
		close(res)
	}()
	return res
}

func (qtt *quadtreeStoreImpl) SetTile(key elements.Ref, tile quadtreeTile) {
    if _,ok := qtt.tiles[key]; ok {
        panic(fmt.Sprintf("tile %d already present", key))
    }
    
    qtt.tiles[key] = tile
    qtt.length += tile.Len()
}


func newQuadtreeStore(useDense bool) quadtreeStore {
    if useDense {
        return &quadtreeStoreImpl{map[elements.Ref]quadtreeTile{}, 0}
    }
    return quadtreeStoreMap{}
}


type quadtreeStoreMap map[int64]quadtree.Quadtree
func (oqm quadtreeStoreMap) Get(r elements.Ref) quadtree.Quadtree {
    a,ok:=oqm[int64(r)]
    if !ok {
        return quadtree.Null
    }
    return a
}

func (oqm quadtreeStoreMap) Set(r elements.Ref, q quadtree.Quadtree) {
    oqm[int64(r)]=q
}

func (oqm quadtreeStoreMap) Expand(r elements.Ref, q quadtree.Quadtree) {
    oq := oqm.Get(r)
    nq := oq.Common(q)
    oqm.Set(r,nq)
}

func (oqm quadtreeStoreMap) Len() int { return len(oqm) }
func (oqm quadtreeStoreMap)	ObjsIter(objT elements.ElementType, bs int) <-chan elements.ByElementId {
    ks:=make(utils.Int64Slice, 0,len(oqm))
    for k,_ := range oqm {
        ks=append(ks,k)
    }
    ks.Sort()
    cc:=make(chan elements.ByElementId)
    go func() {
        for i:=0; i < len(ks); i+=bs {
            bss:=bs
            if len(ks)-i < bs {
                bss=len(ks)-i
            }
            bb:=make(elements.ByElementId, bss)
            for j,k := range ks[i:i+bss] {
                bb[j] = read.MakeObjQt(objT, elements.Ref(k), oqm[k])
            }
            cc <- bb
        }
        close(cc)
    }()
    return cc
}

func (oqm quadtreeStoreMap) Clear() { oqm=nil }




type wayBboxMap map[int64]*quadtree.Bbox

func (wbm wayBboxMap) Expand(r elements.Ref, ln int64, lt int64) {
    
    ri := int64(r)
    bx,ok := wbm[ri]
    if !ok {
        bx = &quadtree.Bbox{ln,lt,ln,lt}
    } else {
        bx = bx.ExpandXY(ln,lt)
    }
    wbm[ri] = bx
}
func (wbm wayBboxMap) Get(r elements.Ref) quadtree.Bbox {
    
    ri := int64(r)
    bx,ok := wbm[ri]
    if !ok {
        return *quadtree.NullBbox()
    }
    return *bx
}

func (wbm wayBboxMap) Len() int { return len(wbm) }
func (wbm wayBboxMap) NumTiles() int { return 1 }

func (wbm wayBboxMap) Qts(store quadtreeStore, md uint, buf float64) quadtreeStore {
    qq,ok := store.(quadtreeStoreMap)
    if !ok {
        panic("wrong type of quadtreeStore")
    }
    for k,v := range wbm {
        
        q,err := quadtree.Calculate(*v, buf, md)
        if err!=nil { panic(err.Error()) }
        qq[k]=q
    }
    return store
}


type wayBboxTileImpl struct {
    minLon, minLat []int32
    maxLon, maxLat []int32
    length int
}

func newWayBboxTile() wayBboxTile {
    tt := &wayBboxTileImpl{make([]int32,tileSz),make([]int32,tileSz),make([]int32,tileSz),make([]int32,tileSz),0}
    for i,_ := range tt.minLon {
        tt.minLon[i]=mxInt32
        tt.minLat[i]=mxInt32
        tt.maxLon[i]=mnInt32
        tt.maxLat[i]=mnInt32
    }
    return tt
}

func (wbt *wayBboxTileImpl) Expand(j int, ln, lt int64) bool {
    nv := wbt.minLon[j] == mxInt32
	if int32(ln) < wbt.minLon[j] {
		wbt.minLon[j] = int32(ln)
	}
	if int32(lt) < wbt.minLat[j] {
		wbt.minLat[j] = int32(lt)
	}
	if int32(ln) > wbt.minLat[j] {
		wbt.minLat[j] = int32(ln)
	}
	if int32(lt) > wbt.maxLat[j] {
		wbt.maxLat[j] = int32(lt)
	}
    if nv {
        wbt.length++
    }
    return nv
}

func (wbt *wayBboxTileImpl) Get(j int) quadtree.Bbox {
    if wbt.minLon[j] == mxInt32 || wbt.minLat[j] == mxInt32 {
		return *quadtree.NullBbox()
	}
	return quadtree.Bbox{int64(wbt.minLon[j]), int64(wbt.minLat[j]), int64(wbt.maxLon[j]), int64(wbt.maxLat[j])}
}

func (wbt *wayBboxTileImpl) CalcQuadtree(buf float64, md uint) (int,quadtreeTile) {
    ll:=0
    res:=newQuadtreeTile()
    for i:=0; i < tileLen; i++ {
        if wbt.minLon[i]!=mxInt32 {
            q,err := quadtree.Calculate(wbt.Get(i), buf, md)
            if err!=nil { panic(err.Error()) }
            if res.Set(i,q) {
                ll++
            }
        }
    }
    return ll,res
}
func (wbt *wayBboxTileImpl) Len() int { return wbt.length }


type wayBboxTileMap map[int]*quadtree.Bbox

func (wbm wayBboxTileMap) Expand(ri int, ln int64, lt int64) bool {
    
    
    bx,ok := wbm[ri]
    if !ok {
        bx = &quadtree.Bbox{ln,lt,ln,lt}
    } else {
        bx = bx.ExpandXY(ln,lt)
    }
    wbm[ri] = bx
    return !ok
}
func (wbm wayBboxTileMap) Get(ri int) quadtree.Bbox {
    
    
    bx,ok := wbm[ri]
    if !ok {
        return *quadtree.NullBbox()
    }
    return *bx
}


func (wbm wayBboxTileMap) CalcQuadtree(buf float64, md uint) (int,quadtreeTile) {
    
    qq := quadtreeTileMap{}
    
    for k,v := range wbm {
        
        q,err := quadtree.Calculate(*v, buf, md)
        if err!=nil { panic(err.Error()) }
        qq[k]=q
    }
    return len(qq),qq
}

func (wbm wayBboxTileMap) Len() int { return len(wbm) }

type wayBboxStoreImpl struct {
    tiles map[elements.Ref]wayBboxTile
	length int
    newTile func() wayBboxTile 
}

func (wbi *wayBboxStoreImpl) Len() int { return wbi.length }
func (wbi *wayBboxStoreImpl) NumTiles() int { return len(wbi.tiles) }
func (wbi *wayBboxStoreImpl) Expand(id elements.Ref, ln, lt int64) {
	k := id / tileSz
	if _, ok := wbi.tiles[k]; !ok {
		wbi.tiles[k] = wbi.newTile()
	}
    if wbi.tiles[k].Expand(int(id % tileSz),ln,lt) {
        wbi.length++
    }
}

func (wbi *wayBboxStoreImpl) Get(id elements.Ref) quadtree.Bbox {
	k := id / tileSz
	if _, ok := wbi.tiles[k]; !ok {
		return *quadtree.NullBbox()
	}
	
	j := (id % tileSz)
    return wbi.tiles[k].Get(int(j))
}


type wayBoxTileTemp struct {
    key elements.Ref
    tile wayBboxTile
}

type quadtreeTileTemp struct {
    key elements.Ref
    tile quadtreeTile
}

func (wbi *wayBboxStoreImpl) Qts(store quadtreeStore, md uint, buf float64) quadtreeStore {
   
    
    nqt:=store.Len()
    st:=time.Now()
    
    addTiles, ok := store.(quadtreeStoreTiled)
    if !ok {
        panic("wrong type of store passed")
    }
        
    
    gct:=0.0
    
    wbc := make(chan wayBoxTileTemp)
    go func() {
        z := 0
        for k,v := range wbi.tiles {
            wbc <- wayBoxTileTemp{k,v}
            delete(wbi.tiles, k)
            z++
            if (z%100) == 0 {
                ss:=time.Now()
                runtime.GC()
                gct+=time.Since(ss).Seconds()
            }
        }
        close(wbc)
    }()
    
    qtc := make(chan quadtreeTileTemp)
    go func() {
        wg:=sync.WaitGroup{}
        wg.Add(4)    
        for s := 0; s < 4; s++ {
            go func() {
                
                for w := range wbc {
                    _,qts := w.tile.CalcQuadtree(buf,md)
                    qtc <- quadtreeTileTemp{w.key, qts}
                    
                }
                wg.Done()
            }()
        }
        wg.Wait()
        close(qtc)
    }()
	
    for q := range qtc {
        addTiles.SetTile(q.key, q.tile)
    }
    fmt.Printf("calculated %d qts [%8.1fs / %8.1fs GC]\n", store.Len()-nqt, time.Since(st).Seconds(),gct)
    return store
}
            
func newWayBbox(ty int) wayBbox {
    if ty==0 {
        return wayBboxMap{}
    }
    
        
    nt:=newWayBboxTile
    switch ty {
        case 2: nt = newWayBboxTileCgo
        case 3: nt = func() wayBboxTile { return wayBboxTileMap{} }
    }
    return &wayBboxStoreImpl{map[elements.Ref]wayBboxTile{}, 0, nt}
    
}



