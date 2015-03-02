// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    
    "fmt"
    "errors"
)

type CoordStore interface {
    Find(ref elements.Ref) (int64,int64,bool)
    Len() int
    AsCoordSlice() []Coord
}

type CoordBlockStore interface {
    CoordStore
    Add(elements.ExtendedBlock)
    NumBlocks() int
}

func getCoords(cs CoordStore, refs elements.Refs) ([]Coord,error) {
    res, ok :=  refs.(interface{Coords() []Coord})
    if ok {
        return res.Coords(),nil
    }
    
    ans:=make([]Coord, 0, refs.Len())    
    for i:=0; i < refs.Len(); i++ {
        c:=coordImpl{}
        r:=refs.Ref(i)
        c.ref = r
        ok := false
        c.lon,c.lat,ok = cs.Find(r)
        
        
        if !ok {
            return nil, errors.New(fmt.Sprintf("Missing node %d @ %d", r, i))
        }
        ans = append(ans, c)
    }
    return ans,nil
}
            
    

type lonLat struct {
    lon,lat int64
}

type mapCoordStore map[elements.Ref]lonLat

func (mcs mapCoordStore) Find(ref elements.Ref) (int64,int64,bool) {
    ll,ok := mcs[ref]
    return ll.lon,ll.lat,ok
}

func (mcs mapCoordStore) Len() int { return len(mcs) }
func (mcs mapCoordStore) AsCoordSlice() []Coord {
    rr:=make([]Coord,0,len(mcs))
    for k,v := range mcs {
        rr=append(rr, coordImpl{k,v.lon,v.lat})
    }
    return rr
}

type mapCoordBlockStore map[quadtree.Quadtree]CoordStore

func (mcbs mapCoordBlockStore) Find(ref elements.Ref) (int64,int64,bool) {
    for _,v:=range mcbs {
        a,b,c:= v.Find(ref)
        if c {
            return a,b,c
        }
    }
    return 0,0,false
}

func (mcbs mapCoordBlockStore) Len() int  {
    r:=0
    for _,v:=range mcbs {
        r+=v.Len()
    }
    return r
}

func (mcbs mapCoordBlockStore) AsCoordSlice() []Coord {
    rs := make([]Coord, mcbs.Len())
    i:=0
    
    for _,v:=range mcbs {
        copy(rs[i:], v.AsCoordSlice())
        i+=v.Len()
    }
    return rs
}

func (mcbs mapCoordBlockStore) Add(bl elements.ExtendedBlock) {
    nt := mapCoordStore{}
    for i:=0; i< bl.Len();i++ {
        e:=bl.Element(i)
        if e.Type()!=elements.Node {
            continue
        }
        ll,ok := e.(elements.LonLat)
        if !ok {
            continue
        }
        
        nt[e.Id()] = lonLat{ll.Lon(),ll.Lat()}
    }
    
    q := bl.Quadtree()
    tr:=make(quadtree.QuadtreeSlice,0,len(mcbs))
    for k,_:=range mcbs {
        if k.Common(q)!=k {
            tr=append(tr,k)
        }
        
    }
    //println("delete",len(tr),"tiles")
    
    for _,t:=range tr {
        mcbs[t]=nil
        delete(mcbs,t)
    }
    mcbs[q]=nt
    //println("have",len(mcbs),"tiles",mcbs.Len())
}

func (mcbs mapCoordBlockStore) NumBlocks() int { return len(mcbs) }


type coordWay struct {
    id      elements.Ref
    info    elements.Info
    tags    elements.Tags
    qt      quadtree.Quadtree
    ct      elements.ChangeType
    
    cc      []Coord
}
func (tw *coordWay) Type() elements.ElementType { return elements.Way }
func (tw *coordWay) Id() elements.Ref { return tw.id }
func (tw *coordWay) Info() elements.Info { return tw.info }
func (tw *coordWay) Tags() elements.Tags { return tw.tags }
func (tw *coordWay) ChangeType() elements.ChangeType { return tw.ct }
func (tw *coordWay) Quadtree() quadtree.Quadtree { return tw.qt }

func (tw *coordWay) SetChangeType(c elements.ChangeType) { tw.ct=c }
func (tw *coordWay) SetQuadtree(q quadtree.Quadtree) { tw.qt=q }

func (tw *coordWay) Len() int { return len(tw.cc) }
func (tw *coordWay) Ref(i int) elements.Ref { return tw.cc[i].Ref() }

func (tw *coordWay) Coords() []Coord { return tw.cc }

func (tw *coordWay) Pack() []byte {
    
    return elements.PackFullElement(tw,elements.PackRefs(tw))
}
func (tw *coordWay) String() string {
    return fmt.Sprintf("CoordsWay: %8d [%4d refs] %-18s",tw.id,len(tw.cc),tw.qt)
}

func makeCoordWay(cbs CoordBlockStore, e elements.Element) (elements.Element,*quadtree.Bbox, error) {
    ans := coordWay{}
    
    fw,ok := e.(elements.FullWay)
    if !ok {
        return nil, nil,errors.New("Not a FullWay")
    }
    var err error
    ans.cc, err = getCoords(cbs,fw)
    if err!=nil {
        return nil,nil,err
    }
    ans.id = fw.Id()
    ans.info = fw.Info()
    ans.tags=MakeTagsEditable(fw.Tags())
    ans.qt = fw.Quadtree()
    ans.ct = fw.ChangeType()
    
    return &ans,makeBbox(ans.cc),nil
}

func AddWayCoords(inc <- chan elements.ExtendedBlock, bx *quadtree.Bbox) <-chan elements.ExtendedBlock {
    ans := make(chan elements.ExtendedBlock)
    
    go func() {
        bs := mapCoordBlockStore{}
        idx:=0
        for bl := range inc {
            bs.Add(bl)
            nr := make(elements.ByElementId,0, bl.Len())
            for i:=0; i < bl.Len(); i++ {
                e := bl.Element(i)
                switch e.Type() {
                    case elements.Node:
                        fn := e.(elements.FullNode)
                        if bx!=nil && !bx.ContainsXY(fn.Lon(),fn.Lat()) {
                            continue
                        }
                        
                        
                        if fn.Tags()!=nil && fn.Tags().Len()>0 {
                            ne := elements.MakeNode(fn.Id(),
                                fn.Info(),MakeTagsEditable(fn.Tags()),
                                fn.Lon(),fn.Lat(),fn.Quadtree(),
                                fn.ChangeType())
                            
                            nr = append(nr, ne)
                        }
                    case elements.Way:
                        fw := e.(elements.FullWay)
                        tw,wbx,err:=makeCoordWay(bs,fw)
                        
                        
                        if err!=nil {
                            fmt.Println(bl,fw)
                            panic(err.Error())
                        }
                        if bx!=nil && !bx.Intersects(*wbx) {
                            continue
                        }
                        nr = append(nr,tw)
                    case elements.Relation:
                        fr := e.(elements.FullRelation)
                        tt := MakeTagsEditable(fr.Tags())
                        if tt.Has("type") {
                            switch tt.Get("type") {
                                case "boundary","multipolygon","route":
                                    
                                    nl := elements.MakeRelationCopy(
                                        fr.Id(), fr.Info(), tt, fr,
                                        fr.Quadtree(),fr.ChangeType())                    
                    
                                    nr = append(nr, nl)
                            }
                        }
                }
            }
            if len(nr)>0 {
                ans <- elements.MakeExtendedBlock(idx,nr,bl.Quadtree(),bl.StartDate(),bl.EndDate(),nil)
                idx++
            }
        }
        close(ans)
    }()
    return ans
}
    
    
    

