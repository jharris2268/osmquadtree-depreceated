// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "sync"
    "errors"
    "fmt"
    "io/ioutil"
    "os"
    //"time"
    //"runtime/debug"
)

func nodeTags(tags TagsEditable, tagsFilter map[string]TagTest) bool {
    rms := make([]string,0,tags.Len())
    isfeat := false
    for i:=0; i < tags.Len(); i++ {
        tt,ok := tagsFilter[tags.Key(i)]
        if !ok {
            rms=append(rms,tags.Key(i))
            continue
        }
        if !tt.IsNode {
            rms=append(rms,tags.Key(i))
            continue
        }
        if tt.IsFeature {
            isfeat=true
        }
    }
    for _,t:=range rms {
        tags.Delete(t)
    }
    return isfeat
}

// IsFeature returns true if at least one tag key from tags is marked as
// IsFeature in tagsFilter
func IsFeature(tags elements.Tags, tagsFilter map[string]TagTest) bool {
    for i := 0; i < tags.Len(); i++ {
        k:=tags.Key(i)
        if tt,ok := tagsFilter[k]; ok {
            if tt.IsWay && tt.IsFeature {
                return true
            }
        }
    }
    return false
}

func wayTags(tags TagsEditable, tagsFilter map[string]TagTest) (int64,bool) {
    isp := false
    rms := make([]string,0,tags.Len())
    for i:=0; i < tags.Len(); i++ {
        tt,ok := tagsFilter[tags.Key(i)]
        if !ok {
            rms=append(rms,tags.Key(i))
            continue
        }
        if !tt.IsWay {
            rms=append(rms,tags.Key(i))
            continue
        }
        if (tt.IsPoly=="yes") || ( (tags.Key(i)=="area") && is_true(tags.Value(i))) {
            isp = true
        }
    }
    
    if tags.Has("boundary") /* && tags.Get("boundary")=="administrative"*/ {
        isp=true
    }
    
    for _,t:=range rms {
        tags.Delete(t)
    }
    
    zo,_ := find_zorder(tags)
    
    return zo,isp
}


// MakeGeometries converts the nodes and ways of input chan inc into
// Point, Linestring and Polygon geometries. Tags are filtered by tagsFilter.
// Note that the way objects must also satisify the Coorder interface, 
// so the input chan inc should be be result of AddWayCoords. Relations
// objects are passed to the output chanel directly: these are handled by
// HandleRelations.
func MakeGeometries(inc <-chan elements.ExtendedBlock, tagsFilter map[string]TagTest) <-chan elements.ExtendedBlock {
    
    res := make(chan elements.ExtendedBlock)
    
    go func() {
        for bl:=range inc {
            nb:=make(elements.ByElementId, 0, bl.Len())
            
            for i:=0; i < bl.Len(); i++ {
                e:=bl.Element(i)
                switch e.Type() {
                    case elements.Node:
                        fn := e.(elements.FullNode)
                        ok := nodeTags(fn.Tags().(TagsEditable), tagsFilter)
                        if ok {
                            nb = append(nb, makePointGeometry(fn, fn.Tags(), coordImpl{fn.Id(),fn.Lon(),fn.Lat()}))
                        }
                    case elements.Way:
                        fw := e.(elements.FullWay)
                        zo,isp := wayTags(fw.Tags().(TagsEditable), tagsFilter)
                        
                        cc := fw.(interface{Coords() []Coord}).Coords()
                        if isp {
                            isp = check_ring(cc)
                        }
                        
                        if isp {
                            p := [][]Coord{cc}
                            ar,_ := calculate_polygon_area(p)
                            nb = append(nb, makePolygonGeometry(fw,fw.Tags(),  p, zo, ar))
                        } else {
                            nb = append(nb, makeLinestringGeometry(fw, fw.Tags(), cc, zo))
                        }
                        
                    default:
                        nb=append(nb,e)
                }
            }
            
            nb.Sort()
            res <- elements.MakeExtendedBlock(bl.Idx(),nb,bl.Quadtree(),bl.StartDate(),bl.EndDate(),nil)
        }
        close(res)
    }()
    
    return res
}

func addWays(wayp map[elements.Ref]bool, mems elements.Members) {
    if mems.Len()==0 {
        return
    }
    for i:=0; i < mems.Len(); i++ {
        if mems.MemberType(i) == elements.Way {
            wayp[mems.Ref(i)]=true
        }
        
    }
}

func relType(e elements.Element) string {
    tt := e.(interface{Tags() elements.Tags}).Tags()
    if tte,ok := tt.(TagsEditable); ok {
        return tte.Get("type")
    }
    for i:=0; i < tt.Len(); i++ {
        if tt.Key(i)=="type" {
            return tt.Value(i)
        }
    }
    return ""
}


// HandleRelations generates multipolygon geometries from the
// boundary=administrative and mulitpolygon relations present in the input
// chan inc. The way members specified in a relation are merged together
// to form complete rings. If one valid ring is found then a Polygon
// geometry is produced, otherwise a Multipolyon. Any tags present in both
// the relation and the way geometries forming the outer rings are removed
// from the Linestring / Polygons. If no tags are left the objects are
// dropped. If the these objects still have tags, and are still features,
// then they are added to the output channel along with the objects forming
// the inner rings. This behaviour should be similar to the osm2pgsql
// application.
func HandleRelations(inc <-chan elements.ExtendedBlock, tagsFilter map[string]TagTest) <-chan elements.ExtendedBlock {
    
    res := make(chan elements.ExtendedBlock)
    
    relc := make(chan elements.ExtendedBlock)
    wg:=sync.WaitGroup{}
    wg.Add(1)
    
    
    go func() {
        err:=finishRelations(relc, res, tagsFilter)
        if err!=nil {
            panic(err.Error())
        }
        wg.Done()
    }()
    
    go func() {
        wayp := map[elements.Ref]bool{}
        ii := 0
        for bl:=range inc {
           
            rb:=make(elements.ByElementId, 0, bl.Len())
            nb:=make(elements.ByElementId, 0, bl.Len())
            for i:=0; i < bl.Len(); i++ {
                e:=bl.Element(i)
                switch e.Type() {
                    case elements.Node,elements.Way:
                        panic("wtf")
                    case elements.Relation:
                        switch relType(e) {
                            case "boundary","multipolygon":
                                addWays(wayp, e.(elements.Members))
                                rb=append(rb,e)
                        }
                        
                    case elements.Geometry:
                        if e.Id()>>59 == 1 {
                            ei := e.Id() & 0xffffffffffff
                            if _,ok := wayp[ei]; ok {
                                rb=append(rb,e)
                                delete(wayp,ei)
                            } else {
                                nb = append(nb, e)
                            }
                        } else {
                            nb = append(nb, e)
                        }
                }
            }
            
            nb.Sort()
            if len(nb)>0 {
                //println("ret",ii,bl.Quadtree().String())
                res <- elements.MakeExtendedBlock(ii,nb,bl.Quadtree(),bl.StartDate(),bl.EndDate(),nil)
                ii++
            }
            if len(rb)>0 {
                //println("waiting for relc",ii,bl.Quadtree().String())
                relc <- elements.MakeExtendedBlock(ii,rb,bl.Quadtree(),bl.StartDate(),bl.EndDate(),nil)
                ii++
            }
        }
        close(relc)
        //println("waiting for rels...")
        wg.Wait()
        close(res)
    }()
    
    return res
}



type pendingEle struct {
    qt quadtree.Quadtree
    ee elements.FullElement
    ww map[elements.Ref]bool
}

type pendingEleMap map[elements.Ref]*pendingEle

func getLinestringCoords(ls LinestringGeometry) []Coord {
    lsi,ok := ls.(*linestringGeometryImpl)
    if ok {
        return lsi.coords
    }
    res:=make([]Coord,ls.NumCoords())
    for i,_:=range res {
        res[i] = ls.Coord(i)
    }
    return res
}

func getPolygonCoords(ls PolygonGeometry) []Coord {
    lsi,ok := ls.(*polygonGeometryImpl)
    if ok {
        return lsi.coords[0]
    }
    res:=make([]Coord,ls.NumCoords(0))
    for i,_:=range res {
        res[i] = ls.Coord(0,i)
    }
    return res
}


func joinrings(A []Coord, revA bool, B []Coord, revB bool) ([]Coord, elements.Ref, elements.Ref) {
	ans := make([]Coord, len(A)+len(B)-1)
	for i, p := range A {
		j := i
		if revA {
			j = len(A) - 1 - i
		}
		ans[j] = p
	}
	for i, p := range B {
		j := len(A) - 1 + i
		if revB {
			j = len(B) + len(A) - 2 - i
		}
		ans[j] = p
	}
	return ans, ans[0].Ref(), ans[len(ans)-1].Ref()
}

func check_ring(rr []Coord) bool {
	if len(rr) < 3 {
		return false
	}
	if rr[0].Ref() != rr[len(rr)-1].Ref() {
		return false
	}
	return true
}


func merge_rings(rings [][]Coord) ([][]Coord, error) {
	if len(rings) == 0 {
		return nil, nil
	}
	if len(rings) == 1 {
		return rings, nil
	}

	var r0 []Coord
	a, b := elements.Ref(0), elements.Ref(0)

	rem := make([][]Coord, 0, len(rings))
	fin := make([][]Coord, 0, len(rings))
	added := false

	for _, r := range rings {
		//if i==0 { continue }
		if r[0].Ref() == r[len(r)-1].Ref() {
			fin = append(fin, r)
		} else if r0 == nil {
			r0 = r
			a = r0[0].Ref()
			b = r0[len(r0)-1].Ref()
		} else if r[0].Ref() == a {
			r0, a, b = joinrings(r0, true, r, false)
			added = true
		} else if r[0].Ref() == b {
			r0, a, b = joinrings(r0, false, r, false)
			added = true
		} else if r[len(r)-1].Ref() == a {
			r0, a, b = joinrings(r0, true, r, true)
			added = true
		} else if r[len(r)-1].Ref() == b {
			r0, a, b = joinrings(r0, false, r, true)
			added = true
		} else {
			rem = append(rem, r)
			//added=true
		}

	}
	if r0 != nil {
		if a == b || !added {
			fin = append(fin, r0)
		} else {
			rem = append(rem, r0)
		}
	}

	if len(rem) > 0 {

		pp, err := merge_rings(rem)
		if err != nil {
			return nil, err
		}
		fin = append(fin, pp...)
	}

	return fin, nil
	/*
	   //rm[0]=r0
	   if added {
	       return mergeRings(rm)
	   }
	   return rm, nil*/
}

func group_rings(outers [][]Coord, inners [][]Coord, allowLoose bool) ([][][]Coord, error) {
	if len(outers) == 1 {
		nobj := make([][]Coord, len(inners)+1)
		nobj[0] = outers[0]
		copy(nobj[1:], inners)
		return [][][]Coord{nobj}, nil
	}
	if len(inners) == 0 {
		ans := make([][][]Coord, len(outers))
		for i, o := range outers {
			ans[i] = [][]Coord{o}
		}
		return ans, nil
	}

	ans := make([][][]Coord, len(outers))

	for i, o := range outers {
		ans[i] = make([][]Coord, 0, len(inners)+1)
		ans[i] = append(ans[i], o)
	}

	for z, in := range inners {
		added := false
		for i, o := range ans {
			if ring_contains(o[0], in) {
				ans[i] = append(ans[i], in)
				added = true
				continue
			}
		}
		if !added && !allowLoose {
			return nil, errors.New(fmt.Sprintf("inner %d not contained by any outer", z))
		}
	}

	return ans, nil

	//return nil, errors.New("multi outers and multi inners")
}

func finishRel(ways *pendingEleMap, rel *pendingEle, tagsFilter map[string]TagTest) (finished elements.ByElementId, err error) {
    ri := rel.ee.Id()
    //println("finishRel",ri,len(rel.ww))
    
    ele := rel.ee.(elements.FullRelation)
    outers:=make([][]Coord, 0, len(rel.ww))
    outerTags:=MakeTagsEditable(nil)
    inners:=make([][]Coord, 0, len(rel.ww))
    finished = elements.ByElementId{}
    
    
    rt := ele.Tags().(TagsEditable)
    isboundary := (rt.Has("boundary") /*&& rt.Get("boundary")=="administrative"*/)
    outerRefs := make([]elements.Ref, 0, len(rel.ww))
    for i:=0; i < ele.Len(); i++ {
        if ele.MemberType(i)!=elements.Way {
            continue
        }
        
        
        w := ele.Ref(i)
        
        wy,ok := (*ways)[w]
        if !ok || wy.ee == nil {
            continue
        }
        
        gt := wy.ee.(interface{GeometryType() GeometryType}).GeometryType()
        if gt == Linestring {
            ls := wy.ee.(LinestringGeometry)
            if ele.Role(i) == "inner" {
                inners = append(inners,getLinestringCoords(ls))
            } else {
                outers = append(outers,getLinestringCoords(ls))
                if !isboundary {
                    outerTags.Add(ls.Tags())
                }
                outerRefs = append(outerRefs,w)
            }
        } else if gt == Polygon {
            ls := wy.ee.(PolygonGeometry)
            if ls.NumRings()!=1 {
                panic("??")
            }
            if ele.Role(i) == "inner" {
                inners = append(inners,getPolygonCoords(ls))
            } else {
                outers = append(outers,getPolygonCoords(ls))
                if !isboundary {
                    outerTags.Add(ls.Tags())
                }
                outerRefs = append(outerRefs,w)
                //outerTags = append(outerTags, ls.Tags().(TagsEditable))
            }
        }
        delete((*ways)[w].ww, ri)
    }
    defer func() {
        for i:=0; i < ele.Len(); i++ {
            if ele.MemberType(i)!=elements.Way {
                continue
            }
            
            
            w := ele.Ref(i)
            
            wy,ok := (*ways)[w]
            if !ok || wy.ee == nil {
                continue
            }
            
            if len((*ways)[w].ww) == 0 {
                wy := (*ways)[w].ee
                if wy != nil && IsFeature(wy.Tags(),tagsFilter ) {
                    finished = append(finished, wy)
                }
                delete(*ways, w)
            }
        }
        
    }()
    
    
    
    if len(outers)==0 {
        return
    }
    var outerRings,innerRings, outerChecked, innerChecked [][]Coord
    var groups [][][]Coord
    err = nil
    
    outerRings,err = merge_rings(outers)
    if err!=nil { return }
    
    outerChecked=make([][]Coord,0,len(outerRings))
    for _,r:=range outerRings {
        if check_ring(r) {
            outerChecked=append(outerChecked, r)
        }
    }
    
    innerRings,err = merge_rings(inners)
    if err!=nil { return }
    innerChecked=make([][]Coord,0,len(innerRings))
    for _,r:=range innerRings {
        if check_ring(r) {
            innerChecked=append(innerChecked, r)
        }
    }
    
    if len(outerChecked)==0 { return }
    
    
    groups,err = group_rings(outerChecked,innerChecked,true)
    if err!=nil { return }
    /*
    wo := ""
    if rt.Len()<2 {
        wo = "**"
    }*/
    
    rt.Add(outerTags)
    rt.Clip()
    zo,isp := wayTags(rt,tagsFilter)
    
    if rt.Len() == 0 || !isp { return }
    
    if !isboundary {
        for _,w := range outerRefs {
            wy,ok := (*ways)[w]
            if !ok || wy.ee == nil || wy.ee.Tags().Len()==0 {
                continue
            }
            wyt := wy.ee.Tags().(TagsEditable)
            for j:=0; j < rt.Len(); j++ {
                k:=rt.Key(j)
                v:=rt.Value(j)
                if wyt.Has(k)&& wyt.Get(k)==v {
                    (*ways)[w].ee.Tags().(TagsEditable).Delete(k)
                }
            }
        }
    }
    ar:=0.0
    for _,py := range groups {
        arp,_ := calculate_polygon_area(py)
        ar+=arp
    }
    
    if len(groups)==1 {
        finished = append(finished, makePolygonGeometry(ele,rt,groups[0],zo,ar))
    } else {
        finished = append(finished, makeMultiGeometry(ele,rt,groups,zo,ar))
    }
    
    
    //fmt.Println(wo,ele.Id(),rt,len(groups),len(outerChecked),len(innerChecked),zo,isp)
        
    return
}
    
func writeMsgs() chan <-string {
    t:=make(chan string)
    go func() {
        mm,_ := ioutil.TempFile(os.Getenv("GOPATH"),"osmquadtree.geometry.tmp")
        defer mm.Close()
        println("tf:",mm.Name())
        
        for s:=range t {
            mm.Write([]byte(s))
            //mm.Sync()
        }
        
    }()
    return t
}
            


func finishRelations(
    inc <-chan elements.ExtendedBlock,
    res chan <-elements.ExtendedBlock,
    tagsFilter map[string]TagTest) error {


    rels := pendingEleMap{}
    ways := pendingEleMap{}
    li:=0
    
    
    /*mm := writeMsgs()
    defer close(mm)
    zz:=0
    tgg:=0
    rc:=0
    tin:=0
    st:=time.Now()*/
    for bl := range inc {
        /*w:=time.Since(st).Seconds()
        st=time.Now()*/
        finished := make(elements.ByElementId, 0, bl.Len())
        
        bq:=bl.Quadtree()
        /*mm <- fmt.Sprintf("finishRelations %s: have %d rels, %d ways %d in %d fin %d total sent wait: %0.3fs ", bl, len(rels),len(ways), tin,rc,tgg, w)
        //fg:=false
        tin += bl.Len()*/
        for i:=0; i < bl.Len(); i++ {
            
            e:=bl.Element(i)
            //println("@",e.String())
            
            switch e.Type(){
                case elements.Relation:
                    rel := e.(elements.FullRelation)
                    rr := map[elements.Ref]bool{}
                    addWays(rr, rel)
                    if len(rr)>0 {
                        rels[rel.Id()] = &pendingEle{bq,rel,rr}
                        for r,_ := range rr {
                            if _,ok:=ways[r]; !ok {
                                ways[r] = &pendingEle{bq,nil,map[elements.Ref]bool{}}
                            }
                            ways[r].ww[rel.Id()]=true
                            
                        }
                    }
                case elements.Geometry:
                    
                    ei := e.Id()&0xffffffffffff
                    
                    if _,ok:=ways[ei]; !ok {
                        fmt.Println("Not needed?",e)
                        finished = append(finished, e)
                        continue
                        //panic("way not needed?")
                    }
                    t:=ways[ei]
                    t.qt=bq
                    
                    t.ee=e.(elements.FullElement)
                    ways[ei]=t
                    
                    
                    for r,_:=range ways[ei].ww {
                        rl,ok := rels[r]
                        if ok {
                            if _,ok := rl.ww[ei]; ok {
                                delete(rels[r].ww, ei)
                            }
                        }
                    }
                    
            }
            
            
        }
        fws:=make([]elements.Ref,0, len(rels))
        //println("check rels")
        for r,v :=range rels {
            if len(v.ww)==0 || bq.Common(v.qt) != v.qt {
                fws=append(fws,r)
            }
        }
        //mm <- fmt.Sprintf("finish: %d",len(fws))
        //nw := len(ways)
        if len(fws) >0 {
            //println("finish",len(fws),"rels")
            for _,r:=range fws {
                var err error
                rl,ok := rels[r]
                if !ok { println("??",r); continue }
                //rc++
                gg, err := finishRel(&ways, rl,tagsFilter)
                if err!=nil { panic(err.Error()) }
                finished = append(finished, gg...)
                
                delete(rels, r)
            }
            //println("done")
        }
        //nwr := nw-len(ways)
        
        //mm <- fmt.Sprintf(" %d %d",len(gg),nwr)
        
        
        if len(finished)>0 {
            //tgg+=len(gg)
            rb:=elements.MakeExtendedBlock(bl.Idx(),finished,bl.Quadtree(),bl.StartDate(),bl.EndDate(),nil)
            //fmt.Println(bl.Idx(),"output:",rb)
            res <- rb
        }
        
        //t:=time.Since(st).Seconds()
        //mm <- fmt.Sprintf(" %0.3fs\n",t)
        
        li = bl.Idx()
        /*st=time.Now()
        if (zz%1273) == 0 {
            debug.FreeOSMemory()
        }
        zz++*/
    }
    //mm <- fmt.Sprintf("have %d rels, %d ways remaining\n", len(rels),len(ways))
    
    finished := make(elements.ByElementId,0,len(rels)+len(ways))
    for _,r:=range rels {
        var err error
        gg,err := finishRel(&ways,r,tagsFilter)
        if err!=nil { panic(err.Error()) }
        finished = append(finished, gg...)
    }
    for _,w := range ways {
        if w.ee!=nil {
            finished=append(finished, w.ee)
        }
    }
    if len(finished)>0 {
    
   //mm <- fmt.Sprintf("sending %d objs\n", len(gg))
        rb:=elements.MakeExtendedBlock(li+1,finished,0,0,0,nil)
        fmt.Println("remaining output:",rb)
        res <- rb
    }
    
    return nil
}



