// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package readfile

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/utils"
    
    "fmt"
    
)

type qtPacked struct {
    ct elements.ChangeType
    ty elements.ElementType
    id elements.Ref
    qt quadtree.Quadtree
    dt []byte
}

func (qp *qtPacked) ChangeType() elements.ChangeType { return qp.ct }
func (qp *qtPacked) Type() elements.ElementType { return qp.ty }
func (qp *qtPacked) Id() elements.Ref { return qp.id }
func (qp *qtPacked) Quadtree() quadtree.Quadtree { return qp.qt }
func (qp *qtPacked) Pack() []byte { return qp.dt }

func (qp *qtPacked) String() string {
    return fmt.Sprintf("AddQt %0.2s %0.1s %10d %-18s [%5d bytes]",
        qp.ct,qp.ty,qp.id,qp.qt,len(qp.dt))
}

func makeQtPacked(e elements.Element, qt quadtree.Quadtree) elements.Element {
    
    orig := e.Pack()
    if orig[10] != 0 && orig[10]!=1 {
        panic(fmt.Sprintf("already has quadtree... %d",orig[10]))
    }
    
    nv := make([]byte, len(orig)+10)
    copy(nv, orig[:10])
    p := utils.WriteVarint(nv,10,int64(qt))
    copy(nv[p:], orig[11:])
    nv = nv[:p+len(orig)-11]
    
    return &qtPacked{e.ChangeType(),e.Type(),e.Id(),qt, nv}
}
    
    
func getSortedChan(data func(func(int,elements.ExtendedBlock) error) error) <-chan elements.ExtendedBlock {
    
    iter := make(chan elements.ExtendedBlock)
    add := func(i int, e elements.ExtendedBlock) error {
        iter <- e
        return nil
    }
    go func() {
        data(add)
        close(iter)
    }()
    
    return SortExtendedBlockChan(iter)
}
    
    
type blockPair struct {
    main elements.ExtendedBlock
    qts  []quadtree.Quadtree
}


// AddQts combines the qt values from the qtsFn file (e.g. produced by
// calcqts.CalcObjectQts, with the original data (e.g. a downloaded
// planet.osm.pbf), returning a slice of nc ExtendedBlock channels
func AddQts(
    mainFn string,
    qtsFn string,
    nc int) ([]chan elements.ExtendedBlock,error) {
        
    main,err := ReadExtendedBlockMultiSorted(mainFn,nc)
    if err!=nil { return nil,err }
    qts,err := ReadExtendedBlockMultiSorted(qtsFn,nc)
    if err!=nil { return nil,err }
    
    qtj := make([]chan blockPair, nc)
    for i,_ := range qtj {
        qtj[i] = make(chan blockPair)
    }
    
    go func() {
        
        qtb,ok := <- qts
        qti:=0
        
        for bl := range main {
            pp := blockPair{bl,make([]quadtree.Quadtree,bl.Len())}
            
            for i,_ := range pp.qts {
                for ok && qti==qtb.Len() {
                    qtb,ok = <- qts
                    qti=0
                }
                if !ok {
                    panic("ran out out qts...")
                }
                e:=bl.Element(i)
                q:=qtb.Element(qti)
                
                if e.Type()<q.Type() || e.Id()<q.Id() {
                    fmt.Printf("No qt for object %s,{%s}\n", e, q)
                    pp.qts[i] = quadtree.Null
                } else if e.Type()>q.Type() || e.Id()>q.Id() {
                    
                    for ok && (e.Type()>q.Type() || e.Id()>q.Id()) {
                        fmt.Printf("no object for qt %s{%s}\n", q, e)
                        qti++
                        for ok && qti==qtb.Len() {
                            qtb,ok = <- qts
                            qti=0
                        }
                        if ok {
                            q = qtb.Element(qti)
                        } 
                    }
                } else {
                    /*
                    
                                                
                if e.Type()!=q.Type() || e.Id()!=q.Id() {
                                       
                    
                    panic("out of sync")
                }*/
                    qq,ok:=q.(interface{Quadtree() quadtree.Quadtree})
                    if !ok {
                        panic("no quadtree")
                    }
                    pp.qts[i] = qq.Quadtree()
                    qti++
                }
                
            }
            //println(pp.main.Idx(),pp.main.Len(),len(pp.qts))
            qtj[bl.Idx()%nc] <- pp
        }
        if qti != qtb.Len() {
            panic("still have qts left")
        }
        qtb,ok = <-qts
        if ok {
            panic("still have qts left")
        }
        
        for _,q:=range qtj {
            close(q)
        }
    }()
    
    
    res := make([]chan elements.ExtendedBlock, nc)
        
    for i,_:=range qtj {
        res[i] = make(chan elements.ExtendedBlock)
        
        go func(i int) {
            for p:=range qtj[i] {
                nb:=make(elements.ByElementId, 0, p.main.Len())
                for j,q:=range p.qts {
                    e:=p.main.Element(j)
                    
                    //if e.Type()!=elements.Node {
                    if q == quadtree.Null {
                        fmt.Println("skip",e,q)
                    } else {
                        nb = append(nb, makeQtPacked(e,q))
                    }
                }
                if len(nb)>0 {
                    res[i] <- elements.MakeExtendedBlock(p.main.Idx(),nb, quadtree.Null,0,0,nil)
                }
            }
            fmt.Println("close chan",i,"/",len(res))
            close(res[i])
        }(i)
    }
    return res, nil
}

                
                
            
            
            
            
        
        
    
    
    
    
    
    
    
    
