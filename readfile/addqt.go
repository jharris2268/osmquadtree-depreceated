package readfile

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/utils"
    
    "fmt"
    "sync"
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

    
func AddQts(
    mainFn string,
    qtsFn string,
    nc int,
    addFunc func(int, elements.ExtendedBlock) error) error {
    
    main,err := ReadFileBlocksFullSorted(mainFn,nc)
    if err!=nil { return err }
    qts,err := ReadFileBlocksFullSorted(qtsFn,nc)
    if err!=nil { return err }
    
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
                if e.Type()!=q.Type() || e.Id()!=q.Id() {
                    panic("out of sync")
                }
                qq,ok:=q.(interface{Quadtree() quadtree.Quadtree})
                if !ok {
                    panic("no quadtree")
                }
                pp.qts[i] = qq.Quadtree()
                qti++
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
    
    wg:=sync.WaitGroup{}
    wg.Add(len(qtj))
    
    for i,_:=range qtj {
        go func(i int) {
            for p:=range qtj[i] {
                nb:=make(elements.ByElementId, p.main.Len())
                for i,q:=range p.qts {
                    nb[i] = makeQtPacked(p.main.Element(i),q)
                }
                
                addFunc(i,elements.MakeExtendedBlock(p.main.Idx(),nb, quadtree.Null,0,0,nil))
            }
            wg.Done()
        }(i)
    }
    wg.Wait()
    return nil
}

                
                
            
            
            
            
        
        
    
    
    
    
    
    
    
    