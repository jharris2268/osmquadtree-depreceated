package blocksort

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/utils"
    
    "fmt"
    "sync"
)



func SortElementsById(
        dataFunc func(func(int,elements.ExtendedBlock) error) error,
        nc int,
        addFunc func(int, elements.ExtendedBlock) error,
        endDate elements.Timestamp, groupSize int, sortType string) error {

    
    toSort:=make(chan elements.ExtendedBlock)
    
    addf := func(i int, idx int, al int,bl elements.Block) error {
        //println(i,idx,bl.Len())
        toSort <- elements.MakeExtendedBlock(idx,bl, quadtree.Null, 0, endDate,nil)
        return nil
    }
    
    go func() {
        SortElementsByAlloc(dataFunc, alloc, nc, addf, sortType)
        close(toSort)
    }()
    
    return sortAndGroupTiles(toSort,groupSize,endDate,nc,addFunc)
}



func alloc(o elements.Element) int {
    r:=uint(14)
    switch o.Type() {
        case elements.Node: return int(o.Id()>>r)
        case elements.Way: return int(o.Id()>>(r-3)) | (1<<32)
        case elements.Relation: return int(o.Id()>>(r-5)) | (2<<32)
    }
    return 3<<32 +  int(o.Id()>>(r-5))
}

    



type ep struct {
    i int
    e elements.ByElementId
}

func sortAndGroupTiles(toSort <-chan elements.ExtendedBlock, groupSize int, endDate elements.Timestamp, nc int, addFunc func(int,elements.ExtendedBlock) error) error {
    sorted := readfile.SortExtendedBlockChan(toSort)

    wg:=sync.WaitGroup{}
    rr:=make([]chan ep,nc)
    for i,_ := range rr {
        rr[i]=make(chan ep)
        wg.Add(1)
        go func(i int) {
            for e:=range rr[i] {
                addFunc(i, elements.MakeExtendedBlock(e.i,e.e,quadtree.Null,0,endDate,nil))
            }
            wg.Done()
        }(i)
    }
      
    pp := make(elements.ByElementId,0,groupSize)
    tt:=0.0
    
    ii := 0
    for bl := range sorted {
       
        j := 0
        for j < bl.Len() {
            
            for len(pp)<groupSize && j < bl.Len() {
                pp = append(pp, bl.Element(j))
                j++                    
            }
            
            if len(pp)==groupSize {
                
                if (ii % 1373)==0 {
                    fmt.Printf("%-4d %-8d %-40s %-40s %s\n", ii, len(pp), pp[0], pp[len(pp)-1],utils.MemstatsStr())
                }
                
                rr[ii%nc] <- ep{ii,pp}
                
                pp = make(elements.ByElementId,0,groupSize)
                ii++
                
            }
        }
        
    }

    
    if len(pp)>0 {
        fmt.Printf("%-4d %-8d %-40s %-40s %s: %0.1fs sending\n", ii, len(pp), pp[0], pp[len(pp)-1],utils.MemstatsStr(),tt)
        rr[ii%nc] <- ep {ii,pp}        
        ii++
    }
    
    for _,r:=range rr {
        close(r)
    }
    
    wg.Wait()
    return nil
}
                
