package blocksort

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/utils"
    
    "fmt"
    
)



func SortElementsById(
        inChans []chan elements.ExtendedBlock,
        nc int,
        endDate elements.Timestamp, groupSize int, sortType string) ([]chan elements.ExtendedBlock, error) {

    
    //toSort:=make(chan elements.ExtendedBlock)
    
    addf := func(idx int, al int,bl elements.Block) (elements.ExtendedBlock, error) {
        return elements.MakeExtendedBlock(idx,bl, quadtree.Null, 0, endDate,nil),nil
    }
    
    toSort, err := SortElementsByAlloc(inChans, alloc, nc, addf, sortType)
    if err!=nil {
        return nil,err
    }
    
    return sortAndGroupTiles(readfile.CollectExtendedBlockChans(toSort,false),groupSize,endDate,nc)
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

func sortAndGroupTiles(toSort <-chan elements.ExtendedBlock, groupSize int, endDate elements.Timestamp, nc int) ([]chan elements.ExtendedBlock, error) {
    sorted := readfile.SortExtendedBlockChan(toSort)

    res := make([]chan elements.ExtendedBlock, nc)
    for i,_:=range res {
        res[i] = make(chan elements.ExtendedBlock)
    }
    
    go func() {    
      
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
                    
                    res[ii%nc] <- elements.MakeExtendedBlock(ii, pp, 0,0,endDate,nil)
                    
                    pp = make(elements.ByElementId,0,groupSize)
                    ii++
                    
                }
            }
            
        }

    
        if len(pp)>0 {
            fmt.Printf("%-4d %-8d %-40s %-40s %s: %0.1fs sending\n", ii, len(pp), pp[0], pp[len(pp)-1],utils.MemstatsStr(),tt)
            res[ii%nc] <- elements.MakeExtendedBlock(ii, pp, 0,0,endDate,nil)
            ii++
        }

    
        for _,r:=range res {
            close(r)
        }
    }()
    
    return res,nil
}
                
