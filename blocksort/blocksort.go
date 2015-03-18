// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

/*
Package blocksort groups unsorted Elements into blocks.
This can be in-memory, or written to temporary files. 
*/ 
package blocksort

import (
    "github.com/jharris2268/osmquadtree/elements"
        
    "fmt"
    "sync"
)

type IdPacked struct {
    Key  int
    Data []byte
}

type IdPackedList interface {
    Len()     int
    At(i int) IdPacked
}


type BlockStore interface {
    Add(IdPacked)
    Len()   int
    Flush()
    
    All()   IdPackedList
}




type BlockStoreAllocPair struct {
    alloc   int
    block   BlockStore
    idx     int
}


type AllocBlockStore interface {
    Add(IdPacked)
    NumBlocks() int
    TotalLen()  int
    Flush()   
    
    Iter() <-chan BlockStoreAllocPair
    Finish()
}



func MakeAllocBlockStore(ty string) AllocBlockStore {
    
    switch ty {
        case "block": return newMapAllocBlockStore(makeNewSliceBlockStore,nil)
        case "tempfile":
            bsw := newBlockStoreWriterIdx(false, 64*1024)
            return newMapAllocBlockStore(bsw.MakeNew, bsw.Finish)
        case "tempfilesplit":
            bsw := newBlockStoreWriterSplit(100, 2*1024*1024)
            abs := newMapAllocBlockStoreSplit(bsw.MakeNew,500,bsw.Finish)
    
            return &groupAllocBlockStore{abs}
        case "tempfileslim":
            bsw := newBlockStoreWriterSplit(500,64*1024)
            abs := newMapAllocBlockStoreSplit(bsw.MakeNew,1,bsw.Finish)
    
            return &groupAllocBlockStore{abs}
    }
    panic("incorrect ty "+ty)
    return nil
}
    
            
func AddData(
    abs AllocBlockStore,
    inChans []chan elements.ExtendedBlock,
    addFunc func(elements.ExtendedBlock, chan IdPacked) error) error {
    
    res:=make(chan IdPacked)
    go func() {
        wg:=sync.WaitGroup{}
        wg.Add(len(inChans))
        for _,c:=range inChans {
            go func(c chan elements.ExtendedBlock) {
                for b:=range c {
                    addFunc(b, res)
                }
                wg.Done()
            }(c)
        }
        wg.Wait()
        close(res)
    }()
    
    for oo := range res {
        abs.Add(oo)
    }
    
    abs.Flush()
    
    //debug.FreeOSMemory()
    fmt.Printf("have %d objs in %d blocks\n", abs.TotalLen(), abs.NumBlocks())
    return nil
}



func ReadData(abs AllocBlockStore, nc int, outputFunc func(int,int,int,IdPackedList) error) error {
    itr := abs.Iter()
    wg:=sync.WaitGroup{}
    wg.Add(nc)
    
    zz := make([]chan BlockStoreAllocPair,nc)
    for i,_:=range zz {
        zz[i]=make(chan BlockStoreAllocPair,5)
    }
    go func() {
        for bl:=range itr {
            zz[bl.idx%nc] <- bl
        }
        for _,z:=range zz {
            close(z)
        }
    }()
    
    for i:=0; i < nc; i++ {
        go func(i int) {
            for bl := range zz[i] {
                err:=outputFunc(i,bl.idx,bl.alloc,bl.block.All())
                if err!=nil {
                    panic(err.Error())
                }
            }
            
            wg.Done()
            
        }(i)
    }
    wg.Wait()
    return nil
}

    
func SortByTile(
    inChans []chan elements.ExtendedBlock,
    addFunc func(elements.ExtendedBlock,chan IdPacked) error,
    nc int,
    outputFunc func(int,int,int,IdPackedList) error,
    abs AllocBlockStore) error {

    AddData(abs,inChans,addFunc)
    err:=ReadData(abs,nc,outputFunc)
    abs.Finish()
    return err
}
    


type Allocater func(elements.Element) int

func makeByElementId(ipl IdPackedList) elements.ByElementId {
    ans := make(elements.ByElementId, ipl.Len())
    
    for i,_ := range ans {
        ip := ipl.At(i)
        ans[i] = elements.UnpackElement(ip.Data)
    }
    ans.Sort()
    return ans
}

func makeIdPacked(alloc Allocater, o elements.Element) IdPacked {
    return IdPacked{alloc(o), o.Pack()}
}
    
func addToPackedPairBlock(bl elements.ExtendedBlock, alloc Allocater, res chan IdPacked) error {
    if bl==nil || bl.Len()==0 { return nil }
    for i:=0; i < bl.Len(); i++ {
        o := bl.Element(i)
        res <- makeIdPacked(alloc,o)
    }
    return nil
}
 

func SortElementsByAlloc(
        inChans []chan elements.ExtendedBlock,
        alloc Allocater,
        nc int,
        makeBlock func(int, int,elements.Block) (elements.ExtendedBlock, error),
        absType string) ([]chan elements.ExtendedBlock, error) {
    
    if absType == "inmem" {
        return SortInMem(inChans,alloc,nc,makeBlock)
    }
    
    abs:=MakeAllocBlockStore(absType)
    
    addFunc := func(bl elements.ExtendedBlock, res chan IdPacked) error {
        return addToPackedPairBlock(bl,alloc,res)
    }
    
    res := make([]chan elements.ExtendedBlock,nc)
    for i,_:=range res {
        res[i] = make(chan elements.ExtendedBlock)
    }
        
    outputFunc := func(i int, idx int, al int, all IdPackedList) error {
        pp := makeByElementId(all)
        bl,err := makeBlock(idx, al, pp)
        if err!=nil { return err }
        res[i] <- bl
        return nil
    }
    
    go func() {
        err:=SortByTile(inChans,addFunc,nc,outputFunc,abs)
        if err!=nil {
            fmt.Println("SortByTile error:", err.Error())
        }
        for _,r:=range res {
            //fmt.Println("close chan",i,"/",len(res))
            close(r)
        }
    }()
    return res,nil
}

      

            
                    
                
            
    

    
    
        
        
    
    

