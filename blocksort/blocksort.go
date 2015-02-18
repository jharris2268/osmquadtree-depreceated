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
    dataFunc func(func(int,elements.ExtendedBlock) error) error,
    addFunc func(chan IdPacked) func(int, elements.ExtendedBlock) error) error {
    
    res:=make(chan IdPacked)
    go func() {
        dataFunc(addFunc(res))
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
    for i:=0; i < nc; i++ {
        go func(i int) {
            for bl := range itr {
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
    dataFunc func(func(int,elements.ExtendedBlock) error) error,
    addFunc func(chan IdPacked) func(int,elements.ExtendedBlock) error,
    nc int,
    outputFunc func(int,int,int,IdPackedList) error,
    abs AllocBlockStore) error {

    AddData(abs,dataFunc,addFunc)
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
    
func makeAddToPackedPairBlock(alloc Allocater, res chan IdPacked) func(int, elements.ExtendedBlock) error {
    return func(k int, bl elements.ExtendedBlock) error {
        if bl==nil || bl.Len()==0 { return nil }
        for i:=0; i < bl.Len(); i++ {
            o := bl.Element(i)
            res <- makeIdPacked(alloc,o)
        }
        return nil
    }
}
 

func SortElementsByAlloc(
        dataFunc func(func(int,elements.ExtendedBlock) error) error,
        alloc Allocater,
        nc int,
        makeBlock func(int, int, int,elements.Block) error,
        absType string) error {
    
    if absType == "inmem" {
        return SortInMem(dataFunc,alloc,nc,makeBlock)
    }
    
    abs:=MakeAllocBlockStore(absType)
    
    addFunc := func(res chan IdPacked) func(int,elements.ExtendedBlock) error {
        return makeAddToPackedPairBlock(alloc,res)
    }
    
    outputFunc :=func(i int, idx int, al int, all IdPackedList) error {
         pp := makeByElementId(all)
         return makeBlock(i, idx, al,pp)
    }

    return SortByTile(dataFunc,addFunc,nc,outputFunc,abs)
}

      

            
                    
                
            
    

    
    
        
        
    
    

