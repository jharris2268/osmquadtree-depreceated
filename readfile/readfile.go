package readfile

import (
    "github.com/jharris2268/osmquadtree/read"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/pbffile"
    
    "os"
    //"io"
    "sync"
    "strings"
    
)

func readBlocks(
    blocks <-chan pbffile.FileBlock,
    readfn func(int, []byte, bool) (elements.ExtendedBlock,error),
    pc     int,
    ischange bool,
    procfn func(int, elements.ExtendedBlock) error ) error {
    
    for bl := range blocks {
        
        isc,ok := func(t pbffile.FileBlock) (bool,bool) {
            switch string(bl.BlockType()) {
                case "OSMData": return false || ischange,true
                case "OSMChange": return true,true
            }
            return false,false
        }(bl)
        if ok {
            dd,err:=readfn(bl.Idx(), bl.BlockData(), isc)
            if err!=nil {
                return err
            }
            err = procfn(pc,dd)
            if err!=nil {
                return err
            }
        } else {
            err := procfn(pc, elements.MakeExtendedBlock(bl.Idx(),nil,quadtree.Null,0,0,nil))
            if err!=nil {
                return err
            }
        }
    }
    return nil
}

func ReadFileBlocksFull(fn string) (<-chan elements.ExtendedBlock,error) {
    fl,err := os.Open(fn)
    if err!=nil {
        return nil, err
    }
    isc:=strings.HasSuffix(fn,"pbfc")
    blocks := pbffile.ReadPbfFileBlocks(fl)
    res := make(chan elements.ExtendedBlock)
    addto := func(p int, e elements.ExtendedBlock) error {
        res <- e
        return nil
    }
    go func() {
        readBlocks(blocks, read.ReadExtendedBlock,0,isc,addto)
        close(res)
        fl.Close()
    }()
    return res, nil
}
    
func ReadFileBlocksMulti(fn string, nc int) (<-chan elements.ExtendedBlock,error) {
    
    res := make(chan elements.ExtendedBlock)
    addto := func(p int, e elements.ExtendedBlock) error {
        res <- e
        return nil
    }
    
    go func() {
        ProcessFileBlocksFullMulti(fn, addto, nc)
        close(res)
    }()
    
    return SortExtendedBlockChan(res),nil
}

func SortExtendedBlockChan(inc <-chan elements.ExtendedBlock) <-chan elements.ExtendedBlock {
    idxc := make(chan utils.Idxer)
    go func() {
        for i := range inc {
            idxc <- i
        }
        close(idxc)
    }()
    
    res:=make(chan elements.ExtendedBlock)
    go func() {
        for i := range utils.SortIdxerChan(idxc) {
            res <- i.(elements.ExtendedBlock)
        }
        close(res)
    }()
    return res
}


func ProcessFileBlocksMulti(fn string, proc func(int, elements.ExtendedBlock) error, nc int, readBlock func(int, []byte, bool) (elements.ExtendedBlock,error) ) error {
    
    
    fl,err := os.Open(fn)
    if err!=nil {
        return err
    }
    isc:=strings.HasSuffix(fn,"pbfc")
    //blocks := pbffile.ReadPbfFileBlocksMulti(fl,nc)
    blocks := pbffile.ReadPbfFileBlocksDeferSplit(fl,nc)
    
    wg:=sync.WaitGroup{}
    wg.Add(nc)
    
    for i:=0; i < nc; i++ {
        go func(i int) {
            readBlocks(blocks[i], readBlock , i, isc,proc)
            wg.Done()
        }(i)
    }
    wg.Wait()
    fl.Close()
    return nil
}
    


    

func ReadFileBlocksSorted(fn string, nc int, procf func(string,func(p int, e elements.ExtendedBlock) error,int) error) (<-chan elements.ExtendedBlock,error) {
    
    
    resp := make([]chan elements.ExtendedBlock, nc)
    for i,_ := range resp {
        resp[i] = make(chan elements.ExtendedBlock, 5)
    }
    
    addto := func(p int, e elements.ExtendedBlock) error {
        resp[p] <- e
        return nil
    }
    
    go func() {
        procf(fn, addto, nc)
        for _,r:= range resp {
            close(r)
        }
    }()
    
    
    return collect(resp,false),nil
    //return SortExtendedBlockChan(res),nil
}

func collect(resp []chan elements.ExtendedBlock,msgs bool) <-chan elements.ExtendedBlock {
    res := make(chan elements.ExtendedBlock)
    go func() {
        nc:=len(resp)
        rem:=nc
        i:=0
        fins:=make([]bool,nc)
        for i,_:=range fins {
            fins[i]=true
        }
        
        for rem>0 {
            if fins[i%nc] {
                var b elements.ExtendedBlock
                b,fins[i%nc] = <- resp[i%nc]
                if fins[i%nc] {
                    if msgs {
                        println("collect: ",i,i%nc,b.Idx(),b.String())
                    }
                    res <- b
                } else {
                    //println("finished",i,i%nc)
                    rem--
                }
            }
            i++
        }
        close(res)
    }()
    return res
}


func ProcessFileBlocksFullMulti(fn string, proc func(int, elements.ExtendedBlock) error, nc int) error {
    return ProcessFileBlocksMulti(fn,proc,nc,read.ReadExtendedBlock)
}

func ReadFileBlocksFullSorted(fn string, nc int) (<-chan elements.ExtendedBlock,error) {
    return ReadFileBlocksSorted(fn,nc,ProcessFileBlocksFullMulti)
}


func ProcessFileBlocksDataMulti(
    fn string, proc func(int, elements.ExtendedBlock) error,
    nc int, nn, ww, rr bool) error {
    
    rof := func(idx int, bl []byte, isc bool) (elements.ExtendedBlock,error) {
        return read.ReadObjsData(idx,bl,nn,ww,rr)
    }
    
    return ProcessFileBlocksMulti(fn,proc,nc,rof)
}

