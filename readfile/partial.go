package readfile

import (
    "github.com/jharris2268/osmquadtree/quadtree"
    //"github.com/jharris2268/osmquadtree/change"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/read"
    "github.com/jharris2268/osmquadtree/pbffile"
    
    "os"
    "strings"
    "sync"
    "errors"
    //"fmt"
)

func GetHeaderBlock(fn string) (*os.File, *read.HeaderBlock,error) {
    fl,err := os.Open(fn)
    if err!=nil {
        return nil,nil,err
    }
    bl,err := pbffile.ReadPbfFileBlockAt(fl,0)
    if err!=nil {
        return nil,nil,err
    }
    if string(bl.BlockType())!="OSMHeader" {
        return nil,nil,errors.New("No header present")
    }
    fp,err := fl.Seek(0,1)
    if err != nil {
        return nil,nil,err
    }
    hb,err := read.ReadHeaderBlock(bl.BlockData(),fp)
    if err!=nil {
        fl.Close()
        return nil,nil,err
    }
    return fl,hb,nil
}

func ProcessFileBlocksFullMultiPartial(fn string, locs []int64, proc func(int, elements.ExtendedBlock) error, nc int) error {
    fl,err := os.Open(fn)
    if err!=nil {
        return err
    }
    isc:=strings.HasSuffix(fn,"pbfc")
    //blocks := pbffile.ReadPbfFileBlocksMulti(fl,nc)
    
    blocks := pbffile.ReadPbfFileBlocksDeferPartial(fl, locs)
    
    wg:=sync.WaitGroup{}
    wg.Add(nc)
        
    for i:=0; i < nc; i++ {
        go func(i int) {
            readBlocks(blocks, read.ReadExtendedBlock, i, isc,proc)
            wg.Done()
        }(i)
    }
    wg.Wait()
    fl.Close()
    return nil
}

func ProcessFileBlocksPartialQts(fn string, passQt func(quadtree.Quadtree) bool, proc func(int, elements.ExtendedBlock) error, nc int) error {
    fl,hb,err := GetHeaderBlock(fn)
    if err!=nil {
        return err
    }
    locs:=make([]int64,0,hb.Index.Len())
    for i:=0; i < hb.Index.Len(); i++ {
        q:=hb.Index.Quadtree(i)
        if passQt(q) {
            locs=append(locs, hb.Index.Filepos(i))
        }
    }
    
    isc:=strings.HasSuffix(fn,"pbfc")
    //blocks := pbffile.ReadPbfFileBlocksMulti(fl,nc)
    
    //println(fn,isc,len(locs))
    
    blocks := pbffile.ReadPbfFileBlocksDeferSplitPartial(fl, locs,nc)
    
    wg:=sync.WaitGroup{}
    wg.Add(nc)
        
    for i:=0; i < nc; i++ {
        go func(i int) {
            readBlocks(blocks[i], read.ReadExtendedBlock, i, isc, proc)
            wg.Done()
        }(i)
    }
    wg.Wait()
    fl.Close()
    return nil
}

func MakeProcessFileBlocksPartialQts(passQt func(quadtree.Quadtree) bool) func(string,func(p int, e elements.ExtendedBlock) error,int) error {
    return func(fn string, proc func(int, elements.ExtendedBlock) error, nc int) error {
        return ProcessFileBlocksPartialQts(fn,passQt,proc,nc)
    }
}
