package readfile

import (
    "github.com/jharris2268/osmquadtree/quadtree"
    //"github.com/jharris2268/osmquadtree/change"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/read"
    "github.com/jharris2268/osmquadtree/pbffile"
    
    "os"
    "strings"
    //"sync"
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


func MakeFileBlockChanSplitPartial(fn string, nc int, locs []int64) ([]<-chan pbffile.FileBlock, bool, error) {
    fl,err := os.Open(fn)
    if err!=nil { return nil,false,err}
    isc:=strings.HasSuffix(fn,"pbfc")
    return pbffile.ReadPbfFileBlocksDeferSplitPartial(fl, locs, nc), isc, nil
}

func getPartialLocs(fn string, passQt func(quadtree.Quadtree) bool) ([]int64, error) {
    
    _,hb,err := GetHeaderBlock(fn)
    if err!=nil {
        return nil,err
    }
    locs:=make([]int64,0,hb.Index.Len())
    for i:=0; i < hb.Index.Len(); i++ {
        q:=hb.Index.Quadtree(i)
        if passQt(q) {
            locs=append(locs, hb.Index.Filepos(i))
        }
    }
    return locs,nil
}

func ReadExtendedBlockMultiSortedQts(fn string, nc int, passQt func(quadtree.Quadtree) bool) (<-chan elements.ExtendedBlock, error) {
    
    locs,err := getPartialLocs(fn, passQt)
    if err!=nil { return nil,err }
    return ReadExtendedBlockMultiSortedPartial(fn,nc,locs)
}


func ReadExtendedBlockMultiSortedPartial(fn string, nc int, locs []int64) (<-chan elements.ExtendedBlock, error) {    
    blocks,isc,err := MakeFileBlockChanSplitPartial(fn,nc,locs)
    if err!=nil { return nil,err }
    
    return ReadDataMultiSorted(blocks,isc, read.ReadExtendedBlock)
}

