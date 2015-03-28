// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

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


func MakeFileBlockChanSplitPartial(fn string, nc int, locs []int64) ([]<-chan pbffile.FileBlock, error) {
    fl,err := os.Open(fn)
    if err!=nil { return nil,err}
    //isc:=strings.HasSuffix(fn,"pbfc")
    return pbffile.ReadPbfFileBlocksDeferSplitPartial(fl, locs, nc), nil
}

func getPartialLocs(fn string, passQt func(quadtree.Quadtree) bool) ([]int64, bool, error) {
    isc := strings.HasSuffix(fn,"pbfc")
    _,hb,err := GetHeaderBlock(fn)
    if err!=nil {
        return nil,false,err
    }
    locs:=make([]int64,0,hb.Index.Len())
    for i:=0; i < hb.Index.Len(); i++ {
        q:=hb.Index.Quadtree(i)
        if passQt(q) {
            locs=append(locs, hb.Index.Filepos(i))
            isc = isc || hb.Index.IsChange(i)
        }
    }
    return locs,isc,nil
}

func ReadExtendedBlockMultiSortedQts(fn string, nc int, passQt func(quadtree.Quadtree) bool) (<-chan elements.ExtendedBlock, error) {
    
    locs,isc, err := getPartialLocs(fn, passQt)
    if err!=nil { return nil,err }
    return ReadExtendedBlockMultiSortedPartial(fn,nc,locs,isc)
}


func ReadExtendedBlockMultiSortedPartial(fn string, nc int, locs []int64, isc bool) (<-chan elements.ExtendedBlock, error) { 
       
    blocks,err := MakeFileBlockChanSplitPartial(fn,nc,locs)
    if err!=nil { return nil,err }
    
    return ReadDataMultiSorted(blocks,isc, read.ReadExtendedBlock)
}

