package locationscache


import (
    "encoding/json"
    "os"
    "fmt"
    "sync"
    
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/elements"

)

func MakeLocationsCacheNull(
    inChans []chan elements.ExtendedBlock, infn string, prfx string,
    endDate elements.Timestamp, state int64) error {
    
    writeSpecs(prfx, []IdxItem{IdxItem{0,infn,endDate,state}}, []int{0})
    return nil
    
}




func GetLastStateFileList(pp string) (int64, error) {
    spec,_,err := readSpecs(pp)
    if err!=nil { return 0,err }
    return spec[len(spec)-1].State, nil
}


type FS struct {
    State       int64
    EndDate     string
    Filename    string
    NumTiles    int
}

func readSpecs(prfx string) ([]IdxItem, []int, error) {
    specf,err := os.Open(prfx+"filelist.json")
    if err!=nil { return nil,nil,err}
    defer specf.Close()
    
    spec:= []FS{}
    err = json.NewDecoder(specf).Decode(&spec)
    if err!=nil { return nil,nil,err}
    
    
    idx := make([]IdxItem, len(spec))
    offsets:=make([]int, len(spec))
    
    
    c:=0
    for i, fs := range spec {
        //fmt.Println(i,fs,len(rr.idx))
        idx[i].State = fs.State
        idx[i].Filename = fs.Filename
        idx[i].Timestamp,err = elements.ReadDateString(fs.EndDate)
        if err!=nil { return nil,nil,err }
        idx[i].Idx = i
        offsets[i] = c+fs.NumTiles
        c = offsets[i]
    }
    
    
    return idx,offsets, nil
}

func writeSpecs(prfx string, idx []IdxItem, offsets []int) {
    specf,err := os.Create(prfx+"filelist.json")
    if err!=nil { panic(err.Error()) }
    tt := make([]FS, len(idx))
    
    c:=0
    for i,ii:=range idx {
        
        tt[i].State=ii.State
        tt[i].Filename=ii.Filename
        tt[i].EndDate=ii.Timestamp.String()
        tt[i].NumTiles = offsets[i]-c
        c=offsets[i]
    }
    
    json.NewEncoder(specf).Encode(&tt)
    specf.Close()
}


type nullLocationsCache struct {
    prfx    string
    idx     []IdxItem
    offsets []int
    changed bool
}

func OpenNullLocationsCache(prfx string) (LocationsCache,error) {
    rr := nullLocationsCache{}
    rr.prfx = prfx
    var err error
    rr.idx,rr.offsets,err = readSpecs(prfx)
    if err!=nil { return nil,err }
    
    return &rr,nil
}


func (fflc *nullLocationsCache) Close() {
    if fflc.changed {
        writeSpecs(fflc.prfx, fflc.idx, fflc.offsets)
        
    }
    
}

func (fflc *nullLocationsCache) NumFiles() int {
    return len(fflc.idx)
}

func (fflc *nullLocationsCache) FileSpec(i int) IdxItem {
    return fflc.idx[i]
}

func (fflc *nullLocationsCache) FindTiles(inc <-chan int64) (Locs,TilePairSet) {
    
    ll := Locs{}
    tm := TilePairSet{}
    for ii := range inc {
        ll[elements.Ref(ii)]=TilePair{-1,-1}
    }
    
    for fl,idx := range fflc.idx {
        ts := make([]map[elements.Ref]int,4)
        
        fmt.Println("scan",fflc.prfx+idx.Filename)
        
        _,ii,err := readfile.GetHeaderBlock(fflc.prfx+idx.Filename)
        if err!=nil { panic(err.Error()) }
        qqm := map[quadtree.Quadtree]int{}
        for i:=0; i < ii.Index.Len(); i++ {
            qqm[ii.Index.Quadtree(i)]=i
        }
        
        
        tiles,err := readfile.ReadQtsMulti(fflc.prfx+idx.Filename, 4)
        if err!=nil { panic(err.Error()) }
        
        wg:=sync.WaitGroup{}
        wg.Add(4)
        for i,_ := range tiles {
            ts[i]=map[elements.Ref]int{}
            go func(i int) {
                for bl:=range tiles[i] {
                    tl := qqm[bl.Quadtree()]
                    for j:=0; j < bl.Len(); j++ {
                        e:=bl.Element(j)
                        k := elements.Ref(e.Type()<<59) | e.Id()
                        
                        if k == 2524940079 {
                            fmt.Println(fl, tl, e, e.ChangeType())
                        }
                        
                        if _,ok := ll[k]; ok {
                            
                            switch e.ChangeType() {
                                case 1: ts[i][k]=-1
                                case 0,3,4,5: ts[i][k] = tl
                            }
                        }
                    }
                }
                wg.Done()
            }(i)
        }
        wg.Wait()
        fmt.Println("found",len(ts[0])+len(ts[1])+len(ts[2])+len(ts[3]),"elements")
        for _,t := range ts {
            for k,v := range t {
                if v==-1 {
                    ll[k] = TilePair{-1,-1}
                } else {
                    ll[k] = TilePair{fl,v}
                }
            }
        }
    }
    for _,v := range ll {
        if v.File>=0 {
            tm[v] = true
        }
    }
        
    
    return ll,tm
}

func (fflc *nullLocationsCache) AddTiles(lcs Locs, idx IdxItem) int {
    
    
    
    o := len(fflc.idx)
    
    mxT := 0
    for _,v:=range lcs {
        if v.File==o && v.Tile > mxT {
            mxT = v.Tile
        }
    }
    mxT++
    
    idx.Idx=o
    fflc.idx = append(fflc.idx, idx)
    fflc.offsets=append(fflc.offsets, fflc.offsets[len(fflc.offsets)-1]+mxT+1)    
    fflc.changed=true
    return o
}

    
