package locationscache


import (
    
    "os"
    "fmt"
    "sync"
    "sort"
    
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/pbffile"
    "github.com/jharris2268/osmquadtree/elements"
    
    "github.com/jharris2268/osmquadtree/read"
    "github.com/jharris2268/osmquadtree/blocksort"
    "github.com/jharris2268/osmquadtree/writefile"
    "github.com/jharris2268/osmquadtree/quadtree"

)


type idxLocationsCache struct {
    prfx    string
    idx     []IdxItem
    offsets []int
    changed bool
}


func readIndexBlock(dd []byte) ([]int64, []int64) {
    p,a := utils.ReadPbfTag(dd,0)
    p,b := utils.ReadPbfTag(dd,p)
    
    c,_ := utils.ReadDeltaPackedList(a.Data)
    d,_ := utils.ReadDeltaPackedList(b.Data)
    
    return c,d
}


type tt struct {
    
    i int64
    v int
}

type tts []tt
func (t tts) Len() int { return len(t) }
func (t tts) Swap(i,j int) { t[i],t[j]=t[j],t[i] }
func (t tts) Less(i,j int) bool { return t[i].i < t[j].i }
func (t tts) Sort() { sort.Sort(t) }

func prepBlock(tt tts) []byte {
    a := make([]int64, len(tt))
    b := make([]int64, len(tt))
    
    for i,t := range tt {
        a[i] = t.i
        b[i] = int64(t.v)
    }
    return serializeLocs(a,b)
}

func serializeLocs(a,b []int64) []byte {
    c,_ := utils.PackDeltaPackedList(a)
    d,_ := utils.PackDeltaPackedList(b)
    
    res := make([]byte, len(c)+len(d)+20)
    p:=0
    
    res,p = utils.WritePbfData(res,p,1,c)
    res,p = utils.WritePbfData(res,p,2,d)
    
    return res[:p]
}

type idxData struct {
    i int
    q quadtree.Quadtree
    d []byte
}

func (i *idxData) Idx() int { return i.i }
func (i *idxData) Quadtree() quadtree.Quadtree { return i.q }
func (i *idxData) Data() []byte { return i.d }

func addIndexBlock(bl elements.ExtendedBlock, i int) (utils.Idxer,error) {
    aa := make([]int64, bl.Len())
    bb := make([]int64, bl.Len())

    for i,_ := range aa {
        e := bl.Element(i)
        aa[i] = int64(e.Type())<<59 | int64(e.Id())
        eq,ok:=e.(elements.Quadtreer)
        if !ok {
            panic(e.String()+" NOT A QUADTREER")
        }
        //bb[i] = qqm[eq.Quadtree()]
        bb[i] = int64(eq.Quadtree())
    }
    
    cc := serializeLocs(aa,bb)
    
    dd,err := pbffile.PreparePbfFileBlock([]byte("IdxBlock"),cc,true)
    if err!=nil { return nil,err }
        
    return &idxData{bl.Idx(),quadtree.Null,dd},nil
}


func MakeLocationsCachePbfIndexzz(
    inChans []chan elements.ExtendedBlock, infn string, prfx string,
    endDate elements.Timestamp, state int64) error {
    
    
    tiles,qq := IterObjectLocations(inChans,1<<16, 1)
    sorted := make(chan elements.ExtendedBlock)
    
    
    
    go func() {
        i:=0
        for bl := range tiles {
            uu,_ := utils.ReadDeltaPackedList(bl.B)
            tt := make(elements.ByElementId, 0, 65536)
            
            t:=elements.Node
            o:=elements.Ref(bl.K&0xffffffff)*65536
            switch bl.K>>43 {
                case 1: t=elements.Way
                case 2: t=elements.Relation
            }
            
            for i,u := range uu {
                if u!=0 {
                    tt=append(tt, read.MakeObjQt(t,o+elements.Ref(i),quadtree.Quadtree(u-1)))
                }
            }
            if len(tt)>0 {
                sorted <- elements.MakeExtendedBlock(i, tt, 0,0,0,nil)
                i+=1
            }
        }
        close(sorted)
    }()
    
    
    grped, err := blocksort.GroupTiles(sorted, 100000,0,1)
    if err!=nil { panic(err.Error()) }
    
    
    outf,err := os.Create(prfx+infn+"-index.pbf")
    if err!=nil { panic(err.Error()) }
    defer outf.Close()
    
    zz,err := writefile.WriteBlocks(grped[0], outf, addIndexBlock, false)
    if err!=nil {
        return err
    }
    
    fmt.Println("have", len(zz), "tiles", len(qq), "qts", utils.MemstatsStr())
    
    
    writeSpecs(prfx, []IdxItem{IdxItem{0,infn,endDate,state}}, []int{len(qq)})
    return nil
}

func MakeLocationsCachePbfIndex(
    inChans []chan elements.ExtendedBlock, infn string, prfx string,
    endDate elements.Timestamp, state int64) error {
 
    outcc := make(chan utils.Idxer)
    go func() {
        wg:=sync.WaitGroup{}
        for i,_ := range inChans {
            wg.Add(1)
            go func(i int) {
                for bl := range inChans[i] {
                    ii := int64(bl.Idx()-1)
                    aa := make([]int64, bl.Len())
                    bb := make([]int64, bl.Len())
                    
                    for j,_ := range aa {
                        e:=bl.Element(j)
                        k := int64(e.Type())<<59 | int64(e.Id())
                        aa[j] = k
                        bb[j] = ii
                    }
                    
                    cc := serializeLocs(aa,bb)
                    dd,err := pbffile.PreparePbfFileBlock([]byte("IdxBlock"),cc,true)
                    if err!=nil { panic(err.Error()) }
                    outcc <- &idxData{bl.Idx(),bl.Quadtree(),dd}
                }
                wg.Done()
            }(i)
        }
        wg.Wait()
        close(outcc)
    }()
    
    
    
    
    outf,err := os.Create(prfx+infn+"-index.pbf")
    if err!=nil { panic(err.Error()) }
    defer outf.Close()
    
    zz:=0
    qq:=make(quadtree.QuadtreeSlice,0,500000)
    for pp := range utils.SortIdxerChan(outcc) {
        dd := pp.(interface{ Data() []byte }).Data()
        pbffile.WriteFileBlockAtEnd(outf, dd)
        zz++
        
        q := pp.(elements.Quadtreer).Quadtree()
        qq=append(qq,q)
    }
    
    fmt.Println("have", zz, "tiles", len(qq), "qts", utils.MemstatsStr())
    
    
    writeSpecs(prfx, []IdxItem{IdxItem{0,infn,endDate,state}}, []int{len(qq)})
    return nil
}


func OpenPbfIndexLocationsCache(prfx string) (LocationsCache,error) {
    rr := idxLocationsCache{}
    rr.prfx = prfx
    
    var err error
    rr.idx,rr.offsets,err = readSpecs(prfx)
    if err!=nil { return nil, err }
    
    return &rr,nil
}


func (fflc *idxLocationsCache) Close() {
    if fflc.changed {
        writeSpecs(fflc.prfx, fflc.idx, fflc.offsets)
        
    }
    
    
}

func (fflc *idxLocationsCache) NumFiles() int {
    return len(fflc.idx)
}

func (fflc *idxLocationsCache) FileSpec(i int) IdxItem {
    return fflc.idx[i]
}

func (fflc *idxLocationsCache) FindTiles(inc <-chan int64) (Locs,TilePairSet) {
    
    ll := Locs{}
    tm := TilePairSet{}
    for ii := range inc {
        ll[elements.Ref(ii)]=TilePair{-1,-1}
    }
    
    for fl,idx := range fflc.idx {
        
        
        fn := fflc.prfx+idx.Filename+"-index.pbf"
        fmt.Println("scan",fn)
        fbs,_,err := readfile.MakeFileBlockChanSplit(fn,4)
        if err!=nil { panic(err.Error()) }
        ts := make([]map[elements.Ref]int64, 4)
        wg:=sync.WaitGroup{}
        for j:=0; j < 4; j++ {
            ts[j]=map[elements.Ref]int64{}
            wg.Add(1)
            go func(j int) {
                for st:=range fbs[j] {
                    aa,bb := readIndexBlock(st.BlockData())
                    for i,a := range aa {
                        if _,ok := ll[elements.Ref(a)]; ok {
                            ts[j][elements.Ref(a)] = bb[i]
                        }
                    }
                }
                wg.Done()
            }(j)
        }
        wg.Wait()
        for _,t:=range ts {
            for k,v:=range t {
                if v==-1 {
                    ll[k] = TilePair{-1,-1}
                } else {
                    ll[k]=TilePair{fl,int(v)}
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

func (fflc *idxLocationsCache) AddTiles(lcs Locs, idx IdxItem) int {
    
    
    
    o := len(fflc.idx)
    
    ts := make(tts,0,len(lcs))
    
    
    mxT := 0
    for k,v:=range lcs {
        if v.File==o {
            if v.Tile > mxT {
                mxT = v.Tile
            }
            ts=append(ts, tt{int64(k),v.Tile})
        } else if v.File==-1 {
            ts=append(ts, tt{int64(k),-1})
        }
    }
    mxT++
    
    ts.Sort()
    
    wf,err := os.Create(fflc.prfx+idx.Filename+"-index.pbf")
    if err!=nil { panic(err.Error()) }
    
    for i := 0; i < len(ts); i+=100000 {
        j := i+100000
        if j>len(ts) {
            j=len(ts)
        }
        
        s := prepBlock(ts[i:j])
        pbffile.WritePbfFileBlock(wf,[]byte("IdxBlock"),s,true)
        
    }
    wf.Close()
    
    idx.Idx=o
    fflc.idx = append(fflc.idx, idx)
    fflc.offsets=append(fflc.offsets, fflc.offsets[len(fflc.offsets)-1]+mxT+1)    
    fflc.changed=true
    return o
}

    
