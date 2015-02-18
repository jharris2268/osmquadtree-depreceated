package writefile

import (
    "github.com/jharris2268/osmquadtree/write"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/pbffile"
    "github.com/jharris2268/osmquadtree/quadtree"
    "sync"
    "os"
    "sort"
    "fmt"
    "io/ioutil"
    "io"
)

type idxData struct {
    i int
    d []byte
}
func (i *idxData) Idx() int { return i.i }

type IdxItem struct {
    Idx int
    Quadtree quadtree.Quadtree
    Len int64
    Isc bool
}

type blockIdx []IdxItem
func (bi blockIdx) Len() int {return len(bi) }
func (bi blockIdx) Swap(i,j int) { bi[j],bi[i]=bi[i],bi[j] }
func (bi blockIdx) Less(i,j int) bool { return bi[i].Quadtree < bi[j].Quadtree }

func (bi blockIdx) Quadtree(i int) quadtree.Quadtree { return bi[i].Quadtree}
func (bi blockIdx) IsChange(i int) bool { return bi[i].Isc}
func (bi blockIdx) BlockLen(i int) int64 { return bi[i].Len}


func addQtBlock(bl elements.ExtendedBlock, idxoff int) (utils.Idxer,error) {
    
    a,err := write.WriteExtendedBlock(bl,false,true)
    if err!=nil { return nil,err }
    b,err:=pbffile.PreparePbfFileBlock([]byte("OSMData"),a,true)
    if err!=nil { return nil,err }
    return &idxData{bl.Idx()-idxoff, b},nil
}

func addFullBlock(bl elements.ExtendedBlock, idxoff int, isc bool, bh []byte) (utils.Idxer,error) {
    a,err := write.WriteExtendedBlock(bl,isc,true)
    if err!=nil { return nil,err }
    
    b,err:=pbffile.PreparePbfFileBlock(bh,a,true)
    if err!=nil { return nil,err }
    return &idxData{bl.Idx()-idxoff, b},nil
}

func WritePbfFile(inc <-chan elements.ExtendedBlock, outfn string, idx bool, isc bool) (write.BlockIdxWrite,error) {
    outf,err:=os.Create(outfn)
    if err!=nil {
        return nil,err
    }
    defer outf.Close()
    
    if !idx {
        return WritePbfIndexed(inc, outf, nil, idx, isc)
    }
    
    tf,err := ioutil.TempFile("","osmquadtree.writefile.tmp")
    if err!=nil {
        return nil,err
    }
        
    defer func() {
        tf.Close()
        os.Remove(tf.Name())
    }()
        
    return WritePbfIndexed(inc, outf, tf, idx, isc)
}
    
func WritePbfIndexed(inc <-chan elements.ExtendedBlock, outf io.Writer, tf io.ReadWriter, idx bool, isc bool) (write.BlockIdxWrite,error) {
    
    mt := sync.Mutex{}
    qm := map[int]quadtree.Quadtree{}
    addBl := func(bl elements.ExtendedBlock,i int) (utils.Idxer,error) {
        mt.Lock()
        qm[bl.Idx()] = bl.Quadtree()
        mt.Unlock()
        return addFullBlock(bl,i,isc,[]byte("OSMData"))
    }
    
    if !idx {
        ii,err := WriteBlocks(inc,outf,addBl,isc)
        if err!=nil { return nil,err}
        sort.Sort(blockIdx(ii))
        for i,_:=range ii {
            
            
            ii[i].Quadtree=qm[i]
            ii[i].Isc=isc
        }
        
        
        return blockIdx(ii),err
    }
    
    
    ii,err := WriteBlocks(inc,tf,addBl,false)
    if err!=nil {
        return nil,err
    }
    
    tfs,ok := tf.(interface{
        Sync() bool
        Seek(int,int)
    })
    if ok {
        
    
        tfs.Sync()
        tfs.Seek(0,0)
    } else {
        tfr, ok := tf.(interface{ Reset() })
        if ok {
            tfr.Reset()
        }
    }
    
    sort.Sort(blockIdx(ii))
    for i,_:=range ii {
        
        
        ii[i].Quadtree=qm[i]
        ii[i].Isc=isc
    }
    
    header,err := write.WriteHeaderBlock(quadtree.PlanetBbox(), blockIdx(ii))
    if err!=nil {
        return nil,err
    }
    
    dd,err := pbffile.PreparePbfFileBlock([]byte("OSMHeader"),header,true)
    if err!=nil { return nil,err }
    
    err = pbffile.WriteFileBlockAtEnd(outf,dd)
    if err!=nil { return nil,err }
    
    ll,err := io.Copy(outf, tf)
    if err!=nil {
        return nil,err
    }
    
    nm:=""
    tfn, ok := tf.(interface{Name() string})
    if ok {
        nm = tfn.Name()
    }
    fmt.Printf("copied %d bytes from %s\n", ll, nm)
    return blockIdx(ii),nil
    
    
}

 
func WriteQts(inc <-chan elements.ExtendedBlock, outfn string) error {
    outf,err:=os.Create(outfn)
    if err!=nil {
        return err
    }
    defer outf.Close()
    _,err = WriteBlocks(inc,outf,addQtBlock,true)
    return err
}

func WriteBlocks(inc <-chan elements.ExtendedBlock,
    outf io.Writer,
    addBlock func(elements.ExtendedBlock, int) (utils.Idxer,error),
    off bool ) ([]IdxItem,error) {

    

    outc:=make(chan utils.Idxer)
    
    go func() {
        wg:=sync.WaitGroup{}
        wg.Add(4)
        fo:=0
        if off {
        
            fb := <- inc
            
            fo := fb.Idx()
            
            t,err := addBlock(fb,fo)
            if err!=nil {
                panic(err.Error())
            }
            outc <- t
        }
        
        for i:=0; i < 4; i++ {
            go func() {
                for bl:=range inc {
                    t,err := addBlock(bl,fo)
                    if err!=nil {
                        panic(err.Error())
                    }
                    outc <- t
                }
                wg.Done()
            }()
        }
        wg.Wait()
        close(outc)
    }()
    
    items:=make([]IdxItem, 0, 400000)
    for p:=range utils.SortIdxerChan(outc) {
        d:=p.(*idxData).d
        if d!=nil {
            
            pbffile.WriteFileBlockAtEnd(outf,d)
            items=append(items, IdxItem{p.Idx(),quadtree.Null,int64(len(d)),false})
        } else {
            println("null data @", p.Idx())
            items=append(items, IdxItem{p.Idx(),quadtree.Null,int64(0),false})
        }
    }
    
    
    return items, nil
}
        
        
            
    
