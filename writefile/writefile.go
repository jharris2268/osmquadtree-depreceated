// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package writefile

import (
    "github.com/jharris2268/osmquadtree/write"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/pbffile"
    "github.com/jharris2268/osmquadtree/quadtree"
    "sync"
    "os"
    //"sort"
    "fmt"
    "io/ioutil"
    "io"
    "time"
)

type idxData struct {
    i int
    d []byte
    q quadtree.Quadtree
}
func (i *idxData) Idx() int { return i.i }

type DataQuadtreer interface {
    Quadtree()  quadtree.Quadtree
    Data()      []byte
}

func (i *idxData) Quadtree() quadtree.Quadtree { return i.q }
func (i *idxData) Data() []byte { return i.d }




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
    return &idxData{bl.Idx()-idxoff, b, quadtree.Null},nil
}

func addFullBlock(bl elements.ExtendedBlock, idxoff int, isc bool, bh []byte) (utils.Idxer,error) {
    a,err := write.WriteExtendedBlock(bl,isc,true)
    if err!=nil { return nil,err }
    
    b,err:=pbffile.PreparePbfFileBlock(bh,a,true)
    if err!=nil { return nil,err }
    return &idxData{bl.Idx()-idxoff, b, bl.Quadtree()},nil
}

func addOrigBlock(bl elements.ExtendedBlock, bh []byte) (utils.Idxer,error) {
    a,err := write.WriteExtendedBlock(bl,false,false)
    if err!=nil { return nil,err }
    
    b,err:=pbffile.PreparePbfFileBlock(bh,a,true)
    if err!=nil { return nil,err }
    return &idxData{bl.Idx(),b, quadtree.Null},err
}


func WritePbfFile(inc <-chan elements.ExtendedBlock, outfn string, idx bool, isc bool, plain bool) (write.BlockIdxWrite,error) {
    outf,err:=os.Create(outfn)
    if err!=nil {
        return nil,err
    }
    defer outf.Close()
    
    if !idx {
        return WritePbfIndexed(inc, outf, nil, idx, isc, plain)
    }
    
    tf,err := ioutil.TempFile("","osmquadtree.writefile.tmp")
    if err!=nil {
        return nil,err
    }
        
    
    defer func() {
        tf.Close()
        os.Remove(tf.Name())
    }()
        
    return WritePbfIndexed(inc, outf, tf, idx, isc, plain)
    
}
    
func WritePbfIndexed(inc <-chan elements.ExtendedBlock, outf io.Writer, tf io.ReadWriter, idx bool, isc bool, plain bool) (write.BlockIdxWrite,error) {
    
    //mt := sync.Mutex{}
    //qm := map[int]quadtree.Quadtree{}
    addBl := func(bl elements.ExtendedBlock,i int) (utils.Idxer,error) {
        //mt.Lock()
        //qm[bl.Idx()] = bl.Quadtree()
        //mt.Unlock()
        return addFullBlock(bl,i,isc,[]byte("OSMData"))
    }
    
    if !idx {
        if plain {
            addBl = func(bl elements.ExtendedBlock, i int) (utils.Idxer,error) {
                return addOrigBlock(bl,[]byte("OSMData"))
            }
        }
        
        return writeUnIndexed(inc,outf,isc,addBl)
        
        
    }
    
    
    ii,err := WriteBlocks(inc,tf,addBl,false)
    //ii,err := WriteBlocksOrdered(inc,tf,addBl)
    if err!=nil {
        return nil,err
    }
    
    return finishAndHeader(outf, tf, ii,isc)
}

func writeUnIndexed(inc <- chan elements.ExtendedBlock, outf io.Writer, isc bool,
    addBl func(bl elements.ExtendedBlock,i int) (utils.Idxer,error) ) (write.BlockIdxWrite,error) {
    
    ii,err := WriteBlocks(inc,outf,addBl,isc)
        
    if err!=nil { return nil,err}
    if ii==nil {
        return nil,nil
    }
        
    //sort.Sort(blockIdx(ii))
    for i,_:=range ii {
        
        
        //ii[i].Quadtree=qm[i]
        ii[i].Isc=isc
    }
    
    
    return blockIdx(ii),err
}


func finishAndHeader(outf io.Writer, tf io.ReadWriter, ii []IdxItem,isc bool) (write.BlockIdxWrite,error) {
    
    tfs,ok := tf.(interface{
        Sync() error
        Seek(int64,int) (int64,error)
    })
    if ok {
        
    
        tfs.Sync()
        tfs.Seek(0,0)
    } else {
        fmt.Println("tempfile not a Seeker...")
        tfr, ok := tf.(interface{ Reset() })
        if ok {
            tfr.Reset()
        } else {
            fmt.Println("tempfile not a Reseter...")
        }
    }
    
    //sort.Sort(blockIdx(ii))
    for i,_:=range ii {
        
        
        //ii[i].Quadtree=qm[i]
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

 
 
 
func WritePbfFileM(inc []chan elements.ExtendedBlock, outfn string, isc bool) (write.BlockIdxWrite,error) {
    outf,err:=os.Create(outfn)
    if err!=nil {
        return nil,err
    }
    defer outf.Close()
    
    tf,err := ioutil.TempFile("","osmquadtree.writefile.tmp")
    if err!=nil {
        return nil,err
    }
        
    
    defer func() {
        tf.Close()
        os.Remove(tf.Name())
    }()
    
     addBl := func(bl elements.ExtendedBlock,i int) (utils.Idxer,error) {
        return addFullBlock(bl,i,isc,[]byte("OSMData"))
    }
    
    ii,err := WriteBlocksOrdered(inc,tf,addBl)
    if err!=nil {
        return nil,err
    }
    
    return finishAndHeader(outf, tf, ii,isc)
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


func WriteBlocksOrdered(
    inchans []chan elements.ExtendedBlock,
    outf io.Writer,
    addBlock func(elements.ExtendedBlock, int) (utils.Idxer, error),
    ) ([]IdxItem, error) {
    
    /*mm := make(chan string)
    go func() {
        for m:=range mm {
            fmt.Println(m)
        }
        fmt.Println("closed mm")
    }()*/
    
    vv := make([]chan utils.Idxer,len(inchans))
    for j,_:=range inchans {
        vv[j] = make(chan utils.Idxer,5)
    }
    
    
    
    for i,_ := range inchans {
        
        go func(i int) {
            //z:=0
            for bl := range inchans[i] {
                //fmt.Println("ch",i,bl.Idx(),i%4,bl.Quadtree())
                t,err := addBlock(bl,0)
                if err!=nil { panic(err.Error()) }
                
                vv[i] <- t
           // z = bl.Idx()
            }
            //mm<-fmt.Sprintf("%d done @ %d",i,z)
            close(vv[i])
        }(i)
    }
            
            
    st:=time.Now()
    items:=make([]IdxItem, 0, 450000)
    rem := 4
    j:=0
    
    var mm string
    
    for rem>0 {
        
        p,ok := <- vv[j%4]
        if !ok {
            
            //mm<-fmt.Sprintf("%d: @ %d rem %d",j%4,j,rem)
            rem -= 1
        } else {
        
            d:=p.(DataQuadtreer)
            if d.Data()!=nil {
                
                
                
                pbffile.WriteFileBlockAtEnd(outf,d.Data())
                li := IdxItem{p.Idx(),d.Quadtree(),int64(len(d.Data())),false}
                items=append(items, li)
                
                mm = fmt.Sprintf("\r%8.1fs %6d %-18s %8d bytes", time.Since(st).Seconds(), li.Idx, li.Quadtree, li.Len)
                if (p.Idx()%3871)==0 {
                    fmt.Printf(mm)
                }
                
            } else {
                fmt.Printf("\n%8.1fs: NULL p.Idx()\n", time.Since(st).Seconds(), p.Idx())
                mm=""
                items=append(items, IdxItem{p.Idx(),d.Quadtree(),int64(0),false})
            }
        }
        
        j++
    }
    fmt.Println(mm)
    
    //close(mm)
    return items, nil
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
            go func(i int) {
                //z:=0
                //l:=0
                for bl:=range inc {
                    t,err := addBlock(bl,fo)
                    if err!=nil {
                        panic(err.Error())
                    }
                    outc <- t
                    //z++
                    //l = bl.Idx()
                }
                //println("finished",i,"(",z,"blocks, last", l,")")
                wg.Done()
            }(i)
        }
        wg.Wait()
        //println("done, closing outc")
        close(outc)
    }()
    
    st:=time.Now()
    items:=make([]IdxItem, 0, 450000)
    for p:=range utils.SortIdxerChan(outc) {
        
        d:=p.(DataQuadtreer)
        if d.Data()!=nil {
            
            
            
            pbffile.WriteFileBlockAtEnd(outf,d.Data())
            items=append(items, IdxItem{p.Idx(),d.Quadtree(),int64(len(d.Data())),false})
            
            if (p.Idx()%12874)==0 {
                
                fmt.Println(items[len(items)-1],time.Since(st).Seconds())
            }
            
        } else {
            println("null data @", p.Idx())
            items=append(items, IdxItem{p.Idx(),d.Quadtree(),int64(0),false})
        }
    }
    
    
    return items, nil
}
        
        
            
    
