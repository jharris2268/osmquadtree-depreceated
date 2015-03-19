// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

// +build !windows

package locationscache

import (
	"encoding/binary"
	
	"github.com/jmhodges/levigo"

	"path"
	
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/utils"
)

type Cache struct {
	db      *levigo.DB
	cache   *levigo.Cache
	wo      *levigo.WriteOptions
	ro      *levigo.ReadOptions
}

func (c *Cache) open(path string, create bool) error {
	

	opts := levigo.NewOptions()

	opts.SetCreateIfMissing(create)
	c.cache = levigo.NewLRUCache(16 * 1024 * 1024)
	opts.SetCache(c.cache)
	opts.SetMaxOpenFiles(64)
	opts.SetBlockRestartInterval(128)
	opts.SetWriteBufferSize(64 * 1024 * 1024)
	
	
	db, err := levigo.Open(path, opts)
	if err != nil {
		return err
	}
	c.db = db
	c.wo = levigo.NewWriteOptions()
	c.ro = levigo.NewReadOptions()
	return nil
}

func idToKeyBuf(id int64, k int) []byte {
	b := make([]byte, 10)
	if id < 0 {
		for i := 0; i < 8; i++ {
			b[i] = '\377'
		}
	} else {
		binary.BigEndian.PutUint64(b, uint64(id))
	}
	binary.BigEndian.PutUint16(b[8:], uint16(k))
	return b
}

func (c *Cache) Close() {
	if c.ro != nil {
		c.ro.Close()
		c.ro = nil
	}
	if c.wo != nil {
		c.wo.Close()
		c.wo = nil
	}
	if c.db != nil {
		c.db.Close()
		c.db = nil
	}
	if c.cache != nil {
		c.cache.Close()
		c.cache = nil
	}

}

func (c *Cache) Stats() string {
	return c.db.PropertyValue("leveldb.stats")
}

func MakeLocationsCacheLevelDb(
        inputChans []chan elements.ExtendedBlock,
        inputfn string, prfx string, enddate int64, state int64) error {

    outch,qtm := IterObjectLocations(inputChans,32, 2)


    outcache := new(Cache)
	outcache.open(prfx+"locationscache", true)


	for cc := range outch {

		keyBuf := idToKeyBuf(cc.K, 0)
		//keyBuf = append(keyBuf, '\000')
		outcache.db.Put(outcache.wo, keyBuf, cc.B)
	}

	_, ii := path.Split(inputfn)
	outcache.db.Put(outcache.wo, idToKeyBuf(-1, 0), make_date_header(ii, enddate, state))
	qtp, _ := utils.PackDeltaPackedList(qtm)
	outcache.db.Put(outcache.wo, idToKeyBuf(-1, 65535), qtp)
	outcache.Close()
	return nil
}

func write_locsmap_tile(cache *Cache, k int64, v []int64, o int) {
	kb := idToKeyBuf(k, o)
	//kb = append(kb, o)

	vb := packCC(v[:])
	cache.db.Put(cache.wo, kb, vb)
}

func write_locsmap(cache *Cache, lm LocsMap, o int, edd int64, fstr string, state int64) error {
	for k, v := range lm {
		write_locsmap_tile(cache, k, v, o)
	}
	out := make_date_header(fstr, edd, state)
	cache.db.Put(cache.wo, idToKeyBuf(-1, o), out)
	return nil
}

/*    
func AddNewEntries(prfx string, lm LocsMap, o int, edd elements.Timestamp, fstr string, state int64) error {

    
       

	cache := new(Cache)
	err := cache.open(prfx+"locationscache", false)
	if err != nil {
		panic(err.Error())
	}
	defer cache.Close()
    
    
	return write_locsmap(cache, lm, o, int64(edd), fstr, state)
}*/

func make_date_header(fstr string, edd int64, state int64) []byte {
	out := make([]byte, 30+len(fstr))
	p := binary.PutVarint(out, edd)
	q := binary.PutUvarint(out[p:], uint64(len(fstr)))
	p += q

	copy(out[p:], []byte(fstr))
	p += len(fstr)

	if state > 0 {
		q = binary.PutUvarint(out[p:], uint64(state))
		p += q
	}
	return out[:p]

}

type LocsMap map[int64]([]int64)

func compkey(a []byte, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, ai := range a {
		if ai != b[i] {
			return false
		}
	}
	return true
}

func get_tile(cache *Cache, k int64) ([]int64, error) {
	kb := idToKeyBuf(k, 0)
	it := cache.db.NewIterator(cache.ro)
	defer it.Close()
	it.Seek(kb)

	var data []byte
	for it = it; it.Valid(); it.Next() {
		if !compkey(it.Key()[:8], kb[:8]) {
			break
		}
		data = it.Value()
	}
	//data, err := cache.db.Get(cache.ro, kb)
	err := it.GetError()
	if err != nil {
		return nil, err
	}
	if data == nil {
		//return [32]int64{}, nil
		return nil, nil
	}

	return unpackTile(data), nil
}



func GetCacheSpecsLevelDb(prfx string) ([]IdxItem, []quadtree.Quadtree, error) {

	cache := new(Cache)
	err := cache.open(prfx+"locationscache", false)
	if err != nil {
		return nil,nil,err
	}
    
    
    
	defer cache.Close()
    return getCacheSpecs(cache)
    
}

func getCacheSpecs(cache *Cache) ([]IdxItem, []quadtree.Quadtree, error) {

	kb := idToKeyBuf(-1, 0)
	it := cache.db.NewIterator(cache.ro)
	defer it.Close()

	it.Seek(kb)
    if it.Valid() && len(it.Key())==9 {
        it.Seek(kb[:9])
    }
	ans := []IdxItem{}
	qts := []quadtree.Quadtree{}
	for it = it; it.Valid(); it.Next() {
		item := IdxItem{}
        if len(it.Key())==9 {
            item.Idx = int(it.Key()[8])
        } else {
            item.Idx = int(it.Key()[8])<<8 | int(it.Key()[9])
        }

		//println(it.Key()[8],it.Key()[9],item.Idx)

		v := it.Value()
		if item.Idx == 65535 {
			qtsp, _ := utils.ReadDeltaPackedList(v)
            qts =make([]quadtree.Quadtree,len(qtsp))
            for i,q:=range qtsp {
                qts[i]=quadtree.Quadtree(q)
            }
            
		} else {
			
			ts, p := utils.ReadVarint(v, 0)
            item.Timestamp = elements.Timestamp(ts)
			f := []byte{}
			f, p = utils.ReadData(v, p)
			item.Filename = string(f)
			if p < len(v) {
				s := uint64(0)
				s, p = utils.ReadUvarint(v, p)
				item.State = int64(s)
			}
			ans = append(ans, item)
		}
	}
	//println("len(ans)=",len(ans),"len(qts)=",len(qts))
	return ans, qts,nil
}

func GetLastStateLevelDb(prfx string) (int64,error) {
	ii, _,err := GetCacheSpecsLevelDb(prfx)
    if err!=nil { return 0, err }
	if len(ii) == 0 {
		return 0,nil
	}
	return ii[len(ii)-1].State,nil
}

func unpackTile(data []byte) []int64 {
	res, _ := utils.ReadDeltaPackedList(data)
	return res
	/*

		a := int64(0)
		p := 0
		ans := make([]int64,32)
		for i := 0; i < 32; i++ {
			b, q := binary.Varint(data[p:])
			a += b
			ans[i] = a
			p += q
		}
		return ans*/
}

//func GetTiles(prfx string, inids <-chan int64) (map[int64]bool, LocsMap) {
/*func GetTiles(prfx string, inids <-chan int64) LocsMap {

	cache := new(Cache)
	cache.open(prfx+"locationscache", false)
	defer cache.Close()
    
    return getTiles(cache, inids)
}*/

func getTiles(cache *Cache, inids <-chan int64) LocsMap {
    
	//ans := map[int64]bool{}
	lm := LocsMap{}
	mm := map[int64]bool{}
	for i := range inids {
		if i < 0 {
			println("wtf", i)
			continue
		}
		k := i / 32
		_, ok := lm[k]
		if !ok {
			if _, ok := mm[k]; !ok {
				t, _ := get_tile(cache, k)
				if t != nil {
					lm[k] = t
				} else {
					mm[k] = true
				}
			}
		}

		//ans[lm[k][i-32*k]-1] = true
	}
	//return ans, lm
	println(len(lm), len(mm))
	return lm
}

type levelDbLocationsCache struct {
    cache   *Cache
    locsMap LocsMap
    idx     []IdxItem
}

func OpenLevelDbLocationsCache(prfx string) (LocationsCache,error) {
    cache := new(Cache)
	err := cache.open(prfx+"locationscache", false)
    if err!=nil {
        return nil, err
    }
    
    idx,_,err := getCacheSpecs(cache)
    if err!=nil {
        cache.Close()
        return nil,err
    }
    
    return &levelDbLocationsCache{cache, nil,idx},nil
}

func (ldlc *levelDbLocationsCache) Close() {
    ldlc.cache.Close()
}

func (ldlc *levelDbLocationsCache) NumFiles() int {
    return len(ldlc.idx)
}

func (ldlc *levelDbLocationsCache) FileSpec(i int) IdxItem {
    return ldlc.idx[i]
}

    

func (ldlc *levelDbLocationsCache) FindTiles(inc <-chan int64) (Locs,TilePairSet) {
    nc := make(chan int64)
    ll := Locs{}
 
    go func() {
        for i := range inc {
            ll[elements.Ref(i)] = TilePair{-1,-1}
            nc <- i
        }
        close(nc)
    }()
    
    lm := getTiles(ldlc.cache, nc)
    ldlc.locsMap = lm
    
    tm := TilePairSet{}
    
    for k,_ := range ll {
        
        r := int64(k)
        
        if _,ok := lm[r/32]; ok {
            j := lm[r/32][r % 32]
            
            
            if j>0 {
                jj :=int(j)-1
                tp := TilePair{jj>>32, jj&0xffffffff}
                
                tm[tp]=true
                ll[k] = tp
                
            }
        }
    }
    
    return ll,tm
}

func (ldlc *levelDbLocationsCache) AddTiles(lcs Locs, idx IdxItem) int {
    o := len(ldlc.idx)
    
    lma := LocsMap{}
    for rf,tp := range lcs {
        k := int64(rf)
        v := 0
        if tp.File!=-1 {
            v = ((tp.File<<32) | tp.Tile) +1
        }
        _,ok := lma[k/32]
        if !ok {
            t,ok := ldlc.locsMap[k/32]
            if !ok {
                t = make([]int64,32)
            }
            lma[k/32]=t[:]
        }
        lma[k/32][k%32] = int64(v)
    }
    
        
    for k, v := range lma {
		write_locsmap_tile(ldlc.cache, k, v, o)
	}
    
    out := make_date_header(idx.Filename, int64(idx.Timestamp), idx.State)
	ldlc.cache.db.Put(ldlc.cache.wo, idToKeyBuf(-1, o), out)
    
    
    return o
}
    

func zzGetTileQts(prfx string, inids <-chan int64) ([]string, []quadtree.Quadtree ,map[quadtree.Quadtree]bool,elements.Timestamp,error) {
    cache := new(Cache)
	err := cache.open(prfx+"locationscache", false)
	if err != nil {
		return nil,nil,nil,0,err
	}
	defer cache.Close()
    idx,qts,err := getCacheSpecs(cache)
    fns:=make([]string, len(idx))
    for i,ii := range idx {
        fns[i] = prfx+ii.Filename
    }
    
    ans := map[quadtree.Quadtree]bool{}
    
    lm := LocsMap{}
	mm := map[int64]bool{}
	for i := range inids {
		if i < 0 {
			println("wtf", i)
			continue
		}
		k := i / 32
		_, ok := lm[k]
		if !ok {
			if _, ok := mm[k]; !ok {
				t, _ := get_tile(cache, k)
				if t != nil {
					lm[k] = t
				} else {
					mm[k] = true
				}
			}
		}
        a := get_alloc_rb(lm,i)
        if a>=0 {
            ans[qts[a]]=true
        }
		//ans[lm[k][i-32*k]-1] = true
	}
	//return ans, lm
	println(len(lm), len(mm),len(ans))
	return fns, qts, ans,idx[len(idx)-1].Timestamp,nil
}
        
        
    

func get_alloc(lm LocsMap, i int64) int64 {
	k := i / 32
	_, ok := lm[k]
	if !ok {
		return -1
	}
	return lm[k][i-32*k] - 1
}

func get_alloc_rb(lm LocsMap, i int64) int64 {
	a := get_alloc(lm, i)
	if a <= 0 {
		return a
	}
	return a & 0xffffffff
}
