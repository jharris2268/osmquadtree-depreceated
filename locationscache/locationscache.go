package locationscache

import (
	"encoding/binary"
	"fmt"
	"github.com/jmhodges/levigo"

	"path"
	"runtime/debug"
	"sort"
	"sync"

	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/utils"
)

type tp struct {
	i int64
	b int64
}

type tvs struct {
	t []tp
	l int
}

func (tvs *tvs) Len() int           { return tvs.l }
func (tvs *tvs) Less(i, j int) bool { return tvs.t[i].i < tvs.t[j].i }
func (tvs *tvs) Swap(i, j int)      { tvs.t[j], tvs.t[i] = tvs.t[i], tvs.t[j] }

func (tvs *tvs) pack() []byte {

	ans := make([]byte, 20*tvs.l+10)

	p := binary.PutUvarint(ans, uint64(tvs.l))
	ai, ab := int64(0), int64(0)
	for i := 0; i < tvs.l; i++ {

		q := binary.PutVarint(ans[p:], tvs.t[i].i-ai)
		p += q
		q = binary.PutVarint(ans[p:], tvs.t[i].b-ab)
		p += q
		ai = tvs.t[i].i
		ab = tvs.t[i].b
	}

	//println("fr",tvs.t[0].i,tvs.t[0].b,"to",tvs.t[tvs.l-1].i,tvs.t[tvs.l-1].b,"tvs.l=",tvs.l,"len(ans)=",len(ans),"p=",p)
	return ans[:p]
}

func readTvs(ans *tvs, c []byte) {
	l, p := binary.Uvarint(c)
	//println(len(c),l,p)
	ai, ab := int64(0), int64(0)
	for i := 0; i < int(l); i++ {
		ii, q := binary.Varint(c[p:])
		ai += ii
		p += q

		bb, q := binary.Varint(c[p:])
		ab += bb
		p += q
		ans.t[ans.l].i = ai
		ans.t[ans.l].b = ab
		//println(ans.l, ai, ab)
		ans.l++

	}

}

func unpackTvs(cc []kbb) *tvs {
	ans := new(tvs)
	ans.l = 0
	ans.t = make([]tp, len(cc)*128*1024)

	for _, c := range cc {
		d, _ := utils.Decompress(c.b, uint64(c.l))
		readTvs(ans, d)
	}
	sort.Sort(ans)
	return ans
}

func packCC(cc []int64) []byte {
	ans := make([]byte, 320)
	p := 0
	a := int64(0)
	for _, c := range cc {
		p = utils.WriteVarint(ans, p, c-a)
		a = c
	}
	return ans[:p]
}

func makeCC(qtl map[int64]int64, tvs *tvs, out chan kbb) int {
	a := int64(0)
	b := make([]int64, 32, 32)
	c := false
	d := 0
	for i := 0; i < tvs.l; i++ {
		if tvs.t[i].i/32 != a {
			if c {
				out <- kbb{a, 0, packCC(b)}
				d++
			}
			a = tvs.t[i].i / 32
			b = make([]int64, 32, 32)
			c = false
		}
		//println(i, a, tvs.t[i].i,tvs.t[i].b)
		ii := tvs.t[i].i - a*32

		s, ok := qtl[tvs.t[i].b]
		if !ok {
			println("wtf", quadtree.Quadtree(tvs.t[i].b).String())
			panic(0)
		}
		b[ii] = s + 1
		c = true
	}
	if c {
		out <- kbb{a, 0, packCC(b)}
		d++
	}
	return d
}

type int64Slice []int64

func (tvs int64Slice) Len() int           { return len(tvs) }
func (tvs int64Slice) Less(i, j int) bool { return tvs[i] < tvs[j] }
func (tvs int64Slice) Swap(i, j int)      { tvs[j], tvs[i] = tvs[i], tvs[j] }


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

type kbb struct {
	k int64
	l int
	b []byte
}

func MakeLocationsCache(
        dataFunc func(func(int, elements.ExtendedBlock) error) error,
        inputfn string, prfx string, enddate int64, state int64) error {
	
	outc := make(chan kbb)

	qtm := make(int64Slice, 0, 400000)
	qtc := make(chan int64)
	qtcwg := sync.WaitGroup{}
	qtcwg.Add(1)
	go func() {
		for q := range qtc {
			qtm = append(qtm, q)
		}
		qtcwg.Done()
	}()

    vvs := make([]map[int64]*tvs, 4)
    for i,_:=range vvs {
        vvs[i]=map[int64]*tvs{}
    }

	addBlock := func(j int, bl elements.ExtendedBlock) error {
        
        qt := int64(bl.Quadtree())
        if qt<0 {
            return nil
        }
        qtc <- qt
        for i := 0; i < bl.Len(); i++ {
            o := bl.Element(i)
            id := int64(o.Type()) << 59
            id |= int64(o.Id())

            oi := id >> 25
            _, ok := vvs[j][oi]
            if !ok {
                vvs[j][oi] = &tvs{make([]tp, 128*1024), 0}
            }
            vs := vvs[j][oi]
            vs.t[vs.l].i = id
            vs.t[vs.l].b = qt
            vs.l += 1

            if vs.l == 128*1024 {
                sort.Sort(vs)
                b := vs.pack()
                bp, _ := utils.Compress(b)
                outc <- kbb{oi, len(b), bp}
                vs.l = 0
                b = nil
            }
            vvs[j][oi] = vs
        }
        return nil
    }
    
    go func() {
        err := dataFunc(addBlock)
        if err!=nil {
            panic(err.Error())
        }
        
        for _,vv := range vvs {
            for oi, vs := range vv {
                if vs.l > 0 {
                    sort.Sort(vs)
                    b := vs.pack()
                    bp, _ := utils.Compress(b)
                    outc <- kbb{oi, len(b), bp}
                }
                delete(vv, oi)
            }
            
        }
        close(outc)
		close(qtc)
		//println("done")
	}()

	//debug.FreeOSMemory()
	tc := map[int64][]kbb{}
	for c := range outc {
		tc[c.k] = append(tc[c.k], c)
	}

	outcache := new(Cache)
	outcache.open(prfx+"locationscache", true)

	outch := make(chan kbb)

	//tv,tv2:=0,0
	vl := make(int64Slice, 0, 256)
	for k, _ := range tc {
		vl = append(vl, k)
	}
	println("len(vl)=", len(vl), "; cap(vl)=", cap(vl))
	sort.Sort(vl)

	qtcwg.Wait()

	sort.Sort(qtm)
	qtl := map[int64]int64{}
	for i, q := range qtm {
		qtl[q] = int64(i)
	}
	println("len(qtc)=", len(qtm), "; cap(qtc)=", cap(qtm), "; len(qtl)=", len(qtl))

	go func() {
		wg := sync.WaitGroup{}
		ml := sync.Mutex{}
		np := 4
		for i := 0; i < np; i++ {
			wg.Add(1)
			go func(i int) {

				for j := i; j < len(vl); j += np {

					k := vl[j]
					tt := unpackTvs(tc[k])
					nbs := 0
					for _, c := range tc[k] {
						nbs += len(c.b)
					}
					nb := makeCC(qtl, tt, outch)

					ml.Lock()
					fmt.Printf("[%d %-5d]: %-4d blobs [%-10d bytes] => %-8d objs in %-7d pcks [%5.1f%%]\n", i, k, len(tc[k]), nbs, tt.l, nb, float64(tt.l)/float64(nb)*100.0/32.0)
					delete(tc, k)
					ml.Unlock()

					debug.FreeOSMemory()
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		close(outch)
	}()

	for cc := range outch {

		keyBuf := idToKeyBuf(cc.k, 0)
		//keyBuf = append(keyBuf, '\000')
		outcache.db.Put(outcache.wo, keyBuf, cc.b)
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


type NewLoc struct {
    Id, Lc int64
}
type NewLocSlice []NewLoc
func (nls NewLocSlice) Len() int { return len(nls) }
func (nls NewLocSlice) Less(i,j int) bool { return nls[i].Id < nls[j].Id }
func (nls NewLocSlice) Swap(i,j int) { nls[i],nls[j]=nls[j],nls[i] }
func (nls NewLocSlice) Sort() { sort.Sort(nls) }

    
func AddNewEntries(prfx string, lm LocsMap, o int, edd elements.Timestamp, fstr string, state int64) error {

    
       

	cache := new(Cache)
	err := cache.open(prfx+"locationscache", false)
	if err != nil {
		panic(err.Error())
	}
	defer cache.Close()
    
    
	return write_locsmap(cache, lm, o, int64(edd), fstr, state)
}

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

type IdxItem struct {
	Idx       int
	Filename  string
	Timestamp elements.Timestamp
	State     int64
}

func GetCacheSpecs(prfx string) ([]IdxItem, []quadtree.Quadtree, error) {

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

func GetLastState(prfx string) (int64,error) {
	ii, _,err := GetCacheSpecs(prfx)
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
func GetTiles(prfx string, inids <-chan int64) LocsMap {

	cache := new(Cache)
	cache.open(prfx+"locationscache", false)
	defer cache.Close()
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

func GetTileQts(prfx string, inids <-chan int64) ([]string, []quadtree.Quadtree,map[quadtree.Quadtree]bool,elements.Timestamp,error) {
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
	return fns,qts,ans,idx[len(idx)-1].Timestamp,nil
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
