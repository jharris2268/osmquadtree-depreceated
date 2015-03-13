// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package locationscache

import (
	"encoding/binary"
	"fmt"
	
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

func unpackTvs(cc []Kbb) *tvs {
	ans := new(tvs)
	ans.l = 0
	ans.t = make([]tp, len(cc)*128*1024)

	for _, c := range cc {
		d, _ := utils.Decompress(c.B, uint64(c.L))
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

func makeCC(qtl map[int64]int64, tvs *tvs, out chan Kbb, blSize int64) int {
	a := int64(0)
	b := make([]int64, blSize, blSize)
	c := false
	d := 0
	for i := 0; i < tvs.l; i++ {
		if tvs.t[i].i/blSize != a {
			if c {
                cc,_ := utils.PackDeltaPackedList(b)
				out <- Kbb{a, 0, cc}
				d++
			}
			a = tvs.t[i].i / blSize
			b = make([]int64, blSize, blSize)
			c = false
		}
		//println(i, a, tvs.t[i].i,tvs.t[i].b)
		ii := tvs.t[i].i - a*blSize

		s, ok := qtl[tvs.t[i].b]
		if !ok {
			println("wtf", quadtree.Quadtree(tvs.t[i].b).String())
			panic(0)
		}
		b[ii] = s + 1
		c = true
	}
	if c {
        cc,_ := utils.PackDeltaPackedList(b)
		out <- Kbb{a, 0, cc}
		d++
	}
	return d
}

type int64Slice []int64

func (tvs int64Slice) Len() int           { return len(tvs) }
func (tvs int64Slice) Less(i, j int) bool { return tvs[i] < tvs[j] }
func (tvs int64Slice) Swap(i, j int)      { tvs[j], tvs[i] = tvs[i], tvs[j] }


type Kbb struct {
	K int64
	L int
	B []byte
}



func IterObjectLocations(inputChans []chan elements.ExtendedBlock, blSize int64, np int) (<-chan Kbb, []int64) {

	outc := make(chan Kbb)

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

    
    go func() {

        wg:=sync.WaitGroup{}
        wg.Add(len(inputChans))
        //fmt.Println("have",len(inputChans),"input chans")
        for _,inc:=range inputChans {
            go func(inc chan elements.ExtendedBlock) {
        
                vvs := map[int64]*tvs{}
                for bl:=range inc {
                    
                    qt := int64(bl.Quadtree())
                    if qt<0 {
                        //panic("block with qt < 0??")
                        fmt.Println(bl.Idx(),"??",bl)
                        continue
                    }
                    qtc <- qt
                    for i := 0; i < bl.Len(); i++ {
                        o := bl.Element(i)
                        id := int64(o.Type()) << 59
                        id |= int64(o.Id())

                        oi := id >> 25
                        _, ok := vvs[oi]
                        if !ok {
                            vvs[oi] = &tvs{make([]tp, 128*1024), 0}
                        }
                        vs := vvs[oi]
                        vs.t[vs.l].i = id
                        vs.t[vs.l].b = qt
                        vs.l += 1

                        if vs.l == 128*1024 {
                            sort.Sort(vs)
                            b := vs.pack()
                            bp, _ := utils.Compress(b)
                            outc <- Kbb{oi, len(b), bp}
                            vs.l = 0
                            b = nil
                        }
                        vvs[oi] = vs
                    }
                }
                for oi, vs := range vvs {
                    if vs.l > 0 {
                        sort.Sort(vs)
                        b := vs.pack()
                        bp, _ := utils.Compress(b)
                        outc <- Kbb{oi, len(b), bp}
                    }
                    delete(vvs, oi)
                }
                
                wg.Done()
                
            }(inc)
        }
    
        wg.Wait()
        close(outc)
		close(qtc)
		//println("done")
	}()

	//debug.FreeOSMemory()
	tc := map[int64][]Kbb{}
	for c := range outc {
		tc[c.K] = append(tc[c.K], c)
	}

	

	outch := make(chan Kbb)

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
		
		for i := 0; i < np; i++ {
			wg.Add(1)
			go func(i int) {

				for j := i; j < len(vl); j += np {

					k := vl[j]
					tt := unpackTvs(tc[k])
					nbs := 0
					for _, c := range tc[k] {
						nbs += len(c.B)
					}
					nb := makeCC(qtl, tt, outch, blSize)

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
    
    return outch, qtm
}
    
