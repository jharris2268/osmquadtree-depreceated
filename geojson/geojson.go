// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geojson

import (
    "encoding/json"
    "os"
    "compress/gzip"
    "fmt"
    "strings"
    "math"
    "io"
    
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/geometry"
    
)

type idxData struct {
    i int
    d []byte
}
func (id idxData) Idx() int { return id.i }

func WriteOsmJson(sblc <-chan utils.Idxer, outfn string, header string, footer string) (int, int, error) {

	fmt.Println("outfn: ", outfn)
	var outfz io.Writer

	outf, err := os.OpenFile(outfn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		return 0, 0, err

	}

	if strings.HasSuffix(outfn, ".gz") {
		outfgz := gzip.NewWriter(outf)
		defer outfgz.Close()
		outfz = outfgz
	} else {
		defer outf.Close()
		outfz = outf
	}

	tb := 0
	bc := 0
	li := 0
	outfz.Write([]byte(header))
	for s := range utils.SortIdxerChan(sblc) {
		if bc > 0 {
			outfz.Write([]byte(",\n"))
		}
		d := s.(idxData).d
		bc += 1
		tb += len(d)
		if (s.Idx() % 100) == 0 {
			fmt.Printf("%-6d: %d blocks, %10.1f mb\n", s.Idx(), bc, float64(tb)/1024.0/1024.0)
		}

		outfz.Write(d)
		li = s.Idx()
	}
	fmt.Printf("%-6d: %d blocks, %10.1f mb\n", li, bc, float64(tb)/1024.0/1024.0)
	outfz.Write([]byte(footer))
	return tb, bc, nil
}

func MakeFeature(o geometry.Geometry, asMerc bool) (map[string]interface{}, error) {
    om := map[string]interface{}{}
    om["type"] = "Feature"
    om["id"] = o.Id()
    pp := map[string]interface{}{}
    tt := o.Tags()
    for j := 0; j < tt.Len(); j++ {
        k := tt.Key(j)
        if k=="" {
            continue
        }
        switch k[0] {
        case '!':
            ii, _ := utils.ReadVarint([]byte(tt.Value(j)), 0)
            pp[k[1:]] = ii
        case '%':
            ii, _ := utils.ReadUvarint([]byte(tt.Value(j)), 0)
            pp[k[1:]] = math.Float64frombits(ii)
        case '$':
            pp[k[1:]] = nil
        default:
            pp[k] = tt.Value(j)
        }
    }
    om["properties"] = pp

    om["geometry"] = o.AsGeoJson(asMerc)
    
    return om,nil
}

func MakeFeatureCollection(bl elements.ExtendedBlock, asMerc bool) map[string]interface{} {
	bll := map[string]interface{}{}
	bll["type"] = "FeatureCollection"
	ps := map[string]interface{}{}
	ps["quadtree"] = bl.Quadtree().String()
	bt := bl.Tags()
	if bt != nil {
		for i := 0; i < bt.Len(); i++ {
			ps[bt.Key(i)] = bt.Value(i)
		}
	}

	bll["properties"] = ps

	oo := make([]interface{}, bl.Len())
	for i, _ := range oo {
        
        
		o, err := geometry.ExtractGeometry(bl.Element(i))
		if err != nil {
			panic(err.Error())
		}
		
        om, err := MakeFeature(o, asMerc)
        if err != nil {
			panic(err.Error())
		}
        
		oo[i] = om
	}

	bll["features"] = oo
	return bll
}

func WriteGeoJson(sblc <-chan elements.ExtendedBlock, outfn string) (int, int, error) {
	outc := make(chan utils.Idxer)
	go func() {
		for bl := range sblc {

			bll := MakeFeatureCollection(bl, false)

			blc, err := json.Marshal(bll)
			if err != nil {
				panic(err.Error())
			}

			outc <- idxData{bl.Idx(), blc}
		}
		close(outc)
	}()
	return WriteOsmJson(outc, outfn, `{"type": "FeatureCollection","features":[`+"\n", "\n]}")
}

