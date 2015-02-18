package quadtree

import (
    "fmt"
)


func ToFloat(i int64) float64 {
    return float64(i)*0.0000001
}

func ToInt(f float64) int64 {
    if f<0 {
        return int64((f*10000000.0)-0.5)
    }
    return int64((f*10000000.0)+0.5)
}


//Bounding box made up of integer values of 10^-7 degrees.
type Bbox struct {
    Minx, Miny, Maxx, Maxy int64
}

const MaxLon = int64(1800000000)
const MaxLat = MaxLon/2
const MinLon = -1*MaxLon
const MinLat = -1*MaxLat


func NullBbox() *Bbox {
    return &Bbox{MaxLon,MaxLat,MinLon,MinLat}
}

func PlanetBbox() *Bbox {
    return &Bbox{MinLon,MinLat,MaxLon,MaxLat}
}

func (bbox Bbox) String() string {
    return fmt.Sprintf("[%10d, %10d, %10d, %10d]",
            bbox.Minx,bbox.Miny,
            bbox.Maxx,bbox.Maxy)
}

//Return true if bbox overlaps other
func (bbox Bbox) Intersects(other Bbox) bool {
    if (bbox.Minx > other.Maxx) { return false }
    if (bbox.Miny > other.Maxy) { return false }
    if (bbox.Maxx < other.Minx) { return false }
    if (bbox.Maxy < other.Miny) { return false }
    return true
}

//Return true if bbox contains other
func (bbox Bbox) Contains(other Bbox) bool {
    if (bbox.Minx > other.Minx) { return false }
    if (bbox.Miny > other.Minx) { return false }
    if (bbox.Maxx < other.Maxx) { return false }
    if (bbox.Maxy < other.Maxy) { return false }
    return true
}

func (bbox Bbox) ContainsXY(x,y int64) bool {
    if (bbox.Minx > x) { return false }
    if (bbox.Miny > y) { return false }
    if (bbox.Maxx < x) { return false }
    if (bbox.Maxy < y) { return false }
    return true
}


func (bbox *Bbox) ExpandBox(other Bbox) *Bbox {
    return bbox.Expand(other.Minx,other.Miny,other.Maxx,other.Maxy)
}

func (bbox *Bbox) Expand(mx,my,Mx,My int64) *Bbox {
    if mx < bbox.Minx { bbox.Minx = mx }
    if my < bbox.Miny { bbox.Miny = my }
    if Mx > bbox.Maxx { bbox.Maxx = Mx }
    if My > bbox.Maxy { bbox.Maxy = My }
    
    return bbox

}

func (bbox *Bbox) ExpandXY(x,y int64) *Bbox {
    return bbox.Expand(x,y,x,y)
}


func (bbox *Bbox) ExpandPt(pt interface{} ) *Bbox {
    lnlt,ok := pt.(interface{
        Lon() int64
        Lat() int64
    })
    
    if !ok {
        return nil
    }
    return bbox.ExpandXY(lnlt.Lon(), lnlt.Lat())
}




type LonLatBlock interface {
    Len() int
	Lon(i int) int64
	Lat(i int) int64
}

func PointInPoly(verts LonLatBlock, testlon, testlat int64) bool {
	/*
	   from http://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html

	   Copyright (c) 1970-2003, Wm. Randolph Franklin

	   Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

	       1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
	       2. Redistributions in binary form must reproduce the above copyright notice in the documentation and/or other materials provided with the distribution.
	       3. The name of W. Randolph Franklin may not be used to endorse or promote products derived from this Software without specific prior written permission.

	   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

	   int pnpoly(int nvert, float *vertx, float *verty, float testx, float testy)
	   {
	     int i, j, c = 0;
	     for (i = 0, j = nvert-1; i < nvert; j = i++) {
	       if ( ((verty[i]>testy) != (verty[j]>testy)) &&
	   	 (testx < (vertx[j]-vertx[i]) * (testy-verty[i]) / (verty[j]-verty[i]) + vertx[i]) )
	          c = !c;
	     }
	     return c;
	   }*/
	j, c := verts.Len()-1, false
	for i := 0; i < verts.Len(); i++ {
		if (verts.Lat(i) > testlat) != (verts.Lat(j) > testlat) {
			tp := float64(verts.Lon(j)-verts.Lon(i))*float64(testlat-verts.Lat(i))/float64(verts.Lat(j)-verts.Lat(i)) + float64(verts.Lon(i))

			if float64(testlon) < tp {
				c = !c
			}
		}
		j = i
	}
	return c
}
