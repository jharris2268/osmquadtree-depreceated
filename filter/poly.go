// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package filter

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"
    
	"strings"
)

type lonLat struct {
	lon, lat int64
}
type lonLatSlice []lonLat

func (ll lonLatSlice) Len() int { return len(ll) }
func (ll lonLatSlice) Lon(i int) int64 { return ll[i].lon }
func (ll lonLatSlice) Lat(i int) int64 { return ll[i].lat }

type locTestPolygon struct {
	verts quadtree.LonLatBlock
	bb    *quadtree.Bbox
}

// MakeLocTestPolygon constructs a LocTest which checks if a point is within
// the specified polygon. Note that the ContainsQuadtree function tests the four corners
// of the quadtree only.
func MakeLocTestPolygon(lons, lats []int64) LocTest {
    verts := make(lonLatSlice,len(lons))
    for i,ln := range lons {
        verts[i] = lonLat{ln,lats[i]}
    }
        
    return locTestPolygon{verts, nil }
}
    

func (tp locTestPolygon) Bbox() quadtree.Bbox {
	if tp.bb == nil {
		tp.bb = &quadtree.Bbox{1800000000, 900000000, -1800000000, -900000000}
		for i:=0; i < tp.verts.Len(); i++ {
			if tp.verts.Lon(i) < tp.bb.Minx {
				tp.bb.Minx = tp.verts.Lon(i)
			}
			if tp.verts.Lon(i) > tp.bb.Maxx {
				tp.bb.Maxx = tp.verts.Lon(i)
			}

			if tp.verts.Lat(i) < tp.bb.Miny {
				tp.bb.Miny = tp.verts.Lat(i)
			}
			if tp.verts.Lat(i) > tp.bb.Maxy {
				tp.bb.Maxy = tp.verts.Lat(i)
			}
		}
	}
	return *tp.bb
}

func (tp locTestPolygon) Contains(x, y int64) bool {

	if !locTestBbox(tp.Bbox()).Contains(x, y) {
		return false
	}
	return quadtree.PointInPoly(tp.verts, x, y)

}
func (tp locTestPolygon) ContainsQuadtree(qt quadtree.Quadtree) bool {
	bx := qt.Bounds(0.05)
	if !tp.Bbox().Contains(bx) {
		return false
	}
	if !tp.Contains(bx.Minx, bx.Miny) {
		return false
	}
	if !tp.Contains(bx.Minx, bx.Maxy) {
		return false
	}
	if !tp.Contains(bx.Maxx, bx.Miny) {
		return false
	}
	if !tp.Contains(bx.Maxx, bx.Maxy) {
		return false
	}
	return true
}

func (tp locTestPolygon) String() string {
	return fmt.Sprintf("locTestPolygon: %d verts %s", tp.verts.Len(), tp.Bbox().String())
}

func (ltp locTestPolygon) Intersects(other quadtree.Bbox) bool {
	return ltp.Bbox().Intersects(other)
}

func (ltp locTestPolygon) IntersectsQuadtree(qt quadtree.Quadtree) bool {
	bx := qt.Bounds(0.05)
	return ltp.Intersects(bx)

}

type locTestPolygonMulti struct {
	polys []locTestPolygon
	holes []locTestPolygon
	bb    *quadtree.Bbox
}

func (tp locTestPolygonMulti) Bbox() quadtree.Bbox {
	if tp.bb == nil {

		tp.bb = &quadtree.Bbox{1800000000, 900000000, -1800000000, -900000000}
		for _, pl := range tp.polys {
			tp.bb.ExpandBox(pl.Bbox())
		}
	}

	return *tp.bb
}

func (tp locTestPolygonMulti) Contains(x, y int64) bool {
	for _, p := range tp.polys {
		if !p.Contains(x, y) {
			return false
		}
	}
	for _, p := range tp.holes {
		if p.Contains(x, y) {
			return false
		}
	}
	return false
}

func (tp locTestPolygonMulti) ContainsQuadtree(qt quadtree.Quadtree) bool {
	bx := qt.Bounds(0.05)
	if !tp.Bbox().Contains(bx) {
		return false
	}
	if !tp.Contains(bx.Minx, bx.Miny) {
		return false
	}
	if !tp.Contains(bx.Minx, bx.Maxy) {
		return false
	}
	if !tp.Contains(bx.Maxx, bx.Miny) {
		return false
	}
	if !tp.Contains(bx.Maxx, bx.Maxy) {
		return false
	}
	return true
}

func (tp locTestPolygonMulti) Intersects(other quadtree.Bbox) bool {
	return tp.Bbox().Intersects(other)
}
func (tp locTestPolygonMulti) IntersectsQuadtree(qt quadtree.Quadtree) bool {
	bx := qt.Bounds(0.05)
	return tp.Intersects(bx)

}
func (tp locTestPolygonMulti) String() string {
	return fmt.Sprintf("locTestPolygonMulti: %d polys, %d holes %s", len(tp.polys), len(tp.holes), tp.Bbox().String())
}


// ReadPolyFile reads the osmosis poly file fn (see
// http://wiki.openstreetmap.org/wiki/Osmosis/Polygon_Filter_File_Format
// ) and constructs a LocTest which returns true if a point is within the
// polygon. Note that the ContainsQuadtree function tests the four corners
// of the quadtree only.
func ReadPolyFile(fn string) (LocTest, error) {
	fl, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	scan := bufio.NewScanner(fl)

	//nme:=""
	i := 0
	inply, label := false, ""
	curr := locTestPolygon{}
    currverts:=lonLatSlice{}
	res := locTestPolygonMulti{}
	for scan.Scan() {
		ln := strings.TrimSpace(scan.Text())
		//println(i,ln)
		if i == 0 {
			//nme=ln
		} else if ln == "" {
			//pass
		} else if inply {
			if ln == "END" {
				inply = false
                curr.verts=currverts
				if strings.HasPrefix(label, "!") {
					res.holes = append(res.holes, curr)
				} else {
					res.polys = append(res.polys, curr)
				}
				curr = locTestPolygon{}
                currverts=lonLatSlice{}
			} else {
				xy := strings.Fields(ln)
				if len(xy) != 2 {
					return nil, errors.New(ln + "  expected lines to be two numbers")
				}
				x, _, err := utils.ParseStringInt(xy[0])
				if err != nil {
					return nil, err
				}
				y, _, err := utils.ParseStringInt(xy[1])
				if err != nil {
					return nil, err
				}
				currverts = append(currverts, lonLat{x, y})

			}
		} else if ln == "END" {
			//pass
		} else {
			label = ln
			inply = true
		}
		i++
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if len(res.polys) == 1 && len(res.holes) == 0 {
		return res.polys[0], nil
	}
	return res, nil
	
}
