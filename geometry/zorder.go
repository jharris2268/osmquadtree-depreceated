// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
	"github.com/jharris2268/osmquadtree/elements"
	
    "github.com/jharris2268/osmquadtree/quadtree"
	//"osmfeats5/simpleobj"
	"strconv"
	"fmt"
    "strings"

	"errors"
	"math"
	//"sort"
)

var hworder map[string]int64

func init_hworder() {
	hworder = map[string]int64{}
	hworder["service"] = 1
	hworder["footway"] = 1
	hworder["path"] = 1
	hworder["footpath"] = 1
	hworder["cycleway"] = 1
	hworder["bridlepath"] = 1
	hworder["residential"] = 3
	hworder["unclassified"] = 3
	hworder["road"] = 3
	hworder["tertiary_link"] = 4
	hworder["tertiary"] = 4
	hworder["secondary_link"] = 6
	hworder["secondary"] = 6
	hworder["primary_link"] = 7
	hworder["primary"] = 7
	hworder["trunk_link"] = 8
	hworder["trunk"] = 8
	hworder["motorway_link"] = 9
	hworder["motorway"] = 9
}

func is_true(v string) bool {
	switch strings.ToLower(v) {
	case "1", "yes", "true":
		return true
	}
	return false
}

func is_false(v string) bool {
	switch strings.ToLower(v) {
	case "0", "no", "false":
		return true
	}
	return false
}

func find_zorder(tt elements.Tags) (int64,error) {
    if hworder ==nil {
        init_hworder()
    }
	
	zo := int64(0)
	l := int64(0)
	haszo := ""
	//kk,vv:=make([][]byte,0,tt.Len()+2),make([][]byte,0,tt.Len()+2)
	for i := 0; i < tt.Len(); i++ {
		k := tt.Key(i)
		v := tt.Value(i)
		if k == "z_order" {
			//return obj,nil
			haszo = v
			//break
		}

		switch k {
		case "highway":
			if len(hworder) == 0 {
				init_hworder()
			}
			z, ok := hworder[v]
			if ok && z > 1 {
				if z > zo {
					zo = z
				}
			}
		case "railway":
			if 5 > zo {
				zo = 5
			}
		case "layer":
			ll, e := strconv.Atoi(v)

			if e == nil {
				l += int64(ll)
			}
		case "bridge":
			if !is_false(v) {
				l += 1
			}
		case "tunnel":
			if !is_false(v) {
				l -= 1
			}
		}
		//kk=append(kk,[]byte(k))
		//vv=append(vv,[]byte(v))
	}

	var err error
	if haszo != "" {
		zo, err = strconv.ParseInt(haszo, 10, 64)
		if err != nil {
			return 0, errors.New("coundn't parse z_order tag " + haszo)
		}

	}

	zo += l * 10
    
    return zo,nil
}

/*
    
	g := obj.Data()
	gp, ok := g.(*polygonGeom)
	if ok {
		gp.zorder = zo
		gp.area, err = calcWayArea(obj.Data().(SimpleObjGeometry))
		if err != nil {
			return nil, errors.New("prob with calc poly area " + err.Error())
		}
		return osmread.MakeSimpleObj(
			obj.ObjectType(), obj.ObjectId(), obj.Info(),
			obj.Tags(), gp, obj.Quadtree(), obj.ChangeType()), nil
	}
	lg, ok := g.(*linestringGeom)
	if ok {
		lg.zorder = zo
		return osmread.MakeSimpleObj(
			obj.ObjectType(), obj.ObjectId(), obj.Info(),
			obj.Tags(), lg, obj.Quadtree(), obj.ChangeType()), nil
	}
	mg, ok := g.(*multiGeom)
	if ok {
		mg.zorder = zo
		for i, gg := range mg.geoms {
			switch gg.(type) {
			case *polygonGeom:
				gp := gg.(*polygonGeom)
				gp.zorder = zo
				gp.area, err = calcWayArea(gp)
				if err != nil {
					return obj, errors.New("prob with calc poly area " + err.Error())
				}
				mg.geoms[i] = gp
			case *linestringGeom:
				lg := gg.(*linestringGeom)
				lg.zorder = zo
				mg.geoms[i] = lg
			}
		}

		return osmread.MakeSimpleObj(
			obj.ObjectType(), obj.ObjectId(), obj.Info(),
			obj.Tags(), mg, obj.Quadtree(), obj.ChangeType()), nil
	}

	return nil, errors.New("object not a polygonGeom or a lineGeom")
}
*/

func same_point(a, b Coord) bool {
	if a.Lon() != b.Lon() {
		return false
	}
	if a.Lat() != b.Lat() {
		return false
	}
	return true
}

func drop_repeats(rr []Coord) []Coord {
	if rr == nil || len(rr) == 0 {
		return rr
	}
	np := make([]Coord, 0, len(rr))
	lp := rr[0]
	for i, p := range rr {
		if i == 0 || !same_point(p, lp) {
			np = append(np, p)

		}
		lp = p
	}
	return np
}


func calculate_polygon_area(poly [][]Coord) (float64,error) {
    polyArea := 0.0
    
    for i, p := range poly {
		a := 1.0
		if i > 0 {
			a = -1.0
		}
		poly[i] = drop_repeats(p)
		if len(poly[i]) < 4 {
			return 0, errors.New("Not enough points in ring")
		}
		pa, is_ccw := calculate_ring_area(p)
		if is_ccw != (i != 0) {
			reverse_ring(poly[i])
		}
		polyArea += pa * a
	}
	return polyArea, nil
}

func reverse_ring(rr []Coord) {
	for i := 0; i < len(rr)/2; i++ {
		li := len(rr) - 1 - i
		rr[i], rr[li] = rr[li], rr[i]
	}
}



func calculate_ring_area(rr []Coord) (float64, bool) {

	numpt := len(rr)
	if numpt < 3 {
		return 0, false
	}
	rrx, rry := make([]float64, len(rr)), make([]float64, len(rr))
	for i, p := range rr {
		rrx[i], rry[i] = p.XY()
	}

	ss := 0.0
	for i := 1; i < numpt; i++ {
		if i == numpt-1 {
			ss += rrx[0] * (rry[1] - rry[i-1])
		} else {
			ss += rrx[i] * (rry[i+1] - rry[i-1])
		}
	}

	return math.Abs(ss) / 2.0, ss > 0

	
}

func rings_intersect(lhs, rhs []Coord) bool {
	for i := 0; i < len(lhs)-1; i++ {
		for j := 0; j < len(rhs)-1; j++ {
			if lines_intersect(lhs[i], lhs[i+1], rhs[j], rhs[j+1]) {
				return true
			}
		}
	}
	return false
}

type llb []Coord
func (l llb) Len() int { return len(l) }
func (l llb) Lat(i int) int64 { return l[i].Lat() }
func (l llb) Lon(i int) int64 { return l[i].Lon() }

func ring_contains(outer, inner []Coord) bool {
	if !rings_intersect(outer, inner) {
		return quadtree.PointInPoly(llb(outer), inner[0].Lon(),inner[0].Lat())
	}
	return false
}


/*
func calcWayArea(data SimpleObjGeometry) (float64, error) {
	poly, ok := data.(*polygonGeom)
	if !ok {
		return 0.0, errors.New("not a polygonGeom")
	}
	polyArea := 0.0
	for i, p := range poly.rings {
		a := 1.0
		if i > 0 {
			a = -1.0
		}
		poly.rings[i] = dropRepeats(p)
		if len(poly.rings[i]) < 4 {
			return 0, errors.New("Not enough points in ring")
		}
		pa, is_ccw := calculate_ring_area(p)
		if is_ccw != (i != 0) {
			reverseRing(poly.rings[i])
		}
		polyArea += pa * a
	}
	return polyArea, nil
}*/

func lines_intersect(p0, p1, p2, p3 Coord) bool {
	s1_x := float64(p1.Lon() - p0.Lon())
	s1_y := float64(p1.Lat() - p0.Lat())
	s2_x := float64(p3.Lon() - p2.Lon())
	s2_y := float64(p3.Lat() - p2.Lat())

	qx := float64(p0.Lon() - p2.Lon())
	qy := float64(p0.Lat() - p2.Lat())

	s := (-s1_y*qx + s1_x*qy) / (-s2_x*s1_y + s1_x*s2_y)
	t := (s2_x*qy - s2_y*qx) / (-s2_x*s1_y + s1_x*s2_y)

	return (s >= 0 && s <= 1 && t >= 0 && t <= 1)
}




func findParentHighway(pp []string) string {
	mv := ""
	sc := int64(0)

	if hworder == nil {
		init_hworder()
	}
	if len(pp) == 1 {
		return pp[0]
	}
	for _, p := range pp {
		s, ok := hworder[p]
		if ok && (s > sc || (s == sc && strings.HasSuffix(mv, "link"))) {
			mv = p
			sc = s
		}
	}
	if mv == "" {
		ppm := map[string]int{}
		for _, p := range pp {
			ppm[p] += 1
		}
		maxm := 0
		for k, v := range ppm {
			if v > maxm {
				mv = k
			}
		}
		if len(ppm) > 1 {
			fmt.Println("pick", mv, "from", ppm)
		}
	}
	return mv
}
