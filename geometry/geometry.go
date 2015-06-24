// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
	"fmt"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"

	"errors"
	"math"

	/*"osmfeats5/osmread"
	"osmfeats5/quadtree"
	"osmfeats5/simpleobj"
	"reflect"*/)

type GeometryType int

const (
	NullGeometry GeometryType = iota
	Point
	Linestring
	Polygon
	MultiPoint
	MultiLinestring
	MultiPolygon
	Multi
)

var UnrecognisedGeometryError = errors.New("Unrecognised Geometry Type")

type Coord interface {
	Ref() elements.Ref
	Lon() int64
	Lat() int64

	LonLat() (float64, float64)
	XY() (float64, float64)
}

type Geometry interface {
	elements.FullElement

	AsWkt(bool) string
	AsWkb(bool) []byte
	AsGeoJson(bool) interface{}
	GeometryType() GeometryType
	GeometryData() []byte
	Bbox() quadtree.Bbox
	IsValid() bool
}

type PointGeometry interface {
	Geometry
	Coord() Coord
}

type LinestringGeometry interface {
	Geometry
	NumCoords() int
	Coord(int) Coord

	ZOrder() int64
}

type PolygonGeometry interface {
	Geometry
	NumRings() int
	NumCoords(int) int
	Coord(int, int) Coord

	ZOrder() int64
	Area() float64
}

type MultiGeometry interface {
	Geometry

	NumGeometries() int
	NumRings(int) int
	NumCoords(int, int) int
	Coord(int, int, int) Coord

	ZOrder() int64
	Area() float64
}

func ExtractGeometryBboxData(o elements.Element) (GeometryType, *quadtree.Bbox, []byte, error) {
	gg, ok := o.(Geometry)
	if ok {
		bx := gg.Bbox()
		return gg.GeometryType(), &bx, gg.GeometryData(), nil
	} else {
		gp, ok := o.(elements.PackedGeometry)
		if ok {
			gd := gp.GeometryData()
			gt, bx, err := readGeometryBbox(gd)
			if err != nil {
				return 0, nil, nil, err
			}
			if bx != nil {
				return gt, bx, gd, nil
			}

			gt, cc, _, _, err := extractGeometryData(gd)

			if err != nil {
				return 0, nil, nil, err
			}
			bx = quadtree.NullBbox()
			for _, c := range cc {
				expandBbox(bx, c[0])
			}
			return gt, bx, gd, nil
		}
	}
	return 0, nil, nil, UnrecognisedGeometryError
}

func ExtractGeometryBbox(o elements.Element) (GeometryType, *quadtree.Bbox, error) {
	gg, ok := o.(Geometry)
	if ok {
		bx := gg.Bbox()
		return gg.GeometryType(), &bx, nil
	} else {
		gp, ok := o.(elements.PackedGeometry)
		if ok {
			gt, bx, err := readGeometryBbox(gp.GeometryData())
			if err != nil {
				return 0, nil, err
			}
			if bx != nil {
				return gt, bx, nil
			}

			gt, cc, _, _, err := extractGeometryData(gp.GeometryData())

			if err != nil {
				return 0, nil, err
			}
			bx = quadtree.NullBbox()
			for _, c := range cc {
				expandBbox(bx, c[0])
			}
			return gt, bx, nil
		}
	}
	return 0, nil, UnrecognisedGeometryError
}

func ReadGeometryZOrderWayArea(gd []byte) (int64, float64, error) {
	zo := int64(0)
	ar := float64(0)

	fa, fb := false, false
	pos, msg := utils.ReadPbfTag(gd, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(gd, pos) {
		switch msg.Tag {
		case 11:
			zo = utils.UnZigzag(msg.Value)
			fa = true
		case 12:
			ar = math.Float64frombits(msg.Value)
			fb = true
		}
		if fa && fb {
			return zo, ar, nil
		}
	}

	return 0, 0, UnrecognisedGeometryError
}

func ExtractGeometry(e elements.Element) (Geometry, error) {

	ge, ok := e.(Geometry)
	if ok {
		return ge, nil
	}

	gp, ok := e.(elements.PackedGeometry)
	if !ok {
		return nil, errors.New(fmt.Sprintf("object not a Geometry type"))
	}

	return extractGeometry(gp)
}

func hasTags(needed map[string]string) func(TagsEditable) bool {
	return func(tt TagsEditable) bool {
		for k, v := range needed {
			if !tt.Has(k) || tt.Get(k) != v {
				return false
			}
		}
		return true
	}
}

func GenerateGeometries(
	makeInChan func() <-chan elements.ExtendedBlock,
	fbx *quadtree.Bbox,
	tagsFilter map[string]TagTest,
	recalc bool, msgs bool) (<-chan elements.ExtendedBlock, error) {

	A := makeInChan()

	B := AddWayCoords(A, fbx)
	var C <-chan elements.ExtendedBlock

	if _, ok := tagsFilter["parent_highway"]; ok {
		C = AddNodeParent(B, FindParentHighway, "highway", "highway", "parent_highway")
	} else {
		println("skip parent_highway")
		C = B
	}
	var D <-chan elements.ExtendedBlock
	if _, ok := tagsFilter["min_admin_level"]; ok {

		D = AddRelationRange(C, hasTags(map[string]string{"boundary": "adminstrative", "type": "boundary"}), "admin_level", AdminLevels)
	} else {
		println("skip admin_level")
		D = C
	}
	var D2 <-chan elements.ExtendedBlock
	if _, ok := tagsFilter["bus_routes"]; ok {

		D2 = AddRelationRange(D, hasTags(map[string]string{"type": "route", "route": "bus"}), "ref", RouteList("bus_routes").Proc)
	} else {
		println("skip bus_routes")
		D2 = D
	}

	var D3 <-chan elements.ExtendedBlock
	if _, ok := tagsFilter["cycle_routes"]; ok {

		D3 = AddRelationRange(D2, hasTags(map[string]string{"type": "route", "route": "bicycle"}), "network", RouteList("cycle_routes").Proc)
	} else {
		println("skip cycle_routes")
		D3 = D
	}

	E := MakeGeometries(D3, tagsFilter)

	hasArea := false
	for _, t := range tagsFilter {
		if t.IsPoly == "yes" {
			hasArea = true
		}
	}
	var F <-chan elements.ExtendedBlock
	if hasArea {
		F = HandleRelations(E, tagsFilter)
	} else {
		println("skip relations")
		Ff := make(chan elements.ExtendedBlock)
		go func() {
			for b := range E {
				nb := make(elements.ByElementId, 0, b.Len())
				for i := 0; i < b.Len(); i++ {
					e := b.Element(i)
					if e.Type() == elements.Geometry {
						if IsFeature(e.(elements.FullElement).Tags(), tagsFilter) {
							nb = append(nb, e)
						}
					}
				}
				if len(nb) > 0 {
					Ff <- elements.MakeExtendedBlock(b.Idx(), nb, b.Quadtree(), b.StartDate(), b.EndDate(), nil)
				}
			}
			close(Ff)
		}()
		F = Ff
	}

	result := make(chan elements.ExtendedBlock)

	go func() {

		j := 0
		var b elements.ExtendedBlock
		for b = range F {
			b.SetIdx(j)

			if recalc {
				for i := 0; i < b.Len(); i++ {
					fe := b.Element(i).(Geometry)
					qt, _ := quadtree.Calculate(fe.Bbox(), 0.025, 18)
					fe.SetQuadtree(qt)
				}
			}

			result <- b
			j++
		}
		close(result)
	}()

	return result, nil

}
