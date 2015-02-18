package geometry

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/utils"
    
    "fmt"
    "strings"
)

type coordImpl struct {
    ref elements.Ref
    lon,lat int64
}

type coordImplSlice []coordImpl
func (cis coordImplSlice) Len() int { return len(cis) }
func (cis coordImplSlice) Ref(i int) elements.Ref { return cis[i].ref }


func (co coordImpl) Ref() elements.Ref { return co.ref }
func (co coordImpl) Lon() int64 { return co.lon }
func (co coordImpl) Lat() int64 { return co.lat }
func (co coordImpl) LonLat() (float64,float64) { return utils.AsFloat(co.lon),utils.AsFloat(co.lat) }
func (co coordImpl) XY() (float64,float64) {
    a,b := co.LonLat()
    return quadtree.Mercator(a,b)
}
//func (co coordImpl) XY() []float64 { return []float64{co.lon),utils.AsFloat(co.lat) } }

func makeBbox(cc []Coord) *quadtree.Bbox {
    bx:=quadtree.NullBbox()
    return expandBbox(bx, cc)
}

func expandBbox(bx *quadtree.Bbox, cc []Coord) *quadtree.Bbox {
    for _,c := range cc {
        bx.ExpandXY(c.Lon(),c.Lat())
    }
    return bx
}


type pointGeometryImpl struct {
    ct   elements.ChangeType
    id   elements.Ref
    info elements.Info
    tags elements.Tags
    qt   quadtree.Quadtree
    
    coord Coord
}
 
func makePointGeometry(gp elements.FullElement, tg elements.Tags, coord Coord) PointGeometry {
    
    return &pointGeometryImpl{gp.ChangeType(), gp.Id(), gp.Info(),tg, gp.Quadtree(),coord}
}

type linestringGeometryImpl struct {
    ct   elements.ChangeType
    id   elements.Ref
    info elements.Info
    tags elements.Tags
    qt   quadtree.Quadtree
    
    coords []Coord
    zorder int64
    bbox   *quadtree.Bbox
}
 
func makeLinestringGeometry(gp elements.FullElement, tg elements.Tags, coords []Coord, zorder int64) LinestringGeometry {
    i := gp.Id()
    switch gp.Type() {
        case elements.Way:
            i |= elements.Ref(1<<59)
        
    }
    bb := makeBbox(coords)
    return &linestringGeometryImpl{gp.ChangeType(), i, gp.Info(), tg, gp.Quadtree(),coords, zorder, bb}
}

type polygonGeometryImpl struct {
    ct   elements.ChangeType
    id   elements.Ref
    info elements.Info
    tags elements.Tags
    qt   quadtree.Quadtree
    
    coords [][]Coord
    zorder int64
    area   float64
    bbox   *quadtree.Bbox
}


func makePolygonGeometry(gp elements.FullElement, tg elements.Tags, coords [][]Coord, zorder int64, area float64) PolygonGeometry {
    i:=gp.Id()
    switch gp.Type() {
        case elements.Way:
            i |= elements.Ref(1<<59)
        case elements.Relation:
            i |= elements.Ref(2<<59)
        
    }
    
    
    bb := makeBbox(coords[0])
    return &polygonGeometryImpl{gp.ChangeType(), i, gp.Info(), tg, gp.Quadtree(),coords, zorder, area, bb}
}


type multiGeometryImpl struct {
    ct   elements.ChangeType
    id   elements.Ref
    info elements.Info
    tags elements.Tags
    qt   quadtree.Quadtree
    
    coords [][][]Coord
    zorder int64
    area float64
    bbox   *quadtree.Bbox
}

func makeMultiGeometry(gp elements.FullElement, tg elements.Tags, coords [][][]Coord, zorder int64, area float64) MultiGeometry {
    
    i:=gp.Id()
    switch gp.Type() {
        case elements.Way:
            i |= elements.Ref(1<<59)
        case elements.Relation:
            i |= elements.Ref(2<<59)
        
    }
    
    
    bb := quadtree.NullBbox()
    for _,cc := range coords[0] {
        expandBbox(bb, cc)
    }
    return &multiGeometryImpl{gp.ChangeType(), i, gp.Info(), tg, gp.Quadtree(),coords, zorder, area, bb}
}



func (pt *pointGeometryImpl) Type() elements.ElementType { return elements.Geometry }
func (ln *linestringGeometryImpl) Type() elements.ElementType { return elements.Geometry }
func (py *polygonGeometryImpl) Type() elements.ElementType { return elements.Geometry }
func (mg *multiGeometryImpl) Type() elements.ElementType { return elements.Geometry }

func (pt *pointGeometryImpl) Id() elements.Ref { return pt.id }
func (ln *linestringGeometryImpl) Id() elements.Ref { return ln.id }
func (py *polygonGeometryImpl) Id() elements.Ref { return py.id }
func (mg *multiGeometryImpl) Id() elements.Ref { return mg.id }

func (pt *pointGeometryImpl) Info() elements.Info { return pt.info }
func (ln *linestringGeometryImpl) Info() elements.Info { return ln.info }
func (py *polygonGeometryImpl) Info() elements.Info { return py.info }
func (mg *multiGeometryImpl) Info() elements.Info { return mg.info }

func (pt *pointGeometryImpl) Tags() elements.Tags { return pt.tags }
func (ln *linestringGeometryImpl) Tags() elements.Tags { return ln.tags }
func (py *polygonGeometryImpl) Tags() elements.Tags { return py.tags }
func (mg *multiGeometryImpl) Tags() elements.Tags { return mg.tags }

func (pt *pointGeometryImpl) ChangeType() elements.ChangeType { return pt.ct }
func (ln *linestringGeometryImpl) ChangeType() elements.ChangeType { return ln.ct }
func (py *polygonGeometryImpl) ChangeType() elements.ChangeType { return py.ct }
func (mg *multiGeometryImpl) ChangeType() elements.ChangeType { return mg.ct }

func (pt *pointGeometryImpl) Quadtree() quadtree.Quadtree { return pt.qt }
func (ln *linestringGeometryImpl) Quadtree() quadtree.Quadtree { return ln.qt }
func (py *polygonGeometryImpl) Quadtree() quadtree.Quadtree { return py.qt }
func (mg *multiGeometryImpl) Quadtree() quadtree.Quadtree { return mg.qt }

func (pt *pointGeometryImpl) SetChangeType(ct elements.ChangeType) { pt.ct=ct }
func (ln *linestringGeometryImpl) SetChangeType(ct elements.ChangeType) { ln.ct=ct }
func (py *polygonGeometryImpl) SetChangeType(ct elements.ChangeType) { py.ct=ct }
func (mg *multiGeometryImpl) SetChangeType(ct elements.ChangeType) { mg.ct=ct }

func (pt *pointGeometryImpl) SetQuadtree(qt quadtree.Quadtree) { pt.qt=qt }
func (ln *linestringGeometryImpl) SetQuadtree(qt quadtree.Quadtree) { ln.qt=qt }
func (py *polygonGeometryImpl) SetQuadtree(qt quadtree.Quadtree) { py.qt=qt }
func (mg *multiGeometryImpl) SetQuadtree(qt quadtree.Quadtree) { mg.qt=qt }

func (pt *pointGeometryImpl) Pack() []byte { return elements.PackFullElement(pt,pt.GeometryData()) }
func (ln *linestringGeometryImpl) Pack() []byte { return elements.PackFullElement(ln,ln.GeometryData()) }
func (py *polygonGeometryImpl) Pack() []byte { return elements.PackFullElement(py,py.GeometryData()) }
func (mg *multiGeometryImpl) Pack() []byte { return elements.PackFullElement(mg,mg.GeometryData()) }

func (pt *pointGeometryImpl) AsWkt(prj bool) string { return fmt.Sprintf("POINT(%s)", ptWkt(pt.coord, prj)) }
func (ln *linestringGeometryImpl) AsWkt(prj bool) string { return fmt.Sprintf("LINESTRING%s", ringWkt(ln.coords, prj)) }
func (py *polygonGeometryImpl) AsWkt(prj bool) string { return polyWkt(py.coords, prj) }

func ptWkt(c Coord, prj bool) string {
    x,y := c.LonLat()
    if prj {
        x,y = quadtree.Mercator(x,y)
    }
    
    return fmt.Sprintf("%f %f", x,y)
}

func ringWkt(cc []Coord, prj bool) string {
    pts:=make([]string,len(cc))
    for i,c:=range cc {
        pts[i] = ptWkt(c,prj)
    }
    return fmt.Sprintf("(%s)",strings.Join(pts,", "))
}

func polyWkt(cc [][]Coord, prj bool) string {
    rings := make([]string,len(cc))
    for i,c:=range cc {
        rings[i] = ringWkt(c,prj)
    }
    return fmt.Sprintf("POLYGON(%s)", strings.Join(rings,", "))
}

func (mg *multiGeometryImpl) AsWkt(prj bool) string { 
    ss := make([]string,len(mg.coords))
    for i,cc:=range mg.coords {
        ss[i] = polyWkt(cc,prj)
    }
    return fmt.Sprintf("GEOMETRYCOLLECTION(%s)",strings.Join(ss,", "))
}

func ptWkb(c Coord, prj bool) []byte {
    ans:=make([]byte, 16)
    ans,_ = writeXY(ans, 0, c, prj)
    return ans
}

func writeXY(ans []byte, p int, c Coord, prj bool) ([]byte,int) {
    x,y := c.LonLat()
    if prj {
        x,y = quadtree.Mercator(x,y)
    }
    
    utils.WriteFloat64(ans,p,x)
    utils.WriteFloat64(ans,p+8,x)
    return ans,p+16
}

func ringWkb(cc []Coord, prj bool) []byte {
    ans:=make([]byte, 16*len(cc)+4)
    p := utils.WriteInt32(ans,0,int32(len(cc)))
    for _,c:= range cc {
        ans,p = writeXY(ans,p,c,prj)
    }
    return ans
}
    
    

func (pt *pointGeometryImpl) AsWkb(prj bool) []byte {
    return append([]byte{0,0,0,0,1}, ptWkb(pt.coord, prj)...)
}
func (ln *linestringGeometryImpl) AsWkb(prj bool) []byte {
    return append([]byte{0,0,0,0,2}, ringWkb(ln.coords, prj)...)
}
        
func (py *polygonGeometryImpl) AsWkb(prj bool) []byte {
    return append([]byte{0}, polyWkb(py.coords, prj)...)
}

func joinArr(aa [][]byte) []byte {
    tl:=0
    for _,a:=range aa {
        tl+=len(a)
    }
    res:=make([]byte, tl)
    i:=0
    for _,a:=range aa{
        copy(res[i:],a)
        i+=len(a)
    }
    return res
}
    
    
    

func polyWkb(ccs [][]Coord, prj bool) []byte {
    rr := make([][]byte, len(ccs)+1)
    rr[0] = []byte{0,0,0,3}
    for i,cc := range ccs {
        rr[i+1] = ringWkb(cc, prj)
    }
    return joinArr(rr)
}


func (mg *multiGeometryImpl) AsWkb(prj bool) []byte {
    rr := make([][]byte, len(mg.coords)+1)
    rr[0] = []byte{0,0,0,7}
    for i,cc := range mg.coords {
        rr[i+1] = polyWkb(cc, prj)
    }
    return joinArr(rr)
}


func (pt *pointGeometryImpl) GeometryType() GeometryType { return Point }
func (ln *linestringGeometryImpl) GeometryType() GeometryType { return Linestring } 
func (py *polygonGeometryImpl) GeometryType() GeometryType { return Polygon }
func (mg *multiGeometryImpl) GeometryType() GeometryType { return Multi }

func (pt *pointGeometryImpl) GeometryData() []byte { return packPointData(pt.coord) }
func (ln *linestringGeometryImpl) GeometryData() []byte { return packLinestringData(ln.coords, ln.zorder, ln.bbox) }
func (py *polygonGeometryImpl) GeometryData() []byte { return packPolygonData(py.coords, py.zorder, py.area, py.bbox) }
func (mg *multiGeometryImpl) GeometryData() []byte { return packMultiGeometryData(mg.coords, mg.zorder, mg.area,mg.bbox) }

func (pt *pointGeometryImpl) Bbox() quadtree.Bbox { return quadtree.Bbox{pt.coord.Lon(),pt.coord.Lat(),pt.coord.Lon(),pt.coord.Lat()} }
func (ln *linestringGeometryImpl) Bbox() quadtree.Bbox { return *ln.bbox }
func (py *polygonGeometryImpl) Bbox() quadtree.Bbox { return *py.bbox }
func (mg *multiGeometryImpl) Bbox() quadtree.Bbox { return *mg.bbox }


func (pt *pointGeometryImpl) Coord() Coord { return pt.coord }

func (ln *linestringGeometryImpl) NumCoords() int { return len(ln.coords) }
func (ln *linestringGeometryImpl) Coord(i int) Coord { return ln.coords[i] }
func (ln *linestringGeometryImpl) ZOrder() int64 { return ln.zorder }

func (py *polygonGeometryImpl) NumRings() int { return len(py.coords) }
func (py *polygonGeometryImpl) NumCoords(i int) int { return len(py.coords[i]) }
func (py *polygonGeometryImpl) Coord(i,j int) Coord { return py.coords[i][j] }
func (py *polygonGeometryImpl) ZOrder() int64 { return py.zorder }
func (py *polygonGeometryImpl) Area() float64 { return py.area }

func (mg *multiGeometryImpl) NumGeometries() int { return len(mg.coords) }
func (mg *multiGeometryImpl) NumRings(i int) int { return len(mg.coords[i]) }
func (mg *multiGeometryImpl) NumCoords(i, j int) int { return len(mg.coords[i][i]) }
func (mg *multiGeometryImpl) Coord(i,j,k int) Coord { return mg.coords[i][j][i] }
func (mg *multiGeometryImpl) ZOrder() int64 { return mg.zorder }
func (mg *multiGeometryImpl) Area() float64 { return mg.area }

func (pt *pointGeometryImpl) String() string { return fmt.Sprintf("Point %8d %.20s", pt.id, pt.AsWkt(false)) }
func (ln *linestringGeometryImpl) String() string { return fmt.Sprintf("Linestring %8d %.20s", ln.id&0xffffffffffff, ln.AsWkt(false)) }
func (py *polygonGeometryImpl) String() string { return fmt.Sprintf("Polygon %8d %.20s", py.id&0xffffffffffff, py.AsWkt(false)) }
func (mg *multiGeometryImpl) String() string { return fmt.Sprintf("MultiGeometry %8d %.20s", mg.id&0xffffffffffff, mg.AsWkt(false)) }

func coordGeo(coord Coord, asMerc bool) []float64 {
    
    x,y := coord.LonLat()
    if asMerc {
        x,y = coord.XY()
    }
    return []float64{x,y}
}
func coordSliceGeom(cs []Coord, asMerc bool) [][]float64 {
    r:=make([][]float64,len(cs))
    for i,c:=range cs {
        r[i] = coordGeo(c,asMerc)
    }
    return r
}


func (pt *pointGeometryImpl) AsGeoJson(asMerc bool) interface{} {
    
    return map[string]interface{}{"type":"Point","coordinates":coordGeo(pt.coord, asMerc)}
}
func (ln *linestringGeometryImpl) AsGeoJson(asMerc bool) interface{} {
    
    return map[string]interface{}{"type":"LineString","coordinates":coordSliceGeom(ln.coords,asMerc)}
}

func (py *polygonGeometryImpl) AsGeoJson(asMerc bool) interface{} {
    
    cc := make([]interface{}, len(py.coords))
    for i,r:=range py.coords {
        cc[i] = coordSliceGeom(r, asMerc)
    }
    
    return map[string]interface{}{"type":"Polygon","coordinates":cc}
}

func (mg *multiGeometryImpl) AsGeoJson(asMerc bool) interface{} {
    
    mm := make([]interface{}, len(mg.coords))
    
    for i,rr:=range mg.coords {
        cc := make([]interface{}, len(rr))
        for j,r:=range rr {
            cc[j] = coordSliceGeom(r, asMerc)
        }
        
        ss := map[string]interface{}{"type":"Polygon","coordinates":cc}
        mm[i]=ss
    }
    
    return map[string]interface{}{"type":"GeometryCollection","geometries":mm}
}

func (pt *pointGeometryImpl)      IsValid() bool { return true }
func (ln *linestringGeometryImpl) IsValid() bool { return len(ln.coords)>=2 } 
func (py *polygonGeometryImpl)    IsValid() bool {
    for _,r := range py.coords {
        if len(r)<4 {
            return false
        }
    }
    return true
}

func (mg *multiGeometryImpl)      IsValid() bool {
    for _, g:=range mg.coords {
        for _,r := range g {
            if len(r)<4 {
                return false
            
            }
        }
    }
    return true
}
 
