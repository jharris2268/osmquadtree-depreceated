package read


import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/quadtree"
    
    "fmt"
   
)

type nodeqt struct {
    ref, qt int64
}

type wayqt struct {
    ref, qt int64
}

type relqt struct {
    ref, qt int64
}

type geomqt struct {
    ref, qt int64
}

func (r *nodeqt) Type() elements.ElementType { return elements.Node }
func (r *nodeqt) Id() elements.Ref { return elements.Ref(r.ref) }
func (r *nodeqt) ChangeType() elements.ChangeType { return elements.Normal }
func (r *nodeqt) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),r.Quadtree(),nil,nil,nil) }
func (r *nodeqt) String() string { return fmt.Sprintf("Node refqt %d %-18s", r.ref,r.Quadtree()) }

func (r *nodeqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }


func (r *wayqt) Type() elements.ElementType { return elements.Way }
func (r *wayqt) Id() elements.Ref { return elements.Ref(r.ref) }
func (r *wayqt) ChangeType() elements.ChangeType { return elements.Normal }
func (r *wayqt) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),r.Quadtree(),nil,nil,nil) }
func (r *wayqt) String() string { return fmt.Sprintf("Way refqt %d %-18s", r.ref, r.Quadtree() ) }

func (r *wayqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }

func (r *relqt) Type() elements.ElementType { return elements.Relation }
func (r *relqt) Id() elements.Ref { return elements.Ref(r.ref) }
func (r *relqt) ChangeType() elements.ChangeType { return elements.Normal }
func (r *relqt) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),r.Quadtree(),nil,nil,nil) }
func (r *relqt) String() string { return fmt.Sprintf("Relation refqt %d %-18s", r.ref, r.Quadtree() ) }

func (r *relqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }

func (r *geomqt) Type() elements.ElementType { return elements.Geometry }
func (r *geomqt) Id() elements.Ref { return elements.Ref(r.ref) }
func (r *geomqt) ChangeType() elements.ChangeType { return elements.Normal }
func (r *geomqt) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),r.Quadtree(),nil,nil,nil) }
func (r *geomqt) String() string { return fmt.Sprintf("Geometry refqt %d %-18s", r.ref, r.Quadtree()) }

func (r *geomqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }

func MakeObjQt(ty elements.ElementType, ref elements.Ref, qt quadtree.Quadtree) elements.Element {
    switch ty {
        case elements.Node: return &nodeqt{int64(ref),int64(qt)}
        case elements.Way: return &wayqt{int64(ref),int64(qt)}
        case elements.Relation: return &relqt{int64(ref),int64(qt)}
        case elements.Geometry: return &geomqt{int64(ref),int64(qt)}
    }
    return nil
}





type readObjsRefqt struct {}
func (rorq readObjsRefqt) addType(e elements.ElementType) bool { return true }

func (readObjsRefqt) node(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData}
    b,ok := getV(buf,20)

    return &nodeqt{int64(a),utils.UnZigzag(b)},nil
}

func (readObjsRefqt) way(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData}
    b,ok := getV(buf,20)

    return &wayqt{int64(a),utils.UnZigzag(b)},nil
}
    
    
func (readObjsRefqt) relation(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData}
    b,ok := getV(buf,20)

    return &relqt{int64(a),utils.UnZigzag(b)},nil
}
    
    
func (readObjsRefqt) geometry(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData}
    b,ok := getV(buf,20)

    
    return &geomqt{int64(a),utils.UnZigzag(b)},nil
}

func (readObjsRefqt) dense(buf []byte, st []string, objs elements.ByElementId, ct elements.ChangeType) (elements.ByElementId, error) {
    var ii,qq []int64
    
    var err error
    pos,msg:=utils.ReadPbfTag(buf,0)
    for ; msg.Tag>0; pos,msg=utils.ReadPbfTag(buf,pos) {
        switch msg.Tag{
            case 1:
                ii,err = utils.ReadDeltaPackedList(msg.Data)
            case 20:
                qq,err = utils.ReadDeltaPackedList(msg.Data)
        } 
        
        if err!=nil {
            return nil,err
        }
        
    }
    for i,id := range ii {
        if i>=len(qq) {
            return nil,missingData
        }
        objs=append(objs, &nodeqt{id,qq[i]})
        
    }
    return objs, nil
}


func ReadQts(buf []byte) (elements.Block, error) {
    
    return readPlain(buf, readObjsRefqt{})
}

