package elements

import (
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/quadtree"
    "fmt"
)

func PackFullElement(fe FullElement, data []byte) []byte {
    var info,tags []byte
    if fe.Info()!=nil {
        info=fe.Info().Pack()
    }
    if fe.Tags()!=nil {
        tags=fe.Tags().Pack()
    }
    return PackElement(
        fe.Type(), fe.ChangeType(),
        fe.Id(), fe.Quadtree(),
        data, info, tags)
}
        


func PackElement(
    et ElementType, ct ChangeType,
    id Ref, qt quadtree.Quadtree,
    data []byte, info []byte, tags []byte) []byte {
    
    tl := 50 + len(data) + len(info) + len(tags)
    p:=0
    res := make([]byte, tl)
    res[0] = byte(et)
    res[1] = byte(ct)
    p = utils.WriteInt64(res, 2, int64(id))
    p = utils.WriteVarint(res, p, int64(qt))
    p = utils.WriteData(res, p, data)
    p = utils.WriteData(res, p, info)
    p = utils.WriteData(res, p, tags)
    
    return res[:p]
}
 
 
type PackedElement []byte

func (po PackedElement) Type() ElementType {
    return ElementType(po[0])
}

func (po PackedElement) ChangeType() ChangeType {
    return ChangeType(po[1])
}

func (po PackedElement) Id() Ref {
    r,_ := utils.ReadInt64([]byte(po),2)
    return Ref(r)
}

func (po PackedElement) Pack() []byte {
    return []byte(po)
}

func (po PackedElement) String() string {
    return fmt.Sprintf("Packed %d %d %10d [%d bytes]",po.ChangeType(),po.Type(),po.Id(),len(po))
}

func packInfo(vs int64, ts Timestamp, cs Ref, ui int64, user string) []byte {
	l := 50 + len(user)
	res := make([]byte, l)
	p := utils.WriteVarint(res, 0, vs)
	p = utils.WriteVarint(res, p, int64(ts))
	p = utils.WriteVarint(res, p, int64(cs))
	p = utils.WriteVarint(res, p, ui)
	p = utils.WriteData(res, p, []byte(user))

	return res[:p]
}

func unpackInfo(buf []byte) (int64,Timestamp,Ref,int64,string) {
    vs,ts,cs,ui := int64(0),int64(0),int64(0),int64(0)
    us := []byte{}
    p:=0
    vs,p=utils.ReadVarint(buf,p)
    ts,p=utils.ReadVarint(buf,p)
    cs,p=utils.ReadVarint(buf,p)
    ui,p=utils.ReadVarint(buf,p)
    us,p=utils.ReadData(buf,p)
    return vs,Timestamp(ts),Ref(cs),ui,string(us)
}
    

func unpackTags(buf []byte) ([]string, []string) {
	l, p := utils.ReadVarint(buf, 0)
	keys:=make([]string, l)
    vals:=make([]string, l)
	for i, _ := range keys {
        s:=[]byte{}
        s, p = utils.ReadData(buf, p)
		keys[i] = string(s)
        s, p = utils.ReadData(buf, p)
		vals[i] = string(s)
	}

	return keys, vals
}

func packTags(keys, vals []string) []byte {
	tl := 10
    for i,k:=range keys {
        tl += 10 + len([]byte(k)) + len([]byte(vals[i]))
    }
    
	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(len(keys)))
	for i, k := range keys {
		p = utils.WriteData(res, p, []byte(k))
		p = utils.WriteData(res, p, []byte(vals[i]))
	}
	return res[:p]
}
func PackTags(tags Tags) []byte {
    tl := 10
    for i := 0; i < tags.Len(); i++ {
        tl += 10 + len([]byte(tags.Key(i))) + len([]byte(tags.Value(i)))
    }
    
	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(tags.Len()))
	for i := 0; i < tags.Len(); i++ {
		p = utils.WriteData(res, p, []byte(tags.Key(i)))
		p = utils.WriteData(res, p, []byte(tags.Value(i)))
	}
	return res[:p]
}

func PackLonlat(ln int64, lt int64) []byte {
	res := make([]byte, 8)
	res[0] = byte((ln >> 24) & 255)
	res[1] = byte((ln >> 16) & 255)
	res[2] = byte((ln >> 8) & 255)
	res[3] = byte((ln) & 255)
	res[4] = byte((lt >> 24) & 255)
	res[5] = byte((lt >> 16) & 255)
	res[6] = byte((lt >> 8) & 255)
	res[7] = byte((lt) & 255)
	return res

}

func unpackLonlat(buf []byte) (int64,int64) {
	a := int64(buf[0]) << 24
	a |= int64(buf[1]) << 16
	a |= int64(buf[2]) << 8
	a |= int64(buf[3])
	if a > 2147483648 {
		a -= (int64(1) << 32)
	}

	b := int64(buf[4]) << 24
	b |= int64(buf[5]) << 16
	b |= int64(buf[6]) << 8
	b |= int64(buf[7])
	if b > 2147483648 {
		b -= (int64(1) << 32)
	}
	return a,b
}

func PackRefs(nn Refs) []byte {
    res := make([]byte, 10*(1+nn.Len()))
    p := utils.WriteVarint(res, 0, int64(nn.Len()))
	s := Ref(0)
	for i:=0; i < nn.Len(); i++ {
		p = utils.WriteVarint(res, p, int64(nn.Ref(i)-s))
		s = nn.Ref(i)
	}

	return res[:p]
}

func packRefs(nn []Ref) []byte {
	res := make([]byte, 10*(1+len(nn)))
	p := utils.WriteVarint(res, 0, int64(len(nn)))
	s := int64(0)
	for _, n := range nn {
		p = utils.WriteVarint(res, p, int64(n)-s)
		s = int64(n)
	}

	return res[:p]
}

func unpackRefs(buf []byte) []Ref {

	l, p := utils.ReadVarint(buf, 0)
	if l < 0 || l == 0 && len(buf) > p {
		return nil
	}
	ans := make([]Ref, l)
	n, np := int64(0), int64(0)

	for i := 0; i < int(l); i++ {
		np, p = utils.ReadVarint(buf, p)
		n += np
		ans[i] = Ref(n)
	}

	return ans

}

func PackMembers(mm Members) []byte {
    tl := 10 + 15*mm.Len()
	for i:=0; i < mm.Len(); i++ {
		tl += len(mm.Role(i))
	}
	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(mm.Len()))
	s := Ref(0)
	for i:=0; i < mm.Len(); i++ {
		p = utils.WriteVarint(res, p, int64(mm.MemberType(i)))
		//println("p",r.ref,r.ref-s)
		p = utils.WriteVarint(res, p, int64(mm.Ref(i)-s))
		s = mm.Ref(i)
        p = utils.WriteData(res, p, []byte(mm.Role(i)))
        
	}
	return res[:p]
}

func packMembers(rms []relMember) []byte {
	tl := 10 + 15*len(rms)
	for _, r := range rms {
		tl += len(r.role)
	}
	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(len(rms)))
	s := int64(0)
	for _, r := range rms {
		p = utils.WriteVarint(res, p, int64(r.memType))
		//println("p",r.ref,r.ref-s)
		p = utils.WriteVarint(res, p, int64(r.ref)-s)
		s = int64(r.ref)
		p = utils.WriteData(res, p, []byte(r.role))
	}
	return res[:p]
}

func unpackMembers(buf []byte) []relMember {
    l,p := utils.ReadVarint(buf,0)
    
    t:=int64(0)
    s:=int64(0)
    ss:=int64(0)
    var rl []byte
    
    ans:=make([]relMember, l)
    for i,_ := range ans {
        t,p = utils.ReadVarint(buf,p)
        ans[i].memType = ElementType(t)
        
        ss,p = utils.ReadVarint(buf,p)
        s+=ss
        ans[i].ref= Ref(s)
        
        rl,p = utils.ReadData(buf,p)
        ans[i].role = string(rl)
    }
    return ans
}


func UnpackElement(buf []byte) FullElement {
    et := ElementType(buf[0])
    ct := ChangeType(buf[1])
    idi,p := utils.ReadInt64(buf,2)
    id := Ref(idi)
    
    qti,p := utils.ReadVarint(buf,p)
    qt := quadtree.Quadtree(qti)
    
    dt,p := utils.ReadData(buf,p)
    in,p := utils.ReadData(buf,p)
    tg,p := utils.ReadData(buf,p)
    
    if p!=len(buf) {
        panic("not at end")
    }
    
    var info Info
    if in != nil {
        a,b,c,d,e := unpackInfo(in)
        info = &unpackedInfo{a,b,c,d,e}
    }
    var tags Tags 
    if tg != nil {
        kk,vv := unpackTags(tg)
        tags = &unpackedTags{kk,vv}
    }
    
    switch et {
        case Node:
            
            ln,lt := int64(0),int64(0)
            if dt!=nil {
                ln,lt = unpackLonlat(dt)
            }
            return &fullNode{id,ct,qt,ln,lt,info,tags}
        case Way:
            
            refs:=[]Ref{}
            if dt!=nil {
                refs = unpackRefs(dt)
            }
            return &fullWay{id,ct,qt,refs,info,tags}
        case Relation:
            mems:=[]relMember{}
            if dt !=nil {
                mems = unpackMembers(dt)
            }
            return &fullRelation{id,ct,qt,mems,info,tags}
        case Geometry:
            return &packedGeometry{id,ct,qt,dt,info,tags}
    }
    
    panic("unknown element type")
}

type refSlice []Ref
func (rf refSlice) Len() int { return len(rf) }
func (rf refSlice) Ref(i int) Ref { return rf[i] }

func UnpackQtRefs(buf []byte) (ElementType, ChangeType, Ref, quadtree.Quadtree, Refs) {
    et := ElementType(buf[0])
    ct := ChangeType(buf[1])
    idi,p := utils.ReadInt64(buf,2)
    id := Ref(idi)
    
    qti,p := utils.ReadVarint(buf,p)
    qt := quadtree.Quadtree(qti)
    
    dt,p := utils.ReadData(buf,p)
    refs:=refSlice{}
    if dt!=nil {
        switch et {
            case Node:
                ln,lt := unpackLonlat(dt)
                refs=refSlice{Ref(ln),Ref(lt)}
        
            case Way:
                
                refs = unpackRefs(dt)
                
            case Relation:
                mems:=unpackMembers(dt)
                refs=make(refSlice,len(mems))
                for i,m:=range mems {
                    refs[i] = (Ref(m.memType)<<59) | m.ref
                }
        }
    }
    return et,ct,id,qt,refs
}
