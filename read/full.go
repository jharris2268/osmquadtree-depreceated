// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

// Package read provides functions to read pbf format data. 
package read

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/quadtree"
    
    //"time"
)

type readObjsFull struct {
    Info   bool
    Tags   bool
    //Change  bool
}

func (rof readObjsFull) addType(e elements.ElementType) bool {
    return true
}

// ReadExtendedBlock returns an ExtendedBlock (with extra block metadata)
// containing FullElements: e.g. elements with type Node can be
// converted to elements.FullNode, type Way to elements.FullWay, type
// Relation to elements.FullRelation and Geometry to elements.FullGeometry.
func ReadExtendedBlock(idx int, buf []byte, change bool) (elements.ExtendedBlock, error) {
    
    return readFull(idx, buf, readObjsFull{true,true}, change)
}

func readStrings(buf []byte, st []string) ([]string, error) {
    ii,err := utils.ReadPackedList(buf)
    if err!=nil { return nil, err}
    ans:=make([]string,len(ii))
    for i,id := range ii {
        ans[i] = st[id]
    }
    return ans,nil
}

func readStringsDelta(buf []byte, st []string) ([]string, error) {
    ii,err := utils.ReadDeltaPackedList(buf)
    if err!=nil { return nil, err}
    ans:=make([]string,len(ii))
    for i,id := range ii {
        ans[i] = st[id]
    }
    return ans,nil
}


func readInfo(buf []byte, st []string) (elements.Info, error) {
    vs,ts,cs,ui := int64(0),elements.Timestamp(0),elements.Ref(0),int64(0)
    vv := true
    
    us := ""
    pos,msg := utils.ReadPbfTag(buf,0)
    for ; msg.Tag>0; pos,msg=utils.ReadPbfTag(buf,pos) {
        switch msg.Tag { 
            case 1: vs = int64(msg.Value)
            case 2: ts = elements.Timestamp(msg.Value)
            case 3: cs = elements.Ref(msg.Value)
            case 4: ui = int64(msg.Value)
            case 5: us = st[msg.Value]
            case 6: vv = msg.Value!=0
        }
    }
    return elements.MakeInfo(vs,ts,cs,ui,us,vv), nil
}
            
            
    
    

func (r readObjsFull) readCommon(buf []byte, st []string) (elements.Ref, elements.Info,elements.Tags, quadtree.Quadtree, utils.PbfMsgSlice, error) {
    id := elements.Ref(0)
    var info elements.Info
    kk,vv := []string{}, []string{}
    qt := quadtree.Quadtree(0)
    rem := utils.PbfMsgSlice{}
    var err error
    
    pos,msg := utils.ReadPbfTag(buf,0)
    for ; msg.Tag>0; pos,msg=utils.ReadPbfTag(buf,pos) {
        switch msg.Tag {
            case 1: id=elements.Ref(msg.Value)
            case 2: if (r.Info) { kk,err=readStrings(msg.Data, st) }
            case 3: if (r.Info) { vv,err=readStrings(msg.Data, st) }
            case 4: if (r.Info) { info,err=readInfo(msg.Data, st)  }
            case 20: qt=quadtree.Quadtree(utils.UnZigzag(msg.Value))
            default:
                rem = append(rem, msg)
        }
        if err!=nil {
            return 0,nil,nil,0,nil,err
        }
    }
    var tags elements.Tags
    if r.Tags {
        tags = elements.MakeTags(kk,vv)
    }
    
    return id,info,tags,qt,rem,nil
}
    
    
    

func (r readObjsFull) node(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    id,info,tag,qt, rem, err := r.readCommon(buf,st)
    if err!=nil {
        return nil, err
    }
    
    ln,lt := int64(0),int64(0)
    for _,m := range rem {
        switch m.Tag {
            case 8: lt=utils.UnZigzag(m.Value)
            case 9: ln=utils.UnZigzag(m.Value)
        }
    }
    return elements.MakeNode(id, info, tag, ln, lt, qt, ct), nil
}

func (r readObjsFull) way(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    id,info,tag,qt, rem, err := r.readCommon(buf,st)
    if err!=nil {
        return nil, err
    }
    
    rfsi := []int64{}
    for _,m := range rem {
        switch m.Tag {
            case 8: rfsi,err = utils.ReadDeltaPackedList(m.Data)
        }
        if err!=nil { return nil,err }
    }
    
    rfs:=make([]elements.Ref,len(rfsi))
    for i,r:=range rfsi {
        rfs[i]=elements.Ref(r)
    }
    
    return elements.MakeWay(id, info, tag, rfs, qt, ct), nil
}

func (r readObjsFull) relation(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    id,info,tag,qt, rem, err := r.readCommon(buf,st)
    if err!=nil {
        return nil, err
    }
    
    tyy,rfsi,rl := []uint64{}, []int64{}, []string{}
    for _,m := range rem {
        switch m.Tag {
            case 8: rl,err=readStrings(m.Data, st)
            case 9: rfsi,err=utils.ReadDeltaPackedList(m.Data)
            case 10: tyy,err=utils.ReadPackedList(m.Data)
        }
        if err!=nil { return nil,err}
    }
    ty := make([]elements.ElementType, len(tyy))
    for i,t := range tyy {
        ty[i] = elements.ElementType(t)
    }
    rfs:=make([]elements.Ref,len(rfsi))
    for i,r:=range rfsi {
        rfs[i]=elements.Ref(r)
    }
    
    
    return elements.MakeRelation(id, info, tag, ty,rfs,rl,qt,ct), nil
}

func (r readObjsFull) geometry(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    
    id,info,tag,qt, rem, err := r.readCommon(buf,st)
    if err!=nil {
        return nil, err
    }
    dt := rem.Pack()
    
    return elements.MakeGeometry(id,info,tag,dt,qt,ct),nil
}

func readDenseInfo(buf []byte, st []string) ([]elements.Info, error) {
    vs, ts, cs, ui, us, vv := []uint64{},[]int64{},[]int64{},[]int64{},[]string{}, []uint64{}
    var err error
    pos,msg := utils.ReadPbfTag(buf,0)
    for ; msg.Tag>0; pos,msg = utils.ReadPbfTag(buf,pos) {
        switch msg.Tag {
            case 1: vs,err = utils.ReadPackedList(msg.Data)
            case 2: ts,err = utils.ReadDeltaPackedList(msg.Data)
            case 3: cs,err = utils.ReadDeltaPackedList(msg.Data)
            case 4: ui,err = utils.ReadDeltaPackedList(msg.Data)
            case 5: 
                if st!=nil {
                    us,err = readStringsDelta(msg.Data, st)
                }
            case 6: vv,err = utils.ReadPackedList(msg.Data)
        }
        if err!=nil {
            return nil, err
        }
    }
    res:=make([]elements.Info, len(vs))
    for i,v := range vs {
        
        u := ""
        if i < len(us) {
            u = us[i]
        }
        vis := true
        if i < len(vv) {
            vis=vv[i]!=0
        }
        res[i] = elements.MakeInfo(int64(v), elements.Timestamp(ts[i]), elements.Ref(cs[i]), ui[i], u,vis)
    }
    return res,nil
}
        

func (r readObjsFull) dense(buf []byte, st []string, objs elements.ByElementId, ct elements.ChangeType) (elements.ByElementId, error) {
    
    ids,lns,lts,kv,qts := []int64{},[]int64{},[]int64{},[]uint64{},[]int64{}
    infos := []elements.Info{}
    
    var err error
    pos,msg := utils.ReadPbfTag(buf,0)
    for ; msg.Tag>0; pos,msg = utils.ReadPbfTag(buf,pos) {
        switch msg.Tag {
            case 1: ids,err = utils.ReadDeltaPackedList(msg.Data)
            case 5: if (r.Info) { infos,err = readDenseInfo(msg.Data, st) }
            case 8: lts,err = utils.ReadDeltaPackedList(msg.Data)
            case 9: lns,err = utils.ReadDeltaPackedList(msg.Data)
            case 10: 
                if r.Tags && (st != nil) {
                    kv, err = utils.ReadPackedList(msg.Data)
                }
            case 20: qts,err = utils.ReadDeltaPackedList(msg.Data)
        }
        if err!=nil {
            return nil,err
        }
    }
    kvp := 0
    for i,id := range ids {
        kk,vv := []string{},[]string{}
        for kvp < len(kv) && kv[kvp] != 0 {
            kk = append(kk, st[kv[kvp]])
            vv = append(vv, st[kv[kvp+1]])
            kvp+=2
        }
        kvp++
        
        qt:=quadtree.Null
        if i < len(qts) {
            qt=quadtree.Quadtree(qts[i])
        }
        var inf elements.Info
        if i < len(infos) {
            inf = infos[i]
        }
        
        objs = append(objs, elements.MakeNode(elements.Ref(id), inf, elements.MakeTags(kk,vv), lns[i], lts[i], qt, ct))
    }

    return objs,nil
}
    
    
