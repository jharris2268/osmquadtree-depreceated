package geometry

import (
    "github.com/jharris2268/osmquadtree/elements"
    
    "encoding/json"
    "os"
    "io/ioutil"
    "sort"
    
)

type TagTest struct {
	IsWay  bool
	IsNode bool
	IsPoly string
	Tag    string
	Type   string
    IsFeature bool
}

func ReadStyleFile(fn string) (map[string]TagTest,error) {
    fl,err := os.Open(fn)
    if err!=nil { return nil,err }
    
    data, err := ioutil.ReadAll(fl)
    
    var res []TagTest
    err = json.Unmarshal(data, &res)
    if err!=nil { return nil,err }
    
    ans:=map[string]TagTest{}
    for _,t:=range res {
        ans[t.Tag] = t
    }
    return ans,nil
}


    

type TagsEditable interface {
    elements.Tags
    Has(string) bool
    Get(string) string
    Put(string,string)
    Delete(string)
    Add(elements.Tags)
    Clip()
}

type tagsMap struct {
    tags  map[string]string
    keys  []string
}

func (tm *tagsMap) Len() int { return len(tm.tags) }
func (tm *tagsMap) Key(i int) string {
    if tm.keys==nil {
        tm.keys = make([]string,0,len(tm.tags))
        for k,_ := range tm.tags {
            tm.keys=append(tm.keys,k)
        }
        sort.Strings(tm.keys)
    }
    return tm.keys[i]
}

func (tm *tagsMap) Value(i int) string {
    return tm.tags[tm.Key(i)]
}

func (tm *tagsMap) Pack() []byte {
    return elements.PackTags(tm)
}

func (tm *tagsMap) Has(k string) bool {
    _,ok :=tm.tags[k]
    return ok
}
func (tm *tagsMap) Get(k string) string {
    return tm.tags[k]
}

func (tm *tagsMap) Put(k string, v string) {
    if !tm.Has(k) {
        tm.keys=nil
    }
    tm.tags[k]=v
}

func (tm *tagsMap) Delete(k string) {
    if tm.Has(k) {
        tm.keys=nil
        delete(tm.tags,k)
    }
    
}

func (tm *tagsMap) Add(tt elements.Tags) {
    //tp:=map[string]bool{}
    for i:=0; i < tt.Len(); i++ {
        k := tt.Key(i)
        //tp[k]=true
        v := tt.Value(i)
        if v=="" {
            continue
        }
        if tm.Has(k) {
            
            switch tm.Get(k) {
                case "": continue
                case v: continue
                default:
                    tm.Put(k,"")
            }
        } else {
            tm.Put(k,v)
        }
    }
    
    /*for k,_ := range tm.tags {
        if _,ok := tp[k]; !ok {
            tp[k] = false
        }
    }
    for k,v := range tp {
        if v==false {
            tm.Put(k,"")
        }
    }*/
}

func (tm *tagsMap) Clip() {
    rm:=make([]string,0,tm.Len())
    for k,v := range tm.tags {
        if v=="" {
            rm = append(rm,k)
        }
    }
    for _,r:= range rm {
        tm.Delete(r)
    }
}
        

func MakeTagsEditable(tt elements.Tags) TagsEditable {
    tm := &tagsMap{map[string]string{},nil}
    if tt==nil {
        return tm
    }
    for i:=0; i < tt.Len(); i++ {
        tm.Put(tt.Key(i),tt.Value(i))
    }
    return tm
}



    
