// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
	"github.com/jharris2268/osmquadtree/elements"

	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
)

type TagTest struct {
	IsWay     bool   // keep tag if way
	IsNode    bool   // keep tag if
	IsPoly    string // yes, no, maybe
	Tag       string // tag key
	Type      string // text for normal tags, calc_?? for function
	IsFeature bool   // true if enough to make an object: e.g. highway would be true, name would be false
}

// ReadStyleFile reads a json file conisting of a list of TagTest objects,
// returning a map[string]TagTest.
func ReadStyleFile(fn string) (map[string]TagTest, error) {
	fl, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(fl)

	var res []TagTest
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}

	ans := map[string]TagTest{}
	for _, t := range res {
		ans[t.Tag] = t
	}
	return ans, nil
}

// TagsEditable extends elements.Tag to allow looking up, adding and deleting
// tag objects. It is expected that the Tags member of Geometry objects
// will also satisify this interface.
type TagsEditable interface {
	elements.Tags
	Has(string) bool        //Return true if tag is present
	Get(string) string      // Return tag value for given key
	Put(string, string)     // Insert tag with given key, value
	Delete(string)          // Remove tag with given key
	Add(tags elements.Tags) // Updates values for tags already present
	Clip()                  // Delete all
}

type tagsMap struct {
	tags map[string]string
	keys []string
}

func (tm *tagsMap) Len() int { return len(tm.tags) }
func (tm *tagsMap) Key(i int) string {
	if tm.keys == nil {
		tm.keys = make([]string, 0, len(tm.tags))
		for k, _ := range tm.tags {
			tm.keys = append(tm.keys, k)
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
	_, ok := tm.tags[k]
	return ok
}
func (tm *tagsMap) Get(k string) string {
	return tm.tags[k]
}

func (tm *tagsMap) Put(k string, v string) {
	if !tm.Has(k) {
		tm.keys = nil
	}
	tm.tags[k] = v
}

func (tm *tagsMap) Delete(k string) {
	if tm.Has(k) {
		tm.keys = nil
		delete(tm.tags, k)
	}

}

func (tm *tagsMap) Add(tt elements.Tags) {
	//tp:=map[string]bool{}
	for i := 0; i < tt.Len(); i++ {
		k := tt.Key(i)
		//tp[k]=true
		v := tt.Value(i)
		if v == "" {
			continue
		}
		if tm.Has(k) {

			switch tm.Get(k) {
			case "":
				continue
			case v:
				continue
			default:
				tm.Put(k, "")
			}
		} else {
			tm.Put(k, v)
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
	rm := make([]string, 0, tm.Len())
	for k, v := range tm.tags {
		if v == "" {
			rm = append(rm, k)
		}
	}
	for _, r := range rm {
		tm.Delete(r)
	}
}

// MakeTagsEditable converts the given tags into a TagsEditable. The default
// implementations is a map[string]string, with a slice giving the tag
// order (which is given by sort.Strings)
func MakeTagsEditable(tags elements.Tags) TagsEditable {
	tm := &tagsMap{map[string]string{}, nil}
	if tags == nil {
		return tm
	}
	for i := 0; i < tags.Len(); i++ {
		tm.Put(tags.Key(i), tags.Value(i))
	}
	return tm
}
