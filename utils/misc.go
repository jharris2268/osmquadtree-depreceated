// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

// +build !windows

package utils

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
    "sort"
    "path/filepath"
    
    "runtime"
    "runtime/pprof"
    "os/signal"
    "syscall"
)

func WriteMemoryProfile() error {
    tempdir:=os.Getenv("GOPATH")
    f,err := ioutil.TempFile(tempdir, "osmquadtree.utils.memprofile")
    if err != nil {
        return err
    }
    pprof.WriteHeapProfile(f)
    p,_:=f.Seek(0,2)
    f.Close()
    fmt.Printf("Memprofile: %d bytes to %s [%s]\n", p, f.Name(),MemstatsStr())
    return nil
}    


func MemstatsStr() string {
	p := os.Getpid()
	statm, e := os.Open(fmt.Sprintf("/proc/%d/statm", p))
	defer statm.Close()
	if e != nil {
		return e.Error()
	}
	statms, _ := ioutil.ReadAll(statm)
	statmss := strings.Fields(string(statms))
	if len(statmss) < 2 {
		return string(statms)
	}
	m0, _ := strconv.ParseFloat(statmss[0], 64)
	m1, _ := strconv.ParseFloat(statmss[1], 64)

	return fmt.Sprintf("%8.1fmb // %8.1fmb", m0*4.0/1024.0, m1*4.0/1024.0)
}

func GetFileSize(fn string) (int64, error) {
	fl, err := os.Open(fn)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Open [%s]: %s", fn, err.Error()))
	}

	fi, err := fl.Stat()
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Stat [%s]: %s", fn, err.Error()))
	}

	sz := fi.Size()
	fl.Close()
	return sz, nil
}

func FileExists(fn string) (bool, error) {
	_, err := os.Stat(fn)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GetFileNamesWithExtension(dir string, ext string) ([]string, error) {
    
    ok,err := FileExists(dir)
    if !ok {
        return nil, errors.New("No such file")
    }
    
	if strings.HasSuffix(dir, ext) {
        return []string{dir}, nil
	}

	dc, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fn := make([]string, 0, len(dc))
	for _, f := range dc {
		
		if strings.HasSuffix(f.Name(), ext) {
			fn = append(fn, filepath.Join(dir,f.Name()))
		}
	}
	sort.Sort(sort.StringSlice(fn))
	if len(fn) == 0 {
		return nil, errors.New("no " + ext +" files in directory")
	}
	return fn, nil
}

func OnTerm() {
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt)
    signal.Notify(c, syscall.SIGTERM)
    go func() {
        z:=<-c
        fmt.Println("TERM", z)
        
        buf := make([]byte, 1<<16)
        sl:=runtime.Stack(buf, true)
        fmt.Println(string(buf[:sl]))
        
        WriteMemoryProfile()
        
        os.Exit(1)
    }()
}
