package utils

import (
	"reflect"
)

type Idxer interface {
	Idx() int
}

func SortIdxerChan(inc <-chan Idxer) <-chan Idxer {
	res := make(chan Idxer)
	go func() {
		tt := map[int]Idxer{}
		nv := 0
		for b := range inc {
			if b.Idx() == nv {
				res <- b

				nv++
				s, ok := tt[nv]
				for ok {
					res <- s
					delete(tt, nv)
					nv++
					s, ok = tt[nv]
				}
			} else if b.Idx() < nv {
                println("??WTF??",b.Idx(), nv)
                res <- b
            } else {
				tt[b.Idx()] = b
			}
			if len(tt) > 2000 {
				println("len(tt)=", len(tt), ", nv=", nv)
			}
		}
		if len(tt) > 0 {
			println("at end, have", len(tt), "blocks remaining...")
			for _, s := range tt {
				println(s.Idx(), reflect.TypeOf(s))
				res <- s

			}
		}
		close(res)
	}()
	return res
}
