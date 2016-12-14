// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/tikv"
)

var (
	dataCnt   = flag.Int("N", 1000000, "data num")
	workerCnt = flag.Int("C", 100, "concurrent num")
	pdAddr    = flag.String("pd", "localhost:2379", "pd address:localhost:2379")
	prefix    = flag.String("prefix", "key", "bench key prefix")
	valueSize = flag.Int("V", 5, "value size in byte")
)

func batchRawPut(value []byte) {
	cli, err := tikv.NewRawKVClient(strings.Split(*pdAddr, ","))
	if err != nil {
		log.Fatal(err)
	}

	progress := make(chan int)

	wg := sync.WaitGroup{}
	base := *dataCnt / *workerCnt
	wg.Add(*workerCnt)

	var cnt uint64 = 0
	for i := 0; i < *workerCnt; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < base; j++ {
				k := base*i + j
				key := fmt.Sprintf("%s_%d", *prefix, k)
				err = cli.Put([]byte(key), value)
				if err != nil {
					log.Fatal(errors.ErrorStack(err))
				}
				if j%100 == 0 {
					atomic.AddUint64(&cnt, 100)
				}
			}
		}(i)
	}

	// show speed
	go func() {
		tick := time.Tick(2 * time.Second)
		lastCnt := atomic.LoadUint64(&cnt)
		lastTime := time.Now()

		for range tick {
			cur := atomic.LoadUint64(&cnt)

			delta := float64(cur - lastCnt)
			elapse := float64(time.Since(lastTime)) / float64(time.Second)
			fmt.Printf("speed: %v\n", delta/float64(elapse))

			lastTime = time.Now()
			lastCnt = cur
		}
	}()

	wg.Wait()
	close(progress)
}

func main() {
	flag.Parse()
	log.SetLevelByString("warn")
	go http.ListenAndServe(":9191", nil)

	value := make([]byte, *valueSize)
	t := time.Now()
	batchRawPut(value)

	fmt.Printf("\nelapse:%v, total %v\n", time.Since(t), *dataCnt)
}
