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

package tikv

import (
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"golang.org/x/net/context"
)

var (
	MaxRetryCount = 5
)

// onePhaseCommitter executes a one-phase commit protocol.
// Just used when import data.
type onePhaseCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	ts   uint64
	keys      [][]byte
	mutations map[string]*pb.Mutation
	mu        struct {
		sync.RWMutex
		writtenKeys [][]byte
		committed   bool
	}
}

// newOnePhaseCommitter creates a OnePhaseCommitter.
func newOnePhaseCommitter(txn *tikvTxn) (*onePhaseCommitter, error) {
	var keys [][]byte
	var size int
	mutations := make(map[string]*pb.Mutation)
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		if len(v) > 0 {
			mutations[string(k)] = &pb.Mutation{
				Op:    pb.Op_Put,
				Key:   k,
				Value: v,
			}
		} else {
			mutations[string(k)] = &pb.Mutation{
				Op:  pb.Op_Del,
				Key: k,
			}
		}
		keys = append(keys, k)
		size += len(k) + len(v)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Transactions without Put/Del, only Locks are readonly.
	// We can skip commit directly.
	if len(keys) == 0 {
		return nil, nil
	}
	for _, lockKey := range txn.lockKeys {
		if _, ok := mutations[string(lockKey)]; !ok {
			mutations[string(lockKey)] = &pb.Mutation{
				Op:  pb.Op_Lock,
				Key: lockKey,
			}
			keys = append(keys, lockKey)
			size += len(lockKey)
		}
	}
	txnWriteKVCountHistogram.Observe(float64(len(keys)))
	txnWriteSizeHistogram.Observe(float64(size / 1024))

	return &onePhaseCommitter{
		store:     txn.store,
		txn:       txn,
		ts:   txn.StartTS(),
		keys:      keys,
		mutations: mutations,
	}, nil
}

func (c *onePhaseCommitter) writeKeys(bo *Backoffer, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	groups, _, err := c.store.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return errors.Trace(err)
	}

	var batches []batchKeys
	var sizeFunc = c.keySize
	sizeFunc = c.keyValueSize
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	err = c.doActionOnBatches(bo, batches)
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
func (c *onePhaseCommitter) doActionOnBatches(bo *Backoffer, batches []batchKeys) error {
	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		e := c.writeSingleBatch(bo, batches[0])
		if e != nil {
			log.Warnf("1PC doActionOnBatches failed: %v, tid: %d", e, c.ts)
		}
		return errors.Trace(e)
	}

	// Stop sending other requests after receiving first error.
	var cancel context.CancelFunc
	cancel = bo.WithCancel()

	// Concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		go func(batch batchKeys) {
			ch <- c.writeSingleBatch(bo.Fork(), batch)
		}(batch)
	}
	var err error
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			log.Warnf("1PC doActionOnBatches failed: %v, tid: %d", e, c.ts)
			if cancel != nil {
				// Cancel other requests and return the first error.
				cancel()
				return errors.Trace(e)
			}
			err = e
		}
	}
	return errors.Trace(err)
}

func (c *onePhaseCommitter) keyValueSize(key []byte) int {
	size := len(key)
	if mutation := c.mutations[string(key)]; mutation != nil {
		size += len(mutation.Value)
	}
	return size
}

func (c *onePhaseCommitter) keySize(key []byte) int {
	return len(key)
}

func (c *onePhaseCommitter) writeSingleBatch(bo *Backoffer, batch batchKeys) error {
	mutations := make([]*pb.Mutation, len(batch.keys))
	for i, k := range batch.keys {
		mutations[i] = c.mutations[string(k)]
	}
	req := &pb.Request{
		Type: pb.MessageType_CmdImportData,
		CmdImportDataReq: &pb.CmdImportDataRequest{
			Mutations:    mutations,
			Version: c.ts,
		},
	}

	retryCount := 0
	for {
		resp, err := c.store.SendKVReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.writeKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		importResp := resp.GetCmdImportDataResp()
		if importResp == nil {
			return errors.Trace(errBodyMissing)
		}
		keyErrs := importResp.GetErrors()
		if len(keyErrs) == 0 {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.mu.writtenKeys = append(c.mu.writtenKeys, batch.keys...)
			return nil
		}
		if retryCount < MaxRetryCount {
			retryCount++
			continue
		}
		return errors.Errorf("write single batch failed after %d retries.", retryCount)
	}
}

// execute executes the two-phase commit protocol.
func (c *onePhaseCommitter) execute() error {
	ctx := context.Background()

	err := c.writeKeys(NewBackoffer(importDataMaxBackoff, ctx), c.keys)
	if err != nil {
		log.Warnf("1PC failed on write: %v, tid: %d", err, c.ts)
		return errors.Trace(err)
	}

	return nil
}
