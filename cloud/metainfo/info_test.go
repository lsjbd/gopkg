// Copyright 2021 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metainfo_test

import (
	"context"
	"testing"

	"github.com/bytedance/gopkg/cloud/metainfo"
)

func TestWithValue(t *testing.T) {
	ctx := context.Background()

	k, v := "Key", "Value"
	ctx = metainfo.WithValue(ctx, k, v)
	assert(t, ctx != nil)

	x, ok := metainfo.GetValue(ctx, k)
	assert(t, ok)
	assert(t, x == v)
}

func TestWithEmpty(t *testing.T) {
	ctx := context.Background()

	k, v := "Key", "Value"
	ctx = metainfo.WithValue(ctx, k, "")
	assert(t, ctx != nil)

	_, ok := metainfo.GetValue(ctx, k)
	assert(t, !ok)

	ctx = metainfo.WithValue(ctx, "", v)
	assert(t, ctx != nil)

	_, ok = metainfo.GetValue(ctx, "")
	assert(t, !ok)
}

func TestDelValue(t *testing.T) {
	ctx := context.Background()

	k, v := "Key", "Value"
	ctx = metainfo.WithValue(ctx, k, v)
	assert(t, ctx != nil)

	x, ok := metainfo.GetValue(ctx, k)
	assert(t, ok)
	assert(t, x == v)

	ctx = metainfo.DelValue(ctx, k)
	assert(t, ctx != nil)

	x, ok = metainfo.GetValue(ctx, k)
	assert(t, !ok)

	assert(t, metainfo.DelValue(ctx, "") == ctx)
}

func TestGetAll(t *testing.T) {
	ctx := context.Background()

	ss := []string{"1", "2", "3"}
	for _, k := range ss {
		ctx = metainfo.WithValue(ctx, "key"+k, "val"+k)
	}

	m := metainfo.GetAllValues(ctx)
	assert(t, m != nil)
	assert(t, len(m) == len(ss))

	for _, k := range ss {
		assert(t, m["key"+k] == "val"+k)
	}
}

func TestRangeValues(t *testing.T) {
	ctx := context.Background()

	ss := []string{"1", "2", "3"}
	for _, k := range ss {
		ctx = metainfo.WithValue(ctx, "key"+k, "val"+k)
	}

	m := make(map[string]string, 3)
	f := func(k, v string) bool {
		m[k] = v
		return true
	}

	metainfo.RangeValues(ctx, f)
	assert(t, m != nil)
	assert(t, len(m) == len(ss))

	for _, k := range ss {
		assert(t, m["key"+k] == "val"+k)
	}
}

func TestGetAll2(t *testing.T) {
	ctx := context.Background()

	ss := []string{"1", "2", "3"}
	for _, k := range ss {
		ctx = metainfo.WithValue(ctx, "key"+k, "val"+k)
	}

	ctx = metainfo.DelValue(ctx, "key2")

	m := metainfo.GetAllValues(ctx)
	assert(t, m != nil)
	assert(t, len(m) == len(ss)-1)

	for _, k := range ss {
		if k == "2" {
			_, exist := m["key"+k]
			assert(t, !exist)
		} else {
			assert(t, m["key"+k] == "val"+k)
		}
	}
}

///////////////////////////////////////////////

func TestWithPersistentValue(t *testing.T) {
	ctx := context.Background()

	k, v := "Key", "Value"
	ctx = metainfo.WithPersistentValue(ctx, k, v)
	assert(t, ctx != nil)

	x, ok := metainfo.GetPersistentValue(ctx, k)
	assert(t, ok)
	assert(t, x == v)
}

func TestWithPersistentEmpty(t *testing.T) {
	ctx := context.Background()

	k, v := "Key", "Value"
	ctx = metainfo.WithPersistentValue(ctx, k, "")
	assert(t, ctx != nil)

	_, ok := metainfo.GetPersistentValue(ctx, k)
	assert(t, !ok)

	ctx = metainfo.WithPersistentValue(ctx, "", v)
	assert(t, ctx != nil)

	_, ok = metainfo.GetPersistentValue(ctx, "")
	assert(t, !ok)
}

func TestDelPersistentValue(t *testing.T) {
	ctx := context.Background()

	k, v := "Key", "Value"
	ctx = metainfo.WithPersistentValue(ctx, k, v)
	assert(t, ctx != nil)

	x, ok := metainfo.GetPersistentValue(ctx, k)
	assert(t, ok)
	assert(t, x == v)

	ctx = metainfo.DelPersistentValue(ctx, k)
	assert(t, ctx != nil)

	x, ok = metainfo.GetPersistentValue(ctx, k)
	assert(t, !ok)

	assert(t, metainfo.DelPersistentValue(ctx, "") == ctx)
}

func TestGetAllPersistent(t *testing.T) {
	ctx := context.Background()

	ss := []string{"1", "2", "3"}
	for _, k := range ss {
		ctx = metainfo.WithPersistentValue(ctx, "key"+k, "val"+k)
	}

	m := metainfo.GetAllPersistentValues(ctx)
	assert(t, m != nil)
	assert(t, len(m) == len(ss))

	for _, k := range ss {
		assert(t, m["key"+k] == "val"+k)
	}
}

func TestRangePersistent(t *testing.T) {
	ctx := context.Background()

	ss := []string{"1", "2", "3"}
	for _, k := range ss {
		ctx = metainfo.WithPersistentValue(ctx, "key"+k, "val"+k)
	}

	m := make(map[string]string, 3)
	f := func(k, v string) bool {
		m[k] = v
		return true
	}

	metainfo.RangePersistentValues(ctx, f)
	assert(t, m != nil)
	assert(t, len(m) == len(ss))

	for _, k := range ss {
		assert(t, m["key"+k] == "val"+k)
	}
}

func TestGetAllPersistent2(t *testing.T) {
	ctx := context.Background()

	ss := []string{"1", "2", "3"}
	for _, k := range ss {
		ctx = metainfo.WithPersistentValue(ctx, "key"+k, "val"+k)
	}

	ctx = metainfo.DelPersistentValue(ctx, "key2")

	m := metainfo.GetAllPersistentValues(ctx)
	assert(t, m != nil)
	assert(t, len(m) == len(ss)-1)

	for _, k := range ss {
		if k == "2" {
			_, exist := m["key"+k]
			assert(t, !exist)
		} else {
			assert(t, m["key"+k] == "val"+k)
		}
	}
}

///////////////////////////////////////////////

func TestNilSafty(t *testing.T) {
	assert(t, metainfo.TransferForward(nil) == nil)

	_, tOK := metainfo.GetValue(nil, "any")
	assert(t, !tOK)
	assert(t, metainfo.GetAllValues(nil) == nil)
	assert(t, metainfo.WithValue(nil, "any", "any") == nil)
	assert(t, metainfo.DelValue(nil, "any") == nil)

	_, pOK := metainfo.GetPersistentValue(nil, "any")
	assert(t, !pOK)
	assert(t, metainfo.GetAllPersistentValues(nil) == nil)
	assert(t, metainfo.WithPersistentValue(nil, "any", "any") == nil)
	assert(t, metainfo.DelPersistentValue(nil, "any") == nil)
}

func TestTransitAndPersistent(t *testing.T) {
	ctx := context.Background()

	ctx = metainfo.WithValue(ctx, "A", "a")
	ctx = metainfo.WithPersistentValue(ctx, "A", "b")

	x, xOK := metainfo.GetValue(ctx, "A")
	y, yOK := metainfo.GetPersistentValue(ctx, "A")

	assert(t, xOK)
	assert(t, yOK)
	assert(t, x == "a")
	assert(t, y == "b")

	_, uOK := metainfo.GetValue(ctx, "B")
	_, vOK := metainfo.GetPersistentValue(ctx, "B")

	assert(t, !uOK)
	assert(t, !vOK)

	ctx = metainfo.DelValue(ctx, "A")
	_, pOK := metainfo.GetValue(ctx, "A")
	q, qOK := metainfo.GetPersistentValue(ctx, "A")
	assert(t, !pOK)
	assert(t, qOK)
	assert(t, q == "b")
}

func TestTransferForward(t *testing.T) {
	ctx := context.Background()

	ctx = metainfo.WithValue(ctx, "A", "t")
	ctx = metainfo.WithPersistentValue(ctx, "A", "p")
	ctx = metainfo.WithValue(ctx, "A", "ta")
	ctx = metainfo.WithPersistentValue(ctx, "A", "pa")

	ctx = metainfo.TransferForward(ctx)
	assert(t, ctx != nil)

	x, xOK := metainfo.GetValue(ctx, "A")
	y, yOK := metainfo.GetPersistentValue(ctx, "A")

	assert(t, xOK)
	assert(t, yOK)
	assert(t, x == "ta")
	assert(t, y == "pa")

	ctx = metainfo.TransferForward(ctx)
	assert(t, ctx != nil)

	x, xOK = metainfo.GetValue(ctx, "A")
	y, yOK = metainfo.GetPersistentValue(ctx, "A")

	assert(t, !xOK)
	assert(t, yOK)
	assert(t, y == "pa")

	ctx = metainfo.WithValue(ctx, "B", "tb")

	ctx = metainfo.TransferForward(ctx)
	assert(t, ctx != nil)

	y, yOK = metainfo.GetPersistentValue(ctx, "A")
	z, zOK := metainfo.GetValue(ctx, "B")

	assert(t, yOK)
	assert(t, y == "pa")
	assert(t, zOK)
	assert(t, z == "tb")
}

func TestOverride(t *testing.T) {
	ctx := context.Background()
	ctx = metainfo.WithValue(ctx, "base", "base")
	ctx = metainfo.WithValue(ctx, "base2", "base")
	ctx = metainfo.WithValue(ctx, "base3", "base")

	ctx1 := metainfo.WithValue(ctx, "a", "a")
	ctx2 := metainfo.WithValue(ctx, "b", "b")

	av, ae := metainfo.GetValue(ctx1, "a")
	bv, be := metainfo.GetValue(ctx2, "b")
	assert(t, ae && av == "a", ae, av)
	assert(t, be && bv == "b", be, bv)
}
