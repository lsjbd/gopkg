// Copyright 2022 ByteDance Inc.
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
	"fmt"
	"testing"

	"github.com/bytedance/gopkg/cloud/metainfo"
)

func initMetaInfo(count int) (ctx context.Context, keys, vals, kvs []string, mkv map[string]string) {
	ctx = context.Background()
	ctx = metainfo.WithBackwardValues(ctx)
	ctx = metainfo.WithBackwardValuesToSend(ctx)
	mkv = make(map[string]string, count)
	for i := 0; i < count; i++ {
		k, v := fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i)
		ctx = metainfo.WithValue(ctx, k, v)
		ctx = metainfo.WithPersistentValue(ctx, k, v)
		metainfo.SetBackwardValue(ctx, k, v)
		metainfo.SendBackwardValue(ctx, k, v)
		keys = append(keys, k)
		vals = append(vals, v)
		kvs = append(kvs, k, v)
		mkv[k] = v
	}
	return
}

func benchmark(b *testing.B, api string, count int) {
	ctx, keys, vals, kvs, mkv := initMetaInfo(count)
	switch api {
	case "TransferForward":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.TransferForward(ctx)
		}
	case "GetValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = metainfo.GetValue(ctx, keys[i%len(keys)])
		}
	case "GetAllValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.GetAllValues(ctx)
		}
	case "RangeValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			metainfo.RangeValues(ctx, func(_, _ string) bool {
				return true
			})
		}
	case "WithValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.WithValue(ctx, "key", "val")
		}
	case "WithValueAcc":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx = metainfo.WithValue(ctx, vals[i%len(vals)], "val")
		}
	case "DelValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.DelValue(ctx, "key")
		}
	case "GetPersistentValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = metainfo.GetPersistentValue(ctx, keys[i%len(keys)])
		}
	case "GetAllPersistentValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.GetAllPersistentValues(ctx)
		}
	case "RangePersistentValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			metainfo.RangePersistentValues(ctx, func(_, _ string) bool {
				return true
			})
		}
	case "WithPersistentValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.WithPersistentValue(ctx, "key", "val")
		}
	case "WithPersistentValueAcc":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx = metainfo.WithPersistentValue(ctx, vals[i%len(vals)], "val")
		}
		_ = ctx
	case "DelPersistentValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.DelPersistentValue(ctx, "key")
		}
	case "SaveMetaInfoToMap":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := make(map[string]string)
			metainfo.SaveMetaInfoToMap(ctx, m)
		}
	case "SetMetaInfoFromMap":
		m := make(map[string]string)
		c := context.Background()
		metainfo.SaveMetaInfoToMap(ctx, m)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = metainfo.SetMetaInfoFromMap(c, m)
		}

	case "AllBackwardValuesToSend":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := metainfo.AllBackwardValuesToSend(ctx)
			_ = m
		}
	case "GetAllBackwardValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := metainfo.GetAllBackwardValues(ctx)
			_ = m
		}
	case "GetBackwardValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, ok := metainfo.GetBackwardValue(ctx, keys[count/2])
			_ = val
			_ = ok
		}
	case "RecvAllBackwardValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := metainfo.RecvAllBackwardValues(ctx)
			_ = m
		}
	case "RecvBackwardValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, ok := metainfo.RecvBackwardValue(ctx, keys[count/2])
			_ = val
			_ = ok
		}
	case "SendBackwardValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ok := metainfo.SendBackwardValue(ctx, keys[count/2], vals[count/2])
			_ = ok
		}
	case "SendBackwardValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx := metainfo.WithBackwardValuesToSend(context.Background())
			ok := metainfo.SendBackwardValues(ctx, kvs...)
			_ = ok
		}
	case "SendBackwardValuesFromMap":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx := metainfo.WithBackwardValuesToSend(context.Background())
			ok := metainfo.SendBackwardValuesFromMap(ctx, mkv)
			_ = ok
		}
	case "SetBackwardValue":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ok := metainfo.SetBackwardValue(ctx, keys[count/2], vals[count/2])
			_ = ok
		}
	case "SetBackwardValues":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx := metainfo.WithBackwardValues(context.Background())
			ok := metainfo.SetBackwardValues(ctx, kvs...)
			_ = ok
		}
	case "SetBackwardValuesFromMap":
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx := metainfo.WithBackwardValues(context.Background())
			ok := metainfo.SetBackwardValuesFromMap(ctx, mkv)
			_ = ok
		}
	case "WithBackwardValues":
		if count > 1 {
			b.SkipNow()
		}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx1 := metainfo.WithBackwardValues(ctx)
			_ = ctx1
		}
	case "WithBackwardValuesToSend":
		if count > 1 {
			b.SkipNow()
		}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx1 := metainfo.WithBackwardValuesToSend(ctx)
			_ = ctx1
		}
	}
}

func benchmarkParallel(b *testing.B, api string, count int) {
	ctx, keys, vals, _, _ := initMetaInfo(count)
	switch api {
	case "TransferForward":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.TransferForward(ctx)
			}
		})
	case "GetValue":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var i int
			for pb.Next() {
				_, _ = metainfo.GetValue(ctx, keys[i%len(keys)])
				i++
			}
		})
	case "GetAllValues":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.GetAllValues(ctx)
			}
		})
	case "RangeValues":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				metainfo.RangeValues(ctx, func(_, _ string) bool {
					return true
				})
			}
		})
	case "WithValue":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.WithValue(ctx, "key", "val")
			}
		})
	case "WithValueAcc":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			tmp := ctx
			var i int
			for pb.Next() {
				tmp = metainfo.WithValue(tmp, vals[i%len(vals)], "val")
				i++
			}
		})
	case "DelValue":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.DelValue(ctx, "key")
			}
		})
	case "GetPersistentValue":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var i int
			for pb.Next() {
				_, _ = metainfo.GetPersistentValue(ctx, keys[i%len(keys)])
				i++
			}
		})
	case "GetAllPersistentValues":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.GetAllPersistentValues(ctx)
			}
		})
	case "RangePersistentValues":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				metainfo.RangePersistentValues(ctx, func(_, _ string) bool {
					return true
				})
			}
		})
	case "WithPersistentValue":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.WithPersistentValue(ctx, "key", "val")
			}
		})
	case "WithPersistentValueAcc":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			tmp := ctx
			var i int
			for pb.Next() {
				tmp = metainfo.WithPersistentValue(tmp, vals[i%len(vals)], "val")
				i++
			}
		})
	case "DelPersistentValue":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.DelPersistentValue(ctx, "key")
			}
		})
	case "SaveMetaInfoToMap":
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m := make(map[string]string)
				metainfo.SaveMetaInfoToMap(ctx, m)
			}
		})
	case "SetMetaInfoFromMap":
		m := make(map[string]string)
		c := context.Background()
		metainfo.SaveMetaInfoToMap(ctx, m)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = metainfo.SetMetaInfoFromMap(c, m)
			}
		})
	}
}

func BenchmarkAll(b *testing.B) {
	APIs := []string{
		"TransferForward",
		"GetValue",
		"GetAllValues",
		"WithValue",
		"WithValueAcc",
		"DelValue",
		"GetPersistentValue",
		"GetAllPersistentValues",
		"RangeValues",
		"RangePersistentValues",
		"WithPersistentValue",
		"WithPersistentValueAcc",
		"DelPersistentValue",
		"SaveMetaInfoToMap",
		"SetMetaInfoFromMap",

		"AllBackwardValuesToSend",
		"GetAllBackwardValues",
		"GetBackwardValue",
		"RecvAllBackwardValues",
		"RecvBackwardValue",
		"SendBackwardValue",
		"SendBackwardValues",
		"SendBackwardValuesFromMap",
		"SetBackwardValue",
		"SetBackwardValues",
		"SetBackwardValuesFromMap",
		"WithBackwardValues",
		"WithBackwardValuesToSend",
	}
	for _, api := range APIs {
		for _, cnt := range []int{1, 10, 20, 50, 100} {
			fun := fmt.Sprintf("%s_%d", api, cnt)
			b.Run(fun, func(b *testing.B) {
				benchmark(b, api, cnt)
			})
		}
	}
}

func BenchmarkAllParallel(b *testing.B) {
	APIs := []string{
		"TransferForward",
		"GetValue",
		"GetAllValues",
		"WithValue",
		"WithValueAcc",
		"DelValue",
		"GetPersistentValue",
		"GetAllPersistentValues",
		"RangePersistentValues",
		"RangeValues",
		"WithPersistentValue",
		"WithPersistentValueAcc",
		"DelPersistentValue",
		"SaveMetaInfoToMap",
		"SetMetaInfoFromMap",

		/* Don't need to test backward APIs in parallel */
		// "AllBackwardValuesToSend",
		// "GetAllBackwardValues",
		// "GetBackwardValue",
		// "RecvAllBackwardValues",
		// "RecvBackwardValue",
		// "SendBackwardValue",
		// "SendBackwardValues",
		// "SendBackwardValuesFromMap",
		// "SetBackwardValue",
		// "SetBackwardValues",
		// "SetBackwardValuesFromMap",
		// "WithBackwardValues",
		// "WithBackwardValuesToSend",
	}
	for _, api := range APIs {
		for _, cnt := range []int{1, 10, 20, 50, 100} {
			fun := fmt.Sprintf("%s_%d", api, cnt)
			b.Run(fun, func(b *testing.B) {
				benchmarkParallel(b, api, cnt)
			})
		}
	}
}
