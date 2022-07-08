package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_getBatchSemaphore(t *testing.T) {
	test(t, getBatchSemaphore)
}

func Test_getBatchDivideWork(t *testing.T) {
	test(t, getBatchDivideWork)
}

func Test_GetBatch(t *testing.T) {
	test(t, getBatch)
}

func test(t *testing.T, target func(int64, int64) []user) {
	type args struct {
		n    int64
		pool int64
	}
	tests := []struct {
		args    args
		wantRes []user
	}{
		{args: args{n: 10, pool: 1}, wantRes: createRes(10)},
		{args: args{n: 10, pool: 2}, wantRes: createRes(10)},
		{args: args{n: 10, pool: 5}, wantRes: createRes(10)},
		{args: args{n: 20, pool: 4}, wantRes: createRes(20)},
		{args: args{n: 100, pool: 10}, wantRes: createRes(100)},
		{args: args{n: 15, pool: 5}, wantRes: createRes(15)},
		{args: args{n: 35, pool: 5}, wantRes: createRes(35)},
		{args: args{n: 100, pool: 6}, wantRes: createRes(100)},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			start := time.Now()
			wantTime := start.Add(time.Duration(tt.args.n/tt.args.pool) * 100)
			actualRes := target(tt.args.n, tt.args.pool)
			since := time.Since(start).Milliseconds()
			assert.WithinDuration(t, wantTime, start.Add(time.Duration(since)), time.Nanosecond*200)
			assert.ElementsMatch(t, tt.wantRes, actualRes)
		})
	}
}

func createRes(v int64) []user {
	res := make([]user, 0, v)
	for i := 0; i < int(v); i++ {
		res = append(res, user{ID: int64(i)})
	}
	return res
}

// benchmarking

var input = []struct {
	n    int64
	pool int64
}{
	{100000, 6},
	{100000, 12},
	{100000, 24},
	{100000, 56},
}

func benchmarkOverInput(b *testing.B, target func(int64, int64) []user) {
	for _, in := range input {
		b.Run(fmt.Sprintf("n_%d_pool_%d", in.n, in.pool), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				target(in.n, in.pool)
			}
		})
	}
}

func Benchmark_getBatchSemaphore(b *testing.B) {
	benchmarkOverInput(b, getBatchSemaphore)
}

func Benchmark_getBatchDivideWork(b *testing.B) {
	benchmarkOverInput(b, getBatchDivideWork)
}
