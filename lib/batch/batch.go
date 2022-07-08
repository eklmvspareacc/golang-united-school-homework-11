package batch

import (
	"math"
	"sync"
	"time"
)

type user struct {
	ID int64
}

func getOne(id int64) user {
	time.Sleep(time.Millisecond * 100)
	return user{ID: id}
}

func getBatch(n int64, pool int64) (res []user) {
	return getBatchSemaphore(n, pool)
}

// Goroutine per each request
func getBatchSemaphore(n int64, pool int64) (res []user) {
	var wg sync.WaitGroup
	var lock sync.Mutex
	sem := make(chan struct{}, pool)
	for i := int64(0); i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(id int64) {
			user := getOne(id)
			lock.Lock()
			res = append(res, user)
			lock.Unlock()
			<-sem
			wg.Done()
		}(i)
	}
	wg.Wait()
	return
}

//Goroutine per batch of requests
func getBatchWorkers(n int64, pool int64) (res []user) {
	producer := make(chan []user)
	workerCapacity := int64(math.Ceil(float64(n) / float64(pool)))
	for i := int64(0); i < pool; i++ {
		go func(worker int64) {
			startId := worker * workerCapacity
			endId := (worker + 1) * workerCapacity
			if worker == pool-1 {
				endId = n
			}
			var batch []user
			for id := startId; id < endId; id++ {
				batch = append(batch, getOne(id))
			}
			producer <- batch
		}(i)
	}

	for i := int64(0); i < pool; i++ {
		batch := <-producer
		res = append(res, batch...)
	}
	return
}
