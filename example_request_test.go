package pipeliner_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/keybase/pipeliner"
)

type Request struct{ i int } // nolint
type Result struct{ i int }

func (r Request) Do() (Result, error) {
	time.Sleep(time.Millisecond)
	return Result(r), nil
}

func Example() {
	requests := []Request{{0}, {1}, {2}, {3}, {4}}
	results, _ := makeRequests(context.Background(), requests, 2)
	for _, r := range results {
		fmt.Printf("%d ", r.i)
	}
	// Output:
	// 0 1 2 3 4
}

// makeRequests calls `Do` on all of the given requests, with only `window` outstanding
// at any given time. It puts the results in `results`, and errors out on the first
// failure.
func makeRequests(ctx context.Context, requests []Request, window int) (results []Result, err error) {

	var resultsLock sync.Mutex
	results = make([]Result, len(requests))

	pipeliner := pipeliner.NewPipeliner(window)

	worker := func(ctx context.Context, i int) error {
		res, err := requests[i].Do()
		resultsLock.Lock()
		results[i] = res
		resultsLock.Unlock()
		return err // the first error will kill the pipeline
	}

	for i := range requests {
		err := pipeliner.WaitForRoom(ctx)
		if err != nil {
			return nil, err
		}
		go func(i int) { pipeliner.CompleteOne(worker(ctx, i)) }(i)
	}
	return results, pipeliner.Flush(ctx)
}
