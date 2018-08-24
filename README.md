# pipeliner

A simplified pipline library, for parallel requests with bounded parallelism.

## Getting

```sh
go get github.com/keybase/pipeliner
```

## Background

Often you want do network requests with bounded parallelism. Let's say you have
1,000 DNS queries to make, and don't want to wait for them to complete in serial,
but don't want to blast your server with 1,000 simultaneous requests. In this case,
*bounded parallelism* makes sense. Make 1,000 requests with only 10 outstanding
at any one time.

At this point, I usually Google for it, and come up with [this blog post](https://blog.golang.org/pipelines), and I become slightly sad, because that is a lot of code to digest and
understand to do something that should be rather simple. It's not really the fault
of the languge, but more so the library. Here is a library that makes it a lot
easier:

## Example

```go

import (
	"github.com/keybase/pipeliner"
)

// makeRequests calls `Do` on all of the given requests, with only `window` outstanding
// at any given time. It puts the results in `results`, and errors out on the first
// failure.
func makeRequests(ctx context.Context, requests []Requests, window int) (results []Results, err error) {

	var resultsLock sync.Mutex
	results = make([]Results, len(requests))

	pipeliner := pipeliner.NewPipeliner(window)

	worker := func(ctx context.Context, i int) error {
		res, err := requests[i].Do()
		vlock.Lock()
		results[i] = res
		vlock.Unlock()
		return err // the first error will kill the pipeline
	}

	for i := range v {
		err := pipeliner.WaitForRoom(ctx)
		if err != nil {
			return v, err
		}
		go func(i int) { pipeliner.CompleteOne(worker(ctx, i)) }(i)
	}
	return results, pipeliner.Flush(ctx)
}
```
