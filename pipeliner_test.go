package pipeliner

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPipeliner(t *testing.T) {
	v, err := testPipeliner(false, false)
	require.NoError(t, err)
	for i, e := range v {
		require.Equal(t, i, e)
	}
}

func TestPipelinerError(t *testing.T) {
	testPipelinerError(t)
}

func testPipelinerError(t *testing.T) {
	v, err := testPipeliner(true, false)
	require.Error(t, err)
	if err != nil {
		require.Equal(t, err.Error(), "errored out")
	}
	for _, e := range v[28:] {
		require.Equal(t, 0, e)
	}
}

func TestPipelinerCancel(t *testing.T) {
	v, err := testPipeliner(false, true)
	require.Error(t, err)
	require.Equal(t, err.Error(), "context canceled")
	for _, e := range v[28:] {
		require.Equal(t, 0, e)
	}
}

func TestPipelinerErrorStress(t *testing.T) {
	for i := 0; i < 200; i++ {
		testPipelinerError(t)
	}
}

func testPipeliner(doError bool, doCancel bool) ([]int, error) {
	v := make([]int, 100)
	var vlock sync.Mutex
	pipeliner := NewPipeliner(4)
	ctx := context.Background()
	var cancelFunc func()
	if doCancel {
		ctx, cancelFunc = context.WithCancel(ctx)
	}

	f := func(_ context.Context, i int) error {
		if doError && i == 20 {
			return errors.New("errored out")
		}
		if cancelFunc != nil && i == 20 {
			cancelFunc()
		}
		vlock.Lock()
		v[i] = i
		vlock.Unlock()
		time.Sleep(time.Microsecond * time.Duration((rand.Int() % 17))) //nolint:gosec // Test code, weak RNG is acceptable
		return nil
	}

	for i := range v {
		err := pipeliner.WaitForRoom(ctx)
		if err != nil {
			// Ensure any in-flight goroutines are complete before returning. Note that Flush does not actually wait if there is an error.
			for pipeliner.hasOutstanding() {
				<-pipeliner.ch
			}
			return v, err
		}
		go func(i int) { pipeliner.CompleteOne(f(ctx, i)) }(i)
	}

	err := pipeliner.Flush(ctx)
	return v, err
}
