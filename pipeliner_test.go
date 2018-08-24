package pipeliner

import (
	"errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestPipeliner(t *testing.T) {
	v, err := testPipeliner(false, false)
	require.NoError(t, err)
	for i, e := range v {
		require.Equal(t, i, e)
	}
}

func TestPipelinerError(t *testing.T) {
	v, err := testPipeliner(true, false)
	require.Error(t, err)
	if err != nil {
		require.Equal(t, err.Error(), "errored out")
	}
	for _, e := range v[25:] {
		require.Equal(t, 0, e)
	}
}

func TestPipelinerCancel(t *testing.T) {
	v, err := testPipeliner(false, true)
	require.Error(t, err)
	require.Equal(t, err.Error(), "context canceled")
	for _, e := range v[25:] {
		require.Equal(t, 0, e)
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

	f := func(ctx context.Context, i int) error {
		if doError && i == 20 {
			return errors.New("errored out")
		}
		if cancelFunc != nil && i == 20 {
			cancelFunc()
		}
		vlock.Lock()
		v[i] = i
		vlock.Unlock()
		time.Sleep(time.Microsecond * time.Duration((rand.Int() % 17)))
		return nil
	}

	for i := range v {
		err := pipeliner.WaitForRoom(ctx)
		if err != nil {
			return v, err
		}
		go func(i int) { pipeliner.CompleteOne(f(ctx, i)) }(i)
	}
	err := pipeliner.Flush(ctx)
	return v, err
}
