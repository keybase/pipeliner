package pipeliner

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPipeliner(t *testing.T) {
	v, launched, err := testPipeliner(false, false)
	require.NoError(t, err)
	require.Equal(t, len(v), launched)
	for i, e := range v {
		require.Equal(t, i, e)
	}
}

func TestPipelinerError(t *testing.T) {
	testPipelinerError(t)
}

func testPipelinerError(t *testing.T) {
	v, launched, err := testPipeliner(true, false)
	require.Error(t, err)
	if err != nil {
		require.Equal(t, err.Error(), "errored out")
	}
	// Indices below `launched` were handed to a goroutine and may have
	// written a value; how many of those race ahead of the error being
	// noticed varies run to run. Only indices at or past `launched` were
	// never launched at all, so they must be untouched.
	for _, e := range v[launched:] {
		require.Equal(t, 0, e)
	}
}

func TestPipelinerCancel(t *testing.T) {
	v, launched, err := testPipeliner(false, true)
	require.Error(t, err)
	require.Equal(t, err.Error(), "context canceled")
	for _, e := range v[launched:] {
		require.Equal(t, 0, e)
	}
}

func TestPipelinerErrorStress(t *testing.T) {
	for range 200 {
		testPipelinerError(t)
	}
}

// TestPipelinerNoGoroutineLeakOnError is a regression test: callers that
// follow the documented pattern of returning immediately when WaitForRoom
// errors out must not leak the goroutines that were still in flight.
func TestPipelinerNoGoroutineLeakOnError(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()

	for range 200 {
		testPipelinerError(t)
	}

	// Give any leaked goroutines a chance to be scheduled and block, so
	// they show up in the goroutine count rather than racing this check.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()

	require.LessOrEqual(t, after, before+2,
		"goroutine count grew from %d to %d, suggesting a leak", before, after)
}

// TestTryReserveConcurrentNeverExceedsWindow hammers tryReserve from many
// goroutines to guard the check-and-reserve race. It calls tryReserve
// directly rather than through WaitForRoom: concurrent WaitForRoom callers
// can race over CompleteOne's wakeups, a separate hazard this library
// doesn't promise to handle.
func TestTryReserveConcurrentNeverExceedsWindow(t *testing.T) {
	const window = 4
	const attempts = 5000

	p := NewPipeliner(window)

	var wg sync.WaitGroup
	var active atomic.Int32
	var maxActive atomic.Int32

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !p.tryReserve() {
				return
			}
			defer p.pending.Done()

			n := active.Add(1)
			for {
				old := maxActive.Load()
				if n <= old || maxActive.CompareAndSwap(old, n) {
					break
				}
			}
			active.Add(-1)
			p.landOne()
		}()
	}

	wg.Wait()
	require.LessOrEqual(t, int(maxActive.Load()), window,
		"observed %d reservations active at once, exceeding window %d", maxActive.Load(), window)
}

// TestPublicAPIDoesNotExposeLockMethods guards against re-embedding
// sync.RWMutex (rather than holding it as an unexported field), which would
// silently promote Lock/Unlock/RLock/RUnlock onto the public API again.
func TestPublicAPIDoesNotExposeLockMethods(t *testing.T) {
	typ := reflect.TypeFor[*Pipeliner]()
	for _, name := range []string{"Lock", "Unlock", "RLock", "RUnlock"} {
		_, ok := typ.MethodByName(name)
		require.False(t, ok, "Pipeliner must not export %s", name)
	}
}

// TestSetErrorFirstWins pins down the sticky-error invariant directly: once
// an error is recorded, later errors must never overwrite it.
func TestSetErrorFirstWins(t *testing.T) {
	p := NewPipeliner(4)
	first := errors.New("first")
	second := errors.New("second")

	p.setError(first)
	p.setError(second)

	require.Equal(t, first, p.getError())
}

// TestStickyErrorReturnedByWaitForRoomAndFlush verifies the sticky error
// surfaces consistently through the public API: once set, WaitForRoom and
// Flush keep returning it, even across further calls on the same instance.
func TestStickyErrorReturnedByWaitForRoomAndFlush(t *testing.T) {
	p := NewPipeliner(1)
	ctx := context.Background()

	require.NoError(t, p.WaitForRoom(ctx))
	first := errors.New("first")
	go p.CompleteOne(first)

	err := p.Flush(ctx)
	require.Equal(t, first, err)

	// Reusing the instance after an error keeps returning the same error
	// rather than doing further work or surfacing a different one.
	err = p.WaitForRoom(ctx)
	require.Equal(t, first, err)

	err = p.Flush(ctx)
	require.Equal(t, first, err)
}

// testPipeliner runs a batch of 100 simulated requests through a Pipeliner.
// It returns the results, the number of requests actually handed to a
// goroutine before WaitForRoom stopped returning nil, and any resulting
// error.
func testPipeliner(doError bool, doCancel bool) (v []int, launched int, err error) {
	v = make([]int, 100)
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
		werr := pipeliner.WaitForRoom(ctx)
		if werr != nil {
			// WaitForRoom drains any in-flight goroutines before returning
			// an error, so it's safe to return immediately here.
			return v, launched, werr
		}
		launched++
		go func(i int) { pipeliner.CompleteOne(f(ctx, i)) }(i)
	}

	err = pipeliner.Flush(ctx)
	return v, launched, err
}
