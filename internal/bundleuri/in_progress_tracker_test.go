package bundleuri

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInProgressTracker(t *testing.T) {
	key := "key1"

	testCases := []struct {
		desc               string
		expectedInProgress uint
		actions            func(InProgressTracker)
	}{
		{
			desc:               "one in flight",
			expectedInProgress: 1,
			actions: func(t InProgressTracker) {
				t.IncrementInProgress(key)
				t.IncrementInProgress(key)
				t.DecrementInProgress(key)
			},
		},
		{
			desc:               "multiple in flight with concurrent writes",
			expectedInProgress: 4,
			actions: func(t InProgressTracker) {
				var wg sync.WaitGroup

				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func() {
						t.IncrementInProgress(key)
						wg.Done()
					}()
				}
				wg.Wait()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tracker := NewInProgressTracker()

			tc.actions(tracker)
			require.Equal(t, tc.expectedInProgress, tracker.GetInProgress(key))
		})
	}

	t.Run("key is deleted when in progress reaches 0", func(t *testing.T) {
		tracker := NewInProgressTracker()
		tracker.IncrementInProgress(key)
		tracker.DecrementInProgress(key)
		require.Equal(t, uint(0), tracker.GetInProgress(key))
		_, ok := tracker.inProgress[key]
		require.False(t, ok)
	})
}
