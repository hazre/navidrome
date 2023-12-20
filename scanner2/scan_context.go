package scanner2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/navidrome/navidrome/model"
)

type scanContext struct {
	lib         model.Library
	ds          model.DataStore
	startTime   time.Time
	lastUpdates map[string]time.Time
	lock        sync.RWMutex
}

func newScannerContext(ctx context.Context, ds model.DataStore, lib model.Library) (*scanContext, error) {
	lastUpdates, err := ds.Folder(ctx).GetLastUpdates(lib)
	if err != nil {
		return nil, fmt.Errorf("error getting last updates: %w", err)
	}
	return &scanContext{
		lib:         lib,
		ds:          ds,
		startTime:   time.Now(),
		lastUpdates: lastUpdates,
	}, nil
}

func (s *scanContext) getLastUpdatedInDB(id string) time.Time {
	s.lock.RLock()
	defer s.lock.RUnlock()

	t, ok := s.lastUpdates[id]
	if !ok {
		return time.Time{}
	}
	return t
}
