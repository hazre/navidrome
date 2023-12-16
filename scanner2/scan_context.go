package scanner2

import (
	"time"

	"github.com/navidrome/navidrome/model"
)

type scanContext struct {
	lib       model.Library
	startTime time.Time
}

func newScannerContext(lib model.Library) *scanContext {
	return &scanContext{
		lib:       lib,
		startTime: time.Now(),
	}
}
