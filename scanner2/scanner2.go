package scanner2

import (
	"context"
	"io/fs"
	"time"

	"github.com/charlievieth/fastwalk"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/model/request"
	"github.com/navidrome/navidrome/scanner"
	"github.com/navidrome/navidrome/utils/pl"
)

type scanner2 struct {
	processCtx context.Context
	ds         model.DataStore
}

func New(ctx context.Context, ds model.DataStore) scanner.Scanner {
	return &scanner2{processCtx: ctx, ds: ds}
}

func (s *scanner2) RescanAll(requestCtx context.Context, fullRescan bool) error {
	ctx := request.AddValues(s.processCtx, requestCtx)

	libs, err := s.ds.Library(ctx).GetAll()
	if err != nil {
		return err
	}

	startTime := time.Now()
	log.Info(ctx, "Scanner: Starting scan", "fullRescan", fullRescan, "numLibraries", len(libs))

	scanCtxChan := createScanContexts(ctx, s.ds, libs)
	folderChan, folderErrChan := walkDirEntries(ctx, scanCtxChan)
	changedFolderChan, changedFolderErrChan := pl.Filter(ctx, 4, folderChan, onlyOutdated(fullRescan))

	// TODO Next: load tags from all files that are newer than or not in DB

	logErrChan := pl.Sink(ctx, 4, changedFolderChan, func(ctx context.Context, folder *folderEntry) error {
		log.Debug(ctx, "Scanner: Found folder", "folder", folder.Name(), "_path", folder.path,
			"audioCount", len(folder.audioFiles), "imageCount", len(folder.imageFiles), "plsCount", len(folder.playlists))
		return nil
	})

	// Wait for pipeline to end, return first error found
	for err := range pl.Merge(ctx, folderErrChan, logErrChan, changedFolderErrChan) {
		return err
	}

	log.Info(ctx, "Scanner: Finished scanning all libraries", "duration", time.Since(startTime))
	return nil
}

// onlyOutdated returns a filter function that returns true if the folder is outdated (needs to be scanned)
func onlyOutdated(fullScan bool) func(ctx context.Context, entry *folderEntry) (bool, error) {
	return func(ctx context.Context, entry *folderEntry) (bool, error) {
		return fullScan || entry.isExpired(), nil
	}
}

func createScanContexts(ctx context.Context, ds model.DataStore, libs []model.Library) chan *scanContext {
	outputChannel := make(chan *scanContext, len(libs))
	go func() {
		defer close(outputChannel)
		for _, lib := range libs {
			scanCtx, err := newScannerContext(ctx, ds, lib)
			if err != nil {
				log.Error(ctx, "Scanner: Error creating scan context", "lib", lib.Name, err)
				continue
			}
			outputChannel <- scanCtx
		}
	}()
	return outputChannel
}

func walkDirEntries(ctx context.Context, libsChan <-chan *scanContext) (chan *folderEntry, chan error) {
	outputChannel := make(chan *folderEntry)
	errChannel := make(chan error)
	go func() {
		defer close(outputChannel)
		defer close(errChannel)
		errChan := pl.Sink(ctx, 1, libsChan, func(ctx context.Context, scanCtx *scanContext) error {
			conf := &fastwalk.Config{Follow: true}
			// lib.Path
			err := fastwalk.Walk(conf, scanCtx.lib.Path, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					log.Warn(ctx, "Scanner: Error walking path", "lib", scanCtx.lib.Name, "path", path, err)
					return nil
				}

				// Skip non-directories
				if !d.IsDir() {
					return nil
				}

				// Load all pertinent info from directory
				folder, _, err := loadDir(ctx, scanCtx, path, d.(fastwalk.DirEntry))
				if err != nil {
					log.Warn(ctx, "Scanner: Error loading dir", "lib", scanCtx.lib.Name, "path", path, err)
					return nil
				}
				outputChannel <- folder
				return nil
			})
			if err != nil {
				log.Warn(ctx, "Scanner: Error scanning library", "lib", scanCtx.lib.Name, err)
			}
			return nil
		})

		// Wait for pipeline to end, and forward any errors
		for err := range pl.ReadOrDone(ctx, errChan) {
			select {
			case errChannel <- err:
			default:
			}
		}
	}()
	return outputChannel, errChannel
}
func (s *scanner2) Status(context.Context) (*scanner.StatusInfo, error) {
	return &scanner.StatusInfo{}, nil
}

//nolint:unused
func (s *scanner2) doScan(ctx context.Context, lib model.Library, fullRescan bool, folders <-chan string) error {
	return nil
}

var _ scanner.Scanner = (*scanner2)(nil)
