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
	ctx context.Context
	ds  model.DataStore
}

func New(ctx context.Context, ds model.DataStore) scanner.Scanner {
	return &scanner2{ctx: ctx, ds: ds}
}

func (s *scanner2) RescanAll(ctx context.Context, fullRescan bool) error {
	ctx = request.AddValues(s.ctx)

	libs, err := s.ds.Library(ctx).GetAll()
	if err != nil {
		return err
	}

	startTime := time.Now()
	log.Info(ctx, "Scanner: Starting scan", "fullRescan", fullRescan, "numLibraries", len(libs))
	scanCtxChan := createScanContexts(ctx, libs)
	folderChan, folderErrChan := walkDirEntries(ctx, scanCtxChan)
	logErrChan := pl.Sink(ctx, 4, folderChan, func(ctx context.Context, folder *folderEntry) error {
		log.Debug(ctx, "Scanner: Found folder", "folder", folder.Name(), "_path", folder.path, "audioCount", folder.audioFilesCount, "images", folder.images, "hasPlaylist", folder.hasPlaylists)
		return nil
	})

	// Wait for pipeline to end, return first error found
	for err := range pl.Merge(ctx, folderErrChan, logErrChan) {
		return err
	}

	log.Info(ctx, "Scanner: Scan finished", "duration", time.Since(startTime))
	return nil
}

func createScanContexts(ctx context.Context, libs []model.Library) chan *scanContext {
	outputChannel := make(chan *scanContext, len(libs))
	go func() {
		defer close(outputChannel)
		for _, lib := range libs {
			outputChannel <- newScannerContext(lib)
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
