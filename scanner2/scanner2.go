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
	"github.com/navidrome/navidrome/utils/slice"
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

	scanCtxChan := createScanContexts(ctx, s.ds, libs, fullRescan)
	folderChan, folderErrChan := walkDirEntries(ctx, scanCtxChan)
	changedFolderChan, changedFolderErrChan := pl.Filter(ctx, 4, folderChan, onlyOutdated)
	processedFolderChan, processedFolderErrChan := pl.Stage(ctx, 4, changedFolderChan, processFolder)

	logErrChan := pl.Sink(ctx, 4, processedFolderChan, func(ctx context.Context, folder *folderEntry) error {
		log.Debug(ctx, "Scanner: Found folder", "folder", folder.Name(), "_path", folder.path,
			"audioCount", len(folder.audioFiles), "imageCount", len(folder.imageFiles), "plsCount", len(folder.playlists))
		return nil
	})

	// Wait for pipeline to end, return first error found
	for err := range pl.Merge(ctx, folderErrChan, logErrChan, changedFolderErrChan, processedFolderErrChan) {
		return err
	}

	log.Info(ctx, "Scanner: Finished scanning all libraries", "duration", time.Since(startTime))
	return nil
}

func createScanContexts(ctx context.Context, ds model.DataStore, libs []model.Library, fullRescan bool) chan *scanContext {
	outputChannel := make(chan *scanContext, len(libs))
	go func() {
		defer close(outputChannel)
		for _, lib := range libs {
			scanCtx, err := newScannerContext(ctx, ds, lib, fullRescan)
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

// onlyOutdated returns a filter function that returns true if the folder is outdated (needs to be scanned)
func onlyOutdated(_ context.Context, entry *folderEntry) (bool, error) {
	return entry.scanCtx.fullRescan || entry.isExpired(), nil
}

func processFolder(ctx context.Context, entry *folderEntry) (*folderEntry, error) {
	// Load children mediafiles from DB
	mfs, err := entry.scanCtx.ds.MediaFile(ctx).GetByFolder(entry.id)
	if err != nil {
		log.Warn(ctx, "Scanner: Error loading mediafiles from DB. Skipping", "folder", entry.path, err)
		return entry, nil
	}
	dbTracks := slice.ToMap(mfs, func(mf model.MediaFile) (string, model.MediaFile) { return mf.Path, mf })

	// Get list of files to import, leave dbTracks with tracks to be removed
	var filesToImport []string
	for afPath, af := range entry.audioFiles {
		dbTrack, foundInDB := dbTracks[afPath]
		if !foundInDB || entry.scanCtx.fullRescan {
			filesToImport = append(filesToImport, afPath)
		} else {
			info, err := af.Info()
			if err != nil {
				log.Warn(ctx, "Scanner: Error getting file info", "folder", entry.path, "file", af.Name(), err)
				return nil, err
			}
			if info.ModTime().After(dbTrack.UpdatedAt) {
				filesToImport = append(filesToImport, afPath)
			}
		}
		delete(dbTracks, afPath)
	}
	//tracksToRemove := dbTracks // Just to name things properly

	// Load tags from files to import
	// Add new/updated files to DB
	// Remove deleted mediafiles from DB
	// Update folder info in DB

	return entry, nil
}

func (s *scanner2) Status(context.Context) (*scanner.StatusInfo, error) {
	return &scanner.StatusInfo{}, nil
}

//nolint:unused
func (s *scanner2) doScan(ctx context.Context, fullRescan bool, folders <-chan string) error {
	return nil
}

var _ scanner.Scanner = (*scanner2)(nil)
