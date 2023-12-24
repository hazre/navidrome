package scanner2

import (
	"context"
	"io/fs"
	"time"

	"github.com/charlievieth/fastwalk"
	"github.com/google/go-pipeline/pkg/pipeline"
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

	err = s.runPipeline(
		pipeline.NewProducer(s.produceFolders(ctx, libs, fullRescan), pipeline.Name("read folders from disk")),
		pipeline.NewStage(s.processFolder(ctx), pipeline.Name("process folder")),
		pipeline.NewStage(s.logFolder(ctx), pipeline.Name("log results")),
	)

	if err != nil {
		log.Error(ctx, "Scanner: Error scanning libraries", "duration", time.Since(startTime), err)
	} else {
		log.Info(ctx, "Scanner: Finished scanning all libraries", "duration", time.Since(startTime))
	}
	return err
}

func (s *scanner2) runPipeline(producer pipeline.Producer[*folderEntry], stages ...pipeline.Stage[*folderEntry]) error {
	if log.CurrentLevel() >= log.LevelDebug {
		metrics, err := pipeline.Measure(producer, stages...)
		log.Trace(metrics.String())
		return err
	}
	return pipeline.Do(producer, stages...)
}

func (s *scanner2) logFolder(ctx context.Context) func(folder *folderEntry) (out *folderEntry, err error) {
	return func(folder *folderEntry) (out *folderEntry, err error) {
		log.Debug(ctx, "Scanner: Found folder", "folder", folder.Name(), "_path", folder.path,
			"audioCount", len(folder.audioFiles), "imageCount", len(folder.imageFiles), "plsCount", len(folder.playlists))
		return folder, nil
	}
}

func (s *scanner2) produceFolders(ctx context.Context, libs []model.Library, fullRescan bool) pipeline.ProducerFn[*folderEntry] {
	scanCtxChan := make(chan *scanContext, len(libs))
	go func() {
		defer close(scanCtxChan)
		for _, lib := range libs {
			scanCtx, err := newScannerContext(ctx, s.ds, lib, fullRescan)
			if err != nil {
				log.Error(ctx, "Scanner: Error creating scan context", "lib", lib.Name, err)
				continue
			}
			scanCtxChan <- scanCtx
		}
	}()
	return func(put func(entry *folderEntry)) error {
		outputChan := make(chan *folderEntry)
		go func() {
			defer close(outputChan)
			for scanCtx := range pl.ReadOrDone(ctx, scanCtxChan) {
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
					outputChan <- folder
					return nil
				})
				if err != nil {
					log.Warn(ctx, "Scanner: Error scanning library", "lib", scanCtx.lib.Name, err)
				}
			}
		}()
		var total int
		for folder := range pl.ReadOrDone(ctx, outputChan) {
			total++
			put(folder)
		}
		log.Info(ctx, "Scanner: Finished loading all folders", "numFolders", total)
		return nil
	}
}

func (s *scanner2) processFolder(ctx context.Context) pipeline.StageFn[*folderEntry] {
	return func(entry *folderEntry) (*folderEntry, error) {
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
}

func (s *scanner2) Status(context.Context) (*scanner.StatusInfo, error) {
	return &scanner.StatusInfo{}, nil
}

//nolint:unused
func (s *scanner2) doScan(ctx context.Context, fullRescan bool, folders <-chan string) error {
	return nil
}

var _ scanner.Scanner = (*scanner2)(nil)
