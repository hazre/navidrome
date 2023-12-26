package scanner2

import (
	"context"
	"path/filepath"

	"github.com/google/go-pipeline/pkg/pipeline"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/scanner/metadata"
	"github.com/navidrome/navidrome/utils/slice"
	"golang.org/x/exp/maps"
)

const (
	// filesBatchSize used for batching file metadata extraction
	filesBatchSize = 100
)

func processFolder(ctx context.Context) pipeline.StageFn[*folderEntry] {
	return func(entry *folderEntry) (*folderEntry, error) {
		// Load children mediafiles from DB
		mfs, err := entry.scanCtx.ds.MediaFile(ctx).GetByFolder(entry.id)
		if err != nil {
			log.Error(ctx, "Scanner: Error loading mediafiles from DB", "folder", entry.path, err)
			return entry, err
		}
		dbTracks := slice.ToMap(mfs, func(mf model.MediaFile) (string, model.MediaFile) { return mf.Path, mf })

		// Get list of files to import, leave dbTracks with tracks to be removed
		var filesToImport []string
		for afPath, af := range entry.audioFiles {
			fullPath := filepath.Join(entry.path, afPath)
			dbTrack, foundInDB := dbTracks[afPath]
			if !foundInDB || entry.scanCtx.fullRescan {
				filesToImport = append(filesToImport, fullPath)
			} else {
				info, err := af.Info()
				if err != nil {
					log.Warn(ctx, "Scanner: Error getting file info", "folder", entry.path, "file", af.Name(), err)
					return nil, err
				}
				if info.ModTime().After(dbTrack.UpdatedAt) {
					filesToImport = append(filesToImport, fullPath)
				}
			}
			delete(dbTracks, afPath)
		}

		// Remaining dbTracks are tracks that were not found in the folder, so they should be marked as missing
		entry.missingTracks = maps.Values(dbTracks)

		entry.tracks, err = loadTagsFromFiles(ctx, entry, filesToImport)
		if err != nil {
			log.Warn(ctx, "Scanner: Error loading tags from files. Skipping", "folder", entry.path, err)
			return entry, nil
		}

		entry.albums = loadAlbumsFromTags(ctx, entry)
		entry.artists = loadArtistsFromTags(ctx, entry)

		return entry, nil
	}
}

func loadTagsFromFiles(ctx context.Context, entry *folderEntry, toImport []string) (model.MediaFiles, error) {
	tracks := model.MediaFiles{}
	mapper := newMediaFileMapper(entry)
	err := slice.RangeByChunks(toImport, filesBatchSize, func(chunk []string) error {
		allTags, err := metadata.Extract(toImport...)
		if err != nil {
			log.Warn(ctx, "Scanner: Error extracting tags from files. Skipping", "folder", entry.path, err)
			return err
		}
		for _, tags := range allTags {
			track := mapper.toMediaFile(tags)
			tracks = append(tracks, track)
		}
		return nil
	})
	return tracks, err
}

func loadAlbumsFromTags(ctx context.Context, entry *folderEntry) model.Albums {
	return nil // TODO
}

func loadArtistsFromTags(ctx context.Context, entry *folderEntry) model.Artists {
	return nil // TODO
}
