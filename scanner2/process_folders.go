package scanner2

import (
	"context"

	"github.com/google/go-pipeline/pkg/pipeline"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/utils/slice"
)

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
