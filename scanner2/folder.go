package scanner2

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/charlievieth/fastwalk"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"golang.org/x/exp/slices"
)

type folderEntry struct {
	fastwalk.DirEntry
	scanCtx         *scanContext
	path            string    // Full path
	id              string    // DB ID
	updTime         time.Time // From DB
	modTime         time.Time // From FS
	audioFiles      []fs.DirEntry
	imageFiles      []fs.DirEntry
	playlists       []fs.DirEntry
	imagesUpdatedAt time.Time
}

func (f *folderEntry) isExpired() bool {
	return f.updTime.Before(f.modTime)
}

func loadDir(ctx context.Context, scanCtx *scanContext, dirPath string, d fastwalk.DirEntry) (folder *folderEntry, children []string, err error) {
	folder = &folderEntry{DirEntry: d, scanCtx: scanCtx, path: dirPath}
	folder.id = model.FolderID(scanCtx.lib, dirPath)
	folder.updTime = scanCtx.getLastUpdatedInDB(folder.id)

	dirInfo, err := d.Stat()
	if err != nil {
		log.Error(ctx, "Error stating dir", "path", dirPath, err)
		return nil, nil, err
	}
	folder.modTime = dirInfo.ModTime()

	dir, err := os.Open(dirPath)
	if err != nil {
		log.Error(ctx, "Error in Opening directory", "path", dirPath, err)
		return folder, children, err
	}
	defer dir.Close()

	for _, entry := range fullReadDir(ctx, dir) {
		isDir, err := isDirOrSymlinkToDir(dirPath, entry)
		// Skip invalid symlinks
		if err != nil {
			log.Error(ctx, "Invalid symlink", "dir", filepath.Join(dirPath, entry.Name()), err)
			continue
		}
		if isDir && isDirReadable(ctx, dirPath, entry) {
			children = append(children, filepath.Join(dirPath, entry.Name()))
		} else {
			fileInfo, err := entry.Info()
			if err != nil {
				log.Error(ctx, "Error getting fileInfo", "name", entry.Name(), err)
				return folder, children, err
			}
			if fileInfo.ModTime().After(folder.modTime) {
				folder.modTime = fileInfo.ModTime()
			}
			switch {
			case model.IsAudioFile(entry.Name()):
				folder.audioFiles = append(folder.audioFiles, entry)
			case model.IsValidPlaylist(entry.Name()):
				folder.playlists = append(folder.playlists, entry)
			case model.IsImageFile(entry.Name()):
				folder.imageFiles = append(folder.imageFiles, entry)
				if fileInfo.ModTime().After(folder.imagesUpdatedAt) {
					folder.imagesUpdatedAt = fileInfo.ModTime()
				}
			}
		}
	}
	slices.SortFunc(folder.audioFiles, func(i, j fs.DirEntry) bool { return i.Name() < j.Name() })
	slices.SortFunc(folder.imageFiles, func(i, j fs.DirEntry) bool { return i.Name() < j.Name() })
	slices.SortFunc(folder.playlists, func(i, j fs.DirEntry) bool { return i.Name() < j.Name() })
	return folder, children, nil
}

// fullReadDir reads all files in the folder, skipping the ones with errors.
// It also detects when it is "stuck" with an error in the same directory over and over.
// In this case, it stops and returns whatever it was able to read until it got stuck.
// See discussion here: https://github.com/navidrome/navidrome/issues/1164#issuecomment-881922850
func fullReadDir(ctx context.Context, dir fs.ReadDirFile) []fs.DirEntry {
	var allEntries []fs.DirEntry
	var prevErrStr = ""
	for {
		entries, err := dir.ReadDir(-1)
		allEntries = append(allEntries, entries...)
		if err == nil {
			break
		}
		log.Warn(ctx, "Skipping DirEntry", err)
		if prevErrStr == err.Error() {
			log.Error(ctx, "Duplicate DirEntry failure, bailing", err)
			break
		}
		prevErrStr = err.Error()
	}
	sort.Slice(allEntries, func(i, j int) bool { return allEntries[i].Name() < allEntries[j].Name() })
	return allEntries
}

// isDirOrSymlinkToDir returns true if and only if the dirEnt represents a file
// system directory, or a symbolic link to a directory. Note that if the dirEnt
// is not a directory but is a symbolic link, this method will resolve by
// sending a request to the operating system to follow the symbolic link.
// originally copied from github.com/karrick/godirwalk, modified to use dirEntry for
// efficiency for go 1.16 and beyond
func isDirOrSymlinkToDir(baseDir string, dirEnt fs.DirEntry) (bool, error) {
	if dirEnt.IsDir() {
		return true, nil
	}
	if dirEnt.Type()&os.ModeSymlink == 0 {
		return false, nil
	}
	// Does this symlink point to a directory?
	fileInfo, err := os.Stat(filepath.Join(baseDir, dirEnt.Name()))
	if err != nil {
		return false, err
	}
	return fileInfo.IsDir(), nil
}

// isDirReadable returns true if the directory represented by dirEnt is readable
func isDirReadable(ctx context.Context, baseDir string, dirEnt fs.DirEntry) bool {
	path := filepath.Join(baseDir, dirEnt.Name())

	dir, err := os.Open(path)
	if err != nil {
		log.Warn("Skipping unreadable directory", "path", path, err)
		return false
	}

	err = dir.Close()
	if err != nil {
		log.Warn(ctx, "Error closing directory", "path", path, err)
	}

	return true
}
