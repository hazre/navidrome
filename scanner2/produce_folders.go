package scanner2

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/google/go-pipeline/pkg/pipeline"
	"github.com/navidrome/navidrome/consts"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/utils/pl"
	"golang.org/x/exp/maps"
)

func produceFolders(ctx context.Context, ds model.DataStore, libs []model.Library, fullRescan bool) pipeline.ProducerFn[*folderEntry] {
	scanCtxChan := make(chan *scanContext, len(libs))
	go func() {
		defer close(scanCtxChan)
		for _, lib := range libs {
			scanCtx, err := newScannerContext(ctx, ds, lib, fullRescan)
			if err != nil {
				log.Error(ctx, "Scanner: Error creating scan context", "lib", lib.Name, err)
				continue
			}
			scanCtxChan <- scanCtx
		}
	}()
	return func(put func(entry *folderEntry)) error {
		// TODO Parallelize multiple scanCtx
		var total int64
		for scanCtx := range pl.ReadOrDone(ctx, scanCtxChan) {
			outputChan, err := walkDirTree(ctx, scanCtx)
			if err != nil {
				log.Warn(ctx, "Scanner: Error scanning library", "lib", scanCtx.lib.Name, err)
			}
			for folder := range pl.ReadOrDone(ctx, outputChan) {
				put(folder)
			}
			total += scanCtx.numFolders.Load()
		}
		log.Info(ctx, "Scanner: Finished loading all folders", "numFolders", total)
		return nil
	}
}

func walkDirTree(ctx context.Context, scanCtx *scanContext) (<-chan *folderEntry, error) {
	results := make(chan *folderEntry)
	go func() {
		defer close(results)
		rootFolder := scanCtx.lib.Path
		err := walkFolder(ctx, scanCtx, rootFolder, results)
		if err != nil {
			log.Error(ctx, "Scanner: There were errors reading directories from filesystem", "path", rootFolder, err)
			return
		}
		log.Debug(ctx, "Scanner: Finished reading folders", "lib", scanCtx.lib.Name, "path", rootFolder, "numFolders", scanCtx.numFolders.Load())
	}()
	return results, nil
}

func walkFolder(ctx context.Context, scanCtx *scanContext, currentFolder string, results chan<- *folderEntry) error {
	folder, children, err := loadDir(ctx, scanCtx, currentFolder)
	if err != nil {
		log.Warn(ctx, "Scanner: Error loading dir. Skipping", "path", currentFolder, err)
		return nil
	}
	scanCtx.numFolders.Add(1)
	for _, c := range children {
		err := walkFolder(ctx, scanCtx, c, results)
		if err != nil {
			return err
		}
	}

	if !folder.isOutdated() && !scanCtx.fullRescan {
		return nil
	}
	dir := filepath.Clean(currentFolder)
	log.Trace(ctx, "Scanner: Found directory", "_path", dir, "audioFiles", maps.Keys(folder.audioFiles),
		"images", maps.Keys(folder.imageFiles), "playlists", folder.playlists, "imagesUpdatedAt", folder.imagesUpdatedAt,
		"updTime", folder.updTime, "modTime", folder.modTime, "numChildren", len(children))
	folder.path = dir
	results <- folder

	return nil
}

func loadDir(ctx context.Context, scanCtx *scanContext, dirPath string) (folder *folderEntry, children []string, err error) {
	folder = &folderEntry{scanCtx: scanCtx, path: dirPath}
	folder.id = model.FolderID(scanCtx.lib, dirPath)
	folder.updTime = scanCtx.getLastUpdatedInDB(folder.id)
	folder.audioFiles = make(map[string]fs.DirEntry)
	folder.imageFiles = make(map[string]fs.DirEntry)

	dirInfo, err := os.Stat(dirPath)
	if err != nil {
		log.Warn(ctx, "Scanner: Error stating dir", "path", dirPath, err)
		return nil, nil, err
	}
	folder.modTime = dirInfo.ModTime()

	dir, err := os.Open(dirPath)
	if err != nil {
		log.Warn(ctx, "Scanner: Error in Opening directory", "path", dirPath, err)
		return folder, children, err
	}
	defer dir.Close()

	for _, entry := range fullReadDir(ctx, dir) {
		if ctx.Err() != nil {
			return folder, children, ctx.Err()
		}
		isDir, err := isDirOrSymlinkToDir(dirPath, entry)
		// Skip invalid symlinks
		if err != nil {
			log.Warn(ctx, "Scanner: Invalid symlink", "dir", filepath.Join(dirPath, entry.Name()), err)
			continue
		}
		if isDir && !isDirIgnored(dirPath, entry) && isDirReadable(ctx, dirPath, entry) {
			children = append(children, filepath.Join(dirPath, entry.Name()))
		} else {
			fileInfo, err := entry.Info()
			if err != nil {
				log.Warn(ctx, "Scanner: Error getting fileInfo", "name", entry.Name(), err)
				return folder, children, err
			}
			if fileInfo.ModTime().After(folder.modTime) {
				folder.modTime = fileInfo.ModTime()
			}
			switch {
			case model.IsAudioFile(entry.Name()):
				folder.audioFiles[entry.Name()] = entry
			case model.IsValidPlaylist(entry.Name()):
				folder.playlists = append(folder.playlists, entry)
			case model.IsImageFile(entry.Name()):
				folder.imageFiles[entry.Name()] = entry
				if fileInfo.ModTime().After(folder.imagesUpdatedAt) {
					folder.imagesUpdatedAt = fileInfo.ModTime()
				}
			}
		}
	}
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
		if ctx.Err() != nil {
			return []fs.DirEntry{}
		}
		entries, err := dir.ReadDir(-1)
		allEntries = append(allEntries, entries...)
		if err == nil {
			break
		}
		log.Warn(ctx, "Skipping DirEntry", err)
		if prevErrStr == err.Error() {
			log.Error(ctx, "Scanner: Duplicate DirEntry failure, bailing", err)
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
		log.Warn("Scanner: Skipping unreadable directory", "path", path, err)
		return false
	}

	err = dir.Close()
	if err != nil {
		log.Warn(ctx, "Scanner: Error closing directory", "path", path, err)
	}

	return true
}

// isDirIgnored returns true if the directory represented by dirEnt contains an
// `ignore` file (named after skipScanFile)
func isDirIgnored(baseDir string, dirEnt fs.DirEntry) bool {
	// allows Album folders for albums which eg start with ellipses
	name := dirEnt.Name()
	if strings.HasPrefix(name, ".") && !strings.HasPrefix(name, "..") {
		return true
	}

	if runtime.GOOS == "windows" && strings.EqualFold(name, "$RECYCLE.BIN") {
		return true
	}
	_, err := os.Stat(filepath.Join(baseDir, name, consts.SkipScanFile))
	return err == nil
}
