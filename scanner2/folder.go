package scanner2

import (
	"io/fs"
	"time"
)

type folderEntry struct {
	scanCtx         *scanContext
	path            string    // Full path
	id              string    // DB ID
	updTime         time.Time // From DB
	modTime         time.Time // From FS
	audioFiles      map[string]fs.DirEntry
	imageFiles      map[string]fs.DirEntry
	playlists       []fs.DirEntry
	imagesUpdatedAt time.Time
}

func (f *folderEntry) isExpired() bool {
	return f.updTime.Before(f.modTime)
}
