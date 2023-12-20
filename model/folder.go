package model

import (
	"crypto/md5"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type Folder struct {
	ID        string
	LibraryID int
	Path      string
	Name      string
	ParentID  string
	UpdateAt  time.Time
	CreatedAt time.Time
}

func FolderID(lib Library, path string) string {
	path = strings.TrimPrefix(path, lib.Path)
	key := fmt.Sprintf("%d:%s", lib.ID, path)
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}
func NewFolder(lib Library, path string) *Folder {
	id := FolderID(lib, path)
	dir, name := filepath.Split(path)
	return &Folder{
		ID:   id,
		Path: dir,
		Name: name,
	}
}

type FolderRepository interface {
	Get(lib Library, path string) (*Folder, error)
	GetAll(lib Library) ([]Folder, error)
	GetLastUpdates(lib Library) (map[string]time.Time, error)
	Put(lib Library, path string) error
	Touch(lib Library, path string, t time.Time) error
}
