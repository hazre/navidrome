package model

import (
	"io/fs"
	"os"
	"time"
)

type Library struct {
	ID         int32
	Name       string
	Path       string
	RemotePath string
	LastScanAt time.Time
	UpdatedAt  time.Time
	CreatedAt  time.Time
}

func (f Library) FS() fs.FS {
	return os.DirFS(f.Path)
}

type Libraries []Library

type LibraryRepository interface {
	Get(id int32) (*Library, error)
	GetAll() (Libraries, error)
}
