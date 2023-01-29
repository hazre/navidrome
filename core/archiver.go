package core

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/utils/slice"
)

type Archiver interface {
	ZipAlbum(ctx context.Context, id string, format string, bitrate int, w io.Writer) error
	ZipArtist(ctx context.Context, id string, format string, bitrate int, w io.Writer) error
	ZipPlaylist(ctx context.Context, id string, format string, bitrate int, w io.Writer) error
}

func NewArchiver(ms MediaStreamer, ds model.DataStore) Archiver {
	return &archiver{ds: ds, ms: ms}
}

type archiver struct {
	ds model.DataStore
	ms MediaStreamer
}

func (a *archiver) ZipAlbum(ctx context.Context, id string, format string, bitrate int, out io.Writer) error {
	mfs, err := a.ds.MediaFile(ctx).GetAll(model.QueryOptions{
		Filters: squirrel.Eq{"album_id": id},
		Sort:    "album",
	})
	if err != nil {
		log.Error(ctx, "Error loading mediafiles from album", "id", id, err)
		return err
	}
	return a.zipAlbums(ctx, id, format, bitrate, out, mfs)
}

func (a *archiver) ZipArtist(ctx context.Context, id string, format string, bitrate int, out io.Writer) error {
	mfs, err := a.ds.MediaFile(ctx).GetAll(model.QueryOptions{
		Filters: squirrel.Eq{"album_artist_id": id},
		Sort:    "album",
	})
	if err != nil {
		log.Error(ctx, "Error loading mediafiles from artist", "id", id, err)
		return err
	}
	return a.zipAlbums(ctx, id, format, bitrate, out, mfs)
}

func (a *archiver) zipAlbums(ctx context.Context, id string, format string, bitrate int, out io.Writer, mfs model.MediaFiles) error {
	z := zip.NewWriter(out)
	albums := slice.Group(mfs, func(mf model.MediaFile) string {
		return mf.AlbumID
	})
	for _, album := range albums {
		discs := slice.Group(album, func(mf model.MediaFile) int { return mf.DiscNumber })
		isMultDisc := len(discs) > 1
		log.Debug(ctx, "Zipping album", "name", album[0].Album, "artist", album[0].AlbumArtist,
			"format", format, "bitrate", bitrate, "isMultDisc", isMultDisc, "numTracks", len(album))
		for _, mf := range album {
			file := a.albumFilename(mf, format, isMultDisc)
			_ = a.addFileToZip(ctx, z, mf, format, bitrate, file)
		}
	}
	err := z.Close()
	if err != nil {
		log.Error(ctx, "Error closing zip file", "id", id, err)
	}
	return err
}

func (a *archiver) albumFilename(mf model.MediaFile, format string, isMultDisc bool) string {
	_, file := filepath.Split(mf.Path)
	if format != "raw" {
		file = strings.TrimSuffix(file, mf.Suffix) + format
	}
	if isMultDisc {
		file = fmt.Sprintf("Disc %02d/%s", mf.DiscNumber, file)
	}
	return fmt.Sprintf("%s/%s", mf.Album, file)
}

func (a *archiver) ZipPlaylist(ctx context.Context, id string, format string, bitrate int, out io.Writer) error {
	pls, err := a.ds.Playlist(ctx).GetWithTracks(id, true)
	if err != nil {
		log.Error(ctx, "Error loading mediafiles from playlist", "id", id, err)
		return err
	}
	return a.zipPlaylist(ctx, id, format, bitrate, out, pls)
}

func (a *archiver) zipPlaylist(ctx context.Context, id string, format string, bitrate int, out io.Writer, pls *model.Playlist) error {
	mfs := pls.MediaFiles()
	z := zip.NewWriter(out)
	log.Debug(ctx, "Zipping playlist", "name", pls.Name, "format", format, "bitrate", bitrate, "numTracks", len(mfs))
	for idx, mf := range mfs {
		file := a.playlistFilename(mf, format, idx)
		_ = a.addFileToZip(ctx, z, mf, format, bitrate, file)
	}
	err := z.Close()
	if err != nil {
		log.Error(ctx, "Error closing zip file", "id", id, err)
	}
	return err
}

func (a *archiver) playlistFilename(mf model.MediaFile, format string, idx int) string {
	ext := mf.Suffix
	if format != "raw" {
		ext = format
	}
	file := fmt.Sprintf("%02d - %s - %s.%s", idx+1, mf.Artist, mf.Title, ext)
	return file
}

func (a *archiver) addFileToZip(ctx context.Context, z *zip.Writer, mf model.MediaFile, format string, bitrate int, filename string) error {
	w, err := z.CreateHeader(&zip.FileHeader{
		Name:     filename,
		Modified: mf.UpdatedAt,
		Method:   zip.Store,
	})
	if err != nil {
		log.Error(ctx, "Error creating zip entry", "file", mf.Path, err)
		return err
	}

	var r io.ReadCloser
	if format != "raw" {
		r, err = a.ms.DoStream(ctx, &mf, format, bitrate)
	} else {
		r, err = os.Open(mf.Path)
	}
	if err != nil {
		log.Error(ctx, "Error opening file for zipping", "file", mf.Path, "format", format, err)
		return err
	}

	defer func() {
		if err := r.Close(); err != nil && log.CurrentLevel() >= log.LevelDebug {
			log.Error("Error closing stream", "id", mf.ID, "file", mf.Path, err)
		}
	}()

	_, err = io.Copy(w, r)
	if err != nil {
		log.Error(ctx, "Error zipping file", "file", mf.Path, err)
		return err
	}

	return nil
}
