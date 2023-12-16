package scanner2

import (
	"context"
	"io/fs"

	"github.com/charlievieth/fastwalk"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/scanner"
	"github.com/navidrome/navidrome/utils/pl"
)

type scanner2 struct {
	ctx context.Context
	ds  model.DataStore
}

func New(ctx context.Context, ds model.DataStore) scanner.Scanner {
	return &scanner2{ctx: ctx, ds: ds}
}

func (s *scanner2) RescanAll(ctx context.Context, fullRescan bool) error {
	libs, err := s.ds.Library(s.ctx).GetAll()
	if err != nil {
		return err
	}

	libsChan := pl.FromSlice(s.ctx, libs)
	folderChan, folderErrChan := walkDirEntries(s.ctx, libsChan)
	for folder := range folderChan {
		log.Debug(s.ctx, "Scanner: Found folder", "folder", folder.Name())
	}

	// Wait for pipeline to end, return first error found
	for err := range pl.Merge(ctx, folderErrChan) {
		return err
	}
	return nil
}

func walkDirEntries(ctx context.Context, libsChan <-chan model.Library) (chan fastwalk.DirEntry, chan error) {
	outputChannel := make(chan fastwalk.DirEntry)
	errChannel := make(chan error)
	go func() {
		defer close(outputChannel)
		defer close(errChannel)
		errChan := pl.Sink(ctx, 1, libsChan, func(ctx context.Context, lib model.Library) error {
			conf := &fastwalk.Config{Follow: true}
			return fastwalk.Walk(conf, lib.Path, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					log.Error(ctx, "Scanner: Error walking path", "lib", lib.Name, "path", path, err)
				}
				if d.IsDir() {
					outputChannel <- d.(fastwalk.DirEntry)
				}
				return nil
			})
		})

		// Wait for pipeline to end, and forward any errors
		for err := range pl.ReadOrDone(ctx, errChan) {
			errChannel <- err
		}
	}()
	return outputChannel, errChannel
}
func (s *scanner2) Status(context.Context) (*scanner.StatusInfo, error) {
	return nil, nil
}

//nolint:unused
func (s *scanner2) doScan(ctx context.Context, lib model.Library, fullRescan bool, folders <-chan string) error {
	return nil
}

var _ scanner.Scanner = (*scanner2)(nil)
