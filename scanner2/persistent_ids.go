package scanner2

import (
	"crypto/md5"
	"fmt"

	"github.com/navidrome/navidrome/scanner/metadata"
	. "github.com/navidrome/navidrome/utils/gg"
)

func artistPID(md metadata.Tags) string {
	key := FirstOr(md.Artist(), "M"+md.MbzArtistID())
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}

func albumArtistPID(md metadata.Tags) string {
	key := FirstOr(md.AlbumArtist(), "M"+md.MbzAlbumArtistID())
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}

func albumPID(md metadata.Tags) string {
	var key string
	if md.MbzAlbumID() != "" {
		key = "M" + md.MbzAlbumID()
	} else {
		key = fmt.Sprintf("%s%s%t", albumArtistPID(md), md.Album(), md.Compilation())
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}

func trackPID(md metadata.Tags) string {
	return fmt.Sprintf("%s%x", albumPID(md), md5.Sum([]byte(md.FilePath())))
}
