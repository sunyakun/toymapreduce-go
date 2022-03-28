package fs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrUnsupportFS = errors.New("unsupport fs scheme")
)

func NewFs(fpath string) (fs.FS, error) {
	if !strings.Contains(fpath, "://") {
		return os.DirFS(filepath.Dir(fpath)), nil
	} else {
		return nil, ErrUnsupportFS
	}
}
