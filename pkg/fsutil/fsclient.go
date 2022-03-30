package fsutil

import (
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type FsClientFactory func(URL *url.URL) (FsClient, error)

var (
	ErrUnsupportFS = errors.New("unsupport fs scheme")
	ErrEmptyPath   = errors.New("path can't be empty")

	SupportedFS = map[string]FsClientFactory{
		"file": NewLocalFsClient,
	}
)

func Register(scheme string, factoryFunc FsClientFactory) {
	SupportedFS[scheme] = factoryFunc
}

type FsClient interface {
	Open(string) (io.ReadCloser, error)
	Append(string) (io.WriteCloser, error)
	Create(string) (io.WriteCloser, error)
	Close() error
}

func NewFsClient(URL string) (FsClient, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return nil, ErrUnsupportFS
	}

	if factory, ok := SupportedFS[u.Scheme]; !ok {
		return nil, ErrUnsupportFS
	} else {
		return factory(u)
	}
}

type LocalFsClientImpl struct {
	root string
}

func NewLocalFsClient(URL *url.URL) (FsClient, error) {
	if URL.Host != "" {
		return nil, errors.New("local fs not support relative path")
	}
	return &LocalFsClientImpl{root: "/"}, nil
}

func (client *LocalFsClientImpl) getFilePath(fpath string) string {
	return filepath.Join(client.root, strings.TrimPrefix(fpath, "file://"))
}

func (client *LocalFsClientImpl) Open(fpath string) (io.ReadCloser, error) {
	return os.Open(client.getFilePath(fpath))
}

func (client *LocalFsClientImpl) Append(fpath string) (io.WriteCloser, error) {
	return os.OpenFile(client.getFilePath(fpath), os.O_APPEND, 0)
}

func (client *LocalFsClientImpl) Create(fpath string) (io.WriteCloser, error) {
	return os.Create(client.getFilePath(fpath))
}

func (client *LocalFsClientImpl) Close() error {
	return nil
}
