package fsutil

import (
	"io/ioutil"
)

func ReadAll(fpath string) ([]byte, error) {
	client, err := NewFsClient(fpath)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	reader, err := client.Open(fpath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}
