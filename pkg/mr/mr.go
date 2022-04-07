package mr

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var (
	ErrStopIter = errors.New("there have no more data")
)

type Iterator interface {
	Next() (any, error)
}

type LineIterator struct {
	reader *bufio.Reader
}

func NewLineIterator(reader io.Reader) *LineIterator {
	return &LineIterator{reader: bufio.NewReader(reader)}
}

func (iter *LineIterator) Next() (interface{}, error) {
	line, err := iter.reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			return nil, ErrStopIter
		}
		return nil, err
	}
	return string(bytes.TrimRight(line, "\n")), nil
}

type KVIterator struct {
	reader   *bufio.Reader
	lastKV   *KeyValue
	overflow *KeyValue
	hasNext  bool
	stopIter bool
}

func NewKVIterator(reader io.Reader) *KVIterator {
	return &KVIterator{reader: bufio.NewReader(reader), hasNext: true}
}

func (iter *KVIterator) Next() (interface{}, error) {
	if iter.stopIter {
		return nil, ErrStopIter
	}

	if iter.overflow != nil {
		iter.lastKV, iter.overflow = iter.overflow, nil
		return *iter.lastKV, nil
	}

	line, err := iter.reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			iter.hasNext = false
			iter.stopIter = true
			return nil, ErrStopIter
		}
		return nil, err
	}
	kv := &KeyValue{}
	if err := json.Unmarshal(bytes.TrimRight(line, "\n"), kv); err != nil {
		return nil, err
	}

	if iter.lastKV != nil && iter.lastKV.Key != kv.Key {
		iter.lastKV, iter.overflow, iter.stopIter = nil, kv, true
		return nil, ErrStopIter
	}

	iter.lastKV = kv
	return *kv, nil
}

func (iter *KVIterator) NextKey() bool {
	if iter.hasNext {
		iter.stopIter = false
		return true
	}
	return false
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MapFunc func(filename string, content string) ([]KeyValue, error)

type ReduceFunc func(valueIter Iterator) (Iterator, error)
