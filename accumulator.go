package archiver

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Accumulator map[string]*File

func (a Accumulator) Accumulate(key string, value []byte) (err error) {
	file, ok := a[key]
	if !ok {
		file, err = newFile()
		if err != nil {
			return err
		}
		a[key] = file
	}

	return file.Append(line)
}

func (a Accumulate) Close() (err error) {
	for _, file := range a {
		err = file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

type File struct {
	f     *os.File
	count int64
}

func newFile() (*File, error) {
	f, err := ioutil.NewFile("", "")
	return File{f: f}, err
}

func (f *File) Append(line []byte) error {
	_, err := fmt.Fprintf(f.f, line)
	f.count++
	return err
}

func (f *File) Close() error {
	return f.f.Close()
}
