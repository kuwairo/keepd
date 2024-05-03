package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/mistifyio/go-zfs/v3"
)

var (
	ErrInvalidProperty       = errors.New("invalid property")
	ErrPermissionDenied      = errors.New("permission denied")
	ErrPoolNotFound          = errors.New("pool not found")
	ErrDatasetNotFound       = errors.New("target dataset not found")
	ErrSnapshotNotFound      = errors.New("target snapshot not found")
	ErrSnapshotAlreadyExists = errors.New("snapshot already exists")
)

func CreateSnapshot(target, prefix, tag string, localTime, recursive bool) error {
	t := time.Now()
	if !localTime {
		t = t.UTC()
	}

	f := t.Format("2006-01-02.15:04:05")
	name := fmt.Sprintf("%s.%s.%s", prefix, f, tag)

	rmark := ""
	if recursive {
		rmark = "[r]"
	}
	log.Printf("[+]%s create snapshot %s@%s\n", rmark, target, name)

	ds := &zfs.Dataset{Name: target}
	_, err := ds.Snapshot(name, recursive)
	if err != nil {
		error := err.Error()
		switch {
		case strings.Contains(error, "dataset does not exist"):
			return errors.Join(ErrDatasetNotFound, err)
		case strings.Contains(error, "dataset already exists"):
			return errors.Join(ErrSnapshotAlreadyExists, err)
		case strings.Contains(error, "permission denied"):
			return errors.Join(ErrPermissionDenied, err)
		default:
			return err
		}
	}

	return nil
}

func DestroySnapshot(target, name string, recursive bool) error {
	rmark := ""
	if recursive {
		rmark = "[r]"
	}
	log.Printf("[-]%s destroy snapshot %s@%s\n", rmark, target, name)

	var f zfs.DestroyFlag
	if recursive {
		f = zfs.DestroyRecursive
	}

	ds := &zfs.Dataset{Name: fmt.Sprintf("%s@%s", target, name)}
	if err := ds.Destroy(f); err != nil {
		error := err.Error()
		switch {
		case strings.Contains(error, "dataset does not exist"):
			return errors.Join(ErrDatasetNotFound, err)
		case strings.Contains(error, "could not find any snapshots to destroy"):
			return errors.Join(ErrSnapshotNotFound, err)
		case strings.Contains(error, "permission denied"):
			return errors.Join(ErrPermissionDenied, err)
		default:
			return err
		}
	}

	return nil
}

func ListSnapshotNames(target string, re *regexp.Regexp) ([][]byte, error) {
	arg := []string{
		"list", "-Hp",
		"-o", "name",
		"-S", "creation",
		"-t", "snapshot",
	}
	if target != "" {
		arg = append(arg, target)
	}
	c := exec.Command("zfs", arg...)

	var stderr bytes.Buffer
	c.Stderr = &stderr

	out, err := c.Output()
	if err != nil {
		if bytes.Contains(stderr.Bytes(), []byte("dataset does not exist")) {
			return nil, errors.Join(ErrDatasetNotFound, err)
		}
		return nil, fmt.Errorf("%w: %s", err, stderr.String())
	}

	return re.FindAll(out, -1), nil
}

func GetPoolProperty(pool, key string) (string, error) {
	arg := []string{
		"get", "-Hp",
		"-o", "value",
		key,
		pool,
	}
	c := exec.Command("zpool", arg...)

	var stderr bytes.Buffer
	c.Stderr = &stderr

	out, err := c.Output()
	if err != nil {
		switch {
		case bytes.Contains(stderr.Bytes(), []byte("missing pool name")):
			return "", errors.Join(ErrPoolNotFound, err)
		case bytes.Contains(stderr.Bytes(), []byte("bad property list")):
			return "", errors.Join(ErrInvalidProperty, err)
		default:
			return "", fmt.Errorf("%w: %s", err, stderr.String())
		}
	}

	return strings.TrimSpace(string(out)), nil
}

func SetPoolProperty(pool, key, value string) error {
	arg := []string{
		"set",
		fmt.Sprintf("%s=%s", key, value),
		pool,
	}
	c := exec.Command("zpool", arg...)

	var stderr bytes.Buffer
	c.Stderr = &stderr

	if _, err := c.Output(); err != nil {
		switch {
		case bytes.Contains(stderr.Bytes(), []byte("is not a pool")):
			return errors.Join(ErrPoolNotFound, err)
		case bytes.Contains(stderr.Bytes(), []byte("invalid property")):
			return errors.Join(ErrInvalidProperty, err)
		case bytes.Contains(stderr.Bytes(), []byte("permission denied")):
			return errors.Join(ErrPermissionDenied, err)
		default:
			return fmt.Errorf("%w: %s", err, stderr.String())
		}
	}

	return nil
}

func ReasonOf(err error) (r string) {
	if err == nil {
		return r
	}

	switch {
	case errors.Is(err, ErrDatasetNotFound):
		r = ErrDatasetNotFound.Error()
	case errors.Is(err, ErrPoolNotFound):
		r = ErrPoolNotFound.Error()
	case errors.Is(err, ErrSnapshotAlreadyExists):
		r = ErrSnapshotAlreadyExists.Error()
	case errors.Is(err, ErrSnapshotNotFound):
		r = ErrSnapshotNotFound.Error()
	case errors.Is(err, ErrInvalidProperty):
		r = ErrInvalidProperty.Error()
	case errors.Is(err, ErrPermissionDenied):
		r = ErrPermissionDenied.Error()
	}

	return r
}
