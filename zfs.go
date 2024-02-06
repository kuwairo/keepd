package main

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/mistifyio/go-zfs/v3"
)

var (
	ErrPoolNotFound    = errors.New("pool not found")
	ErrInvalidProperty = errors.New("invalid property")
)

// TODO: use UTC for snapshot names
// TODO: should check for 'permission denied'?
func CreateSnapshot(target, prefix, tag string) error {
	t := time.Now().Format("2006-01-02.15:04:05")
	name := fmt.Sprintf("%s.%s.%s", prefix, t, tag)

	ds := &zfs.Dataset{Name: target}
	_, err := ds.Snapshot(name, false)
	return err
}

// TODO: should check for 'permission denied'?
func DestroySnapshot(target, name string) error {
	ds := &zfs.Dataset{Name: fmt.Sprintf("%s@%s", target, name)}
	return ds.Destroy(0)
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
		if bytes.Contains(stderr.Bytes(), []byte("missing pool name")) {
			return "", errors.Join(ErrPoolNotFound, err)
		}
		if bytes.Contains(stderr.Bytes(), []byte("bad property list")) {
			return "", errors.Join(ErrInvalidProperty, err)
		}
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}

	return strings.TrimSpace(string(out)), nil
}

// TODO: should check for 'permission denied'?
func SetPoolProperty(pool, key, value string) error {
	arg := []string{
		"set",
		fmt.Sprintf("%s=%q", key, value),
		pool,
	}
	c := exec.Command("zpool", arg...)

	var stderr bytes.Buffer
	c.Stderr = &stderr

	if _, err := c.Output(); err != nil {
		if bytes.Contains(stderr.Bytes(), []byte("is not a pool")) {
			return errors.Join(ErrPoolNotFound, err)
		}
		if bytes.Contains(stderr.Bytes(), []byte("invalid property")) {
			return errors.Join(ErrInvalidProperty, err)
		}
		return fmt.Errorf("%w: %s", err, stderr.String())
	}

	return nil
}
