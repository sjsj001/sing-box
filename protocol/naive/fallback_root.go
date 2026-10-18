package naive

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"strings"
)

// webRoot serves the fallback site without letting a request out of it.
// http.Dir follows any symlink and serves any file, and this process may read
// everything (the unit grants CAP_DAC_READ_SEARCH), so one stray link or a
// cloned site's .git would be an unauthenticated read of the machine.
//
// os.Root refuses every absolute symlink, even one that points back inside;
// relative links within the root still work. The root is opened per request,
// so a site missing at startup is a 404 rather than a failure, and a site
// swapped in by rename is picked up.
type webRoot string

func (r webRoot) Open(name string) (fs.File, error) {
	for _, segment := range strings.Split(name, "/") {
		// .well-known is where an ACME webroot challenge lands.
		if strings.HasPrefix(segment, ".") && segment != "." && segment != ".well-known" {
			return nil, fs.ErrNotExist
		}
	}
	root, err := os.OpenRoot(string(r))
	if err != nil {
		return nil, notFound(err)
	}
	defer root.Close()
	file, err := root.Open(name)
	if err != nil {
		return nil, notFound(err)
	}
	// A directory without an index would be listed, which no front-facing
	// server does by default.
	info, err := file.Stat()
	if err == nil && info.IsDir() {
		info, err = root.Stat(path.Join(name, "index.html"))
		if err == nil && info.IsDir() {
			err = fs.ErrNotExist
		}
	}
	if err != nil {
		file.Close()
		return nil, fs.ErrNotExist
	}
	return file, nil
}

// notFound makes a refused path look like a missing one: the escape error is
// neither, and net/http would answer it with a 500.
func notFound(err error) error {
	if errors.Is(err, fs.ErrPermission) {
		return err
	}
	return fs.ErrNotExist
}
