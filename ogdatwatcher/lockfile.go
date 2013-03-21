package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const lockfilename = ".lock.pid"

func createlockfile(filename string) *os.File {
	lockfile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, os.FileMode(0666))
	if lockfile == nil || err != nil {
		fmt.Printf("Could not create lock file %s. Probably an instance of %s is running?\n", lockfilename, filepath.Base(os.Args[0]))
		logger.Panicln("Fatal: Lockfile creation error")
	}
	logger.Println("Info: Lockfile successfully created")
	return lockfile
}

func deletelockfile(lockfile *os.File) {
	filename := lockfile.Name()
	if err := lockfile.Close(); err != nil { // Windows want's it's file closed before unlinking
		logger.Panicln("Fatal: Can not close lockfile")
	}
	if err := os.Remove(filename); err != nil {
		logger.Panicln("Fatal: Can not delete lockfile")
	}
	logger.Println("Info: Lockfile successfully deleted")
}

func writeinfotolockfile(lockfile *os.File) {
	if err := lockfile.Truncate(0); err != nil {
		logger.Panicln("Fatal: Can not truncate lockfile")
	}
	if _, err := lockfile.Seek(0, 0); err != nil {
		logger.Panicln("Fatal: Can not seek in lockfile")
	}
	if _, err := lockfile.WriteString(fmt.Sprintf("%d:%s", os.Getpid(), time.Now())); err != nil {
		logger.Panicln("Fatal: Can not write to lockfile")
	}
	logger.Println("Info: Lockfile successfully written")
}
