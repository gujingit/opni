package test

import (
	"os"
	"os/exec"
	"os/signal"

	"github.com/kralicky/spellbook/build"
	"github.com/kralicky/spellbook/testbin"
	"github.com/magefile/mage/mg"
)

func Env() {
	mg.Deps(testbin.Testbin, build.Build)
	cmd := exec.Command("bin/testenv")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	proc := cmd.Process
	go func() {
		<-sigint
		proc.Signal(os.Interrupt)
	}()
	if err := cmd.Wait(); err != nil {
		panic(err)
	}
}
