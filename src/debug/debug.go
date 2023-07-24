package debugging

import "fmt"

const (
	_                    = iota
	DEBUG_NONE       int = 1
	DEBUG_ATTRIBUTES int = 2
	DEBUG_EVENTS     int = 3
	DEBUG_OUTPUT     int = 4
	DEBUG_ALL        int = 5
)

var Debuglevel int = DEBUG_NONE

func Setdebug(level int) {
	Debuglevel = level
}

func Getdebug() int {
	return Debuglevel
}

func Debug(level int, format string, a ...interface{}) {
	if level == Debuglevel {
		fmt.Printf(format, a...)
	}
}
