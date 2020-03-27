// +build windows

package main

import "syscall"

func platform_init() {
  kernel32 := syscall.NewLazyDLL("kernel32.dll")
  attachConsole := kernel32.NewProc("AttachConsole")
  syscall.Syscall(attachConsole.Addr(), 1, uintptr(^uint32(0)), 0, 0)
}
