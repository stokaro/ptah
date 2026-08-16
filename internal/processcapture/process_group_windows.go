//go:build windows

package processcapture

import (
	"errors"
	"fmt"
	"math"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func prepareProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_SUSPENDED}
}

type windowsProcessTree struct {
	job windows.Handle
}

func attachProcessTree(cmd *exec.Cmd) (processTree, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create Job Object: %w", err)
	}
	tree := windowsProcessTree{job: job}

	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	_, err = windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
	if err != nil {
		return nil, tree.closeWithError(fmt.Errorf("configure Job Object: %w", err))
	}

	pid, err := windowsProcessID(cmd.Process.Pid)
	if err != nil {
		return nil, tree.closeWithError(err)
	}
	process, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false,
		pid,
	)
	if err != nil {
		return nil, tree.closeWithError(fmt.Errorf("open process: %w", err))
	}

	assignErr := windows.AssignProcessToJobObject(job, process)
	closeProcessErr := windows.CloseHandle(process)
	if assignErr != nil || closeProcessErr != nil {
		if assignErr != nil {
			assignErr = fmt.Errorf("assign process to Job Object: %w", assignErr)
		}
		return nil, tree.closeWithError(errors.Join(assignErr, closeProcessErr))
	}
	if err := resumeProcessThreads(pid); err != nil {
		return nil, tree.closeWithError(err)
	}

	return tree, nil
}

func resumeProcessThreads(processID uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return fmt.Errorf("snapshot process threads: %w", err)
	}
	defer windows.CloseHandle(snapshot)

	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err := windows.Thread32First(snapshot, &entry); err != nil {
		return fmt.Errorf("find process initial thread: %w", err)
	}
	for {
		if entry.OwnerProcessID == processID {
			thread, openErr := windows.OpenThread(
				windows.THREAD_SUSPEND_RESUME,
				false,
				entry.ThreadID,
			)
			if openErr != nil {
				return fmt.Errorf("open process initial thread: %w", openErr)
			}
			_, resumeErr := windows.ResumeThread(thread)
			closeErr := windows.CloseHandle(thread)
			if resumeErr != nil {
				resumeErr = fmt.Errorf("resume process initial thread: %w", resumeErr)
			}
			return errors.Join(resumeErr, closeErr)
		}
		if err := windows.Thread32Next(snapshot, &entry); err != nil {
			return fmt.Errorf("find process initial thread: %w", err)
		}
	}
}

func (tree windowsProcessTree) terminate() error {
	return windows.TerminateJobObject(tree.job, 1)
}

func (tree windowsProcessTree) close() error {
	return windows.CloseHandle(tree.job)
}

func (tree windowsProcessTree) closeWithError(err error) error {
	return errors.Join(err, tree.terminate(), tree.close())
}

// windowsProcessID narrows a process id to the DWORD the Windows API takes.
//
// os.Process.Pid is an int because the type is shared with platforms whose
// process ids are signed. A Windows process id is a DWORD, so a value outside
// that range did not come from a process this program started, and narrowing
// it silently would name a different process to a call that terminates one.
func windowsProcessID(pid int) (uint32, error) {
	if pid < 0 || int64(pid) > math.MaxUint32 {
		return 0, fmt.Errorf("process id %d is not a Windows process id", pid)
	}
	return uint32(pid), nil
}
