package runtime

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

func parentPID() (int, error) {
	pid, err := strconv.Atoi(os.Getenv(parentEnvironment))
	if err != nil || pid <= 1 || uint64(pid) > math.MaxUint32 || pid == os.Getpid() {
		return 0, fmt.Errorf("provider must be started by an authorized host")
	}
	return pid, nil
}
