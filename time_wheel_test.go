package TimeWheel

import (
	"TimeWheel/time_wheel"
	"testing"
	"time"
)

func Test_timeWheel(t *testing.T) {
	timeWheel := time_wheel.NewTimeWheel(10, 500*time.Millisecond)
	defer timeWheel.Stop()

	timeWheel.AddTask(time.Duration(time.Second), func() {
		t.Logf("test1, %v", time.Now())
	}, "test1")
	timeWheel.AddTask(time.Duration(4*time.Second), func() {
		t.Logf("test2, %v", time.Now())
	}, "test2")
	timeWheel.AddTask(time.Duration(5*time.Second), func() {
		t.Logf("test2, %v", time.Now())
	}, "test2")

	<-time.After(6 * time.Second)
}
