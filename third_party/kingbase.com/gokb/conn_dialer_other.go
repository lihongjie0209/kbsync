//go:build !linux && !windows

package gokb

import (
	"net"
	"time"
)

// CreateDialer uses portable keepalive settings on systems that do not expose
// Linux-specific TCP_KEEPCNT and TCP_USER_TIMEOUT socket options.
func CreateDialer(timeout timeoutParams) net.Dialer {
	return net.Dialer{
		Timeout:   time.Duration(timeout.connect_timeout) * time.Second,
		KeepAlive: time.Duration(timeout.keepalive_interval) * time.Second,
	}
}
