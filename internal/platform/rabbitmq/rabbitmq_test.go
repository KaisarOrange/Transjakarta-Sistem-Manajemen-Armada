package rabbitmq

import (
	"strings"
	"testing"
)

func TestValidateURL(t *testing.T) {
	for _, ok := range []string{"amqp://fleet:fleet@localhost:5672/", "amqps://u:p@broker.example.com/vhost"} {
		if err := ValidateURL(ok); err != nil {
			t.Errorf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "bogus", "http://localhost:5672", "amqp://[::1"} {
		err := ValidateURL(bad)
		if err == nil {
			t.Errorf("%q accepted", bad)
		} else if strings.Contains(err.Error(), bad) && bad != "" {
			t.Errorf("error for %q echoes the input: %v", bad, err)
		}
	}
}
