package naive

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/auth"

	"github.com/stretchr/testify/require"
)

func newInboundWithUsers(users ...auth.User) error {
	_, err := NewInbound(context.Background(), nil, log.NewNOPFactory().Logger(), "", option.NaiveInboundOptions{
		Users:   users,
		Network: "tcp",
	})
	return err
}

// A user with neither name nor password is "Basic Og==", which anyone can send.
func TestAUserAnyoneCanBeIsRejected(t *testing.T) {
	require.ErrorContains(t, newInboundWithUsers(auth.User{}), "missing username and password")
	require.ErrorContains(t, newInboundWithUsers(auth.User{Username: "user", Password: "pass"}, auth.User{}), "missing username and password")
}

// naiveproxy itself accepts either one empty, so that stays a valid deployment.
func TestAUserWithOnlyANameOrOnlyAPasswordIsAccepted(t *testing.T) {
	require.NoError(t, newInboundWithUsers(auth.User{Username: "user"}))
	require.NoError(t, newInboundWithUsers(auth.User{Password: "pass"}))
}
