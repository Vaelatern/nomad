package nomad

import (
	"testing"

	msgpackrpc "github.com/hashicorp/net-rpc-msgpackrpc"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/nomad/ci"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/testutil"
)

// TestEncrypter_LoadSave exercises loading and saving keys in the keystore
func TestEncrypter_LoadSave(t *testing.T) {

	ci.Parallel(t)

	// use a known tempdir so that we can restore from it
	tmpDir := t.TempDir()

	srv, rootToken, shutdown := TestACLServer(t, func(c *Config) {
		c.NumSchedulers = 0 // Prevent automatic dequeue
		c.DataDir = tmpDir
	})
	defer shutdown()
	testutil.WaitForLeader(t, srv.RPC)
	codec := rpcClient(t, srv)

	// Send a few key rotations so that we add keys

	rotateReq := &structs.KeyringRotateRootKeyRequest{
		WriteRequest: structs.WriteRequest{
			Region:    "global",
			AuthToken: rootToken.SecretID,
		},
	}
	var rotateResp structs.KeyringRotateRootKeyResponse
	for i := 0; i < 4; i++ {
		err := msgpackrpc.CallWithCodec(codec, "Keyring.Rotate", rotateReq, &rotateResp)
		require.NoError(t, err)
	}

	shutdown()

	srv, rootToken, shutdown2 := TestACLServer(t, func(c *Config) {
		c.NumSchedulers = 0
		c.DataDir = tmpDir
	})
	defer shutdown2()
	testutil.WaitForLeader(t, srv.RPC)
	codec = rpcClient(t, srv)

	// Verify we've restored all the keys from the old keystore

	listReq := &structs.KeyringListRootKeyMetaRequest{
		QueryOptions: structs.QueryOptions{
			Region: "global",
		},
	}
	var listResp structs.KeyringListRootKeyMetaResponse
	err := msgpackrpc.CallWithCodec(codec, "Keyring.List", listReq, &listResp)
	require.NoError(t, err)
	require.Len(t, listResp.Keys, 4)
}
