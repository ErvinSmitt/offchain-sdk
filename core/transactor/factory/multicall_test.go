package factory_test

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/berachain/offchain-sdk/client/eth"
	"github.com/berachain/offchain-sdk/contracts/bindings"
	"github.com/berachain/offchain-sdk/core/transactor/factory"
	"github.com/berachain/offchain-sdk/core/transactor/types"
	"github.com/berachain/offchain-sdk/log"
	sdk "github.com/berachain/offchain-sdk/types"
	"github.com/stretchr/testify/assert"

	"github.com/ethereum/go-ethereum/common"
	ethclient "github.com/ethereum/go-ethereum/ethclient"
)

// TODO: read in from config or input.
var (
	multicallAddr = common.HexToAddress("0x9d1dB8253105b007DDDE65Ce262f701814B91125")
	erc20Addr     = common.HexToAddress("0x7EeCA4205fF31f947EdBd49195a7A88E6A91161B")
	from          = common.Address{}
	ethHTTPURL    = "http://localhost:8545"
)

// TestMulticall demonstrates how to use the multicall contract to batch multiple calls to other
// contracts on a Ethereum blockchain.
func TestMulticall(t *testing.T) {
	// setup eth client and multicaller
	ctx := context.Background()
	chain, err := ethclient.DialContext(ctx, ethHTTPURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = chain.ChainID(ctx); err != nil {
		if assert.ErrorContains(t, err, "connection refused") {
			t.Skipf("Skipping test: %s", err)
		}
		t.Fatal(err)
	}
	sCtx := sdk.NewContext(
		ctx, eth.NewExtendedEthClient(chain, 5*time.Second),
		log.NewLogger(os.Stdout, "test-runner"), nil,
	)
	multicaller := factory.NewMulticall3Batcher(multicallAddr)

	// set up some test calls to make
	mcPacker := types.Packer{MetaData: bindings.Multicall3MetaData}
	call1, err := mcPacker.CreateRequest("", multicallAddr, nil, nil, nil, 0, "getBlockNumber")
	if err != nil {
		t.Fatal(err)
	}
	erc20Packer := types.Packer{MetaData: bindings.IERC20MetaData}
	call2, err := erc20Packer.CreateRequest("", erc20Addr, nil, nil, nil, 0, "balanceOf", from)
	if err != nil {
		t.Fatal(err)
	}

	// batch and send the calls to the chain
	responses, err := multicaller.BatchCallRequests(sCtx, from, call1.CallMsg, call2.CallMsg)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	assert.True(t, responses[0].Success)
	assert.True(t, responses[1].Success)

	// unpack the first response
	ret1, err := mcPacker.GetCallResult("getBlockNumber", responses[0].ReturnData)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(ret1))
	assert.Less(t, uint64(0), ret1[0].(*big.Int).Uint64())

	// unpack the second response
	ret2, err := erc20Packer.GetCallResult("balanceOf", responses[1].ReturnData)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(ret2))
	assert.Equal(t, uint64(0), ret2[0].(*big.Int).Uint64())
}
