package transactor

import (
	"context"
	"sync"
	"time"

	"github.com/berachain/offchain-sdk/core/transactor/event"
	"github.com/berachain/offchain-sdk/core/transactor/factory"
	"github.com/berachain/offchain-sdk/core/transactor/sender"
	"github.com/berachain/offchain-sdk/core/transactor/tracker"
	"github.com/berachain/offchain-sdk/core/transactor/types"
	"github.com/berachain/offchain-sdk/log"
	sdk "github.com/berachain/offchain-sdk/types"
	kmstypes "github.com/berachain/offchain-sdk/types/kms/types"
	"github.com/berachain/offchain-sdk/types/queue/mem"
	"github.com/berachain/offchain-sdk/types/queue/sqs"
	queuetypes "github.com/berachain/offchain-sdk/types/queue/types"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

// TxrV2 is the main transactor object. TODO: deprecate off being a job.
type TxrV2 struct {
	cfg    Config
	logger log.Logger

	requests   queuetypes.Queue[*types.Request]
	factory    *factory.Factory
	noncer     *tracker.Noncer
	sender     *sender.Sender
	senderMu   sync.Mutex
	dispatcher *event.Dispatcher[*tracker.Response]
	tracker    *tracker.Tracker

	preconfirmedStates map[string]types.PreconfirmedState
	preconfirmedMu     sync.RWMutex
}

// NewTransactor creates a new transactor with the given config and signer.
func NewTransactor(cfg Config, signer kmstypes.TxSigner) (*TxrV2, error) {
	var queue queuetypes.Queue[*types.Request]
	if cfg.SQS.QueueURL != "" {
		var err error
		if queue, err = sqs.NewQueueFromConfig[*types.Request](cfg.SQS); err != nil {
			return nil, err
		}
	} else {
		queue = mem.NewQueue[*types.Request]()
	}

	noncer := tracker.NewNoncer(signer.Address(), cfg.PendingNonceInterval)
	factory := factory.New(
		noncer, signer,
		factory.NewMulticall3Batcher(common.HexToAddress(cfg.Multicall3Address)),
	)
	dispatcher := event.NewDispatcher[*tracker.Response]()
	tracker := tracker.New(noncer, dispatcher, cfg.InMempoolTimeout, cfg.TxReceiptTimeout)

	return &TxrV2{
		cfg:                cfg,
		requests:           queue,
		factory:            factory,
		noncer:             noncer,
		sender:             sender.New(factory),
		dispatcher:         dispatcher,
		tracker:            tracker,
		preconfirmedStates: make(map[string]types.PreconfirmedState),
	}, nil
}

// RegistryKey implements job.Basic.
func (t *TxrV2) RegistryKey() string {
	return "transactor"
}

// Setup implements job.HasSetup.
func (t *TxrV2) Setup(ctx context.Context) error {
	sCtx := sdk.UnwrapContext(ctx)
	chain := sCtx.Chain()
	t.logger = sCtx.Logger()

	// Register the transactor as a subscriber to the tracker.
	ch := make(chan *tracker.Response)
	go func() {
		subCtx, cancel := context.WithCancel(ctx)
		_ = tracker.NewSubscription(t, t.logger).Start(subCtx, ch) // TODO: handle error
		cancel()
	}()
	t.dispatcher.Subscribe(ch)

	// Setup and start all the transactor components.
	t.factory.SetClient(chain)
	t.sender.Setup(chain, t.logger)
	t.tracker.SetClient(chain)
	t.noncer.Start(ctx, chain)
	go t.mainLoop(ctx)

	return nil
}

// Execute implements job.Basic.
func (t *TxrV2) Execute(_ context.Context, _ any) (any, error) {
	acquired, inFlight := t.noncer.Stats()
	t.logger.Info(
		"🧠 system status",
		"waiting-tx", acquired, "in-flight-tx", inFlight, "pending-requests", t.requests.Len(),
	)
	return nil, nil //nolint:nilnil // its okay.
}

// IntervalTime implements job.Polling.
func (t *TxrV2) IntervalTime(context.Context) time.Duration {
	return t.cfg.StatusUpdateInterval
}

// SubscribeTxResults sends the tx results, once confirmed, to the given subscriber.
func (t *TxrV2) SubscribeTxResults(ctx context.Context, subscriber tracker.Subscriber) {
	ch := make(chan *tracker.Response)
	go func() {
		subCtx, cancel := context.WithCancel(ctx)
		_ = tracker.NewSubscription(subscriber, t.logger).Start(subCtx, ch) // TODO: handle error
		cancel()
	}()
	t.dispatcher.Subscribe(ch)
}

// SendTxRequest adds the given tx request to the tx queue, after validating it.
func (t *TxrV2) SendTxRequest(txReq *types.Request) (string, error) {
	if err := txReq.Validate(); err != nil {
		return "", err
	}

	msgID := txReq.MsgID
	queueID, err := t.requests.Push(txReq)
	if err != nil {
		return "", err
	}
	if t.cfg.UseQueueMessageID {
		msgID = queueID
	}

	t.markState(types.StateQueued, msgID)
	return msgID, nil
}

// GetPreconfirmedState returns the status of the given message ID before it has been confirmed by
// the chain.
func (t *TxrV2) GetPreconfirmedState(msgID string) types.PreconfirmedState {
	t.preconfirmedMu.RLock()
	defer t.preconfirmedMu.RUnlock()

	return t.preconfirmedStates[msgID]
}

// mainLoop is the main transaction sending / batching loop.
func (t *TxrV2) mainLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Attempt the retrieve a batch from the queue.
			requests := t.retrieveBatch(ctx)
			if len(requests) == 0 {
				// We didn't get any transactions, so we wait for more.
				t.logger.Info("no tx requests to process....")
				time.Sleep(t.cfg.EmptyQueueDelay)
				continue
			}

			// We got a batch, so we can build and fire, after the previous fire has finished.
			t.senderMu.Lock()
			go func() {
				defer t.senderMu.Unlock()

				t.fire(
					ctx,
					&tracker.Response{MsgIDs: requests.MsgIDs(), InitialTimes: requests.Times()},
					true, requests.Messages()...,
				)
			}()
		}
	}
}

// retrieveBatch retrieves a batch of transaction requests from the queue. It waits until 1) it
// hits the batch timeout or 2) tx batch size is reached only if waitFullBatchTimeout is false.
func (t *TxrV2) retrieveBatch(ctx context.Context) types.Requests {
	var (
		requests types.Requests
		timer    = time.NewTimer(t.cfg.TxBatchTimeout)
	)
	defer timer.Stop()

	// Loop until the batch tx timeout expires.
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			return requests
		default:
			txsRemaining := t.cfg.TxBatchSize - len(requests)

			// If we reached max batch size, we can break out of the loop.
			if txsRemaining == 0 {
				// Wait until the timer hits if we want to wait for the full batch timeout.
				if t.cfg.WaitFullBatchTimeout {
					<-timer.C
				}
				return requests
			}

			// Get at most txsRemaining tx requests from the queue.
			msgIDs, txReqs, err := t.requests.ReceiveMany(int32(txsRemaining))
			if err != nil {
				t.logger.Error("failed to receive tx request", "err", err)
				continue
			}

			// Update the batched tx requests.
			for i, txReq := range txReqs {
				if t.cfg.UseQueueMessageID {
					txReq.MsgID = msgIDs[i]
				}
				requests = append(requests, txReq)
			}
		}
	}
}

// fire processes the tracked tx response. If requested to build, it will first batch the messages.
// Then it sends the batch as one tx and asynchronously tracks the tx for its status.
// NOTE: if toBuild is false, resp.Transaction must be a valid, non-nil tx.
func (t *TxrV2) fire(
	ctx context.Context, resp *tracker.Response, toBuild bool, msgs ...*ethereum.CallMsg,
) {
	defer func() {
		// If there was an error in building or sending the tx, let the subscribers know.
		if resp.Status() == tracker.StatusError {
			t.dispatcher.Dispatch(resp)
		}
	}()

	if toBuild {
		// Call the factory to build the (batched) transaction.
		t.markState(types.StateBuilding, resp.MsgIDs...)
		resp.Transaction, resp.Error = t.factory.BuildTransactionFromRequests(ctx, msgs...)
		if resp.Error != nil {
			return
		}
	}

	// Call the sender to send the transaction to the chain.
	t.markState(types.StateSending, resp.MsgIDs...)
	if resp.Error = t.sender.SendTransaction(ctx, resp.Transaction); resp.Error != nil {
		return
	}
	t.logger.Debug("📡 sent transaction", "hash", resp.Hash().Hex(), "reqs", len(resp.MsgIDs))

	// Call the tracker to track the transaction async.
	t.markState(types.StateInFlight, resp.MsgIDs...)
	t.tracker.Track(ctx, resp)
}

// markState marks the given preconfirmed state for the given message IDs.
func (t *TxrV2) markState(state types.PreconfirmedState, msgIDs ...string) {
	t.preconfirmedMu.Lock()
	defer t.preconfirmedMu.Unlock()

	for _, msgID := range msgIDs {
		t.preconfirmedStates[msgID] = state
	}
}

// removeStateTracking removes preconfirmed state tracking of the given message IDs, equivalent to
// marking the state as StateUnknown.
func (t *TxrV2) removeStateTracking(msgIDs ...string) {
	t.preconfirmedMu.Lock()
	defer t.preconfirmedMu.Unlock()

	for _, msgID := range msgIDs {
		delete(t.preconfirmedStates, msgID)
	}
}
