package baseapp

import (
	"context"

	"github.com/berachain/offchain-sdk/client/eth"
	"github.com/berachain/offchain-sdk/job"
	"github.com/berachain/offchain-sdk/log"
	"github.com/berachain/offchain-sdk/server"

	ethdb "github.com/ethereum/go-ethereum/ethdb"
)

// BaseApp is the base application structure.
type BaseApp struct {
	name    string        // Name of the application
	logger  log.Logger    // Logger for the base application
	jobMgr  *JobManager   // Job manager for handling jobs
	server  *server.Server // HTTP server for the application
}

// New creates and initializes a new BaseApp instance.
func New(
	name string,
	logger log.Logger,
	ethClient eth.Client,
	jobs []job.Basic,
	db ethdb.KeyValueStore,
	server *server.Server,
) *BaseApp {
	return &BaseApp{
		name:   name,
		logger: logger,
		jobMgr: NewManager(
			jobs,
			&contextFactory{
				connPool: ethClient,
				logger:   logger,
				db:       db,
			},
		),
		server: server,
	}
}

// Logger returns a namespaced logger for the BaseApp.
func (b *BaseApp) Logger() log.Logger {
	return b.logger.With("namespace", "baseapp")
}

// Start initializes and starts the BaseApp.
func (b *BaseApp) Start(ctx context.Context) error {
	b.Logger().Info("Attempting to start BaseApp")
	defer b.Logger().Info("BaseApp successfully started")

	// Start the job manager and producers.
	if err := b.jobMgr.Start(ctx); err != nil {
		b.Logger().Error("Failed to start job manager", "error", err)
		return err
	}

	go func() {
		if err := b.jobMgr.RunProducers(ctx); err != nil {
			b.Logger().Error("Failed to run job producers", "error", err)
		}
	}()

	if b.server == nil {
		b.Logger().Info("No HTTP server registered, skipping server start")
	} else {
		go func() {
			if err := b.server.Start(ctx); err != nil {
				b.Logger().Error("Failed to start server", "error", err)
			}
		}()
	}

	return nil
}

// Stop gracefully stops the BaseApp.
func (b *BaseApp) Stop() {
	b.Logger().Info("Attempting to stop BaseApp")
	defer b.Logger().Info("BaseApp successfully stopped")

	b.jobMgr.Stop()

	if b.server != nil {
		b.server.Stop()
	}
}
