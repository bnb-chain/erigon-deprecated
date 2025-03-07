package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
)

func SendPayloadStatus(hd *headerdownload.HeaderDownload, headBlockHash common.Hash, err error) {
	if pendingPayloadStatus := hd.GetPendingPayloadStatus(); pendingPayloadStatus != nil {
		if err != nil {
			hd.PayloadStatusCh <- engineapi.PayloadStatus{CriticalError: err}
		} else {
			hd.PayloadStatusCh <- *pendingPayloadStatus
		}
	} else if pendingPayloadHash := hd.GetPendingPayloadHash(); pendingPayloadHash != (common.Hash{}) {
		if err != nil {
			hd.PayloadStatusCh <- engineapi.PayloadStatus{CriticalError: err}
		} else {
			var status remote.EngineStatus
			if headBlockHash == pendingPayloadHash {
				status = remote.EngineStatus_VALID
			} else {
				log.Warn("Failed to execute pending payload", "pendingPayload", pendingPayloadHash, "headBlock", headBlockHash)
				status = remote.EngineStatus_INVALID
			}
			hd.PayloadStatusCh <- engineapi.PayloadStatus{
				Status:          status,
				LatestValidHash: headBlockHash,
			}
		}
	}
	hd.ClearPendingPayloadHash()
	hd.SetPendingPayloadStatus(nil)
}

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db kv.RwDB,
	sync *stagedsync.Sync,
	hd *headerdownload.HeaderDownload,
	notifications *stagedsync.Notifications,
	updateHead func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int),
	waitForDone chan struct{},
	loopMinTime time.Duration,
) {
	defer close(waitForDone)
	initialCycle := true

	for {
		start := time.Now()

		// Estimate the current top height seen from the peer
		height := hd.TopSeenHeight()
		headBlockHash, err := StageLoopStep(ctx, db, sync, height, notifications, initialCycle, updateHead, nil)

		SendPayloadStatus(hd, headBlockHash, err)

		if err != nil {
			if errors.Is(err, libcommon.ErrStopped) || errors.Is(err, context.Canceled) {
				return
			}

			log.Error("Staged Sync", "err", err)
			if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
				log.Error("Failed to recover header sentriesClient", "err", recoveryErr)
			}
			time.Sleep(500 * time.Millisecond) // just to avoid too much similar errors in logs
			continue
		}

		initialCycle = false
		hd.EnableRequestChaining()

		if loopMinTime != 0 {
			waitTime := loopMinTime - time.Since(start)
			log.Info("Wait time until next loop", "for", waitTime)
			c := time.After(waitTime)
			select {
			case <-ctx.Done():
				return
			case <-c:
			}
		}
	}
}

func StageLoopStep(
	ctx context.Context,
	db kv.RwDB,
	sync *stagedsync.Sync,
	highestSeenHeader uint64,
	notifications *stagedsync.Notifications,
	initialCycle bool,
	updateHead func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int),
	snapshotMigratorFinal func(tx kv.Tx) error,
) (headBlockHash common.Hash, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	var origin, finishProgressBefore uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		origin, err = stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return err
		}
		finishProgressBefore, err = stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return headBlockHash, err
	}

	canRunCycleInOneTransaction := !initialCycle && highestSeenHeader < origin+8096 && highestSeenHeader < finishProgressBefore+8096

	var tx kv.RwTx // on this variable will run sync cycle.
	if canRunCycleInOneTransaction {
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return headBlockHash, err
		}
		defer tx.Rollback()
	}

	if notifications != nil && notifications.Accumulator != nil && canRunCycleInOneTransaction {
		notifications.Accumulator.Reset(tx.ViewID())
	}

	err = sync.Run(db, tx, initialCycle)
	if err != nil {
		return headBlockHash, err
	}
	if canRunCycleInOneTransaction {
		commitStart := time.Now()
		errTx := tx.Commit()
		if errTx != nil {
			return headBlockHash, errTx
		}
		log.Info("Commit cycle", "in", time.Since(commitStart))
	}
	var rotx kv.Tx
	if rotx, err = db.BeginRo(ctx); err != nil {
		return headBlockHash, err
	}
	defer rotx.Rollback()

	// Update sentry status for peers to see our sync status
	var headTd *big.Int
	var head uint64
	var headHash common.Hash
	if head, err = stages.GetStageProgress(rotx, stages.Headers); err != nil {
		return headBlockHash, err
	}
	if headHash, err = rawdb.ReadCanonicalHash(rotx, head); err != nil {
		return headBlockHash, err
	}
	if headTd, err = rawdb.ReadTd(rotx, headHash, head); err != nil {
		return headBlockHash, err
	}
	headBlockHash = rawdb.ReadHeadBlockHash(rotx)

	if canRunCycleInOneTransaction && snapshotMigratorFinal != nil {
		err = snapshotMigratorFinal(rotx)
		if err != nil {
			log.Error("snapshot migration failed", "err", err)
		}
	}

	if headTd != nil {
		headTd256, overflow := uint256.FromBig(headTd)
		if overflow {
			return headBlockHash, fmt.Errorf("headTds higher than 2^256-1")
		}
		updateHead(ctx, head, headHash, headTd256)
	}
	if notifications != nil {
		if notifications.Accumulator != nil {
			header := rawdb.ReadCurrentHeader(rotx)
			if header != nil {
				pendingBaseFee := misc.CalcBaseFee(notifications.Accumulator.ChainConfig(), header)
				if header.Number.Uint64() == 0 {
					notifications.Accumulator.StartChange(0, header.Hash(), nil, false)
				}
				notifications.Accumulator.SendAndReset(ctx, notifications.StateChangesConsumer, pendingBaseFee.Uint64(), header.GasLimit)

				if err = stagedsync.NotifyNewHeaders(ctx, finishProgressBefore, head, sync.PrevUnwindPoint(), notifications.Events, rotx); err != nil {
					return headBlockHash, nil
				}
			}
		}
	}

	return headBlockHash, nil
}

func MiningStep(ctx context.Context, kv kv.RwDB, mining *stagedsync.Sync) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	tx, err := kv.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	miningBatch := memdb.NewMemoryBatch(tx)
	defer miningBatch.Rollback()

	if err = mining.Run(nil, miningBatch, false); err != nil {
		return err
	}
	tx.Rollback()
	return nil
}

func StateStep(ctx context.Context, batch kv.RwTx, stateSync *stagedsync.Sync, headerReader services.FullBlockReader, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody, txNums *exec22.TxNums) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	// Construct side fork if we have one
	if unwindPoint > 0 {
		// Run it through the unwind
		stateSync.UnwindTo(unwindPoint, common.Hash{})
		if err = stateSync.RunUnwind(nil, batch); err != nil {
			return err
		}
	}
	// Once we unwond we can start constructing the chain (assumption: len(headersChain) == len(bodiesChain))
	for i := range headersChain {
		currentHeader := headersChain[i]
		currentBody := bodiesChain[i]
		currentHeight := headersChain[i].Number.Uint64()
		currentHash := headersChain[i].Hash()
		// Prepare memory state for block execution
		ok, lastTxnNum, err := rawdb.WriteRawBodyIfNotExists(batch, currentHash, currentHeight, currentBody)
		if err != nil {
			return err
		}
		if ok {
			if txNums != nil {
				txNums.Append(currentHeight, lastTxnNum)
			}
		}
		rawdb.WriteHeader(batch, currentHeader)
		if err = rawdb.WriteHeaderNumber(batch, currentHash, currentHeight); err != nil {
			return err
		}
		if err = rawdb.WriteCanonicalHash(batch, currentHash, currentHeight); err != nil {
			return err
		}
	}

	// If we did not specify header or body we stop here
	if header == nil {
		return nil
	}
	// Setup
	height := header.Number.Uint64()
	hash := header.Hash()
	// Prepare memory state for block execution
	rawdb.WriteHeader(batch, header)
	if err = rawdb.WriteHeaderNumber(batch, hash, height); err != nil {
		return err
	}

	if err = rawdb.WriteCanonicalHash(batch, hash, height); err != nil {
		return err
	}

	if err := rawdb.WriteHeadHeaderHash(batch, hash); err != nil {
		return err
	}

	if err = stages.SaveStageProgress(batch, stages.Headers, height); err != nil {
		return err
	}
	if body != nil {
		if err = stages.SaveStageProgress(batch, stages.Bodies, height); err != nil {
			return err
		}
		ok, lastTxnNum, err := rawdb.WriteRawBodyIfNotExists(batch, hash, height, body)
		if err != nil {
			return err
		}
		if ok {
			if txNums != nil {
				txNums.Append(height, lastTxnNum)
			}
		}
	} else {
		if err = stages.SaveStageProgress(batch, stages.Bodies, height-1); err != nil {
			return err
		}
	}
	// Run state sync
	if err = stateSync.Run(nil, batch, false); err != nil {
		return err
	}
	return nil
}

func NewStagedSync(ctx context.Context,
	db kv.RwDB,
	p2pCfg p2p.Config,
	cfg *ethconfig.Config,
	controlServer *sentry.MultiClient,
	notifications *stagedsync.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	snapshots *snapshotsync.RoSnapshots,
	headCh chan *types.Block,
	txNums *exec22.TxNums, agg *state.Aggregator22,
	forkValidator *engineapi.ForkValidator,
) (*stagedsync.Sync, error) {
	dirs := cfg.Dirs
	var blockReader services.FullBlockReader
	if cfg.Snapshot.Enabled {
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(snapshots)
	} else {
		blockReader = snapshotsync.NewBlockReader()
	}
	blockRetire := snapshotsync.NewBlockRetire(1, dirs.Tmp, snapshots, db, snapDownloader, notifications.Events)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode
	isBor := controlServer.ChainConfig.Bor != nil
	var sprint uint64
	if isBor {
		sprint = controlServer.ChainConfig.Bor.Sprint
	}

	return stagedsync.New(
		stagedsync.DefaultStages(ctx, cfg.Prune,
			stagedsync.StageHeadersCfg(
				db,
				controlServer.Hd,
				controlServer.Bd,
				*controlServer.ChainConfig,
				controlServer.SendHeaderRequest,
				controlServer.PropagateNewBlockHashes,
				controlServer.Penalize,
				cfg.BatchSize,
				p2pCfg.NoDiscovery,
				cfg.MemoryOverlay,
				snapshots,
				snapDownloader,
				blockReader,
				dirs.Tmp,
				notifications.Events,
				notifications,
				forkValidator),
			stagedsync.StageCumulativeIndexCfg(db),
			stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig),
			stagedsync.StageBodiesCfg(
				db,
				controlServer.Bd,
				controlServer.SendBodyRequest,
				controlServer.Penalize,
				controlServer.BroadcastNewBlock,
				cfg.Sync.BodyDownloadTimeoutSeconds,
				*controlServer.ChainConfig,
				cfg.BatchSize,
				snapshots,
				blockReader,
				cfg.HistoryV2,
				txNums,
			),
			stagedsync.StageIssuanceCfg(db, controlServer.ChainConfig, blockReader, cfg.EnabledIssuance),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockRetire, controlServer.Hd),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				nil,
				controlServer.ChainConfig,
				controlServer.Engine,
				&vm.Config{EnableTEMV: cfg.Prune.Experiments.TEVM},
				notifications.Accumulator,
				cfg.StateStream,
				/*stateStream=*/ false,
				cfg.HistoryV2,
				dirs,
				blockReader,
				controlServer.Hd,
				cfg.Genesis,
				cfg.Sync.ExecWorkerCount,
				txNums,
				agg,
			),
			stagedsync.StageTranspileCfg(db, cfg.BatchSize, controlServer.ChainConfig),
			stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV2, txNums, agg),
			stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV2, txNums, agg),
			stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
			stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp),
			stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
			stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, snapshots, isBor, sprint),
			stagedsync.StageFinishCfg(db, dirs.Tmp, headCh, forkValidator), runInTestMode),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
	), nil
}

func NewInMemoryExecution(ctx context.Context, db kv.RwDB, cfg *ethconfig.Config, controlServer *sentry.MultiClient, dirs datadir.Dirs, notifications *stagedsync.Notifications, snapshots *snapshotsync.RoSnapshots, txNums *exec22.TxNums, agg *state.Aggregator22) (*stagedsync.Sync, error) {
	var blockReader services.FullBlockReader
	if cfg.Snapshot.Enabled {
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(snapshots)
	} else {
		blockReader = snapshotsync.NewBlockReader()
	}

	return stagedsync.New(
		stagedsync.StateStages(ctx,
			stagedsync.StageHeadersCfg(
				db,
				controlServer.Hd,
				controlServer.Bd,
				*controlServer.ChainConfig,
				controlServer.SendHeaderRequest,
				controlServer.PropagateNewBlockHashes,
				controlServer.Penalize,
				cfg.BatchSize,
				false,
				cfg.MemoryOverlay,
				snapshots,
				nil,
				blockReader,
				dirs.Tmp,
				notifications.Events,
				nil, nil), stagedsync.StageBodiesCfg(
				db,
				controlServer.Bd,
				controlServer.SendBodyRequest,
				controlServer.Penalize,
				controlServer.BroadcastNewBlock,
				cfg.Sync.BodyDownloadTimeoutSeconds,
				*controlServer.ChainConfig,
				cfg.BatchSize,
				snapshots,
				blockReader,
				cfg.HistoryV2,
				txNums,
			), stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, true, dirs.Tmp, cfg.Prune, nil, controlServer.Hd),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				nil,
				controlServer.ChainConfig,
				controlServer.Engine,
				&vm.Config{EnableTEMV: cfg.Prune.Experiments.TEVM},
				notifications.Accumulator,
				cfg.StateStream,
				true,
				cfg.HistoryV2,
				cfg.Dirs,
				blockReader,
				controlServer.Hd,
				cfg.Genesis,
				1,
				txNums,
				agg,
			),
			stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV2, txNums, agg),
			stagedsync.StageTrieCfg(db, true, true, true, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV2, txNums, agg)),
		stagedsync.StateUnwindOrder,
		nil,
	), nil
}
