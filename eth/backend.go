// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package eth implements the Ethereum protocol.
package eth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/direct"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	txpool2 "github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpooluitl"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloadergrpc"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/parlia"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/ethutils"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/ethstats"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Config contains the configuration options of the ETH protocol.
// Deprecated: use ethconfig.Config instead.
type Config = ethconfig.Config

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	config *ethconfig.Config
	log    log.Logger

	// DB interfaces
	chainDB    kv.RwDB
	privateAPI *grpc.Server

	engine consensus.Engine

	gasPrice  *uint256.Int
	etherbase common.Address

	networkID uint64

	lock              sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	chainConfig       *params.ChainConfig
	genesisHash       common.Hash
	miningSealingQuit chan struct{}
	pendingBlocks     chan *types.Block
	minedBlocks       chan *types.Block

	// downloader fields
	sentryCtx      context.Context
	sentryCancel   context.CancelFunc
	sentriesClient *sentry.MultiClient
	sentryServers  []*sentry.GrpcServer

	stagedSync *stagedsync.Sync

	downloaderClient proto_downloader.DownloaderClient

	notifications *stagedsync.Notifications

	waitForStageLoopStop chan struct{}
	waitForMiningStop    chan struct{}

	txPool2DB               kv.RwDB
	txPool2                 *txpool2.TxPool
	newTxs2                 chan types2.Hashes
	txPool2Fetch            *txpool2.Fetch
	txPool2Send             *txpool2.Send
	txPool2GrpcServer       txpool_proto.TxpoolServer
	notifyMiningAboutNewTxs chan struct{}
	forkValidator           *engineapi.ForkValidator
	downloader              *downloader.Downloader
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(stack *node.Node, config *ethconfig.Config, logger log.Logger) (*Ethereum, error) {
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(common.Big0) <= 0 {
		log.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}

	dirs := stack.Config().Dirs
	tmpdir := dirs.Tmp
	if err := RemoveContents(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	chainKv, err := node.OpenDatabase(stack.Config(), logger, kv.ChainDB)
	if err != nil {
		return nil, err
	}

	var currentBlock *types.Block

	config.HistoryV2 = tool.HistoryV2FromDB(chainKv)
	// Check if we have an already initialized chain and fall back to
	// that if so. Otherwise we need to generate a new genesis spec.
	if err := chainKv.View(context.Background(), func(tx kv.Tx) error {
		h, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			panic(err)
		}
		if h != (common.Hash{}) {
			config.Genesis = nil // fallback to db content
		}
		currentBlock = rawdb.ReadCurrentBlock(tx)
		return nil
	}); err != nil {
		panic(err)
	}

	chainConfig, genesis, genesisErr := core.CommitGenesisBlockWithOverride(chainKv, config.Genesis, config.OverrideMergeNetsplitBlock, config.OverrideTerminalTotalDifficulty)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}

	config.Snapshot.Enabled = ethconfig.UseSnapshotsByChainName(chainConfig.ChainName) && config.Sync.UseSnapshots

	types.SetHeaderSealFlag(chainConfig.IsHeaderWithSeal())
	log.Info("Initialised chain configuration", "config", chainConfig, "genesis", genesis.Hash())

	// Apply special hacks for BSC params
	if chainConfig.Parlia != nil {
		params.ApplyBinanceSmartChainParams()
	}

	if err := chainKv.Update(context.Background(), func(tx kv.RwTx) error {
		if err = stagedsync.UpdateMetrics(tx); err != nil {
			return err
		}

		config.Prune, err = prune.EnsureNotChanged(tx, config.Prune)
		if err != nil {
			return err
		}
		isCorrectSync, useSnapshots, err := snap.EnsureNotChanged(tx, config.Snapshot)
		if err != nil {
			return err
		}

		config.HistoryV2, err = rawdb.HistoryV2.WriteOnce(tx, config.HistoryV2)
		if err != nil {
			return err
		}

		// if we are in the incorrect syncmode then we change it to the appropriate one
		if !isCorrectSync {
			log.Warn("Incorrect snapshot enablement", "got", config.Sync.UseSnapshots, "change_to", useSnapshots)
			config.Sync.UseSnapshots = useSnapshots
			config.Snapshot.Enabled = ethconfig.UseSnapshotsByChainName(chainConfig.ChainName) && useSnapshots
		}
		log.Info("Effective", "prune_flags", config.Prune.String(), "snapshot_flags", config.Snapshot.String(), "history.v2", config.HistoryV2)

		return nil
	}); err != nil {
		return nil, err
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	// kv_remote architecture does blocks on stream.Send - means current architecture require unlimited amount of txs to provide good throughput
	//limiter := make(chan struct{}, kv.ReadersLimit)
	backend := &Ethereum{
		sentryCtx:            ctx,
		sentryCancel:         ctxCancel,
		config:               config,
		log:                  logger,
		chainDB:              chainKv,
		networkID:            config.NetworkID,
		etherbase:            config.Miner.Etherbase,
		chainConfig:          chainConfig,
		genesisHash:          genesis.Hash(),
		waitForStageLoopStop: make(chan struct{}),
		waitForMiningStop:    make(chan struct{}),
		notifications: &stagedsync.Notifications{
			Events:      privateapi.NewEvents(),
			Accumulator: shards.NewAccumulator(chainConfig),
		},
	}
	blockReader, allSnapshots, err := backend.setUpBlockReader(ctx, config.Dirs, config.Snapshot, config.Downloader)
	if err != nil {
		return nil, err
	}

	txNums := exec22.TxNumsFromDB(allSnapshots, chainKv)

	kvRPC := remotedbserver.NewKvServer(ctx, chainKv, allSnapshots)
	backend.notifications.StateChangesConsumer = kvRPC

	backend.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	var sentries []direct.SentryClient
	if len(stack.Config().P2P.SentryAddr) > 0 {
		for _, addr := range stack.Config().P2P.SentryAddr {
			sentryClient, err := sentry.GrpcClient(backend.sentryCtx, addr)
			if err != nil {
				return nil, err
			}
			sentries = append(sentries, sentryClient)
		}
	} else {
		var readNodeInfo = func() *eth.NodeInfo {
			var res *eth.NodeInfo
			_ = backend.chainDB.View(context.Background(), func(tx kv.Tx) error {
				res = eth.ReadNodeInfo(tx, backend.chainConfig, backend.genesisHash, backend.networkID)
				return nil
			})

			return res
		}

		discovery, err := setupDiscovery(backend.config.EthDiscoveryURLs)
		if err != nil {
			return nil, err
		}
		cfg := stack.Config().P2P
		cfg.NodeDatabase = filepath.Join(stack.Config().Dirs.Nodes, eth.ProtocolToString[cfg.ProtocolVersion])
		server := sentry.NewGrpcServer(backend.sentryCtx, discovery, readNodeInfo, &cfg, cfg.ProtocolVersion)

		backend.sentryServers = append(backend.sentryServers, server)
		sentries = []direct.SentryClient{direct.NewSentryClientDirect(cfg.ProtocolVersion, server)}

		go func() {
			logEvery := time.NewTicker(120 * time.Second)
			defer logEvery.Stop()

			var logItems []interface{}

			for {
				select {
				case <-backend.sentryCtx.Done():
					return
				case <-logEvery.C:
					logItems = logItems[:0]
					for _, srv := range backend.sentryServers {
						logItems = append(logItems, eth.ProtocolToString[srv.Protocol.Version], strconv.Itoa(srv.SimplePeerCount()))
					}
					log.Info("[p2p] GoodPeers", logItems...)
				}
			}
		}()
	}

	aggDir := filepath.Join(dirs.DataDir, "agg22")
	dir.MustExist(aggDir)
	agg, err := libstate.NewAggregator22(aggDir, ethconfig.HistoryV2AggregationStep)
	if err != nil {
		return nil, err
	}
	defer agg.Close()

	inMemoryExecution := func(batch kv.RwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody) error {
		stateSync, err := stages2.NewInMemoryExecution(backend.sentryCtx, backend.chainDB, config, backend.sentriesClient, dirs, backend.notifications, allSnapshots, txNums, agg)
		if err != nil {
			return err
		}
		// We start the mining step
		if err := stages2.StateStep(ctx, batch, stateSync, blockReader, header, body, unwindPoint, headersChain, bodiesChain, txNums); err != nil {
			log.Warn("Could not validate block", "err", err)
			return err
		}
		return nil
	}
	currentBlockNumber := uint64(0)
	if currentBlock != nil {
		currentBlockNumber = currentBlock.NumberU64()
	}

	log.Info("Initialising Ethereum protocol", "network", config.NetworkID)
	var consensusConfig interface{}

	if chainConfig.Clique != nil {
		consensusConfig = &config.Clique
	} else if chainConfig.Aura != nil {
		config.Aura.Etherbase = config.Miner.Etherbase
		consensusConfig = &config.Aura
	} else if chainConfig.Parlia != nil {
		consensusConfig = &config.Parlia
	} else if chainConfig.Bor != nil {
		consensusConfig = &config.Bor
	} else {
		consensusConfig = &config.Ethash
	}
	backend.engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, config.HeimdallURL, config.WithoutHeimdall, stack.DataDir(), allSnapshots, false /* readonly */, backend.chainDB)
	backend.forkValidator = engineapi.NewForkValidator(currentBlockNumber, inMemoryExecution)

	backend.sentriesClient, err = sentry.NewMultiClient(
		chainKv,
		stack.Config().NodeName(),
		chainConfig,
		genesis.Hash(),
		backend.engine,
		backend.config.NetworkID,
		sentries,
		config.Sync,
		blockReader,
		stack.Config().SentryLogPeerInfo,
		backend.forkValidator,
	)
	if err != nil {
		return nil, err
	}

	var miningRPC txpool_proto.MiningServer
	if config.DeprecatedTxPool.Disable {
		backend.txPool2GrpcServer = &txpool2.GrpcDisabled{}
	} else {
		//cacheConfig := kvcache.DefaultCoherentCacheConfig
		//cacheConfig.MetricsLabel = "txpool"

		stateDiffClient := direct.NewStateDiffClientDirect(kvRPC)
		backend.newTxs2 = make(chan types2.Hashes, 1024)
		//defer close(newTxs)
		backend.txPool2DB, backend.txPool2, backend.txPool2Fetch, backend.txPool2Send, backend.txPool2GrpcServer, err = txpooluitl.AllComponents(
			ctx, config.TxPool, kvcache.NewDummy(), backend.newTxs2, backend.chainDB, backend.sentriesClient.Sentries(), stateDiffClient,
		)
		if err != nil {
			return nil, err
		}
	}

	backend.notifyMiningAboutNewTxs = make(chan struct{}, 1)
	backend.miningSealingQuit = make(chan struct{})
	backend.pendingBlocks = make(chan *types.Block, 1)
	backend.minedBlocks = make(chan *types.Block, 1)

	miner := stagedsync.NewMiningState(&config.Miner)
	backend.pendingBlocks = miner.PendingResultCh
	backend.minedBlocks = miner.MiningResultCh
	newHeaderCh, newHeaderChClean := backend.notifications.Events.AddHeaderSubscription()
	miner.NewHeaderCh = newHeaderCh

	// proof-of-work mining
	mining := stagedsync.New(
		stagedsync.MiningStages(backend.sentryCtx,
			stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miner, *backend.chainConfig, backend.engine, backend.txPool2, backend.txPool2DB, nil, tmpdir),
			stagedsync.StageMiningExecCfg(backend.chainDB, miner, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, nil),
			stagedsync.StageHashStateCfg(backend.chainDB, dirs, config.HistoryV2, txNums, agg),
			stagedsync.StageTrieCfg(backend.chainDB, false, true, true, tmpdir, blockReader, nil, config.HistoryV2, txNums, agg),
			stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miner, backend.miningSealingQuit),
		), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)

	var ethashApi *ethash.API
	if casted, ok := backend.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}

	// proof-of-stake mining
	assembleBlockPOS := func(param *core.BlockBuilderParameters, interrupt *int32) (*types.Block, error) {
		miningStatePos := stagedsync.NewProposingState(&config.Miner)
		miningStatePos.MiningConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			stagedsync.MiningStages(backend.sentryCtx,
				stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miningStatePos, *backend.chainConfig, backend.engine, backend.txPool2, backend.txPool2DB, param, tmpdir),
				stagedsync.StageMiningExecCfg(backend.chainDB, miningStatePos, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, interrupt),
				stagedsync.StageHashStateCfg(backend.chainDB, dirs, config.HistoryV2, txNums, agg),
				stagedsync.StageTrieCfg(backend.chainDB, false, true, true, tmpdir, blockReader, nil, config.HistoryV2, txNums, agg),
				stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miningStatePos, backend.miningSealingQuit),
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)
		// We start the mining step
		if err := stages2.MiningStep(ctx, backend.chainDB, proposingSync); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultPOSCh
		return block, nil
	}

	// Initialize ethbackend
	ethBackendRPC := privateapi.NewEthBackendServer(ctx, backend, backend.chainDB, backend.notifications.Events,
		blockReader, chainConfig, assembleBlockPOS, backend.sentriesClient.Hd, config.Miner.EnabledPOS)
	miningRPC = privateapi.NewMiningServer(ctx, backend, ethashApi)

	if stack.Config().PrivateApiAddr != "" {
		var creds credentials.TransportCredentials
		if stack.Config().TLSConnection {
			creds, err = grpcutil.TLS(stack.Config().TLSCACert, stack.Config().TLSCertFile, stack.Config().TLSKeyFile)
			if err != nil {
				return nil, err
			}
		}
		backend.privateAPI, err = privateapi.StartGrpc(
			kvRPC,
			ethBackendRPC,
			backend.txPool2GrpcServer,
			miningRPC,
			stack.Config().PrivateApiAddr,
			stack.Config().PrivateApiRateLimit,
			creds,
			stack.Config().HealthCheck)
		if err != nil {
			return nil, fmt.Errorf("private api: %w", err)
		}
	}

	if currentBlock == nil {
		currentBlock = genesis
	}
	// We start the transaction pool on startup, for a couple of reasons:
	// 1) Hive tests requires us to do so and starting it from eth_sendRawTransaction is not viable as we have not enough data
	// to initialize it properly.
	// 2) we cannot propose for block 1 regardless.
	go func() {
		time.Sleep(10 * time.Millisecond)
		baseFee := uint64(0)
		if currentBlock.BaseFee() != nil {
			baseFee = currentBlock.BaseFee().Uint64()
		}
		backend.notifications.Accumulator.StartChange(currentBlock.NumberU64(), currentBlock.Hash(), nil, false)
		backend.notifications.Accumulator.SendAndReset(ctx, backend.notifications.StateChangesConsumer, baseFee, currentBlock.GasLimit())

	}()

	if !config.DeprecatedTxPool.Disable {
		backend.txPool2Fetch.ConnectCore()
		backend.txPool2Fetch.ConnectSentries()
		var newTxsBroadcaster *txpool2.NewSlotsStreams
		if casted, ok := backend.txPool2GrpcServer.(*txpool2.GrpcServer); ok {
			newTxsBroadcaster = casted.NewSlotsStreams
		}
		go txpool2.MainLoop(backend.sentryCtx,
			backend.txPool2DB, backend.chainDB,
			backend.txPool2, backend.newTxs2, backend.txPool2Send, newTxsBroadcaster,
			func() {
				select {
				case backend.notifyMiningAboutNewTxs <- struct{}{}:
				default:
				}
			})
	}
	go func() {
		defer debug.LogPanic()
		defer newHeaderChClean()

		for {
			select {
			case b := <-backend.minedBlocks:
				//p2p
				//backend.sentriesClient.BroadcastNewBlock(context.Background(), b, b.Difficulty())
				//rpcdaemon
				if err := miningRPC.(*privateapi.MiningServer).BroadcastMinedBlock(b); err != nil {
					log.Error("txpool rpc mined block broadcast", "err", err)
				}
				log.Trace("BroadcastMinedBlock successful", "number", b.Number(), "GasUsed", b.GasUsed(), "txn count", b.Transactions().Len())
				backend.sentriesClient.PropagateNewBlockHashes(ctx, []headerdownload.Announce{
					{
						Number: b.NumberU64(),
						Hash:   b.Hash(),
					},
				})
				if err := backend.sentriesClient.Bd.AddMinedBlock(b); err != nil {
					log.Error("add mined block to body downloader", "err", err)
				}
				if err := backend.sentriesClient.Hd.AddMinedHeader(b.Header()); err != nil {
					log.Error("add mined block to header downloader", "err", err)
				}

			case b := <-backend.pendingBlocks:
				if err := miningRPC.(*privateapi.MiningServer).BroadcastPendingBlock(b); err != nil {
					log.Error("txpool rpc pending block broadcast", "err", err)
				}
			case <-backend.sentriesClient.Hd.QuitPoWMining:
				return
			}
		}
	}()

	if err := backend.StartMining(context.Background(), backend.chainDB, mining, backend.config.Miner, backend.gasPrice, backend.sentriesClient.Hd.QuitPoWMining); err != nil {
		return nil, err
	}

	var headCh chan *types.Block
	if config.Ethstats != "" {
		headCh = make(chan *types.Block, 1)
	}

	backend.stagedSync, err = stages2.NewStagedSync(backend.sentryCtx, backend.chainDB, stack.Config().P2P, config, backend.sentriesClient, backend.notifications, backend.downloaderClient, allSnapshots, headCh, txNums, agg, backend.forkValidator)
	if err != nil {
		return nil, err
	}

	backend.sentriesClient.Hd.StartPoSDownloader(backend.sentryCtx, backend.sentriesClient.SendHeaderRequest, backend.sentriesClient.Penalize)

	emptyBadHash := config.BadBlockHash == common.Hash{}
	if !emptyBadHash {
		var badBlockHeader *types.Header
		if err = chainKv.View(context.Background(), func(tx kv.Tx) error {
			header, hErr := rawdb.ReadHeaderByHash(tx, config.BadBlockHash)
			badBlockHeader = header
			return hErr
		}); err != nil {
			return nil, err
		}

		if badBlockHeader != nil {
			unwindPoint := badBlockHeader.Number.Uint64() - 1
			backend.stagedSync.UnwindTo(unwindPoint, config.BadBlockHash)
		}
	}

	//eth.APIBackend = &EthAPIBackend{stack.Config().ExtRPCEnabled(), stack.Config().AllowUnprotectedTxs, eth, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.Miner.GasPrice
	}
	//eth.APIBackend.gpo = gasprice.NewOracle(eth.APIBackend, gpoParams)
	if config.Ethstats != "" {
		if err := ethstats.New(stack, backend.sentryServers, chainKv, backend.engine, config.Ethstats, backend.networkID, ctx.Done(), headCh); err != nil {
			return nil, err
		}
	}
	// start HTTP API
	httpRpcCfg := stack.Config().Http
	ethRpcClient, txPoolRpcClient, miningRpcClient, starkNetRpcClient, stateCache, ff, txNums, err := cli.EmbeddedServices(ctx, chainKv, httpRpcCfg.StateCache, blockReader, allSnapshots, ethBackendRPC, backend.txPool2GrpcServer, miningRPC)
	if err != nil {
		return nil, err
	}

	var borDb kv.RoDB
	if casted, ok := backend.engine.(*bor.Bor); ok {
		borDb = casted.DB
	}
	apiList := commands.APIList(chainKv, borDb, ethRpcClient, txPoolRpcClient, miningRpcClient, starkNetRpcClient, ff, stateCache, blockReader, agg, txNums, httpRpcCfg)
	authApiList := commands.AuthAPIList(chainKv, ethRpcClient, txPoolRpcClient, miningRpcClient, ff, stateCache, blockReader, httpRpcCfg)
	go func() {
		if err := cli.StartRpcServer(ctx, httpRpcCfg, apiList, authApiList); err != nil {
			log.Error(err.Error())
			return
		}
	}()

	// Register the backend on the node
	stack.RegisterLifecycle(backend)
	return backend, nil
}

func (s *Ethereum) APIs() []rpc.API {
	return []rpc.API{}
}

func (s *Ethereum) Etherbase() (eb common.Address, err error) {
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()

	if etherbase != (common.Address{}) {
		return etherbase, nil
	}
	return common.Address{}, fmt.Errorf("etherbase must be explicitly specified")
}

// isLocalBlock checks whether the specified block is mined
// by local miner accounts.
//
// We regard two types of accounts as local miner account: etherbase
// and accounts specified via `txpool.locals` flag.
func (s *Ethereum) isLocalBlock(block *types.Block) bool { //nolint
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()
	return ethutils.IsLocalBlock(s.engine, etherbase, s.config.DeprecatedTxPool.Locals, block.Header())
}

// shouldPreserve checks whether we should preserve the given block
// during the chain reorg depending on whether the author of block
// is a local account.
func (s *Ethereum) shouldPreserve(block *types.Block) bool { //nolint
	// The reason we need to disable the self-reorg preserving for clique
	// is it can be probable to introduce a deadlock.
	//
	// e.g. If there are 7 available signers
	//
	// r1   A
	// r2     B
	// r3       C
	// r4         D
	// r5   A      [X] F G
	// r6    [X]
	//
	// In the round5, the inturn signer E is offline, so the worst case
	// is A, F and G sign the block of round5 and reject the block of opponents
	// and in the round6, the last available signer B is offline, the whole
	// network is stuck.
	if _, ok := s.engine.(*clique.Clique); ok {
		return false
	}
	return s.isLocalBlock(block)
}

type chainReader struct {
	config      *params.ChainConfig
	tx          kv.Tx
	blockReader services.FullBlockReader
}

func (cr chainReader) Config() *params.ChainConfig  { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header {
	if cr.blockReader != nil {
		number := rawdb.ReadHeaderNumber(cr.tx, hash)
		if number == nil {
			return nil
		}
		return cr.GetHeader(hash, *number)
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr chainReader) GetTd(hash common.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		log.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}

// StartMining starts the miner with the given number of CPU threads. If mining
// is already running, this method adjust the number of threads allowed to use
// and updates the minimum price required by the transaction pool.
func (s *Ethereum) StartMining(pctx context.Context, db kv.RwDB, mining *stagedsync.Sync, cfg params.MiningConfig, gasPrice *uint256.Int, quitCh chan struct{}) error {
	if !cfg.Enabled {
		return nil
	}

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		log.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %w", err)
	}

	var clq *clique.Clique
	if c, ok := s.engine.(*clique.Clique); ok {
		clq = c
	} else if cl, ok := s.engine.(*serenity.Serenity); ok {
		if c, ok := cl.InnerEngine().(*clique.Clique); ok {
			clq = c
		}
	}
	if clq != nil {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		clq.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(message), cfg.SigKey)
		})
	}

	var prl *parlia.Parlia
	if p, ok := s.engine.(*parlia.Parlia); ok {
		prl = p
	} else if cl, ok := s.engine.(*serenity.Serenity); ok {
		if p, ok := cl.InnerEngine().(*parlia.Parlia); ok {
			prl = p
		}
	}
	if prl != nil {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		prl.Authorize(eb, func(validator common.Address, payload []byte, chainId *big.Int) ([]byte, error) {
			return crypto.Sign(payload, cfg.SigKey)
		})
	}

	var borcfg *bor.Bor
	if b, ok := s.engine.(*bor.Bor); ok {
		borcfg = b
	} else if br, ok := s.engine.(*serenity.Serenity); ok {
		if b, ok := br.InnerEngine().(*bor.Bor); ok {
			borcfg = b
		}
	}
	if borcfg != nil {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		borcfg.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(message), cfg.SigKey)
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer debug.LogPanic()
		defer close(s.waitForMiningStop)

		mineEvery := time.NewTicker(3 * time.Second)
		defer mineEvery.Stop()

		newHeaderCh, clean := s.notifications.Events.AddHeaderSubscription()
		defer clean()

		var works bool
		var hasWork bool
		errc := make(chan error, 1)

		for {
			mineEvery.Reset(3 * time.Second)
			select {
			case <-s.notifyMiningAboutNewTxs:
				hasWork = false
			case <-mineEvery.C:
				hasWork = true
			case headersRlp := <-newHeaderCh:
				if prl != nil {
					if len(headersRlp) == 0 {
						continue
					}
					headerRlp := headersRlp[0]
					header := new(types.Header)
					if err := rlp.Decode(bytes.NewReader(headerRlp), header); err != nil {
						log.Warn("miner loop receive new header, decode header RLP err", err)
						continue
					}
					log.Warn("New header coming ", "new_header_num: ", header.Number.Uint64(), " new_header_hash: ", header.Hash())

					tx, err := s.chainDB.BeginRo(context.Background())
					if err != nil {
						continue
					}
					signedRecent, err := prl.SignRecently(&chainReader{config: s.sentriesClient.ChainConfig, tx: tx, blockReader: snapshotsync.NewBlockReader()}, header)
					tx.Rollback()
					if err != nil {
						log.Warn("Not allowed to propose block", "err", err)
						continue
					}
					if signedRecent {
						log.Info("Signed recently, must wait")
						continue
					}
					log.Warn("New header coming, mining block interrupt,", " new_header_num: ", header.Number.Uint64(), ", new_header_hash: ", header.Hash())
					cancel()
					ctx, cancel = context.WithCancel(context.Background())
					select {
					case s.miningSealingQuit <- struct{}{}:
					default:
						log.Info("None in-flight sealing task.")
					}
					hasWork = true
				}
			case err := <-errc:
				works = false
				if errors.Is(err, libcommon.ErrStopped) {
					return
				}
				if err != nil {
					log.Warn("mining", "err", err)
				}
			case <-quitCh:
				return
			}

			if !works && hasWork {
				works = true
				hasWork = false
				go func() {
					select {
					case errc <- stages2.MiningStep(ctx, db, mining):
						log.Warn("MiningStep finish", "err", err)
						return
					case <-ctx.Done():
						errc <- errors.New("MiningStep cancel")
						return
					}

				}()
			}
		}
	}()

	return nil
}

func (s *Ethereum) IsMining() bool { return s.config.Miner.Enabled }

func (s *Ethereum) ChainKV() kv.RwDB            { return s.chainDB }
func (s *Ethereum) NetVersion() (uint64, error) { return s.networkID, nil }
func (s *Ethereum) NetPeerCount() (uint64, error) {
	var sentryPc uint64 = 0

	log.Trace("sentry", "peer count", sentryPc)
	for _, sc := range s.sentriesClient.Sentries() {
		ctx := context.Background()
		reply, err := sc.PeerCount(ctx, &proto_sentry.PeerCountRequest{})
		if err != nil {
			log.Warn("sentry", "err", err)
			return 0, nil
		}
		sentryPc += reply.Count
	}

	return sentryPc, nil
}

func (s *Ethereum) NodesInfo(limit int) (*remote.NodesInfoReply, error) {
	if limit == 0 || limit > len(s.sentriesClient.Sentries()) {
		limit = len(s.sentriesClient.Sentries())
	}

	nodes := make([]*prototypes.NodeInfoReply, 0, limit)
	for i := 0; i < limit; i++ {
		sc := s.sentriesClient.Sentries()[i]

		nodeInfo, err := sc.NodeInfo(context.Background(), nil)
		if err != nil {
			log.Error("sentry nodeInfo", "err", err)
		}

		nodes = append(nodes, nodeInfo)
	}

	nodesInfo := &remote.NodesInfoReply{NodesInfo: nodes}
	slices.SortFunc(nodesInfo.NodesInfo, remote.NodeInfoReplyLess)

	return nodesInfo, nil
}

// sets up blockReader and client downloader
func (s *Ethereum) setUpBlockReader(ctx context.Context, dirs datadir.Dirs, snConfig ethconfig.Snapshot, downloaderCfg *downloadercfg.Cfg) (services.FullBlockReader, *snapshotsync.RoSnapshots, error) {
	if !snConfig.Enabled {
		blockReader := snapshotsync.NewBlockReader()
		return blockReader, nil, nil
	}

	allSnapshots := snapshotsync.NewRoSnapshots(snConfig, dirs.Snap)
	var err error
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)

	if !snConfig.NoDownloader {
		if snConfig.DownloaderAddr != "" {
			// connect to external Downloader
			s.downloaderClient, err = downloadergrpc.NewClient(ctx, snConfig.DownloaderAddr)
		} else {
			// start embedded Downloader
			s.downloader, err = downloader.New(downloaderCfg)
			if err != nil {
				return nil, nil, err
			}
			go downloader.MainLoop(ctx, s.downloader, true)
			bittorrentServer, err := downloader.NewGrpcServer(s.downloader)
			if err != nil {
				return nil, nil, fmt.Errorf("new server: %w", err)
			}

			s.downloaderClient = direct.NewDownloaderClient(bittorrentServer)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return blockReader, allSnapshots, nil
}

func (s *Ethereum) Peers(ctx context.Context) (*remote.PeersReply, error) {
	var reply remote.PeersReply
	for _, sentryClient := range s.sentriesClient.Sentries() {
		peers, err := sentryClient.Peers(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.Peers error: %w", err)
		}
		reply.Peers = append(reply.Peers, peers.Peers...)
	}
	return &reply, nil
}

// Protocols returns all the currently configured
// network protocols to start.
func (s *Ethereum) Protocols() []p2p.Protocol {
	protocols := make([]p2p.Protocol, 0, len(s.sentryServers))
	for i := range s.sentryServers {
		protocols = append(protocols, s.sentryServers[i].Protocol)
	}
	return protocols
}

// Start implements node.Lifecycle, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start() error {
	s.sentriesClient.StartStreamLoops(s.sentryCtx)
	time.Sleep(10 * time.Millisecond) // just to reduce logs order confusion

	go stages2.StageLoop(s.sentryCtx, s.chainDB, s.stagedSync, s.sentriesClient.Hd, s.notifications, s.sentriesClient.UpdateHead, s.waitForStageLoopStop, s.config.Sync.LoopThrottle)

	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	// Stop all the peer-related stuff first.
	s.sentryCancel()
	if s.downloader != nil {
		s.downloader.Close()
	}
	if s.privateAPI != nil {
		shutdownDone := make(chan bool)
		go func() {
			defer close(shutdownDone)
			s.privateAPI.GracefulStop()
		}()
		select {
		case <-time.After(1 * time.Second): // shutdown deadline
			s.privateAPI.Stop()
		case <-shutdownDone:
		}
	}
	libcommon.SafeClose(s.sentriesClient.Hd.QuitPoWMining)

	_ = s.engine.Close()
	<-s.waitForStageLoopStop
	if s.config.Miner.Enabled {
		<-s.waitForMiningStop
	}
	for _, sentryServer := range s.sentryServers {
		sentryServer.Close()
	}
	s.chainDB.Close()
	if s.txPool2DB != nil {
		s.txPool2DB.Close()
	}
	return nil
}

func (s *Ethereum) ChainDB() kv.RwDB {
	return s.chainDB
}

func (s *Ethereum) StagedSync() *stagedsync.Sync {
	return s.stagedSync
}

func (s *Ethereum) Notifications() *stagedsync.Notifications {
	return s.notifications
}

func (s *Ethereum) SentryCtx() context.Context {
	return s.sentryCtx
}

func (s *Ethereum) SentryControlServer() *sentry.MultiClient {
	return s.sentriesClient
}

// RemoveContents is like os.RemoveAll, but preserve dir itself
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
