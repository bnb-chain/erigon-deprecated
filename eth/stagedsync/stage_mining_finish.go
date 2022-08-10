package stagedsync

import (
	"fmt"
	eth_metrics "github.com/ethereum/go-ethereum/metrics"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

var (
	miningFinishTimer   = eth_metrics.NewRegisteredTimer("mining/finish/delay", nil)
	miningFinishCounter = eth_metrics.NewRegisteredCounter("mining/finish/cost", nil)
)

type MiningFinishCfg struct {
	db          kv.RwDB
	chainConfig params.ChainConfig
	engine      consensus.Engine
	sealCancel  chan struct{}
	miningState MiningState
}

func StageMiningFinishCfg(
	db kv.RwDB,
	chainConfig params.ChainConfig,
	engine consensus.Engine,
	miningState MiningState,
	sealCancel chan struct{},
) MiningFinishCfg {
	return MiningFinishCfg{
		db:          db,
		chainConfig: chainConfig,
		engine:      engine,
		miningState: miningState,
		sealCancel:  sealCancel,
	}
}

func SpawnMiningFinishStage(s *StageState, tx kv.RwTx, cfg MiningFinishCfg, quit <-chan struct{}) error {
	start := time.Now()
	defer func() {
		miningFinishTimer.Update(time.Since(start))
		miningFinishCounter.Inc(time.Since(start).Nanoseconds())
	}()
	logPrefix := s.LogPrefix()
	current := cfg.miningState.MiningBlock

	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlock(current.Header, current.Txs, current.Uncles, current.Receipts)
	*current = MiningBlock{} // hack to clean global data

	//sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	//if sealHash == prev {
	//	s.Done()
	//	return nil
	//}
	//prev = sealHash

	if cfg.miningState.MiningResultPOSCh != nil {
		cfg.miningState.MiningResultPOSCh <- block
		return nil
	}
	// Tests may set pre-calculated nonce
	if block.NonceU64() != 0 {
		cfg.miningState.MiningResultCh <- block
		return nil
	}

	cfg.miningState.PendingResultCh <- block

	if block.Transactions().Len() > 0 {
		log.Info(fmt.Sprintf("[%s] block ready for seal", logPrefix),
			"block_num", block.NumberU64(),
			"transactions", block.Transactions().Len(),
			"gas_used", block.GasUsed(),
			"gas_limit", block.GasLimit(),
			"difficulty", block.Difficulty(),
		)
	}
	// interrupt aborts the in-flight sealing task.
	select {
	case cfg.sealCancel <- struct{}{}:
	default:
		log.Trace("None in-flight sealing task.")
	}
	chain := ChainReader{Cfg: cfg.chainConfig, Db: tx}
	if err := cfg.engine.Seal(chain, block, cfg.miningState.MiningResultCh, cfg.sealCancel); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}

	return nil
}
