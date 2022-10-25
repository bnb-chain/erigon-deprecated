package stagedsync

import (
	"fmt"
	"time"

	metrics2 "github.com/VictoriaMetrics/metrics"
	eth_metrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/ledgerwatch/erigon/miningmetrics"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

var (
	miningFinishTimer   = eth_metrics.NewRegisteredTimer("mining/finish/delay", nil)
	miningFinishCounter = eth_metrics.NewRegisteredCounter("mining/finish/cost", nil)
	gasUsedCount        = eth_metrics.NewRegisteredGauge("mining/finish/gas", nil)
	gasUsedCounter      = eth_metrics.NewRegisteredCounter("mining/finish/gastotal", nil)

	miningTotalTimer   = metrics2.GetOrCreateHistogram("mining_total_seconds")
	miningTotalCounter = eth_metrics.NewRegisteredCounter("mining/total/counter", nil)
	miningFinishTps    = eth_metrics.NewRegisteredGauge("mining/finish/tps", nil)
	miningFinishMeter  = metrics2.GetOrCreateHistogram("mining_finish_seconds")
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
	txnNum := int64(0)
	defer func() {
		miningFinishTimer.Update(time.Since(start))
		miningFinishCounter.Inc(time.Since(start).Nanoseconds())
		if txnNum > 0 {
			txnTps := float64(txnNum) / float64(time.Since(miningmetrics.GetMiningTime()).Milliseconds())
			miningFinishTps.Update(int64(txnTps * 1000))
			miningFinishMeter.UpdateDuration(start)
		}
		miningTotalCounter.Inc(int64(time.Since(miningmetrics.GetMiningTime()).Nanoseconds()))
		miningTotalTimer.UpdateDuration(miningmetrics.GetMiningTime())
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
		txnNum = int64(block.Transactions().Len())
		gasUsedCount.Update(int64(block.GasUsed()))
		gasUsedCounter.Inc(int64(block.GasUsed()))
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
