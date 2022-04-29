package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
	"github.com/yanmengfei/logical/logger"
	"github.com/yanmengfei/logical/model"
	"go.uber.org/zap"
)

// client for logical
type client struct {
	cfg             pgx.ConnConfig
	table           string
	slot            string
	repConn         *pgx.ReplicationConn
	cancel          context.CancelFunc
	mutex           sync.Mutex
	receivePosition uint64
	replyPosition   uint64
	maxPosition     uint64
	callback        func(records []*model.Waldata)
	records         []*model.Waldata
}

// New client
func New(cfg pgx.ConnConfig, table, slot string, callback func(records []*model.Waldata)) (*client, error) {
	return &client{cfg: cfg, table: table, slot: slot, callback: callback}, nil
}

// getReceivePosition get receive position
func (c *client) getReceivePosition() uint64 {
	return atomic.LoadUint64(&c.receivePosition)
}

// setReceivePosition set receive position
func (c *client) setReceivePosition(position uint64) {
	atomic.StoreUint64(&c.receivePosition, position)
}

// getReplyPosition get reply position
func (c *client) getReplyPosition() uint64 {
	return atomic.LoadUint64(&c.replyPosition)
}

// setReplyPosition set reply position
func (c *client) setReplyPosition(position uint64) {
	atomic.StoreUint64(&c.replyPosition, position)
}

// status get connect status
func (c *client) status() (status *pgx.StandbyStatus, err error) {
	replyPosition := c.getReplyPosition()
	return pgx.NewStandbyStatus(c.getReceivePosition(), replyPosition, replyPosition)
}

// send heartbeat to postgres
func (c *client) heartbeat() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	status, err := c.status()
	logger.Debug("send heartbeat")
	if err != nil {
		return err
	}
	return c.repConn.SendStandbyStatus(status)
}

// create replication connect
func (c *client) connect() (ssid string, err error) {
	if c.repConn, err = pgx.ReplicationConnect(c.cfg); err != nil {
		return ssid, err
	}
	logger.Info("connect slot", zap.String("slot", c.slot))
	if _, ssid, err = c.repConn.CreateReplicationSlotEx(c.slot, "test_decoding"); err != nil {
		if pgerr, ok := err.(pgx.PgError); !ok || pgerr.Code != "42710" {
			return ssid, fmt.Errorf("failed to create replication slot: %s", err)
		}
	}
	logger.Info("start replication", zap.String("slot", c.slot))
	if err = c.repConn.StartReplication(c.slot, 0, -1); err != nil {
		_ = c.repConn.Close()
		return ssid, err
	}
	return ssid, nil
}

// replication message
func (c *client) replication(message *pgx.ReplicationMessage) (err error) {
	if message.ServerHeartbeat != nil {
		if message.ServerHeartbeat.ServerWalEnd > c.getReceivePosition() {
			c.setReceivePosition(message.ServerHeartbeat.ServerWalEnd)
		}
		if message.ServerHeartbeat.ReplyRequested == 1 {
			_ = c.heartbeat()
		}
	}
	if message.WalMessage != nil {
		var data = model.AcquireWaldata()
		if err = data.Decode(message.WalMessage, c.table); err != nil {
			return fmt.Errorf("invalid postgres output message: %s", err)
		}
		if data.Timestamp > 0 {
			c.commit(data)
		}
	}
	return nil
}

// message commit handler
func (c *client) commit(data *model.Waldata) {
	var flush bool
	switch data.OperationType {
	case model.BEGIN, model.UNKNOW:
	case model.COMMIT:
		flush = true
	default:
		c.records = append(c.records, data)
		flush = len(c.records) > 20000
		// TODO
		if data.Pos > c.maxPosition {
			c.maxPosition = data.Pos
		}
	}
	if flush && len(c.records) > 0 {
		c.callback(c.records)
		c.setReplyPosition(c.maxPosition)
		c.records = nil
	}
}

// timer call heartbeat regularly
func (c *client) timer(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			_ = c.heartbeat()
		case <-ctx.Done():
			return
		}
	}
}

// Start client
func (c *client) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)
	_, err := c.connect()
	if err != nil {
		return err
	}
	if err = c.heartbeat(); err != nil {
		return err
	}
	go c.timer(ctx)
	for {
		message, err := c.repConn.WaitForReplicationMessage(ctx)
		if err != nil {
			if err == ctx.Err() {
				return err
			}
			logger.Error("wait for replication message error", zap.Error(err))
			if c.repConn == nil || !c.repConn.IsAlive() {
				if _, err = c.connect(); err != nil {
					return fmt.Errorf("reset replication connection error: %s", err)
				}
			}
			continue
		}
		if message == nil {
			continue
		}
		if err = c.replication(message); err != nil {
			continue
		}
	}
}

func (c *client) Stop() error {
	logger.Info("stop replication connect")
	c.cancel()
	return c.repConn.Close()
}
