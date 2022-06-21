package dbjobqueue

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
)

type GenericQuerier interface {
	pgxtype.Querier

	Begin(ctx context.Context) (pgx.Tx, error)
}

// GenericConn delegates all query functions to either pgx.Conn or pgxpool.Conn
type GenericConn struct {
	// delegated interface
	GenericQuerier

	// native reference to pgx.Conn or pgxpool.Conn to properly release connection
	nativeConn interface{}
}

func (p *GenericConn) Release() error {
	panic(fmt.Errorf("use Release() from GenericPool instead"))
}

func (p *GenericConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	switch v := p.nativeConn.(type) {
	case *pgx.Conn:
		return v.WaitForNotification(ctx)
	case *pgxpool.Conn:
		return v.Conn().WaitForNotification(ctx)
	default:
		panic(fmt.Errorf("cannot wait for notification, unknown type %T", v))
	}
}

type GenericPool interface {
	Acquire(ctx context.Context) (*GenericConn, error)
	Release(conn *GenericConn) error
	Close()
}

type stdlibPool struct {
	pool *sql.DB
}

func (p *stdlibPool) Acquire(_ context.Context) (*GenericConn, error) {
	pgxConn, err := stdlib.AcquireConn(p.pool)
	if err != nil {
		return nil, fmt.Errorf("error acquiring connection: %v", err)
	}
	return &GenericConn{nativeConn: pgxConn, GenericQuerier: pgxConn}, nil
}

func (p *stdlibPool) Release(conn *GenericConn) error {
	return stdlib.ReleaseConn(p.pool, conn.nativeConn.(*pgx.Conn))
}

func (p *stdlibPool) Close() {
	// stdlib does not support Close()
}

func NewStdlibPool(db *sql.DB) GenericPool {
	return &stdlibPool{db}
}

type pgxPool struct {
	pool *pgxpool.Pool
}

func (p *pgxPool) Acquire(ctx context.Context) (*GenericConn, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("error acquiring connection: %v", err)
	}
	return &GenericConn{nativeConn: conn, GenericQuerier: conn}, nil
}

func (p *pgxPool) Release(conn *GenericConn) error {
	conn.nativeConn.(*pgxpool.Conn).Release()
	return nil
}

func (p *pgxPool) Close() {
	p.pool.Close()
}

func NewPgxPool(db *pgxpool.Pool) GenericPool {
	return &pgxPool{db}
}
