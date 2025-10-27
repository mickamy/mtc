// Package mtc provides a Multi-Tenant Connector for PostgreSQL.
// It applies tenant-scoped context values (app.tenant_id) transaction-locally
// using set_config(..., true) right after BeginTx, for safe RLS operation.
package mtc

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
)

// TenantIDFunc defines a function that extracts tenant ID from context.Context.
type TenantIDFunc func(context.Context) (string, error)

// Connector injects tenant ID into PostgreSQL "transaction-local" settings
// so that RLS policies can rely on current_setting('app.tenant_id', true).
type Connector struct {
	dsn        string
	driver     driver.Driver
	tenantIDFn TenantIDFunc
}

// New returns a Connector.
func New(drv driver.Driver, dsn string, fn TenantIDFunc) *Connector {
	return &Connector{driver: drv, dsn: dsn, tenantIDFn: fn}
}

// Connect is called when a new physical connection is created.
// NOTE: We DO NOT set tenant here to avoid session-scope leaks.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	rawConn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, fmt.Errorf("mtc: failed to open connection: %w", err)
	}
	return &conn{Conn: rawConn, tenantIDFn: c.tenantIDFn}, nil
}

func (c *Connector) Driver() driver.Driver { return c.driver }

type conn struct {
	driver.Conn
	tenantIDFn TenantIDFunc
}

// ResetSession is called when the connection is taken from the pool.
// We proactively RESET to ensure no leftover session vars.
func (c *conn) ResetSession(ctx context.Context) error {
	return execContext(ctx, c.Conn, "RESET app.tenant_id")
}

// BeginTx hooks transaction start and sets transaction-local tenant_id.
// If the underlying driver doesn't support ConnBeginTx, we fallback to Begin().
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		tx    driver.Tx
		txErr error
	)

	if cbtx, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, txErr = cbtx.BeginTx(ctx, opts)
	} else {
		// Fallback to legacy Begin() if ConnBeginTx is not implemented
		tx, txErr = c.Conn.Begin()
	}
	if txErr != nil {
		return nil, fmt.Errorf("mtc: failed to begin transaction: %w", txErr)
	}

	// Apply transaction-local tenant setting
	tenantID, tErr := c.tenantIDFn(ctx)
	if tErr != nil || tenantID == "" {
		// No tenant → explicit RESET inside this Tx is harmless but optional.
		if e := execContext(ctx, c.Conn, "RESET app.tenant_id"); e != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("mtc: failed to reset app.tenant_id: %w", e)
		}
		return tx, nil
	}

	if execErr := execContext(
		ctx,
		c.Conn,
		"SELECT set_config('app.tenant_id', $1, true)",
		driver.NamedValue{Ordinal: 1, Value: tenantID},
	); execErr != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("mtc: failed to set app.tenant_id: %w", execErr)
	}

	return tx, nil
}

// Begin is for drivers that call it directly.
func (c *conn) Begin() (driver.Tx, error) {
	// database/sql won't pass a context here; we can’t fetch tenant from context.
	// Use empty context; tenant setting will be RESET (no tenant) in this path.
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// execContext executes a statement with best available driver interfaces.
// 1) driver.ExecerContext
// 2) Prepare + driver.StmtExecContext
// 3) Prepare + legacy Stmt.Exec([]driver.Value)
func execContext(ctx context.Context, cn driver.Conn, query string, args ...driver.NamedValue) error {
	// 1) Preferred fast path
	if ec, ok := cn.(driver.ExecerContext); ok {
		if _, err := ec.ExecContext(ctx, query, args); err != nil && !errors.Is(err, driver.ErrSkip) {
			return err
		} else if err == nil {
			return nil
		}
	}

	// 2) Prepare path
	stmt, err := cn.Prepare(query)
	if err != nil {
		return fmt.Errorf("mtc: failed to prepare query %q: %w", query, err)
	}
	defer func() { _ = stmt.Close() }()

	if se, ok := stmt.(driver.StmtExecContext); ok {
		if _, err := se.ExecContext(ctx, args); err != nil {
			return fmt.Errorf("mtc: failed to execute query %q: %w", query, err)
		}
		return nil
	}

	// 3) Legacy fallback
	vals := make([]driver.Value, len(args))
	for i, nv := range args {
		vals[i] = nv.Value
	}
	if _, err := stmt.Exec(vals); err != nil {
		return fmt.Errorf("mtc: failed to execute legacy query %q: %w", query, err)
	}
	return nil
}

var (
	_ driver.Connector       = (*Connector)(nil)
	_ driver.SessionResetter = (*conn)(nil)
	_ driver.ConnBeginTx     = (*conn)(nil)
)
