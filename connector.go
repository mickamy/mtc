// Package mtc provides a Multi-Tenant Connector for PostgreSQL.
// It applies tenant-scoped context values transaction-locally
// via set_config(..., true) right after BeginTx, for safe RLS operation.
package mtc

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"sync"
)

// TenantIDFunc defines a function that extracts tenant ID from context.Context.
type TenantIDFunc func(context.Context) (string, error)

// Option configures Connector.
type Option func(*Connector)

var settingNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_.]*$`)

// WithSettingName sets the PostgreSQL GUC name used for tenant scoping (default: "app.tenant_id").
// It must match ^[a-z_][a-z0-9_.]*$ (e.g. "app.tenant_id").
func WithSettingName(name string) Option {
	return func(c *Connector) {
		if !settingNameRe.MatchString(name) {
			panic(fmt.Sprintf("invalid setting name %q", name))
		}
		c.settingName = name
	}
}

// Connector injects tenant ID into PostgreSQL "transaction-local" settings
// so that RLS policies can rely on current_setting(settingName, true).
type Connector struct {
	dsn         string
	driver      driver.Driver
	tenantIDFn  TenantIDFunc
	settingName string // e.g. "app.tenant_id"
}

// New returns a Connector with optional settings.
func New(drv driver.Driver, dsn string, fn TenantIDFunc, opts ...Option) *Connector {
	c := &Connector{
		driver:      drv,
		dsn:         dsn,
		tenantIDFn:  fn,
		settingName: "app.tenant_id",
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Connect is called when a new physical connection is created.
// NOTE: We DO NOT set tenant here to avoid session-scope leaks.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	rawConn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, fmt.Errorf("mtc: failed to open connection: %w", err)
	}
	return &conn{Conn: rawConn, tenantIDFn: c.tenantIDFn, settingName: c.settingName}, nil
}

func (c *Connector) Driver() driver.Driver { return c.driver }

type conn struct {
	driver.Conn
	tenantIDFn  TenantIDFunc
	settingName string
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	cleanup, err := c.attachTenantSession(ctx)
	if err != nil {
		return nil, err
	}

	res, execErr := c.exec(ctx, query, args)
	if cerr := cleanup(ctx); cerr != nil && execErr == nil {
		execErr = cerr
	}
	return res, execErr
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	cleanup, err := c.attachTenantSession(ctx)
	if err != nil {
		return nil, err
	}

	rows, cleanupFn, queryErr := c.query(ctx, query, args, cleanup)
	if queryErr != nil {
		_ = cleanupFn(ctx)
		return nil, queryErr
	}

	return &tenantAwareRows{
		Rows:    rows,
		cleanup: cleanupFn,
		ctx:     ctx,
	}, nil
}

// PrepareContext wraps prepared statements so that executions also enforce tenant scoping.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := prepareStmt(ctx, c.Conn, query)
	if err != nil {
		return nil, fmt.Errorf("mtc: failed to prepare statement: %w", err)
	}
	return &tenantAwareStmt{Stmt: stmt, conn: c}, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := prepareStmt(context.Background(), c.Conn, query)
	if err != nil {
		return nil, fmt.Errorf("mtc: failed to prepare statement: %w", err)
	}
	return &tenantAwareStmt{Stmt: stmt, conn: c}, nil
}

func (c *conn) exec(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execerCtx, ok := c.Conn.(driver.ExecerContext); ok {
		res, err := execerCtx.ExecContext(ctx, query, args)
		if err == nil || !errors.Is(err, driver.ErrSkip) {
			return res, err
		}
	}

	stmt, err := prepareStmt(ctx, c.Conn, query)
	if err != nil {
		return nil, fmt.Errorf("mtc: failed to prepare statement for exec: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	if se, ok := stmt.(driver.StmtExecContext); ok {
		return se.ExecContext(ctx, args)
	}
	return stmt.Exec(namedValueToValue(args))
}

func (c *conn) query(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
	cleanup func(context.Context) error,
) (driver.Rows, func(context.Context) error, error) {
	if queryerCtx, ok := c.Conn.(driver.QueryerContext); ok {
		rows, err := queryerCtx.QueryContext(ctx, query, args)
		if err == nil || !errors.Is(err, driver.ErrSkip) {
			return rows, cleanup, err
		}
	}

	stmt, err := prepareStmt(ctx, c.Conn, query)
	if err != nil {
		return nil, cleanup, fmt.Errorf("mtc: failed to prepare statement for query: %w", err)
	}

	var (
		rows     driver.Rows
		queryErr error
	)

	if sqc, ok := stmt.(driver.StmtQueryContext); ok {
		rows, queryErr = sqc.QueryContext(ctx, args)
	} else {
		rows, queryErr = stmt.Query(namedValueToValue(args))
	}
	if queryErr != nil {
		_ = stmt.Close()
		return nil, cleanup, queryErr
	}

	stmtCleanup := func(cleanCtx context.Context) error {
		var firstErr error
		if cerr := stmt.Close(); cerr != nil {
			firstErr = fmt.Errorf("mtc: failed to close statement: %w", cerr)
		}
		if rerr := cleanup(cleanCtx); rerr != nil && firstErr == nil {
			firstErr = rerr
		}
		return firstErr
	}

	return rows, stmtCleanup, nil
}

// ResetSession is called when the connection is taken from the pool.
// We proactively RESET to ensure no leftover session vars.
func (c *conn) ResetSession(ctx context.Context) error {
	return resetSetting(ctx, c.Conn, c.settingName)
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
		//lint:ignore SA1019 legacy drivers still rely on Conn.Begin fallback.
		tx, txErr = c.Conn.Begin()
	}
	if txErr != nil {
		return nil, fmt.Errorf("mtc: failed to begin transaction: %w", txErr)
	}

	// Apply transaction-local tenant setting
	tenantID, tErr := c.tenantIDFn(ctx)
	if tErr != nil || tenantID == "" {
		// No tenant → explicit RESET inside this Tx is harmless but optional.
		if e := resetSetting(ctx, c.Conn, c.settingName); e != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("mtc: failed to reset %s: %w", c.settingName, e)
		}
		return tx, nil
	}

	if execErr := execContext(
		ctx,
		c.Conn,
		"SELECT set_config($1, $2, true)",
		driver.NamedValue{Ordinal: 1, Value: c.settingName},
		driver.NamedValue{Ordinal: 2, Value: tenantID},
	); execErr != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("mtc: failed to set %s: %w", c.settingName, execErr)
	}

	return tx, nil
}

// Begin is for drivers that call it directly.
func (c *conn) Begin() (driver.Tx, error) {
	// database/sql won't pass a context here; we can’t fetch tenant from context.
	// Use empty context; tenant setting will be RESET (no tenant) in this path.
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// resetSetting issues a safe RESET <ident>. Since RESET cannot be parameterized,
// we strictly validate the identifier to prevent SQL injection.
func resetSetting(ctx context.Context, cn driver.Conn, name string) error {
	return execContext(ctx, cn, "RESET "+name)
}

// execContext executes a statement with best available driver interfaces.
// 1) driver.ExecerContext
// 2) Prepare + driver.StmtExecContext
// 3) Prepare + legacy Stmt.Exec([]driver.Value)
func execContext(ctx context.Context, cn driver.Conn, query string, args ...driver.NamedValue) error {
	// 1) ExecerContext fast path
	if ec, ok := cn.(driver.ExecerContext); ok {
		if _, err := ec.ExecContext(ctx, query, args); err != nil && !errors.Is(err, driver.ErrSkip) {
			return err
		} else if err == nil {
			return nil
		}
	}
	// 2) Prepare + StmtExecContext
	stmt, err := cn.Prepare(query)
	if err != nil {
		return fmt.Errorf("mtc: failed to prepare statement: %w", err)
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
	//lint:ignore SA1019 fallback for drivers that only implement legacy Stmt.Exec.
	if _, err := stmt.Exec(vals); err != nil {
		return fmt.Errorf("mtc: failed to execute legacy query %q: %w", query, err)
	}
	return nil
}

func prepareStmt(ctx context.Context, cn driver.Conn, query string) (driver.Stmt, error) {
	if pc, ok := cn.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return cn.Prepare(query)
}

func namedValueToValue(args []driver.NamedValue) []driver.Value {
	if len(args) == 0 {
		return nil
	}
	vals := make([]driver.Value, len(args))
	for i, nv := range args {
		vals[i] = nv.Value
	}
	return vals
}

func (c *conn) attachTenantSession(ctx context.Context) (func(context.Context) error, error) {
	tenantID, err := c.tenantIDFn(ctx)
	if err != nil || tenantID == "" {
		// Ensure previous session-level values are cleared when no tenant is supplied.
		if err := resetSetting(ctx, c.Conn, c.settingName); err != nil {
			return nil, fmt.Errorf("mtc: failed to reset %s: %w", c.settingName, err)
		}
		return func(context.Context) error { return nil }, nil
	}

	if err := execContext(
		ctx,
		c.Conn,
		"SELECT set_config($1, $2, false)",
		driver.NamedValue{Ordinal: 1, Value: c.settingName},
		driver.NamedValue{Ordinal: 2, Value: tenantID},
	); err != nil {
		return nil, fmt.Errorf("mtc: failed to set %s: %w", c.settingName, err)
	}

	return func(cleanCtx context.Context) error {
		return resetSetting(cleanCtx, c.Conn, c.settingName)
	}, nil
}

type tenantAwareRows struct {
	driver.Rows
	cleanup func(context.Context) error
	ctx     context.Context
	once    sync.Once
}

func (r *tenantAwareRows) Close() error {
	closeErr := r.Rows.Close()
	cleanupErr := r.runCleanup()
	if closeErr != nil {
		return closeErr
	}
	return cleanupErr
}

func (r *tenantAwareRows) runCleanup() error {
	var cleanupErr error
	r.once.Do(func() {
		if r.cleanup != nil {
			cleanupErr = r.cleanup(r.ctx)
		}
	})
	return cleanupErr
}

func (r *tenantAwareRows) NextResultSet() error {
	if nrs, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return nrs.NextResultSet()
	}
	return driver.ErrSkip
}

func (r *tenantAwareRows) HasNextResultSet() bool {
	if nrs, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return nrs.HasNextResultSet()
	}
	return false
}

type tenantAwareStmt struct {
	driver.Stmt
	conn *conn
}

func (s *tenantAwareStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	cleanup, err := s.conn.attachTenantSession(ctx)
	if err != nil {
		return nil, err
	}

	var (
		res     driver.Result
		execErr error
	)

	if se, ok := s.Stmt.(driver.StmtExecContext); ok {
		res, execErr = se.ExecContext(ctx, args)
		if errors.Is(execErr, driver.ErrSkip) {
			execErr = nil
			res, execErr = s.Stmt.Exec(namedValueToValue(args))
		}
	} else {
		res, execErr = s.Stmt.Exec(namedValueToValue(args))
	}

	if cerr := cleanup(ctx); cerr != nil && execErr == nil {
		execErr = cerr
	}
	return res, execErr
}

func (s *tenantAwareStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	cleanup, err := s.conn.attachTenantSession(ctx)
	if err != nil {
		return nil, err
	}

	var (
		rows     driver.Rows
		queryErr error
	)

	if sqc, ok := s.Stmt.(driver.StmtQueryContext); ok {
		rows, queryErr = sqc.QueryContext(ctx, args)
		if errors.Is(queryErr, driver.ErrSkip) {
			rows, queryErr = s.Stmt.Query(namedValueToValue(args))
		}
	} else {
		rows, queryErr = s.Stmt.Query(namedValueToValue(args))
	}

	if queryErr != nil {
		_ = cleanup(ctx)
		return nil, queryErr
	}

	return &tenantAwareRows{
		Rows:    rows,
		cleanup: cleanup,
		ctx:     ctx,
	}, nil
}

func (s *tenantAwareStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.Stmt.Exec(args)
}

func (s *tenantAwareStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.Stmt.Query(args)
}

var (
	_ driver.Connector          = (*Connector)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.StmtExecContext    = (*tenantAwareStmt)(nil)
	_ driver.StmtQueryContext   = (*tenantAwareStmt)(nil)
)
