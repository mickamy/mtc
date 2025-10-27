package mtc

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

func TestIntegrationTenantSetting(t *testing.T) {
	const (
		dsn        = "postgres://mtc:mtc@localhost:5432/mtc_test?sslmode=disable"
		testTenant = "tenant-123"
	)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	connector := New(stdlib.GetDefaultDriver(), dsn, tenantIDFromContext)

	db := sql.OpenDB(connector)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	if err := waitForPostgres(ctx, db); err != nil {
		t.Fatalf("database not ready: %v", err)
	}

	tenantCtx := context.WithValue(ctx, tenantContextKey{}, testTenant)

	tx, err := db.BeginTx(tenantCtx, nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	var tenant string
	if err := tx.QueryRowContext(tenantCtx, "select current_setting('app.tenant_id', true)").Scan(&tenant); err != nil {
		t.Fatalf("QueryRowContext() error = %v", err)
	}
	if tenant != testTenant {
		t.Fatalf("expected tenant %q, got %q", testTenant, tenant)
	}
}

func waitForPostgres(ctx context.Context, db *sql.DB) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

type tenantContextKey struct{}

func tenantIDFromContext(ctx context.Context) (string, error) {
	v := ctx.Value(tenantContextKey{})
	if s, ok := v.(string); ok && s != "" {
		return s, nil
	}
	return "", errors.New("tenant id missing from context")
}
