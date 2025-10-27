# mtc — Multi-Tenant Connector for Go (RLS + Context)

`mtc` is a lightweight Go library that automatically applies tenant-scoped
context values (like `app.tenant_id`) to PostgreSQL connections using
Row-Level Security (RLS).

## Features

- Propagates tenant IDs from `context.Context` into transaction-local GUCs.
- Works with any `database/sql` driver; pgx is supported out of the box.
- Validates tenant setting name to avoid SQL injection (`reset setting` safety).
- Keeps session state clean by resetting the tenant setting on pooled connections.

## Installation

```bash
go get github.com/mickamy/mtc
```

## Quick Start

```go
package main

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mickamy/mtc"
)

type tenantKey struct{}

func tenantFromContext(ctx context.Context) (string, error) {
	if v, ok := ctx.Value(tenantKey{}).(string); ok && v != "" {
		return v, nil
	}
	return "", nil
}

func main() {
	connector := mtc.New(
		stdlib.GetDefaultDriver(),
		"postgres://user:pass@localhost:5432/app?sslmode=disable",
		tenantFromContext,
		mtc.WithSettingName("app.tenant_id"),
	)

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx := context.WithValue(context.Background(), tenantKey{}, "tenant-123")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	// Within this transaction the tenant is visible via current_setting.
}
```

`mtc` plays nicely with PostgreSQL RLS policies that depend on
`current_setting('app.tenant_id', true)`.

## License

[MIT](./LICENSE)
