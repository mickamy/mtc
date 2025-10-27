# mtc — Multi-Tenant Connector for Go (RLS + Context)

`mtc` is a lightweight Go library that automatically applies tenant-scoped
context values (like `app.tenant_id`) to PostgreSQL connections using
Row-Level Security (RLS).
