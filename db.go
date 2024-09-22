package dbrouter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"time"
)

// DB represents a logical database that manages multiple underlying physical databases.
// It includes a single read-write (RW) database and multiple read-only (RO) databases.
// The system automatically directs read and write operations to the appropriate database.
type DB struct {
	readWriteDB      *sql.DB   // The primary (master) database for read-write operations.
	readOnlyDBs      []*sql.DB // List of replication databases for read-only operations.
	totalConnections int       // Total number of database connections (1 RW + n RO databases).
	readOnlyCount    uint64    // Counter for load balancing read-only queries (used in round-robin).
}

// Open initializes the connection to all physical databases (both RW and RO) concurrently.
// `dataSourceNames` is a semicolon-separated list where the first entry is the RW database,
// and subsequent entries are RO databases.
func Open(driverName, dataSourceName string) (*DB, error) {
	conns := strings.Split(dataSourceName, ";")
	db := &DB{
		readOnlyDBs:      make([]*sql.DB, len(conns)-1), // Allocate space for all RO databases.
		totalConnections: len(conns),                    // Total connections = 1 RW + (n - 1) RO databases.
	}

	// Open all databases (RW and RO) concurrently.
	err := db.openConnections(driverName, conns)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Close gracefully closes all database connections (RW and RO) concurrently.
func (db *DB) Close() error {
	return doConcurrent(db.totalConnections, func(i int) error {
		if i == 0 {
			// Close the RW (master) database connection.
			return db.readWriteDB.Close()
		}
		// Close the RO (replica) database connection.
		return db.readOnlyDBs[i-1].Close()
	})
}

// Driver retrieves the driver for the RW database.
func (db *DB) Driver() driver.Driver {
	return db.readWriteDB.Driver()
}

// Begin initiates a transaction on the RW (master) database.
func (db *DB) Begin() (*sql.Tx, error) {
	return db.ReadWrite().Begin()
}

// BeginTx starts a transaction on the RW database with the given context and transaction options.
// The transaction options (TxOptions) can be nil, and if the isolation level is unsupported by the driver, it returns an error.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.ReadWrite().BeginTx(ctx, opts)
}

// Exec executes a query on the RW database without returning rows (e.g., INSERT, UPDATE).
// Arguments are provided for query placeholders.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query with context on the RW database without returning rows.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies that all database connections (RW and RO) are still alive, reconnecting if necessary.
func (db *DB) Ping() error {
	return doConcurrent(db.totalConnections, func(i int) error {
		if i == 0 {
			return db.readWriteDB.Ping()
		}
		return db.readOnlyDBs[i-1].Ping()
	})
}

// PingContext performs the same operation as Ping but allows passing a context for cancellation or timeouts.
func (db *DB) PingContext(ctx context.Context) error {
	return doConcurrent(db.totalConnections, func(i int) error {
		if i == 0 {
			return db.readWriteDB.PingContext(ctx)
		}
		return db.readOnlyDBs[i-1].PingContext(ctx)
	})
}

// Prepare creates a prepared statement for execution on the RW and RO databases.
// It generates a statement for each database concurrently.
func (db *DB) Prepare(query string) (Stmt, error) {
	stmt := &stmt{db: db}
	roStmts := make([]*sql.Stmt, len(db.readOnlyDBs))
	err := doConcurrent(db.totalConnections, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = db.readWriteDB.Prepare(query)
			return err
		}
		// Prepare statement for each RO database.
		roStmts[i-1], err = db.readOnlyDBs[i-1].Prepare(query)
		return err
	})
	if err != nil {
		return nil, err
	}
	stmt.rostmts = roStmts
	return stmt, nil
}

// PrepareContext creates a prepared statement using context for each underlying database (RW and RO).
// The context is used during the preparation phase, not during execution.
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := &stmt{db: db}
	roStmts := make([]*sql.Stmt, len(db.readOnlyDBs))
	err := doConcurrent(db.totalConnections, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = db.readWriteDB.PrepareContext(ctx, query)
			return err
		}
		// Prepare context-based statement for RO databases.
		roStmts[i-1], err = db.readOnlyDBs[i-1].PrepareContext(ctx, query)
		return err
	})
	if err != nil {
		return nil, err
	}
	stmt.rostmts = roStmts
	return stmt, nil
}

// Query executes a query on a RO database (selected using round-robin) and returns rows.
// This is typically used for SELECT statements.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadOnly().Query(query, args...)
}

// QueryContext executes a query with a context on a RO database and returns rows.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadOnly().QueryContext(ctx, query, args...)
}

// QueryRow executes a query on a RO database and returns at most one row.
// This method always returns a non-nil result, with errors deferred until Scan is called.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.ReadOnly().QueryRow(query, args...)
}

// QueryRowContext executes a query on a RO database and returns a single row with the provided context.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.ReadOnly().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of idle connections allowed for each database (RW and RO).
// If n <= 0, no idle connections will be retained.
func (db *DB) SetMaxIdleConns(n int) {
	db.readWriteDB.SetMaxIdleConns(n)
	for _, roDB := range db.readOnlyDBs {
		roDB.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections for each database.
// If n <= 0, there is no limit on the number of open connections.
func (db *DB) SetMaxOpenConns(n int) {
	db.readWriteDB.SetMaxOpenConns(n)
	for _, roDB := range db.readOnlyDBs {
		roDB.SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection can remain open before being closed.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.readWriteDB.SetConnMaxLifetime(d)
	for _, roDB := range db.readOnlyDBs {
		roDB.SetConnMaxLifetime(d)
	}
}

// ReadOnly returns a read-only database (selected via round-robin) to distribute read queries evenly.
func (db *DB) ReadOnly() *sql.DB {
	if db.totalConnections == 1 {
		return db.readWriteDB // No read-only databases available.
	}
	return db.readOnlyDBs[db.roundRobin(len(db.readOnlyDBs))]
}

// ReadWrite returns the read-write (master) database for write operations.
func (db *DB) ReadWrite() *sql.DB {
	return db.readWriteDB
}

// roundRobin selects a read-only database based on a round-robin load balancing algorithm.
func (db *DB) roundRobin(n int) int {
	return int(atomic.AddUint64(&db.readOnlyCount, 1) % uint64(n))
}

// openConnections concurrently opens all database connections (RW and RO).
func (db *DB) openConnections(driverName string, conns []string) error {
	return doConcurrent(db.totalConnections, func(i int) error {
		var err error
		if i == 0 {
			// Open the RW (master) database.
			db.readWriteDB, err = sql.Open(driverName, conns[i])
		} else {
			// Open the RO (replica) databases.
			db.readOnlyDBs[i-1], err = sql.Open(driverName, conns[i])
		}
		return err
	})
}
