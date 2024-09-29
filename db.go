package dbrouter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"time"
)

type DB interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	Driver() driver.Driver
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
}

// DBImplementation represents a logical database that manages multiple underlying physical databases.
// It includes a single read-write (RW) database and multiple read-only (RO) databases.
// The system automatically directs read and write operations to the appropriate database.
type DBImplementation struct {
	readWriteDB      *sql.DB   // The primary (master) database for read-write operations.
	readOnlyDBs      []*sql.DB // List of replication databases for read-only operations.
	totalConnections int       // Total number of database connections (1 RW + n RO databases).
	readOnlyCount    uint64    // Counter for load balancing read-only queries (used in round-robin).
}

// Open initializes the connection to all physical databases (both RW and RO) concurrently.
// `dataSourceNames` is a semicolon-separated list where the first entry is the RW database,
// and subsequent entries are RO databases.
func Open(driverName, dataSourceName string) (*DBImplementation, error) {
	conns := strings.Split(dataSourceName, ";")
	dbImplementation := &DBImplementation{
		readOnlyDBs:      make([]*sql.DB, len(conns)-1), // Allocate space for all RO databases.
		totalConnections: len(conns),                    // Total connections = 1 RW + (n - 1) RO databases.
	}

	// Open all databases (RW and RO) concurrently.
	err := dbImplementation.openConnections(driverName, conns)
	if err != nil {
		return nil, err
	}

	return dbImplementation, nil
}

// Close gracefully closes all database connections (RW and RO) concurrently.
func (dbImplementation *DBImplementation) Close() error {
	return doConcurrent(dbImplementation.totalConnections, func(i int) error {
		if i == 0 {
			// Close the RW (master) database connection.
			return dbImplementation.readWriteDB.Close()
		}
		// Close the RO (replica) database connection.
		return dbImplementation.readOnlyDBs[i-1].Close()
	})
}

// Driver retrieves the driver for the RW database.
func (dbImplementation *DBImplementation) Driver() driver.Driver {
	return dbImplementation.readWriteDB.Driver()
}

// Begin initiates a transaction on the RW (master) database.
func (dbImplementation *DBImplementation) Begin() (*sql.Tx, error) {
	return dbImplementation.ReadWrite().Begin()
}

// BeginTx starts a transaction on the RW database with the given context and transaction options.
// The transaction options (TxOptions) can be nil, and if the isolation level is unsupported by the driver, it returns an error.
func (dbImplementation *DBImplementation) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return dbImplementation.ReadWrite().BeginTx(ctx, opts)
}

// Exec executes a query on the RW database without returning rows (e.g., INSERT, UPDATE).
// Arguments are provided for query placeholders.
func (dbImplementation *DBImplementation) Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbImplementation.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query with context on the RW database without returning rows.
func (dbImplementation *DBImplementation) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbImplementation.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies that all database connections (RW and RO) are still alive, reconnecting if necessary.
func (dbImplementation *DBImplementation) Ping() error {
	return doConcurrent(dbImplementation.totalConnections, func(i int) error {
		if i == 0 {
			return dbImplementation.readWriteDB.Ping()
		}
		return dbImplementation.readOnlyDBs[i-1].Ping()
	})
}

// PingContext performs the same operation as Ping but allows passing a context for cancellation or timeouts.
func (dbImplementation *DBImplementation) PingContext(ctx context.Context) error {
	return doConcurrent(dbImplementation.totalConnections, func(i int) error {
		if i == 0 {
			return dbImplementation.readWriteDB.PingContext(ctx)
		}
		return dbImplementation.readOnlyDBs[i-1].PingContext(ctx)
	})
}

// Prepare creates a prepared statement for execution on the RW and RO databases.
// It generates a statement for each database concurrently.
func (dbImplementation *DBImplementation) Prepare(query string) (Stmt, error) {
	stmt := &stmt{db: dbImplementation}
	roStmts := make([]*sql.Stmt, len(dbImplementation.readOnlyDBs))
	err := doConcurrent(dbImplementation.totalConnections, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = dbImplementation.readWriteDB.Prepare(query)
			return err
		}
		// Prepare statement for each RO database.
		roStmts[i-1], err = dbImplementation.readOnlyDBs[i-1].Prepare(query)
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
func (dbImplementation *DBImplementation) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := &stmt{db: dbImplementation}
	roStmts := make([]*sql.Stmt, len(dbImplementation.readOnlyDBs))
	err := doConcurrent(dbImplementation.totalConnections, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = dbImplementation.readWriteDB.PrepareContext(ctx, query)
			return err
		}
		// Prepare context-based statement for RO databases.
		roStmts[i-1], err = dbImplementation.readOnlyDBs[i-1].PrepareContext(ctx, query)
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
func (dbImplementation *DBImplementation) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return dbImplementation.ReadOnly().Query(query, args...)
}

// QueryContext executes a query with a context on a RO database and returns rows.
func (dbImplementation *DBImplementation) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return dbImplementation.ReadOnly().QueryContext(ctx, query, args...)
}

// QueryRow executes a query on a RO database and returns at most one row.
// This method always returns a non-nil result, with errors deferred until Scan is called.
func (dbImplementation *DBImplementation) QueryRow(query string, args ...interface{}) *sql.Row {
	return dbImplementation.ReadOnly().QueryRow(query, args...)
}

// QueryRowContext executes a query on a RO database and returns a single row with the provided context.
func (dbImplementation *DBImplementation) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return dbImplementation.ReadOnly().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of idle connections allowed for each database (RW and RO).
// If n <= 0, no idle connections will be retained.
func (dbImplementation *DBImplementation) SetMaxIdleConns(n int) {
	dbImplementation.readWriteDB.SetMaxIdleConns(n)
	for _, roDB := range dbImplementation.readOnlyDBs {
		roDB.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections for each database.
// If n <= 0, there is no limit on the number of open connections.
func (dbImplementation *DBImplementation) SetMaxOpenConns(n int) {
	dbImplementation.readWriteDB.SetMaxOpenConns(n)
	for _, roDB := range dbImplementation.readOnlyDBs {
		roDB.SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection can remain open before being closed.
func (dbImplementation *DBImplementation) SetConnMaxLifetime(d time.Duration) {
	dbImplementation.readWriteDB.SetConnMaxLifetime(d)
	for _, roDB := range dbImplementation.readOnlyDBs {
		roDB.SetConnMaxLifetime(d)
	}
}

// SetConnMaxIdleTime sets the maximum amount of time a connection can remain idle before being closed.
func (dbImplementation *DBImplementation) SetConnMaxIdleTime(d time.Duration) {
	dbImplementation.readWriteDB.SetConnMaxIdleTime(d)
	for i := range dbImplementation.readOnlyDBs {
		dbImplementation.readOnlyDBs[i].SetConnMaxIdleTime(d)
	}
}

// ReadOnly returns a read-only database (selected via round-robin) to distribute read queries evenly.
func (dbImplementation *DBImplementation) ReadOnly() *sql.DB {
	if dbImplementation.totalConnections == 1 {
		return dbImplementation.readWriteDB // No read-only databases available.
	}
	return dbImplementation.readOnlyDBs[dbImplementation.roundRobin(len(dbImplementation.readOnlyDBs))]
}

// ReadWrite returns the read-write (master) database for write operations.
func (dbImplementation *DBImplementation) ReadWrite() *sql.DB {
	return dbImplementation.readWriteDB
}

// roundRobin selects a read-only database based on a round-robin load balancing algorithm.
func (dbImplementation *DBImplementation) roundRobin(n int) int {
	return int(atomic.AddUint64(&dbImplementation.readOnlyCount, 1) % uint64(n))
}

// openConnections concurrently opens all database connections (RW and RO).
func (dbImplementation *DBImplementation) openConnections(driverName string, conns []string) error {
	return doConcurrent(dbImplementation.totalConnections, func(i int) error {
		var err error
		if i == 0 {
			// Open the RW (master) database.
			dbImplementation.readWriteDB, err = sql.Open(driverName, conns[i])
		} else {
			// Open the RO (replica) databases.
			dbImplementation.readOnlyDBs[i-1], err = sql.Open(driverName, conns[i])
		}
		return err
	})
}
