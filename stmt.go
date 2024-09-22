package dbrouter

import (
	"context"
	"database/sql"
)

// Stmt represents an aggregate prepared statement.
// It holds a prepared statement for both the RW and RO databases.
type Stmt interface {
	Close() error
	Exec(...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
	Query(...interface{}) (*sql.Rows, error)
	QueryRow(...interface{}) *sql.Row
}

// stmt is the internal implementation of the Stmt interface.
type stmt struct {
	db      *DB         // Reference to the DB instance.
	rwstmt  *sql.Stmt   // Prepared statement for the RW database.
	rostmts []*sql.Stmt // Prepared statements for the RO databases.
}

// Close closes all prepared statements (RW and RO) concurrently.
func (s *stmt) Close() error {
	return doConcurrent(s.db.totalConnections, func(i int) error {
		if i == 0 {
			return s.rwstmt.Close() // Close RW statement.
		}
		return s.rostmts[i-1].Close() // Close RO statement.
	})
}

// Exec executes a prepared statement on the RW database with the provided arguments.
// It returns a Result summarizing the effect of the statement.
func (s *stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.RWStmt().Exec(args...)
}

// ExecContext executes a prepared statement with context on the RW database.
func (s *stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	return s.RWStmt().ExecContext(ctx, args...)
}

// Query executes a prepared query on a RO database and returns the results as *sql.Rows.
func (s *stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.ROStmt().Query(args...)
}

// QueryContext executes a prepared query with context on a RO database and returns the results as *sql.Rows.
func (s *stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	return s.ROStmt().QueryContext(ctx, args...)
}

// QueryRow executes a query on a RO database and returns at most one row.
// Errors are deferred until Scan is called on the returned *sql.Row.
func (s *stmt) QueryRow(args ...interface{}) *sql.Row {
	return s.ROStmt().QueryRow(args...)
}

// QueryRowContext executes a query with context on a RO database, returning a single row.
// Errors are deferred until Scan is called on the returned *sql.Row.
func (s *stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	return s.ROStmt().QueryRowContext(ctx, args...)
}

// ROStmt returns the prepared statement for a RO database, selected using round-robin.
func (s *stmt) ROStmt() *sql.Stmt {
	if len(s.rostmts) == 0 {
		return s.rwstmt // If no RO statements exist, fall back to the RW statement.
	}
	return s.rostmts[s.db.roundRobin(len(s.rostmts))]
}

// RWStmt returns the prepared statement for the RW (master) database.
func (s *stmt) RWStmt() *sql.Stmt {
	return s.rwstmt
}
