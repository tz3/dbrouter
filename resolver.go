package dbrouter

import "database/sql"

// WrapDBs initializes a DB instance with the provided database connections.
// The first connection in the argument list is used as the primary (RW) database,
// while the remaining connections are treated as read-only (RO) databases.
// If no connections are provided, it will panic, as an RW connection is mandatory.
func WrapDBs(dbs ...*sql.DB) DB {
	if len(dbs) == 0 {
		panic("at least one RW connection is required")
	}
	return &DBImplementation{
		readWriteDB:      dbs[0],   // First DB is the RW (primary) database.
		readOnlyDBs:      dbs[1:],  // Subsequent DBs are RO (replica) databases.
		totalConnections: len(dbs), // Total number of connections (RW + RO).
	}
}
