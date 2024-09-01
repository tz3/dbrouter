package dbrouter

import (
	"database/sql"
	"database/sql/driver"
	"strings"
)

type DB struct {
	readWriteDB      *sql.DB   // master database
	readOnlyDBs      []*sql.DB // replication Databases
	totalConnections int       // total concurrent connections
	readOnlyCount    uint64    // increasing monotonically on each query
}

func Open(driverName string, dataSourceName string) (*DB, error) {
	conns := strings.Split(dataSourceName, ";")
	db := &DB{
		readOnlyDBs:      make([]*sql.DB, len(conns)-1), // minus the master DB
		totalConnections: len(conns),
	}

	err := doConcurrent(db.totalConnections, func(i int) (err error) {
		// open connection to master db
		if i == 0 {
			db.readWriteDB, err = sql.Open(driverName, conns[i])
			return err
		}

		// open connection to other replica db
		var readOnlyDB *sql.DB
		readOnlyDB, err = sql.Open(driverName, conns[i])
		if err != nil {
			return
		}
		db.readOnlyDBs[i-1] = readOnlyDB
		return err
	})

	return db, err

}

func (db *DB) Close() error {
	return doConcurrent(db.totalConnections, func(i int) (err error) {
		// close connection to master db
		if i == 0 {
			return db.readWriteDB.Close()
		}

		// close connection to replica db
		return db.readOnlyDBs[i-1].Close()
	})
}

func (db *DB) Driver() driver.Driver {
	return db.readWriteDB.Driver()
}

// ----------
// Todo:- add Begin
// Todo:- add BeginTx
// Todo:- add Exec
// Todo:- add ExecContext
// Todo:- add Ping
// Todo:- add PingContext
// Todo:- add Prepare
// Todo:- add PrepareContext
// Todo:- add Query
// Todo:- add QueryContext
// Todo:- add QueryRow
// Todo:- add QueryRowContext
// Todo:- add SetMaxIdleConns
// Todo:- add SetMaxOpenConns
// Todo:- add SetConnMaxLifetime
// Todo:- add ReadOnly
// Todo:- add ReadWrite
// Todo:- add roundRobin
