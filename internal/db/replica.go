package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type ReplicaDBResult struct {
	TradingAccount int
	Volume         float32
	OpenProfit     float32
	ClosedProfit   float32
}

type ReplicaMySQL struct {
	db *sql.DB
}

func NewReplicaMySQL(user, password, host, port, dbName string) *ReplicaMySQL {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?multiStatements=true", user, password, host, port, dbName))
	if err != nil {
		log.Fatal(fmt.Errorf("creating new MySQL object: %s", err))
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(fmt.Errorf("db ping: %s", err))
	}
	return &ReplicaMySQL{
		db: db,
	}
}

func (lqd *ReplicaMySQL) GetReplicaDBData(startDate, endDate, query string) map[int]ReplicaDBResult {
	paramQuery := query
	paramQuery = strings.Replace(paramQuery, "<startDate>", startDate, 1)
	paramQuery = strings.Replace(paramQuery, "<endDate>", endDate, 1)

	//fmt.Print(paramQuery, "\n")

	// Cannot use Prepared statements for multiple statements SQL
	// for some strange reason .... (-_-)
	rows, err := lqd.db.Query(paramQuery)
	if err != nil {
		log.Fatal(fmt.Errorf("execute query: %s", err))
	}
	defer rows.Close()

	resMap := make(map[int]ReplicaDBResult, 0)
	for rows.Next() {
		row := ReplicaDBResult{}
		rows.Scan(&row.TradingAccount, &row.Volume, &row.OpenProfit, &row.ClosedProfit)
		resMap[row.TradingAccount] = row
	}

	return resMap
}
