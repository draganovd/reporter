package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type LqdResult struct {
	IBID        int
	Equity      float32
	Deposits    float32
	Withdrawals float32
	Commission  float32
}

type LqdMySQL struct {
	db *sql.DB
}

func NewLqdMySQL(user, password, host, port, dbName string) *LqdMySQL {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?multiStatements=true", user, password, host, port, dbName))
	if err != nil {
		log.Fatal(fmt.Errorf("creating new MySQL object: %s", err))
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(fmt.Errorf("db ping: %s", err))
	}
	return &LqdMySQL{
		db: db,
	}
}

func (lqd *LqdMySQL) GetIBReportData(startDate, endDate string) map[int]LqdResult {

	paramQuery := LQD_BASE_QUERY
	paramQuery = strings.Replace(paramQuery, "<startDate>", startDate, 1)
	paramQuery = strings.Replace(paramQuery, "<endDate>", endDate, 1)

	fmt.Println(paramQuery)
	// Cannot use Prepared statements for multiple statements SQL
	// for some strange reason .... (-_-)
	rows, err := lqd.db.Query(paramQuery)
	if err != nil {
		log.Fatal(fmt.Errorf("execute query: %s", err))
	}
	defer rows.Close()

	resMap := make(map[int]LqdResult, 0)
	for rows.Next() {
		row := LqdResult{}
		rows.Scan(&row.IBID, &row.Equity, &row.Deposits, &row.Withdrawals, &row.Commission)
		resMap[row.IBID] = row
	}

	return resMap
}

type TradingAccountResult struct {
	IBID           int
	TradingAccount int
}

func (lqd *LqdMySQL) GetAccountIBData(accounts []int) map[int]map[int]struct{} {
	accountsFilter := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(accounts)), ","), "[]")
	query := strings.Replace(IB_ID_TO_TRADING_ACCOUNT, "<trading_accounts>", accountsFilter, 1)

	stmt, err := lqd.db.Prepare(query)
	if err != nil {
		log.Fatal(fmt.Errorf("db prepare statement: %s", err))
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(fmt.Errorf("execute query: %s", err))
	}
	defer rows.Close()

	resMap := make(map[int]map[int]struct{}, 0)
	for rows.Next() {
		row := TradingAccountResult{}
		rows.Scan(&row.TradingAccount, &row.IBID)
		if _, ok := resMap[row.IBID]; !ok {
			resMap[row.IBID] = make(map[int]struct{})
		}
		resMap[row.IBID][row.TradingAccount] = struct{}{}
	}

	return resMap
}
