package db

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"

	//"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/ssh"
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

func NewLqdMySQL_SSH_Tunnel(user, password, host, port, dbName string) *LqdMySQL {

	sshKeyPath := "bastion"
	sshKey, err := ioutil.ReadFile(sshKeyPath)
	if err != nil {
		log.Fatal(fmt.Println("Error reading SSH key file:", err))
	}

	signer, err := ssh.ParsePrivateKey(sshKey)
	if err != nil {
		fmt.Println("Error parsing SSH private key:", err)
	}

	// Configure SSH tunnel
	sshConfig := &ssh.ClientConfig{
		User:            "bastion",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshAddress := fmt.Sprintf("%s:%s", "bastion.lqdfx.com", "22")

	sshClient, err := ssh.Dial("tcp", sshAddress, sshConfig)
	if err != nil {
		log.Fatal(fmt.Println("Error establishing SSH connection:", err))

	}

	// defer sshClient.Close()

	mysql.RegisterDialContext("mysql+tcp", func(ctx context.Context, addr string) (net.Conn, error) {
		return sshClient.Dial("tcp", addr)
	})
	// mysql.RegisterTLSConfig("custom", &tls.Config{})
	// dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?tls=custom", "lqduser", "", "lqdfx.ckbqd6ldkpdr.eu-west-2.rds.amazonaws.com", "3306", "lqdfx_clientarea")
	dns := fmt.Sprintf("%s:%s@mysql+tcp(%s:%s)/%s?multiStatements=true", user, password, host, port, dbName)
	db, err := sql.Open("mysql", dns)
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

func (lqd *LqdMySQL) GetIBReportData(startDate, endDate, db string) map[int]LqdResult {

	paramQuery := LQD_BASE_QUERY
	paramQuery = strings.Replace(paramQuery, "<startDate>", startDate, 1)
	paramQuery = strings.Replace(paramQuery, "<endDate>", endDate, 1)
	paramQuery = strings.Replace(paramQuery, "<db>", db, -1)

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

func (lqd *LqdMySQL) GetAccountIBData(accounts []int, db string) map[int]map[int]struct{} {
	accountsFilter := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(accounts)), ","), "[]")
	query := strings.Replace(IB_ID_TO_TRADING_ACCOUNT, "<trading_accounts>", accountsFilter, 1)
	query = strings.Replace(query, "<db>", db, -1)

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
