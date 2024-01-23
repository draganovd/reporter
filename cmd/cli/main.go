package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"reporter/internal/db"

	"github.com/joho/godotenv"
)

func main() {
	envFile := flag.String("e", "dev.env", "Env file to be used for configs.")
	godotenv.Load(*envFile)
	ValidateEnvVariables()

	// when IB report functionality is moved out of the main func:
	// reportType := flag.String("r", "ib_report", "The type of the report to be generated.")

	from := flag.String("f", "", "Start date of the report.")
	to := flag.String("t", "", "End date of the report.")

	// Datetime format:
	//  '2016-01-01 00:00:00'

	if *from == "" && *to == "" {
		*to = time.Now().Format("2006-01-02 15:04:05")
		*from = time.Now().Add(-1 * time.Hour * 24 * 30).Format("2006-01-02 15:04:05")
	}

	fmt.Println("====================== MT4 Data =======================")

	replicaMt4 := db.NewReplicaMySQL("replica_user", "&v*GF2Y&etmCWq5t", "localhost", "3336", "lqd")
	resMt4 := replicaMt4.GetReplicaDBData(*from, *to, db.MT4_QUERY)
	//fmt.Println(resMt4)

	fmt.Println("======================= Pamm Data ======================")

	replicaPamm := db.NewReplicaMySQL("replica_user", "&v*GF2Y&etmCWq5t", "localhost", "3336", "lqd_pamm")
	resPamm := replicaPamm.GetReplicaDBData(*from, *to, db.PAMM_QUERY)
	//fmt.Println(resPamm)

	fmt.Println("====================== Get all unique Trading Accounts =======================")

	allAccounts := make([]int, 0)
	alreadyAdded := make(map[int]struct{})
	for key := range resMt4 {
		if _, ok := alreadyAdded[key]; !ok {
			alreadyAdded[key] = struct{}{}
			allAccounts = append(allAccounts, key)
		}
	}
	for key := range resPamm {
		if _, ok := alreadyAdded[key]; !ok {
			alreadyAdded[key] = struct{}{}
			allAccounts = append(allAccounts, key)
		}
	}

	lqd := db.NewLqdMySQL("investotest", "pass1234", "87.228.230.182", "3306", "lqdfx")

	ibToTradingAccMap := lqd.GetAccountIBData(allAccounts)

	fmt.Println("===================== Get IB Data =======================")
	ibData := lqd.GetIBReportData(*from, *to)
	//fmt.Println(ibData)
	fmt.Println("=============================================")

	fmt.Println("IB\t\tEquity\t\tDeposits\t\tWithdrawls\t\tVolume\t\tOpenProfit\t\tClosedProfit\t\tCommissions")

	for key, lqd := range ibData {
		replicaData := IBAggregate{}
		accounts, ok := ibToTradingAccMap[key]
		if ok {
			for acc := range accounts {
				if v, ok := resMt4[acc]; ok {
					replicaData.Volume += v.Volume
					replicaData.OpenProfit += v.OpenProfit
					replicaData.ClosedProfit += v.ClosedProfit
				}
				if v, ok := resPamm[acc]; ok {
					replicaData.Volume += v.Volume
					replicaData.OpenProfit += v.OpenProfit
					replicaData.ClosedProfit += v.ClosedProfit
				}
			}
		}

		res := ReportRow{
			IBID:             key,
			Equity:           lqd.Equity,
			DepositsTotal:    lqd.Deposits,
			WithdrawalsTotal: lqd.Withdrawals,
			Commissions:      lqd.Commission,

			OpenProfit:   replicaData.OpenProfit,
			ClosedProfit: replicaData.ClosedProfit,
			Volume:       replicaData.Volume,
		}

		//fmt.Println("IB\t\tEquity\t\tDeposits\t\tWithdrawls\t\tVolume\t\tOpenProfit\t\tClosedProfit\t\tCommissions")
		fmt.Printf("%d\t\t%f\t\t%f\t\t%f\t\t%f\t\t%f\t\t%f\t\t%f\n",
			res.IBID, res.Equity, res.DepositsTotal,
			res.WithdrawalsTotal, res.Volume,
			res.OpenProfit, res.ClosedProfit, res.Commissions)
	}

}

type ReportRow struct {
	IBID             int
	Equity           float32
	DepositsTotal    float32
	WithdrawalsTotal float32
	Volume           float32
	OpenProfit       float32
	ClosedProfit     float32
	Commissions      float32
}

type IBAggregate struct {
	IBID         int
	Volume       float32
	OpenProfit   float32
	ClosedProfit float32
}

func ValidateEnvVariables() {
	envVariables := []string{"LQD_DB_HOST",
		"LQD_DB_PORT", "LQD_DB_USERNAME",
		"LQD_DB_PASSWORD", "LQD_DB_DATABSE_NAME",
		"MT4_DB_HOST", "MT4_DB_PORT",
		"MT4_DB_USERNAME", "MT4_DB_PASSWORD",
		"MT4_DB_DATABSE_NAME", "PAMM_DB_HOST",
		"PAMM_DB_PORT", "PAMM_DB_USERNAME",
		"PAMM_DB_PASSWORD", "PAMM_DB_DATABSE_NAME"}

	for _, v := range envVariables {
		envVar := os.Getenv(v)
		if envVar == "" {
			log.Fatalf("environment variable %s is empty", v)
		}
	}
}
