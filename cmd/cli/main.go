package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reporter/internal/db"
	"time"

	"github.com/joho/godotenv"
)

func main() {

	from := flag.String("f", "", "Start date of the report in the format 2006-01-02 15:04:05.")
	to := flag.String("t", "", "End date of the report in the format 2006-01-02 15:04:05.")
	env := flag.String("e", "dev", "Env file to be used for configs.")
	flag.Parse()

	godotenv.Load(fmt.Sprintf("%s.env", *env))
	ValidateEnvVariables()

	// when IB report functionality is moved out of the main func:
	// reportType := flag.String("r", "ib_report", "The type of the report to be generated.")

	// Datetime format:
	//  '2016-01-01 00:00:00'

	// data for LQD ===> BETWEEN  '2017-01-01 00:00:00' AND '2017-03-01 00:00:00'

	if *from == "" && *to == "" {
		*to = time.Now().Format("2006-01-02 15:04:05")
		*from = time.Now().Add(-1 * time.Hour * 24 * 30).Format("2006-01-02 15:04:05")
	} else if *from == "" {
		toTime, err := time.Parse(*to, "2006-01-02 15:04:05")
		if err != nil {
			log.Fatalf("parse *to time: %s", err)
		}
		*from = toTime.Add(-1 * time.Hour * 24 * 30).Format("2006-01-02 15:04:05")
	} else if *to == "" {
		fromTime, err := time.Parse(*from, "2006-01-02 15:04:05")
		if err != nil {
			log.Fatalf("parse *from time: %s", err)
		}
		*to = fromTime.Add(time.Hour * 24 * 30).Format("2006-01-02 15:04:05")
	}

	startTime := time.Now()

	fmt.Println("====================== MT4 Data =======================")

	replicaMt4 := db.NewReplicaMySQL(
		os.Getenv("MT4_DB_USERNAME"),
		os.Getenv("MT4_DB_PASSWORD"),
		os.Getenv("MT4_DB_HOST"),
		os.Getenv("MT4_DB_PORT"),
		os.Getenv("MT4_DB_DATABSE_NAME"))

	resMt4 := replicaMt4.GetReplicaDBData(*from, *to, db.MT4_QUERY)
	//fmt.Println(resMt4)

	fmt.Println("======================= Pamm Data ======================")

	replicaPamm := db.NewReplicaMySQL(
		os.Getenv("PAMM_DB_USERNAME"),
		os.Getenv("PAMM_DB_PASSWORD"),
		os.Getenv("PAMM_DB_HOST"),
		os.Getenv("PAMM_DB_PORT"),
		os.Getenv("PAMM_DB_DATABSE_NAME"))

	resPamm := replicaPamm.GetReplicaDBData(*from, *to, db.PAMM_QUERY)
	//fmt.Println(resPamm)

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
	fmt.Println("====================== Get all unique Trading Accounts =======================")

	var lqdDB *db.LqdMySQL
	if *env == "prd" {
		lqdDB = db.NewLqdMySQL_SSH_Tunnel(
			os.Getenv("LQD_DB_USERNAME"),
			os.Getenv("LQD_DB_PASSWORD"),
			os.Getenv("LQD_DB_HOST"),
			os.Getenv("LQD_DB_PORT"),
			os.Getenv("LQD_DB_DATABSE_NAME"))
	} else {
		lqdDB = db.NewLqdMySQL(
			os.Getenv("LQD_DB_USERNAME"),
			os.Getenv("LQD_DB_PASSWORD"),
			os.Getenv("LQD_DB_HOST"),
			os.Getenv("LQD_DB_PORT"),
			os.Getenv("LQD_DB_DATABSE_NAME"))
	}

	ibToTradingAccMap := lqdDB.GetAccountIBData(allAccounts, os.Getenv("LQD_DB_DATABSE_NAME"))

	fmt.Println("===================== Get IB Data =======================")
	ibData := lqdDB.GetIBReportData(*from, *to, os.Getenv("LQD_DB_DATABSE_NAME"))
	//fmt.Println(ibData)
	fmt.Println("===================== Generate report =======================")

	fileName := time.Now().Format("20060102150405")

	repFile, err := os.Create(fmt.Sprintf("generated/%s.%s.txt", fileName, *env))
	if err != nil {
		log.Fatalf("creating new file: %s", err)
	}
	defer repFile.Close()

	header := fmt.Sprintf("IB\t\tEquity\t\tDeposits\t\tWithdrawls\t\tVolume\t\tOpenProfit\t\tClosedProfit\t\tCommissions\n")
	repFile.Write([]byte(header))

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

		row := fmt.Sprintf("%d\t\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f\n",
			res.IBID, res.Equity, res.DepositsTotal,
			res.WithdrawalsTotal, res.Volume,
			res.OpenProfit, res.ClosedProfit, res.Commissions)

		repFile.Write([]byte(row))
	}

	duration := time.Since(startTime)
	// fmt.Printf("Report for time frame %s - %s\n", *from, *to)
	fmt.Printf("Generation took %s duration.\n", duration)
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
