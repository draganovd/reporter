package db

const PAMM_QUERY string = `
DROP TEMPORARY TABLE IF EXISTS ib_open_profit;
DROP TEMPORARY TABLE IF EXISTS ib_volume;
DROP TEMPORARY TABLE IF EXISTS ib_closed_profit;

CREATE TEMPORARY TABLE ib_open_profit
SELECT LOGIN AS trading_account,
		SUM(PROFIT) AS open_profit
FROM lqd_pamm.MT4_TRADES
WHERE CLOSE_TIME = '1970-01-01 00:00:00'
GROUP BY LOGIN;

CREATE TEMPORARY TABLE ib_volume
SELECT LOGIN AS trading_account,
		SUM(VOLUME) AS volume
FROM lqd_pamm.MT4_TRADES
WHERE CLOSE_TIME = '1970-01-01 00:00:00' OR CLOSE_TIME BETWEEN '<startDate>' AND '<endDate>'
GROUP BY LOGIN;

CREATE TEMPORARY TABLE ib_closed_profit
SELECT LOGIN AS trading_account,
	SUM(PROFIT) AS closed_profit
FROM lqd_pamm.MT4_TRADES
WHERE CLOSE_TIME BETWEEN '<startDate>' AND '<endDate>'
GROUP BY LOGIN;

SELECT v.trading_account AS trading_account,
	v.volume AS volume,
	IF(op.open_profit IS NULL, 0.0, op.open_profit) AS open_profit,
	IF(cp.closed_profit IS NULL, 0.0, cp.closed_profit) AS closed_profit
FROM ib_volume AS v
LEFT JOIN ib_open_profit AS op ON op.trading_account = v.trading_account
LEFT JOIN ib_closed_profit AS cp ON cp.trading_account = v.trading_account;

DROP TEMPORARY TABLE IF EXISTS ib_open_profit;
DROP TEMPORARY TABLE IF EXISTS ib_volume;
DROP TEMPORARY TABLE IF EXISTS ib_closed_profit;
`

const MT4_QUERY string = `
DROP TEMPORARY TABLE IF EXISTS ib_open_profit;
DROP TEMPORARY TABLE IF EXISTS ib_volume;
DROP TEMPORARY TABLE IF EXISTS ib_closed_profit;

CREATE TEMPORARY TABLE ib_open_profit
SELECT LOGIN AS trading_account,
	SUM(PROFIT) AS open_profit
FROM lqd.MT4_TRADES
WHERE CLOSE_TIME = '1970-01-01 00:00:00'
GROUP BY LOGIN;

CREATE TEMPORARY TABLE ib_volume
SELECT LOGIN AS trading_account,
		SUM(VOLUME) AS volume
FROM lqd.MT4_TRADES
GROUP BY LOGIN;

CREATE TEMPORARY TABLE ib_closed_profit
SELECT LOGIN AS trading_account,
		SUM(PROFIT) AS closed_profit
FROM lqd.MT4_TRADES
WHERE CLOSE_TIME BETWEEN '<startDate>' AND '<endDate>'
GROUP BY LOGIN;
      
SELECT v.trading_account AS trading_account,
					v.volume AS volume,
		IF(op.open_profit IS NULL, 0.0, op.open_profit) AS open_profit,
		IF(cp.closed_profit IS NULL, 0.0, cp.closed_profit) AS closed_profit
FROM ib_volume AS v
LEFT JOIN ib_open_profit AS op ON op.trading_account = v.trading_account
LEFT JOIN ib_closed_profit AS cp ON cp.trading_account = v.trading_account;

DROP TEMPORARY TABLE IF EXISTS ib_open_profit;
DROP TEMPORARY TABLE IF EXISTS ib_volume;
DROP TEMPORARY TABLE IF EXISTS ib_closed_profit;`

// const IB_ID_TO_TRADING_ACCOUNT string = `
// SELECT c.ib_id as ib_id,
// 		c.trading_account_id as trading_account
// FROM lqdfx.client_trading_accounts_by_ib as c
// WHERE c.trading_account_id IN (?)`

const IB_ID_TO_TRADING_ACCOUNT string = `
SELECT trading_account_id as trading_account,
		ib_id as ib_id
 FROM <db>.client_trading_accounts_by_ib
WHERE trading_account_id in (<trading_accounts>) 
GROUP BY trading_account_id, ib_id`

const LQD_BASE_QUERY string = `

SET @start = '<startDate>';
SET @end = '<endDate>';


DROP TEMPORARY TABLE IF EXISTS ib_commissions;
DROP TEMPORARY TABLE IF EXISTS ib_equity;
DROP TEMPORARY TABLE IF EXISTS ib_deposits;
DROP TEMPORARY TABLE IF EXISTS ib_withdrawals;


CREATE TEMPORARY TABLE ib_equity
SELECT cib.ib_id as ib_id,
	SUM(e.equity) as equity
FROM <db>.client_trading_accounts_by_ib as cib
LEFT JOIN <db>.lqdfx_equity_report as e ON e.fs_user_id = cib.fs_user_id
GROUP BY cib.ib_id;


CREATE TEMPORARY TABLE ib_commissions
SELECT c.ib_id AS ib_id,
	   SUM(calculated_commission) AS commission
FROM <db>.lqdfx_commission_logs AS c
WHERE c.order_close_date BETWEEN @start AND @end
GROUP BY ib_id;


CREATE TEMPORARY TABLE ib_deposits
SELECT cib.ib_id AS ib_id,
	SUM(d.deposits) AS deposits
FROM <db>.client_trading_accounts_by_ib AS cib
LEFT JOIN (SELECT p.fs_user_id AS user_id,
				SUM(p.value) AS deposits
			FROM <db>.lqdfx_payments AS p
			WHERE p.status = 'approved' AND p.type = 'deposit' AND 
				p.date BETWEEN @start AND @end
			GROUP BY p.fs_user_id) AS d ON cib.fs_user_id = d.user_id
GROUP BY cib.ib_id
HAVING deposits IS NOT NULL;


CREATE TEMPORARY TABLE ib_withdrawals
SELECT cib.ib_id AS ib_id,
	SUM(d.withdrawals) AS withdrawals
FROM <db>.client_trading_accounts_by_ib AS cib
LEFT JOIN (SELECT p.fs_user_id AS user_id,
				SUM(p.value) AS withdrawals
			FROM <db>.lqdfx_payments AS p
			WHERE p.status = 'approved' AND p.type = 'withdrawal' AND 
				p.date BETWEEN @start AND @end
			GROUP BY p.fs_user_id) AS d ON cib.fs_user_id = d.user_id
GROUP BY cib.ib_id
HAVING withdrawals IS NOT NULL;
          
                              
SELECT e.ib_id,
		IF(e.equity IS NULL, 0.00, e.equity) AS equity,
		IF(d.deposits IS NULL, 0.00, d.deposits) AS deposits,
		IF(w.withdrawals IS NULL, 0.00, w.withdrawals) AS withdrawals,
		IF(c.commission IS NULL, 0.00, c.commission) AS commission
FROM ib_equity AS e
LEFT JOIN ib_commissions AS c ON e.ib_id = c.ib_id 
LEFT JOIN ib_deposits AS d ON e.ib_id = d.ib_id    
LEFT JOIN ib_withdrawals AS w ON e.ib_id = w.ib_id;
                   
                                      
DROP TEMPORARY TABLE IF EXISTS ib_commissions;
DROP TEMPORARY TABLE IF EXISTS ib_equity;
DROP TEMPORARY TABLE IF EXISTS ib_deposits;
DROP TEMPORARY TABLE IF EXISTS ib_withdrawals;
`
