package clickhouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/influxdata/telegraf"
	"log"
	"net/url"
	"strconv"
	"strings"
)

type ClickhouseClient struct {

	// DBI example: tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000

	DBI          string
	User         string   `toml:"user"`
	Password     string   `toml:"password"`
	Database     string   `toml:"database"`
	TableName    string   `toml:"tablename"`
	ReadTimeout  int64    `toml:"read_timeout"`
	WriteTimeout int64    `toml:"write_timeout"`
	Hosts        []string `toml:"hosts"`
	Debug        bool     `toml:"debug"`

	db *sql.DB
}

func newClickhouse() *ClickhouseClient {
	return &ClickhouseClient{}
}

func (c *ClickhouseClient) Connect() error {
	var err error

	u, err := buildDsn(c)
	if err != nil {
		return err
	}

	c.DBI = u

	if c.Debug {
		log.Println("DBI=", c.DBI)
	}

	c.db, err = sql.Open("clickhouse", c.DBI)
	if err != nil {
		return err
	}

	return nil
}

func (c *ClickhouseClient) Close() error {
	return nil
}

func (c *ClickhouseClient) Description() string {
	return "Telegraf Output Plugin for Clickhouse"
}

func (c *ClickhouseClient) SampleConfig() string {
	return `
# Schema:
# CREATE TABLE telegraf.metrics(
# 	date Date DEFAULT toDate(ts),
#	name String,
#	tags String,
#	val Float64,
#	ts DateTime,
#	updated DateTime DEFAULT now()
# ) ENGINE=MergeTree(date,(name,tags,ts),8192)

  user = "default"
  password = ""
  database = "telegraf"
  tablename = "metrics"
  read_timeout = 10
  write_timeout = 10
  hosts = [ "127.0.0.1:9000" ]
  debug = false
`
}

func (c *ClickhouseClient) Write(metrics []telegraf.Metric) (err error) {
	err = nil
	var batchMetrics []clickhouseMetrics

	if c.Debug {
		log.Println("Recv Telegraf Metrics:", metrics)
	}

	for _, metric := range metrics {
		var tmpClickhouseMetrics clickhouseMetrics

		tmpClickhouseMetrics = *newClickhouseMetrics(metric)

		batchMetrics = append(batchMetrics, tmpClickhouseMetrics)
	}

	if c.Debug {
		log.Println("Replace Metrics to Clickhouse Format ", batchMetrics)
	}

	if err = c.db.Ping(); err != nil {
		if c.Debug {
			if exception, ok := err.(*clickhouse.Exception); ok {
				log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			} else {
				log.Println(err)
			}
		}
		return err
	}

	// create database
	stmtCreateDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.Database)
	if c.Debug {
		log.Println("Create Database: ", stmtCreateDatabase)
	}
	_, err = c.db.Exec(stmtCreateDatabase)
	if err != nil {
		if c.Debug {
			log.Println(err.Error())
		}
		return err
	}

	// create table
	stmtCreateTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s(
		date Date DEFAULT toDate(ts),
		name String,
		tags String,
		val Float64,
		ts DateTime,
		updated DateTime DEFAULT now()
	) ENGINE=MergeTree(date,(name,tags,ts),8192)
	`, c.Database, c.TableName)

	if c.Debug {
		log.Println("Create Table :", stmtCreateTable)
	}
	_, err = c.db.Exec(stmtCreateTable)
	if err != nil {
		if c.Debug {
			log.Fatal(err.Error())
		}
		return err
	}

	// start transaction
	Tx, err := c.db.Begin()
	if c.Debug {
		log.Println("Starting Transaction.")
	}
	if err != nil {
		if c.Debug {
			log.Fatal(err.Error())
		}
		return err
	}

	// Prepare stmt
	stmtInsertData := fmt.Sprintf("INSERT INTO %s.%s(name,tags,val,ts) VALUES(?,?,?,?)", c.Database, c.TableName)
	Stmt, err := Tx.Prepare(stmtInsertData)
	if err != nil {
		if c.Debug {
			log.Println(err.Error())
		}
		return err
	}
	defer Stmt.Close()

	for _, metrs := range batchMetrics {
		for _, metr := range metrs {
			tags, _ := json.Marshal(metr.Tags)
			if c.Debug {
				log.Println(
					"Name:", metr.Name,
					"Tags:", string(tags),
					"Val:", metr.Val,
					"Ts:", metr.Ts,
				)
			}
			if _, err := Stmt.Exec(
				metr.Name,
				string(tags),
				metr.Val,
				metr.Ts,
			); err != nil {
				if c.Debug {
					fmt.Println(err.Error())
				}
			}
		}
	}

	// commit transaction.
	if err := Tx.Commit(); err != nil {
		return err
	}

	if c.Debug {
		log.Println("Transaction Commit")
	}

	return err
}

func buildDsn(c *ClickhouseClient) (string, error) {
	v := url.Values{}
	v.Add("read_timeout", string(c.ReadTimeout))
	v.Add("write_timeout", string(c.WriteTimeout))
	v.Add("debug", strconv.FormatBool(c.Debug))

	if len(c.Hosts) == 0 {
		return "", errors.New("hosts must be set")
	}

	if len(c.Hosts) > 1 {
		v.Add("alt_hosts", strings.Join(c.Hosts[1:], ","))
	}

	u := url.URL{
		Scheme:   "tcp",
		Host:     c.Hosts[0],
		RawQuery: v.Encode(),
	}
	return u.String(), nil
}
