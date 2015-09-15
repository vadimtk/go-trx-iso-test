package main

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	//	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	//	"time"
)

func MySQLErrorCode(err error) uint16 {
	if val, ok := err.(*mysql.MySQLError); ok {
		return val.Number
	}

	return 0 // not a mysql error
}

// create N accounts
const N int = 30
const BALANCE int = 100

var wg sync.WaitGroup

func bummer(idx int) {
	defer wg.Done()
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/test")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()
	rand.Seed(100)
	var (
		frombalance int
		tobalance   int
	)
	for i := 0; i < 200; i++ {
		tx, err := db.Begin()
		if err != nil {
			panic(err.Error())
		}
		fromAccnt := rand.Intn(N)
		toAccnt := rand.Intn(N)
		for toAccnt == fromAccnt {
			toAccnt = rand.Intn(N)
		}
		rows, err := tx.Query("select balance from accounts where id=" + strconv.Itoa(fromAccnt)+" FOR UPDATE")
		if MySQLErrorCode(err) == 1213 {
			tx.Rollback()
			continue
		}
		for rows.Next() {
			err := rows.Scan(&frombalance)
			if err != nil {
				panic(err.Error())
			}
		}
		err = rows.Err()
		if err != nil {
			if MySQLErrorCode(err) == 1213 {
				tx.Rollback()
				continue
			}

			panic(err.Error())
		}
		rows, err = tx.Query("select balance from accounts where id=" + strconv.Itoa(toAccnt)+" FOR UPDATE")
		if err != nil {
			if MySQLErrorCode(err) == 1213 {
				tx.Rollback()
				continue
			}

			panic(err.Error())
		}
		for rows.Next() {
			err := rows.Scan(&tobalance)
			if err != nil {
				panic(err.Error())
			}
		}
		err = rows.Err()
		if err != nil {
			if MySQLErrorCode(err) == 1213 {
				tx.Rollback()
				continue
			}

			panic(err.Error())
		}
		if frombalance > 1 {
			moveamt := rand.Intn(frombalance)+1
			_, err = tx.Exec("update accounts set balance=" + strconv.Itoa(tobalance+moveamt) + " where id=" + strconv.Itoa(toAccnt))
			if err != nil {
				if MySQLErrorCode(err) == 1213 {
					tx.Rollback()
					continue
				}

				panic(err.Error())
			}
			_, err = tx.Exec("update accounts set balance=" + strconv.Itoa(frombalance-moveamt) + " where id=" + strconv.Itoa(fromAccnt))
			if err != nil {
				if MySQLErrorCode(err) == 1213 {
					tx.Rollback()
					continue
				}

				panic(err.Error())
			}
			fmt.Printf("Moving %d from %d to %d: from bal:%d to bal:%d\n", moveamt, fromAccnt, toAccnt, frombalance, tobalance)

		}

		err = tx.Commit()
		if err != nil {
			if MySQLErrorCode(err) == 1213 {
				tx.Rollback()
				continue
			}

			panic(err.Error())
		}
	}

	fmt.Println("Done")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	//runtime.GOMAXPROCS(1)

	f, _ := os.Create("mgo_m.cpuprofile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/test")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()
	_, err = db.Exec("DROP TABLE IF EXISTS accounts")
	if err != nil {
		panic(err.Error())
	}
	_, err = db.Exec("CREATE TABLE accounts (id      int not null primary key, balance bigint not null) ENGINE=InnoDB")
	if err != nil {
		panic(err.Error())
	}

	for i := 0; i < N; i++ {
		_, err = db.Exec("INSERT INTO accounts (id,balance) VALUES (" + strconv.Itoa(i) + "," + strconv.Itoa(BALANCE) + ")")
		if err != nil {
			panic(err.Error())
		}
	}
	//_, err =  db.Exec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
	//	if err != nil {
	//		panic(err.Error())
	//	}

	for j := 0; j < 8; j++ {
		wg.Add(1)
		go bummer(j)
	}
	wg.Wait()
}
