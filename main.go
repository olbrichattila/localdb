// Package main is the main entry point, it is only for playing the database locally, will be removed when package is done
package main

import (
	"fmt"
	localdb "godb/pkg/db"
	"os"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	tableName    = "users"
	useIndex     = "idx_f4"
	displayField = "field_3"
)

func main() {
	file, err := os.Create("output.txt")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Redirect standard output to the file
	os.Stdout = file

	dataTest()
	useAndListUp()
	fmt.Println("======================")
	useAndListDown()

}

func useAndListUp() {
	db := localdb.New()

	currTable, err := db.Open(tableName)
	if err != nil {
		panic("Cannot open table " + err.Error())
	}
	defer currTable.Close()

	err = db.Use(currTable, useIndex)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// r, err := db.Locate(currTable, "field_1", "00121")
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return
	// }

	// fmt.Println(r["field_1"])
	// val, eof, err := db.Next(currTable)
	// fmt.Println(val, eof, err)

	err = db.First(currTable)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	x := 0
	for {
		x++
		dat, eof, err := db.Next(currTable)
		if dat != nil {
			fmt.Println(dat[displayField], x)
		}
		if eof {
			break
		}
		if err != nil {
			fmt.Println(err.Error())
			break
		}

	}
}

func useAndListDown() {
	db := localdb.New()

	currTable, err := db.Open(tableName)
	if err != nil {
		panic("Cannot open table " + err.Error())
	}
	defer currTable.Close()

	err = db.Use(currTable, useIndex)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// r, err := db.Locate(currTable, "field_1", "00953")
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return
	// }

	// fmt.Println(r["field_1"])
	// val, eof, err := db.Prev(currTable)
	// fmt.Println(val, eof, err)

	err = db.Last(currTable)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	x, _ := db.RecCount(currTable)
	x--
	for {
		x--
		dat, eof, err := db.Prev(currTable)

		if err != nil {
			fmt.Println(err.Error())
			break
		}
		if dat != nil {
			fmt.Println(dat[displayField], x)
		}
		if eof {
			break
		}

	}
}

func dataTest() {
	db := localdb.New()

	tableStruct := &localdb.FieldDef{
		Fields: []localdb.Field{
			{Name: "field_1", Type: localdb.FtText, Length: 15, Required: true, Indexes: []localdb.IndexDef{{Name: "idx_f1"}}},
			{Name: "field_2", Type: localdb.FtBool},
			{Name: "field_3", Type: localdb.FtInt, Indexes: []localdb.IndexDef{{Name: "idx_int"}}},
			{Name: "field_4", Type: localdb.FtText, Length: 12, Indexes: []localdb.IndexDef{{Name: "idx_f4"}}},
		},
	}

	err := db.Create(tableName, tableStruct)
	if err != nil {
		panic("Cannot run test, Could not create database " + err.Error())
	}

	currTable, err := db.Open(tableName)
	if err != nil {
		panic("Cannot open table " + err.Error())
	}
	defer currTable.Close()

	data := map[string]interface{}{
		// "field_1": "Test data",
		"field_1": "00000",
		"field_2": true,
		"field_3": int64(150),
		"field_4": "test",
	}

	t0 := time.Now()

	// for i := 0; i < 1000; i++ {
	// 	data["field_1"] = "Test data " + strconv.Itoa(i)
	// 	data["field_3"] = int64(i)
	// 	data["field_4"] = strconv.Itoa(i)
	// 	_, err = db.Insert(currTable, data)
	// 	if err != nil {
	// 		panic("Insert error " + err.Error())
	// 	}
	// }

	// for i := 3000; i >= 0; i-- {
	for i := 100000; i >= 0; i-- {
		// data["field_1"] = "Test data " + fmt.Sprintf("%05d", i)
		data["field_1"] = fmt.Sprintf("%07d", i)
		data["field_3"] = int64(i)
		data["field_4"] = strconv.Itoa(i)
		_, err = db.Insert(currTable, data)
		if err != nil {
			panic("Insert error " + err.Error())
		}
	}

	elapsed := time.Since(t0)
	fmt.Println("Elapsed time:", elapsed)
}

// func indexTest() {
// 	t, err := btree.New("testindex", 12)
// 	if err != nil {
// 		fmt.Println(err.Error())
// 	}

// 	max := 999999
// 	// max := 60
// 	// max := 54
// 	for i := 0; i <= max; i++ {
// 		// for i := max; i >= 0; i-- {
// 		err := t.Insert([]byte(fmt.Sprintf("%06d", i)), 5647)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 	}

// 	err = t.Insert([]byte("796993"), 5647)
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	// Seed the random number generator with the current time to ensure different sequences each run
// 	rand.Seed(uint64(time.Now().UnixNano()))

// 	max = 99999
// 	// max := 60
// 	// max := 54
// 	for i := 0; i <= max; i++ {
// 		// for i := max; i >= 0; i-- {
// 		randomNumber := rand.Intn(100000) // Generates a random number between 0 and 99999
// 		t.Insert([]byte(fmt.Sprintf("%05d", randomNumber)), 65535)
// 	}

// item, _, found, err := t.Search([]byte("796993"))
// fmt.Println(item, found, err)
// // -- 00012
// // -- 00042
// }

// func sqLiteTest() {
// 	db, err := sql.Open("sqlite3", "mydatabase.db")
// 	if err != nil {
// 		fmt.Println("Error opening database:", err)
// 		return
// 	}
// 	defer db.Close()

// 	// Create the table
// 	_, err = db.Exec(`
// 			CREATE TABLE IF NOT EXISTS mytable (
// 					field_1 TEXT,
// 					field_2 INTEGER,
// 					field_3 INTEGER,
// 					field_4 TEXT
// 			)
// 	`)
// 	if err != nil {
// 		fmt.Println("Error creating table:", err)
// 		return
// 	}

// 	// Create indexes
// 	_, err = db.Exec("CREATE INDEX idx_name_1 ON mytable (field_1)")
// 	if err != nil {
// 		fmt.Println("Error creating index:", err)
// 		return
// 	}

// 	// Create indexes
// 	_, err = db.Exec("CREATE INDEX idx_name_2 ON mytable (field_3)")
// 	if err != nil {
// 		fmt.Println("Error creating index:", err)
// 		return
// 	}

// 	// Create indexes
// 	_, err = db.Exec("CREATE INDEX idx_name_3 ON mytable (field_4)")
// 	if err != nil {
// 		fmt.Println("Error creating index:", err)
// 		return
// 	}

// 	t0 := time.Now()
// 	for i := 0; i < 1000; i++ {
// 		_, err = db.Exec("INSERT INTO mytable (field_1, field_2, field_3, field_4) VALUES (?, ?, ?, ?)", "Test data "+strconv.Itoa(i), 0, i, strconv.Itoa(i))
// 		if err != nil {
// 			fmt.Println("Error inserting record:", err)
// 			return
// 		}
// 	}

// 	for i := 3000; i > 10; i-- {
// 		{
// 			_, err = db.Exec("INSERT INTO mytable (field_1, field_2, field_3, field_4) VALUES (?, ?, ?, ?)", "Test data "+strconv.Itoa(i), 1, i, strconv.Itoa(i))
// 			if err != nil {
// 				fmt.Println("Error inserting record:", err)
// 				return
// 			}
// 		}
// 	}

// 	elapsed := time.Since(t0)
// 	fmt.Println("Elapsed time:", elapsed)
// }
