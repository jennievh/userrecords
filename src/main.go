package main

import (
	"bufio"
	"context"
	"database/sql"
	"debugging"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"printing"
	"stream"
	"syscall"
	"update"

	"net/http"
	_ "net/http/pprof" // Blank import to pprof

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/profile"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	var errorString string
	var inputFileName string
	var outputFileName string
	var validateFileName string

	/*
		Designate and open input files. Pass them to the stream processor
		to get back a channel that will feed in one record at a time.
	*/
	inputFileName = "../data/messages.2.data"
	outputFileName = inputFileName + ".out.csv"
	validateFileName = "../data/verify.2.csv"

	defer profile.Start(profile.GoroutineProfile).Stop()

	f, err := os.Open(inputFileName)
	check(err)
	defer f.Close()

	f1 := io.ReadSeeker(f)

	ch, err := stream.Process(ctx, f1)
	check(err)

	db, err := sql.Open("mysql", "root:test@tcp(127.0.0.1:3306)/userrecords_db")
	testError(err)

	defer db.Close()
	if err := db.Ping(); err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(1000)

	_, err = db.Query("DELETE FROM attributes")
	testError(err)

	_, err = db.Query("DELETE FROM events")
	testError(err)

	// DEBUG
	debugging.Setdebug(debugging.DEBUG_NONE)
	count := 0
	for rec := range ch {

		count++

		if count%1000 == 0 {
			fmt.Printf("Reached %d actions!\n", count)
		}

		_ = rec
		// THIS IS WHERE THE MAGIC HAPPENS
		if rec.UserID == "" { // that's a bug; continue
			//log.Printf("record number %d has no or blank User ID", count)
			continue
		}

		// Attribute, or event?
		switch {
		case rec.Type == "attributes":

			for attr, val := range rec.Data {
				update.FindOrCreateAttr(db, rec.UserID, attr, val, rec.Timestamp)
			}

			if debugging.Getdebug() == debugging.DEBUG_ATTRIBUTES {
				printing.PrintAttributesForUser(db, rec.UserID)
			}

		case rec.Type == "event":
			event := rec.Name
			eventID := rec.ID

			update.FindOrCreateEvent(db, rec.UserID, event, eventID)

			if debugging.Getdebug() == debugging.DEBUG_EVENTS {
				printing.PrintEventsForUser(db, rec.UserID)
			}

		case rec.Type != "attribute" && rec.Type != "event":
			errorString = fmt.Sprintf("Event type %s not recognized!", rec.Type)
			log.Fatal(errorString)
		}
	}

	f2, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	check(err)
	defer f2.Close()

	// OUTPUT
	// Extract an ordered slice of user IDs, sorted and uniq'd.
	// Walk through this slice and extract attributes and events.
	// Append all to a string. Write it out.

	fmt.Printf("Ready to write out results!\n")

	rows, err := db.Query("SELECT userID FROM attributes UNION SELECT userID FROM events ORDER BY userID")
	testError(err)

	var thisUser string
	var users []string
	for rows.Next() {
		err := rows.Scan(&thisUser)
		testError(err)

		users = append(users, thisUser)
	}
	rows.Close()

	var result string

	var attrName string
	var attrValue string
	var eventName string
	var eventTimes int64
	for _, currUser := range users {
		// USER ID
		result = currUser

		// ATTRIBUTES
		rows, err = db.Query("SELECT attribute_name, attribute_value FROM attributes WHERE userID = ? ORDER BY attribute_name", currUser)
		if err != sql.ErrNoRows {
			testError(err)

			for rows.Next() {
				err = rows.Scan(&attrName, &attrValue)
				testError(err)
				result = fmt.Sprintf("%s,%s=%s", result, attrName, attrValue)
			}
			rows.Close()
		}

		// EVENTS
		rows, err = db.Query("SELECT DISTINCT event_name, COUNT(event_id) OVER (PARTITION BY event_name) AS counter FROM events  WHERE userID = ? ORDER BY event_name", currUser)
		if err != sql.ErrNoRows {
			testError(err)

			for rows.Next() {
				err = rows.Scan(&eventName, &eventTimes)
				testError(err)

				result = fmt.Sprintf("%s,%s=%d", result, eventName, eventTimes)
			}
			rows.Close()
		}
		if err == sql.ErrNoRows {
			result = fmt.Sprintf("%s,", result) // if a user has no events, the verification string will have a final comma (bug)
		}

		result = fmt.Sprintf("%s\n", result)

		debugging.Debug(debugging.DEBUG_ALL, "%s", "about to write out results")
		f2.WriteString(result)
	}

	err = validate(outputFileName, validateFileName)
	if err != nil {
		log.Fatal(err)
	}

	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}
}

// Quick validation of expected and received input.
func validate(have, want string) error {
	f1, err := os.Open(have)
	if err != nil {
		return err
	}
	defer f1.Close()

	f2, err := os.Open(want)
	if err != nil {
		return err
	}
	defer f2.Close()

	s1 := bufio.NewScanner(f1)
	s2 := bufio.NewScanner(f2)
	for s1.Scan() {
		if !s2.Scan() {
			return fmt.Errorf("want: insufficient data")
		}
		t1 := s1.Text()
		t2 := s2.Text()
		if t1 != t2 {
			return fmt.Errorf("have/want: difference\n%s\n%s", t1, t2)
		}
	}
	if s2.Scan() {
		return fmt.Errorf("have: insufficient data")
	}
	if err := s1.Err(); err != nil {
		return err
	}
	if err := s2.Err(); err != nil {
		return err
	}
	return nil
}

func testError(err error) {
	if err != nil {
		panic(err.Error)
	}
}
