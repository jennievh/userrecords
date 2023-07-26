package printing

import (
	"database/sql"
	"fmt"
	"stream"
)

func PrintIncomingRecord(rec *stream.Record) {
	fmt.Printf("Record for User ID %s:\n", rec.UserID)
	switch {
	case rec.Type == "attributes":
		for attr, val := range rec.Data {
			fmt.Printf("attribute: %s value: %s timestamp: %d\n", attr, val, rec.Timestamp)
		}

	case rec.Type == "event":
		fmt.Printf("event: %s id: %s timestamp: %d\n", rec.Name, rec.ID, rec.Timestamp)
	}
}

func PrintAttributesForUser(db *sql.DB, userID string) {
	fmt.Println()
	rows, err := db.Query("SELECT * FROM attributes WHERE userID = ?", userID)
	if err != nil {
		panic(err.Error())
	}

	var user string
	var attrName string
	var attrValue string
	var timestamp int64
	for rows.Next() {

		err := rows.Scan(&user, &attrName, &attrValue, &timestamp)
		if err != nil {
			panic(err.Error)
		}
		fmt.Printf("Attribute for user %s is %s: %s at %d, ", user, attrName, attrValue, timestamp)
	}
	fmt.Println()
}

func PrintEventsForUser(db *sql.DB, userID string) {
	fmt.Println()
	rows, err := db.Query("SELECT userID, event_name, event_id FROM events WHERE userID = ? ORDER BY event_name", userID)
	if err != nil {
		panic(err.Error())
	}

	var user string
	var eventName string
	var eventID string
	for rows.Next() {
		rows.Scan(&user, &eventName, &eventID)
		fmt.Printf("%s: %s event ID %s, ", user, eventName, eventID)
	}
	fmt.Println()
}

