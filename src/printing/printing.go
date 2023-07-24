package printing

import (
	"fmt"
	"stream"
	"update"
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

func PrintAttributesForUser(attrs map[string]update.History, userID string) {
	fmt.Println()
	fmt.Printf("attribute values for ID %s are now\n", userID)
	for attrName, itsHistory := range attrs {
		fmt.Printf("%s: %s at %d, ", attrName, itsHistory.Value, itsHistory.Timestamp)
	}
	fmt.Println()
}

func PrintEventsForUser(events map[string][]string, userID string) {
	fmt.Println()
	fmt.Printf("event values for user ID %s are now\n", userID)
	for eventName, eventIDs := range events {
		fmt.Printf("%s: %s happened %d times, ", userID, eventName, len(eventIDs))
	}
	fmt.Println()
}

func PrintList(theList []string, title string) {
	fmt.Printf("List of %s:\n", title)
	for index, str := range theList {
		fmt.Printf("%d: %s\n", index, str)
	}
}
