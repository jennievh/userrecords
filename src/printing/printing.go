package printing

import (
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
