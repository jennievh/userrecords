package update

import (
	"database/sql"
)

/*
	The FindOrCreateAttr func determines whether this attribute has been entered for this user already.

If not, a new row is entered into the database table.

If it has, the stored timestamp is compared to the latest timestamp. If the latest is
greater (more recent), the attribute value and the timestamp entry are updated.
*/
func FindOrCreateAttr(db *sql.DB, userId string, attrName string, attrValue string, timestamp int64) {
	row := db.QueryRow("SELECT change_date FROM attributes WHERE userID = ? AND attribute_name = ?", userId, attrName)

	var timeSinceEpoch int64
	err := row.Scan(&timeSinceEpoch)
	if err == sql.ErrNoRows {

		_, err = db.Query("INSERT INTO attributes (userID, attribute_name, attribute_value, change_date) VALUES(?, ?, ?, ?)",
			userId, attrName, attrValue, timestamp)

		if err != nil {
			panic(err.Error())
		}
		return
	}
	if timestamp > timeSinceEpoch {
		rows, err := db.Query("UPDATE attributes SET change_date = ?, attribute_value = ? WHERE userID = ? AND attribute_name = ?",
			timestamp, attrValue, userId, attrName)
		if err != nil {
			if err == sql.ErrNoRows {
				rows.Close()
				return
			}
			panic(err.Error())
		}
		rows.Close()
	}
}

/*
	The FindOrCreateEvent func queries the database to see whether this event with this unique

ID has already been stored for this user.

	If so, there is nothing further to do (duplicate events are ignored).

	If not, a new row is added for this user and event name with the new event ID.
*/
func FindOrCreateEvent(db *sql.DB, userId string, eventName string, eventId string) {
	row := db.QueryRow("SELECT event_id FROM events WHERE userID = ? AND event_name = ? AND event_id = ?",
		userId, eventName, eventId)

	var returnedEventId string
	err := row.Scan(&returnedEventId)
	if err == sql.ErrNoRows {
		rows, err := db.Query("INSERT INTO events (userID, event_name, event_id) VALUES(?, ?, ?)",
			userId, eventName, eventId)
		if err != nil {
			panic(err.Error())
		}
		rows.Close()
		return
	}
	if returnedEventId == eventId {
		return
	}
}
