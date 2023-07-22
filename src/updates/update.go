package update

import "fmt"

/*
The History struct stores the latest attribute set for the user ID.

	It contains the name of the attribute that was changed (e.g. "email"),
	what it was changed to (e.g., "george@gmail.com"), and when (by timestamp).

	Question: In the original Record, the attributes were simply a map. Would it be
	worthwhile to keep the attributes as a map? How would we store the timestamp
	for each entry? --because we want the latest value for an attribute, by timestamp.
*/
type History struct {
	// ID string `json:"id"`
	Attribute string `json:"name"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

/*
The Event struct stores the names of the events that have occurred, along with their unique IDs.

	The unique IDs allow us to count only the event occurrences that are unique and omit
	spurious duplicates.
*/
type Event struct {
	Event string `json:"name"`
	ID    string `json:"id"`
}

/*
The UserRecord struct stores the set of attributes and events for a given user ID.

	The resultant set of attributes will show the latest values for each attribute
	assigned or changed. Previous values have been overwritten.

	The resultant set of events will yield the number of unique times each type
	of event occurred for the given user. Duplicate events have been ignored.
*/
type UserRecord struct {
	UserID     string    `json:"user_id"`
	Attributes []History `json:"data"`
	Events     []Event   `json:"events"`
}

func FindOrCreate(recs map[string]UserRecord, s string) (map[string]UserRecord, UserRecord, bool) {
	//DEBUG
	fmt.Printf("recs has %d entries\n", len(recs))
	thisrec, present := recs[s]
	if present {
		return recs, thisrec, true
	}

	recs[s] = UserRecord{
		UserID:     s,
		Attributes: []History{},
		Events:     []Event{},
	}
	//DEBUG: test recs
	for _, r := range recs {
		fmt.Printf("ID: %s is in recs\n", r.UserID)
	}
	return recs, recs[s], true

}
