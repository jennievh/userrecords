package update

import "fmt"

/*
The History struct stores the latest attribute value set for the key attribute.

	It contains what the attribute (e.g. "email") whose value changed, was changed to
	(e.g., "george@gmail.com"), and when (by timestamp).

	Question: In the original Record, the attributes were simply a map. Would it be
	worthwhile to keep the attributes as a map? How would we store the timestamp
	for each entry? --because we want the latest value for an attribute, by timestamp.
*/
type History struct {
	//Attribute string `json:"name"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

/*
The UserRecord struct stores the set of attributes and events for a given user ID.

	The resultant set of attributes will show the latest values for each attribute
	assigned or changed. Previous values have been overwritten.

	For Events, we'll keep track of the unique IDs for the events, in order to count
	them later. The result will yield the number of unique times each type
	of event occurred for the given user. Duplicate (identical) events have been ignored.
*/
type UserRecord struct {
	UserID     string              `json:"user_id"`
	Attributes map[string]History  `json:"data"`
	Events     map[string][]string `json:"events"`
}

// Idea: templatize these funcs. They are so similar to each other
func FindOrCreate(recs map[string]UserRecord, s string) (map[string]UserRecord, UserRecord, bool) {
	//DEBUG
	//fmt.Printf("recs has %d entries\n", len(recs))
	thisrec, present := recs[s]
	if present {
		return recs, thisrec, true
	}

	recs[s] = UserRecord{
		UserID:     s,
		Attributes: map[string]History{},
		Events:     map[string][]string{},
	}
	return recs, recs[s], true

}

func FindAttr(attributes map[string]History, attributeName string) (map[string]History, bool) {
	//DEBUG
	if len(attributes) != 0 {
		fmt.Printf("FindAttr: this user has %d attributes already\n", len(attributes))
	} else {
		attributes = make(map[string]History, 5)
	}
	var present bool
	_, present = attributes[attributeName]
	if !present {
		attributes[attributeName] = History{"", 0}
	}

	return attributes, true
}

func FindEvent(events map[string][]string, eventName string, eventID string) (map[string][]string, bool) {
	//DEBUG
	var eventIDs []string

	if len(events) != 0 {
		fmt.Printf("FindEvent: this user has %d events logged already\n", len(events))
	} else {
		events = make(map[string][]string, 5)
		eventIDs = make([]string, 1)
	}
	eventIDs = append(eventIDs, eventID)
	events[eventName] = eventIDs

	return events, true
}
