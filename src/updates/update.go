package update

import (
	"debugging"
	"fmt"
)

/*
The History struct stores the latest attribute value set for the key attribute.

	It contains the attribute name (e.g. "email") whose value changed, what it
	was changed to (e.g., "george@gmail.com"), and when (by timestamp).
*/
type History struct {
	//Attribute string `json:"name"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

/*
The UserRecord struct stores the set of attributes and events for a given user ID.

	The resultant set of attributes will show *only* the latest values for each attribute
	assigned or changed. Previous values have been overwritten.

	The Events map will keep track of all of the unique IDs for each event, in order to count
	them later. Counting the IDs will yield the number of unique times each type
	of event occurred for the given user. Duplicate (identical) events are quietly discarded.
*/
type UserRecord struct {
	UserID     string              `json:"user_id"`
	Attributes map[string]History  `json:"data"`
	Events     map[string][]string `json:"events"`
}

// Idea: templatize these funcs. They are so similar to each other

/*
The FindOrCreateUser func takes a user id and reviews the records seen thus far to
be able to determine whether this is a new user to add to the set.

	If new, a new user is allocated and added to the set. The set is returned, in
	case it changed.
*/
func FindOrCreateUser(recs map[string]UserRecord, s string) (map[string]UserRecord, UserRecord, bool) {
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

/*
The FindOrCreateAttr func searches the current user's list of attributes (the History) to
determine whether to add to the list.

	If there are no attributes at all, a map is allocated to store them. If the named attribute
	isn't already stored, a new value is allocated and the attribute name and latest value
	are stored.

	If the attribute isn't new, the calling function will replace its value with the latest
	value (by timestamp).
*/
func FindOrCreateAttr(attributes map[string]History, attributeName string) map[string]History {

	if len(attributes) == 0 {
		attributes = make(map[string]History, 5)
	}
	var present bool
	_, present = attributes[attributeName]
	if !present {
		attributes[attributeName] = History{"", 0}
	}

	return attributes
}

/*
The FindOrCreateEvent func determines whether the current user has a map of events allocated yet.

	If not, a map is allocated and an event is allocated. The event name and given eventID are stored.

	If so, and the given event is new, an event is allocated and its name and ID are stored.

	If the event is not new, the calling function will add its ID to the set of IDs, if it
	is unique.
*/
func FindOrCreateEvent(events map[string][]string, eventName string, eventID string) (map[string][]string, bool) {
	var eventIDs []string

	if len(events) != 0 {
		debugging.Debug(debugging.DEBUG_EVENTS, "FindEvent: this user has %d events logged already:\n", len(events))
		var found bool
		found = false
		for thisEventName, eventList := range events {
			if eventName == thisEventName {
				debugging.Debug(debugging.DEBUG_EVENTS, "event name %s found\n", thisEventName)
				found = true
				break
			}
			if debugging.Getdebug() == debugging.DEBUG_EVENTS {
				for _, currentEventID := range eventList {
					fmt.Printf("%s,", currentEventID)
				}
				fmt.Println()
			}
		}

		if !found {
			eventIDs = make([]string, 1)
			eventIDs[0] = eventID
			events[eventName] = eventIDs
		}
	} else {
		events = make(map[string][]string, 5)
		eventIDs = make([]string, 1)
		eventIDs[0] = eventID
		events[eventName] = eventIDs
	}

	return events, true
}
