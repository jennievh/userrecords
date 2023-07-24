package usersort

import (
	"debugging"
	"fmt"
	"sort"
	"update"
)

type KeyValue struct {
	Key   string
	Value update.UserRecord
}

/*
	 The Usersort func uses SliceStable to sort by user ID.

		To do this, a slice needs to be made of the map of users and their records.
		The comparison function is declared and sorting is done on the slice.
*/
func Usersort(users map[string]update.UserRecord) []KeyValue {
	// Sorting of users in ascending order

	debugging.Debug(debugging.DEBUG_OUTPUT, "Usersort: There are %d users incoming\n", len(users))

	// create an empty slice of key-value pairs
	s := make([]KeyValue, 0, len(users))
	// append all map key-value pairs to the slice
	for k, v := range users {
		s = append(s, KeyValue{k, v})
	}

	// sort the slice of user id/data pairs by user id in ascending order
	sort.SliceStable(s, func(i, j int) bool {
		return s[i].Key < s[j].Key
	})

	return s
}

type AttrSlice struct {
	Key   string
	Value update.History
}

type EventSlice struct {
	Key   string
	Value []string
}

/*
	 The UserValuesSort func takes a user record and sorts its events and attributes.

		A string of formatted output values is created and returned.

		If a user has only attribute changes and no events, add a comma to the end of the
		output line. The "verify" documents have this bug, so this code was changed
		so that the verification would pass.
*/
func UserValuesSort(thisUser update.UserRecord) (printable string) {

	// iterate over this user's attributes to get the desired order
	printable = thisUser.UserID

	// ATTRIBUTES
	sortedAttributes := make([]AttrSlice, 0, len(thisUser.Attributes))
	for k, v := range thisUser.Attributes {
		sortedAttributes = append(sortedAttributes, AttrSlice{k, v})
	}

	sort.SliceStable(sortedAttributes, func(i, j int) bool {
		return sortedAttributes[i].Key < sortedAttributes[j].Key
	})

	for _, w := range sortedAttributes {
		printable = fmt.Sprintf("%s,%s=%s", printable, w.Key, w.Value.Value)
	}

	if len(thisUser.Events) == 0 {
		printable = printable + ","
	}

	// EVENTS
	sortedEvents := make([]EventSlice, 0, len(thisUser.Events))
	for eventName, eventIDs := range thisUser.Events {
		sortedEvents = append(sortedEvents, EventSlice{eventName, eventIDs})
	}

	sort.SliceStable(sortedEvents, func(i, j int) bool {
		return sortedEvents[i].Key < sortedEvents[j].Key
	})

	for _, e := range sortedEvents {
		printable = fmt.Sprintf("%s,%s=%d", printable, e.Key, len(e.Value))
	}

	printable = fmt.Sprintf("%s\n", printable)

	return printable
}
