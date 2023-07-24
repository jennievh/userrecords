package usersort

import (
	"sort"
	"update"
)

func Usersort(users map[string]update.UserRecord) map[string]update.UserRecord {
	// Sorting of users in ascending order
	type KeyValue struct {
		Key   string
		Value update.UserRecord
	}

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

	return nil
}
