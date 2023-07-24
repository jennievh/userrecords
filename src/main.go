package main

import (
	"bufio"
	"context"
	"debugging"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"printing"
	"sort"
	"stream"
	"syscall"
	"update"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
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

	inputFileName = "../data/messages.1.data"
	outputFileName = inputFileName + ".out.csv"
	validateFileName = "../data/verify.1.csv"

	f, err := os.Open(inputFileName)
	check(err)

	f1 := io.ReadSeeker(f)

	ch, err := stream.Process(ctx, f1)
	check(err)

	CHUNK := 10
	users := make(map[string]update.UserRecord, CHUNK)

	f2, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	check(err)

	// DEBUG
	debugging.Setdebug(debugging.DEBUG_NONE)
	count := 0
	for rec := range ch {

		count++
		/*
			if count > 100 {
				//os.Exit(0)
				break
			}*/
		_ = rec
		// THIS IS WHERE THE MAGIC HAPPENS
		if rec.UserID == "" { // that's a bug; continue
			log.Printf("record number %d has no or blank User ID", count)
			continue
		}
		users, user, ok := update.FindOrCreateUser(users, rec.UserID)

		if ok {
			user.UserID = rec.UserID
			if debugging.Getdebug() == debugging.DEBUG_ALL {
				printing.PrintIncomingRecord(rec)
			}

			// Attribute, or event?
			switch {
			case rec.Type == "attributes":

				for attr, val := range rec.Data {
					userAttrs := update.FindOrCreateAttr(user.Attributes, attr)

					if userAttrs[attr].Timestamp < rec.Timestamp {
						var newHist update.History
						newHist.Value = val
						newHist.Timestamp = rec.Timestamp
						userAttrs[attr] = newHist
					}
					user.Attributes = userAttrs
				}
				users[user.UserID] = user
				if debugging.Getdebug() == debugging.DEBUG_ATTRIBUTES {
					printing.PrintAttributesForUser(user.Attributes, user.UserID)
				}

			case rec.Type == "event":
				event := rec.Name
				eventID := rec.ID

				events, ok := update.FindOrCreateEvent(user.Events, event, eventID)
				if !ok {
					os.Exit(1)
				}

				for occurrence, idStrings := range events {
					if event == occurrence {
						found := false
						for _, str := range idStrings {
							if str == eventID {
								found = true
								break
							}
						}
						fmt.Println()
						if !found {
							if debugging.Getdebug() == debugging.DEBUG_ALL {
								printing.PrintList(idStrings, "Event IDs before appending")
							}

							idStrings = append(idStrings, eventID)

							if debugging.Getdebug() == debugging.DEBUG_ALL {
								printing.PrintList(idStrings, "Event IDs after appending")
							}
						}
						events[event] = idStrings
						break
					}
				}
				user.Events = events
				users[user.UserID] = user

				if debugging.Getdebug() == debugging.DEBUG_EVENTS {
					printing.PrintEventsForUser(user.Events, user.UserID)
				}

			case rec.Type != "attribute" && rec.Type != "event":
				errorString = fmt.Sprintf("Event type %s not recognized!", rec.Type)
				log.Fatal(errorString)
			}
		}
	}

	if debugging.Getdebug() == debugging.DEBUG_ALL {
		fmt.Printf("Data so far is \n")

		for _, userRecord := range users {
			printing.PrintAttributesForUser(userRecord.Attributes, userRecord.UserID)
			printing.PrintEventsForUser(userRecord.Events, userRecord.UserID)
		}
	}

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

	type AttrSlice struct {
		Key   string
		Value update.History
	}

	type EventSlice struct {
		Key   string
		Value []string
	}
	var doOnce bool
	doOnce = true

	debugging.Debug(debugging.DEBUG_ALL, "%s", "about to write out results")
	for _, v := range s {
		// iterate over the slice of this user's attributes to get the desired order
		// USER ID
		//fmt.Printf("%s", v.Key)
		var result string
		result = v.Key
		if doOnce {
			debugging.Debug(debugging.DEBUG_ALL, "results is now\n%s\n", result)
		}

		// ATTRIBUTES
		sortedAttributes := make([]AttrSlice, 0, len(v.Value.Attributes))
		for k, v := range v.Value.Attributes {
			sortedAttributes = append(sortedAttributes, AttrSlice{k, v})
		}

		sort.SliceStable(sortedAttributes, func(i, j int) bool {
			return sortedAttributes[i].Key < sortedAttributes[j].Key
		})

		for _, w := range sortedAttributes {
			result = fmt.Sprintf("%s,%s=%s", result, w.Key, w.Value.Value)
		}
		if doOnce {
			debugging.Debug(debugging.DEBUG_ALL, "results is now\n%s\n", result)
		}

		// EVENTS
		sortedEvents := make([]EventSlice, 0, len(v.Value.Events))
		for eventName, eventIDs := range v.Value.Events {
			sortedEvents = append(sortedEvents, EventSlice{eventName, eventIDs})
		}

		sort.SliceStable(sortedEvents, func(i, j int) bool {
			return sortedEvents[i].Key < sortedEvents[j].Key
		})

		for _, e := range sortedEvents {
			result = fmt.Sprintf("%s,%s=%d", result, e.Key, len(e.Value))
		}
		if doOnce {
			debugging.Debug(debugging.DEBUG_ALL, "results is now\n%s\n", result)
		}

		result = fmt.Sprintf("%s\n", result)
		if doOnce {
			debugging.Debug(debugging.DEBUG_ALL, "First line of results is\n%s\n", result)
			doOnce = false
		}
		//fmt.Println(result)
		f2.WriteString(result)
	}

	// Close f? f1? ch?
	f.Close()
	f2.Close()

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
