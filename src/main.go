package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"stream"
	"update"
	"userrecords/src/printing"
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

	f2, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE, 0755)
	check(err)

	// DEBUG
	count := 0
	for rec := range ch {

		count++
		if count > 100 {
			//os.Exit(0)
			break
		}
		_ = rec
		// THIS IS WHERE THE MAGIC HAPPENS
		if rec.UserID == "" { // that's a bug; continue
			continue
		}
		users, user, ok := update.FindOrCreate(users, rec.UserID)
		//DEBUG: test recs
		/*fmt.Printf("in main.go:\n")
		for id, record := range users {
			fmt.Printf("User ID: %s (%s) is in recs\n", id, record.UserID)
		}
		*/
		if ok {
			user.UserID = rec.UserID

			printing.PrintIncomingRecord(rec)
			// Attribute, or event?
			switch {
			case rec.Type == "attributes":
				//fmt.Printf("\nThis record has one or more attribute changes\n")

				/*for attr, val := range rec.Data {
					fmt.Printf(", %s: %s", attr, val)
				}
				fmt.Println() */

				for attr, val := range rec.Data {
					userAttrs, ok := update.FindAttr(user.Attributes, attr)
					if !ok {
						errorString = fmt.Sprintf("Unable to find attribute %s!", attr)
						log.Fatal(errorString)
					}
					if userAttrs[attr].Timestamp < rec.Timestamp {
						var newHist update.History
						newHist.Value = val
						newHist.Timestamp = rec.Timestamp
						userAttrs[attr] = newHist
					}
					user.Attributes = userAttrs
				}
				users[user.UserID] = user
				/*
					fmt.Println()
					fmt.Printf("attribute values for ID %s are now\n", user.UserID)
					for attrName, itsHistory := range user.Attributes {
						fmt.Printf("%s: %s at %d, ", attrName, itsHistory.Value, itsHistory.Timestamp)
					}
					fmt.Println() */

			case rec.Type == "event":
				fmt.Printf("\nThis record shows an event logged\n")
				event := rec.Name
				eventID := rec.ID
				fmt.Printf("Event: %s", event)

				events, ok := update.FindOrCreateEvent(user.Events, event, eventID)
				if !ok {
					os.Exit(1)
				}

				for occurrence, idStrings := range events {
					if event == occurrence {
						fmt.Printf("for %s, found event \"%s\"\n", user.UserID, event)
						fmt.Printf("the event id strings are ")
						found := false
						for _, str := range idStrings {
							fmt.Printf("%s,", str)
							if str == eventID {
								found = true
								//break
							}
						}
						fmt.Println()
						if !found {
							fmt.Printf("idstrings before appending:\n")
							for _, str := range idStrings {
								fmt.Printf("%s,", str)
							}
							fmt.Println()
							idStrings = append(idStrings, eventID)
							fmt.Printf("idstrings after appending:\n")
							for _, str := range idStrings {
								fmt.Printf("%s,", str)
							}
							fmt.Println()
						}
						events[event] = idStrings
						break
					}
				}
				user.Events = events
				users[user.UserID] = user

				fmt.Println()
				fmt.Printf("event values for user ID %s are now\n", user.UserID)
				for eventName, eventIDs := range user.Events {
					fmt.Printf("%s: %s happened %d times, ", user.UserID, eventName, len(eventIDs))
				}
				fmt.Println()

			case rec.Type != "attribute" && rec.Type != "event":
				errorString = fmt.Sprintf("Event type %s not recognized!", rec.Type)
				log.Fatal(errorString)
			}
			fmt.Printf("\n")
		}
	}

	fmt.Println("Data is now")
	for thisId, userRecord := range users {
		fmt.Printf("\nUser %s\n", thisId)
		for attrName, attrData := range userRecord.Attributes {
			fmt.Printf("Attribute: %s: %s at %d\n", attrName, attrData.Value, attrData.Timestamp)
		}
		fmt.Println()
		for eventName, eventData := range userRecord.Events {
			fmt.Printf("Event: %s happened %d times\n", eventName, len(eventData))
		}
		fmt.Printf("\n\n")
	}

	fmt.Println("Results-------------------------------------------------")

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

	fmt.Println("about to write out results")
	for _, v := range s {
		// iterate over the slice of this user's attributes to get the desired order
		// USER ID
		//fmt.Printf("%s", v.Key)
		var result string
		result = v.Key
		if doOnce {
			fmt.Printf("results is now\n%s\n", result)
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
			fmt.Printf("results is now\n%s\n", result)
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
			fmt.Printf("results is now\n%s\n", result)
		}

		result = fmt.Sprintf("%s\n", result)
		if doOnce {
			fmt.Printf("First line of results is\n%s\n", result)
			doOnce = false
		}
		fmt.Println(result)
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
