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

	f, err := os.Open("../data/messages.1.data")
	check(err)

	f1 := io.ReadSeeker(f)

	ch, err := stream.Process(ctx, f1)
	check(err)

	CHUNK := 10
	users := make(map[string]update.UserRecord, CHUNK)

	// DEBUG
	count := 0
	for rec := range ch {
		count++
		/*if count > 100 {
			os.Exit(0)
		}*/
		_ = rec
		// THIS IS WHERE THE MAGIC HAPPENS
		fmt.Printf("input id: %s,", rec.ID)
		users, user, ok := update.FindOrCreate(users, rec.ID)
		//DEBUG: test recs
		fmt.Printf("in main.go:\n")
		for id, record := range users {
			fmt.Printf("ID: %s (%s) is in recs\n", id, record.UserID)
		}

		if ok {
			user.UserID = rec.ID
			// Attribute, or event?
			if rec.Type == "attributes" {
				fmt.Printf("\nThis record has one or more attribute changes\n")

				for attr, val := range rec.Data {
					fmt.Printf(", %s: %s", attr, val)
				}
				fmt.Println()

				for attr, val := range rec.Data {
					userAttrs, ok := update.FindAttr(user.Attributes, attr)
					if !ok {
						os.Exit(1)
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
				fmt.Println()
				fmt.Printf("attribute values for ID %s are now\n", user.UserID)
				for attrName, itsHistory := range user.Attributes {
					fmt.Printf("%s: %s at %d, ", attrName, itsHistory.Value, itsHistory.Timestamp)
				}
				fmt.Println()

			} // if "attributes"
			/*else { // events

			}*/
			fmt.Printf("\n")
		}
	}

	fmt.Println("Data is now")
	for thisId, userRecord := range users {
		fmt.Printf("\nUser %s", thisId)
		for attrName, attrData := range userRecord.Attributes {
			fmt.Printf("\nAttribute: %s: %s at %d", attrName, attrData.Value, attrData.Timestamp)
		}
		fmt.Printf("\n\n")
	}

	fmt.Println("Results-------------------------------------------------")
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

	// sort the slice of key-value pairs by value in descending order
	sort.SliceStable(s, func(i, j int) bool {
		return s[i].Key < s[j].Key
	})

	type AttrSlice struct {
		Key   string
		Value update.History
	}

	// iterate over the slice to get the desired order
	for _, v := range s {
		fmt.Printf("%s", v.Key)
		sortedAttributes := make([]AttrSlice, 0, len(v.Value.Attributes))
		for k, v := range v.Value.Attributes {
			sortedAttributes = append(sortedAttributes, AttrSlice{k, v})
		}

		sort.SliceStable(sortedAttributes, func(i, j int) bool {
			return s[i].Key < s[j].Key
		})

		for _, w := range sortedAttributes {
			fmt.Printf(",%s=%s", w.Key, w.Value.Value)
		}
		fmt.Println()
	}
	/*for thisId, userRecord := range users {
		fmt.Printf("\n%s", thisId)
		for attrName, attrData := range userRecord.Attributes {
			fmt.Printf(",%s=%s", attrName, attrData.Value)
		}
		fmt.Printf("\n\n")
	}*/

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
