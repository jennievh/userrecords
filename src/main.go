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
	"stream"
	"syscall"
	"update"
	"usersort"

	"net/http"
	_ "net/http/pprof" // Blank import to pprof

	"github.com/pkg/profile"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

const chunk = 10

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

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

	inputFileName = "../data/messages.3.data"
	outputFileName = inputFileName + ".out.csv"
	validateFileName = "../data/verify.3.csv"

	defer profile.Start(profile.GoroutineProfile).Stop()

	inputFile, err := os.Open(inputFileName)
	check(err)

	inputFileSeek := io.ReadSeeker(inputFile)

	streamingChannel, err := stream.Process(ctx, inputFileSeek)
	check(err)

	users := make(map[string]update.UserRecord, chunk)

	// DEBUG
	debugging.Setdebug(debugging.DEBUG_OUTPUT)
	count := 0
	for rec := range streamingChannel {

		count++

		_ = rec
		if rec.UserID == "" { // that's a bug; continue
			continue
		}
		users := update.FindOrCreateUser(users, rec.UserID)

		user := users[rec.UserID]
		user.UserID = rec.UserID
		if debugging.Getdebug() == debugging.DEBUG_ALL {
			printing.PrintIncomingRecord(rec)
		}

		// Attribute, or event?
		switch rec.Type {
		case "attributes":

			for attr, val := range rec.Data {
				userAttrs := update.FindOrCreateAttr(user.Attributes, attr)

				if userAttrs[attr].Timestamp < rec.Timestamp {
					userAttrs[attr] = update.History{Value: val, Timestamp: rec.Timestamp}
				}
				user.Attributes = userAttrs
			}
			users[user.UserID] = user
			if debugging.Getdebug() == debugging.DEBUG_ATTRIBUTES {
				printing.PrintAttributesForUser(user.Attributes, user.UserID)
			}

		case "event":
			event := rec.Name
			eventID := rec.ID

			events := update.FindOrCreateEvent(user.Events, event, eventID)

			for occurrence, idStrings := range events {
				if event == occurrence {
					found := false
					for _, str := range idStrings {
						if str == eventID {
							found = true
							break
						}
					}

					if !found {
						if debugging.Getdebug() == debugging.DEBUG_ALL {
							printing.PrintList(idStrings, "Event IDs before appending")
						}

						idStrings = append(idStrings, eventID)
						events[event] = idStrings

						if debugging.Getdebug() == debugging.DEBUG_ALL {
							printing.PrintList(idStrings, "Event IDs after appending")
							break
						}
					}
				}
			}
			user.Events = events
			users[user.UserID] = user

			if debugging.Getdebug() == debugging.DEBUG_EVENTS {
				printing.PrintEventsForUser(user.Events, user.UserID)
			}

		default:
			errorString = fmt.Sprintf("Event type %s not recognized!", rec.Type)
			log.Fatal(errorString)
		}
	}

	if debugging.Getdebug() == debugging.DEBUG_ALL {
		fmt.Printf("Data so far is \n")

		for _, userRecord := range users {
			printing.PrintAttributesForUser(userRecord.Attributes, userRecord.UserID)
			printing.PrintEventsForUser(userRecord.Events, userRecord.UserID)
		}
	}

	outputfile, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	check(err)

	/* Now the users need to be sorted by ID. We'll do that by making a slice of them
	   and using StableSort on that slice. Within the Usersort call, a similar process
	   will happen on each user's attributes and events, so that all are sorted before
	   writing out the info.
	*/

	users_slice := usersort.Usersort(users)

	for _, v := range users_slice {
		result := usersort.UserValuesSort(v.Value)

		lastChar := result[len(result)-1:]
		if lastChar == "," {
			result = result[:(len(result) - 2)]
		}

		debugging.Debug(debugging.DEBUG_ALL, "%s", "about to write out results")
		outputfile.WriteString(result)
	}

	// Close files
	defer inputFile.Close()
	defer outputfile.Close()

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
