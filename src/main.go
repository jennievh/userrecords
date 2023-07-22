package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
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

	//var users map[string]update.UserRecord
	CHUNK := 10
	users := make(map[string]update.UserRecord, CHUNK)

	// DEBUG
	count := 0
	for rec := range ch {
		count++
		if count > 10 {
			os.Exit(0)
		}
		_ = rec
		// THIS IS WHERE THE MAGIC HAPPENS
		fmt.Printf("input id: %s,", rec.ID)
		recs, user, ok := update.FindOrCreate(users, rec.ID)
		//DEBUG: test recs
		fmt.Printf("in main.go:\n")
		for id, record := range recs {
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
				//Remember to check the timestamp
			}
			fmt.Printf("\n")
		}
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
