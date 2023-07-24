# userrecords
# Golang project

## Introduction
Consider users that change their profiles and order objects. 

Userrecords takes a generated set of actions for as many as one million users
and produces a text output file summarizing the actions for the users, sorted
by user ID. Each summary has an alphabetically sorted list of attribute names 
and their final values, followed by a sorted list of event names and a count
of their occurrences.

## How to try out this code
### Clone the repository to your local machine.
I prefer using *git clone ... * at the command line, but there are many ways.

### Generate input data
Input data can be generated with the code in the *generate* package. Run the desired command
in the *userrecords* directory.

**Small set of data**
`go run src/generate/main.go -out data/messages.1.data -verify data/verify.1.csv --seed 1560981440 -count 20`
**Medium set of data**
`go run src/generate/main.go -out data/messages.2.data -verify data/verify.2.csv --seed 1560980000 -count 10000 -attrs 20 -events 300000 -maxevents 500 -dupes 10`
**Large set of data** (will take ~10 minutes to generate or to process)
`go run src/generate/main.go -out data/messages.3.data -verify data/verify.3.csv --seed 1560000000 -count 1000000 -attrs 10 -events 5000000 -maxevents 10 -dupes 20`

### Designate the files to process
Set the input and verification data:
Line 49\* of src/main.go: `inputFileName = "../data/messages.2.data"`
Line 51\* of src/main.go: `validateFileName = "../data/verify.2.csv"`

for the medium set of data.

\*approximately

### Run the code
With the *main.go* code window selected, run the code with *Run->Start Debugging* or *Run->Run Without Debugging*.

When the code has run and verified the output, you will likely see only *Process xxx has exited with status 0* (0 designates success).

