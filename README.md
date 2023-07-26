# userrecords, a Golang project

## Introduction
Consider users that change their profiles and order objects. 

Userrecords takes a generated set of actions for as many as one million users
and produces a text output file summarizing the actions for each user, sorted
by user ID. Each summary has the user ID, an alphabetically sorted list 
of attribute names and their final values, followed by a sorted list 
of event names and a count of their occurrences.

### Example input
> {"data":{"city":"Lake Kilback","created_at":"1547848970","email":"theresaauer@collier.net"},"id":"11ee220f-84b5-4fd3-a89e-989be5330c13","timestamp":1560981770,"type":"attributes","user_id":"54639"}
>{"data":{"bluetoothteal":"OakAgedYetiImperialStout","semioticsonline":"programupward-trending"},"id":"fa5bc2c6-7eaa-4f57-9a1a-649602691286","name":"bypassmagenta","timestamp":1560981770,"type":"event","user_id":"54639"}
>{"data":{"lancianorwood":"jeanshortsharddrive","lincolnausten":"narwhalarray","subaruwill":"parkpanel"},"id":"e65b58a3-936f-4dbd-b3a8-819d03104390","timestamp":1560544910,"type":"attributes","user_id":"885393"}
>{"data":{"mobileaqua":"StoneImperialRussianStout","schlitzdigital":"navigateoptimal"},"id":"3efa7a2d-216f-4c31-a468-dd1ea0a9a3a4","name":"navigategreen","timestamp":1560544910,"type":"event","user_id":"885393"}
>{"data":{"daihatsuvernon":"gentrifycapacitor","lamborghinicolton":"hoodieharddrive","martinicasimir":"literallybus"},"id":"f59f8bc6-4797-441b-bf46-28e090aea473","timestamp":1559727170,"type":"attributes","user_id":"236195"}

### Example output
>1,bugattiemilia=AgentStamm,citroenfavian=StrategistYundt,city=Millsside,created_at=1537565600,email=randalcole@denesik.org,first_name=Wava,ip=182.58.181.81,jaguarjan=OrchestratorLangosh,jeepqueen=AgentRunolfsson,last_name=Romaguera,mercedes-benzalison=DirectorBauch,tesladejuan=OrchestratorMohr,backupindigo=1,backupred=1,calculatelime=1,calculatepalegreen=1,compresschocolate=1,compressfirebrick=1,compresssteelblue=1,connectmediumblue=1,connectorangered=1,copytomato=1,generatehotpink=1,hackdarkblue=1,indexpowderblue=1,inputseagreen=1,navigatepeachpuff=1,navigatesalmon=1,programchartreuse=1,programdarkmagenta=1,programlightseagreen=1,quantifycoral=1,quantifywhite=1,rebootslateblue=1,transmitdarkorchid=1,transmitsaddlebrown=1

*N.B.: this is not the output from the example input lines*

## How to try out this code

## Input data generated
Input data can be generated with the code in the *generate* package. Run the desired command
in the *userrecords* directory in the *TERMINAL* window.

**Small set of data**

`go run src/generate/main.go -out data/messages.1.data -verify data/verify.1.csv --seed 1560981440 -count 20`

**Medium set of data**

`go run src/generate/main.go -out data/messages.2.data -verify data/verify.2.csv --seed 1560980000 -count 10000 -attrs 20 -events 300000 -maxevents 500 -dupes 10`

**Large set of data** (will take ~10 minutes to generate or to process)

`go run src/generate/main.go -out data/messages.3.data -verify data/verify.3.csv --seed 1560000000 -count 1000000 -attrs 10 -events 5000000 -maxevents 10 -dupes 20`

### Note
The small set of data is in this repo. Not all input files could be checked in due to size.

### Designate the files to process
Set the input and verification data:

Line 49\* of src/main.go: `inputFileName = "../data/messages.2.data"`

Line 51\* of src/main.go: `validateFileName = "../data/verify.2.csv"`

for the medium set of data.

\**approximately*

### Run the code
With the *main.go* code window selected, run the code with *Run->Start Debugging* or *Run->Run Without Debugging*.

When the code has run and verified the output, you will likely see only *Process xxx has exited with status 0* (0 designates success).

## Results
The output and verification text files will have been stored as you designated in https://github.com/jennievh/userrecords#designate-the-files-to-process and can be compared at the command line with *diff inputfile verificationfile*.

### Improvements
The *implement_db* branch re-implements the data storage and manipulation with MySQL. It works for the 
small data case, but is slow for the other data cases. *to be resolved*