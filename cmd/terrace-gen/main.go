// terrace-gen is a generator for Terrace files.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"

	"github.com/Preetam/terrace"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	inFileFlag := flag.String("in", "", "input file (one JSON event per line)")
	constraintsFileFlag := flag.String("constraints", "", "constraints file (JSON)")
	outFileFlag := flag.String("out", "", "output file")
	flag.Parse()

	if *inFileFlag == "" || *constraintsFileFlag == "" || *outFileFlag == "" {
		logger.Printf("missing in, constraints, or out filename(s)")
		flag.Usage()
		os.Exit(1)
	}

	eventsFile, err := ioutil.ReadFile(*inFileFlag)
	if err != nil {
		logger.Fatal(err)
	}

	constraintsFile, err := os.Open(*constraintsFileFlag)
	if err != nil {
		logger.Fatal(err)
	}

	cs := terrace.ConstraintSet{}
	err = json.NewDecoder(constraintsFile).Decode(&cs)
	if err != nil {
		logger.Fatalf("error reading constraints file: %v", err)
	}

	events := []terrace.Event{}
	for _, eventBytes := range bytes.Split(eventsFile, []byte("\n")) {
		e := terrace.Event{}
		if len(eventBytes) == 0 {
			continue
		}
		err = json.Unmarshal(bytes.TrimSpace(eventBytes), &e)
		if err != nil {
			logger.Fatal(err)
		}
		events = append(events, e)
	}

	level, err := terrace.Generate(logger, events, cs)
	if err != nil {
		logger.Fatalf("error generating Terrace file: %v", err)
	}

	outFile, err := os.OpenFile(*outFileFlag, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logger.Fatal(err)
	}

	err = json.NewEncoder(outFile).Encode(level)
	if err != nil {
		logger.Fatalf("error writing Terrace file: %v", err)
	}
}
