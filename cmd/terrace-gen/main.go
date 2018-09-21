// terrace-gen is a generator for Terrace files.
package main

/**
 * Copyright (C) 2018 Preetam Jinka
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
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
	sizeCost := flag.Bool("size-cost", false, "use size-based cost calculation")
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

	constraints := []terrace.ConstraintSet{}
	err = json.NewDecoder(constraintsFile).Decode(&constraints)
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

	costType := terrace.CostTypeAccess
	if *sizeCost {
		costType = terrace.CostTypeSize
	}
	level, err := terrace.Generate(logger, events, constraints, costType)
	if err != nil {
		logger.Fatalf("error generating Terrace file: %v", err)
	}

	outFile, err := os.OpenFile(*outFileFlag, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logger.Fatal(err)
	}

	err = json.NewEncoder(outFile).Encode(level)
	if err != nil {
		logger.Fatalf("error writing Terrace file: %v", err)
	}

	fmt.Println(level)
}
