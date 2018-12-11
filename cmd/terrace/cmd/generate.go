package cmd

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
	"io/ioutil"
	"log"
	"os"

	"github.com/Preetam/terrace"
	"github.com/spf13/cobra"
)

type generateCommand struct {
	cobraCommand *cobra.Command

	// Args
	inFile  string
	outFile string
	// Flags
	constraintsFile string
	format          string
	fast            bool
	sizeCost        bool
	verbose         bool
}

func (cmd *generateCommand) Run() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Println("Running generate")

	eventsFile, err := ioutil.ReadFile(cmd.inFile)
	if err != nil {
		logger.Fatal(err)
	}

	constraints := []terrace.ConstraintSet{}
	if cmd.constraintsFile != "" {
		constraintsFile, err := os.Open(cmd.constraintsFile)
		if err != nil {
			logger.Fatal(err)
		}
		err = json.NewDecoder(constraintsFile).Decode(&constraints)
		if err != nil {
			logger.Fatalf("error reading constraints file: %v", err)
		}
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

	opts := terrace.Options{
		Fast:     cmd.fast,
		CostType: terrace.CostTypeAccess,
	}
	if cmd.sizeCost {
		opts.CostType = terrace.CostTypeSize
	}
	level, err := terrace.Generate(logger, events, constraints, opts)
	if err != nil {
		logger.Fatalf("error generating Terrace file: %v", err)
	}

	outFile, err := os.OpenFile(cmd.outFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logger.Fatal(err)
	}

	err = json.NewEncoder(outFile).Encode(level)
	if err != nil {
		logger.Fatalf("error writing Terrace file: %v", err)
	}

}

func init() {
	generateCmd := &generateCommand{
		cobraCommand: &cobra.Command{
			Use:   "generate <input file> <output file>",
			Short: "Generate a Terrace file",
			Args:  cobra.MinimumNArgs(2),
		},
	}
	generateCmd.cobraCommand.Run = func(cmd *cobra.Command, args []string) {
		generateCmd.inFile = args[0]
		generateCmd.outFile = args[1]
		generateCmd.Run()
	}

	rootCmd.AddCommand(generateCmd.cobraCommand)

	generateCmd.cobraCommand.
		Flags().StringVar(&generateCmd.constraintsFile, "constraints", "", "Constraints file")
	generateCmd.cobraCommand.
		Flags().StringVar(&generateCmd.format, "format", "json", "Output file format")
	generateCmd.cobraCommand.
		Flags().BoolVar(&generateCmd.fast, "fast", true, "Fast generation")
	generateCmd.cobraCommand.
		Flags().BoolVar(&generateCmd.sizeCost, "size-cost", false, "Size-based cost")
	generateCmd.cobraCommand.
		Flags().BoolVarP(&generateCmd.verbose, "verbose", "v", false, "Verbose logging")
}
