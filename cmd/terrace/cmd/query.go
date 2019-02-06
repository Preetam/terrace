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
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Preetam/query"

	"github.com/Preetam/terrace"
	"github.com/spf13/cobra"
)

type queryCommand struct {
	cobraCommand *cobra.Command

	// Args
	terraceFile string
	query       string
}

func (cmd *queryCommand) Run() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Println("Running query")

	terraceFile, err := os.Open(cmd.terraceFile)
	if err != nil {
		logger.Fatal(err)
	}

	level := &terrace.Level{}
	err = json.NewDecoder(terraceFile).Decode(level)
	if err != nil {
		logger.Fatalf("error reading Terrace file: %v", err)
	}

	parsedQuery, err := query.Parse(cmd.query)
	if err != nil {
		logger.Fatalf("error parsing query: %v", err)
	}

	executor := query.NewExecutor(level)
	queryResult, err := executor.Execute(parsedQuery)
	if err != nil {
		logger.Fatalf("error executing query: %v", err)
	}

	for _, row := range queryResult.Rows() {
		marshaled, _ := json.Marshal(row)
		fmt.Printf("%s\n", marshaled)
	}
}

func init() {
	queryCmd := &queryCommand{
		cobraCommand: &cobra.Command{
			Use:   "query <input file> <query>",
			Short: "Query a Terrace file",
			Args:  cobra.MinimumNArgs(2),
		},
	}
	queryCmd.cobraCommand.Run = func(cmd *cobra.Command, args []string) {
		queryCmd.terraceFile = args[0]
		queryCmd.query = args[1]
		queryCmd.Run()
	}
	rootCmd.AddCommand(queryCmd.cobraCommand)
}
