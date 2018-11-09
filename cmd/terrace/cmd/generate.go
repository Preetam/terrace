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
	"fmt"

	"github.com/spf13/cobra"
)

var generateOpts = struct {
	// Args
	inFile  string
	outFile string
	// Flags
	constraintsFile string
	format          string
	verbose         bool
}{}

// generateCmd represents the generate command
var generateCmd = &cobra.Command{
	Use:   "generate <input file> <output file>",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generate called")
		generateOpts.inFile = args[0]
		generateOpts.outFile = args[1]
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)

	generateCmd.Flags().StringVar(&generateOpts.constraintsFile, "constraints", "", "Constraints file")
	generateCmd.Flags().StringVar(&generateOpts.format, "format", "json", "Output file format")
	generateCmd.Flags().BoolVarP(&generateOpts.verbose, "verbose", "v", false, "Verbose logging")
}
