package main

import (
	"log"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"fmt"
	"strings"
)


func main() {
	processGenotypes()
}

func processGenotypes() {
	f, err := os.Open("data/gdb_10.tsv")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer f.Close()


	output := ""
	reader := csv.NewReader(f)
	reader.Comma = '\t'
	rowIndex := 0
	for {
		row, err := reader.Read()
		if rowIndex == 0 {
			rowIndex++
			continue
		}

		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		sampleId, err := strconv.Atoi(row[0])
		if err != nil {
			log.Fatal(err.Error())
		}
		variantKey := strings.Replace(row[1], ":", "-", -1)

		chrom := strings.Split(variantKey, "-")[0]
		start, err := strconv.Atoi(strings.Split(variantKey, "-")[1])
		if err != nil {
			log.Fatal(err.Error())
		}
		AF, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			log.Fatal(err.Error())
		}

		output += fmt.Sprintf("_:sample%d <name> \"%d\" .\n", sampleId,  sampleId)
		output += fmt.Sprintf("_:variant%s <name> \"%s\" .\n", variantKey,  variantKey)
		output += fmt.Sprintf("_:variant%s <chrom> \"%s\" .\n", variantKey,  chrom)
		output += fmt.Sprintf("_:variant%s <start> \"%d\" .\n", variantKey,  start)
		output += fmt.Sprintf("_:variant%s <AF> \"%f\" .\n", variantKey,  AF)
		output += fmt.Sprintf("_:sample%d <variant> _:variant%s .\n", sampleId,  variantKey)

		rowIndex++
	}
	of, err := os.Create("data/test.rdf")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer of.Close()
	of.WriteString(output)
}

