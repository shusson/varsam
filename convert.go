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
	f, err := os.Open("data/gdb_1m.tsv")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer f.Close()

	of, err := os.Create("data/test.rdf")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer of.Close()


	output := make([]byte, 0, 0)
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

		output = append(output, fmt.Sprintf("_:sample%d <name> \"%d\" .\n", sampleId,  sampleId)...)
		output = append(output, fmt.Sprintf("_:sample%d <sampleId> \"%d\" .\n", sampleId,  sampleId)...)
		output = append(output, fmt.Sprintf("_:variant%s <name> \"%s\" .\n", variantKey,  variantKey)...)
		output = append(output, fmt.Sprintf("_:variant%s <chrom> \"%s\" .\n", variantKey,  chrom)...)
		output = append(output, fmt.Sprintf("_:variant%s <start> \"%d\" .\n", variantKey,  start)...)
		output = append(output, fmt.Sprintf("_:variant%s <AF> \"%f\" .\n", variantKey,  AF)...)
		output = append(output, fmt.Sprintf("_:sample%d <variant> _:variant%s .\n", sampleId,  variantKey)...)

		rowIndex++
		if rowIndex % 1000000 == 0 {
			p := (float32(rowIndex)/67695719.0) * 100.0
			fmt.Printf("%.1f\n", p)
			of.Write(output)
			output = make([]byte, 0, 0)
		}
	}
}

