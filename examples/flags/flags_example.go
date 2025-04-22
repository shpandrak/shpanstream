package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/utils/jsonstream"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	codesURL       = "https://flagcdn.com/en/codes.json"
	svgURLTemplate = "https://flagcdn.com/%s.svg" // %s will be replaced by country code
	newWidth       = "200"                        // The desired fixed width for the output SVG
)

// CountryInfo holds the final structured data for each country
type CountryInfo struct {
	CountryCode string `json:"countryCode"`
	Name        string `json:"name"`
	SVG         string `json:"svg"` // Stores the modified SVG content as a string
}

var outputFilenameFlag = flag.String("output-file", "", "Output filename")
var concurrencyFlag = flag.Int("concurrency", 10, "Number of concurrent requests")

// Fun with flags!
// This example demonstrates how to use the shpanstream package to fetch country codes and their corresponding SVG flags concurrently.
// all of this is done in a streaming fashion, without holding the entire data in memory.
// to control the concurrency, we use a flag to specify the number of concurrent requests.
// Example outputs:
//
// concurrency of 10: "Flags example with concurrency of 10 finished in 421.168417ms"
// concurrency of 5:  "Flags example with concurrency of 5 finished in 874.123833ms"
// concurrency of 2:  "Flags example with concurrency of 2 finished in 1.737893375s"
// concurrency of 1:  "Flags example with concurrency of 1 finished in 3.412212583s"

func main() {
	// parse command line arguments
	flag.Parse()

	// by default, output to stdout
	outputWriter := os.Stdout

	// if an output filename is provided, open the file for writing
	if outputFilenameFlag != nil && *outputFilenameFlag != "" {
		log.Printf("Output filename: %s", *outputFilenameFlag)

		file, err := os.Create(*outputFilenameFlag)
		if err != nil {
			log.Fatalf("failed to open output file: %v", err)
		}
		defer file.Close()
	}

	// By default, the concurrency is set to 10
	flagRequestsConcurrency := 10

	// if a concurrency flag is provided, use that value
	if concurrencyFlag != nil && *concurrencyFlag > 0 {
		flagRequestsConcurrency = *concurrencyFlag
	}

	startTime := time.Now()

	// Using the StreamJsonToWriter helper to stream the JSON output directly to the writer
	err := jsonstream.StreamJsonToWriter(
		context.Background(),
		outputWriter,
		shpanstream.MapStreamWithErrAndCtx(
			fetchCountryCodeToNames(),
			fetchCountryFlag,
			shpanstream.WithConcurrentMapStreamOption(flagRequestsConcurrency),
		),
	)

	if err != nil {
		log.Fatalf("failed to write JSON: %v", err)
	}
	log.Printf("\n\nFlags example with concurrency of %d finished in %s", flagRequestsConcurrency, time.Since(startTime))

}

func fetchCountryFlag(_ context.Context, e shpanstream.Entry[string, string]) (CountryInfo, error) {

	svg, err := fetchCountrySvg(e.Key, e.Value)
	if err != nil {
		return CountryInfo{},
			fmt.Errorf("failed to fetch SVG for %s (%s): %w", e.Value, e.Key, err)
	}

	return CountryInfo{
		CountryCode: e.Key,
		Name:        e.Value,
		SVG:         fmt.Sprintf("<img width=\"%s\" src=\"%s\"/>", newWidth, svgToBase64DataURI(string(svg))),
	}, nil
}

func fetchCountryCodeToNames() shpanstream.Stream[shpanstream.Entry[string, string]] {
	return jsonstream.ReadJsonObject[string](func(ctx context.Context) (io.ReadCloser, error) {
		resp, err := http.Get(codesURL)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch URL %s: %w", codesURL, err)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to fetch URL %s: status code %d", codesURL, resp.StatusCode)
		}
		return resp.Body, nil
	})
}

func fetchCountrySvg(countryCode string, countryName string) ([]byte, error) {
	svgURL := fmt.Sprintf(svgURLTemplate, countryCode)
	log.Printf("Fetching SVG for %s (%s)...", countryName, countryCode)

	svgResp, err := http.Get(svgURL)
	if err != nil {
		return nil, fmt.Errorf("ERROR: Failed to fetch SVG for %s (%s) from %s: %w", countryName, countryCode, svgURL, err)
	}
	defer svgResp.Body.Close()

	if svgResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ERROR: Failed to fetch SVG for %s (%s), status code: %d: %w", countryName, countryCode, svgResp.StatusCode, err)
	}

	svgBytes, err := io.ReadAll(svgResp.Body)
	if err != nil {
		return nil, fmt.Errorf("ERROR: Failed to read SVG body for %s (%s): %w", countryName, countryCode, err)
	}
	return svgBytes, nil
}

// modifySVG takes raw SVG content, extracts original dimensions,
// sets a new width, and adds a viewBox if not already present.

func svgToBase64DataURI(svg string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(svg))
	return "data:image/svg+xml;base64," + encoded
}
