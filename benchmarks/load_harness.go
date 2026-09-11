package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Config struct {
	RouterURL              string  `json:"router_url"`
	DurationSeconds        int     `json:"duration_seconds"`
	RequestRate            int     `json:"request_rate"`
	NumWorkers             int     `json:"num_workers"`
	StrongReadRatio        float64 `json:"strong_read_ratio"`
	WriteRatio             float64 `json:"write_ratio"`
	KeyDistribution        string  `json:"key_distribution"`
	KeyCount               int     `json:"key_count"`
	HotKeyRatio            float64 `json:"hot_key_ratio"`
	OutputFile             string  `json:"output_file"`
	MetricsIntervalSeconds int     `json:"metrics_interval_seconds"`
}

type Result struct {
	Operation   string    `json:"operation"`
	Key         string    `json:"key"`
	Success     bool      `json:"success"`
	LatencyMs   float64   `json:"latency_ms"`
	Consistency string    `json:"consistency,omitempty"`
	Error       string    `json:"error,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run load_harness.go <config.json>")
		os.Exit(1)
	}

	configFile := os.Args[1]
	config, err := loadConfig(configFile)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Starting load test with config:\n")
	fmt.Printf("  Router URL: %s\n", config.RouterURL)
	fmt.Printf("  Duration: %ds\n", config.DurationSeconds)
	fmt.Printf("  Request Rate: %d req/s\n", config.RequestRate)
	fmt.Printf("  Workers: %d\n", config.NumWorkers)
	fmt.Printf("  Strong Read Ratio: %.2f\n", config.StrongReadRatio)
	fmt.Printf("  Write Ratio: %.2f\n", config.WriteRatio)
	fmt.Printf("  Key Distribution: %s\n", config.KeyDistribution)
	fmt.Printf("  Key Count: %d\n", config.KeyCount)

	results := make(chan Result, 1000)
	var workersWg sync.WaitGroup
	var bgWg sync.WaitGroup
	startTime := time.Now()
	duration := time.Duration(config.DurationSeconds) * time.Second

	// Start result writer
	bgWg.Add(1)
	go func() {
		writeResults(results, config.OutputFile, &bgWg)
	}()

	// Start metrics scraper
	stopMetrics := make(chan struct{})
	// scraper runs as a background goroutine tracked by bgWg
	bgWg.Add(1)
	go scrapeMetrics(config.RouterURL, config.MetricsIntervalSeconds, &bgWg, stopMetrics)

	// Start workers
	rateLimiter := time.NewTicker(time.Second / time.Duration(config.RequestRate))
	defer rateLimiter.Stop()

	for i := 0; i < config.NumWorkers; i++ {
		workersWg.Add(1)
		go func(workerID int) {
			defer workersWg.Done()
			worker(workerID, config, rateLimiter, results, startTime, duration)
		}(i)
	}

	// Wait for duration to pass
	time.Sleep(duration)

	// Wait for workers to finish
	workersWg.Wait()

	// Signal background goroutines to stop
	close(results)
	close(stopMetrics)

	// Wait for result writer and metrics scraper to finish
	bgWg.Wait()

	fmt.Printf("Load test completed. Results written to %s\n", config.OutputFile)
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	// Set defaults
	if config.RouterURL == "" {
		config.RouterURL = "http://localhost:8080"
	}
	if config.DurationSeconds == 0 {
		config.DurationSeconds = 30
	}
	if config.RequestRate == 0 {
		config.RequestRate = 100
	}
	if config.NumWorkers == 0 {
		config.NumWorkers = 10
	}
	if config.KeyCount == 0 {
		config.KeyCount = 1000
	}
	if config.KeyDistribution == "" {
		config.KeyDistribution = "uniform"
	}
	if config.OutputFile == "" {
		config.OutputFile = "results.jsonl"
	}
	if config.MetricsIntervalSeconds == 0 {
		config.MetricsIntervalSeconds = 5
	}

	return &config, nil
}

func worker(workerID int, config *Config, rateLimiter *time.Ticker, results chan<- Result, startTime time.Time, duration time.Duration) {
	rand.Seed(time.Now().UnixNano() + int64(workerID))
	fmt.Printf("Worker %d started\n", workerID)

	requestCount := 0
	for time.Since(startTime) < duration {
		<-rateLimiter.C

		// Determine operation type
		var operation string
		r := rand.Float64()
		if r < config.WriteRatio {
			operation = "PUT"
		} else {
			operation = "GET"
		}

		// Determine consistency level for reads
		var consistency string
		if operation == "GET" {
			if rand.Float64() < config.StrongReadRatio {
				consistency = "strong"
			} else {
				consistency = "eventual"
			}
		} else {
			consistency = "strong" // Writes always use strong consistency
		}

		// Select key
		key := selectKey(config.KeyDistribution, config.KeyCount, config.HotKeyRatio)

		start := time.Now()
		var success bool
		var err error

		switch operation {
		case "PUT":
			value := fmt.Sprintf("value-%d", rand.Intn(10000))
			success, err = doPut(config.RouterURL, key, value)
		case "GET":
			success, err = doGet(config.RouterURL, key, consistency)
		}

		latency := time.Since(start)
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}

		results <- Result{
			Operation:   operation,
			Key:         key,
			Success:     success,
			LatencyMs:   float64(latency.Nanoseconds()) / 1_000_000.0,
			Consistency: consistency,
			Error:       errorMsg,
			Timestamp:   start,
		}
		requestCount++
	}
	fmt.Printf("Worker %d finished, sent %d requests\n", workerID, requestCount)
}

func selectKey(distribution string, keyCount int, hotKeyRatio float64) string {
	switch distribution {
	case "uniform":
		return fmt.Sprintf("key-%d", rand.Intn(keyCount))
	case "skewed":
		if rand.Float64() < hotKeyRatio {
			return fmt.Sprintf("key-%d", rand.Intn(keyCount/10))
		}
		return fmt.Sprintf("key-%d", rand.Intn(keyCount))
	default:
		return fmt.Sprintf("key-%d", rand.Intn(keyCount))
	}
}

func doPut(routerURL, key, value string) (bool, error) {
	url := fmt.Sprintf("%s/kv/%s", routerURL, key)
	payload := map[string]string{"value": value}
	jsonPayload, _ := json.Marshal(payload)

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200, nil
}

func doGet(routerURL, key, consistency string) (bool, error) {
	url := fmt.Sprintf("%s/kv/%s?consistency=%s", routerURL, key, consistency)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200, nil
}

func writeResults(results <-chan Result, outputFile string, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Starting result writer for %s\n", outputFile)
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	count := 0
	timeout := time.After(120 * time.Second)

	for {
		select {
		case result, ok := <-results:
			if !ok {
				fmt.Printf("Results channel closed, wrote %d results to %s\n", count, outputFile)
				return
			}
			if err := encoder.Encode(result); err != nil {
				fmt.Printf("Error encoding result: %v\n", err)
			}
			count++
		case <-timeout:
			fmt.Printf("Result writer timeout after 120s, wrote %d results to %s\n", count, outputFile)
			return
		}
	}
}

type ShardResponse struct {
	Shards []struct {
		ID    string   `json:"id"`
		Nodes []string `json:"nodes"`
	} `json:"shards"`
}

func scrapeMetrics(routerURL string, interval int, wg *sync.WaitGroup, stop <-chan struct{}) {
	defer wg.Done()

	resp, err := http.Get(routerURL + "/cluster/shards")
	if err != nil {
		fmt.Printf("Failed to get shards: %v\n", err)
		return
	}
	defer resp.Body.Close()
	
	var shardResp ShardResponse
	if err := json.NewDecoder(resp.Body).Decode(&shardResp); err != nil {
		fmt.Printf("Failed to decode shards: %v\n", err)
		return
	}
	
	var nodes []string
	for _, shard := range shardResp.Shards {
		nodes = append(nodes, shard.Nodes...)
	}
	fmt.Printf("Will scrape metrics from nodes: %v\n", nodes)

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	
	os.MkdirAll("metrics_output", 0755)
	
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, nodeURL := range nodes {
				url := nodeURL
				if !strings.HasPrefix(url, "http://") {
					url = "http://" + url
				}
				url = url + "/metrics"
				
				resp, err := http.Get(url)
				if err != nil {
					fmt.Printf("Failed to fetch metrics from %s: %v\n", url, err)
					continue
				}
				
				timestamp := time.Now().Unix()
				nodeName := strings.Replace(nodeURL, ":", "_", -1)
				nodeName = strings.Replace(nodeName, "http//", "", -1)
				nodeName = strings.Replace(nodeName, "/", "", -1)
				
				f, err := os.Create(fmt.Sprintf("metrics_output/%s_%d.txt", nodeName, timestamp))
				if err == nil {
					io.Copy(f, resp.Body)
					f.Close()
				}
				resp.Body.Close()
			}
		}
	}
}
