package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/replay"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	endpoint := flag.String("url", "http://localhost:18081/v1/replays", "admin endpoint")
	key := flag.String("key", "demo-key", "admin API key")
	file := flag.String("request", "", "bounded replay request JSON file")
	flag.Parse()
	if *file == "" {
		flag.Usage()
		os.Exit(2)
	}
	b, err := os.ReadFile(*file)
	if err != nil {
		panic(err)
	}
	var r replay.Request
	if json.Unmarshal(b, &r) != nil {
		panic("invalid request")
	}
	req, err := http.NewRequest("POST", *endpoint, bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+*key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 40 * time.Second}).Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ = io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	fmt.Println(string(b))
	if resp.StatusCode != 200 {
		os.Exit(1)
	}
}
