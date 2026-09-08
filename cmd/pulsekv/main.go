package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/MetalCloth/pulsekv-go/internal/server"
)

func main() {
	config := server.DefaultConfig()
	port := 6379
	appendOnly := "no"
	flag.IntVar(&port, "port", port, "TCP port to listen on")
	flag.StringVar(&config.Dir, "dir", config.Dir, "directory for RDB and AOF files")
	flag.StringVar(&config.DBFilename, "dbfilename", "", "RDB filename to load and use for SAVE")
	flag.StringVar(&appendOnly, "appendonly", "no", "enable append-only persistence (yes/no)")
	flag.StringVar(&config.AppendDir, "appenddirname", config.AppendDir, "AOF directory under -dir")
	flag.StringVar(&config.AppendFilename, "appendfilename", config.AppendFilename, "AOF manifest filename")
	flag.StringVar(&config.AppendFsync, "appendfsync", config.AppendFsync, "AOF durability: always, everysec, or no")
	flag.StringVar(&config.ReplicaOf, "replicaof", "", "master as HOST PORT for replica mode")
	flag.Parse()
	config.Addr = ":" + strconv.Itoa(port)
	config.AppendOnly = appendOnly == "yes"
	if appendOnly != "yes" && appendOnly != "no" {
		fmt.Fprintln(os.Stderr, "-appendonly must be yes or no")
		os.Exit(2)
	}

	srv, err := server.New(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer srv.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	fmt.Printf("pulsekv listening on %s\n", config.Addr)
	if err := srv.ListenAndServe(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
