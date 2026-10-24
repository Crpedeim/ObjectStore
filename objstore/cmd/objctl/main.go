// Command objctl is the object store's command-line client. It also runs the
// load balancer that sits in front of the nodes.
//
//	objctl put [-context C] KEY [FILE]   store FILE (or standard input) under KEY
//	objctl get [-version V] KEY          write KEY's value to standard output
//	objctl delete [-context C] KEY       delete KEY
//	objctl members                       list the cluster's nodes
//	objctl lb -backends A,B,...          run the load balancer
//
// Every command but lb talks to -addr, else $OBJSTORE_ADDR, else
// localhost:8080: the load balancer scripts/cluster.sh starts.
//
// Exit codes: 0 success; 1 not found; 2 bad usage or another error;
// 3 conflict (get found concurrent versions); 4 unavailable (too few
// replicas answered — for a write, the outcome is unknown).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Crpedeim/objstore/internal/client"
	"github.com/Crpedeim/objstore/internal/lb"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: objctl [-addr host:port] COMMAND ...

  put [-context C] KEY [FILE]   store FILE (or standard input) under KEY
  get [-version V] KEY          write KEY's value to standard output
  delete [-context C] KEY       delete KEY
  members                       list the cluster's nodes
  lb -backends A,B,...          run the load balancer

-addr defaults to $OBJSTORE_ADDR, else localhost:8080.
Exit codes: 0 ok, 1 not found, 2 usage or error, 3 conflict, 4 unavailable.
`)
	os.Exit(2)
}

func main() {
	addr := os.Getenv("OBJSTORE_ADDR")
	if addr == "" {
		addr = "localhost:8080"
	}
	flag.StringVar(&addr, "addr", addr, "a node or load balancer, host:port")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
	}
	c := &client.Client{Addr: addr}
	cmd, args := flag.Arg(0), flag.Args()[1:]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch cmd {
	case "put":
		put(ctx, c, args)
	case "get":
		get(ctx, c, args)
	case "delete":
		del(ctx, c, args)
	case "members":
		members(ctx, c)
	case "lb":
		runLB(ctx, args)
	default:
		usage()
	}
}

// fail reports err and exits with the code that says what kind it was.
func fail(err error, write bool) {
	var conflict *client.ConflictError
	switch {
	case errors.Is(err, client.ErrNotFound):
		fmt.Fprintln(os.Stderr, "not found")
		os.Exit(1)
	case errors.As(err, &conflict):
		os.Exit(3) // get prints the details itself
	case errors.Is(err, client.ErrUnavailable):
		if write {
			fmt.Fprintln(os.Stderr, "unavailable: too few replicas answered; the write may or may not have been stored")
		} else {
			fmt.Fprintln(os.Stderr, "unavailable: too few replicas answered")
		}
		os.Exit(4)
	}
	fmt.Fprintln(os.Stderr, "objctl:", err)
	os.Exit(2)
}

func put(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	withCtx := fs.String("context", "", "replace exactly what a get returned; empty means replace whatever a quorum holds")
	fs.Parse(args)
	if fs.NArg() < 1 || fs.NArg() > 2 {
		usage()
	}
	key, body, size := fs.Arg(0), io.Reader(os.Stdin), int64(-1)
	if fs.NArg() == 2 && fs.Arg(1) != "-" {
		f, err := os.Open(fs.Arg(1))
		if err != nil {
			fail(err, true)
		}
		defer f.Close()
		if info, err := f.Stat(); err == nil {
			size = info.Size()
		}
		body = f
	}
	w, err := c.Put(ctx, key, body, size, *withCtx)
	if err != nil {
		fail(err, true)
	}
	fmt.Fprintf(os.Stderr, "stored %s: version %s, on %d replicas\ncontext: %s\n", key, w.Version, w.Acks, w.Context)
}

func get(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	version := fs.String("version", "", "read this version, e.g. one of several listed after a conflict")
	verbose := fs.Bool("v", false, "print the version and context to standard error")
	fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	key := fs.Arg(0)
	var o *client.Object
	var err error
	if *version != "" {
		o, err = c.GetVersion(ctx, key, *version)
	} else {
		o, err = c.Get(ctx, key)
	}
	var conflict *client.ConflictError
	if errors.As(err, &conflict) {
		fmt.Fprintf(os.Stderr, "%s has %d concurrent versions:\n", key, len(conflict.Versions))
		for _, v := range conflict.Versions {
			if v.Deleted {
				fmt.Fprintf(os.Stderr, "  %s  (a delete)\n", v.Version)
			} else {
				fmt.Fprintf(os.Stderr, "  %s  (%d bytes)\n", v.Version, v.Size)
			}
		}
		fmt.Fprintf(os.Stderr, "read one:  objctl get -version VERSION %s\nresolve:   objctl put -context %s %s FILE\n", key, conflict.Context, key)
	}
	if err != nil {
		fail(err, false)
	}
	defer o.Body.Close()
	if _, err := io.Copy(os.Stdout, o.Body); err != nil {
		fail(err, false)
	}
	if *verbose {
		fmt.Fprintf(os.Stderr, "version: %s\ncontext: %s\n", o.Version, o.Context)
	}
}

func del(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	withCtx := fs.String("context", "", "delete exactly what a get returned; empty means whatever a quorum holds")
	fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	w, err := c.Delete(ctx, fs.Arg(0), *withCtx)
	if err != nil {
		fail(err, true)
	}
	fmt.Fprintf(os.Stderr, "deleted %s: version %s, on %d replicas\n", fs.Arg(0), w.Version, w.Acks)
}

func members(ctx context.Context, c *client.Client) {
	ms, err := c.Members(ctx)
	if err != nil {
		fail(err, false)
	}
	for _, m := range ms {
		fmt.Printf("%-6s %-22s %s\n", m.ID, m.Addr, m.State)
	}
}

func runLB(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("lb", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8080", "address to serve on")
	backends := fs.String("backends", "", "the nodes' HTTP addresses, comma-separated")
	every := fs.Duration("health-every", time.Second, "how often to check each node's /healthz")
	fs.Parse(args)
	if *backends == "" {
		usage()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	b := lb.New(strings.Split(*backends, ","), log)
	go b.Run(ctx, *every)
	srv := &http.Server{Addr: *listen, Handler: b, ReadHeaderTimeout: 10 * time.Second}
	// ListenAndServe returns as soon as Shutdown begins, so wait for it to
	// finish the requests in flight before exiting.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Info("lb: serving", "listen", *listen, "backends", *backends)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fail(err, false)
	}
	<-drained
}
