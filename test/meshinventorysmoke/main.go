//go:build meshinventorysmoke

// meshinventorysmoke drives the mesh inventory source against a live sandboxd mesh with no Kubernetes:
// it lists the nodes, prints each node's capacity, claims one sandbox through the store and releases it.
//
//	go run -tags meshinventorysmoke ./test/meshinventorysmoke \
//	  -seeds 10.0.0.5:7777 -token-file /etc/sandboxd/token -template ghcr.io/cocoonstack/sandbox/rt:24.04
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/meshinventory"
)

func main() {
	seeds := flag.String("seeds", "", "comma-separated sandboxd addresses to dial at start")
	tokenFile := flag.String("token-file", "", "file holding the fleet root api_token")
	template := flag.String("template", "", "pool template to claim from")
	flag.Parse()

	if *seeds == "" || *tokenFile == "" || *template == "" {
		fmt.Fprintln(os.Stderr, "meshinventorysmoke: -seeds, -token-file and -template are required")
		os.Exit(1)
	}
	if err := run(strings.Split(*seeds, ","), *tokenFile, *template); err != nil {
		fmt.Fprintln(os.Stderr, "meshinventorysmoke:", err)
		os.Exit(1)
	}
	fmt.Println("MESHINVENTORYSMOKE PASS")
}

func run(seeds []string, tokenFile, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	raw, err := os.ReadFile(tokenFile) //nolint:gosec // an operator-supplied path
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(raw))

	t0 := time.Now()
	src, err := meshinventory.New(ctx, func(addr string) meshinventory.NodeReader {
		return sandboxd.New(scale.SandboxdBaseURL(addr), token)
	}, meshinventory.Options{Seeds: seeds})
	if err != nil {
		return err
	}
	nodes, err := src.ListNodes(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("NODES %s first-tick=%s\n", strings.Join(nodes, " "), time.Since(t0).Round(time.Millisecond))
	for _, node := range nodes {
		addr, pools, capErr := src.NodeCapacity(ctx, node)
		if capErr != nil {
			return capErr
		}
		fmt.Printf("CAPACITY %s address=%s pools=%+v\n", node, addr, pools)
	}

	store := scale.NewScatterGatherStore(src, scale.WithClaimRouting(token, scale.NewSandboxdClientFactory()))
	t1 := time.Now()
	a, err := store.Claim(ctx, "default", "meshinventorysmoke", scale.PoolKey{Template: template}, 120)
	if err != nil {
		return err
	}
	fmt.Printf("CLAIM node=%s id=%s owner=%s in=%s\n", a.Node, a.SandboxName, a.Address, time.Since(t1).Round(time.Microsecond))
	if err := store.Release(ctx, a.Node, a.SandboxName); err != nil {
		return err
	}
	fmt.Printf("RELEASE node=%s id=%s\n", a.Node, a.SandboxName)
	return nil
}
