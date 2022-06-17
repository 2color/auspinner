package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pinclient "github.com/ipfs/go-pinning-service-http-client"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"

	"github.com/urfave/cli/v2" // imports as package "cli"

	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
)

// TODO: Find a cleaner way to handle default endpoints without duplicating
var validServices = []string{"web3.storage", "pinata", "estuary"}
var servicesEndpoints = map[string]string{
	"web3.storage": "https://api.web3.storage",
	"pinata":       "https://api.pinata.cloud/psa",
	"estuary":      "https://api.estuary.tech/pinning",
}

var serviceFlag = &cli.StringFlag{
	Name: "service", Usage: "Pinning service to use, e.g. web3.storage, pinata, estuary", Required: true,
}

var tokenFlag = &cli.StringFlag{
	Name: "token", Usage: "Bearer token for the pinning service sent in the HTTP Authorization header", Required: true,
}

func main() {
	var name string

	// TODO: move pinning to sub command
	app := &cli.App{
		Name:  "auspinner",
		Usage: `stateless CLI tool to pin CAR files to IPFS pinning services`,
		Commands: []*cli.Command{
			{
				Name:    "list",
				Aliases: []string{"ls"},
				Usage:   "list pins",
				Flags: []cli.Flag{
					serviceFlag,
					tokenFlag,
				},
				Action: func(c *cli.Context) error {
					if !isServiceValid(c.String(serviceFlag.Name)) {
						log.Fatal("services should be one of: ", strings.Join(validServices, ", "))
					}

					pinClient := pinclient.NewClient(servicesEndpoints[c.String(serviceFlag.Name)], c.String(tokenFlag.Name)) // instantiate client with token

					_, err := listPins(c.Context, *pinClient)
					if err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:  "pin",
				Usage: `pin a car file to a pinning service by pinning the root CID and serving the CIDs over Bitswap to the delegate returned from the pinning service`,
				Flags: []cli.Flag{
					serviceFlag,
					tokenFlag,
					&cli.StringFlag{
						Name: "name", Usage: "Optional name for pinned data; can be used for lookups later", Required: false, Destination: &name,
					},
				},
				Action: func(c *cli.Context) error {
					if !isServiceValid(c.String(serviceFlag.Name)) {
						log.Fatal("services should be one of: ", strings.Join(validServices, ", "))
					}

					var carFilePath string
					if carFilePath = c.Args().First(); carFilePath == "" {
						log.Fatal(".car file is required")
					}
					f, err := os.Open(carFilePath)
					if err != nil {
						return err
					}
					defer f.Close()

					r, err := car.NewReader(f)
					if err != nil {
						return err
					}

					pinClient := pinclient.NewClient(servicesEndpoints[c.String(serviceFlag.Name)], c.String(tokenFlag.Name)) // instantiate client with token

					// Create libp2p host
					host, err := libp2p.New(
						libp2p.NATPortMap(),
						libp2p.EnableHolePunching(),
					)
					if err != nil {
						return err
					}

					// Create libp2p host
					var mas []multiaddr.Multiaddr
					var pinStatuses []pinclient.PinStatusGetter
					// wait 10 seconds so port mapping has time to get set up
					time.AfterFunc(time.Second*10, func() {
						addr := peer.AddrInfo{
							ID:    host.ID(),
							Addrs: host.Addrs(),
						}
						mas, err = peer.AddrInfoToP2pAddrs(&addr)

						if err != nil {
							fmt.Println(err)
							panic(err)
						}

						fmt.Println("multiaddrs:")
						for _, a := range mas {
							fmt.Println(a)
						}

						pinStatuses, err = addPins(c.Context, *pinClient, r, mas)

						for _, d := range pinStatuses[0].GetDelegates() {
							p, err := peer.AddrInfoFromP2pAddr(d)
							if err != nil {
								panic(err)
							}

							if err := host.Connect(c.Context, *p); err != nil {
								log.Fatalf("error connecting to remote pin delegate %v : %v", d, err)
							}
						}

						if err != nil {
							fmt.Println(err)
							panic(err)
						}
					})

					bsopts := []bitswap.Option{
						bitswap.EngineBlockstoreWorkerCount(600),
						bitswap.TaskWorkerCount(600),
						bitswap.MaxOutstandingBytesPerPeer(int(5 << 20)),
					}

					robs, err := getCarBlockstore(r)
					if err != nil {
						return err
					}

					// Create a Bitswap server. To connect use the
					bswap := bitswap.New(c.Context, // Make a new Bitswap server (actually it's both a client and a server, but for now you only care about the server aspect)
						bsnet.NewFromIpfsHost( // There's some abstraction layer here and bad naming, but basically it's asking for pieces it need
							host,                   // libp2p host used for communicating with others
							&routinghelpers.Null{}, // a routing system for finding content for the client (also it does this wacky thing where it advertises new blocks it learns about ... but don't worry about this)
						),
						robs,      // this is the blockstore that I'm willing to serve from
						bsopts..., // some configuration options and tuning
					)
					_ = bswap

					// TODO:
					_ = pinStatuses
					// watchPinStatus()

					<-c.Done()
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getCarBlockstore(r *car.Reader) (*blockstore.ReadOnly, error) {
	var idx index.Index
	backingReader := r.DataReader()

	if r.Version == 1 || !r.Header.HasIndex() {
		idx, err := index.New(multicodec.CarMultihashIndexSorted)
		if err != nil {
			return nil, err
		}
		// TODO: Read more about how LoadIndex works for car files without an index
		if err := car.LoadIndex(idx, r.DataReader()); err != nil {
			return nil, err
		}

		// TODO: Save newly created index somewhere
	} else {
		i, err := index.ReadFrom(r.IndexReader())
		if err != nil {
			return nil, err
		}
		if i.Codec() != multicodec.CarMultihashIndexSorted {
			return nil, fmt.Errorf("codec %s not supported for CAR files", i.Codec())
		}
		idx = i
	}

	return blockstore.NewReadOnly(backingReader, idx)
}

func addPins(ctx context.Context, client pinclient.Client, car *car.Reader, origins []multiaddr.Multiaddr) ([]pinclient.PinStatusGetter, error) {
	// TODO: Figure out how a CAR file can have multiple root CIDs
	roots, err := car.Roots()
	fmt.Printf("pinning CID: %v\n", roots)
	if err != nil {
		return nil, err
	}

	pinStatuses := []pinclient.PinStatusGetter{}
	opts := []pinclient.AddOption{}
	for _, cid := range roots {
		fmt.Printf("pinning root CID: %v", roots)
		// TODO: How do I only pass publicly reachable addresses?
		opts = append(opts, pinclient.PinOpts.WithOrigins(origins...)) // Pass the address so that the pinning service can fetch the
		pinStatus, err := client.Add(ctx, cid, opts...)
		if err != nil {
			return nil, err
		}
		pinStatuses = append(pinStatuses, pinStatus)
		fmt.Printf("status %v", pinStatus)
	}
	fmt.Printf("pinned %d CIDs", len(pinStatuses))

	return pinStatuses, nil
}

// func connectToDelegates(ctx context.Context, h host.Host, delegates []string) error {
// 	peers := make(map[peer.ID][]multiaddr.Multiaddr)
// 	for _, d := range delegates {
// 		ai, err := peer.AddrInfoFromString(d)
// 		if err != nil {
// 			return err
// 		}

// 		peers[ai.ID] = append(peers[ai.ID], ai.Addrs...)
// 	}

// 	for p, addrs := range peers {
// 		h.Peerstore().AddAddrs(p, addrs, time.Hour)

// 		if h.Network().Connectedness(p) != network.Connected {
// 			if err := h.Connect(ctx, peer.AddrInfo{
// 				ID: p,
// 			}); err != nil {
// 				return err
// 			}

// 			h.ConnManager().Protect(p, "pinning")
// 		}
// 	}

// 	return nil
// }

func isServiceValid(svc string) bool {
	for _, validSvc := range validServices {
		if validSvc == svc {
			return true
		}
	}
	return false
}

func listPins(ctx context.Context, c pinclient.Client) ([]string, error) {
	psCh, errCh := c.Ls(ctx, pinclient.PinOpts.FilterStatus(pinclient.StatusPinned, pinclient.StatusPinning, pinclient.StatusFailed, pinclient.StatusQueued))
	pinnedCids := []string{}

	fmt.Println("CID | Request ID | Created | Status")
	for ps := range psCh {
		pinnedCids = append(pinnedCids, ps.GetRequestId())
		fmt.Printf("%s %s (%s) %s\n", ps.GetPin().GetCid().String(), ps.GetRequestId(), ps.GetCreated().Format(time.RFC822), ps.GetStatus())
	}

	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("error while listing remote pins: %v", err)
	}
	return pinnedCids, nil
}

// TODO: only needed if the cli can take user provided endpoints, which it should for interoprability...
// endpoint, err := normalizeEndpoint(servicesEndpoints[svc])
// if err != nil {
// 	return err
// }
// func normalizeEndpoint(endpoint string) (string, error) {
// 	uri, err := url.ParseRequestURI(endpoint)
// 	if err != nil || !(uri.Scheme == "http" || uri.Scheme == "https") {
// 		return "", fmt.Errorf("service endpoint must be a valid HTTP URL")
// 	}

// 	// cleanup trailing and duplicate slashes (https://github.com/ipfs/go-ipfs/issues/7826)
// 	uri.Path = path.Clean(uri.Path)
// 	uri.Path = strings.TrimSuffix(uri.Path, ".")
// 	uri.Path = strings.TrimSuffix(uri.Path, "/")

// 	// remove any query params
// 	if uri.RawQuery != "" {
// 		return "", fmt.Errorf("service endpoint should be provided without any query parameters")
// 	}

// 	if strings.HasSuffix(uri.Path, "/pins") {
// 		return "", fmt.Errorf("service endpoint should be provided without the /pins suffix")
// 	}

// 	fmt.Printf("endpoint: %v \n", uri.String())
// 	return uri.String(), nil
// }
