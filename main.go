package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	pinclient "github.com/ipfs/go-pinning-service-http-client"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multicodec"

	"github.com/urfave/cli/v2" // imports as package "cli"

	"github.com/briandowns/spinner"

	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
)

var (
	servicesEndpoints = map[string]string{
		"web3.storage": "https://api.web3.storage",
		"nft.storage":  "https://nft.storage/api",
		"pinata":       "https://api.pinata.cloud/psa",
		"estuary":      "https://api.estuary.tech/pinning",
	}

	serviceFlag = &cli.StringFlag{
		Name: "service", Usage: "Pinning service to use, e.g. web3.storage, nft.storage, pinata, estuary, or pinning service url, e.g. https://api.pinata.cloud/psa", Required: true,
	}

	tokenFlag = &cli.StringFlag{
		Name: "token", Usage: "Bearer token for the pinning service sent in the HTTP Authorization header", Required: true, EnvVars: []string{"PIN_SVC_TOKEN"},
	}

	passOrigins = &cli.BoolFlag{
		Name: "pass-origins", Usage: "Enable NAT port mapping with UPnP and NAT hole punching and passes the public address in the pin request's origins. Use when behind NAT with pinning services that don't return delegates",
	}

	nameFlag = &cli.StringFlag{
		Name: "name", Usage: "Optional name for pinned data; can be used for lookups later", Required: false,
	}
)

func main() {
	// Spinner to visualise ongoing operation
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	s.Color("magenta")

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
					&cli.StringFlag{
						Name: "status", Usage: "filter based on pin status (if empty returns all), e.g. pinned, failed, pinning, queued", Required: false,
					},
				},
				Action: func(c *cli.Context) error {
					endpoint, err := getServiceEndpoint(c.String(serviceFlag.Name))
					if err != nil {
						return err
					}

					pinClient := pinclient.NewClient(endpoint, c.String(tokenFlag.Name)) // instantiate client with token

					s.Start()
					pins, err := listPins(c.Context, *pinClient, pinclient.Status(c.String("status")))
					s.Stop()
					if err != nil {
						return err
					}

					fmt.Println("CID | Pin Request ID | Created | Status")
					for _, pin := range pins {
						fmt.Printf("%s %s (%s) %s\n", pin.GetPin().GetCid().String(), pin.GetRequestId(), pin.GetCreated().Format(time.RFC822), pin.GetStatus())
					}

					if err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:    "remove",
				Aliases: []string{"rm"},
				Usage: `remove a pin request
				auspinner remove --service [SERVICE] --token [TOKEN] [PIN_REQUEST_ID]
				`,
				Flags: []cli.Flag{
					serviceFlag,
					tokenFlag,
				},
				Action: func(c *cli.Context) error {
					endpoint, err := getServiceEndpoint(c.String(serviceFlag.Name))
					if err != nil {
						return err
					}

					var pinID string
					if pinID = c.Args().First(); pinID == "" {
						return fmt.Errorf("pin request ID is required")
					}

					pinClient := pinclient.NewClient(endpoint, c.String(tokenFlag.Name)) // instantiate client with token

					s.Start()
					err = pinClient.DeleteByID(c.Context, pinID)
					s.Stop()
					if err != nil {
						return err
					}

					fmt.Printf("Deleted Pin Request ID: %s\n", pinID)

					return nil
				},
			},
			{
				Name: "pin",
				Usage: `pin a car file to a pinning service by pinning the root CID and serving the CIDs over Bitswap to the delegate returned from the pinning service

				auspinner pin --service web3.storage --token [TOKEN] file.car
				`,
				Flags: []cli.Flag{
					serviceFlag,
					tokenFlag,
					nameFlag,
					passOrigins,
				},
				Action: func(c *cli.Context) error {
					endpoint, err := getServiceEndpoint(c.String(serviceFlag.Name))

					if err != nil {
						return err
					}

					var carFilePath string
					if carFilePath = c.Args().First(); carFilePath == "" {
						return fmt.Errorf(".car file is required")
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

					roots, err := r.Roots()
					if err != nil {
						return err
					}

					if len(roots) > 1 {
						return fmt.Errorf(".car files with only one root CID are supported")
					}

					pinClient := pinclient.NewClient(endpoint, c.String(tokenFlag.Name)) // instantiate client with token

					config := []libp2p.Option{}

					if c.Bool(passOrigins.Name) {
						// To pass the origins we typically need to port map assuming we're behind NAT
						// enable port mapping so that our host can be connected by passing our address as origins
						config = append(config, libp2p.NATPortMap(), libp2p.EnableHolePunching())
					}

					// Create libp2p host
					host, err := libp2p.New(config...)
					if err != nil {
						return err
					}

					bsopts := []bitswap.Option{
						bitswap.EngineBlockstoreWorkerCount(600),
						bitswap.TaskWorkerCount(600),
						bitswap.MaxOutstandingBytesPerPeer(int(5 << 20)),
					}

					robs, err := getCarBlockstore(r)
					if err != nil {
						return err
					}

					// Create a Bitswap server.
					bswap := bitswap.New(c.Context, // Make a new Bitswap server (actually it's both a client and a server, but for now you only care about the server aspect)
						bsnet.NewFromIpfsHost( // There's some abstraction layer here and bad naming, but basically it's asking for pieces it need
							host,                   // libp2p host used for communicating with others
							&routinghelpers.Null{}, // a routing system for finding content for the client (also it does this wacky thing where it advertises new blocks it learns about ...)
						),
						robs,      // this is the blockstore from the car file we're serving over bitswap
						bsopts..., // some configuration options and tuning
					)
					_ = bswap

					var pinRequest pinclient.PinStatusGetter

					if c.Bool(passOrigins.Name) {

						subs, err := host.EventBus().Subscribe(new(event.EvtLocalAddressesUpdated))
						if err != nil {
							return err
						}
						fmt.Println("Waiting to get public multiaddress from UPnP port mapping")
					addrLoop:
						for {
							select {

							// Wait for the public IP after mapping the port
							case <-subs.Out():

								origins, err := getPublicAddr(host)
								if err != nil {
									return err
								}

								if len(origins) > 0 {
									pinRequest, err = addPin(c.Context, *pinClient, roots[0], c.String(nameFlag.Name), origins)
									if err != nil {
										return err
									}
									break addrLoop
								}

							case <-time.After(1 * time.Minute):
								return fmt.Errorf("couldn't make auspinner publicly reachable for passing in origins. Try enabling UPnP in your router")
							}
						}

					} else {
						origins, err := getPublicAddr(host)
						if err != nil {
							return err
						}
						pinRequest, err = addPin(c.Context, *pinClient, roots[0], c.String(nameFlag.Name), origins)

						if err != nil {
							return err
						}

						// If there are no delegates, we need to wait for the port mapping to happen and update the pin request with the origins
						// Otherwise there's no way for the pinning service to fetch the CID if we're the only provider on the network
						if len(pinRequest.GetDelegates()) == 0 {
							return fmt.Errorf("no delegates were returned. Try again with the --pass-origins flag")
						}
					}

					// Connect to the delegates returned from the pinning service
					for _, d := range pinRequest.GetDelegates() {
						p, err := peer.AddrInfoFromP2pAddr(d)
						if err != nil {
							return err
						}

						fmt.Printf("Connecting to delegate: (%s)\n", p.String())

						if err := host.Connect(c.Context, *p); err != nil {
							log.Fatalf("error connecting to remote pin delegate %v : %v", d, err)
						}
					}

					// Track status of pin requests
					for range time.Tick(5 * time.Second) {
						s.Start()
						current, err := pinClient.GetStatusByID(c.Context, pinRequest.GetRequestId())
						if err != nil {
							fmt.Println("failed getting pin request status")
							continue
						}

						if pinRequest.GetStatus() != current.GetStatus() {
							s.Stop()

							fmt.Printf("Pin requestId: %s status change: (%s) -> (%s) | (%s)\n", current.GetRequestId(), pinRequest.GetStatus(), current.GetStatus(), time.Now().Format(time.RFC822))
							pinRequest = current
						}

						if current.GetStatus() == "pinned" {
							s.Stop()
							fmt.Printf("Pin requestId: %s successfully pinned CID: %s! 🎉\n", current.GetRequestId(), current.GetPin().GetCid())
							break
						}

						if current.GetStatus() == "failed" {
							fmt.Printf("Pin requestId: %s failed to pin CID: 😭\n", current.GetRequestId())
							s.Stop()
							break
						}

					}
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

func addPin(ctx context.Context, client pinclient.Client, cid cid.Cid, name string, origins []multiaddr.Multiaddr) (pinclient.PinStatusGetter, error) {
	opts := []pinclient.AddOption{
		pinclient.PinOpts.WithName(name), // Pass the name
	}

	if origins != nil {
		opts = append(opts, pinclient.PinOpts.WithOrigins(origins...)) // Pass our address so that the pinning service can fetch the
	}

	pinRequest, err := client.Add(ctx, cid, opts...)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Created pin request: %s for root CID %s | status: %s | origins: %v | %s\n", pinRequest.GetRequestId(), cid, pinRequest.GetStatus(), origins, time.Now().Format(time.RFC822))

	return pinRequest, nil
}

func updatePinRequestOrigins(ctx context.Context, client pinclient.Client, pin pinclient.PinStatusGetter, origins []multiaddr.Multiaddr) (pinclient.PinStatusGetter, error) {
	opts := []pinclient.AddOption{
		pinclient.PinOpts.WithName(pin.GetPin().GetName()), // Pass the name
		pinclient.PinOpts.WithOrigins(origins...),          // Pass our address so that the pinning service can fetch the
	}
	updatedPinRequest, err := client.Replace(ctx, pin.GetRequestId(), pin.GetPin().GetCid(), opts...)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Updated pin request: %s %s | status: %s\n", updatedPinRequest.GetRequestId(), time.Now().Format(time.RFC822), updatedPinRequest.GetStatus())
	fmt.Printf("Original pin request: %s | New pin request: %s\n", pin.GetRequestId(), updatedPinRequest.GetRequestId())

	return updatedPinRequest, nil
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

// `svc` can be either a valid key service from servicesEndpoints or a url
func getServiceEndpoint(service string) (string, error) {
	if endpoint, ok := servicesEndpoints[service]; ok {
		return endpoint, nil
	}

	endpoint, err := normalizeEndpoint(service)
	if err != nil {
		return "", err
	}

	return endpoint, nil
}

func listPins(ctx context.Context, c pinclient.Client, status pinclient.Status) ([]pinclient.PinStatusGetter, error) {
	var opts pinclient.LsOption

	if status == "" {
		// If status is empty, list all statuses
		opts = pinclient.PinOpts.FilterStatus(pinclient.StatusPinned, pinclient.StatusPinning, pinclient.StatusFailed, pinclient.StatusQueued)
	} else {
		s := pinclient.Status(status)
		if s.String() == string(pinclient.StatusUnknown) {
			return nil, fmt.Errorf("status %s is not valid", status)
		}
		opts = pinclient.PinOpts.FilterStatus(status)
	}
	return c.LsSync(ctx, opts)
}

func normalizeEndpoint(endpoint string) (string, error) {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil || !(uri.Scheme == "http" || uri.Scheme == "https") {
		return "", fmt.Errorf("service endpoint must be a valid HTTP URL")
	}

	// cleanup trailing and duplicate slashes (https://github.com/ipfs/go-ipfs/issues/7826)
	uri.Path = path.Clean(uri.Path)
	uri.Path = strings.TrimSuffix(uri.Path, ".")
	uri.Path = strings.TrimSuffix(uri.Path, "/")

	// remove any query params
	if uri.RawQuery != "" {
		return "", fmt.Errorf("service endpoint should be provided without any query parameters")
	}

	if strings.HasSuffix(uri.Path, "/pins") {
		return "", fmt.Errorf("service endpoint should be provided without the /pins suffix")
	}

	return uri.String(), nil
}

// Get public multi addresses of a host
func getPublicAddr(host host.Host) ([]multiaddr.Multiaddr, error) {
	addr := peer.AddrInfo{
		ID:    host.ID(),
		Addrs: host.Addrs(),
	}

	// All multi addresses including private
	maddr, err := peer.AddrInfoToP2pAddrs(&addr)
	if err != nil {
		return nil, err
	}

	var origins []multiaddr.Multiaddr
	for _, m := range maddr {
		// Check if I have public addresse
		if manet.IsPublicAddr(m) {
			origins = append(origins, m)
		}
	}

	return origins, nil
}
