package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	pinclient "github.com/ipfs/go-pinning-service-http-client"
	"github.com/ipld/go-car/v2"
	"github.com/urfave/cli/v2" // imports as package "cli"
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
	var carFilePath string

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

					pinnedCids, err := listPins(c.Context, *pinClient)
					if err != nil {
						return err
					}
					fmt.Printf("pinned CIDs: %v \n", pinnedCids)

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

					roots, err := r.Roots()
					fmt.Printf("pinning CID: %v to service: %v\n", roots, c.String(serviceFlag.Name))
					if err != nil {
						return err
					}

					pinClient := pinclient.NewClient(servicesEndpoints[c.String(serviceFlag.Name)], c.String(tokenFlag.Name)) // instantiate client with token

					for _, cid := range roots {
						fmt.Printf("pinning root CID: %v", roots)
						pinStatus, err := pinClient.Add(c.Context, cid)
						if err != nil {
							return err
						}
						fmt.Printf("pin status: %v", pinStatus)

					}
					// endpoint, err := normalizeEndpoint(servicesEndpoints[svc])
					// if err != nil {
					// 	return err
					// }

					// fmt.Printf("pinning CID to service: %v", svc)
					// <-c.Done()
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

	for ps := range psCh {
		// pinnedCids = append(pinnedCids, ps.GetRequestId())
		pinnedCids = append(pinnedCids, ps.GetPin().GetCid().String())
	}

	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("error while listing remote pins: %v", err)
	}
	return pinnedCids, nil
}

// TODO: only needed if the cli can take user provided endpoints, which it should for interoprability...
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
