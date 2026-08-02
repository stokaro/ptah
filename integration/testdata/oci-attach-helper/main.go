package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"testing/fstest"
	"time"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func main() {
	reference := flag.String("reference", "", "digest-pinned OCI subject reference")
	worker := flag.Int("worker", 0, "worker identifier embedded in the attachment")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Attach(
		ctx,
		*reference,
		fstest.MapFS{
			"lint.json": {Data: fmt.Appendf(nil, `{"worker":%d}`, *worker)},
		},
		ociartifact.AttachmentOptions{ArtifactType: ociartifact.LintArtifactType},
	)
	if err != nil {
		log.Fatal(err)
	}
}
