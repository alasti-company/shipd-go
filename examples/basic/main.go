package main

import (
	"context"
	"log"
	"os"
	"time"

	shipd "github.com/alasti-company/shipd-go"
	"github.com/alasti-company/shipd-go/examples/basic/flags"
)

func main() {
	client, err := shipd.New(shipd.Options{
		SDKKey:   os.Getenv("SHIPD_SDK_KEY"),
		Defaults: flags.Defaults(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		// Shutdown delivers pending events, then closes the client.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := client.Shutdown(ctx); err != nil {
			log.Printf("shipd events: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := client.Ready(ctx); err != nil {
		log.Printf("shipd startup: %v; continuing with defaults", err)
	}
	cancel()
	user := shipd.EvaluationContext{UserID: "usr_8f2k19", Attributes: map[string]any{"plan": "pro"}}
	log.Printf("checkout=%v search=%s retries=%v", flags.NewCheckout(client, user), flags.SearchVariant(client, user), flags.MaxRetries(client, user))
}
