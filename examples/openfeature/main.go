package main

import (
	"context"
	"fmt"
	"log"
	"os"

	shipd "github.com/alasti-company/shipd-go"
	shipdof "github.com/alasti-company/shipd-go/openfeature"
	"github.com/open-feature/go-sdk/openfeature"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	provider := shipdof.NewProvider(shipd.Options{SDKKey: os.Getenv("SHIPD_SDK_KEY")})
	defer openfeature.Shutdown()
	if err := openfeature.SetProviderAndWait(provider); err != nil {
		return fmt.Errorf("initialize Shipd: %w", err)
	}

	client := openfeature.NewClient("checkout")
	ctx := context.Background()
	user := openfeature.NewEvaluationContext("usr_8f2k19", map[string]any{"plan": "pro"})

	checkout, err := client.BooleanValueDetails(ctx, "new-checkout", false, user)
	if err != nil {
		log.Printf("new-checkout: %v", err)
	}
	log.Printf("checkout=%t reason=%s metadata=%v", checkout.Value, checkout.Reason, checkout.FlagMetadata)
	log.Printf("search=%s", client.String(ctx, "search-variant", "control", user))
	log.Printf("max-retries=%d", client.Int(ctx, "max-retries", 3, user))
	log.Printf("checkout-payload=%v", client.Object(ctx, "checkout-payload", map[string]any{"mode": "strict"}, user))
	return nil
}
