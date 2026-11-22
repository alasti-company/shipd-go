package openfeature

import (
	"context"
	"fmt"
	"math"

	shipd "github.com/alasti-company/shipd-go"
	"github.com/alasti-company/shipd-go/internal/traits"
	of "github.com/open-feature/go-sdk/openfeature"
)

func (p *Provider) BooleanEvaluation(_ context.Context, key string, fallback bool, ctx of.FlattenedContext) of.BoolResolutionDetail {
	value, details := resolve(p, key, "boolean", fallback, ctx)
	return of.BoolResolutionDetail{Value: value, ProviderResolutionDetail: details}
}

func (p *Provider) StringEvaluation(_ context.Context, key string, fallback string, ctx of.FlattenedContext) of.StringResolutionDetail {
	value, details := resolve(p, key, "variant", fallback, ctx)
	return of.StringResolutionDetail{Value: value, ProviderResolutionDetail: details}
}

func (p *Provider) FloatEvaluation(_ context.Context, key string, fallback float64, ctx of.FlattenedContext) of.FloatResolutionDetail {
	value, details := resolve(p, key, "number", fallback, ctx)
	return of.FloatResolutionDetail{Value: value, ProviderResolutionDetail: details}
}

func (p *Provider) IntEvaluation(_ context.Context, key string, fallback int64, ctx of.FlattenedContext) of.IntResolutionDetail {
	value, details := resolve(p, key, "number", float64(0), ctx)
	if details.Reason == of.ErrorReason {
		return of.IntResolutionDetail{Value: fallback, ProviderResolutionDetail: details}
	}
	// The upper bound is exclusive because float64(math.MaxInt64) rounds up.
	if math.Trunc(value) != value || value < -0x1p63 || value >= 0x1p63 {
		details = failure(of.NewTypeMismatchResolutionError(fmt.Sprintf("Flag %q is not representable as an int64", key)))
		return of.IntResolutionDetail{Value: fallback, ProviderResolutionDetail: details}
	}
	return of.IntResolutionDetail{Value: int64(value), ProviderResolutionDetail: details}
}

func (p *Provider) ObjectEvaluation(_ context.Context, key string, fallback any, ctx of.FlattenedContext) of.InterfaceResolutionDetail {
	value, details := resolve(p, key, "json", fallback, ctx)
	return of.InterfaceResolutionDetail{Value: value, ProviderResolutionDetail: details}
}

func resolve[T any](p *Provider, key, kind string, fallback T, flatCtx of.FlattenedContext) (T, of.ProviderResolutionDetail) {
	p.mu.RLock()
	client, ready := p.client, p.ready && !p.closed
	p.mu.RUnlock()
	if !ready {
		return fallback, failure(of.NewProviderNotReadyResolutionError("Shipd provider is not ready"))
	}

	ctx, err := mapContext(flatCtx)
	if err != nil {
		return fallback, failure(of.NewInvalidContextResolutionError(err.Error()))
	}
	result := client.EvaluateDetails(key, ctx)
	if result.Reason == "not_found" {
		return fallback, failure(of.NewFlagNotFoundResolutionError(fmt.Sprintf("Flag %q was not found", key)))
	}
	if result.FlagType != kind {
		return fallback, failure(of.NewTypeMismatchResolutionError(fmt.Sprintf("Flag %q has type %q, expected %q", key, result.FlagType, kind)))
	}
	value, ok := result.Value.(T)
	// A nil interface is a valid JSON null. The Go SDK validates snapshot JSON.
	if !ok && !(kind == "json" && result.Value == nil) {
		return fallback, failure(of.NewTypeMismatchResolutionError(fmt.Sprintf("Flag %q returned an incompatible value", key)))
	}

	details := of.ProviderResolutionDetail{
		Reason:       of.DefaultReason,
		FlagMetadata: of.FlagMetadata{"snapshotVersion": *result.SnapshotVersion},
	}
	switch result.Reason {
	case "rule_match":
		details.Reason = of.TargetingMatchReason
	case "flag_disabled":
		details.Reason = of.DisabledReason
	}
	if result.RuleID != "" {
		details.FlagMetadata["ruleId"] = result.RuleID
	}
	return value, details
}

func failure(err of.ResolutionError) of.ProviderResolutionDetail {
	return of.ProviderResolutionDetail{Reason: of.ErrorReason, ResolutionError: err}
}

func mapContext(flat of.FlattenedContext) (shipd.EvaluationContext, error) {
	ctx := shipd.EvaluationContext{Attributes: make(map[string]any, len(flat))}
	for key, value := range flat {
		switch key {
		case of.TargetingKey:
			id, ok := value.(string)
			if !ok {
				return ctx, fmt.Errorf("targetingKey must be a string")
			}
			ctx.UserID, ctx.UserIDPresent = id, true
		case "userId":
			return ctx, fmt.Errorf("use targetingKey for the user identity")
		default:
			attribute, ok := traits.NormalizeScalar(value)
			if !ok {
				return ctx, fmt.Errorf("context attribute %q must be a finite scalar or null", key)
			}
			ctx.Attributes[key] = attribute
		}
	}
	return ctx, nil
}
