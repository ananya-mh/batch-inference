// This file checks API keys and records which tenant made each request.
package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/ananya-mh/batch-inference/internal/tenants"
)

// tenantIDKey is the context key under which requireTenant stores the
// caller's tenant ID. A private type means no other package can clash with it.
type tenantIDKey struct{}

// requireTenant wraps a handler so it only runs for requests with a valid
// "Authorization: Bearer <api key>" header, the same header the OpenAI SDKs
// send. The handler can then call tenantIDFrom to get the caller's tenant.
func (s *Server) requireTenant(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey, hasBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !hasBearer || apiKey == "" {
			writeError(w, http.StatusUnauthorized, "authentication_error", "missing API key: send an Authorization: Bearer <key> header")
			return
		}

		tenantID, found, err := tenants.FindByAPIKey(r.Context(), s.pool, apiKey)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		if !found {
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid API key")
			return
		}

		ctx := context.WithValue(r.Context(), tenantIDKey{}, tenantID)
		next(w, r.WithContext(ctx))
	}
}

// tenantIDFrom returns the tenant ID that requireTenant stored in ctx.
func tenantIDFrom(ctx context.Context) string {
	tenantID, ok := ctx.Value(tenantIDKey{}).(string)
	if !ok {
		// A route was registered without requireTenant. Failing loudly is
		// safer than carrying on with an empty tenant ID.
		panic("tenantIDFrom called on a route not wrapped in requireTenant")
	}
	return tenantID
}
