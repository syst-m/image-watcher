// image-watcher polls GHCR for new image tags and triggers k8s rollouts.
//
// It runs as a Deployment inside the cluster, authenticating to GHCR via a
// GitHub PAT stored in a Kubernetes Secret. Every ~5 minutes it compares the
// latest available tag against what's currently deployed and runs `kubectl set
// image` when it finds a newer tag.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ServiceWatch describes a service whose image tag we want to monitor.
type ServiceWatch struct {
	Name      string `json:"name"`       // k8s deployment short name (without -deployment suffix)
	Namespace string `json:"namespace"`  // k8s namespace
	AppLabel  string `json:"app_label"`  // selector label for the deployment (e.g. "app=apikey-mgr")
	Container string `json:"container"`  // container name inside the pod spec
	Repo      string `json:"repo"`       // e.g. "syst-m/apikey-mgr"
	Owner     string `json:"owner"`      // e.g. "syst-m" (for GHCR path)
}

// WatcherConfig holds the watcher's configuration.
type WatcherConfig struct {
	GHCRToken    string
	PollInterval time.Duration
	Services     []ServiceWatch
}

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		panic("GITHUB_TOKEN not set")
	}

	intervalStr := os.Getenv("POLL_INTERVAL")
	pollInterval := 5 * time.Minute
	if intervalStr != "" {
		d, err := time.ParseDuration(intervalStr)
		if err != nil {
			panic(fmt.Sprintf("invalid POLL_INTERVAL %q: %v", intervalStr, err))
		}
		pollInterval = d
	}

	cfg := WatcherConfig{
		GHCRToken:    token,
		PollInterval: pollInterval,
		Services: []ServiceWatch{
			{
				Name:      "apikey-mgr",
				Namespace: "ai-api-gateway",
				AppLabel:  "app=apikey-mgr",
				Container: "apikey-mgr",
				Repo:      "syst-m/apikey-mgr",
				Owner:     "syst-m",
			},
			{
				Name:      "rev-proxy",
				Namespace: "ai-api-gateway",
				AppLabel:  "app=rev-proxy",
				Container: "rev-proxy",
				Repo:      "syst-m/rev-proxy",
				Owner:     "syst-m",
			},
		},
	}

	fmt.Printf("image-watcher: polling every %v for %d services\n", cfg.PollInterval, len(cfg.Services))

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// Run immediately on startup so we don't wait a full interval.
	checkAll(cfg)

	for range ticker.C {
		checkAll(cfg)
	}
}

// checkAll iterates over each watched service, compares its deployed tag against GHCR,
// and triggers a rollout if a newer image is found.
func checkAll(cfg WatcherConfig) {
	client := NewGHCRRegistryClient(cfg.GHCRToken)

	for _, svc := range cfg.Services {
		currentTag := getCurrentImageTag(svc)
		if currentTag == "" {
			fmt.Printf("[WARN] %s: could not read current deployment image\n", svc.Name)
			continue
		}

		tags, err := client.ListTags(svc.Owner, svc.Repo)
		if err != nil {
			fmt.Printf("[WARN] %s: failed to check GHCR: %v\n", svc.Name, err)
			continue
		}

		latestTag := pickNewestTag(tags)
		if latestTag == "" || latestTag == currentTag {
			fmt.Printf("[OK]   %s: up-to-date (%s)\n", svc.Name, currentTag)
			continue
		}

		if isNewer(currentTag, latestTag) {
			fmt.Printf("[NEW]  %s: %s -> %s\n", svc.Name, currentTag, latestTag)
			triggerRolloutWithRetry(svc, latestTag, 3)
		} else {
			fmt.Printf("[OK]   %s: up-to-date (%s)\n", svc.Name, currentTag)
		}
	}
}

// getCurrentImageTag reads the running deployment's image tag via kubectl.
func getCurrentImageTag(svc ServiceWatch) string {
	cmd := exec.Command("kubectl", "get", "deployment", svc.Name+"-deployment",
		"-n", svc.Namespace,
		"-o", fmt.Sprintf("jsonpath={.spec.template.spec.containers[?(@.name==\"%s\")].image}", svc.Container))
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	img := strings.TrimSpace(string(out))
	if img == "" {
		return ""
	}
	// Extract tag: ghcr.io/syst-m/apikey-mgr:sha-abc1234 -> sha-abc1234
	parts := strings.Split(img, ":")
	if len(parts) < 2 {
		return img
	}
	return parts[len(parts)-1]
}

// GHCRRegistryClient talks to the Docker Registry V2 API on ghcr.io.
// GHCR requires the Docker Registry V2 auth flow: first request gets a 401
// with WWW-Authenticate header containing a token exchange URL, then we
// exchange our PAT for an OAuth bearer token and retry.
type GHCRRegistryClient struct {
	Token   string
	BaseURL string // e.g. "https://ghcr.io/v2"
	HTTP    *http.Client
}

// ghcrTokenResponse is the response from the GitHub token exchange endpoint.
type ghcrTokenResponse struct {
	Token string `json:"token"`
}

// NewGHCRRegistryClient returns a client configured for ghcr.io.
func NewGHCRRegistryClient(token string) *GHCRRegistryClient {
	return &GHCRRegistryClient{
		Token:   token,
		BaseURL: "https://ghcr.io/v2",
		HTTP:    &http.Client{Timeout: 15 * time.Second},
	}
}

// exchangeToken contacts the GitHub token exchange endpoint (realm from the
// 401 challenge) and returns a short-lived OAuth token suitable for ghcr.io.
func (c *GHCRRegistryClient) exchangeToken(realm, service, scope string) (string, error) {
	exchangeURL := fmt.Sprintf("%s?scope=%s&service=%s", realm, scope, service)

	req, err := http.NewRequest("GET", exchangeURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("token exchange request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token exchange returned status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp ghcrTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}
	return tokenResp.Token, nil
}

// ListTags returns all tag names for a container image repo.
// Uses the Docker Registry V2 API with GHCR's challenge-response auth flow.
func (c *GHCRRegistryClient) ListTags(owner, repo string) ([]string, error) {
	scope := fmt.Sprintf("%s/%s", owner, repo)
	url := fmt.Sprintf("%s/%s/tags/list?n=100", c.BaseURL, scope)

	// Step 1: Make the request without auth to get the challenge.
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request to %s: %w", url, err)
	}

	// If we already have a cached token, use it directly.
	if resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		var result struct {
			Name string   `json:"name"`
			Tags []string `json:"tags"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode tags response: %w", err)
		}
		return result.Tags, nil
	}
	defer resp.Body.Close()

	// Step 2: Parse the WWW-Authenticate header for token exchange details.
	wwwAuth := resp.Header.Get("WWW-Authenticate")
	realm, service, scopeParam := parseGHCRChallenge(wwwAuth)
	if realm == "" {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GHCR returned %d without WWW-Authenticate: %s", resp.StatusCode, string(body))
	}

	// Step 3: Exchange our PAT for a GHCR OAuth token.
	oauthToken, err := c.exchangeToken(realm, service, scopeParam)
	if err != nil {
		return nil, fmt.Errorf("token exchange failed: %w", err)
	}

	// Step 4: Retry the original request with the OAuth token.
	req2, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req2.Header.Set("Authorization", "Bearer "+oauthToken)

	resp2, err := c.HTTP.Do(req2)
	if err != nil {
		return nil, fmt.Errorf("HTTP request to %s: %w", url, err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		return nil, fmt.Errorf("GHCR tags API returned status %d: %s", resp2.StatusCode, string(body))
	}

	var result struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode tags response: %w", err)
	}
	return result.Tags, nil
}

// parseGHCRChallenge extracts realm, service, and scope from a
// WWW-Authenticate: Bearer realm="...",service="...",scope="..." header.
func parseGHCRChallenge(header string) (realm, service, scope string) {
	// Simple parser: extract quoted values after key=
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, `realm=`) {
			realm = strings.Trim(strings.TrimPrefix(part, `realm=`), `"`)
		} else if strings.HasPrefix(part, `service=`) {
			service = strings.Trim(strings.TrimPrefix(part, `service=`), `"`)
		} else if strings.HasPrefix(part, `scope=`) {
			scope = strings.Trim(strings.TrimPrefix(part, `scope=`), `"`)
		}
	}
	return
}

// pickNewestTag selects the most recent tag from a list, preferring semver tags (vX.Y.Z),
// then short-sha tags (sha-<7chars>), then bare numeric tags.
func pickNewestTag(tags []string) string {
	if len(tags) == 0 {
		return ""
	}

	var semverTags, shaTags, numTags, otherTags []string
	for _, t := range tags {
		if isSemverLike(t) {
			semverTags = append(semverTags, t)
		} else if strings.HasPrefix(t, "sha-") && len(t) == 10 { // sha- + 7 hex chars
			shaTags = append(shaTags, t)
		} else if _, err := strconv.Atoi(t); err == nil {
			numTags = append(numTags, t)
		} else {
			otherTags = append(otherTags, t)
		}
	}

	if len(semverTags) > 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(semverTags)))
		return semverTags[0]
	}
	if len(shaTags) > 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(shaTags)))
		return shaTags[0]
	}
	if len(numTags) > 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(numTags)))
		return numTags[0]
	}
	if len(otherTags) > 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(otherTags)))
		return otherTags[0]
	}
	return ""
}

// isSemverLike checks if a tag looks like a semantic version (starts with "v" followed by a digit).
func isSemverLike(t string) bool {
	if len(t) < 2 || t[0] != 'v' {
		return false
	}
	return t[1] >= '0' && t[1] <= '9'
}

// isNewer returns true if latest > current.
// Short-sha tags (sha-<7chars>) are always considered newer than non-sha tags.
func isNewer(current, latest string) bool {
	latestSha := strings.HasPrefix(latest, "sha-")
	currentSha := strings.HasPrefix(current, "sha-")

	if latestSha && !currentSha {
		return true
	}
	if latestSha && currentSha {
		return latest != current
	}

	// Use semver comparison for vX.Y.Z tags.
	if isSemverLike(current) && isSemverLike(latest) {
		return semverCompare(latest, current) > 0
	}

	// Fallback: string inequality (we already checked equality above).
	return latest != current
}

// semverCompare mimics golang.org/x/mod/semver.Compare using pure Go.
// Returns -1 if v < w, 0 if v == w, +1 if v > w.
func semverCompare(v, w string) int {
	// Strip the leading "v" prefix that semver expects.
	v = strings.TrimPrefix(v, "v")
	w = strings.TrimPrefix(w, "v")

	vParts := strings.Split(v, ".")
	wParts := strings.Split(w, ".")

	maxLen := len(vParts)
	if len(wParts) > maxLen {
		maxLen = len(wParts)
	}

	for i := 0; i < maxLen; i++ {
		var vi, wi int
		if i < len(vParts) {
			vi, _ = strconv.Atoi(vParts[i])
		}
		if i < len(wParts) {
			wi, _ = strconv.Atoi(wParts[i])
		}
		if vi < wi {
			return -1
		}
		if vi > wi {
			return 1
		}
	}
	return 0
}

// triggerRolloutWithRetry runs kubectl set image with exponential backoff.
// Attempts: 3 retries with delays of 5s, 15s, 45s.
func triggerRolloutWithRetry(svc ServiceWatch, newTag string, maxRetries int) {
	imageRef := fmt.Sprintf("ghcr.io/%s:%s", svc.Repo, newTag)
	delays := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second}

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			fmt.Printf("[RETRY] %s: attempt %d/%d, waiting %v...\n", svc.Name, attempt+1, maxRetries, delays[attempt-1])
			time.Sleep(delays[attempt-1])
		}

		cmd := exec.Command("kubectl", "set", "image", "deployment/"+svc.Name+"-deployment",
			fmt.Sprintf("%s=%s", svc.Container, imageRef),
			"-n", svc.Namespace)

		out, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Printf("[ERROR] %s rollout attempt %d failed: %v\n%s\n", svc.Name, attempt+1, err, string(out))
			continue
		}
		fmt.Printf("[DEPLOY] %s -> %s\n%s\n", svc.Name, imageRef, string(out))
		return // success
	}

	fmt.Printf("[FATAL] %s: all %d rollout attempts failed, giving up\n", svc.Name, maxRetries)
}
