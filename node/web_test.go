package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

// webFixture is a node with the UI wired up, served through httptest so no
// port is bound.
type webFixture struct {
	*testNode
	ui     *webUI
	server *httptest.Server
}

func newWebFixture(t *testing.T, cfg nodeConfig) *webFixture {
	t.Helper()

	n := buildTestNode(t, freeAddr(t), cfg)
	ui := &webUI{node: n.FileServer, token: "test-token", mux: http.NewServeMux(), loopbackOnly: true}
	ui.routes()

	srv := httptest.NewServer(ui)
	t.Cleanup(srv.Close)

	return &webFixture{testNode: n, ui: ui, server: srv}
}

// do issues a request with the token, unless withoutToken is set.
func (f *webFixture) do(t *testing.T, method, path string, body io.Reader, tweak func(*http.Request)) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, f.server.URL+path, body)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	req.Header.Set(csrfHeader, f.ui.token)
	if tweak != nil {
		tweak(req)
	}

	resp, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// Every API path must refuse a request with no token. There is no cookie, so
// this is the whole authorisation: without it any page the person visits could
// read their files out.
func TestAPIRefusesRequestsWithoutTheToken(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	for _, tc := range []struct {
		method, path string
	}{
		{"GET", "/api/state"},
		{"GET", "/api/events"},
		{"GET", "/api/file?name=x"},
		{"POST", "/api/upload?name=x"},
		{"POST", "/api/delete"},
		{"POST", "/api/recheck"},
		{"POST", "/api/trust"},
		{"POST", "/api/untrust"},
		{"POST", "/api/mode"},
	} {
		resp := f.do(t, tc.method, tc.path, strings.NewReader("{}"), func(r *http.Request) {
			r.Header.Del(csrfHeader)
		})
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s without a token gave %d, want 403", tc.method, tc.path, resp.StatusCode)
		}
	}
}

// A wrong token is refused as firmly as none.
func TestAPIRefusesAWrongToken(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	resp := f.do(t, "GET", "/api/state", nil, func(r *http.Request) {
		r.Header.Set(csrfHeader, "not-the-token")
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a wrong token gave %d, want 403", resp.StatusCode)
	}
}

// The DNS-rebinding kill switch. An attacker's page can make the browser
// resolve their name to 127.0.0.1, but it cannot change the Host header, so a
// Host this node does not answer on is refused.
//
// Fails against a server that does not check Host.
func TestAPIRefusesAnUnexpectedHost(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	for _, host := range []string{"evil.com", "attacker.example:7654", "192.0.2.1:7654"} {
		resp := f.do(t, "GET", "/api/state", nil, func(r *http.Request) {
			r.Host = host
		})
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("Host %q gave %d, want 403", host, resp.StatusCode)
		}
	}

	// And the loopback names it does answer on.
	for _, host := range []string{"localhost:7654", "127.0.0.1:7654", "[::1]:7654"} {
		resp := f.do(t, "GET", "/api/state", nil, func(r *http.Request) {
			r.Host = host
		})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Host %q gave %d, want 200", host, resp.StatusCode)
		}
	}
}

// The page itself must also refuse an unexpected Host, or a rebinding attack
// could read the token straight out of the markup.
//
// Fails against a server that guards only /api/.
func TestThePageRefusesAnUnexpectedHost(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	for _, path := range []string{"/", "/app.js", "/app.css"} {
		resp := f.do(t, "GET", path, nil, func(r *http.Request) {
			r.Host = "evil.com"
			r.Header.Del(csrfHeader)
		})
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s on a foreign Host gave %d, want 403", path, resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		if bytes.Contains(body, []byte(f.ui.token)) {
			t.Fatalf("%s leaked the token to a foreign Host", path)
		}
	}
}

// A mutation from another origin is refused even with a valid token, so a
// token that leaks cannot be used from a hostile page.
func TestMutationsRefuseAForeignOrigin(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	resp := f.do(t, "POST", "/api/trust", strings.NewReader(`{"Node":"x"}`), func(r *http.Request) {
		r.Header.Set("Origin", "http://evil.com")
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a cross-origin mutation gave %d, want 403", resp.StatusCode)
	}

	resp = f.do(t, "POST", "/api/trust", strings.NewReader(`{"Node":"x"}`), func(r *http.Request) {
		r.Header.Set("Sec-Fetch-Site", "cross-site")
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a cross-site mutation gave %d, want 403", resp.StatusCode)
	}
}

// A mutation must be a POST, so it cannot be triggered by a bare navigation
// or an image tag.
func TestMutationsRefuseGET(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	resp := f.do(t, "GET", "/api/trust", nil, nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET on a mutation gave %d, want 405", resp.StatusCode)
	}
}

// No cookie may ever be set: a cookie is ambient authority that another origin
// can make the browser attach, which is exactly what the header token avoids.
func TestNoCookieIsEverSet(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	for _, path := range []string{"/", "/app.js", "/api/state"} {
		resp := f.do(t, "GET", path, nil, nil)
		if got := resp.Header.Values("Set-Cookie"); len(got) != 0 {
			t.Fatalf("%s set a cookie: %v", path, got)
		}
	}
}

// The CSP must forbid inline script, which is why the token is markup and the
// script is a separate file.
func TestSecurityHeadersArePresent(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	resp := f.do(t, "GET", "/", nil, nil)
	csp := resp.Header.Get("Content-Security-Policy")
	for _, want := range []string{"default-src 'none'", "script-src 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, want) {
			t.Errorf("CSP %q is missing %q", csp, want)
		}
	}
	if strings.Contains(csp, "unsafe-inline") {
		t.Errorf("CSP allows inline script: %q", csp)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("nosniff is missing")
	}
	if resp.Header.Get("Referrer-Policy") != "no-referrer" {
		t.Error("Referrer-Policy is missing")
	}
}

// Binding off loopback is refused unless explicitly allowed, because the UI
// can administer trust and there is no machine boundary off-machine.
func TestNonLoopbackBindIsRefused(t *testing.T) {
	n := newTestNode(t)

	for _, addr := range []string{"0.0.0.0:0", ":0", "192.0.2.1:0"} {
		if err := n.ListenHTTP(addr, false, t.TempDir()); err == nil {
			n.StopHTTP()
			t.Errorf("binding %q was allowed without the override", addr)
		}
	}

	// Loopback is fine without any override.
	if err := n.ListenHTTP("127.0.0.1:0", false, t.TempDir()); err != nil {
		t.Fatalf("binding loopback was refused: %v", err)
	}
	n.StopHTTP()
}

// When it is allowed off loopback, the token is written to a file the operator
// can read, at 0600.
func TestExposedBindWritesATokenFile(t *testing.T) {
	n := newTestNode(t)
	dir := t.TempDir()

	// 0.0.0.0 rather than 127.0.0.2: the whole of 127.0.0.0/8 is loopback, so
	// binding there needs no token and the test would prove nothing.
	if err := n.ListenHTTP("0.0.0.0:0", true, dir); err != nil {
		t.Fatalf("an explicitly allowed off-loopback bind was refused: %v", err)
	}
	defer n.StopHTTP()

	path := filepath.Join(dir, httpTokenFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("no token file was written: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("the token file is %v, want 0600", perm)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the token: %v", err)
	}
	if len(strings.TrimSpace(string(body))) < 32 {
		t.Fatalf("the token is too short to be useful: %q", body)
	}
}

// The state endpoint reports the live peer view, never the database's status
// column, and the cached replication rather than a network measurement.
func TestStateReportsTheLiveViewAndCachedReplication(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	// A ghost row, as a crashed node leaves behind.
	now := time.Now()
	if err := f.DB.UpsertPeer(t.Context(), dbpkg.Peer{
		NodeID: "ghost", Address: "192.0.2.30:4000", Status: "connected", LastSeen: &now,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}
	if err := f.Store("web.txt", strings.NewReader("served over http")); err != nil {
		t.Fatalf("Store: %v", err)
	}

	resp := f.do(t, "GET", "/api/state", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("state gave %d", resp.StatusCode)
	}

	var state stateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		t.Fatalf("decoding state: %v", err)
	}

	if len(state.Peers) != 1 {
		t.Fatalf("state reported %d peers, want 1", len(state.Peers))
	}
	if state.Peers[0].Online {
		t.Fatal("a stale database row was reported online")
	}
	if len(state.Files) != 1 || state.Files[0].Name != "web.txt" {
		t.Fatalf("state reported files %+v", state.Files)
	}
	if state.Node.NodeID != f.NodeID() {
		t.Fatalf("state reported node %q, want %q", state.Node.NodeID, f.NodeID())
	}
	if state.Mode == "" {
		t.Fatal("state reported no trust mode")
	}
}

// An upload is streamed into storage rather than spooled through a multipart
// form. Half a megabyte is enough that buffering would show.
func TestUploadStreamsIntoStorage(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	payload := randomBytes(t, 512*1024)
	resp := f.do(t, "POST", "/api/upload?name=big.bin", bytes.NewReader(payload), nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload gave %d: %s", resp.StatusCode, body)
	}

	// Round-trips byte for byte.
	resp = f.do(t, "GET", "/api/file?name=big.bin", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download gave %d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the download: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("the download differs: got %d bytes, want %d", len(got), len(payload))
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, `filename="big.bin"`) {
		t.Fatalf("Content-Disposition = %q", cd)
	}
}

// A name that tries to escape must not reach the filename header as given.
func TestDownloadFilenameIsSanitised(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"notes.txt", "notes.txt"},
		{"../../etc/passwd", "passwd"},
		{`bad"name`, "badname"},
		{"..", "download"},
		{"", "download"},
	} {
		if got := safeFilename(tc.in); got != tc.want {
			t.Errorf("safeFilename(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Trust administration works through the UI, which is what makes it the most
// powerful surface the node has.
func TestTrustAdministrationOverHTTP(t *testing.T) {
	f := newWebFixture(t, nodeConfig{trustMode: dbpkg.TrustModeEnforcing})

	body := fmt.Sprintf(`{"Node":%q,"Label":"a friend"}`, strangerID)
	resp := f.do(t, "POST", "/api/trust", strings.NewReader(body), nil)
	if resp.StatusCode != http.StatusOK {
		out, _ := io.ReadAll(resp.Body)
		t.Fatalf("trust gave %d: %s", resp.StatusCode, out)
	}
	if !f.Trusts(strangerID) {
		t.Fatal("the peer was not approved")
	}

	resp = f.do(t, "POST", "/api/untrust", strings.NewReader(fmt.Sprintf(`{"Node":%q}`, strangerID)), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("untrust gave %d", resp.StatusCode)
	}
	if f.Trusts(strangerID) {
		t.Fatal("the peer is still approved")
	}

	resp = f.do(t, "POST", "/api/mode", strings.NewReader(`{"Mode":"open"}`), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mode gave %d", resp.StatusCode)
	}
	if f.TrustEnforced() {
		t.Fatal("enforcement is still on after switching to open")
	}

	// A nonsense mode is a refusal, not a server fault.
	resp = f.do(t, "POST", "/api/mode", strings.NewReader(`{"Mode":"whatever"}`), nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("an unknown mode gave %d, want 409", resp.StatusCode)
	}
}

// The event stream delivers, and heartbeats while idle so a dead connection is
// noticed rather than waited on.
func TestEventStreamDeliversAndHeartbeats(t *testing.T) {
	f := newWebFixture(t, nodeConfig{watchHeartbeat: 100 * time.Millisecond})

	resp := f.do(t, "GET", "/api/events?token="+f.ui.token, nil, func(r *http.Request) {
		r.Header.Del(csrfHeader)
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events gave %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}

	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = f.Store("streamed.txt", strings.NewReader("watch me"))
	}()

	// Read until an event arrives, so heartbeats in between are tolerated.
	reader := resp.Body
	buf := make([]byte, 4096)
	deadline := time.Now().Add(20 * time.Second)
	var seen strings.Builder
	for time.Now().Before(deadline) {
		n, err := reader.Read(buf)
		if n > 0 {
			seen.Write(buf[:n])
			if strings.Contains(seen.String(), EventFileStored) {
				break
			}
		}
		if err != nil {
			t.Fatalf("reading the stream: %v (so far: %q)", err, seen.String())
		}
	}

	out := seen.String()
	if !strings.Contains(out, EventFileStored) {
		t.Fatalf("the stream never delivered a %s: %q", EventFileStored, out)
	}
	if !strings.Contains(out, ": keepalive") {
		t.Fatalf("the stream never sent a heartbeat: %q", out)
	}
}

// The only guard on the front end: CI runs gofmt and vet over Go alone, so a
// path renamed in the mux and not in the script would otherwise fail silently
// in the browser.
func TestEveryAPIPathTheScriptUsesExists(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	script, err := assetFS.ReadFile("assets/app.js")
	if err != nil {
		t.Fatalf("reading app.js: %v", err)
	}

	paths := regexp.MustCompile(`"(/api/[a-zA-Z0-9_/-]*)`).FindAllStringSubmatch(string(script), -1)
	if len(paths) == 0 {
		t.Fatal("found no /api/ paths in app.js; the extraction is broken, not the script")
	}

	seen := make(map[string]bool)
	for _, match := range paths {
		path := match[1]
		if seen[path] {
			continue
		}
		seen[path] = true

		// Reached with a valid token, so a 403 would mean a missing route
		// rather than a refusal. 404 is the failure being looked for.
		resp := f.do(t, "POST", path, strings.NewReader("{}"), nil)
		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("app.js calls %s, which the mux does not serve", path)
		}
	}
	t.Logf("checked %d distinct API paths from app.js", len(seen))
}

// Likewise for the element ids the script reaches for: a renamed id in the
// HTML is a silent break.
func TestEveryElementTheScriptNeedsExistsInThePage(t *testing.T) {
	script, err := assetFS.ReadFile("assets/app.js")
	if err != nil {
		t.Fatalf("reading app.js: %v", err)
	}
	page, err := assetFS.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("reading index.html: %v", err)
	}

	ids := regexp.MustCompile(`getElementById\("([^"]+)"\)`).FindAllStringSubmatch(string(script), -1)
	if len(ids) == 0 {
		t.Fatal("found no getElementById calls; the extraction is broken")
	}

	for _, match := range ids {
		id := match[1]
		if !strings.Contains(string(page), `id="`+id+`"`) {
			t.Errorf("app.js looks for #%s, which index.html does not define", id)
		}
	}
}

// The page must carry a token to the script, and the placeholder must have
// been substituted.
func TestThePageCarriesTheToken(t *testing.T) {
	f := newWebFixture(t, nodeConfig{})

	resp := f.do(t, "GET", "/", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the page gave %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the page: %v", err)
	}
	if strings.Contains(string(body), "{{token}}") {
		t.Fatal("the token placeholder was not substituted")
	}
	if !strings.Contains(string(body), f.ui.token) {
		t.Fatal("the page does not carry the token, so the script cannot authenticate")
	}
}

// A syntax error in app.js breaks the whole interface and nothing else would
// notice: CI runs gofmt and vet, which look only at Go. The script is embedded,
// so it ships broken and the page renders blank.
//
// Skipped where node is unavailable rather than failing, since it is a
// convenience check and not something the build should depend on.
func TestTheScriptParses(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; skipping the script syntax check")
	}

	script, err := assetFS.ReadFile("assets/app.js")
	if err != nil {
		t.Fatalf("reading app.js: %v", err)
	}

	// Written out rather than checked in place, so this tests the copy that is
	// actually embedded in the binary.
	path := filepath.Join(t.TempDir(), "app.js")
	if err := os.WriteFile(path, script, 0o600); err != nil {
		t.Fatalf("writing the script: %v", err)
	}

	out, err := exec.Command(node, "--check", path).CombinedOutput()
	if err != nil {
		t.Fatalf("app.js does not parse:\n%s", out)
	}
}
