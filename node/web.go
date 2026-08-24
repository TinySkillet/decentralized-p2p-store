package node

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The local web UI.
//
// **This surface grants everything the control socket grants, plus trust
// administration, which makes it the most powerful entry point the node has.**
// The socket's own justification does not carry over: a 0600 socket is
// restricted to one user, but a loopback port is reachable by every process and
// every user on the machine, and a browser on it can be steered by any page the
// person happens to be visiting.
//
// So, cheapest control first:
//
//   - Host is validated on every /api/ request. This is the DNS-rebinding kill
//     switch: an attacker's page can make the browser resolve their domain to
//     127.0.0.1, but it cannot change the Host header it sends.
//   - Origin and Sec-Fetch-Site are validated on mutations.
//   - A CSRF token is required in a header, **with no cookie anywhere**. No
//     cookie means no ambient authority: a cross-site request cannot borrow
//     credentials it was never given. The token is required on GET too, because
//     GET /api/file reads file contents out.
//   - A non-loopback bind is refused unless explicitly allowed and backed by a
//     token file, since off-machine exposure is a different threat entirely.
//   - CSP forbids everything but this origin's own script and style, which is
//     why they are separate files rather than inline.
//
// There is no login. The security boundary is the machine, as it is for the
// control socket; these controls exist to stop *other* origins reaching
// through the browser, not to authenticate the person at the keyboard.

//go:embed assets
var assetFS embed.FS

// csrfHeader carries the token. A header rather than a form field because a
// cross-origin request cannot set custom headers without a successful preflight,
// which this server never grants.
const csrfHeader = "X-P2P-Token"

// httpTokenFile holds the token when the UI is bound off-loopback.
const httpTokenFile = "http-token"

// webUI serves the local interface.
type webUI struct {
	node  *FileServer
	token string
	mux   *http.ServeMux

	// loopbackOnly records whether this instance refuses non-loopback Hosts.
	loopbackOnly bool
}

// ListenHTTP starts the web UI.
//
// Off by default, and started only when an address is configured: it is a
// strictly larger attack surface than the socket, so it is opt-in rather than
// something a node acquires by being upgraded.
func (s *FileServer) ListenHTTP(addr string, allowNonLoopback bool, tokenDir string) error {
	loopback, err := isLoopbackAddr(addr)
	if err != nil {
		return err
	}

	token := ""
	if !loopback {
		if !allowNonLoopback {
			return fmt.Errorf("refusing to serve the web UI on %s: it is not a loopback address, "+
				"and the UI can administer trust. Bind 127.0.0.1 and use an SSH tunnel, "+
				"or pass the explicit override if you really mean to expose it", addr)
		}

		// Off-machine exposure has no machine boundary to rely on, so it needs
		// a shared secret the operator can hand out deliberately.
		token, err = loadOrCreateHTTPToken(tokenDir)
		if err != nil {
			return fmt.Errorf("preparing the web UI token: %w", err)
		}
	} else {
		token, err = randomToken()
		if err != nil {
			return err
		}
	}

	ui := &webUI{node: s, token: token, mux: http.NewServeMux(), loopbackOnly: !allowNonLoopback}
	ui.routes()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Handler:           ui,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.httpServer = srv
	s.web = ui

	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[%s] Web UI stopped: %v", s.Transport.Address(), err)
		}
	}()

	fmt.Printf("[%s] Web UI on http://%s\n", s.Transport.Address(), addr)
	if !loopback {
		fmt.Printf("[%s] Not loopback: callers need the token in %s\n",
			s.Transport.Address(), filepath.Join(tokenDir, httpTokenFile))
	}
	return nil
}

func (ui *webUI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Applied to every response, including errors: a page served without them
	// is a page that can be framed or can load someone else's script.
	h := w.Header()
	h.Set("Content-Security-Policy",
		"default-src 'none'; script-src 'self'; style-src 'self'; "+
			"img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Cache-Control", "no-store")

	ui.mux.ServeHTTP(w, r)
}

func (ui *webUI) routes() {
	ui.mux.HandleFunc("/", ui.serveIndex)
	ui.mux.HandleFunc("/app.js", ui.serveAsset("assets/app.js", "text/javascript; charset=utf-8"))
	ui.mux.HandleFunc("/app.css", ui.serveAsset("assets/app.css", "text/css; charset=utf-8"))

	ui.mux.HandleFunc("/api/state", ui.guard(false, ui.handleState))
	// The one endpoint that accepts the token in the query string, because
	// EventSource cannot set a header. See allowQueryToken.
	ui.mux.HandleFunc("/api/events", ui.guardStream(ui.handleEvents))
	ui.mux.HandleFunc("/api/file", ui.guard(false, ui.handleDownload))

	ui.mux.HandleFunc("/api/upload", ui.guard(true, ui.handleUpload))
	ui.mux.HandleFunc("/api/delete", ui.guard(true, ui.handleDelete))
	ui.mux.HandleFunc("/api/recheck", ui.guard(true, ui.handleRecheck))
	ui.mux.HandleFunc("/api/trust", ui.guard(true, ui.handleTrust))
	ui.mux.HandleFunc("/api/untrust", ui.guard(true, ui.handleUntrust))
	ui.mux.HandleFunc("/api/mode", ui.guard(true, ui.handleMode))
}

// guardStream is guard for the event stream, which may carry its token in the
// query string.
//
// EventSource cannot set headers, and the alternative is a cookie — which is
// strictly worse, because a cookie is ambient authority that any origin can
// make the browser attach. A query parameter is sent only when this page asks
// for it. The cost is that the token may appear in a log; Referrer-Policy is
// no-referrer, the endpoint is read-only, and the Host check still applies.
func (ui *webUI) guardStream(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ui.hostAllowed(r.Host) {
			http.Error(w, "unexpected Host", http.StatusForbidden)
			return
		}

		supplied := r.Header.Get(csrfHeader)
		if supplied == "" {
			supplied = r.URL.Query().Get("token")
		}
		if subtle.ConstantTimeCompare([]byte(supplied), []byte(ui.token)) != 1 {
			http.Error(w, "missing or wrong token", http.StatusForbidden)
			return
		}

		next(w, r)
	}
}

// guard applies the checks every API request must pass. mutating requests get
// the origin checks as well.
func (ui *webUI) guard(mutating bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ui.hostAllowed(r.Host) {
			// The rebinding kill switch. Deliberately before the token check:
			// a wrong Host is never worth answering.
			http.Error(w, "unexpected Host", http.StatusForbidden)
			return
		}

		if subtle.ConstantTimeCompare([]byte(r.Header.Get(csrfHeader)), []byte(ui.token)) != 1 {
			http.Error(w, "missing or wrong "+csrfHeader, http.StatusForbidden)
			return
		}

		if mutating {
			if r.Method != http.MethodPost {
				http.Error(w, "use POST", http.StatusMethodNotAllowed)
				return
			}
			if !ui.originAllowed(r) {
				http.Error(w, "cross-origin request refused", http.StatusForbidden)
				return
			}
		}

		next(w, r)
	}
}

// hostAllowed reports whether the Host header is one this UI answers on.
func (ui *webUI) hostAllowed(host string) bool {
	if host == "" {
		return false
	}
	if !ui.loopbackOnly {
		// Bound deliberately off-loopback, so the name it is reached by is the
		// operator's business. The token is what protects it there.
		return true
	}

	name := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		name = h
	}
	name = strings.Trim(name, "[]")

	if name == "localhost" {
		return true
	}
	ip := net.ParseIP(name)
	return ip != nil && ip.IsLoopback()
}

// originAllowed reports whether a mutation came from this origin.
func (ui *webUI) originAllowed(r *http.Request) bool {
	// Sent by every current browser and not forgeable by a page: same-origin
	// and none (a direct navigation or a tool) are both acceptable.
	switch r.Header.Get("Sec-Fetch-Site") {
	case "same-origin", "none", "":
	default:
		return false
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		// A non-browser caller such as curl. It already had to know the token.
		return true
	}

	// Compared by host, since the scheme is fixed and the port is in Host.
	want := "http://" + r.Host
	return origin == want
}

func (ui *webUI) serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if !ui.hostAllowed(r.Host) {
		http.Error(w, "unexpected Host", http.StatusForbidden)
		return
	}

	page, err := assetFS.ReadFile("assets/index.html")
	if err != nil {
		http.Error(w, "the interface is missing from this build", http.StatusInternalServerError)
		return
	}

	// The token reaches the page as markup, not as inline script, so the CSP
	// can forbid inline script entirely. No cookie is set: the token is held
	// in the page and sent as a header, so there is no ambient authority for
	// another origin to borrow.
	page = []byte(strings.Replace(string(page), "{{token}}", ui.token, 1))

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(page)
}

func (ui *webUI) serveAsset(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ui.hostAllowed(r.Host) {
			http.Error(w, "unexpected Host", http.StatusForbidden)
			return
		}
		body, err := assetFS.ReadFile(name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Write(body)
	}
}

// stateResponse is everything the page draws, in one request.
//
// One call rather than several because the parts have to agree: peers rendered
// from one instant and files from another would show a holder as trusted in one
// panel and not the other.
type stateResponse struct {
	Node    NodeView          `json:"node"`
	Peers   []PeerView        `json:"peers"`
	Files   []ReplicaSnapshot `json:"files"`
	Trusted []TrustedPeerView `json:"trusted"`
	Mode    string            `json:"mode"`
	Now     time.Time         `json:"now"`
}

func (ui *webUI) handleState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	s := ui.node

	nodeView, err := s.NodeView(ctx)
	if err != nil {
		httpError(w, err)
		return
	}
	peers, err := s.PeerViews(ctx)
	if err != nil {
		httpError(w, err)
		return
	}
	// The cached snapshot, never ReplicationStatus: that is one network round
	// trip per file and would make a refresh take minutes.
	files, err := s.ReplicationSnapshot(ctx)
	if err != nil {
		httpError(w, err)
		return
	}
	trusted, err := s.TrustedPeers(ctx)
	if err != nil {
		httpError(w, err)
		return
	}

	writeJSON(w, stateResponse{
		Node: nodeView, Peers: peers, Files: files,
		Trusted: trusted, Mode: s.TrustMode(), Now: time.Now(),
	})
}

func (ui *webUI) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	events, cancel := ui.node.Subscribe(watchBuffer)
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	heartbeat := time.NewTicker(ui.node.heartbeat())
	defer heartbeat.Stop()

	for {
		select {
		case e, ok := <-events:
			if !ok {
				return
			}
			payload, err := json.Marshal(e)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()

		case <-heartbeat.C:
			// A comment, which the EventSource API ignores. It exists so a
			// connection through a proxy is not dropped as idle, and so a
			// write failure reveals a client that has gone.
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()

		case <-r.Context().Done():
			return

		case <-ui.node.quitch:
			return
		}
	}
}

func (ui *webUI) handleUpload(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		http.Error(w, "a name is required", http.StatusBadRequest)
		return
	}

	// The body is streamed straight into storage. Not ParseMultipartForm,
	// which spools the whole upload to a temporary file first — under the
	// systemd unit that is PrivateTmp, and it would double the disk cost of
	// every upload for no benefit.
	if err := ui.node.Store(name, r.Body); err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, map[string]string{"stored": name})
}

func (ui *webUI) handleDownload(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		http.Error(w, "a name is required", http.StatusBadRequest)
		return
	}

	size, body, err := ui.node.Get(name)
	if err != nil {
		httpError(w, err)
		return
	}
	if closer, ok := body.(io.Closer); ok {
		defer closer.Close()
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	// The name is quoted and stripped of anything that could break out of the
	// header or suggest a path.
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", safeFilename(name)))
	io.Copy(w, body)
}

func (ui *webUI) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req struct{ Name string }
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := ui.node.Delete(req.Name); err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, map[string]string{"deleted": req.Name})
}

func (ui *webUI) handleRecheck(w http.ResponseWriter, r *http.Request) {
	var req struct{ Name string }
	if !decodeJSON(w, r, &req) {
		return
	}
	snap, err := ui.node.Recheck(r.Context(), req.Name)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, snap)
}

func (ui *webUI) handleTrust(w http.ResponseWriter, r *http.Request) {
	var req struct{ Node, Label string }
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := ui.node.Trust(req.Node, req.Label); err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, map[string]string{"trusted": req.Node})
}

func (ui *webUI) handleUntrust(w http.ResponseWriter, r *http.Request) {
	var req struct{ Node string }
	if !decodeJSON(w, r, &req) {
		return
	}
	had, err := ui.node.Untrust(req.Node)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, map[string]bool{"changed": had})
}

func (ui *webUI) handleMode(w http.ResponseWriter, r *http.Request) {
	var req struct{ Mode string }
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := ui.node.SetTrustMode(req.Mode); err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, map[string]string{"mode": ui.node.TrustMode()})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, into any) bool {
	// Bounded: these are all small control messages, and an unbounded decode
	// is a way to make the node allocate without limit.
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(into); err != nil {
		http.Error(w, "malformed request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("Could not write a web response: %v", err)
	}
}

// httpError reports a node error to the page. 409 rather than 500: these are
// refusals and conflicts — an untrusted peer, a file that is not here — not
// server faults, and the page shows them to the person as such.
func httpError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusConflict)
}

// safeFilename strips anything that could escape the header or read as a path.
func safeFilename(name string) string {
	name = filepath.Base(name)
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == '"' || r == '\\' || r == '/' {
			return -1
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		return "download"
	}
	return name
}

func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// loadOrCreateHTTPToken reads the shared token, creating it if absent.
func loadOrCreateHTTPToken(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("no directory to keep the token in")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}

	path := filepath.Join(dir, httpTokenFile)
	existing, err := os.ReadFile(path)
	if err == nil {
		token := strings.TrimSpace(string(existing))
		if token != "" {
			return token, nil
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}

	token, err := randomToken()
	if err != nil {
		return "", err
	}
	// 0600: the token is the whole authorisation when the UI is off-loopback.
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return "", err
	}
	return token, nil
}

// isLoopbackAddr reports whether a listen address is confined to this machine.
func isLoopbackAddr(addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("web UI address %q must be host:port: %w", addr, err)
	}

	switch host {
	case "localhost":
		return true, nil
	case "", "0.0.0.0", "::", "[::]":
		// An empty host binds every interface, which is the case most likely
		// to be an accident.
		return false, nil
	}

	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false, nil
	}
	return ip.IsLoopback(), nil
}

// StopHTTP shuts the web UI down.
func (s *FileServer) StopHTTP() {
	if s.httpServer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.httpServer.Shutdown(ctx)
}
