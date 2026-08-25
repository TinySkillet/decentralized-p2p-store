"use strict";

// The token is read from markup rather than a cookie. That is the point: with
// no cookie there is no ambient authority, so another origin cannot make an
// authenticated request even if it can reach this port.
const TOKEN = document.querySelector('meta[name="p2p-token"]').content;

const MAX_ACTIVITY = 40;
const activity = [];

async function api(path, options = {}) {
  const headers = Object.assign({ "X-P2P-Token": TOKEN }, options.headers || {});
  const response = await fetch(path, Object.assign({}, options, { headers }));
  if (!response.ok) {
    throw new Error((await response.text()).trim() || response.statusText);
  }
  return response;
}

async function post(path, body) {
  return api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

let toastTimer = null;
function toast(message) {
  const el = document.getElementById("toast");
  el.textContent = message;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 6000);
}

function short(id) {
  return id ? id.slice(0, 12) : "";
}

function ago(iso) {
  const then = new Date(iso).getTime();
  if (!then) return "never";
  const seconds = Math.max(0, Math.round((Date.now() - then) / 1000));
  if (seconds < 60) return seconds + "s ago";
  if (seconds < 3600) return Math.round(seconds / 60) + "m ago";
  if (seconds < 86400) return Math.round(seconds / 3600) + "h ago";
  return Math.round(seconds / 86400) + "d ago";
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  // textContent, never innerHTML: names, labels and errors all come from
  // elsewhere on the network and must never be parsed as markup.
  if (text !== undefined) node.textContent = text;
  return node;
}

function emptyItem(message) {
  const li = el("li");
  li.appendChild(el("span", "empty", message));
  return li;
}

function peerItem(peer, trustedIDs, options = {}) {
  const li = el("li");
  li.appendChild(el("span", "id", short(peer.NodeID)));

  const label = trustedIDs.get(peer.NodeID);
  if (label) li.appendChild(el("span", "addr", label));

  li.appendChild(el("span", "addr", peer.Address || "no known address"));

  const grow = el("span", "grow");
  li.appendChild(grow);

  // Liveness is membership of the live set, never a database column.
  if (peer.Online) {
    li.appendChild(el("span", "state online", "online"));
  } else {
    li.appendChild(el("span", "state offline", "last seen " + ago(peer.LastSeen)));
  }

  if (options.approve) {
    const button = el("button", "primary", "Approve");
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        await post("/api/trust", { Node: peer.NodeID, Label: "" });
        await refresh();
      } catch (err) {
        toast("Could not approve: " + err.message);
        button.disabled = false;
      }
    });
    li.appendChild(button);
  }

  if (options.revoke) {
    const button = el("button", "danger", "Revoke");
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        await post("/api/untrust", { Node: peer.NodeID });
        await refresh();
      } catch (err) {
        toast("Could not revoke: " + err.message);
        button.disabled = false;
      }
    });
    li.appendChild(button);
  }

  return li;
}

function pips(copies, target, untrusted) {
  const wrap = el("span", "pips");
  const trustedCopies = copies - untrusted;
  const total = Math.max(target, copies);
  for (let i = 0; i < total; i++) {
    const pip = el("span", "pip");
    if (i < trustedCopies) pip.classList.add("filled");
    else if (i < copies) pip.classList.add("fragile");
    wrap.appendChild(pip);
  }
  return wrap;
}

// whyShort explains a file below its replication target, which otherwise looks
// like a fault. There are two quite different reasons and the numbers alone
// cannot tell them apart: either there is somewhere for a copy to go and it has
// not happened yet, or there is nowhere for it to go at all.
function whyShort(file, candidates) {
  if (file.Copies >= file.Target) {
    if ((file.UntrustedHolders || []).length > 0) {
      return "meets its target only by counting peers you no longer approve";
    }
    return "";
  }
  if (candidates > 0) {
    return "waiting to be copied to " + candidates + " approved peer" + (candidates === 1 ? "" : "s");
  }
  return "no approved peer available to hold another copy — approve more peers, or lower the target";
}

function fileItem(file, trustedIDs, candidates) {
  const li = el("li");
  li.appendChild(el("span", "id", file.Name));

  const untrusted = (file.UntrustedHolders || []).length;
  li.appendChild(pips(file.Copies, file.Target, untrusted));

  // "Not checked" and "no copies" are different facts, so they read
  // differently rather than both showing as zero.
  const measured = file.MeasuredAt && !file.MeasuredAt.startsWith("0001-01-01");
  const state = measured
    ? file.Copies + " of " + file.Target + " · checked " + ago(file.MeasuredAt)
    : "not checked yet";
  li.appendChild(el("span", "meta", state));

  li.appendChild(el("span", "meta", file.Size + " bytes"));

  const why = whyShort(file, candidates);
  if (why) li.appendChild(el("span", "why", why));

  const holders = el("span", "holders");
  (file.Holders || []).forEach((id) => {
    const chip = el("span", "chip", short(id));
    if ((file.UntrustedHolders || []).includes(id)) {
      chip.classList.add("untrusted");
      chip.title = "holds a copy, but is not approved, so it will not be sent replacements";
    }
    holders.appendChild(chip);
  });
  li.appendChild(holders);

  li.appendChild(el("span", "grow"));

  const recheck = el("button", null, "Recheck");
  recheck.addEventListener("click", async () => {
    recheck.disabled = true;
    try {
      await post("/api/recheck", { Name: file.Name });
      await refresh();
    } catch (err) {
      toast("Could not recheck: " + err.message);
    } finally {
      recheck.disabled = false;
    }
  });
  li.appendChild(recheck);

  const download = el("button", null, "Download");
  download.addEventListener("click", () => {
    // Fetched with the token and handed to the browser as a blob, because the
    // token lives in a header and a plain link could not carry it.
    api("/api/file?name=" + encodeURIComponent(file.Name))
      .then((r) => r.blob())
      .then((blob) => {
        const url = URL.createObjectURL(blob);
        const a = el("a");
        a.href = url;
        a.download = file.Name;
        a.click();
        URL.revokeObjectURL(url);
      })
      .catch((err) => toast("Could not download: " + err.message));
  });
  li.appendChild(download);

  const remove = el("button", "danger", "Delete");
  remove.addEventListener("click", async () => {
    remove.disabled = true;
    try {
      await post("/api/delete", { Name: file.Name });
      await refresh();
    } catch (err) {
      toast("Could not delete: " + err.message);
      remove.disabled = false;
    }
  });
  li.appendChild(remove);

  return li;
}

function render(state) {
  const trustedIDs = new Map((state.trusted || []).map((p) => [p.NodeID, p.Label]));

  const n = state.node;
  document.getElementById("node-summary").textContent =
    n.Address + " · " + n.Peers + " connected · trust " + state.mode;

  // The full identity, not the abbreviation: this is the string another
  // operator has to type to approve this node, and it is the only place to
  // read it from.
  document.getElementById("node-id").textContent = n.NodeID;
  document.getElementById("node-stats").textContent =
    n.Files + " file" + (n.Files === 1 ? "" : "s") + " · " + n.Bytes +
    " bytes · wants " + n.ReplicationFactor + " copies of each";

  // Enforcement changes what approving a peer actually means, so the page has
  // to say which mode it is in. Saying "they cannot send you files until you
  // approve them" while the node accepts anything is worse than saying nothing.
  const enforcing = state.mode === "enforcing";
  document.getElementById("mode-banner").hidden = enforcing;
  document.getElementById("approval-note").textContent = enforcing
    ? "These peers are on the network and visible, but may not send you files " +
      "or ask you to delete anything until you approve them."
    : "Approval is not being enforced, so these peers can already send you files " +
      "and ask you to delete them. Approving records your intent for when you " +
      "start enforcing.";

  const peers = state.peers || [];
  const online = peers.filter((p) => p.Online);
  const pending = online.filter((p) => !trustedIDs.has(p.NodeID));
  const approved = online.filter((p) => trustedIDs.has(p.NodeID));
  const offline = peers.filter((p) => !p.Online);

  // The approval queue is the reason the interface exists, so it is only shown
  // when there is something to approve.
  const section = document.getElementById("approval-section");
  section.hidden = pending.length === 0;
  document.getElementById("pending-count").textContent = String(pending.length);

  const pendingList = document.getElementById("pending-peers");
  pendingList.replaceChildren();
  pending.forEach((p) => pendingList.appendChild(peerItem(p, trustedIDs, { approve: true })));

  const onlineList = document.getElementById("online-peers");
  onlineList.replaceChildren();
  if (approved.length === 0) {
    onlineList.appendChild(emptyItem(
      online.length > 0 ? "No approved peers connected." : "No peers connected."));
  }
  approved.forEach((p) => onlineList.appendChild(peerItem(p, trustedIDs, { revoke: true })));

  const offlineList = document.getElementById("offline-peers");
  offlineList.replaceChildren();
  if (offline.length === 0) offlineList.appendChild(emptyItem("Nothing remembered."));
  offline.forEach((p) => offlineList.appendChild(
    peerItem(p, trustedIDs, { revoke: trustedIDs.has(p.NodeID) })));

  const files = document.getElementById("files");
  files.replaceChildren();
  if ((state.files || []).length === 0) files.appendChild(emptyItem("No files stored here."));
  (state.files || []).forEach((f) => {
    // Peers that could take a copy: approved, connected, and not already
    // holding it. This node's own copy is counted separately.
    const holders = new Set(f.Holders || []);
    const candidates = approved.filter((p) => !holders.has(p.NodeID)).length;
    files.appendChild(fileItem(f, trustedIDs, candidates));
  });
}

function renderActivity() {
  const list = document.getElementById("activity");
  list.replaceChildren();
  if (activity.length === 0) {
    list.appendChild(emptyItem("Nothing yet."));
    return;
  }
  activity.forEach((e) => {
    const li = el("li");
    li.appendChild(el("span", "when", new Date(e.At).toLocaleTimeString()));
    li.appendChild(el("span", "kind", e.Kind));
    const parts = [];
    if (e.Name) parts.push(e.Name);
    if (e.Node) parts.push(short(e.Node));
    if (e.Peer) parts.push(e.Peer);
    if (e.Size) parts.push(e.Size + " bytes");
    if (e.Count) parts.push("x" + e.Count);
    if (e.Err) parts.push(e.Err);
    li.appendChild(el("span", null, parts.join(" · ")));
    list.appendChild(li);
  });
}

async function refresh() {
  try {
    const state = await (await api("/api/state")).json();
    render(state);
  } catch (err) {
    toast("Could not read the node: " + err.message);
  }
}

function listen() {
  // EventSource cannot set headers, so the token goes in the query for this
  // one endpoint. It is read-only and same-origin, and the Host check still
  // applies.
  const stream = new EventSource("/api/events?token=" + encodeURIComponent(TOKEN));

  stream.addEventListener("message", (message) => {
    let event;
    try {
      event = JSON.parse(message.data);
    } catch (err) {
      return;
    }
    activity.unshift(event);
    if (activity.length > MAX_ACTIVITY) activity.pop();
    renderActivity();
    refresh();
  });

  stream.addEventListener("error", () => {
    // EventSource reconnects on its own; nothing to do but stop reporting it.
  });
}

document.getElementById("enforce-button").addEventListener("click", async () => {
  try {
    await post("/api/mode", { Mode: "enforcing" });
    await refresh();
    toast("Approval is now enforced.");
  } catch (err) {
    toast("Could not switch: " + err.message);
  }
});

document.getElementById("copy-id").addEventListener("click", async () => {
  const id = document.getElementById("node-id").textContent;
  try {
    await navigator.clipboard.writeText(id);
    toast("Identity copied.");
  } catch (err) {
    // Clipboard access can be refused; selecting the text still works.
    toast("Could not copy automatically — select the identity and copy it.");
  }
});

document.getElementById("upload-input").addEventListener("change", async (change) => {
  const status = document.getElementById("upload-status");
  const files = Array.from(change.target.files || []);
  change.target.value = "";

  for (const file of files) {
    status.textContent = "Sending " + file.name + "…";
    try {
      // The file is the whole body, so it streams rather than being spooled
      // into a multipart form on either side.
      await api("/api/upload?name=" + encodeURIComponent(file.name), {
        method: "POST",
        body: file,
      });
    } catch (err) {
      toast("Could not store " + file.name + ": " + err.message);
    }
  }

  status.textContent = "";
  refresh();
});

renderActivity();
refresh();
listen();
setInterval(refresh, 15000);
