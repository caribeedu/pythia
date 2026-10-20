(() => {
  const P = window.PYTHIA;
  const state = { center: P.center || null, depth: 1, t: null, selected: null };
  const $ = (id) => document.getElementById(id);
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const qs = () => (state.t ? `&t=${encodeURIComponent(state.t)}` : "");

  if (typeof cytoscape === "undefined") {
    $("cy").innerHTML = '<p class="muted" style="padding:20px">Could not load the graph library (offline?).</p>';
    return;
  }

  const palette = () => ({
    person: css("--accent"), team: "#8a6fd1", system: "#2e8b7a", repository: "#2e8b7a", project: "#c07a2c",
    concept: "#8c8c8c", narrative: css("--warn"), other: "#8c8c8c",
  });

  const cy = cytoscape({
    container: $("cy"),
    wheelSensitivity: 0.25,
    style: [
      { selector: "node", style: {
        "background-color": (n) => palette()[n.data("kind")] || palette().other,
        "label": "data(label)", "color": css("--text"), "font-size": 11, "text-valign": "bottom", "text-margin-y": 4,
        "width": (n) => 18 + Math.min(n.data("claims") || 0, 30), "height": (n) => 18 + Math.min(n.data("claims") || 0, 30),
        "text-wrap": "ellipsis", "text-max-width": 140 } },
      { selector: "node[group = 'narrative']", style: { "shape": "round-rectangle", "width": 34, "height": 22 } },
      { selector: "node[?center]", style: { "border-width": 3, "border-color": css("--text") } },
      { selector: "node:selected", style: { "border-width": 3, "border-color": css("--accent") } },
      { selector: "edge", style: {
        "width": 1.4, "line-color": css("--line"), "target-arrow-color": css("--line"), "target-arrow-shape": "triangle",
        "curve-style": "bezier", "label": "data(label)", "font-size": 9, "color": css("--muted"),
        "text-rotation": "autorotate", "text-background-color": css("--bg"), "text-background-opacity": 0.8 } },
      { selector: "edge[status = 'disputed']", style: { "line-style": "dashed", "line-color": css("--bad") } },
      { selector: "edge[status = 'member']", style: { "line-style": "dotted", "target-arrow-shape": "none" } },
    ],
  });

  async function loadGraph() {
    const url = `/api/graph?depth=${state.depth}${state.center ? `&center=${encodeURIComponent(state.center)}` : ""}${qs()}`;
    const g = await (await fetch(url)).json();
    state.center = g.center;
    cy.elements().remove();
    cy.add(g.nodes.map((n) => ({ data: n })));
    cy.add(g.edges.map((e) => ({ data: e })));
    if (!g.nodes.length) {
      $("side").innerHTML = '<p class="muted">Nothing to show yet at this point in time.</p>';
      return;
    }
    cy.layout({ name: "concentric", concentric: (n) => (n.data("center") ? 10 : n.data("group") === "narrative" ? 5 : 1),
                levelWidth: () => 1, minNodeSpacing: 60, animate: false }).run();
    cy.fit(undefined, 60);
    if (cy.zoom() > 1.2) { cy.zoom(1.2); cy.center(); }
    if (g.center) loadDetail(state.selected && cy.getElementById(state.selected).length ? state.selected : g.center);
  }

  function md(text) {
    return esc(text)
      .replace(/\[(cl_[0-9a-f]+)\]/g, '<a href="#" data-claim="$1" class="mono">[$1]</a>')
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      .replace(/^### (.*)$/gm, "<strong>$1</strong>")
      .replace(/^## (.*)$/gm, "<strong>$1</strong>")
      .replace(/^# (.*)$/gm, "<strong>$1</strong>");
  }

  async function loadDetail(id) {
    state.selected = id;
    const d = await (await fetch(`/api/node/${encodeURIComponent(id)}?${qs().slice(1)}`)).json();
    if (d.error) { $("side").innerHTML = `<p class="muted">${esc(d.error)}</p>`; return; }
    const claims = (d.claims || []).map((c) => `
      <div class="claim ${esc(c.status)}" id="${esc(c.id)}">
        <div>${esc(c.statement)}</div>
        <div class="meta"><span class="chip">${esc(c.kind)}</span> <span class="chip ${c.status === "disputed" ? "bad" : ""}">${esc(c.status)}</span>
          ${esc(c.origin)} · ${esc(c.source || "")} · ${esc((c.recorded_at || "").slice(0, 10))}
          · <a href="#" data-ev="${esc(c.id)}">evidence</a>
          · <a href="#" data-fix="${esc(c.id)}">correct</a>
          · <a href="#" data-retract="${esc(c.id)}">retract</a>
          <span class="mono">${esc(c.id)}</span></div>
        <div class="evbox"></div>
      </div>`).join("");
    const verdicts = (d.verdicts || []).map((v) =>
      `<div class="claim"><span class="chip accent">${esc(v.kind.replaceAll("_", " "))}</span> ${esc(v.summary)}
        <div class="meta">${esc(v.status)} · ${esc((v.created_at || "").slice(0, 16))} · <a href="/verdicts">${esc(v.id)}</a></div></div>`).join("");
    const mentions = (d.mentions || []).map((m) =>
      `<div class="claim"><strong>“${esc(m.surface)}”</strong> <span class="muted">${esc(m.context || "(context removed)")}</span>
        <div class="meta">confidence ${m.confidence == null ? "?" : Math.round(m.confidence * 100) + "%"} ·
        <a href="#" data-relink="${esc(m.id)}">relink</a> <span class="mono">${esc(m.id)}</span></div></div>`).join("");
    const members = (d.members || []).map((m) => `<a href="#" data-center="${esc(m.id)}">${esc(m.name)}</a>`).join(", ");
    $("side").innerHTML = `
      <h2>${esc(d.title)}</h2>
      <div class="row">
        <span class="chip accent">${esc(d.type || d.kind)}</span>
        ${d.owner ? '<span class="chip">owner</span>' : ""}
        ${d.origin ? `<span class="chip">${esc(d.origin)}</span>` : ""}
        ${d.stale ? '<span class="chip warn">stale</span>' : ""}
        <span class="mono muted">${esc(d.id)}</span>
      </div>
      ${d.aliases && d.aliases.length ? `<p class="muted">Also: ${esc(d.aliases.join(", "))}</p>` : ""}
      ${members ? `<p class="muted">Members: ${members}</p>` : ""}
      <p><strong>${esc(d.l0 || "No page yet.")}</strong></p>
      <div class="row">
        ${d.id !== state.center ? `<button data-center="${esc(d.id)}">Center</button>` : ""}
        <button data-fix="${esc(d.view_id || d.id)}">Correct</button>
        ${d.kind === "entity" ? `<button data-merge="${esc(d.id)}">Merge into…</button>` : ""}
      </div>
      <div id="fixbox"></div>
      ${d.l1 ? `<h2>Page</h2><div class="l1">${md(d.l1)}</div>` : ""}
      <h2>Claims (${(d.claims || []).length})</h2>${claims || '<p class="muted">None.</p>'}
      ${verdicts ? `<h2>Verdicts</h2>${verdicts}` : ""}
      ${mentions ? `<h2>Mentions</h2>${mentions}` : ""}`;
    cy.$(":selected").unselect();
    cy.getElementById(id).select();
  }

  function fixForm(targetId, placeholder) {
    $("fixbox").innerHTML = `<div class="card"><label>Correction for ${esc(targetId)} (maximum authority)</label>
      <textarea id="fixtext" placeholder="${esc(placeholder || "What is wrong and what is true?")}"></textarea>
      <button class="primary" id="fixsend">Send</button> <span id="fixmsg" class="muted"></span></div>`;
    $("fixsend").onclick = async () => {
      const r = await fetch("/api/correct", { method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ target_id: targetId, text: $("fixtext").value }) });
      const j = await r.json();
      $("fixmsg").textContent = `Recorded (${j.correction_id}); it will be applied shortly.`;
    };
  }

  function pickEntity(title, onPick) {
    $("fixbox").innerHTML = `<div class="card"><label>${esc(title)}</label>
      <input type="text" id="entq" placeholder="Search entity…"><div id="entres"></div>
      <label>or create a new entity</label><div class="row"><input type="text" id="newname" placeholder="Name">
      <select id="newtype">${["person","team","system","project","repository","concept","organization","tool","other"].map((t) => `<option>${t}</option>`).join("")}</select>
      <button id="newgo">Create and use</button></div></div>`;
    const search = async () => {
      const rows = await (await fetch(`/api/entities?q=${encodeURIComponent($("entq").value)}`)).json();
      $("entres").innerHTML = rows.map((r) => `<div><a href="#" data-pick="${esc(r.id)}">${esc(r.name)}</a> <span class="muted">${esc(r.type)} · ${esc(r.id)}</span></div>`).join("");
    };
    $("entq").oninput = search; search();
    $("entres").onclick = (e) => { const id = e.target.dataset.pick; if (id) { e.preventDefault(); onPick({ entity_id: id }); } };
    $("newgo").onclick = () => onPick({ new_name: $("newname").value, new_type: $("newtype").value });
  }

  const post = (url, body) => fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

  $("side").addEventListener("click", async (e) => {
    const t = e.target;
    if (t.dataset.center) { e.preventDefault(); state.center = t.dataset.center; state.selected = null; loadGraph(); }
    else if (t.dataset.claim) { e.preventDefault(); const el = document.getElementById(t.dataset.claim); if (el) el.scrollIntoView({ behavior: "smooth" }); }
    else if (t.dataset.ev) {
      e.preventDefault();
      const box = t.closest(".claim").querySelector(".evbox");
      if (box.innerHTML) { box.innerHTML = ""; return; }
      const j = await (await fetch(`/api/open/${t.dataset.ev}?level=3`)).json();
      box.innerHTML = `<pre>${esc(j.text)}</pre>`;
    }
    else if (t.dataset.fix) { e.preventDefault(); fixForm(t.dataset.fix); }
    else if (t.dataset.retract) {
      e.preventDefault();
      if (confirm("Retract this claim?")) { await post("/api/retract", { claim_id: t.dataset.retract }); setTimeout(() => loadDetail(state.selected), 400); }
    }
    else if (t.dataset.relink) {
      e.preventDefault();
      const mention = t.dataset.relink;
      pickEntity(`Relink mention ${mention} to:`, async (choice) => {
        await post("/api/relink", { mention_id: mention, ...choice }); setTimeout(loadGraph, 400);
      });
    }
    else if (t.dataset.merge) {
      e.preventDefault();
      const from = t.dataset.merge;
      pickEntity(`Merge ${from} into:`, async (choice) => {
        if (!choice.entity_id) return;
        await post("/api/merge", { from, into: choice.entity_id }); state.center = choice.entity_id; setTimeout(loadGraph, 400);
      });
    }
  });

  cy.on("tap", "node", (e) => loadDetail(e.target.id()));
  cy.on("dbltap", "node", (e) => { state.center = e.target.id(); state.selected = null; loadGraph(); });

  // Search
  let timer;
  $("q").addEventListener("input", () => {
    clearTimeout(timer);
    const q = $("q").value.trim();
    if (!q) { $("results").style.display = "none"; return; }
    timer = setTimeout(async () => {
      const rows = await (await fetch(`/api/search?q=${encodeURIComponent(q)}`)).json();
      $("results").innerHTML = rows.map((r) => `<div data-id="${esc(r.type === "claim" ? r.entity_id : (r.entity_id || r.id))}">
        <span class="chip">${esc(r.type)}</span> <strong>${esc(r.title)}</strong><br><span class="muted">${esc(r.l0)}</span></div>`).join("") || '<div class="muted">Nothing.</div>';
      $("results").style.display = "block";
    }, 250);
  });
  $("results").addEventListener("click", (e) => {
    const row = e.target.closest("[data-id]");
    if (!row || !row.dataset.id) return;
    state.center = row.dataset.id; state.selected = null; $("results").style.display = "none"; loadGraph();
  });
  $("depth").onchange = () => { state.depth = +$("depth").value; loadGraph(); };
  $("home").onclick = () => { state.center = null; state.selected = null; loadGraph(); };

  // Time
  const first = P.first ? new Date(P.first).getTime() : Date.now();
  const updateTime = () => {
    const v = +$("time").value;
    if (v >= 1000 || !P.first) { state.t = null; $("timelabel").textContent = "now"; }
    else {
      const ts = new Date(first + (Date.now() - first) * (v / 1000));
      state.t = ts.toISOString();
      $("timelabel").textContent = ts.toLocaleString("en-US");
    }
  };
  let ttimer;
  $("time").addEventListener("input", () => { updateTime(); clearTimeout(ttimer); ttimer = setTimeout(loadGraph, 250); });

  // Time markers under the slider: ~6 evenly spaced dates between the first record and now.
  // Clicking a marker moves the slider to it.
  const drawTicks = () => {
    const box = $("ticklabels"), list = $("timeticks");
    if (!box || !P.first) return;
    const span = Date.now() - first, day = 86400000;
    const fmt = (d) => span < 7 * day
      ? d.toLocaleString("en-US", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" })
      : span < 120 * day ? d.toLocaleDateString("en-US", { day: "2-digit", month: "2-digit" })
      : d.toLocaleDateString("en-US", { month: "2-digit", year: "numeric" });
    const n = Math.max(2, Math.min(6, Math.floor(box.clientWidth / (span < 7 * day ? 110 : 80))));
    box.innerHTML = ""; list.innerHTML = "";
    for (let i = 0; i <= n; i++) {
      const v = Math.round(1000 * i / n);
      const label = i === n ? "now" : fmt(new Date(first + span * (v / 1000)));
      const el = document.createElement("span");
      el.style.left = `${v / 10}%`; el.textContent = label;
      el.onclick = () => { $("time").value = v; updateTime(); loadGraph(); };
      box.appendChild(el);
      const opt = document.createElement("option"); opt.value = v; list.appendChild(opt);
    }
  };
  drawTicks();
  window.addEventListener("resize", drawTicks);

  loadGraph();
})();
