(() => {
  // small debounce helper
  function debounce(fn, wait) {
    let t; return function(...args){ clearTimeout(t); t = setTimeout(()=>fn.apply(this,args), wait); };
  }
  const yearSelect = document.getElementById('yearSelect');
  const directionSelect = document.getElementById('directionSelect');
  const topkInput = document.getElementById('topkInput');
  const clearBtn = document.getElementById('clearBtn');
  const mapCanvas = document.getElementById('mapCanvas');
  const flowCanvas = document.getElementById('flowCanvas');
  const currentInfo = document.getElementById('currentInfo');
  const chartEl = document.getElementById('chart');
  const yearChk2018 = document.getElementById('yearChk2018');
  const yearChk2021 = document.getElementById('yearChk2021');
  const yearChk2024 = document.getElementById('yearChk2024');
  const openOverlayBtn = document.getElementById('openOverlayBtn');
  const closeOverlayBtn = document.getElementById('closeOverlayBtn');
  const controlOverlay = document.getElementById('controlOverlay');
  const citySelect = document.getElementById('citySelect');
  const areaSelect = document.getElementById('areaSelect');
  const keywordInput = document.getElementById('keywordInput');
  const resetFilterBtn = document.getElementById('resetFilterBtn');
  const filterInfo = document.getElementById('filterInfo');
  const gridIdInput = document.getElementById('gridIdInput');
  const gridIdConfirmBtn = document.getElementById('gridIdConfirmBtn');
  const gridLookupStatus = document.getElementById('gridLookupStatus');

  const startLabelBtn = document.getElementById('startLabelBtn');
  const labelCountInput = document.getElementById('labelCount');
  const skipBtn = document.getElementById('skipBtn');
  const undoBtn = document.getElementById('undoBtn');
  const labelButtons = document.querySelectorAll('.label-buttons button');
  const labelStatus = document.getElementById('labelStatus');
  const resetQueueBtn = document.getElementById('resetQueueBtn');
  const clearLabelsBtn = document.getElementById('clearLabelsBtn');
  const labelWithinFilter = document.getElementById('labelWithinFilter');
  const enableLowFilter = document.getElementById('enableLowFilter');
  const lowValueInput = document.getElementById('lowValueInput');
  const lowPctInput = document.getElementById('lowPctInput');

  const metricTotal = document.getElementById('metricTotal');
  const metricOut = document.getElementById('metricOut');
  const metricIn = document.getElementById('metricIn');
  const modeOverlay = document.getElementById('modeOverlay');
  const modeSplit = document.getElementById('modeSplit');
  const modeDailyAvg = document.getElementById('modeDailyAvg');
  const modeWeekly = document.getElementById('modeWeekly');

  const importFile = document.getElementById('importFile');
  const importMode = document.getElementById('importMode');
  const importBtn = document.getElementById('importBtn');
  const importInfo = document.getElementById('importInfo');
  const heatToggle = document.getElementById('heatToggle');
  const outlineToggle = document.getElementById('outlineToggle');
  const covInput = document.getElementById('covInput');
  const otherLabelBtn = document.getElementById('otherLabelBtn');
  const otherRemark = document.getElementById('otherRemark');
  const otherSubmitBtn = document.getElementById('otherSubmitBtn');
  const labelStatsEl = document.getElementById('labelStats');

  const YEARS = [2018, 2021, 2024];
  const DEFAULT_YEAR_COLORS = {2018: '#ff7f0e', 2021: '#1f77b4', 2024: '#2ca02c'};
  let yearColors = Object.assign({}, DEFAULT_YEAR_COLORS);
  let gridColor = '#2e3b51';

  let metadata = [];
  let metaById = new Map();
  let quadtree = null;
  let filtered = [];
  let cityAreas = new Map(); // city -> Set(area)
  let transform = d3.zoomIdentity;
  let xScale, yScale; // linear scales for lon/lat -> canvas coords (before zoom)
  // grid cell half-size (deg); lat half-size is constant 250m; lon half-size depends on latitude
  const HALF_LAT_DEG = 250 / 111320; // ~0.002245 deg
  let halfLonCache = null; // Float32Array for per-point half-lon in deg
  let currentGridId = null;
  let labelMode = false;
  let targetCount = 0; // for progress display
  let queueAll = [];
  let queueIndex = 0;
  let lastFlowData = null;
  let heatById = new Map();
  let heatStats = {q95: 0, max: 0};
  let showHeat = false;
  let showOutline = false;
  let isBusy = false;
  let cityBoundsData = null;
  let gridLookupPending = false;
  let ellipsesData = null; // loaded from appdata/ellipses.json

  function setStatus(text) { labelStatus.textContent = text; }
  let lastStatusNote = '';
  function updateStatus(note) {
    if (typeof note === 'string') lastStatusNote = note;
    const base = (queueAll && queueAll.length) ? `进度：${Math.min(queueIndex, queueAll.length)}/${queueAll.length}` : '';
    const extra = lastStatusNote ? `，${lastStatusNote}` : '';
    const text = base ? (base + extra) : (lastStatusNote || '');
    setStatus(text);
  }

  async function refreshQueueStatus(note) {
    try {
      const q = await fetchJSON('/api/label_queue');
      queueAll = q.queue || queueAll || [];
      queueIndex = q.index || queueIndex || 0;
    } catch (e) {
      // keep existing if request fails
    }
    updateStatus(note || '');
  }

  function setControlsEnabled(enabled) {
    const dis = !enabled;
    try {
      labelButtons.forEach(btn => { btn.disabled = dis; });
    } catch (e) {}
    if (skipBtn) skipBtn.disabled = dis;
    if (undoBtn) undoBtn.disabled = dis;
  }

  // 更新右侧九类标签的当前计数
  async function refreshLabelStats() {
    if (!labelStatsEl) return;
    try {
      const j = await fetchJSON('/api/labels/stats');
      const by = j.by_label || {};
      const items = [];
      for (let l=1; l<=9; l++) {
        const name = (typeof LABEL_MAP !== 'undefined' && LABEL_MAP[l]) ? LABEL_MAP[l] : '';
        const n = by[String(l)] || by[l] || 0;
        items.push(`<div>${l} ${name}：${n}</div>`);
      }
      labelStatsEl.innerHTML = items.join('');
    } catch (e) { /* ignore */ }
  }

  function resizeCanvas() {
    const rect = mapCanvas.parentElement.getBoundingClientRect();
    [mapCanvas, flowCanvas].forEach(c => {
      const dpr = window.devicePixelRatio || 1;
      c.width = Math.round(rect.width * dpr);
      c.height = Math.round(rect.height * dpr);
      c.style.width = rect.width + 'px';
      c.style.height = rect.height + 'px';
      const ctx = c.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    });
    renderPoints();
    renderFlows(null);
  }

  function computeScales() {
    const lons = metadata.map(d => d.lon);
    const lats = metadata.map(d => d.lat);
    const lonExtent = d3.extent(lons);
    const latExtent = d3.extent(lats);
    const padding = 10; // px
    const rect = mapCanvas.parentElement.getBoundingClientRect();
    xScale = d3.scaleLinear().domain(lonExtent).range([padding, rect.width - padding]);
    yScale = d3.scaleLinear().domain(latExtent).range([rect.height - padding, padding]);
  }

  function buildHalfLonCache() {
    halfLonCache = new Float32Array(metadata.length);
    for (let i=0;i<metaDataLen();i++) {
      const lat = metadata[i].lat;
      const cosv = Math.max(0.1, Math.cos(lat * Math.PI/180));
      halfLonCache[i] = 250 / (111320 * cosv);
    }
  }
  function metaDataLen(){ return metadata.length|0; }

  function buildQuadtree() {
    quadtree = d3.quadtree()
      .x(d => d.lon)
      .y(d => d.lat)
      .addAll(filtered);
  }

  function worldToScreen(lon, lat) {
    const x = xScale(lon);
    const y = yScale(lat);
    const pt = transform.apply([x, y]);
    return pt;
  }

  function screenToWorld(sx, sy) {
    const p = transform.invert([sx, sy]);
    const lon = xScale.invert(p[0]);
    const lat = yScale.invert(p[1]);
    return [lon, lat];
  }

  function renderPoints() {
    const ctx = mapCanvas.getContext('2d');
    ctx.save();
    ctx.clearRect(0, 0, mapCanvas.width, mapCanvas.height);
    ctx.translate(transform.x, transform.y);
    ctx.scale(transform.k, transform.k);
    // Use opaque fill to avoid background showing through between tiles
    ctx.globalAlpha = 1.0;
    const n = filtered.length;
    // heat color scale if enabled
    let colorScale = null;
    if (showHeat && heatStats && heatStats.q95 > 0) {
      const q95 = heatStats.q95 || 1;
      colorScale = (v) => {
        const t = Math.max(0, Math.min(1, (v || 0) / q95));
        return d3.interpolateTurbo(t);
      };
    }
    for (let i = 0; i < n; i++) {
      const d = filtered[i];
      const halfLon = 250 / (111320 * Math.max(0.1, Math.cos(d.lat * Math.PI/180)));
      const x1 = xScale(d.lon - halfLon);
      const y1 = yScale(d.lat + HALF_LAT_DEG);
      const x2 = xScale(d.lon + halfLon);
      const y2 = yScale(d.lat - HALF_LAT_DEG);
      const w = Math.max(0.8, x2 - x1);
      const h = Math.max(0.8, y2 - y1);
      // Slightly expand each cell in screen space to remove hairline gaps
      const dpr = window.devicePixelRatio || 1;
      const pad = 0.75 / (dpr * Math.max(0.0001, transform.k || 1));
      if (colorScale) {
        const v = heatById.get(d.grid_id) || 0;
        ctx.fillStyle = colorScale(v);
        ctx.fillRect(x1 - pad, y1 - pad, w + 2*pad, h + 2*pad);
      } else {
        ctx.fillStyle = gridColor || '#000000ff';
        ctx.fillRect(x1 - pad, y1 - pad, w + 2*pad, h + 2*pad);
      }
    }
    // city outlines overlay (prefer shapefile)
    if (showOutline) {
      ctx.globalAlpha = 1.0;
      ctx.lineWidth = 1.2 / transform.k;
      ctx.strokeStyle = '#94a3b8';
      const filterCity = citySelect.value || '';
      if (cityBoundsData && Array.isArray(cityBoundsData.items)) {
        for (const it of cityBoundsData.items) {
          if (filterCity && it.name !== filterCity) continue;
          const rings = it.rings || [];
          for (const ring of rings) {
            if (!ring || ring.length < 2) continue;
            ctx.beginPath();
            for (let i=0;i<ring.length;i++){
              const [lon, lat] = ring[i];
              const p = worldToScreen(lon, lat);
              if (i===0) ctx.moveTo(p[0], p[1]); else ctx.lineTo(p[0], p[1]);
            }
            ctx.closePath();
            ctx.stroke();
          }
        }
      } else {
        const byCity = new Map();
        for (const d of filtered) {
          const c = d.city_name || '';
          if (!byCity.has(c)) byCity.set(c, []);
          byCity.get(c).push([d.lon, d.lat]);
        }
        for (const [city, pts] of byCity.entries()) {
          if (!pts || pts.length < 10) continue;
          let arr = pts;
          if (arr.length > 4000) arr = d3.shuffle(arr.slice()).slice(0, 4000);
          const hull = d3.polygonHull(arr);
          if (!hull) continue;
          ctx.beginPath();
          for (let i=0; i<hull.length; i++) {
            const [lon, lat] = hull[i];
            const p = worldToScreen(lon, lat);
            if (i === 0) ctx.moveTo(p[0], p[1]); else ctx.lineTo(p[0], p[1]);
          }
          ctx.closePath();
          ctx.stroke();
        }
      }
    }
    ctx.restore();
    // highlight current grid
    if (currentGridId != null) {
      const d = metaById.get(currentGridId);
      if (d) {
        const ctx2 = mapCanvas.getContext('2d');
        const halfLon = 250 / (111320 * Math.max(0.1, Math.cos(d.lat * Math.PI/180)));
        const p1 = worldToScreen(d.lon - halfLon, d.lat + HALF_LAT_DEG);
        const p2 = worldToScreen(d.lon + halfLon, d.lat - HALF_LAT_DEG);
        const x = Math.min(p1[0], p2[0]);
        const y = Math.min(p1[1], p2[1]);
        const w = Math.abs(p2[0] - p1[0]);
        const h = Math.abs(p2[1] - p1[1]);
        ctx2.save();
        ctx2.strokeStyle = '#f59e0b';
        ctx2.lineWidth = 2;
        ctx2.strokeRect(x, y, w, h);
        ctx2.restore();
      }
    }
  }

  function renderFlows(data) {
    const ctx = flowCanvas.getContext('2d');
    ctx.clearRect(0, 0, flowCanvas.width, flowCanvas.height);
    if (!data) return;
    lastFlowData = data; // cache for zoom re-render
    ctx.lineCap = 'round';

    function drawEdge(p0, p1, color, w, alpha) {
      ctx.save();
      ctx.strokeStyle = color;
      ctx.globalAlpha = alpha;
      ctx.lineWidth = w;
      ctx.beginPath();
      ctx.moveTo(p0[0], p0[1]);
      ctx.lineTo(p1[0], p1[1]);
      ctx.stroke();
      ctx.restore();
    }

    // Build dynamic width/alpha based on zoom and local value range
    const k = Math.max(0.8, transform.k || 1);
    let vals = [];
    const gather = (arr) => { (arr||[]).forEach(e => { if (e && typeof e.num_total === 'number') vals.push(e.num_total); }); };
    if (data.years) {
      for (const y of YEARS) {
        const fd = data.years[y];
        if (!fd) continue;
        gather(fd.out_edges); gather(fd.in_edges);
      }
    } else {
      gather(data.out_edges); gather(data.in_edges);
    }
    if (!vals.length) vals = [1];
    // Option A: make line width more sensitive to large values globally
    // - Use global max instead of 95th percentile for the upper domain bound
    // - Widen pixel range so high vs low flows separate more in appearance
    // - Use a pow scale (exp=0.5 ~ sqrt) which we can tweak easily later
    const vmax = Math.max(1, d3.quantile(vals.sort((a,b)=>a-b), 1.0) || 1);
    const widthScale = d3.scalePow().exponent(0.5).domain([1, vmax]).range([0.6, 8.0]);
    const widthFactor = 1 + 0.8/Math.sqrt(k); // thicker when zoomed out
    const baseAlpha = Math.min(0.75, Math.max(0.25, 0.55*(1/Math.sqrt(k))));

    if (data.years) {
      const yrsFilter = Array.isArray(data._yearsFilter) && data._yearsFilter.length ? data._yearsFilter : YEARS;
      for (const y of yrsFilter) {
        const fd = data.years[y]; if (!fd) continue;
        const edges = [].concat(fd.out_edges || [], fd.in_edges || []);
        const color = yearColors[y] || DEFAULT_YEAR_COLORS[y];
        for (const e of edges) {
          const p0 = worldToScreen(e.o.lon, e.o.lat);
          const p1 = worldToScreen(e.d.lon, e.d.lat);
          const w = widthScale(e.num_total || 1) * widthFactor;
          drawEdge(p0, p1, color, w, baseAlpha);
        }
      }
    } else {
      // single year
      const outEdges = data.out_edges || [];
      const inEdges = data.in_edges || [];
      for (const e of outEdges) {
        const p0 = worldToScreen(e.o.lon, e.o.lat);
        const p1 = worldToScreen(e.d.lon, e.d.lat);
        const w = widthScale(e.num_total || 1) * widthFactor;
        drawEdge(p0, p1, '#d62728', w, baseAlpha);
      }
      for (const e of inEdges) {
        const p0 = worldToScreen(e.o.lon, e.o.lat);
        const p1 = worldToScreen(e.d.lon, e.d.lat);
        const w = widthScale(e.num_total || 1) * widthFactor;
        drawEdge(p0, p1, '#1f77b4', w, baseAlpha);
      }
    }
  }

  // drawChart old version removed; using the new daily-average capable implementation below

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(await r.text());
    return await r.json();
  }

  async function loadMetadata() {
    const years = await fetchJSON('/api/years');
    yearSelect.innerHTML = years.map(y => `<option value="${y}">${y}</option>`).join('');
    metadata = await fetchJSON('/api/metadata');
    metaById = new Map(metadata.map(d => [d.grid_id, d]));
    // build city/area lists
    const cities = new Map();
    for (const m of metadata) {
      const c = m.city_name || '';
      const a = m.area_name || '';
      if (!cities.has(c)) cities.set(c, new Set());
      cities.get(c).add(a);
    }
    cityAreas = new Map([...cities.entries()].map(([k, v]) => [k, new Set([...v].filter(x => x))]));
    const cityOptions = ['<option value="">全部</option>'].concat([...cityAreas.keys()].filter(x=>x).sort().map(c => `<option value="${c}">${c}</option>`));
    citySelect.innerHTML = cityOptions.join('');
    areaSelect.innerHTML = '<option value="">全部</option>';
    // default filtered = all
    filtered = metadata;
    computeScales();
    buildHalfLonCache();
    buildQuadtree();
    resizeCanvas();
    updateFilterInfo();
    // try resume queue
    try { await resumeQueueIfAny(); } catch(e) {}
    // init heat/outline
    showHeat = !!(heatToggle && heatToggle.checked);
    showOutline = !!(outlineToggle && outlineToggle.checked);
    if (showHeat) { await refreshHeat(); renderPoints(); }
  }

  async function loadEllipses() {
    try {
      const r = await fetch('/static/ellipses.json');
      if (!r.ok) throw new Error(await r.text());
      ellipsesData = await r.json();
    } catch (e) {
      console.warn('ellipses.json not loaded:', e);
      ellipsesData = null;
    }
  }

  function updateCurrentInfo(gid) {
    if (!gid) { currentInfo.textContent = '未选择'; return; }
    const m = metaById.get(gid);
    if (!m) { currentInfo.textContent = `${gid}`; return; }
    const loc = [m.lon.toFixed(4), m.lat.toFixed(4)].join(',');
    currentInfo.textContent = `ID ${gid} (${loc}) ${m.city_name || ''} ${m.area_name || ''}`;
  }

  function setGridLookupStatus(text, isError=false) {
    if (!gridLookupStatus) return;
    gridLookupStatus.textContent = text || '';
    if (isError) {
      gridLookupStatus.classList.add('error');
    } else {
      gridLookupStatus.classList.remove('error');
    }
  }

  async function locateGridByInput() {
    if (!gridIdInput) return;
    if (gridLookupPending) return;
    const raw = (gridIdInput.value || '').trim();
    if (!raw) {
      setGridLookupStatus('请输入格网ID', true);
      return;
    }
    const gid = Number(raw);
    if (!Number.isInteger(gid) || gid <= 0) {
      setGridLookupStatus('格网ID需为正整数', true);
      return;
    }
    gridLookupPending = true;
    if (gridIdConfirmBtn) gridIdConfirmBtn.disabled = true;
    setGridLookupStatus('查询中...', false);
    try {
      let meta = metaById.get(gid);
      if (!meta) {
        try {
          const resp = await fetch(`/api/meta/one?grid_id=${gid}`);
          if (!resp.ok) {
            if (resp.status === 404) {
              setGridLookupStatus('未找到指定格网', true);
              return;
            }
            const detail = await resp.text();
            throw new Error(detail || 'lookup failed');
          }
          meta = await resp.json();
          if (meta && typeof meta.grid_id !== 'undefined') {
            metaById.set(meta.grid_id, meta);
          }
        } catch (e) {
          console.error(e);
          setGridLookupStatus('查询失败，请稍后重试', true);
          return;
        }
      }
      await selectGrid(gid);
      setGridLookupStatus(`已展示格网 ${gid}`, false);
    } catch (e) {
      console.error(e);
      setGridLookupStatus('查询失败，请稍后重试', true);
    } finally {
      gridLookupPending = false;
      if (gridIdConfirmBtn) gridIdConfirmBtn.disabled = false;
    }
  }

  async function selectGrid(gid) {
    currentGridId = gid;
    updateCurrentInfo(gid);
    renderPoints();
    try {
      let flowData;
      const yearsMulti = getSelectedYears();
      const dir = directionSelect.value;
      const cov = Math.max(0, Math.min(100, +(covInput && covInput.value || 0))) / 100.0;
      if (labelMode || yearsMulti.length > 0) {
        flowData = await fetchJSON(`/api/flows?grid_id=${gid}&year=all&direction=${dir}&topk=${+topkInput.value||100}&cov=${cov}`);
        flowData._yearsFilter = yearsMulti; // pass to renderer for filtering
      } else {
        const y = +yearSelect.value;
        flowData = await fetchJSON(`/api/flows?grid_id=${gid}&year=${y}&direction=${dir}&topk=${+topkInput.value||100}&cov=${cov}`);
      }
      renderFlows(flowData);
      // ensure ellipse reflects current selection immediately
      try { renderEllipses(); } catch(e) { console.warn('renderEllipses failed', e); }
    } catch (e) {
      console.error(e);
    }
    try {
      const series = await fetchJSON(`/api/hourly?grid_id=${gid}`);
      window._lastSeriesData = series;
      drawChart(series);
    } catch (e) {
      console.error(e);
    }
  }

  function nearestGridAt(sx, sy, maxDeg=0.02) {
    const [lon, lat] = screenToWorld(sx, sy);
    if (!quadtree) return null;
    const found = quadtree.find(lon, lat, maxDeg);
    return found ? found.grid_id : null;
  }

  function setupZoom() {
    const zoom = d3.zoom().scaleExtent([0.8, 30]).on('zoom', (ev) => {
      transform = ev.transform;
      renderPoints();
      if (lastFlowData) renderFlows(lastFlowData);
      renderEllipses();
    });
    d3.select(mapCanvas).call(zoom);
  }

  function setupEvents() {
    window.addEventListener('resize', () => { resizeCanvas(); renderEllipses(); });
    clearBtn.addEventListener('click', ()=>{ currentGridId=null; updateCurrentInfo(null); renderPoints(); renderFlows(null); renderEllipses(); });
    mapCanvas.addEventListener('click', (ev) => {
      const rect = mapCanvas.getBoundingClientRect();
      const sx = ev.clientX - rect.left; const sy = ev.clientY - rect.top;
      const gid = nearestGridAt(sx, sy);
      if (gid) selectGrid(gid);
    });
    if (gridIdConfirmBtn) gridIdConfirmBtn.addEventListener('click', () => { locateGridByInput(); });
    if (gridIdInput) gridIdInput.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') locateGridByInput();
    });

    yearSelect.addEventListener('change', async () => { if (showHeat) { await refreshHeat(); renderPoints(); } if (currentGridId && !labelMode) selectGrid(currentGridId); });
    directionSelect.addEventListener('change', () => { if (currentGridId && !labelMode) selectGrid(currentGridId); });
    topkInput.addEventListener('change', () => { if (currentGridId) selectGrid(currentGridId); });
    ;[yearChk2018, yearChk2021, yearChk2024].forEach(c=> c.addEventListener('change', ()=>{ if (currentGridId) selectGrid(currentGridId); }));

    citySelect.addEventListener('change', async () => {
      const c = citySelect.value;
      if (!c) {
        metadata = []; metaById = new Map(); filtered = [];
        buildQuadtree(); renderPoints(); renderFlows(null); updateFilterInfo();
        return;
      }
      // lazy fetch metadata for selected city
      try {
        const data = await fetchJSON(`/api/metadata?city_name=${encodeURIComponent(c)}`);
        metadata = data || [];
        metaById = new Map(metadata.map(d => [d.grid_id, d]));
        // rebuild areas from city data
        const aset = new Set((metadata||[]).map(m => m.area_name).filter(Boolean));
        const areas = [...aset].sort();
        areaSelect.innerHTML = '<option value="">全部</option>' + areas.map(a=>`<option value="${a}">${a}</option>`).join('');
        filtered = metadata;
        computeScales(); buildHalfLonCache(); buildQuadtree(); resizeCanvas(); updateFilterInfo();
        if (showHeat) { await refreshHeat(); renderPoints(); }
      } catch (e) { console.error(e); }
    });
    areaSelect.addEventListener('change', async () => { applyFilters(); if (showHeat) { await refreshHeat(); renderPoints(); }});
    keywordInput.addEventListener('input', debounce(async () => { applyFilters(); if (showHeat) { await refreshHeat(); renderPoints(); } }, 300));
    resetFilterBtn.addEventListener('click', async () => { citySelect.value = ''; areaSelect.innerHTML = '<option value="">全部</option>'; keywordInput.value=''; applyFilters(); if (showHeat) { await refreshHeat(); renderPoints(); } });

    [metricTotal, metricOut, metricIn, modeOverlay, modeSplit, modeDailyAvg, modeWeekly].forEach(el => el && el.addEventListener('change', () => { if (currentGridId) selectGrid(currentGridId); }));

    // Label controls
    startLabelBtn.addEventListener('click', async () => {
      const n = Math.max(1, +labelCountInput.value || 1);
      await startLabeling(n);
    });
    skipBtn.addEventListener('click', async () => { await doSkip(); });
    undoBtn.addEventListener('click', async () => {
      if (isBusy) return;
      isBusy = true; setControlsEnabled(false);
      try {
        await fetchJSON('/api/label/undo', {method: 'POST'});
        let back;
        try {
          back = await fetchJSON('/api/label_queue/back', {method: 'POST'});
          queueIndex = back.index;
        } catch (e) {
          // fallback for older server without /back: manually set index-1
          const q = await fetchJSON('/api/label_queue');
          const prev = Math.max(0, (q.index||0) - 1);
          const set = await fetchJSON('/api/label_queue/set', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({index: prev})});
          queueIndex = set.index;
          back = {current: set.current, index: set.index};
        }
        await refreshQueueStatus('已撤销上一条');
        await refreshLabelStats();
        const gid = (back && back.current) ? back.current : ((queueAll && queueAll[queueIndex]) ? queueAll[queueIndex] : null);
        if (gid != null) await selectGrid(gid);
      } catch (e) { console.error(e); }
      finally { isBusy = false; setControlsEnabled(true); }
    });
    resetQueueBtn.addEventListener('click', async () => { try { await fetchJSON('/api/label_queue/reset', {method: 'POST'}); labelMode=false; queueAll=[]; queueIndex=0; updateStatus('队列已清空'); } catch(e){} });
    if (clearLabelsBtn) clearLabelsBtn.addEventListener('click', async () => {
      if (!confirm('确认清空标注CSV？系统会自动备份为 labels_backup_时间戳.csv')) return;
      if (isBusy) return; isBusy = true; setControlsEnabled(false);
      try { await fetchJSON('/api/labels/clear', {method:'POST'}); updateStatus('已清空标注CSV'); await refreshLabelStats(); } catch (e) { console.error(e); }
      finally { isBusy = false; setControlsEnabled(true); }
    });
    labelButtons.forEach(btn => btn.addEventListener('click', () => {
      const l = +btn.getAttribute('data-l');
      if (currentGridId != null) submitLabel(currentGridId, l);
    }));
    window.addEventListener('keydown', (e) => {
      if (!labelMode) return;
      const k = e.key;
      // 支持 1-9 的快捷键
      if (k >= '1' && k <= '9' && currentGridId != null) submitLabel(currentGridId, +k);
      if (k === 'Enter') doSkip();
    });

    importBtn.addEventListener('click', importLabels);
    // overlay
    openOverlayBtn.addEventListener('click', ()=> controlOverlay.classList.remove('hidden'));
    closeOverlayBtn.addEventListener('click', ()=> controlOverlay.classList.add('hidden'));
    if (heatToggle) heatToggle.addEventListener('change', async ()=> { showHeat = heatToggle.checked; if (showHeat) { await refreshHeat(); } renderPoints(); });
    if (outlineToggle) outlineToggle.addEventListener('change', async ()=>{ showOutline = outlineToggle.checked; if (showOutline && !cityBoundsData) { await refreshBounds(); } renderPoints(); });
    if (covInput) covInput.addEventListener('change', ()=> { if (currentGridId) selectGrid(currentGridId); });
    // color controls
    const bgColorInput = document.getElementById('bgColorInput');
    const gridColorInput = document.getElementById('gridColorInput');
    const color2018 = document.getElementById('color2018');
    const color2021 = document.getElementById('color2021');
    const color2024 = document.getElementById('color2024');
    const wrap = document.getElementById('canvasWrap');
    if (wrap && bgColorInput) { wrap.style.background = bgColorInput.value; bgColorInput.addEventListener('input', ()=> { wrap.style.background = bgColorInput.value; }); }
    if (gridColorInput) { gridColor = gridColorInput.value || gridColor; gridColorInput.addEventListener('input', ()=> { gridColor = gridColorInput.value || gridColor; renderPoints(); }); }
    function updateYearColors(){ yearColors={2018: color2018?.value||DEFAULT_YEAR_COLORS[2018], 2021: color2021?.value||DEFAULT_YEAR_COLORS[2021], 2024: color2024?.value||DEFAULT_YEAR_COLORS[2024]}; if (lastFlowData) renderFlows(lastFlowData); try { if (window._lastSeriesData) drawChart(window._lastSeriesData); } catch(e){} }
    if (color2018 && color2021 && color2024) {
      color2018.addEventListener('input', updateYearColors);
      color2021.addEventListener('input', updateYearColors);
      color2024.addEventListener('input', updateYearColors);
      updateYearColors();
    }
    if (otherLabelBtn && otherSubmitBtn && otherRemark) {
      otherLabelBtn.addEventListener('click', ()=> { otherRemark.focus(); });
      otherSubmitBtn.addEventListener('click', async ()=>{
        const txt = otherRemark.value.trim();
        if (!currentGridId) return;
        try {
          await fetchJSON('/api/label', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({grid_id: currentGridId, label: 0, remark: txt})});
          const adv = await fetchJSON('/api/label_queue/advance', {method: 'POST'});
          queueIndex = adv.index;
          setStatus(`进度：${Math.min(queueIndex, queueAll.length)}/${queueAll.length}，最近标注：${currentGridId} -> 其他（${txt||'无备注'}）`);
          otherRemark.value='';
          await refreshLabelStats();
          await nextCandidate();
        } catch (e) { console.error(e); }
      });
    }
  }

  // --- Confidence ellipses drawing ---
  const showEllipseCheckbox = document.getElementById('showEllipse');
  function metersToPixelsAt(lat, dx_m, dy_m) {
    const m_per_deg_lat = 111320.0;
    const m_per_deg_lon = 111320.0 * Math.cos(lat * Math.PI/180);
    const dlon = dx_m / m_per_deg_lon;
    const dlat = dy_m / m_per_deg_lat;
    const p0 = worldToScreen(0, 0);
    const p1 = worldToScreen(dlon, dlat);
    return [p1[0] - p0[0], p1[1] - p0[1]];
  }

  function currentYearsFilter() {
    if (lastFlowData && Array.isArray(lastFlowData._yearsFilter)) return lastFlowData._yearsFilter;
    return YEARS;
  }

  function renderEllipses() {
    if (!ellipsesData) return;
    if (!showEllipseCheckbox || !showEllipseCheckbox.checked) return;
    if (!currentGridId) return;
    const ctx = flowCanvas.getContext('2d');
    const yrsFilter = Array.isArray(currentYearsFilter()) && currentYearsFilter().length ? currentYearsFilter() : YEARS;
    const k = Math.max(0.8, transform.k || 1);
    ctx.save();
    ctx.globalAlpha = Math.min(0.65, 0.8/Math.sqrt(k));
    ctx.lineWidth = Math.max(1.6, 2.4/Math.sqrt(k));
    for (const y of yrsFilter) {
      const arr = (ellipsesData.years && ellipsesData.years[y]) || [];
      const it = arr.find(e => e.grid_id === currentGridId);
      if (!it) continue;
      // color by year, reuse current yearColors mapping
      const c = d3.color(yearColors[y] || DEFAULT_YEAR_COLORS[y] || '#f59e0b');
      const stroke = `rgba(${c.r},${c.g},${c.b},1)`;
      ctx.strokeStyle = stroke;
      const lon = it.center.lon, lat = it.center.lat;
      const p = worldToScreen(lon, lat);
      const a_m = (it.axes && it.axes.a) || 0;
      const b_m = (it.axes && it.axes.b) || 0;
      if (a_m <= 0 || b_m <= 0) continue;
      const [ax, _ay] = metersToPixelsAt(lat, a_m, 0);
      const [_bx, by] = metersToPixelsAt(lat, 0, b_m);
      const rx = Math.abs(ax);
      const ry = Math.abs(by);
      if (!isFinite(rx) || !isFinite(ry) || rx < 0.2 || ry < 0.2) continue;
      const angle = (it.angle_deg || 0) * Math.PI/180;
      ctx.beginPath();
      ctx.ellipse(p[0], p[1], rx, ry, angle, 0, Math.PI*2);
      // no fill for ellipse interior
      ctx.stroke();
    }
    ctx.restore();
  }

  // small helper to fetch with timeout (to avoid长时间无反馈)
  async function fetchJSONTimeout(url, opts={}, timeoutMs=25000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), Math.max(1000, timeoutMs));
    try {
      const r = await fetch(url, Object.assign({}, opts, {signal: ctrl.signal}));
      if (!r.ok) throw new Error(await r.text());
      return await r.json();
    } finally { clearTimeout(t); }
  }

  async function startLabeling(n) {
    if (isBusy) return; // avoid duplicate clicks
    isBusy = true; setControlsEnabled(false);
    labelMode = true;
    targetCount = n;
    try {
      // start server-side queue
      const filters = (labelWithinFilter && labelWithinFilter.checked) ? {city_name: citySelect?.value || '', area_name: areaSelect?.value || '', keyword: keywordInput?.value || ''} : {};
      const payload = {count: n, ...filters};
      // optional low-value filter; pass current year for computing hourly daily-avg
      if (enableLowFilter && enableLowFilter.checked) {
        const pct = Math.max(0, Math.min(100, +(lowPctInput?.value ?? 0) || 0));
        const val = Math.max(0, +(lowValueInput?.value ?? 0) || 0);
        payload.low_pct = pct;
        payload.low_value = val;
        const fy = +(yearSelect?.value ?? 0) || null; if (fy) payload.filter_year = fy;
      }
      // 提示正在生成，避免用户误以为“没有反应”
      updateStatus('正在生成队列...');
      let q;
      try {
        q = await fetchJSONTimeout('/api/label_queue/start', {method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload)}, 30000);
      } catch (e) {
        // 如果服务器在线但该请求失败/超时，则自动降级为不启用低流量过滤再试一次
        try {
          // 快速探测后端是否在线
          await fetchJSONTimeout('/api/version', {}, 5000);
          // 后端在线 -> 低流量过滤可能导致耗时过长或失败，降级重试（移除低过滤参数）
          const downgraded = Object.assign({}, payload);
          delete downgraded.low_pct; delete downgraded.low_value; delete downgraded.filter_year;
          q = await fetchJSONTimeout('/api/label_queue/start', {method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(downgraded)}, 30000);
          lastStatusNote = '低流量过滤失败或超时，已自动降级为不启用低过滤';
        } catch (probeErr) {
          // 探测失败 -> 服务不可达
          throw e; // 交由外层处理（会提示 Failed to fetch）
        }
      }
      queueAll = q.queue || [];
      queueIndex = q.index || 0;
      skipBtn.disabled = false;
      updateStatus('按 1-9 快速打标签，Enter 跳过');
      await refreshLabelStats();
      if (queueAll.length === 0) {
        setStatus('没有可标注格网（可能已全部标完或过滤范围为空 / 过滤条件过严）');
        labelMode = false;
        return;
      }
      // Important UX fix: do not keep controls locked while we fetch flows/hourly for the first grid.
      // Enable controls immediately after queue is ready, then load the grid asynchronously.
      isBusy = false; setControlsEnabled(true);
      // Kick off grid selection without awaiting to avoid re-locking buttons during network calls.
      // selectGrid sets currentGridId immediately so users can start labeling as soon as it appears.
      selectGrid(queueAll[queueIndex]).catch(err => console.error(err));
      return; // already unlocked; let async selection proceed
    } catch (e) {
      console.error(e);
      labelMode = false;
      skipBtn.disabled = true;
      // 进一步区分服务器不可达 vs 参数错误
      try {
        await fetchJSONTimeout('/api/version', {}, 4000);
        updateStatus('生成队列失败：请检查过滤阈值与数据是否可用');
        alert('生成队列失败：' + (e && e.message ? e.message : '未知错误'));
      } catch (_) {
        updateStatus('后端服务不可用，请确认 vis/server.py 正在运行');
        alert('生成队列失败：后端服务不可用或网络中断（' + (e && e.message ? e.message : '未知错误') + '）');
      }
    } finally {
      // If an error occurred before we unlocked in the success path, ensure controls are restored.
      if (isBusy) { isBusy = false; setControlsEnabled(true); }
    }
  }

  async function captureAndUpload(gid, label) {
    try {
      // capture area: include map and right-side panels via top-level container
      const root = document.querySelector('.app');
      if (!root || !window.html2canvas) return;
      const scale = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
      const canvas = await html2canvas(root, {backgroundColor: '#ffffff', scale});
      const dataUrl = canvas.toDataURL('image/jpeg', 0.92);
      const fname = `${gid}-${label}.jpg`;
      await fetch('/api/screenshot', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({filename: fname, data: dataUrl})});
    } catch (e) {
      console.warn('screenshot failed', e);
    }
  }

  async function submitLabel(gid, label) {
    if (isBusy) return;
    isBusy = true; setControlsEnabled(false);
    try {
      await fetchJSON('/api/label', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({grid_id: gid, label})});
      // Take screenshot before advancing so UI reflects this grid
      await captureAndUpload(gid, label);
      const adv = await fetchJSON('/api/label_queue/advance', {method: 'POST'});
      queueIndex = adv.index;
      await refreshQueueStatus(`最近标注：${gid} -> ${LABEL_MAP[label]}`);
      await refreshLabelStats();
      await nextCandidate();
    } catch (e) {
      console.error(e);
    } finally { isBusy = false; setControlsEnabled(true); }
  }

  async function nextCandidate() {
    if (!labelMode) return;
    if (queueIndex >= queueAll.length) {
      labelMode = false;
      skipBtn.disabled = true;
      setStatus(`完成，本次共标注 ${queueAll.length} 个。可继续开始新一轮。`);
      return;
    }
    await selectGrid(queueAll[queueIndex]);
  }

  async function doSkip() {
    if (isBusy) return;
    isBusy = true; setControlsEnabled(false);
    try {
      const adv = await fetchJSON('/api/label_queue/advance', {method: 'POST'});
      queueIndex = adv.index;
      await refreshQueueStatus('已跳过');
      await nextCandidate();
    } catch (e) { console.error(e); }
    finally { isBusy = false; setControlsEnabled(true); }
  }

  async function resumeQueueIfAny() {
    const q = await fetchJSON('/api/label_queue');
    if ((q.queue||[]).length && q.index < q.queue.length) {
      labelMode = true; queueAll = q.queue; queueIndex = q.index; targetCount = q.queue.length; skipBtn.disabled = false;
      updateStatus('继续上次');
      await selectGrid(queueAll[queueIndex]);
    }
  }

  function applyFilters() {
    const c = citySelect.value || '';
    const a = areaSelect.value || '';
    const kw = (keywordInput.value || '').toLowerCase();
    filtered = metadata.filter(m => {
      if (c && m.city_name !== c) return false;
      if (a && m.area_name !== a) return false;
      if (kw) {
        const text = `${m.city_name||''} ${m.area_name||''}`.toLowerCase();
        if (!text.includes(kw)) return false;
      }
      return true;
    });
    buildQuadtree();
    renderPoints();
    renderFlows(null);
    updateFilterInfo();
  }

  function updateFilterInfo() {
    filterInfo.textContent = `显示格网：${filtered.length.toLocaleString()}`;
  }

  async function refreshHeat() {
    const y = +yearSelect.value;
    const params = new URLSearchParams();
    params.set('year', y);
    params.set('metric', 'total');
    const c = citySelect.value || '';
    const a = areaSelect.value || '';
    if (c) params.set('city_name', c);
    if (a) params.set('area_name', a);
    try {
      const rsp = await fetchJSON(`/api/heat?${params.toString()}`);
      heatById = new Map((rsp.values || []).map(o => [o.grid_id, o.v]));
      heatStats = {q95: rsp.q95 || 0, max: rsp.max || 0};
    } catch (e) {
      console.error(e);
      heatById = new Map(); heatStats = {q95:0, max:0};
    }
  }

  // Draw chart for either weekly (3 lines per year) or 3-week daily average (1 line per year)
  function drawChart(seriesByYear) {
    chartEl.innerHTML = '';
    const width = chartEl.clientWidth || 360;
    const selMetrics = [];
    if (metricTotal.checked) selMetrics.push('total');
    if (metricOut.checked) selMetrics.push('out');
    if (metricIn.checked) selMetrics.push('in');
    if (selMetrics.length === 0) selMetrics.push('total');
    const split = modeSplit && modeSplit.checked;
    const weekly = modeWeekly && modeWeekly.checked;
    const chartFixY = document.getElementById('chartFixY');
    const chartYMaxInput = document.getElementById('chartYMaxInput');
    const panelCount = split ? selMetrics.length : 1;
    const height = (weekly ? 300 : 240) * panelCount;
    const margin = {top: 10, right: 10, bottom: 30, left: 56};
    const svg = d3.create('svg').attr('width', width).attr('height', height);
    chartEl.appendChild(svg.node());
    const innerW = width - margin.left - margin.right;
    const panelH = (height - margin.top - margin.bottom) / panelCount;
    const yearsToPlot = (function(){ const sel = getSelectedYears(); return sel.length?sel:YEARS; })();

    function maxForMetric(metric) {
      let maxY = 0;
      for (const y of yearsToPlot) {
        const s = seriesByYear[y]; if (!s) continue;
        if (weekly) {
          for (let wi=0; wi<1; wi++) {
            const arr = (s[metric] && s[metric][wi]) || [];
            const m = d3.max(arr) || 0; if (m>maxY) maxY=m;
          }
        } else {
          const weeks = (s[metric]||[]).slice(0,1);
          const avg = d3.range(24).map(h => {
            let sum=0; for (let wi=0; wi<1; wi++){ const arr=(weeks[wi]||[]); sum += (arr[h]||0); } return sum/7; // 7天*1周
          });
          const m = d3.max(avg) || 0; if (m>maxY) maxY=m;
        }
      }
      return maxY <= 0 ? 1 : maxY;
    }

    const sharedMax = split ? null : d3.max(selMetrics.map(m => maxForMetric(m)));

    const metricZh = {total: '总量', out: '流出', in: '流入'};
    selMetrics.forEach((metric, mi) => {
      const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top + mi * panelH})`);
      const innerH = panelH - (split ? 18 : 0);
      const x = d3.scaleLinear().domain([0,23]).range([0, innerW]);
      let ymax = split ? maxForMetric(metric) : sharedMax;
      const fixed = chartFixY && chartFixY.checked;
      const userMax = Math.max(1, +(chartYMaxInput && chartYMaxInput.value || 0) || 200);
      if (fixed) ymax = userMax;
      const y = d3.scaleLinear().domain([0, ymax]).nice().range([innerH, 0]);
      g.append('g').attr('transform', `translate(0,${innerH})`).call(d3.axisBottom(x).ticks(12).tickFormat(d=>d+''));
      g.append('g').call(d3.axisLeft(y).ticks(5));
      // y-axis unit label
      const unit = weekly ? '人次/小时（周）' : '人次/小时（周日均）';
      const mlabel = metricZh[metric] || metric;
      g.append('text')
        .attr('transform', `translate(${-36},${innerH/2}) rotate(-90)`) 
        .attr('text-anchor','middle')
        .attr('fill','#666')
        .attr('font-size', 12)
        .text(`${mlabel} ${unit}`);
      const line = d3.line().x((d,i)=>x(i)).y(d=>y(d));
      for (const year of yearsToPlot) {
        const s = seriesByYear[year]; if (!s) continue;
        const color = d3.color(yearColors[year] || DEFAULT_YEAR_COLORS[year]);
        if (weekly) {
          for (let wi=0; wi<1; wi++) {
            const arr = (s[metric] && s[metric][wi]) || [];
            const alpha = [0.9][wi];
            const stroke = `rgba(${color.r},${color.g},${color.b},${alpha})`;
            const path = g.append('path').datum(arr).attr('fill','none').attr('stroke',stroke).attr('stroke-width',1.8).attr('d',line);
            if (metric==='in') path.attr('stroke-dasharray','4,3');
            if (metric==='out') path.attr('stroke-dasharray','2,2');
          }
        } else {
          const weeks = (s[metric]||[]).slice(0,1);
          const avg = d3.range(24).map(h => { let sum=0; for (let wi=0; wi<1; wi++){ const arr=(weeks[wi]||[]); sum += (arr[h]||0);} return sum/7; });
          const stroke = `rgb(${color.r},${color.g},${color.b})`;
          g.append('path').datum(avg).attr('fill','none').attr('stroke',stroke).attr('stroke-width',2.2).attr('d', line);
        }
      }
      if (split) {
        g.append('text').attr('x',4).attr('y',12).attr('font-size',12).attr('fill','#666').text(metric);
      }
    });

    // Legend
    const legend = svg.append('g').attr('transform', `translate(${margin.left},${height - 6})`);
    let lx=0; for (const year of yearsToPlot) { const color = yearColors[year] || DEFAULT_YEAR_COLORS[year]; legend.append('line').attr('x1',lx).attr('x2',lx+18).attr('y1',-10).attr('y2',-10).attr('stroke',color).attr('stroke-width',3); legend.append('text').attr('x',lx+22).attr('y',-6).attr('font-size',12).text(year); lx+=70; }
  }

  function getSelectedYears() {
    const yrs = [];
    if (yearChk2018.checked) yrs.push(2018);
    if (yearChk2021.checked) yrs.push(2021);
    if (yearChk2024.checked) yrs.push(2024);
    return yrs;
  }

  async function importLabels() {
    const file = importFile.files && importFile.files[0];
    if (!file) { importInfo.textContent = '请选择CSV文件'; return; }
    const mode = importMode.value;
    const fd = new FormData();
    fd.append('file', file);
    try {
      const res = await fetch(`/api/labels/import?mode=${encodeURIComponent(mode)}`, {method: 'POST', body: fd});
      if (!res.ok) throw new Error(await res.text());
      const j = await res.json();
      importInfo.textContent = `导入成功：${j.imported}`;
      await refreshLabelStats();
    } catch (e) {
      importInfo.textContent = '导入失败';
      console.error(e);
    }
  }

  // boot
  (async function init(){
    await loadMetadata();
    await loadEllipses();
    setupZoom();
    setupEvents();
    // re-render chart when y-axis controls change
    const chartFixY = document.getElementById('chartFixY');
    const chartYMaxInput = document.getElementById('chartYMaxInput');
    if (chartFixY) chartFixY.addEventListener('change', ()=>{ try { if (window._lastSeriesData) drawChart(window._lastSeriesData); } catch(e){} });
    if (chartYMaxInput) chartYMaxInput.addEventListener('change', ()=>{ try { if (window._lastSeriesData) drawChart(window._lastSeriesData); } catch(e){} });
    renderEllipses();
    await refreshLabelStats();
  })();
})();
