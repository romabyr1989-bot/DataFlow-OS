/* DataFlow OS — frontend application */
'use strict';

/* ═══════════════════════════════════════════════════
   STATE
═══════════════════════════════════════════════════ */
const API = '';
let ws = null, wsRetries = 0;
let metricsHistory = { rows: [], latency: [] };
let metricsTimer   = null;
let analyticsState = { tables: [], currentRows: [], currentCols: [], charts: [] };
let lastStatsReport = null;

/* Auth */
let jwtToken = sessionStorage.getItem('dfo_jwt') || null;
let isLoggedIn = !!jwtToken;

/* Pipeline builder */
const pb = { steps: [], editId: null, max_retries: 3, retry_delay_sec: 30, webhook_url: '', webhook_on: 'failure', alert_cooldown: 300 };

/* Ingest */
let ingestContent  = null;
let ingestColumns  = [];
let ingestDelimiter = ',';
let ingestMode = 'csv'; // 'csv' | 'parquet'
let ingestFile = null;  // raw File object (for parquet binary upload)

/* User preferences */
let prefs = {};


/* ═══════════════════════════════════════════════════
   PREFERENCES
═══════════════════════════════════════════════════ */
function loadPrefs() {
  try { prefs = JSON.parse(localStorage.getItem('dfo_prefs') || '{}'); } catch (_) { prefs = {}; }
  prefs.autoRefresh      = prefs.autoRefresh      ?? true;
  prefs.refreshInterval  = prefs.refreshInterval  ?? 30;
  prefs.rowLimit         = prefs.rowLimit          ?? 100;
  prefs.compact          = prefs.compact           ?? false;
}

function savePref(key, val) {
  prefs[key] = val;
  localStorage.setItem('dfo_prefs', JSON.stringify(prefs));
}

function applyPrefs() {
  document.body.classList.toggle('compact', !!prefs.compact);
}

function applyPrefsToSettingsForm() {
  const el = (id) => document.getElementById(id);
  el('pref-auto-refresh').checked   = prefs.autoRefresh;
  el('pref-refresh-interval').value = prefs.refreshInterval;
  el('pref-compact').checked        = prefs.compact;
  const rl = el('pref-row-limit');
  if (rl) for (const o of rl.options) o.selected = (+o.value === prefs.rowLimit);
}


/* ═══════════════════════════════════════════════════
   NAVIGATION
═══════════════════════════════════════════════════ */
document.querySelectorAll('.nav-item').forEach(a => {
  a.addEventListener('click', e => { e.preventDefault(); switchView(a.dataset.view); });
});

const VALID_VIEWS = new Set(['query','pipelines','builder','ingest','analytics','matviews','security','metrics','settings']);

function switchView(name, { pushState = true } = {}) {
  if (!VALID_VIEWS.has(name)) name = 'ingest';

  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  const navEl = document.querySelector(`[data-view="${name}"]`);
  if (navEl) navEl.classList.add('active');
  const viewEl = document.getElementById('view-' + name);
  if (viewEl) viewEl.classList.add('active');

  /* hide builder nav item when not editing */
  const nb = document.getElementById('nav-builder');
  if (name !== 'builder') nb.classList.remove('visible');

  if (name === 'query')     { loadQuerySidebar(); }
  if (name === 'pipelines') loadPipelines();
  if (name === 'analytics') loadAnalyticsModule();
  if (name === 'matviews')  loadMatviews();
  if (name === 'security')  { loadRbacPolicies(); }
  if (name === 'metrics')   loadMetrics();
  if (name === 'settings')  loadSettings();

  if (pushState && location.hash !== '#' + name)
    history.pushState({ view: name }, '', '#' + name);
}

window.addEventListener('popstate', e => {
  const view = (e.state?.view) || location.hash.replace('#', '') || 'ingest';
  switchView(view, { pushState: false });
});


/* ═══════════════════════════════════════════════════
   WEBSOCKET
═══════════════════════════════════════════════════ */
function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const host  = location.hostname || 'localhost';
  const port  = location.port     || '8080';
  ws = new WebSocket(`${proto}://${host}:${port}/ws`);

  ws.onopen = () => {
    wsRetries = 0;
    setBadge('ok', 'подключено');
    logActivity('WebSocket подключён');
  };
  ws.onclose = () => {
    setBadge('warn', 'переподключение…');
    const delay = Math.min(1000 * Math.pow(1.5, wsRetries++), 15000);
    setTimeout(connectWS, delay);
  };
  ws.onerror = () => setBadge('err', 'ошибка ws');
  ws.onmessage = e => {
    try { handleWsEvent(JSON.parse(e.data)); } catch (_) {}
  };
}

function handleWsEvent(msg) {
  const ev = msg.event || msg.type || '';
  logActivity(ev + (msg.table ? ` — ${msg.table}` : '') + (msg.id ? ` — ${msg.id}` : ''));
  if (ev === 'table_updated')        { loadQuerySidebar(); }
  if (ev === 'pipeline_created')     { loadPipelines(); }
  if (ev === 'pipeline_run_started') { loadPipelines(); }
  if (ev === 'pipeline_triggered')   { loadPipelines(); }
}

function setBadge(cls, text) {
  const el = document.getElementById('ws-status');
  el.className   = 'badge badge-' + cls;
  el.textContent = text;
}


/* ═══════════════════════════════════════════════════
   TOAST NOTIFICATIONS
═══════════════════════════════════════════════════ */
function showToast(msg, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  const icons = { ok: '✓', error: '✕', warn: '⚠', info: 'ℹ' };
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `<span class="toast-icon">${icons[type] || ''}</span>${escHtml(msg)}`;
  container.appendChild(toast);
  requestAnimationFrame(() => requestAnimationFrame(() => toast.classList.add('show')));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}


/* ═══════════════════════════════════════════════════
   ACTIVITY LOG
═══════════════════════════════════════════════════ */
function logActivity(msg) {
  const box = document.getElementById('activity-log');
  if (!box) return;
  if (box.textContent === 'Событий пока нет.') box.innerHTML = '';
  const row = document.createElement('div');
  row.className = 'log-entry';
  row.innerHTML = `<span class="log-time">${new Date().toLocaleTimeString('ru')}</span><span class="log-msg">${escHtml(msg)}</span>`;
  box.prepend(row);
  while (box.children.length > 100) box.removeChild(box.lastChild);
}


/* ═══════════════════════════════════════════════════
   TABLES + QUERY
═══════════════════════════════════════════════════ */
async function loadQuerySidebar() {
  const list = document.getElementById('query-table-list');
  if (!list) return;
  list.innerHTML = '<div class="empty-state" style="padding:1rem;color:var(--muted)">Загрузка…</div>';
  try {
    const tables = await apiFetch('/api/tables');
    if (!tables) return;
    list.innerHTML = '';
    if (!tables.length) {
      list.innerHTML = '<div class="empty-state">Таблиц пока нет.<br>Перейдите в раздел <strong>Загрузка</strong>, чтобы импортировать CSV.</div>';
      return;
    }
    const ingested = tables.filter(t => t.source !== 'pipeline');
    const pipeline = tables.filter(t => t.source === 'pipeline');

    function renderGroup(label, items) {
      if (!items.length) return;
      const hdr = document.createElement('div');
      hdr.className = 'tables-group-header';
      hdr.textContent = label;
      list.appendChild(hdr);
      const grid = document.createElement('div');
      grid.className = 'tables-group-grid';
      items.forEach(t => grid.appendChild(makeTableCard(t)));
      list.appendChild(grid);
    }

    renderGroup('Загруженные данные', ingested);
    renderGroup('Созданные конвейером', pipeline);
  } catch (err) {
    list.innerHTML = `<div style="color:var(--red)">Error: ${escHtml(String(err))}</div>`;
  }
}

function makeTableCard(t) {
  const card = document.createElement('div');
  card.className = 'table-card';
  const cols = (t.columns || []).map(c => {
    const cls = c.type === 'int64' ? 'int' : c.type === 'double' ? 'double' : c.type === 'bool' ? 'bool' : '';
    return `<span class="col-pill ${cls}">${escHtml(c.name)}<em style="opacity:.5">:${c.type}</em></span>`;
  }).join('');
  const sourceBadge = t.source === 'pipeline'
    ? '<span class="source-badge pipeline">конвейер</span>'
    : t.source === 'parquet'
      ? '<span class="source-badge parquet">Parquet</span>'
      : '<span class="source-badge ingest">CSV</span>';
  card.innerHTML = `
    <div class="table-card-head">
      <h3>${escHtml(t.name)}${sourceBadge}</h3>
      <button class="btn btn-sm btn-danger" title="Удалить таблицу">Удалить</button>
    </div>
    <div class="meta">${fmtNum(t.rows || 0)} строк · ${(t.columns || []).length} столбцов <button class="btn btn-sm idx-btn" onclick="openIndexManager('${escAttr(t.name)}', event)" title="Индексы">⚡ Индексы</button></div>
    <div class="col-list">${cols || '<span class="col-pill">—</span>'}</div>
  `;
  const delBtn = card.querySelector('button');
  delBtn.addEventListener('click', (ev) => {
    ev.stopPropagation();
    deleteTable(t.name);
  });
  card.onclick = () => {
    document.getElementById('sql-input').value = `SELECT * FROM ${t.name} LIMIT ${prefs.rowLimit || 100}`;
    document.getElementById('query-editor-block')?.scrollIntoView({ behavior: 'smooth' });
    runQuery();
  };
  return card;
}

async function openIndexManager(tableName, ev) {
  ev.stopPropagation();
  const modal = document.getElementById('index-modal');
  document.getElementById('index-modal-title').textContent = `Индексы: ${tableName}`;
  document.getElementById('index-modal-table').value = tableName;
  document.getElementById('index-modal-col').value = '';
  document.getElementById('index-modal-status').textContent = '';
  modal.classList.remove('hidden');
  await loadTableIndexes(tableName);
}

async function loadTableIndexes(tableName) {
  const list = document.getElementById('index-modal-list');
  list.innerHTML = '<span style="color:var(--muted)">Загрузка…</span>';
  try {
    const data = await apiFetch(`/api/tables/${encodeURIComponent(tableName)}/indexes`);
    if (!data.length) { list.innerHTML = '<span style="color:var(--muted)">Индексов нет</span>'; return; }
    list.innerHTML = data.map(ix => `<span class="col-pill int">⚡ ${escHtml(ix.column)}</span>`).join(' ');
  } catch (_) { list.innerHTML = '<span style="color:var(--muted)">—</span>'; }
}

async function createIndex() {
  const table = document.getElementById('index-modal-table').value;
  const col   = document.getElementById('index-modal-col').value.trim();
  if (!col) { showToast('Укажите колонку', 'warn'); return; }
  const status = document.getElementById('index-modal-status');
  status.textContent = '…';
  try {
    await apiPost(`/api/tables/${encodeURIComponent(table)}/indexes`, { column: col });
    status.innerHTML = '<span style="color:var(--green)">Индекс создан</span>';
    await loadTableIndexes(table);
    showToast(`Индекс по ${col} создан`, 'ok');
  } catch (err) {
    status.innerHTML = `<span style="color:var(--red)">${escHtml(String(err))}</span>`;
  }
}

async function deleteTable(name) {
  if (!confirm(`Удалить таблицу "${name}"? Это действие нельзя отменить.`)) return;
  try {
    await apiFetch(`/api/tables/${encodeURIComponent(name)}`, 'DELETE');
    showToast(`Таблица "${name}" удалена`, 'ok');
    logActivity(`удалена таблица ${name}`);
    await loadQuerySidebar();
  } catch (err) {
    showToast(`Ошибка удаления: ${err.message || err}`, 'err');
  }
}

function queryTable(name) {
  document.getElementById('sql-input').value = `SELECT * FROM ${name} LIMIT ${prefs.rowLimit || 100}`;
  runQuery();
}

function insertIntoQuery(name) {
  const ta  = document.getElementById('sql-input');
  const pos = ta.selectionStart;
  ta.value  = ta.value.slice(0, pos) + name + ta.value.slice(ta.selectionEnd);
  ta.focus();
  ta.setSelectionRange(pos + name.length, pos + name.length);
}

function clearQuery() {
  document.getElementById('sql-input').value = '';
  document.getElementById('query-result').innerHTML = '';
  document.getElementById('query-status').textContent = '';
}

async function runQuery() {
  const sql = document.getElementById('sql-input').value.trim();
  if (!sql) return;
  const status = document.getElementById('query-status');
  const result = document.getElementById('query-result');
  status.textContent = 'Выполняется…';
  result.innerHTML   = '';
  const t0 = performance.now();
  try {
    const data = await apiPost('/api/tables/query', { sql });
    const ms   = (performance.now() - t0).toFixed(1);
    const cols = data.columns || [];
    const rows = data.rows || [];
    const dmlKey = cols.length === 1 && rows.length === 1 && (cols[0] === 'deleted' || cols[0] === 'updated') ? cols[0] : null;
    if (dmlKey) {
      const n = parseInt(rows[0][0] ?? rows[0][dmlKey] ?? 0, 10);
      const label = dmlKey === 'deleted' ? 'Удалено' : 'Обновлено';
      status.textContent = `${label}: ${n} · ${ms} мс`;
      result.appendChild(makeDmlBanner(dmlKey, n));
    } else {
      status.textContent = `${rows.length} ${pluralRows(rows.length)} · ${ms} мс`;
      result.appendChild(makeResultTable(data));
    }
  } catch (err) {
    status.textContent = 'Ошибка';
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/,'')); if (j.error) msg = j.error; } catch(_) {}
    result.innerHTML = `<div style="color:var(--red);padding:.5rem;font-family:monospace">${escHtml(msg)}</div>`;
  }
}

function makeDmlBanner(op, count) {
  const d = document.createElement('div');
  d.className = 'dml-banner';
  const icon = op === 'deleted' ? '🗑' : '✏';
  const verb = op === 'deleted' ? 'Удалено' : 'Обновлено';
  const rowWord = count === 1 ? 'строка' : count >= 2 && count <= 4 ? 'строки' : 'строк';
  d.innerHTML = `<span class="dml-icon">${icon}</span>${verb}: <span class="dml-count">${count}</span>&nbsp;${rowWord}`;
  return d;
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('sql-input')?.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runQuery(); }
  });
  document.getElementById('an-view-sql')?.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runViewQuery(); }
  });
  document.getElementById('an-sql')?.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runAnalyticsQuery(); }
  });
}, false);


function makeResultTable(data) {
  const cols = data.columns || [];
  const rows = data.rows    || [];
  if (!cols.length && !rows.length) {
    const d = document.createElement('div');
    d.className = 'result-empty'; d.textContent = 'Нет результатов.'; return d;
  }
  const wrap  = document.createElement('div'); wrap.className = 'result-table-wrap';
  const tbl   = document.createElement('table'); tbl.className = 'result-table';
  const thead = tbl.createTHead(); const hrow = thead.insertRow();
  cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; hrow.appendChild(th); });
  const tbody = tbl.createTBody();
  rows.forEach(row => {
    const tr   = tbody.insertRow();
    const vals = Array.isArray(row) ? row : cols.map(c => row[c]);
    vals.forEach(v => {
      const td = tr.insertCell();
      if (v === null || v === undefined) td.innerHTML = '<span class="null-val">NULL</span>';
      else td.textContent = String(v);
    });
  });
  wrap.appendChild(tbl);
  return wrap;
}


/* ═══════════════════════════════════════════════════
   PIPELINE LIST
═══════════════════════════════════════════════════ */
async function loadPipelines() {
  const list = document.getElementById('pipelines-list');
  list.innerHTML = '<div style="color:var(--muted);padding:.5rem">Загрузка…</div>';
  try {
    const pipelines = await apiFetch('/api/pipelines');
    if (!pipelines) return;
    if (!pipelines.length) {
      list.innerHTML = `
        <div class="empty-state">
          Конвейеров пока нет.<br>
          <button class="btn btn-primary" style="margin-top:.75rem" onclick="openPipelineBuilder(null)">
            + Создать первый конвейер
          </button>
        </div>`;
      return;
    }
    list.innerHTML = '';
    pipelines.forEach(p => list.appendChild(makePipelineRow(p)));
  } catch (err) {
    list.innerHTML = `<div style="color:var(--red)">Error: ${escHtml(String(err))}</div>`;
  }
}

const STATUS_LABELS = ['ожидание','выполняется','успех','ошибка','отменён'];
const STATUS_BADGE  = ['badge-warn','badge-run','badge-ok','badge-err','badge-warn'];

const _pipelineCache = {};

function makePipelineRow(p) {
  _pipelineCache[p.id] = p;
  const div    = document.createElement('div');
  div.className = 'pipeline-item pipeline-item-clickable';
  div.onclick = (e) => {
    if (e.target.closest('button')) return;
    openPipelineBuilder(_pipelineCache[p.id]);
  };
  const status = p.status || 0;
  const steps  = (p.steps || []).map(s =>
    `<span class="step-badge">${escHtml(s.name || s.id || 'step')}</span>`).join('');
  const nextRunStr = p.next_run && p.next_run > 0 && p.next_run < 9e15
    ? new Date(p.next_run * 1000).toLocaleString() : '—';
  div.innerHTML = `
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(p.name || p.id)}</span>
        <span class="badge ${STATUS_BADGE[status]}">${STATUS_LABELS[status]}</span>
        ${!p.enabled ? '<span class="badge badge-warn">отключён</span>' : ''}
      </div>
      <div class="pipeline-meta">
        расписание: <code>${escHtml(p.cron || 'вручную')}</code>
        &nbsp;·&nbsp; последний запуск: ${p.last_run ? new Date(p.last_run * 1000).toLocaleString() : '—'}
        &nbsp;·&nbsp; следующий: ${nextRunStr}
      </div>
      <div class="step-list">${steps}</div>
    </div>
    <div class="pipeline-actions">
      <button class="btn btn-sm btn-primary" onclick="triggerPipeline('${escAttr(p.id)}')">▶ Запустить</button>
      <button class="btn btn-sm" onclick="showPipelineRuns('${escAttr(p.id)}','${escAttr(p.name||p.id)}')">История</button>
      <button class="btn btn-sm btn-danger" onclick="deletePipeline('${escAttr(p.id)}','${escAttr(p.name||p.id)}')">✕</button>
    </div>
  `;
  return div;
}

async function triggerPipeline(id) {
  try {
    await apiPost(`/api/pipelines/${id}/run`, {});
    showToast('Конвейер запущен', 'ok');
    logActivity(`запущен конвейер ${id}`);
    setTimeout(loadPipelines, 600);
  } catch (err) {
    showToast(String(err), 'error');
  }
}

async function deletePipeline(id, name) {
  if (!confirm(`Удалить конвейер "${name || id}"?`)) return;
  try {
    await apiFetch(`/api/pipelines/${id}`, 'DELETE');
    showToast(`Конвейер "${name || id}" удалён`, 'ok');
    loadPipelines();
  } catch (err) { showToast(String(err), 'error'); }
}

async function showPipelineRuns(id, name) {
  const modal = document.getElementById('runs-modal');
  const title = document.getElementById('runs-modal-title');
  const body  = document.getElementById('runs-modal-body');
  title.textContent = `История запусков — ${name || id}`;
  body.innerHTML = '<div style="color:var(--muted)">Загрузка…</div>';
  modal.classList.remove('hidden');
  try {
    const runs = await apiFetch(`/api/pipelines/${id}/runs`);
    const list = Array.isArray(runs) ? runs : [];
    if (!list.length) { body.innerHTML = '<div style="color:var(--muted);padding:.5rem">Запусков пока нет.</div>'; return; }
    let html = '<table class="runs-table"><thead><tr><th>#</th><th>Начало</th><th>Окончание</th><th>Статус</th><th>Повторы</th><th>Ошибка</th></tr></thead><tbody>';
    list.forEach(r => {
      const statusLabel = r.status === 0 ? '<span class="badge badge-ok">успех</span>'
        : '<span class="badge badge-err">ошибка</span>';
      html += `<tr>
        <td>${r.id}</td>
        <td>${r.started ? new Date(r.started*1000).toLocaleString() : '—'}</td>
        <td>${r.finished ? new Date(r.finished*1000).toLocaleString() : '—'}</td>
        <td>${statusLabel}</td>
        <td>${typeof r.retries === 'number' ? r.retries : 0}</td>
        <td>${escHtml(r.error || '')}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    body.innerHTML = html;
  } catch (err) { body.innerHTML = `<div style="color:var(--red)">${escHtml(String(err))}</div>`; }
}


/* ═══════════════════════════════════════════════════
   PIPELINE BUILDER
═══════════════════════════════════════════════════ */
const CRON_DESCRIPTIONS = {
  '@reboot':     'При запуске (однократно)',
  '@hourly':     'Каждый час',
  '@daily':      'Каждый день в полночь',
  '@weekly':     'Каждое воскресенье в полночь',
  '@monthly':    '1-е число каждого месяца в полночь',
  '@yearly':     '1 января в полночь',
  '* * * * *':   'Каждую минуту',
  '*/5 * * * *': 'Каждые 5 минут',
  '*/15 * * * *':'Каждые 15 минут',
  '*/30 * * * *':'Каждые 30 минут',
  '0 * * * *':   'Каждый час (ровно)',
  '0 0 * * *':   'Каждый день в полночь',
  '0 6 * * *':   'Каждый день в 6:00',
  '0 9 * * *':   'Каждый день в 9:00',
  '0 9 * * 1-5': 'По будням в 9:00',
  '0 0 * * 0':   'Каждое воскресенье',
  '0 0 1 * *':   '1-е число каждого месяца',
};

function cronDescription(expr) {
  return CRON_DESCRIPTIONS[expr] || (expr ? 'Произвольное расписание' : 'Без расписания — только вручную');
}

function openPipelineBuilder(pipeline) {
  pb.editId = pipeline ? (pipeline.id || null) : null;
  pb.steps  = pipeline ? (pipeline.steps || []).map(s => ({...s, deps: s.deps || []})) : [];
  pb.max_retries     = pipeline ? (pipeline.max_retries || 3) : 3;
  pb.retry_delay_sec = pipeline ? (pipeline.retry_delay_sec || 30) : 30;
  pb.webhook_url     = pipeline ? (pipeline.webhook_url || '') : '';
  pb.webhook_on      = pipeline ? (pipeline.webhook_on || 'failure') : 'failure';
  pb.alert_cooldown  = pipeline ? (pipeline.alert_cooldown || 300) : 300;

  document.getElementById('pb-name').value      = pipeline ? (pipeline.name || '') : '';
  document.getElementById('pb-enabled').checked = pipeline ? !!pipeline.enabled : true;
  document.getElementById('pb-cron').value       = pipeline ? (pipeline.cron || '') : '';
  document.getElementById('pb-max-retries').value = pb.max_retries;
  document.getElementById('pb-retry-delay').value = pb.retry_delay_sec;
  document.getElementById('pb-webhook-url').value = pb.webhook_url;
  document.getElementById('pb-webhook-on').value = pb.webhook_on;
  document.getElementById('pb-alert-cooldown').value = pb.alert_cooldown;
  document.getElementById('pb-cron-preset').value = '';
  syncCronPreset();

  const title = document.getElementById('builder-title');
  title.textContent = pb.editId ? `Редактирование — ${pipeline.name || pb.editId}` : 'Новый конвейер';

  /* show builder nav */
  const nb = document.getElementById('nav-builder');
  nb.classList.add('visible');

  renderBuilderSteps();
  switchView('builder');
}

/* listen to enabled toggle */
document.getElementById('pb-enabled').addEventListener('change', function() {
  document.getElementById('pb-enabled-label').textContent = this.checked ? 'Активен' : 'Неактивен';
});

function applyCronPreset(val) {
  if (!val) return;
  document.getElementById('pb-cron').value = val;
  updateCronHuman(val);
}

function syncCronPreset() {
  const val = document.getElementById('pb-cron').value.trim();
  updateCronHuman(val);
  const sel = document.getElementById('pb-cron-preset');
  let found = false;
  for (const o of sel.options) { if (o.value === val) { o.selected = true; found = true; break; } }
  if (!found) sel.value = '';
}

function updateCronHuman(expr) {
  const el = document.getElementById('pb-cron-human');
  if (el) el.textContent = cronDescription(expr.trim());
}

/* ── Step management ── */
function pbAddStep() {
  pb.steps.push({
    id: `step_${pb.steps.length + 1}`,
    name: '',
    connector_type: '',
    connector_config: '',
    transform_sql: '',
    target_table: '',
    deps: [],
    max_retries: 3,
    retry_delay_sec: 30,
  });
  renderBuilderSteps();
  /* scroll to bottom */
  const last = document.getElementById('pb-steps').lastElementChild;
  if (last) last.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function pbRemoveStep(idx) {
  pb.steps.splice(idx, 1);
  /* fix dep references: remove refs to deleted step, shift down refs > idx */
  pb.steps.forEach(s => {
    s.deps = (s.deps || [])
      .filter(d => d !== idx)
      .map(d => d > idx ? d - 1 : d);
  });
  renderBuilderSteps();
}

function pbMoveStep(idx, dir) {
  const newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= pb.steps.length) return;
  [pb.steps[idx], pb.steps[newIdx]] = [pb.steps[newIdx], pb.steps[idx]];
  /* fix dep references: swap idx and newIdx */
  pb.steps.forEach(s => {
    s.deps = (s.deps || []).map(d => d === idx ? newIdx : d === newIdx ? idx : d);
  });
  renderBuilderSteps();
}

function pbUpdateStep(idx, field, val) {
  pb.steps[idx][field] = val;
}

function pbChangeConnType(idx, type) {
  pb.steps[idx].connector_type    = type;
  pb.steps[idx].connector_config  = '';
  /* re-render only the connector config section */
  const cfgDiv = document.getElementById(`step-conn-cfg-${idx}`);
  if (cfgDiv) cfgDiv.innerHTML = makeConnectorConfigHTML(pb.steps[idx], idx);
}

function pbUpdateConnConfig(idx, field, val) {
  const cfg = safeParse(pb.steps[idx].connector_config, {});
  cfg[field] = val;
  pb.steps[idx].connector_config = JSON.stringify(cfg);
}

function pbToggleDep(stepIdx, depIdx, checked) {
  const deps = pb.steps[stepIdx].deps || [];
  if (checked && !deps.includes(depIdx)) deps.push(depIdx);
  pb.steps[stepIdx].deps = checked ? deps : deps.filter(d => d !== depIdx);
}

/* ── Rendering ── */
function renderBuilderSteps() {
  renderFlowGraph();
  const container = document.getElementById('pb-steps');
  if (!pb.steps.length) {
    container.innerHTML = `
      <div class="empty-state">
        Шагов пока нет.<br>
        <button class="btn btn-primary" style="margin-top:.75rem" onclick="pbAddStep()">+ Добавить первый шаг</button>
      </div>`;
    return;
  }
  container.innerHTML = '';
  pb.steps.forEach((step, i) => container.appendChild(makeStepCard(step, i)));
}

function renderFlowGraph() {
  const wrap = document.getElementById('pb-flow-graph');
  if (!wrap) return;
  if (!pb.steps.length) { wrap.innerHTML = ''; return; }

  const NW = 200, NH = 80, GAP = 52, PADX = 20, PADY = 28;
  const W = pb.steps.length * (NW + GAP) - GAP + PADX * 2;
  const H = NH + PADY * 2;

  const ACCENT  = '#5b6ef5';
  const NODE_BG = '#252d4a';
  const TEXT    = '#f1f5f9';
  const SUB     = '#94a3b8';
  const BADGE_BG= '#5b6ef5';

  let s = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <marker id="arh" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
      <polygon points="0 0,10 4,0 8" fill="${ACCENT}"/>
    </marker>
  </defs>`;

  pb.steps.forEach((step, i) => {
    const x = PADX + i * (NW + GAP), y = PADY;
    const cx = x + NW / 2;
    const name  = (step.name || `Шаг ${i + 1}`).slice(0, 24);
    const table = step.target_table ? step.target_table.slice(0, 26) : '';

    if (i > 0) {
      const x0 = x - GAP, my = y + NH / 2;
      s += `<line x1="${x0}" y1="${my}" x2="${x - 3}" y2="${my}"
              stroke="${ACCENT}" stroke-width="2" marker-end="url(#arh)"/>`;
    }

    /* badge pill sits above the node with 10px gap */
    const bw = 52, bh = 20, bx = cx - bw / 2, by = y - bh - 6;

    s += `<g onclick="document.getElementById('step-card-${i}')?.scrollIntoView({behavior:'smooth',block:'nearest'})" style="cursor:pointer">
      <rect x="${x}" y="${y}" width="${NW}" height="${NH}" rx="10"
            fill="${NODE_BG}" stroke="${ACCENT}" stroke-width="2"/>
      <rect x="${bx}" y="${by}" width="${bw}" height="${bh}" rx="10"
            fill="${BADGE_BG}"/>
      <text x="${cx}" y="${by + 14}" text-anchor="middle" font-size="10" font-weight="700"
            fill="#fff" font-family="system-ui,sans-serif" letter-spacing=".05em">ШАГ ${i + 1}</text>
      <text x="${cx}" y="${y + 30}" text-anchor="middle" font-size="13" font-weight="600"
            fill="${TEXT}" font-family="system-ui,sans-serif">${escHtml(name)}</text>
      ${table ? `<text x="${cx}" y="${y + 52}" text-anchor="middle" font-size="11"
            fill="${SUB}" font-family="system-ui,sans-serif">→ ${escHtml(table)}</text>` : ''}
    </g>`;
  });

  s += '</svg>';
  wrap.innerHTML = s;
}

function makeStepCard(step, idx) {
  const div = document.createElement('div');
  div.className = 'step-card';
  div.id = `step-card-${idx}`;
  div.innerHTML = `
    <div class="step-header">
      <span class="step-num">${idx + 1}</span>
      <input class="step-name-input" type="text" placeholder="Название шага"
             value="${escAttr(step.name)}"
             oninput="pbUpdateStep(${idx},'name',this.value);renderFlowGraph()">
      <div class="step-move-btns">
        ${idx > 0                      ? `<button class="btn btn-sm" onclick="pbMoveStep(${idx},-1)" title="Вверх">↑</button>` : ''}
        ${idx < pb.steps.length - 1   ? `<button class="btn btn-sm" onclick="pbMoveStep(${idx}, 1)" title="Вниз">↓</button>` : ''}
      </div>
      <button class="btn btn-sm btn-danger" onclick="pbRemoveStep(${idx})">Удалить</button>
    </div>
    <div class="step-body">
      <div class="step-row-2">
        <div class="form-group" style="margin:0">
          <label>Источник данных</label>
          <select onchange="pbChangeConnType(${idx}, this.value)">
            <option value=""           ${!step.connector_type                      ?'selected':''}>— только SQL-трансформация —</option>
            <option value="csv"        ${step.connector_type==='csv'               ?'selected':''}>CSV файл</option>
            <option value="parquet"    ${step.connector_type==='parquet'           ?'selected':''}>Parquet файл</option>
            <option value="json_http"  ${step.connector_type==='json_http'         ?'selected':''}>HTTP / REST API (JSON)</option>
            <option value="postgresql" ${step.connector_type==='postgresql'        ?'selected':''}>PostgreSQL</option>
          </select>
        </div>
        <div class="form-group" style="margin:0">
          <label>Результат → таблица</label>
          <input type="text" placeholder="имя_таблицы"
                 value="${escAttr(step.target_table)}"
                 oninput="pbUpdateStep(${idx},'target_table',this.value);renderFlowGraph()">
        </div>
      <div class="step-row-2" style="margin-top:0.75rem">
        <div class="form-group" style="margin:0">
          <label>Max retries</label>
          <input type="number" min="0" value="${escAttr(step.max_retries != null ? step.max_retries : 3)}"
                 oninput="pbUpdateStep(${idx},'max_retries',parseInt(this.value,10) || 0)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Backoff (сек)</label>
          <input type="number" min="1" value="${escAttr(step.retry_delay_sec != null ? step.retry_delay_sec : 30)}"
                 oninput="pbUpdateStep(${idx},'retry_delay_sec',parseInt(this.value,10) || 30)">
        </div>
      </div>
      <div class="form-group" style="margin:0">
        <label>SQL-трансформация <span class="label-hint">выполняется после загрузки данных</span></label>
        <textarea class="mono-textarea" rows="4"
                  placeholder="SELECT * FROM source_table WHERE ..."
                  oninput="pbUpdateStep(${idx},'transform_sql',this.value)">${escHtml(step.transform_sql || '')}</textarea>
      </div>
    </div>
  `;
  return div;
}

function makeConnectorConfigHTML(step, idx) {
  const type = step.connector_type;
  const cfg  = safeParse(step.connector_config, {});

  if (!type) return '';

  if (type === 'csv') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Путь к файлу</label>
        <input type="text" value="${escAttr(cfg.path || '')}"
               oninput="pbUpdateConnConfig(${idx},'path',this.value)"
               placeholder="/data/input.csv">
      </div>
      <div class="form-group" style="margin:0">
        <label>Разделитель</label>
        <select onchange="pbUpdateConnConfig(${idx},'delimiter',this.value)">
          <option value=","  ${(cfg.delimiter||',')===',' ?'selected':''}>Запятая (,)</option>
          <option value=";"  ${cfg.delimiter===';'       ?'selected':''}>Точка с запятой (;)</option>
          <option value="\t" ${cfg.delimiter==='\t'      ?'selected':''}>Табуляция (TSV)</option>
        </select>
      </div>
    </div>`;

  if (type === 'parquet') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0;flex:3">
        <label>Путь или glob-паттерн</label>
        <input type="text" value="${escAttr(cfg.path || '')}"
               oninput="pbUpdateConnConfig(${idx},'path',this.value)"
               placeholder="/data/exports/*.parquet">
      </div>
    </div>`;

  if (type === 'json_http') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0;flex:3">
        <label>Адрес (URL)</label>
        <input type="text" value="${escAttr(cfg.url || '')}"
               oninput="pbUpdateConnConfig(${idx},'url',this.value)"
               placeholder="https://api.example.com/data">
      </div>
      <div class="form-group" style="margin:0;flex:1">
        <label>Метод</label>
        <select onchange="pbUpdateConnConfig(${idx},'method',this.value)">
          <option ${(cfg.method||'GET')==='GET'?'selected':''}>GET</option>
          <option ${cfg.method==='POST'?'selected':''}>POST</option>
        </select>
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Авторизация</label>
        <select onchange="pbUpdateConnConfig(${idx},'auth_type',this.value)">
          <option value="none"    ${(cfg.auth_type||'none')==='none'   ?'selected':''}>Нет</option>
          <option value="bearer"  ${cfg.auth_type==='bearer'           ?'selected':''}>Bearer Token</option>
          <option value="api_key" ${cfg.auth_type==='api_key'          ?'selected':''}>API Key</option>
        </select>
      </div>
      <div class="form-group" style="margin:0;flex:2" ${(cfg.auth_type||'none')==='none'?'style="display:none"':''}>
        <label>Токен / ключ</label>
        <input type="text" value="${escAttr(cfg.auth_token || '')}"
               oninput="pbUpdateConnConfig(${idx},'auth_token',this.value)"
               placeholder="sk-...">
      </div>
      <div class="form-group" style="margin:0;flex:2">
        <label>Путь к массиву (data_path)</label>
        <input type="text" value="${escAttr(cfg.data_path || '')}"
               oninput="pbUpdateConnConfig(${idx},'data_path',this.value)"
               placeholder="data.items">
      </div>
    </div>`;

  if (type === 'postgresql') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Хост</label>
        <input type="text" value="${escAttr(cfg.host || '')}"
               oninput="pbUpdateConnConfig(${idx},'host',this.value)"
               placeholder="localhost">
      </div>
      <div class="form-group" style="margin:0" style="max-width:100px">
        <label>Порт</label>
        <input type="text" value="${escAttr(cfg.port || '5432')}"
               oninput="pbUpdateConnConfig(${idx},'port',this.value)"
               placeholder="5432">
      </div>
      <div class="form-group" style="margin:0">
        <label>База данных</label>
        <input type="text" value="${escAttr(cfg.dbname || '')}"
               oninput="pbUpdateConnConfig(${idx},'dbname',this.value)"
               placeholder="postgres">
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Пользователь</label>
        <input type="text" value="${escAttr(cfg.user || '')}"
               oninput="pbUpdateConnConfig(${idx},'user',this.value)"
               placeholder="postgres">
      </div>
      <div class="form-group" style="margin:0">
        <label>Пароль</label>
        <input type="password" value="${escAttr(cfg.password || '')}"
               oninput="pbUpdateConnConfig(${idx},'password',this.value)"
               placeholder="">
      </div>
      <div class="form-group" style="margin:0;flex:2">
        <label>Таблица / SQL-запрос источника</label>
        <input type="text" value="${escAttr(cfg.query || cfg.table || '')}"
               oninput="pbUpdateConnConfig(${idx},'table',this.value)"
               placeholder="orders или SELECT * FROM orders WHERE active=true">
      </div>
    </div>`;

  /* fallback: raw JSON */
  return `
    <div class="form-group" style="margin:0">
      <label>Конфигурация коннектора (JSON)</label>
      <textarea class="mono-textarea" rows="2" placeholder="{}"
                oninput="pb.steps[${idx}].connector_config=this.value">${escHtml(step.connector_config || '')}</textarea>
    </div>`;
}

/* ── Save pipeline ── */
async function savePipeline() {
  const name    = document.getElementById('pb-name').value.trim();
  const cron    = document.getElementById('pb-cron').value.trim();
  const enabled = document.getElementById('pb-enabled').checked;
  const maxRetries = parseInt(document.getElementById('pb-max-retries').value, 10) || 3;
  const retryDelay = parseInt(document.getElementById('pb-retry-delay').value, 10) || 30;
  const webhookUrl = document.getElementById('pb-webhook-url').value.trim();
  const webhookOn  = document.getElementById('pb-webhook-on').value || 'failure';
  const alertCooldown = parseInt(document.getElementById('pb-alert-cooldown').value, 10);

  if (!name) { showToast('Необходимо указать название конвейера', 'error'); return; }

  const steps = pb.steps.map((s, i) => ({
    id:               s.id || `step_${i + 1}`,
    name:             s.name || `Шаг ${i + 1}`,
    connector_type:   s.connector_type   || '',
    connector_config: s.connector_config || '',
    transform_sql:    s.transform_sql    || '',
    target_table:     s.target_table     || '',
    max_retries:      s.max_retries != null ? s.max_retries : 3,
    retry_delay_sec:  s.retry_delay_sec != null ? s.retry_delay_sec : 30,
    deps:             s.deps             || [],
    ndeps:            (s.deps || []).length,
  }));

  const body = { name, cron, enabled, steps,
                 max_retries: maxRetries,
                 retry_delay_sec: retryDelay,
                 webhook_url: webhookUrl,
                 webhook_on: webhookOn,
                 alert_cooldown: isNaN(alertCooldown) ? 300 : alertCooldown };
  if (pb.editId) body.id = pb.editId;

  try {
    if (pb.editId) {
      /* update = delete + re-create */
      await apiFetch(`/api/pipelines/${pb.editId}`, 'DELETE');
    }
    await apiPost('/api/pipelines', body);
    showToast(`Конвейер "${name}" сохранён`, 'ok');
    logActivity(`сохранён конвейер: ${name}`);
    document.getElementById('nav-builder').classList.remove('visible');
    switchView('pipelines');
    loadPipelines();
  } catch (err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}


/* ═══════════════════════════════════════════════════
   INGEST
═══════════════════════════════════════════════════ */
const dropZone = document.getElementById('drop-zone');
dropZone.addEventListener('dragover',  e => { e.preventDefault(); dropZone.classList.add('over'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('over'));
dropZone.addEventListener('drop', e => {
  e.preventDefault(); dropZone.classList.remove('over');
  const file = e.dataTransfer.files[0]; if (file) handleFile(file);
});

function handleFileSelect(input) { if (input.files[0]) handleFile(input.files[0]); }

const DROP_ZONE_DEFAULT_HTML = `
  <svg class="drop-svg" viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
    <rect x="6" y="10" width="36" height="28" rx="4" stroke="currentColor" stroke-width="2.2" fill="none"/>
    <path d="M16 22l8-8 8 8M24 14v16" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
    <path d="M14 34h20" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"/>
  </svg>
  <div class="drop-hint">Перетащите файл сюда</div>
  <div class="drop-or">или</div>
  <button class="btn" onclick="document.getElementById('file-input').click()">Выбрать файл</button>
`;

function handleFile(file) {
  const isParquet = /\.parquet$/i.test(file.name);
  if (isParquet) switchIngestMode('parquet');

  ingestFile = file;
  const tableName = file.name.replace(/\.[^.]+$/, '').replace(/[^a-z0-9_]/gi, '_').toLowerCase();
  document.getElementById('ingest-table').value = tableName;
  document.getElementById('upload-btn').disabled = false;
  dropZone.innerHTML = `
    <svg class="drop-svg" style="color:var(--green)" viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="24" cy="24" r="18" stroke="currentColor" stroke-width="2.2" fill="none"/>
      <path d="M16 24l6 6 10-10" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
    <div class="drop-hint">${escHtml(file.name)}</div>
    <div class="drop-or">${fmtBytes(file.size)}</div>
    <button class="btn btn-sm" onclick="document.getElementById('file-input').click()">Заменить файл</button>
  `;
  if (!isParquet) {
    const reader = new FileReader();
    reader.onload = e => { ingestContent = e.target.result; processIngestCSV(ingestContent); };
    reader.readAsText(file);
  } else {
    document.getElementById('ingest-right').style.display = 'none';
  }
  document.getElementById('file-input').value = '';
}

function switchIngestMode(mode) {
  ingestMode = mode;
  document.getElementById('tab-csv')?.classList.toggle('active', mode === 'csv');
  document.getElementById('tab-parquet')?.classList.toggle('active', mode === 'parquet');
  const hint = document.querySelector('.drop-hint');
  if (hint && !ingestFile) hint.textContent = mode === 'parquet' ? 'Перетащите .parquet файл сюда' : 'Перетащите файл сюда';
}

function detectDelimiterStr(line) {
  const commas = (line.match(/,/g) || []).length;
  const tabs   = (line.match(/\t/g) || []).length;
  const semis  = (line.match(/;/g)  || []).length;
  if (tabs > commas && tabs > semis) return '\t';
  if (semis > commas) return ';';
  return ',';
}

function inferType(val) {
  const v = (val || '').trim();
  if (!v || /^(null|na|n\/a)$/i.test(v)) return 'text';
  if (/^(true|false)$/i.test(v))         return 'bool';
  if (/^-?\d+$/.test(v))                 return 'int64';
  if (/^-?\d+\.?\d*([eE][+-]?\d+)?$/.test(v)) return 'double';
  return 'text';
}

function processIngestCSV(content) {
  const lines = content.split('\n').filter(l => l.trim());
  if (!lines.length) return;
  ingestDelimiter = detectDelimiterStr(lines[0]);
  const headers = splitCSVLineDelim(lines[0], ingestDelimiter)
    .map(h => h.replace(/\r/g, '').trim());
  const sample = lines.length > 1 ? splitCSVLineDelim(lines[1], ingestDelimiter) : [];
  ingestColumns = headers.map((name, i) => ({ name, type: inferType(sample[i] || '') }));
  showPreview(content);
  document.getElementById('ingest-right').style.display = '';
}

function showPreview(csv) {
  const lines = csv.split('\n').filter(Boolean).slice(0, 6);
  if (!lines.length) return;
  const delim = ingestDelimiter || ',';
  const cols  = splitCSVLineDelim(lines[0], delim);
  const rows  = lines.slice(1).map(l => splitCSVLineDelim(l, delim));
  let html = '<table class="preview-table"><thead><tr>';
  cols.forEach(c => { html += `<th>${escHtml(c.replace(/\r/g,''))}</th>`; });
  html += '</tr></thead><tbody>';
  rows.forEach(row => {
    html += '<tr>';
    row.forEach(v => { html += `<td>${escHtml(v.replace(/\r/g,''))}</td>`; });
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('preview-table').innerHTML = html;
}

async function uploadCSV() {
  if (ingestMode === 'parquet') { uploadParquet(); return; }
  const table = document.getElementById('ingest-table').value.trim() || 'upload';
  if (!ingestContent) { showToast('Файл не выбран', 'warn'); return; }
  const status = document.getElementById('ingest-status');
  status.textContent = 'Загрузка…';
  try {
    const resp = await apiPostRaw(`/api/ingest/csv?table=${encodeURIComponent(table)}`, ingestContent, 'text/csv');
    status.innerHTML = `<span style="color:var(--green)">✓ ${fmtNum(resp.rows)} строк → <strong>${resp.table}</strong></span>`;
    showToast(`Импортировано ${fmtNum(resp.rows)} строк в "${resp.table}"`, 'ok');
    logActivity(`загружено ${resp.rows} строк → ${resp.table}`);
    document.getElementById('upload-btn').disabled = true;
    ingestContent = null; ingestColumns = [];
    dropZone.innerHTML = DROP_ZONE_DEFAULT_HTML;
    document.getElementById('ingest-right').style.display = 'none';
    document.getElementById('ingest-table').value = '';
    document.getElementById('file-input').value = '';
  } catch (err) {
    status.innerHTML = `<span style="color:var(--red)">Error: ${escHtml(String(err))}</span>`;
    showToast(String(err), 'error');
  }
}


async function uploadParquet() {
  const table = document.getElementById('ingest-table').value.trim() || 'upload';
  if (!ingestFile) { showToast('Файл не выбран', 'warn'); return; }
  const status = document.getElementById('ingest-status');
  status.textContent = 'Загрузка…';
  try {
    const resp = await apiPostRaw(`/api/ingest/parquet?table=${encodeURIComponent(table)}`, ingestFile, 'application/octet-stream');
    status.innerHTML = `<span style="color:var(--green)">✓ ${fmtNum(resp.rows)} строк → <strong>${resp.table}</strong></span>`;
    showToast(`Импортировано ${fmtNum(resp.rows)} строк в "${resp.table}"`, 'ok');
    logActivity(`загружено ${resp.rows} строк (parquet) → ${resp.table}`);
    document.getElementById('upload-btn').disabled = true;
    ingestContent = null; ingestFile = null;
    dropZone.innerHTML = DROP_ZONE_DEFAULT_HTML;
    document.getElementById('ingest-right').style.display = 'none';
    document.getElementById('ingest-table').value = '';
    document.getElementById('file-input').value = '';
  } catch (err) {
    status.innerHTML = `<span style="color:var(--red)">Error: ${escHtml(String(err))}</span>`;
    showToast(String(err), 'error');
  }
}


function splitCSVLineDelim(line, delim) {
  const out = []; let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { inQ = !inQ; }
    else if (c === delim && !inQ) { out.push(cur.trim()); cur = ''; }
    else cur += c;
  }
  out.push(cur.trim());
  return out;
}


/* ═══════════════════════════════════════════════════
   METRICS
═══════════════════════════════════════════════════ */
async function loadMetrics() {
  try {
    const [raw, tables, pipelines] = await Promise.all([
      apiFetch('/api/metrics').catch(() => null),
      apiFetch('/api/tables').catch(() => []),
      apiFetch('/api/pipelines').catch(() => []),
    ]);
    if (!raw) return; /* logged out or network error */
    const m = raw.metrics ?? raw; /* /api/metrics wraps in {metrics:{...}} */

    /* ── Stat cards ── */
    const grid = document.getElementById('metrics-detail');
    grid.innerHTML = '';
    const fields = [
      ['uptime',                  'Аптайм',                    v => fmtUptime(v)],
      ['total_rows',              'Строк загружено',           v => fmtNum(v)],
      ['total_queries',           'Запросов выполнено',        v => fmtNum(v)],
      ['total_pipelines_run',     'Запусков конвейеров',       v => fmtNum(v)],
      ['avg_query_latency_ms',    'Задержка запроса, мс',      v => v.toFixed(1)],
      ['avg_pipeline_latency_ms', 'Задержка конвейера, мс',   v => v.toFixed(1)],
    ];
    fields.forEach(([key, label, fmt]) => {
      const v    = m[key] ?? 0;
      const card = document.createElement('div');
      card.className = 'stat-card';
      card.innerHTML = `<div class="stat-val">${fmt(v)}</div><div class="stat-label">${label}</div>`;
      grid.appendChild(card);
    });

    /* ── Tables bar chart ── */
    const barList = document.getElementById('metrics-bar-list');
    const sorted  = [...tables].sort((a, b) => (b.rows || 0) - (a.rows || 0)).slice(0, 10);
    const maxRows = Math.max(...sorted.map(t => t.rows || 0), 1);
    if (!sorted.length) {
      barList.innerHTML = '<div class="metrics-empty">Нет таблиц</div>';
    } else {
      barList.innerHTML = sorted.map(t => {
        const pct  = Math.max(2, Math.round(((t.rows || 0) / maxRows) * 100));
        const rows = fmtNum(t.rows || 0);
        return `<div class="mbar-row">
          <div class="mbar-label" title="${escHtml(t.name)}">${escHtml(t.name)}</div>
          <div class="mbar-track"><div class="mbar-fill" style="width:${pct}%"></div></div>
          <div class="mbar-val">${rows}</div>
        </div>`;
      }).join('');
    }

    /* ── Pipelines summary ── */
    const plList = document.getElementById('metrics-pipeline-list');
    if (!pipelines.length) {
      plList.innerHTML = '<div class="metrics-empty">Нет конвейеров</div>';
    } else {
      plList.innerHTML = pipelines.map(p => {
        const st   = p.status || 0;
        const badge= STATUS_BADGE[st]  || '';
        const lbl  = STATUS_LABELS[st] || '';
        const last = p.last_run ? new Date(p.last_run * 1000).toLocaleString() : 'не запускался';
        return `<div class="mpl-row">
          <span class="mpl-name">${escHtml(p.name || p.id)}</span>
          <span class="badge ${badge}" style="flex-shrink:0">${lbl}</span>
          <span class="mpl-meta">${last}</span>
        </div>`;
      }).join('');
    }

  } catch (err) {
    document.getElementById('metrics-detail').innerHTML =
      `<div style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

function restartMetricsTimer() {
  if (metricsTimer) clearInterval(metricsTimer);
  if (prefs.autoRefresh) {
    const ms = Math.max(5000, (prefs.refreshInterval || 30) * 1000);
    metricsTimer = setInterval(() => {
      if (document.getElementById('view-metrics').classList.contains('active'))
        loadMetrics();
    }, ms);
  }
}


/* ═══════════════════════════════════════════════════
   ANALYTICS — Visual Chart Builder
═══════════════════════════════════════════════════ */

const CHART_PALETTE = ['#5b6ef5','#34c759','#ff9f0a','#ff375f','#bf5af2',
                       '#5ac8fa','#ff6b35','#30d158','#f5a623','#00b4d8',
                       '#a8e063','#c77dff'];
const chartStateMap = new Map();
let chartIdCounter = 0;

function newChartCfg(overrides = {}) {
  return { id:'ch'+(++chartIdCounter), title:'График '+chartIdCounter, xCol:'', yCol:'', type:'bar', agg:'sum', topN:20, ...overrides };
}

function switchAnTab(tab) {
  document.getElementById('an-pane-charts').style.display = tab === 'charts' ? '' : 'none';
  document.getElementById('an-pane-stats').style.display  = tab === 'stats'  ? '' : 'none';
  document.querySelectorAll('.an-tab').forEach(t =>
    t.classList.toggle('active', t.dataset.tab === tab));
}

async function loadAnalyticsModule() {
  showAnScreen('list');
  loadSavedResults();
  /* prefetch tables for editor */
  try {
    const tables = await apiFetch('/api/tables');
    analyticsState.tables = tables || [];
  } catch(_) {}
}

function showAnScreen(screen) {
  document.getElementById('an-list-screen').style.display   = screen === 'list'   ? '' : 'none';
  document.getElementById('an-view-screen').style.display   = screen === 'view'   ? '' : 'none';
  document.getElementById('an-editor-screen').style.display = screen === 'editor' ? '' : 'none';
}

function openNewAnalytics() {
  document.getElementById('an-editor-title').textContent = 'Новая аналитика';
  document.getElementById('an-sql').value = '';
  document.getElementById('an-status').textContent = '';
  document.getElementById('an-chart-area').style.display = 'none';
  document.getElementById('an-save-btn').style.display = 'none';
  analyticsState.currentRows = []; analyticsState.currentCols = []; analyticsState.charts = [];
  /* fill table pills */
  const pills = document.getElementById('an-table-pills');
  if (pills) pills.innerHTML = analyticsState.tables.length
    ? analyticsState.tables.map(t =>
        `<button class="btn btn-sm" onclick="anQuickTable('${escAttr(t.name)}')">${escHtml(t.name)}</button>`
      ).join('')
    : '<span style="color:var(--muted);font-size:.85rem">Нет таблиц</span>';
  showAnScreen('editor');
}

function closeAnalyticsEditor() {
  showAnScreen('list');
}

function anQuickTable(name) {
  const el = document.getElementById('an-sql');
  if (el) el.value = `SELECT * FROM ${name} LIMIT 500`;
  runAnalyticsQuery();
}

async function runAnalyticsQuery() {
  const sqlEl = document.getElementById('an-sql');
  const sql = (sqlEl?.value || '').trim();
  if (!sql) { setText('an-status', 'Введите SQL запрос'); return; }
  setText('an-status', 'Выполнение…');
  const t0 = Date.now();
  try {
    const data = await apiPost('/api/tables/query', { sql });
    const cols = data.columns || [];
    const srcRows = data.rows || [];
    const rows = srcRows.map(r => {
      const obj = {};
      const vals = Array.isArray(r) ? r : cols.map(c => r[c]);
      cols.forEach((c, i) => { obj[c] = vals[i]; });
      return obj;
    });
    analyticsState.currentRows = rows;
    analyticsState.currentCols = cols;
    setText('an-status', `${rows.length} строк · ${((Date.now()-t0)/1000).toFixed(2)}с`);
    document.getElementById('an-chart-area').style.display = '';
    document.getElementById('an-save-btn').style.display = '';
    switchAnTab('charts');
    renderAnalyticsDataTable(rows, cols);
    fillStatsSelectors(cols);
    onStatMethodChange();
    if (!analyticsState.charts.length) makeDefaultCharts(cols, rows);
    else buildAllCharts();
  } catch(err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/, '')); if (j.error) msg = j.error; } catch(_) {}
    setText('an-status', `Ошибка: ${msg}`);
  }
}

/* ── Chart management ── */
function makeDefaultCharts(cols, rows) {
  const numCols  = cols.filter(c => rows.slice(0,20).some(r => Number.isFinite(Number(r[c])) && r[c] !== ''));
  const lblCols  = cols.filter(c => !numCols.includes(c));
  const xCands   = lblCols.filter(c => !/^id$|_id$/i.test(c));
  const xCol     = xCands[0] || lblCols[0] || cols[0] || '';
  const metrics  = numCols.filter(c => isLikelyMetricColumn(c));
  const nonId    = numCols.filter(c => !/^id$|_id$/i.test(c));
  const yCol     = metrics[0] || nonId[0] || numCols[0] || cols[1] || '';
  const dateCol  = lblCols.find(c => /date|time|month|year|day|week/i.test(c));
  clearAllCharts();
  addAnalyticsChart('bar',  { xCol, yCol });
  if (dateCol && yCol)       addAnalyticsChart('area',    { xCol: dateCol, yCol });
  else if (nonId.length >= 2) addAnalyticsChart('scatter', { xCol: nonId[0], yCol: nonId[1] });
}

function clearAllCharts() {
  analyticsState.charts = [];
  chartStateMap.clear();
  chartIdCounter = 0;
  const g = document.getElementById('an-charts-grid');
  if (g) g.innerHTML = '';
}

function addAnalyticsChart(typeHint = 'bar', overrides = {}) {
  const cols = analyticsState.currentCols;
  const rows = analyticsState.currentRows;
  if (!cols.length) return;
  let { xCol, yCol } = overrides;
  if (!xCol || !yCol) {
    const numCols = cols.filter(c => rows.slice(0,20).some(r => Number.isFinite(Number(r[c])) && r[c] !== ''));
    const lblCols = cols.filter(c => !numCols.includes(c));
    const xCands  = lblCols.filter(c => !/^id$|_id$/i.test(c));
    if (!xCol) xCol = xCands[0] || lblCols[0] || cols[0] || '';
    const metrics = numCols.filter(c => isLikelyMetricColumn(c));
    const nonId   = numCols.filter(c => !/^id$|_id$/i.test(c));
    if (!yCol) yCol = metrics[0] || nonId[0] || numCols[0] || cols[1] || '';
  }
  const cfg = newChartCfg({ type: typeHint, xCol, yCol });
  analyticsState.charts.push(cfg);
  const grid = document.getElementById('an-charts-grid');
  if (!grid) return;
  const wrap = document.createElement('div');
  wrap.innerHTML = renderChartCardHtml(cfg);
  grid.appendChild(wrap.firstElementChild);
  requestAnimationFrame(() => buildChartById(cfg.id));
}

function removeAnalyticsChart(id) {
  analyticsState.charts = analyticsState.charts.filter(c => c.id !== id);
  chartStateMap.delete(id);
  document.getElementById('cc_' + id)?.remove();
}

function anChartSet(id, prop, val) {
  const cfg = analyticsState.charts.find(c => c.id === id);
  if (!cfg) return;
  cfg[prop] = prop === 'topN' ? (Math.max(2, Number(val)) || 20) : val;
  requestAnimationFrame(() => { buildChartById(id); observeChartResize(id); });
}

function buildAllCharts() {
  analyticsState.charts.forEach(cfg => requestAnimationFrame(() => buildChartById(cfg.id)));
}

function buildChartById(id) {
  const cfg  = analyticsState.charts.find(c => c.id === id);
  if (!cfg) return;
  const rows = analyticsState.currentRows || [];
  if (!rows.length) return;
  const pts  = aggregateForChart(cfg, rows);
  const aggL = cfg.agg === 'count' ? 'COUNT(*)' : `${cfg.agg.toUpperCase()}(${cfg.yCol})`;
  drawChartOnCanvas(id, pts, cfg.type, aggL, cfg.xCol);
  renderChartLegend(id, pts, cfg.type);
}

/* backward-compat alias used by stats module */
function buildAnalyticsChart() { buildAllCharts(); }

/* ── Chart card HTML ── */
/* ── Drag-and-drop reorder ──
   draggable="true" lives on the ⠿ handle span only.
   dragover/drop/dragleave live on each card via currentTarget.
── */
let anDragSrcId = null;

function anDragStartHandle(ev, id) {
  anDragSrcId = id;
  ev.dataTransfer.effectAllowed = 'move';
  ev.dataTransfer.setData('text/plain', id);
  /* use the whole card as ghost image */
  const card = document.getElementById('cc_' + id);
  if (card && ev.dataTransfer.setDragImage) ev.dataTransfer.setDragImage(card, 24, 24);
  setTimeout(() => card?.classList.add('an-dragging'), 0);
}
function anDragOver(ev) {
  if (!anDragSrcId) return;
  const card = ev.currentTarget;
  if (card.id === 'cc_' + anDragSrcId) return;
  ev.preventDefault();
  ev.dataTransfer.dropEffect = 'move';
  document.querySelectorAll('.an-chart-card').forEach(c => c.classList.remove('an-drag-over'));
  card.classList.add('an-drag-over');
}
function anDragLeave(ev) {
  if (!ev.currentTarget.contains(ev.relatedTarget))
    ev.currentTarget.classList.remove('an-drag-over');
}
function anDragEnd() {
  document.querySelectorAll('.an-chart-card').forEach(c => c.classList.remove('an-dragging','an-drag-over'));
  anDragSrcId = null;
}
function anDrop(ev) {
  ev.preventDefault();
  const srcId = anDragSrcId;
  const tgtEl = ev.currentTarget;
  anDragEnd();
  if (!srcId) return;
  const tgtId = tgtEl.id.replace('cc_', '');
  if (srcId === tgtId) return;
  const grid = document.getElementById('an-charts-grid');
  const srcEl = document.getElementById('cc_' + srcId);
  if (!srcEl || !tgtEl || !grid) return;
  const kids = [...grid.children];
  if (kids.indexOf(srcEl) < kids.indexOf(tgtEl)) tgtEl.after(srcEl);
  else tgtEl.before(srcEl);
  const newOrder = [...grid.children].map(el => el.id.replace('cc_','')).filter(Boolean);
  analyticsState.charts.sort((a,b) => newOrder.indexOf(a.id) - newOrder.indexOf(b.id));
}

/* ── Mouse resize: right edge (width) and bottom edge (height) ── */
let _anResizeRAF = null;
function _anRAFRedraw(id) {
  if (_anResizeRAF) cancelAnimationFrame(_anResizeRAF);
  _anResizeRAF = requestAnimationFrame(() => { buildChartById(id); _anResizeRAF = null; });
}

function anResizeRight(ev, id) {
  ev.preventDefault();
  ev.stopPropagation();
  const card = document.getElementById('cc_' + id);
  if (!card) return;
  const startX = ev.clientX;
  const widths = ['1', '2', 'full'];
  const startIdx = Math.max(0, widths.indexOf(card.dataset.width || '1'));
  let lastIdx = startIdx;

  const onMove = e => {
    const delta = e.clientX - startX;
    let idx = startIdx;
    if      (delta >  140) idx = Math.min(startIdx + 2, 2);
    else if (delta >   60) idx = Math.min(startIdx + 1, 2);
    else if (delta < -140) idx = Math.max(startIdx - 2, 0);
    else if (delta <  -60) idx = Math.max(startIdx - 1, 0);
    if (idx !== lastIdx) {
      lastIdx = idx;
      card.dataset.width = widths[idx];
      _anRAFRedraw(id);
    }
  };
  const onUp = () => {
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  };
  document.body.style.cursor = 'ew-resize';
  document.body.style.userSelect = 'none';
  document.addEventListener('mousemove', onMove);
  document.addEventListener('mouseup', onUp);
}

function anResizeBottom(ev, id) {
  ev.preventDefault();
  const wrap = document.getElementById('cw_' + id);
  if (!wrap) return;
  const startY = ev.clientY;
  const startH = wrap.getBoundingClientRect().height;

  const onMove = e => {
    wrap.style.height = Math.max(180, startH + (e.clientY - startY)) + 'px';
    _anRAFRedraw(id);
  };
  const onUp = () => {
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  };
  document.body.style.cursor = 'ns-resize';
  document.body.style.userSelect = 'none';
  document.addEventListener('mousemove', onMove);
  document.addEventListener('mouseup', onUp);
}

function observeChartResize(id) {
  /* no-op: resizing is driven by anResizeBottom/anResizeRight directly */
}

function renderChartCardHtml(cfg) {
  const cols = analyticsState.currentCols;
  const colOpts = sel => cols.map(c => `<option value="${escAttr(c)}"${sel===c?' selected':''}>${escHtml(c)}</option>`).join('');
  const types = [
    ['bar','📊 Столбчатая'],['line','📈 Линия'],['area','🌊 Область'],['pie','🥧 Круговая'],
    ['scatter','⬤ Точечная'],['histogram','📉 Распределение'],['waterfall','💧 Водопад'],['funnel','🔽 Воронка']
  ];
  const typeOpts = types.map(([v,l]) => `<option value="${v}"${cfg.type===v?' selected':''}>${l}</option>`).join('');
  const aggs = [['sum','Σ Сумма'],['avg','∅ Среднее'],['count','# Кол-во'],['min','↓ Мин'],['max','↑ Макс']];
  const aggOpts = aggs.map(([v,l]) => `<option value="${v}"${cfg.agg===v?' selected':''}>${l}</option>`).join('');
  const id = cfg.id;
  const noAgg = cfg.type === 'scatter' || cfg.type === 'histogram';
  return `<div class="card an-chart-card" id="cc_${id}" data-width="1"
  ondragover="anDragOver(event)" ondrop="anDrop(event)"
  ondragend="anDragEnd()" ondragleave="anDragLeave(event)">
  <div class="an-chart-card-header">
    <span class="an-drag-handle" draggable="true"
          ondragstart="anDragStartHandle(event,'${id}')"
          title="Перетащить для перестановки">⠿</span>
    <input class="an-chart-title-input" value="${escAttr(cfg.title)}"
           oninput="analyticsState.charts.find(c=>c.id==='${id}').title=this.value">
    <div class="an-chart-card-btns">
      <button class="btn btn-sm" onclick="downloadChartById('${id}')" title="Скачать PNG">⬇</button>
      <button class="btn btn-sm" onclick="removeAnalyticsChart('${id}')" title="Удалить">✕</button>
    </div>
  </div>
  <div class="an-chart-controls-row">
    <div class="form-group"><label>Тип</label>
      <select onchange="anChartSet('${id}','type',this.value)">${typeOpts}</select></div>
    <div class="form-group" style="flex:1.5"><label>Ось X</label>
      <select onchange="anChartSet('${id}','xCol',this.value)">${colOpts(cfg.xCol)}</select></div>
    <div class="form-group" style="flex:1.5"><label>Ось Y</label>
      <select onchange="anChartSet('${id}','yCol',this.value)">${colOpts(cfg.yCol)}</select></div>
    <div class="form-group"${noAgg?' style="display:none"':''}><label>Агрег.</label>
      <select onchange="anChartSet('${id}','agg',this.value)">${aggOpts}</select></div>
    <div class="form-group" style="flex:0.65;min-width:55px"><label>Top N</label>
      <input type="number" value="${cfg.topN}" min="2" max="500" style="width:100%"
             oninput="anChartSet('${id}','topN',this.value)"></div>
  </div>
  <div id="cw_${id}" class="an-chart-canvas-wrap">
    <canvas id="c_${id}"></canvas>
    <div id="ct_${id}" class="analytics-tooltip hidden"></div>
    <div class="an-edge-bottom" onmousedown="anResizeBottom(event,'${id}')" title="Растянуть по высоте"></div>
  </div>
  <div id="cl_${id}" class="analytics-legend"></div>
  <div class="an-edge-right" onmousedown="anResizeRight(event,'${id}')" title="Растянуть по ширине"></div>
</div>`;
}

/* ── Data aggregation ── */
function aggregateForChart(cfg, rows) {
  const { xCol, yCol, type, agg } = cfg;
  const N = Math.max(2, cfg.topN || 20);

  if (type === 'scatter') {
    return rows.map(r => ({ x: Number(r[xCol])||0, y: Number(r[yCol])||0,
      label:`(${r[xCol]},${r[yCol]})`, value: Number(r[yCol])||0 })).slice(0, 2000);
  }
  if (type === 'histogram') return computeHistoBuckets(rows, yCol);

  const map = new Map();
  rows.forEach(r => {
    const key = String(r[xCol] ?? '');
    const v = Number(r[yCol]) || 0;
    if (!map.has(key)) map.set(key, { label:key, sum:0, n:0, min:Infinity, max:-Infinity });
    const e = map.get(key);
    e.sum += v; e.n++;
    if (v < e.min) e.min = v;
    if (v > e.max) e.max = v;
  });
  const getV = e =>
    agg==='avg'   ? (e.n ? e.sum/e.n : 0) :
    agg==='count' ? e.n :
    agg==='min'   ? (isFinite(e.min)?e.min:0) :
    agg==='max'   ? (isFinite(e.max)?e.max:0) : e.sum;

  let pts = [...map.values()].map(e => ({ label:e.label, value:getV(e), count:e.n }));
  if (type==='line'||type==='area') {
    pts.sort((a,b) => { const na=Number(a.label),nb=Number(b.label);
      return (Number.isFinite(na)&&Number.isFinite(nb)) ? na-nb : a.label.localeCompare(b.label,'ru'); });
  } else if (type!=='waterfall') {
    pts.sort((a,b) => b.value - a.value);
  }
  if (pts.length > N && type!=='line' && type!=='area' && type!=='waterfall') {
    const rest = pts.slice(N).reduce((s,p) => s+p.value, 0);
    pts = pts.slice(0, N);
    if (Math.abs(rest) > 0) pts.push({ label:'Остальные', value:rest, count:0 });
  }
  return pts;
}

function computeHistoBuckets(rows, col, bins = 12) {
  const vals = rows.map(r => Number(r[col])).filter(Number.isFinite);
  if (!vals.length) return [];
  const mn = Math.min(...vals), mx = Math.max(...vals);
  if (mn === mx) return [{ label:String(mn), value:vals.length }];
  const step = (mx-mn)/bins;
  const counts = new Array(bins).fill(0);
  vals.forEach(v => counts[Math.min(bins-1, Math.floor((v-mn)/step))]++);
  return counts.map((cnt,i) => ({ label:`${shortNum(mn+i*step)}–${shortNum(mn+(i+1)*step)}`, value:cnt }));
}

/* ── Main draw dispatcher ── */
function drawChartOnCanvas(chartId, points, type, metricLabel, xLabel) {
  const canvas  = document.getElementById('c_' + chartId);
  const tooltip = document.getElementById('ct_' + chartId);
  const wrap    = document.getElementById('cw_' + chartId);
  if (!canvas || !wrap) return;
  const dpr = window.devicePixelRatio || 1;
  const W = wrap.clientWidth;
  if (W < 10) return;
  /* use wrap's explicit height if user resized it, else derive from width */
  const wrapH = wrap.clientHeight;
  const H = wrapH > 50 ? wrapH : Math.max(320, Math.round(W * 0.5));
  canvas.style.width = W+'px'; canvas.style.height = H+'px';
  canvas.width = Math.round(W*dpr); canvas.height = Math.round(H*dpr);
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const cs     = getComputedStyle(document.documentElement);
  const bg     = cs.getPropertyValue('--surface').trim()||'#181c27';
  const gridC  = cs.getPropertyValue('--border').trim()||'#2a2f3d';
  const muteC  = cs.getPropertyValue('--muted').trim()||'#8e8e93';
  const textC  = cs.getPropertyValue('--text').trim()||'#e0e0e0';
  const accent = cs.getPropertyValue('--accent').trim()||'#5b6ef5';
  ctx.fillStyle = bg; ctx.fillRect(0,0,W,H);
  const state = { hitboxes:[], type, points, total:points.reduce((s,p)=>s+Math.max(0,p.value||0),0), dpr };
  chartStateMap.set(chartId, state);
  bindChartHover(chartId, canvas, tooltip);
  if (tooltip) tooltip.classList.add('hidden');
  if (!points.length) { ctx.fillStyle=muteC; ctx.font='14px system-ui,sans-serif'; ctx.fillText('Нет данных',20,44); return; }
  const C = { bg, gridC, muteC, textC, accent };
  switch (type) {
    case 'pie':       return drawChartPie(ctx, points, W, H, metricLabel, C, state);
    case 'scatter':   return drawChartScatter(ctx, points, W, H, metricLabel, xLabel, C, state);
    case 'funnel':    return drawChartFunnel(ctx, points, W, H, metricLabel, C, state);
    case 'waterfall': return drawChartWaterfall(ctx, points, W, H, metricLabel, C, state);
    default:          return drawChartAxes(ctx, points, type, W, H, metricLabel, C, state);
  }
}

/* ── niceYTicks ── */
function niceYTicks(rawMin, rawMax, count = 5) {
  if (rawMin === rawMax) { rawMax = rawMin + 1; }
  const range = rawMax - rawMin, rough = range/count;
  const mag = Math.pow(10, Math.floor(Math.log10(rough)));
  const f = rough/mag;
  const step = f<=1?mag : f<=2?2*mag : f<=5?5*mag : 10*mag;
  const lo = Math.floor(rawMin/step)*step, hi = Math.ceil(rawMax/step)*step;
  const ticks = [];
  for (let v=lo; v<=hi+step*0.001; v+=step) ticks.push(Math.round(v*1e9)/1e9);
  return ticks;
}

/* ── Bar / Line / Area / Histogram ── */
function drawChartAxes(ctx, points, type, W, H, metricLabel, C, state) {
  const { bg, gridC, muteC, textC, accent } = C;
  const pad = { l:76, r:20, t:42, b:88 };
  const plotW = W-pad.l-pad.r, plotH = H-pad.t-pad.b;
  const rawMax = Math.max(...points.map(p=>p.value));
  const rawMin = (type==='line'||type==='area') ? Math.min(...points.map(p=>p.value)) : Math.min(0,...points.map(p=>p.value));
  const ticks  = niceYTicks(rawMin, rawMax);
  const axMin  = ticks[0], axMax = ticks[ticks.length-1];
  const axSpan = Math.max(1e-9, axMax-axMin);
  const yPx    = v => pad.t + plotH*(1-(v-axMin)/axSpan);
  const zeroY  = yPx(0);
  ctx.font = '11px system-ui,sans-serif';
  ticks.forEach(v => {
    const gy = yPx(v);
    if (gy<pad.t-2||gy>pad.t+plotH+2) return;
    ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]);
    ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(W-pad.r,gy); ctx.stroke();
    ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='right';
    ctx.fillText(shortNum(v), pad.l-8, gy+4);
  });
  ctx.textAlign='left';
  ctx.strokeStyle=gridC; ctx.lineWidth=1.5;
  ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,pad.t+plotH); ctx.lineTo(pad.l+plotW,pad.t+plotH); ctx.stroke();
  if (axMin<0&&axMax>0) { ctx.strokeStyle=muteC; ctx.lineWidth=1.5; ctx.beginPath(); ctx.moveTo(pad.l,zeroY); ctx.lineTo(W-pad.r,zeroY); ctx.stroke(); }

  if (type==='bar'||type==='histogram') {
    const slotW=plotW/points.length, barW=Math.max(4,Math.min(64,slotW*0.72));
    const showV=barW>=22;
    ctx.font='bold 10px system-ui,sans-serif';
    points.forEach((p,i) => {
      const color=CHART_PALETTE[type==='histogram'?0:i%CHART_PALETTE.length];
      const top=yPx(Math.max(p.value,0)), bot=yPx(Math.min(p.value,0)), bh=Math.max(1,bot-top);
      const bx=pad.l+i*slotW+(slotW-barW)/2;
      ctx.fillStyle=color; ctx.beginPath();
      if (ctx.roundRect) ctx.roundRect(bx,top,barW,bh,p.value>=0?[3,3,0,0]:[0,0,3,3]); else ctx.rect(bx,top,barW,bh);
      ctx.fill();
      if (showV) {
        ctx.textAlign='center';
        if (top-5>pad.t+10) { ctx.fillStyle=textC; ctx.fillText(shortNum(p.value),bx+barW/2,top-5); }
        else { ctx.fillStyle='#fff'; ctx.fillText(shortNum(p.value),bx+barW/2,top+13); }
      }
      state.hitboxes.push({ x:bx,y:top-2,w:barW,h:bh+4,label:p.label,value:p.value,cx:bx+barW/2,cy:top-16,type:'bar' });
    });
    ctx.textAlign='left';
  } else {
    const n=points.length, xOf=i=>pad.l+(n<2?plotW/2:(i/(n-1))*plotW);
    if (type==='area') {
      ctx.beginPath(); points.forEach((p,i)=>{ const px=xOf(i),py=yPx(p.value); i===0?ctx.moveTo(px,py):ctx.lineTo(px,py); });
      ctx.lineTo(xOf(n-1),zeroY); ctx.lineTo(xOf(0),zeroY); ctx.closePath(); ctx.fillStyle=accent+'33'; ctx.fill();
    }
    ctx.beginPath(); points.forEach((p,i)=>{ const px=xOf(i),py=yPx(p.value); i===0?ctx.moveTo(px,py):ctx.lineTo(px,py); });
    ctx.strokeStyle=accent; ctx.lineWidth=2.5; ctx.lineJoin='round'; ctx.stroke();
    const showDots=n<=120;
    points.forEach((p,i) => {
      const px=xOf(i),py=yPx(p.value);
      if (showDots) { ctx.beginPath(); ctx.arc(px,py,4.5,0,Math.PI*2); ctx.fillStyle=bg; ctx.fill(); ctx.beginPath(); ctx.arc(px,py,3,0,Math.PI*2); ctx.fillStyle=accent; ctx.fill(); }
      state.hitboxes.push({ x:px-12,y:py-12,w:24,h:24,label:p.label,value:p.value,cx:px,cy:py-18,type:'point' });
    });
  }
  ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif';
  const evN=Math.max(1,Math.ceil(points.length/16));
  points.forEach((p,i) => {
    if (i%evN!==0&&i!==points.length-1) return;
    const lx=(type==='bar'||type==='histogram') ? pad.l+i*(plotW/points.length)+plotW/points.length/2
                                                 : pad.l+(points.length<2?plotW/2:(i/(points.length-1))*plotW);
    ctx.save(); ctx.translate(lx,H-56); ctx.rotate(-0.55); ctx.textAlign='right'; ctx.fillText(shortenLabel(String(p.label),18),0,0); ctx.restore();
  });
  ctx.textAlign='left';
  ctx.save(); ctx.translate(13,pad.t+plotH/2); ctx.rotate(-Math.PI/2); ctx.textAlign='center'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText(metricLabel,0,0); ctx.restore(); ctx.textAlign='left';
}

/* ── Scatter ── */
function drawChartScatter(ctx, points, W, H, metricLabel, xLabel, C, state) {
  const { bg, gridC, muteC } = C;
  const pad={l:76,r:20,t:30,b:70};
  const plotW=W-pad.l-pad.r, plotH=H-pad.t-pad.b;
  const xs=points.map(p=>p.x), ys=points.map(p=>p.y);
  const xTicks=niceYTicks(Math.min(...xs),Math.max(...xs)), yTicks=niceYTicks(Math.min(...ys),Math.max(...ys));
  const axXmin=xTicks[0],axXmax=xTicks[xTicks.length-1],axYmin=yTicks[0],axYmax=yTicks[yTicks.length-1];
  const xOf=v=>pad.l+(v-axXmin)/Math.max(1e-9,axXmax-axXmin)*plotW;
  const yOf=v=>pad.t+plotH*(1-(v-axYmin)/Math.max(1e-9,axYmax-axYmin));
  ctx.font='11px system-ui,sans-serif';
  yTicks.forEach(v => { const gy=yOf(v); ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]); ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(W-pad.r,gy); ctx.stroke(); ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='right'; ctx.fillText(shortNum(v),pad.l-8,gy+4); });
  xTicks.forEach(v => { const gx=xOf(v); ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]); ctx.beginPath(); ctx.moveTo(gx,pad.t); ctx.lineTo(gx,pad.t+plotH); ctx.stroke(); ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='center'; ctx.fillText(shortNum(v),gx,pad.t+plotH+14); });
  ctx.textAlign='left';
  ctx.strokeStyle=gridC; ctx.lineWidth=1.5; ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,pad.t+plotH); ctx.lineTo(pad.l+plotW,pad.t+plotH); ctx.stroke();
  const r=Math.max(2.5,Math.min(5,280/Math.sqrt(points.length)));
  points.forEach((p,i) => {
    const px=xOf(p.x), py=yOf(p.y);
    ctx.beginPath(); ctx.arc(px,py,r,0,Math.PI*2); ctx.fillStyle=CHART_PALETTE[i%CHART_PALETTE.length]+'bb'; ctx.fill();
    state.hitboxes.push({ x:px-8,y:py-8,w:16,h:16,label:p.label,value:p.y,cx:px,cy:py-14,type:'point' });
  });
  ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif';
  ctx.save(); ctx.translate(13,pad.t+plotH/2); ctx.rotate(-Math.PI/2); ctx.textAlign='center'; ctx.fillText(metricLabel,0,0); ctx.restore();
  ctx.textAlign='center'; ctx.fillText(xLabel,pad.l+plotW/2,H-8); ctx.textAlign='left';
}

/* ── Waterfall ── */
function drawChartWaterfall(ctx, points, W, H, metricLabel, C, state) {
  const { bg, gridC, muteC, textC } = C;
  const pad={l:76,r:20,t:42,b:88};
  const plotW=W-pad.l-pad.r, plotH=H-pad.t-pad.b;
  let running=0;
  const bars=points.map(p => { const from=running; running+=p.value; return { label:p.label, from, to:running, value:p.value }; });
  const allV=bars.flatMap(b=>[b.from,b.to]);
  const ticks=niceYTicks(Math.min(...allV),Math.max(...allV));
  const axMin=ticks[0],axMax=ticks[ticks.length-1],axSpan=Math.max(1e-9,axMax-axMin);
  const yPx=v=>pad.t+plotH*(1-(v-axMin)/axSpan);
  ctx.font='11px system-ui,sans-serif';
  ticks.forEach(v => { const gy=yPx(v); ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]); ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(W-pad.r,gy); ctx.stroke(); ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='right'; ctx.fillText(shortNum(v),pad.l-8,gy+4); });
  ctx.textAlign='left'; ctx.strokeStyle=gridC; ctx.lineWidth=1.5; ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,pad.t+plotH); ctx.lineTo(pad.l+plotW,pad.t+plotH); ctx.stroke();
  const slotW=plotW/bars.length, barW=Math.max(4,Math.min(60,slotW*0.68));
  ctx.font='bold 10px system-ui,sans-serif';
  bars.forEach((b,i) => {
    const color=b.value>=0?'#34c759':'#ff375f';
    const top=yPx(Math.max(b.from,b.to)), bot=yPx(Math.min(b.from,b.to)), bh=Math.max(1,bot-top);
    const bx=pad.l+i*slotW+(slotW-barW)/2;
    ctx.fillStyle=color; ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(bx,top,barW,bh,[3,3,0,0]); else ctx.rect(bx,top,barW,bh);
    ctx.fill();
    if (i<bars.length-1) { ctx.strokeStyle=muteC; ctx.lineWidth=1; ctx.setLineDash([4,4]); ctx.beginPath(); ctx.moveTo(bx+barW,yPx(b.to)); ctx.lineTo(pad.l+(i+1)*slotW,yPx(b.to)); ctx.stroke(); ctx.setLineDash([]); }
    const lbl=(b.value>=0?'+':'')+shortNum(b.value);
    ctx.textAlign='center';
    if (top-5>pad.t+10) { ctx.fillStyle=textC; ctx.fillText(lbl,bx+barW/2,top-5); } else { ctx.fillStyle='#fff'; ctx.fillText(lbl,bx+barW/2,top+13); }
    state.hitboxes.push({ x:bx,y:top-2,w:barW,h:bh+4,label:b.label,value:b.value,cx:bx+barW/2,cy:top-16,type:'bar' });
  });
  ctx.textAlign='left';
  const evN=Math.max(1,Math.ceil(bars.length/16));
  bars.forEach((b,i) => { if (i%evN!==0&&i!==bars.length-1) return; const lx=pad.l+i*slotW+slotW/2; ctx.save(); ctx.translate(lx,H-56); ctx.rotate(-0.55); ctx.textAlign='right'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText(shortenLabel(b.label,18),0,0); ctx.restore(); });
  ctx.textAlign='left';
}

/* ── Funnel ── */
function drawChartFunnel(ctx, points, W, H, metricLabel, C, state) {
  const { muteC, textC } = C;
  const slices=points.slice(0,20).sort((a,b)=>b.value-a.value);
  if (!slices.length) return;
  const pad={l:20,r:20,t:30,b:20};
  const plotW=W-pad.l-pad.r, plotH=H-pad.t-pad.b;
  const rowH=Math.max(24,plotH/slices.length), maxV=slices[0].value||1;
  slices.forEach((p,i) => {
    const bw=Math.max(16,(p.value/maxV)*plotW*0.92), bx=pad.l+(plotW-bw)/2;
    const by=pad.t+i*rowH+2, bh=Math.max(18,rowH-4);
    ctx.fillStyle=CHART_PALETTE[i%CHART_PALETTE.length]; ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(bx,by,bw,bh,4); else ctx.rect(bx,by,bw,bh);
    ctx.fill();
    ctx.fillStyle='#fff'; ctx.font=`bold ${Math.min(13,bh*0.55)}px system-ui,sans-serif`;
    ctx.textAlign='left'; ctx.textBaseline='middle'; ctx.fillText(shortenLabel(p.label,14),bx+10,by+bh/2);
    ctx.textAlign='right'; ctx.fillText(shortNum(p.value),bx+bw-8,by+bh/2);
    ctx.textBaseline='alphabetic';
    state.hitboxes.push({ x:bx,y:by,w:bw,h:bh,label:p.label,value:p.value,cx:bx+bw/2,cy:by-8,type:'bar' });
  });
  ctx.textAlign='center'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText(metricLabel,W/2,H-6); ctx.textAlign='left';
}

/* ── Pie/Donut ── */
function drawChartPie(ctx, points, W, H, metricLabel, C, state) {
  const { bg, muteC, textC } = C;
  let slices=points.filter(p=>p.value>0);
  if (!slices.length) { ctx.fillStyle=muteC; ctx.font='13px system-ui'; ctx.fillText('Нет данных',20,40); return; }
  if (slices.length>CHART_PALETTE.length) {
    const rest=slices.slice(CHART_PALETTE.length-1).reduce((s,p)=>s+p.value,0);
    slices=[...slices.slice(0,CHART_PALETTE.length-1),{ label:'Остальные', value:rest }];
  }
  const total=slices.reduce((s,p)=>s+p.value,0)||1;
  const cx=W/2,cy=H/2,r=Math.min(W*0.34,H*0.44),innerR=r*0.5;
  let a0=-Math.PI/2;
  slices.forEach((p,i) => {
    const sweep=(p.value/total)*Math.PI*2,a1=a0+sweep,mid=a0+sweep/2;
    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.arc(cx,cy,r,a0,a1); ctx.closePath();
    ctx.fillStyle=CHART_PALETTE[i]; ctx.fill(); ctx.strokeStyle=bg; ctx.lineWidth=2; ctx.stroke();
    if (p.value/total>0.06) { const lr=(r+innerR)/2; ctx.fillStyle='#fff'; ctx.font=`bold ${Math.max(10,Math.round(r*0.1))}px system-ui,sans-serif`; ctx.textAlign='center'; ctx.fillText(`${Math.round(p.value/total*100)}%`,cx+Math.cos(mid)*lr,cy+Math.sin(mid)*lr+4); }
    state.hitboxes.push({ type:'pie',cx,cy,r,innerR,a0,a1,label:p.label,value:p.value,color:CHART_PALETTE[i] });
    a0=a1;
  });
  ctx.beginPath(); ctx.arc(cx,cy,innerR,0,Math.PI*2); ctx.fillStyle=bg; ctx.fill();
  ctx.textAlign='center'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText('итого',cx,cy-7);
  ctx.fillStyle=textC; ctx.font=`bold ${Math.max(13,Math.round(r*0.17))}px system-ui,sans-serif`; ctx.fillText(shortNum(total),cx,cy+12);
  ctx.textAlign='left';
}

/* ── Hover ── */
function bindChartHover(chartId, canvas, tooltip) {
  if (canvas._hoverBound) canvas.removeEventListener('mousemove', canvas._hoverBound);
  if (canvas._leaveBound) canvas.removeEventListener('mouseleave', canvas._leaveBound);
  canvas._hoverBound = ev => {
    const state=chartStateMap.get(chartId);
    if (!state||!tooltip) return;
    const rect=canvas.getBoundingClientRect();
    const hit=findChartHit(ev.clientX-rect.left, ev.clientY-rect.top, state.hitboxes);
    if (!hit) { tooltip.classList.add('hidden'); return; }
    const pct=state.total>0?` (${((Math.max(0,hit.value)/state.total)*100).toFixed(1)}%)`:'';
    tooltip.innerHTML=`<strong>${escHtml(shortenLabel(String(hit.label),40))}</strong><br>${escHtml(fmtNum(Math.round(hit.value*100)/100))}${escHtml(pct)}`;
    tooltip.style.left=`${hit.cx}px`; tooltip.style.top=`${hit.cy}px`;
    tooltip.classList.remove('hidden');
  };
  canvas._leaveBound = () => tooltip?.classList.add('hidden');
  canvas.addEventListener('mousemove', canvas._hoverBound);
  canvas.addEventListener('mouseleave', canvas._leaveBound);
}

function findChartHit(x, y, hitboxes) {
  for (let i=hitboxes.length-1; i>=0; i--) {
    const h=hitboxes[i];
    if (h.type==='pie') {
      const dx=x-h.cx,dy=y-h.cy,dist=Math.sqrt(dx*dx+dy*dy);
      if (dist>h.r||dist<h.innerR) continue;
      let ang=Math.atan2(dy,dx); if (ang<-Math.PI/2) ang+=Math.PI*2;
      if (ang>=h.a0&&ang<h.a1) return h; continue;
    }
    if (x>=h.x&&x<=h.x+h.w&&y>=h.y&&y<=h.y+h.h) return h;
  }
  return null;
}

/* ── Legend ── */
function renderChartLegend(chartId, points, type) {
  const wrap=document.getElementById('cl_'+chartId);
  if (!wrap) return;
  if (!points.length) { wrap.innerHTML=''; return; }
  const total=points.reduce((s,p)=>s+Math.max(0,p.value),0);
  const accent=getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()||'#5b6ef5';
  wrap.innerHTML=points.slice(0,12).map((p,i)=>{
    const color=(type==='pie'||type==='bar'||type==='funnel') ? CHART_PALETTE[i%CHART_PALETTE.length]
              : type==='waterfall' ? (p.value>=0?'#34c759':'#ff375f') : accent;
    const pct=total>0?` · ${((Math.max(0,p.value)/total)*100).toFixed(1)}%`:'';
    return `<div class="analytics-legend-item"><span class="analytics-legend-dot" style="background:${color}"></span><span>${escHtml(shortenLabel(String(p.label),24))}: <b>${escHtml(shortNum(p.value))}</b>${escHtml(pct)}</span></div>`;
  }).join('');
}

/* ── Download ── */
function downloadChartById(id) {
  const canvas=document.getElementById('c_'+id);
  if (!canvas) return;
  const title=analyticsState.charts.find(c=>c.id===id)?.title||'chart';
  const a=document.createElement('a');
  a.href=canvas.toDataURL('image/png');
  a.download=`${title.replace(/[^a-zA-Zа-яёА-ЯЁ0-9]+/g,'_')}.png`;
  a.click();
}

/* ── Data table with color scale ── */
function renderAnalyticsDataTable(rows, cols) {
  const wrap=document.getElementById('analytics-table');
  if (!wrap) return;
  if (!rows.length) { wrap.innerHTML='<div class="empty-state">Нет данных</div>'; return; }
  const colorFmt=document.getElementById('an-colorfmt')?.checked;
  const numCols=colorFmt ? cols.filter(c=>rows.slice(0,50).some(r=>Number.isFinite(Number(r[c]))&&r[c]!=='')) : [];
  const colRange={};
  numCols.forEach(c => { const vals=rows.map(r=>Number(r[c])).filter(Number.isFinite); colRange[c]={ min:Math.min(...vals), max:Math.max(...vals) }; });
  const show=rows.slice(0,500);
  let html='<div class="result-table-wrap"><table class="result-table"><thead><tr>';
  cols.forEach(c => { html+=`<th>${escHtml(c)}</th>`; });
  html+='</tr></thead><tbody>';
  show.forEach(r => {
    html+='<tr>';
    cols.forEach(c => {
      const v=r[c]; let style='';
      if (colorFmt&&colRange[c]) {
        const {min,max}=colRange[c],n=Number(v);
        if (Number.isFinite(n)&&max>min) { const t=(n-min)/(max-min); style=` style="background:hsla(${Math.round(t*120)},70%,45%,0.25)"`; }
      }
      html+=`<td${style}>${escHtml(v==null?'':String(v))}</td>`;
    });
    html+='</tr>';
  });
  html+='</tbody></table></div>';
  if (rows.length>500) html+=`<div class="query-stat">Показаны первые 500 из ${rows.length} строк</div>`;
  wrap.innerHTML=html;
}

/* ── Stats selectors ── */
function fillStatsSelectors(cols) {
  ['st-col-x','st-col-y','st-group-col'].forEach(id => {
    const el=document.getElementById(id); if (!el) return;
    const old=el.value;
    el.innerHTML=`<option value="">—</option>`+cols.map(c=>`<option value="${escAttr(c)}">${escHtml(c)}</option>`).join('');
    if (cols.includes(old)) el.value=old;
  });
}

/* ── Stat method visibility ── */
const ST_CONTROLS = {
  'st-ctrl-xcol':   ['describe','percentile_analysis','freq','corr','covariance','regression','moving_avg','exp_smooth','ci_mean','ttest','ftest','anova','ztest','normalize','rank','bootstrap_mean','permutation_diff'],
  'st-ctrl-ycol':   ['corr','covariance','regression'],
  'st-ctrl-gcol':   ['ttest','ftest','anova','permutation_diff'],
  'st-ctrl-mcols':  ['corr_matrix'],
  'st-ctrl-window': ['moving_avg'],
  'st-ctrl-smooth': ['exp_smooth'],
  'st-ctrl-normm':  ['normalize'],
  'st-ctrl-hyp':    ['ci_mean','ttest','ztest','ftest','anova'],
  'st-ctrl-batch':  ['multi_hyp'],
  'st-ctrl-pctiles':['percentile_analysis'],
};

function onStatMethodChange() {
  const method = document.getElementById('st-method').value;
  Object.entries(ST_CONTROLS).forEach(([id, methods]) => {
    const el = document.getElementById(id);
    if (el) el.style.display = methods.includes(method) ? '' : 'none';
  });
  const xLabel = document.getElementById('st-xcol-label');
  if (xLabel) {
    if (method === 'freq') xLabel.textContent = 'Поле (любое)';
    else if (method === 'rank') xLabel.textContent = 'Числовое поле';
    else xLabel.textContent = 'Поле X / числовое';
  }
  runStatMethod();
}

/* ── Helpers ── */
function isLikelyMetricColumn(name) {
  return /(amount|sum|total|price|cost|qty|quantity|count|revenue|sales|score|age|value|metric|discount)/i.test(String(name||''));
}
function shortenLabel(s, n) { const t=String(s||''); return t.length>n?t.slice(0,Math.max(1,n-1))+'…':t; }
function shortNum(v) {
  const n=Number(v)||0, a=Math.abs(n);
  const fmt=x=>String(parseFloat(x.toFixed(1)));
  if (a>=1e9) return `${fmt(n/1e9)}B`;
  if (a>=1e6) return `${fmt(n/1e6)}M`;
  if (a>=1e3) return `${fmt(n/1e3)}K`;
  return `${Math.round(n*100)/100}`;
}


function runStatMethod() {
  const out = document.getElementById('stats-output');
  if (!out) return;
  const rows = analyticsState.currentRows || [];
  if (!rows.length) { out.textContent = 'Сначала выполните запрос.'; return; }

  const method    = document.getElementById('st-method')?.value || 'describe';
  const xCol      = document.getElementById('st-col-x')?.value || '';
  const yCol      = document.getElementById('st-col-y')?.value || '';
  const groupCol  = document.getElementById('st-group-col')?.value || '';
  const hOp       = document.getElementById('st-h-op')?.value || 'gt';
  const threshold = Number(document.getElementById('st-threshold')?.value || 0);
  const alpha     = Math.max(0.001, Math.min(0.5, Number(document.getElementById('st-alpha')?.value || 0.05)));

  const x = numericCol(rows, xCol);
  const y = numericCol(rows, yCol);

  /* ── 1. Описательная статистика ── */
  if (method === 'describe') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const s = descExt(x);
    if (!s) { out.textContent = 'Нет числовых данных в поле X.'; return; }
    const cv = s.mean ? Math.abs(s.sd / s.mean) * 100 : null;
    const data = [
      ['Поле', xCol], ['N', s.n],
      ['Mean (среднее)', rnd(s.mean)],
      ['Median (медиана)', rnd(s.median)],
      ['Mode (мода)', s.mode !== null ? rnd(s.mode) : '—'],
      ['StdDev (σ)', rnd(s.sd)],
      ['Variance (дисперсия)', rnd(s.variance)],
      ['CV (%)', cv !== null ? rnd(cv) + '%' : '—'],
      ['Min', rnd(s.min)], ['Max', rnd(s.max)], ['Range', rnd(s.max - s.min)],
      ['Q1 (25%)', rnd(s.q1)], ['Q3 (75%)', rnd(s.q3)], ['IQR', rnd(s.iqr)],
      ['Skewness (асимметрия)', rnd(s.skew)],
      ['Kurtosis эксцесс', rnd(s.kurt)],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Квантили и процентили ── */
  if (method === 'percentile_analysis') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    if (!x.length) { out.textContent = 'Нет числовых данных.'; return; }
    const pctRaw = (document.getElementById('st-pctiles')?.value || '1,5,10,25,50,75,90,95,99')
      .split(',').map(v => Number(v.trim())).filter(v => Number.isFinite(v) && v >= 0 && v <= 100);
    const sorted = [...x].sort((a, b) => a - b);
    const data = pctRaw.map(p => [`P${p}`, rnd(pctOf(sorted, p))]);
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml([['Поле', xCol], ['N', x.length], ...data]);
    return;
  }

  /* ── Частотный анализ ── */
  if (method === 'freq') {
    if (!xCol) { out.textContent = 'Выберите поле.'; return; }
    const vals = rows.map(r => String(r[xCol] ?? '(пусто)'));
    const freq = new Map();
    vals.forEach(v => freq.set(v, (freq.get(v) || 0) + 1));
    const total = vals.length;
    const sorted = [...freq.entries()].sort((a, b) => b[1] - a[1]);
    const tableRows = sorted.map(([v, n], i) => ({
      rank: i + 1, value: v, count: n,
      pct: rnd(n / total * 100) + '%',
      cum_pct: rnd(sorted.slice(0, i + 1).reduce((s, [, c]) => s + c, 0) / total * 100) + '%'
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, total, unique: freq.size }, rows: tableRows };
    out.innerHTML = freqHtml(xCol, total, freq.size, tableRows);
    return;
  }

  /* ── Корреляция Пирсона ── */
  if (method === 'corr') {
    if (!xCol || !yCol) { out.textContent = 'Выберите поля X и Y.'; return; }
    const r = pearson(x, y);
    const n = Math.min(x.length, y.length);
    const tStat = n > 2 ? r * Math.sqrt(n - 2) / Math.sqrt(1 - r * r) : 0;
    const p = n > 2 ? 2 * (1 - normalCdf(Math.abs(tStat))) : 1;
    const data = [
      ['Поле X', xCol], ['Поле Y', yCol], ['N пар', n],
      ['Pearson r', rnd(r)], ['r²', rnd(r * r)],
      ['t-stat', rnd(tStat)], ['p-value', rnd(p)],
      ['Интерпретация', corrLabel(r)],
      ['Значимость', p < 0.05 ? 'Значима (p < 0.05)' : 'Не значима (p ≥ 0.05)'],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Матрица корреляций ── */
  if (method === 'corr_matrix') {
    const input = document.getElementById('st-matrix-cols')?.value || '';
    let mCols = input.split(',').map(s => s.trim()).filter(Boolean);
    if (!mCols.length) {
      mCols = (analyticsState.currentCols || []).filter(c => {
        const v = numericCol(rows, c); return v.length >= Math.max(2, rows.length * 0.5);
      });
    }
    if (mCols.length < 2) { out.textContent = 'Нужно минимум 2 числовых поля. Укажите их явно или загрузите данные с числовыми столбцами.'; return; }
    const matrix = mCols.map(c1 => mCols.map(c2 => rnd(pearson(numericCol(rows, c1), numericCol(rows, c2)))));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { cols: mCols.join(', '), n: mCols.length }, rows: [] };
    out.innerHTML = corrMatrixHtml(mCols, matrix);
    return;
  }

  /* ── Ковариация ── */
  if (method === 'covariance') {
    if (!xCol || !yCol) { out.textContent = 'Выберите поля X и Y.'; return; }
    const n = Math.min(x.length, y.length);
    if (n < 2) { out.textContent = 'Нет данных.'; return; }
    const cov = covarianceCalc(x.slice(0, n), y.slice(0, n));
    const sx = descExt(x), sy = descExt(y);
    const data = [
      ['Поле X', xCol], ['Поле Y', yCol], ['N пар', n],
      ['Ковариация (выборочная)', rnd(cov)],
      ['StdDev X', rnd(sx?.sd || 0)], ['StdDev Y', rnd(sy?.sd || 0)],
      ['Pearson r', rnd(pearson(x, y))],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Линейная регрессия ── */
  if (method === 'regression') {
    if (!xCol || !yCol) { out.textContent = 'Выберите поля X и Y.'; return; }
    const reg = linreg(x, y);
    const n = reg.n;
    const data = [
      ['Поле X', xCol], ['Поле Y', yCol],
      ['Модель', `y = ${rnd(reg.a)} + ${rnd(reg.b)} × x`],
      ['R²', rnd(reg.r2)], ['R (корреляция)', rnd(Math.sqrt(reg.r2))],
      ['Intercept (a)', rnd(reg.a)], ['Slope (b)', rnd(reg.b)], ['N пар', n],
    ];
    const preview = x.slice(0, 30).map((xi, i) => ({
      x: rnd(xi), y_actual: rnd(y[i] ?? 0), y_predicted: rnd(reg.a + reg.b * xi),
      residual: rnd((y[i] ?? 0) - (reg.a + reg.b * xi))
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: Object.fromEntries(data), rows: preview };
    out.innerHTML = statHtml(data) + (n > 0 ? statRowsHtml(preview, ['x','y_actual','y_predicted','residual'], ['X','Y факт','Y прогноз','Остаток']) : '');
    return;
  }

  /* ── Скользящее среднее ── */
  if (method === 'moving_avg') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const win = Math.max(2, parseInt(document.getElementById('st-ma-window')?.value || 3));
    if (x.length < win) { out.textContent = `Нужно минимум ${win} значений.`; return; }
    const ma = movingAvg(x, win);
    const tableRows = x.slice(0, 100).map((v, i) => ({
      index: i + 1, original: rnd(v), ma: rnd(ma[i])
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, window: win, n: x.length }, rows: tableRows };
    out.innerHTML = statHtml([['Поле', xCol], ['Окно', win], ['N', x.length],
      ['MA(последнее)', rnd(ma[ma.length - 1])], ['Тренд', ma[ma.length-1] > ma[0] ? '▲ Рост' : '▼ Снижение']]) +
      statRowsHtml(tableRows, ['index','original','ma'], ['#','Значение','MA']);
    return;
  }

  /* ── Экспоненциальное сглаживание ── */
  if (method === 'exp_smooth') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const al = Math.max(0.01, Math.min(0.99, parseFloat(document.getElementById('st-smooth-alpha')?.value || 0.3)));
    if (x.length < 2) { out.textContent = 'Нужно минимум 2 значения.'; return; }
    const sm = expSmooth(x, al);
    const tableRows = x.slice(0, 100).map((v, i) => ({
      index: i + 1, original: rnd(v), smoothed: rnd(sm[i])
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, alpha: al, n: x.length, forecast_next: rnd(sm[sm.length - 1]) }, rows: tableRows };
    out.innerHTML = statHtml([['Поле', xCol], ['α', al], ['N', x.length],
      ['Прогноз (следующий)', rnd(sm[sm.length - 1])]]) +
      statRowsHtml(tableRows, ['index','original','smoothed'], ['#','Значение','Сглаженное']);
    return;
  }

  /* ── Доверительный интервал ── */
  if (method === 'ci_mean') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const s = descExt(x);
    if (!s) { out.textContent = 'Нет данных.'; return; }
    const z95 = 1.96, z99 = 2.576;
    const se = s.sd / Math.sqrt(s.n);
    const data = [
      ['Поле', xCol], ['N', s.n], ['Mean', rnd(s.mean)], ['StdDev', rnd(s.sd)], ['SE', rnd(se)],
      ['95% ДИ', `[${rnd(s.mean - z95 * se)}, ${rnd(s.mean + z95 * se)}]`],
      ['99% ДИ', `[${rnd(s.mean - z99 * se)}, ${rnd(s.mean + z99 * se)}]`],
      ['Гипотеза H1', hypothesisText(s.mean, threshold, hOp)],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── t-тест (Welch) ── */
  if (method === 'ttest') {
    const g = splitGroups(rows, groupCol, xCol);
    if (!g) { out.textContent = 'Для t-теста выберите поле группы (2 категории) и числовое поле X.'; return; }
    const t = welchT(g.a.values, g.b.values);
    const da = descExt(g.a.values), db = descExt(g.b.values);
    const data = [
      ['Группа A', `${g.a.name} (n=${g.a.values.length})`],
      ['Группа B', `${g.b.name} (n=${g.b.values.length})`],
      ['Mean A', rnd(da?.mean || 0)], ['Mean B', rnd(db?.mean || 0)],
      ['StdDev A', rnd(da?.sd || 0)], ['StdDev B', rnd(db?.sd || 0)],
      ['t-статистика', rnd(t.t)], ['p-value (прибл.)', rnd(t.p)],
      ['alpha', alpha],
      ['Заключение', t.p < alpha ? `Различие значимо (p=${rnd(t.p)} < ${alpha})` : `Нет значимого различия (p=${rnd(t.p)} ≥ ${alpha})`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── F-тест (равенство дисперсий) ── */
  if (method === 'ftest') {
    const g = splitGroups(rows, groupCol, xCol);
    if (!g) { out.textContent = 'Для F-теста выберите поле группы (2 категории) и числовое поле X.'; return; }
    const da = descExt(g.a.values), db = descExt(g.b.values);
    if (!da || !db || da.variance === 0 || db.variance === 0) { out.textContent = 'Нет дисперсии в одной из групп.'; return; }
    const F = da.variance / db.variance;
    const df1 = da.n - 1, df2 = db.n - 1;
    const p = fPValue(F, df1, df2);
    const data = [
      ['Группа A', `${g.a.name} (n=${da.n})`], ['Var A', rnd(da.variance)],
      ['Группа B', `${g.b.name} (n=${db.n})`], ['Var B', rnd(db.variance)],
      ['F-статистика', rnd(F)], ['df1', df1], ['df2', df2],
      ['p-value (прибл.)', rnd(p)],
      ['Заключение', p < alpha ? `Дисперсии различаются значимо (p=${rnd(p)} < ${alpha})` : `Дисперсии не различаются (p=${rnd(p)} ≥ ${alpha})`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Однофакторный ANOVA ── */
  if (method === 'anova') {
    if (!xCol || !groupCol) { out.textContent = 'Выберите числовое поле X и поле группы.'; return; }
    const res = oneWayAnova(rows, groupCol, xCol);
    if (!res) { out.textContent = 'Для ANOVA нужно минимум 2 группы с данными.'; return; }
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { F: res.F, p: res.p, k: res.k, N: res.N }, rows: res.groups };
    out.innerHTML = anovaHtml(res);
    return;
  }

  /* ── z-тест ── */
  if (method === 'ztest') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const s = descExt(x);
    if (!s || !s.n) { out.textContent = 'Нет данных.'; return; }
    const z = s.sd > 0 ? (s.mean - threshold) / (s.sd / Math.sqrt(s.n)) : 0;
    let p = 1 - normalCdf(Math.abs(z));
    if (hOp === 'ne') p *= 2;
    else if (hOp === 'lt') p = normalCdf(z);
    else p = 1 - normalCdf(z);
    const data = [
      ['Поле', xCol], ['N', s.n], ['Mean', rnd(s.mean)], ['StdDev', rnd(s.sd)],
      ['Порог H0', threshold], ['H1', hOp === 'gt' ? `mean > ${threshold}` : hOp === 'lt' ? `mean < ${threshold}` : `mean ≠ ${threshold}`],
      ['z-статистика', rnd(z)], ['p-value (одностор./двустор.)', rnd(p)], ['alpha', alpha],
      ['Заключение', p < alpha ? 'H0 отвергается' : 'H0 не отвергается'],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Пакетная проверка гипотез ── */
  if (method === 'multi_hyp') {
    const colsRaw = (document.getElementById('st-multi-cols')?.value || '').split(',').map(s => s.trim()).filter(Boolean);
    const thrRaw  = (document.getElementById('st-multi-thresholds')?.value || '').split(',').map(s => Number(s.trim())).filter(Number.isFinite);
    const bAlpha  = Math.max(0.001, Math.min(0.5, Number(document.getElementById('st-alpha-batch')?.value || 0.05)));
    if (!colsRaw.length || !thrRaw.length) { out.textContent = 'Укажите поля и пороги.'; return; }
    const m = [];
    colsRaw.forEach((c, ci) => {
      const arr = numericCol(rows, c);
      const s = descExt(arr);
      if (!s || !s.n) return;
      thrRaw.forEach((thr, ti) => {
        const z = s.sd > 0 ? (s.mean - thr) / (s.sd / Math.sqrt(s.n)) : 0;
        let p = 1 - normalCdf(Math.abs(z));
        if (hOp === 'ne') p *= 2;
        else if (hOp === 'lt') p = normalCdf(z);
        else p = 1 - normalCdf(z);
        m.push({ id:`${ci+1}.${ti+1}`, field:c, n:s.n, mean:rnd(s.mean), sd:rnd(s.sd),
          threshold:thr, op:hOp, z_stat:rnd(z), p_value:rnd(p),
          decision: p < bAlpha ? '✓ reject H0' : '— fail to reject' });
      });
    });
    if (!m.length) { out.textContent = 'Нет валидных данных для пакетного теста.'; return; }
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { alpha: bAlpha, count: m.length }, rows: m };
    out.innerHTML = statRowsHtml(m, ['id','field','n','mean','sd','threshold','op','z_stat','p_value','decision'],
      ['#','Поле','N','Mean','SD','Порог','H1','z','p','Решение']);
    return;
  }

  /* ── Нормализация ── */
  if (method === 'normalize') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const normMethod = document.getElementById('st-norm-method')?.value || 'zscore';
    const s = descExt(x);
    if (!s || !s.n) { out.textContent = 'Нет данных.'; return; }
    let normed;
    if (normMethod === 'zscore') {
      normed = s.sd > 0 ? x.map(v => (v - s.mean) / s.sd) : x.map(() => 0);
    } else {
      const range = s.max - s.min;
      normed = range > 0 ? x.map(v => (v - s.min) / range) : x.map(() => 0);
    }
    const sn = descExt(normed);
    const tableRows = x.slice(0, 60).map((v, i) => ({ index: i + 1, original: rnd(v), normalized: rnd(normed[i]) }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, method: normMethod, mean_before: rnd(s.mean), sd_before: rnd(s.sd),
        mean_after: rnd(sn?.mean || 0), sd_after: rnd(sn?.sd || 0) }, rows: tableRows };
    out.innerHTML = statHtml([
      ['Поле', xCol], ['Метод', normMethod === 'zscore' ? 'Z-оценка' : 'Min-Max'],
      ['Mean до', rnd(s.mean)], ['SD до', rnd(s.sd)],
      ['Mean после', rnd(sn?.mean || 0)], ['SD после', rnd(sn?.sd || 0)],
      ['Min после', rnd(sn?.min || 0)], ['Max после', rnd(sn?.max || 0)],
    ]) + statRowsHtml(tableRows, ['index','original','normalized'], ['#','Исходное','Нормализованное']);
    return;
  }

  /* ── Ранжирование ── */
  if (method === 'rank') {
    if (!xCol) { out.textContent = 'Выберите числовое поле.'; return; }
    const indexed = x.map((v, i) => ({ i, v })).sort((a, b) => b.v - a.v);
    indexed.forEach((item, ri) => { item.rank = ri + 1; });
    indexed.sort((a, b) => a.i - b.i);
    const tableRows = indexed.slice(0, 100).map(item => ({
      row: item.i + 1, value: rnd(item.v), rank: item.rank,
      pct_rank: rnd((1 - (item.rank - 1) / (x.length - 1 || 1)) * 100) + '%'
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, n: x.length }, rows: tableRows };
    out.innerHTML = statHtml([['Поле', xCol], ['N', x.length]]) +
      statRowsHtml(tableRows, ['row','value','rank','pct_rank'], ['Строка','Значение','Ранг','Процентиль']);
    return;
  }

  /* ── Bootstrap для среднего ── */
  if (method === 'bootstrap_mean') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const B = 1000;
    if (x.length < 5) { out.textContent = 'Для bootstrap нужно минимум 5 наблюдений.'; return; }
    const means = [];
    for (let b = 0; b < B; b++) {
      let s = 0;
      for (let i = 0; i < x.length; i++) s += x[(Math.random() * x.length) | 0];
      means.push(s / x.length);
    }
    means.sort((a, b) => a - b);
    const lo = means[Math.floor(0.025 * B)], hi = means[Math.floor(0.975 * B)];
    const mu = means.reduce((s, v) => s + v, 0) / means.length;
    const data = [
      ['Поле', xCol], ['N', x.length], ['Bootstrap итераций', B],
      ['Mean (bootstrap)', rnd(mu)], ['95% ДИ', `[${rnd(lo)}, ${rnd(hi)}]`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: Object.fromEntries(data),
      rows: means.slice(0, 200).map((v, i) => ({ sample: i + 1, mean: rnd(v) })) };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Permutation test ── */
  if (method === 'permutation_diff') {
    const g = splitGroups(rows, groupCol, xCol);
    if (!g) { out.textContent = 'Выберите поле группы (2 группы) и числовое поле X.'; return; }
    const A = g.a.values, B = g.b.values;
    const obs = meanOf(A) - meanOf(B);
    const all = A.concat(B);
    const nA = A.length;
    const R = 1000;
    let extreme = 0;
    for (let r = 0; r < R; r++) {
      shuffleInPlace(all);
      if (Math.abs(meanOf(all.slice(0, nA)) - meanOf(all.slice(nA))) >= Math.abs(obs)) extreme++;
    }
    const p = (extreme + 1) / (R + 1);
    const data = [
      ['Группа A', g.a.name], ['N(A)', A.length], ['Mean A', rnd(meanOf(A))],
      ['Группа B', g.b.name], ['N(B)', B.length], ['Mean B', rnd(meanOf(B))],
      ['Наблюд. разница mean', rnd(obs)], ['Перестановок', R],
      ['p-value', rnd(p)],
      ['Заключение', p < alpha ? `Различие значимо (p=${rnd(p)} < ${alpha})` : `Нет значимого различия (p=${rnd(p)} ≥ ${alpha})`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }
}

function numericCol(rows, col) {
  if (!col) return [];
  return rows.map(r => Number(r[col])).filter(Number.isFinite);
}

function pctOf(sorted, p) {
  if (!sorted.length) return 0;
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return sorted[lo] + (idx - lo) * ((sorted[hi] ?? sorted[lo]) - sorted[lo]);
}

function descExt(arr) {
  if (!arr || !arr.length) return null;
  const n = arr.length;
  const sorted = [...arr].sort((a, b) => a - b);
  const mean = arr.reduce((s, v) => s + v, 0) / n;
  const variance = n > 1 ? arr.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1) : 0;
  const sd = Math.sqrt(variance);
  const median = pctOf(sorted, 50);
  const q1 = pctOf(sorted, 25), q3 = pctOf(sorted, 75);
  const freq = new Map();
  arr.forEach(v => freq.set(v, (freq.get(v) || 0) + 1));
  let mode = null, maxF = 0;
  freq.forEach((f, v) => { if (f > maxF) { maxF = f; mode = v; } });
  if (maxF === 1) mode = null;
  const skew = sd > 0 ? arr.reduce((s, v) => s + ((v - mean) / sd) ** 3, 0) / n : 0;
  const kurt = sd > 0 ? arr.reduce((s, v) => s + ((v - mean) / sd) ** 4, 0) / n - 3 : 0;
  return { n, mean, variance, sd, min: sorted[0], max: sorted[n - 1], median, q1, q3, iqr: q3 - q1, mode, skew, kurt };
}

/* Keep desc as alias for backward compat with old callers */
function desc(arr) { return descExt(arr) || { n: 0, mean: 0, sd: 0, min: 0, max: 0 }; }

function pearson(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return 0;
  const x = a.slice(0, n), y = b.slice(0, n);
  const mx = x.reduce((s, v) => s + v, 0) / n;
  const my = y.reduce((s, v) => s + v, 0) / n;
  let num = 0, dx = 0, dy = 0;
  for (let i = 0; i < n; i++) {
    const vx = x[i] - mx, vy = y[i] - my;
    num += vx * vy; dx += vx * vx; dy += vy * vy;
  }
  return dx && dy ? num / Math.sqrt(dx * dy) : 0;
}

function covarianceCalc(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return 0;
  const mx = a.slice(0,n).reduce((s,v)=>s+v,0)/n;
  const my = b.slice(0,n).reduce((s,v)=>s+v,0)/n;
  return a.slice(0,n).reduce((s,v,i) => s + (v-mx)*(b[i]-my), 0) / (n-1);
}

function linreg(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return { a: 0, b: 0, r2: 0, n };
  const x = a.slice(0, n), y = b.slice(0, n);
  const mx = x.reduce((s, v) => s + v, 0) / n;
  const my = y.reduce((s, v) => s + v, 0) / n;
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) { num += (x[i] - mx) * (y[i] - my); den += (x[i] - mx) ** 2; }
  const b1 = den ? num / den : 0;
  const a0 = my - b1 * mx;
  const r = pearson(x, y);
  return { a: a0, b: b1, r2: r * r, n };
}

function movingAvg(arr, win) {
  return arr.map((_, i) => {
    const sl = arr.slice(Math.max(0, i - win + 1), i + 1);
    return sl.reduce((s, v) => s + v, 0) / sl.length;
  });
}

function expSmooth(arr, al) {
  if (!arr.length) return [];
  const r = [arr[0]];
  for (let i = 1; i < arr.length; i++) r.push(al * arr[i] + (1 - al) * r[i - 1]);
  return r;
}

function oneWayAnova(rows, groupCol, valCol) {
  if (!groupCol || !valCol) return null;
  const groups = new Map();
  rows.forEach(r => {
    const k = String(r[groupCol] ?? '');
    const v = Number(r[valCol]);
    if (!Number.isFinite(v)) return;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(v);
  });
  const gl = [...groups.entries()].filter(([, v]) => v.length > 0);
  const k = gl.length;
  if (k < 2) return null;
  const allVals = gl.flatMap(([, v]) => v);
  const N = allVals.length;
  const grandMean = allVals.reduce((s, v) => s + v, 0) / N;
  let SSB = 0, SSW = 0;
  gl.forEach(([, v]) => {
    const n = v.length, gm = v.reduce((s, x) => s + x, 0) / n;
    SSB += n * (gm - grandMean) ** 2;
    SSW += v.reduce((s, x) => s + (x - gm) ** 2, 0);
  });
  const dfB = k - 1, dfW = N - k;
  const MSB = SSB / dfB, MSW = dfW > 0 ? SSW / dfW : 0;
  const F = MSW > 0 ? MSB / MSW : 0;
  const p = fPValue(F, dfB, dfW);
  return {
    k, N, SSB: rnd(SSB), SSW: rnd(SSW), dfB, dfW,
    MSB: rnd(MSB), MSW: rnd(MSW), F: rnd(F), p: rnd(p),
    significant: p < 0.05,
    groups: gl.map(([name, v]) => {
      const s = descExt(v);
      return { group: name, n: v.length, mean: rnd(s?.mean||0), sd: rnd(s?.sd||0), min: rnd(s?.min||0), max: rnd(s?.max||0) };
    })
  };
}

function splitGroups(rows, groupCol, valCol) {
  if (!groupCol || !valCol) return null;
  const m = new Map();
  rows.forEach(r => {
    const k = String(r[groupCol] ?? '(empty)');
    const v = Number(r[valCol]);
    if (!Number.isFinite(v)) return;
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(v);
  });
  const keys = [...m.keys()].filter(k => m.get(k).length > 1);
  if (keys.length < 2) return null;
  const a = keys[0], b = keys[1];
  return { a: { name: a, values: m.get(a) }, b: { name: b, values: m.get(b) } };
}

function welchT(a, b) {
  const da = descExt(a) || {mean:0,sd:0,n:0}, db = descExt(b) || {mean:0,sd:0,n:0};
  const se = Math.sqrt((da.sd ** 2 / (da.n||1)) + (db.sd ** 2 / (db.n||1)));
  const t = se ? (da.mean - db.mean) / se : 0;
  const p = 2 * (1 - normalCdf(Math.abs(t)));
  return { t, p };
}

function normalCdf(x) {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

function erf(x) {
  const s = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * ax);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t) * Math.exp(-ax * ax);
  return s * y;
}

function chiSqPValue(chi2, df) {
  /* Wilson-Hilferty normal approximation for chi-square p-value */
  if (df <= 0 || chi2 < 0) return 1;
  const x = Math.pow(chi2 / df, 1 / 3);
  const mu = 1 - 2 / (9 * df);
  const sigma = Math.sqrt(2 / (9 * df));
  return 1 - normalCdf((x - mu) / sigma);
}

function fPValue(F, df1, df2) {
  /* F-distribution p-value via chi-square approximation */
  if (F <= 0 || df1 <= 0 || df2 <= 0) return 1;
  return chiSqPValue(F * df1, df1);
}

function corrLabel(r) {
  const ar = Math.abs(r);
  if (ar >= 0.8) return 'Сильная связь';
  if (ar >= 0.5) return 'Умеренная связь';
  if (ar >= 0.3) return 'Слабая связь';
  return 'Связь почти отсутствует';
}

function hypothesisText(mean, threshold, op) {
  const ok = (op === 'gt' && mean > threshold) || (op === 'lt' && mean < threshold) || (op === 'ne' && Math.abs(mean - threshold) > 1e-9);
  return ok ? 'H1 поддерживается на уровне среднего' : 'H1 не поддерживается на уровне среднего';
}

function statHtml(rows) {
  return `<div class="result-table-wrap"><table class="result-table st-kv-table"><tbody>${
    rows.map(([k, v]) => `<tr><td class="st-key">${escHtml(String(k))}</td><td>${escHtml(String(v))}</td></tr>`).join('')
  }</tbody></table></div>`;
}

function statRowsHtml(rows, columns, headers) {
  const hdrs = headers || columns;
  let html = '<div class="result-table-wrap" style="margin-top:0.75rem"><table class="result-table"><thead><tr>';
  html += hdrs.map(c => `<th>${escHtml(c)}</th>`).join('');
  html += '</tr></thead><tbody>';
  rows.forEach(r => {
    html += '<tr>' + columns.map(c => `<td>${escHtml(String(r[c] ?? ''))}</td>`).join('') + '</tr>';
  });
  html += '</tbody></table></div>';
  return html;
}

function corrMatrixHtml(cols, matrix) {
  const n = cols.length;
  let html = '<div class="result-table-wrap" style="margin-top:0.5rem"><table class="result-table corr-matrix">';
  html += '<thead><tr><th></th>' + cols.map(c => `<th>${escHtml(c)}</th>`).join('') + '</tr></thead><tbody>';
  matrix.forEach((row, i) => {
    html += `<tr><td class="st-key">${escHtml(cols[i])}</td>` + row.map((v, j) => {
      const cls = i === j ? 'corr-diag' : v > 0 ? 'corr-pos' : 'corr-neg';
      const intensity = Math.round(Math.abs(v) * 180);
      const bg = i === j ? `rgba(91,110,245,0.15)` :
        v > 0 ? `rgba(52,199,89,${(Math.abs(v)*0.4).toFixed(2)})` :
                `rgba(255,55,95,${(Math.abs(v)*0.4).toFixed(2)})`;
      return `<td class="${cls}" style="background:${bg};text-align:center">${v}</td>`;
    }).join('') + '</tr>';
  });
  html += '</tbody></table></div>';
  return html;
}

function freqHtml(field, total, unique, rows) {
  const maxCount = rows[0]?.count || 1;
  let html = `<div class="st-section-title">Поле: ${escHtml(field)} · Всего: ${total} · Уникальных: ${unique}</div>`;
  html += '<div class="result-table-wrap"><table class="result-table"><thead><tr>';
  html += '<th>Значение</th><th>Кол-во</th><th>%</th><th>Накоп.%</th><th style="width:120px">Частота</th>';
  html += '</tr></thead><tbody>';
  rows.forEach(r => {
    const barW = Math.round(r.count / maxCount * 100);
    html += `<tr>
      <td>${escHtml(r.value)}</td>
      <td style="text-align:right">${r.count}</td>
      <td style="text-align:right">${r.pct}</td>
      <td style="text-align:right">${r.cum_pct}</td>
      <td><div class="freq-bar" style="width:${barW}%"></div></td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  return html;
}

function anovaHtml(res) {
  let html = `<div class="st-section-title">Однофакторный ANOVA · k=${res.k} групп · N=${res.N}</div>`;
  const anovaTable = [
    { source:'Между группами', SS:res.SSB, df:res.dfB, MS:res.MSB, F:res.F, p:res.p },
    { source:'Внутри групп',   SS:res.SSW, df:res.dfW, MS:res.MSW, F:'',   p:'' },
  ];
  html += statRowsHtml(anovaTable, ['source','SS','df','MS','F','p'],
    ['Источник вариации','SS','df','MS','F','p-value']);
  html += `<div class="st-conclusion ${res.significant ? 'st-sig' : 'st-ns'}">`;
  html += res.significant
    ? `✓ Значимые различия между группами (F=${res.F}, p=${res.p} < 0.05)`
    : `— Значимых различий не обнаружено (F=${res.F}, p=${res.p} ≥ 0.05)`;
  html += '</div>';
  html += '<div class="st-section-title" style="margin-top:0.75rem">Статистика по группам</div>';
  html += statRowsHtml(res.groups, ['group','n','mean','sd','min','max'],
    ['Группа','N','Mean','SD','Min','Max']);
  return html;
}

function exportStatsReport(format) {
  if (!lastStatsReport) {
    showToast('Сначала выполните статистический расчет', 'warn');
    return;
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  if (format === 'json') {
    const text = JSON.stringify(lastStatsReport, null, 2);
    downloadText(`stats-report-${stamp}.json`, 'application/json', text);
    return;
  }
  const summaryRows = Object.entries(lastStatsReport.summary || {}).map(([k, v]) => ({ section: 'summary', key: k, value: v }));
  const detailRows = (lastStatsReport.rows || []).map(r => ({ section: 'detail', ...r }));
  const all = summaryRows.concat(detailRows);
  if (!all.length) {
    downloadText(`stats-report-${stamp}.csv`, 'text/csv', 'section,key,value\n');
    return;
  }
  const cols = Array.from(all.reduce((s, r) => {
    Object.keys(r).forEach(k => s.add(k));
    return s;
  }, new Set()));
  const lines = [cols.join(',')];
  all.forEach(r => {
    lines.push(cols.map(c => csvEscape(r[c])).join(','));
  });
  downloadText(`stats-report-${stamp}.csv`, 'text/csv', lines.join('\n'));
}

/* ── Save stats result to backend ── */
const STAT_METHOD_LABELS = {
  describe: 'Описательная статистика',
  percentile_analysis: 'Квантили и процентили',
  freq: 'Частотный анализ',
  corr: 'Корреляция Пирсона',
  corr_matrix: 'Матрица корреляций',
  covariance: 'Ковариация',
  regression: 'Линейная регрессия',
  moving_avg: 'Скользящее среднее',
  exp_smooth: 'Экспоненциальное сглаживание',
  ci_mean: 'Доверительный интервал',
  ttest: 't-тест',
  ftest: 'F-тест',
  anova: 'ANOVA',
  ztest: 'z-тест',
  multi_hyp: 'Пакетная проверка гипотез',
  normalize: 'Нормализация',
  rank: 'Ранжирование',
  bootstrap_mean: 'Bootstrap',
  permutation_diff: 'Permutation test',
};

function saveStatsResult() {
  if (!lastStatsReport) {
    showToast('Сначала выполните статистический расчёт', 'warn');
    return;
  }

  /* Build a default name */
  const method = lastStatsReport.method || 'stats';
  const label  = STAT_METHOD_LABELS[method] || method;
  const xCol   = document.getElementById('st-col-x')?.value || '';
  const yCol   = document.getElementById('st-col-y')?.value || '';
  const fieldPart = [xCol, yCol].filter(Boolean).join(' / ');
  const sql    = (document.getElementById('an-sql')?.value || '').trim();
  const defaultName = fieldPart ? `${label}: ${fieldPart}` : label;

  const nameEl = document.getElementById('save-result-name');
  if (nameEl) nameEl.value = defaultName;

  /* Store pending type so confirmSaveResult knows to use stats data */
  document.getElementById('save-result-modal')._saveType = 'stats';
  document.getElementById('save-result-modal').style.display = 'flex';
  setTimeout(() => nameEl?.select(), 50);
}

function _statsReportToTable(report) {
  /* Convert lastStatsReport {summary, rows} → {columns, rows} for the API */
  const summaryEntries = Object.entries(report.summary || {});
  const detailRows     = report.rows || [];

  if (detailRows.length) {
    /* Tabular result (correlation matrix, ANOVA, normalize, rank, MA, regression…) */
    const detailCols = Array.from(detailRows.reduce((s, r) => {
      Object.keys(r).forEach(k => s.add(k)); return s;
    }, new Set()));

    const allCols  = summaryEntries.length
      ? ['параметр', 'значение', ...detailCols]   /* mixed: summary block + detail table */
      : detailCols;

    const allRows = [];
    /* summary as leading rows */
    summaryEntries.forEach(([k, v]) => {
      const row = new Array(allCols.length).fill('');
      row[0] = k; row[1] = String(v ?? '');
      allRows.push(row);
    });
    /* separator if both present */
    if (summaryEntries.length && detailRows.length) {
      allRows.push(new Array(allCols.length).fill('---'));
    }
    /* detail rows */
    detailRows.forEach(r => {
      const row = new Array(allCols.length).fill('');
      detailCols.forEach((c, i) => {
        const offset = summaryEntries.length ? 2 : 0;
        row[offset + i] = String(r[c] ?? '');
      });
      allRows.push(row);
    });
    return { columns: allCols, rows: allRows };
  }

  /* Summary-only result (describe, percentiles, t-test…) */
  return {
    columns: ['параметр', 'значение'],
    rows: summaryEntries.map(([k, v]) => [k, String(v ?? '')]),
  };
}

function downloadText(filename, mime, text) {
  const blob = new Blob([text], { type: `${mime};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 500);
  showToast(`Экспортирован отчет: ${filename}`, 'ok');
}

function csvEscape(v) {
  const s = String(v ?? '');
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function meanOf(arr) {
  return arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : 0;
}

function shuffleInPlace(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0;
    const t = arr[i]; arr[i] = arr[j]; arr[j] = t;
  }
}

function rnd(v) {
  return Math.round((Number(v) || 0) * 10000) / 10000;
}


/* ── API Keys ── */
async function loadApiKeys() {
  const el = document.getElementById('settings-apikeys-list');
  if (!el) return;
  try {
    const keys = await apiFetch('/api/auth/apikeys');
    if (!Array.isArray(keys) || !keys.length) {
      el.innerHTML = '<div class="settings-loading">Ключей нет</div>';
      return;
    }
    const roleLabel = { admin: 'admin', analyst: 'analyst', viewer: 'viewer' };
    el.innerHTML = keys.map(k => {
      const keyId = escAttr(k.key || '');
      const created = k.created_at ? new Date(k.created_at * 1000).toLocaleDateString('ru') : '—';
      return `
        <div class="settings-row" id="akrow-${keyId}">
          <div class="settings-row-label">
            <div class="settings-row-title" style="font-family:var(--mono);font-size:.85rem">${escHtml(k.key || '—')}</div>
            <div class="settings-row-desc">
              user: <b>${escHtml(k.user_id || '—')}</b> · роль: <b>${escHtml(k.role || '—')}</b> · создан: ${escHtml(created)}
            </div>
          </div>
          <button class="btn btn-sm btn-danger" onclick="deleteApiKey('${keyId}')">Удалить</button>
        </div>`;
    }).join('');
  } catch(err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

async function createApiKey() {
  const userEl = document.getElementById('new-apikey-user');
  const roleEl = document.getElementById('new-apikey-role');
  const user_id = (userEl?.value || '').trim();
  const role    = roleEl?.value || 'analyst';
  if (!user_id) { showToast('Введите user_id', 'warn'); return; }
  try {
    const res = await apiPost('/api/auth/apikeys', { user_id, role });
    showToast(`Ключ создан: ${res.key || '—'}`, 'ok');
    if (userEl) userEl.value = '';
    loadApiKeys();
  } catch(err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}

async function deleteApiKey(key) {
  if (!confirm('Удалить API-ключ?')) return;
  try {
    await apiDelete(`/api/auth/apikeys/${encodeURIComponent(key)}`);
    showToast('Ключ удалён', 'ok');
    const row = document.getElementById(`akrow-${key}`);
    if (row) row.remove();
  } catch(err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}

/* ═══════════════════════════════════════════════════
   SETTINGS
═══════════════════════════════════════════════════ */
async function loadSettings() {
  applyPrefsToSettingsForm();

  /* ── Server stat cards ── */
  const grid = document.getElementById('server-info-grid');
  try {
    const h = await apiFetch('/health');
    const m = h.metrics || {};
    const cards = [
      ['Статус',              `<span class="badge badge-ok">${escHtml(h.status||'ok')}</span>`],
      ['Версия',              escHtml(h.version || '1.0.0')],
      ['Аптайм',              fmtUptime(m.uptime || 0)],
      ['Строк загружено',     fmtNum(m.total_rows || 0)],
      ['Запросов выполнено',  fmtNum(m.total_queries || 0)],
      ['Запусков конвейеров', fmtNum(m.total_pipelines_run || 0)],
    ];
    grid.innerHTML = cards.map(([label, val]) =>
      `<div class="stat-card"><div class="stat-val" style="font-size:1.25rem">${val}</div><div class="stat-label">${label}</div></div>`
    ).join('');
  } catch (err) {
    grid.innerHTML = `<div class="settings-loading" style="color:var(--red)">Нет связи с сервером</div>`;
  }

  /* ── Tables management ── */
  const tbl = document.getElementById('settings-tables-list');
  try {
    const tables = await apiFetch('/api/tables');
    if (!tables.length) {
      tbl.innerHTML = '<div class="settings-loading">Таблиц нет</div>';
    } else {
      tbl.innerHTML = tables.map(t => `
        <div class="settings-row" id="strow-${escAttr(t.name)}">
          <div class="settings-row-label">
            <div class="settings-row-title">${escHtml(t.name)}</div>
            <div class="settings-row-desc">${fmtNum(t.rows||0)} строк · ${(t.columns||[]).length} столбцов ·
              <span class="source-badge ${t.source==='ingest'?'ingest':'pipeline'}">${t.source==='ingest'?'загрузка':'конвейер'}</span>
            </div>
          </div>
          <button class="btn btn-sm btn-danger" onclick="settingsDropTable('${escAttr(t.name)}')">Удалить</button>
        </div>`).join('');
    }
  } catch (err) {
    tbl.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
  loadApiKeys();
  loadClusterStatus();
}

async function settingsDropTable(name) {
  if (!confirm(`Удалить таблицу "${name}"? Это действие необратимо.`)) return;
  try {
    await apiFetch(`/api/tables/${encodeURIComponent(name)}`, 'DELETE');
    showToast(`Таблица "${name}" удалена`, 'ok');
    const row = document.getElementById(`strow-${name}`);
    if (row) row.remove();
  } catch (err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}


/* ── Saved Results ── */
async function loadSavedResults() {
  try {
    const list = await apiGet('/api/analytics/results');
    const el = document.getElementById('an-saved-list');
    if (!el) return;
    if (!list.length) {
      el.innerHTML = '<div class="qs-empty">Нет сохранённых результатов.<br>Нажмите «+ Добавить аналитику» чтобы создать первый анализ.</div>';
      return;
    }
    el.innerHTML = '';
    list.forEach(r => {
      const card = document.createElement('div');
      card.className = 'table-card';
      card.id = `sr_${r.id}`;
      const cols = Array.isArray(r.columns_json) ? r.columns_json
        : (r.columns_json ? JSON.parse(r.columns_json) : []);
      const pills = cols.map(c => `<span class="col-pill">${escHtml(c)}</span>`).join('');
      card.innerHTML = `
        <div class="table-card-head">
          <h3>${escHtml(r.name)}</h3>
          <button class="btn btn-sm btn-danger" title="Удалить">✕</button>
        </div>
        <div class="meta">${r.row_count} строк · ${new Date(r.created_at*1000).toLocaleDateString('ru')}</div>
        ${pills ? `<div class="col-list">${pills}</div>` : ''}
      `;
      card.querySelector('button').addEventListener('click', ev => {
        ev.stopPropagation();
        deleteSavedResult(r.id);
      });
      card.onclick = () => openSavedResult(r.id);
      el.appendChild(card);
    });
  } catch(e) { console.error('loadSavedResults', e); }
}

function saveAnalyticsResult() {
  if (!analyticsState.currentRows.length) return;
  const nameEl = document.getElementById('save-result-name');
  const sql = (document.getElementById('an-sql')?.value || '').trim();
  if (nameEl) nameEl.value = sql.replace(/^SELECT\s+/i,'').substring(0,60);
  document.getElementById('save-result-modal').style.display = 'flex';
  setTimeout(() => nameEl?.focus(), 50);
}

function closeSaveModal() {
  const m = document.getElementById('save-result-modal');
  m.style.display = 'none';
  delete m._saveType;
}

async function confirmSaveResult() {
  const name = (document.getElementById('save-result-name')?.value || '').trim();
  if (!name) { document.getElementById('save-result-name')?.focus(); return; }
  const modal   = document.getElementById('save-result-modal');
  const saveType = modal._saveType || 'query';
  const sql = (document.getElementById('an-sql')?.value || '').trim();

  let columns, rows;
  if (saveType === 'stats' && lastStatsReport) {
    const tbl = _statsReportToTable(lastStatsReport);
    columns = tbl.columns;
    rows    = tbl.rows;
  } else {
    columns = analyticsState.currentCols;
    rows    = analyticsState.currentRows.map(r => columns.map(c => r[c] ?? ''));
  }

  try {
    await apiPost('/api/analytics/results', { name, sql, columns, rows });
    closeSaveModal();
    showToast(`Сохранено: "${name}"`, 'ok');
    closeAnalyticsEditor();
    loadSavedResults();
  } catch(e) { alert('Ошибка при сохранении: ' + e.message); }
}

async function openSavedResult(id) {
  try {
    const r = await apiGet(`/api/analytics/results/${id}`);
    const cols = JSON.parse(r.columns_json || '[]');
    const rawRows = JSON.parse(r.rows_json || '[]');
    const rows = rawRows.map(arr => {
      const obj = {};
      cols.forEach((c,i) => { obj[c] = arr[i] ?? ''; });
      return obj;
    });

    document.getElementById('an-view-title').textContent = r.name;
    document.getElementById('an-view-meta').textContent =
      `${r.row_count} строк · ${new Date(r.created_at * 1000).toLocaleDateString('ru')}`;
    document.getElementById('an-view-sql').value = r.sql_text || '';
    document.getElementById('an-view-status').textContent = '';

    renderViewCharts(cols, rows);
    renderViewDataTable(rows, cols);

    showAnScreen('view');
  } catch(e) { alert('Ошибка загрузки: ' + e.message); }
}

function renderViewDataTable(rows, cols) {
  const card = document.getElementById('an-view-data-card');
  const el   = document.getElementById('an-view-table');
  if (!rows.length) { card.style.display = 'none'; return; }
  card.style.display = '';
  const head = cols.map(c => `<th>${escHtml(c)}</th>`).join('');
  const body = rows.slice(0, 500).map(r =>
    `<tr>${cols.map(c => `<td>${escHtml(String(r[c] ?? ''))}</td>`).join('')}</tr>`
  ).join('');
  el.innerHTML = `<div class="table-scroll"><table class="result-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

async function runViewQuery() {
  const sql = document.getElementById('an-view-sql').value.trim();
  if (!sql) return;
  const status = document.getElementById('an-view-status');
  status.textContent = 'Выполняется…';
  const t0 = performance.now();
  try {
    const data = await apiPost('/api/tables/query', { sql });
    const cols = data.columns || [];
    const rows = (data.rows || []).map(r => {
      const obj = {}, vals = Array.isArray(r) ? r : cols.map(c => r[c]);
      cols.forEach((c, i) => { obj[c] = vals[i]; });
      return obj;
    });
    status.textContent = `${rows.length} строк · ${(performance.now()-t0).toFixed(0)} мс`;
    renderViewDataTable(rows, cols);
  } catch(err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/,'')); if (j.error) msg = j.error; } catch(_) {}
    status.textContent = `Ошибка: ${msg}`;
  }
}

function renderViewCharts(cols, rows) {
  /* reuse analyticsState temporarily for chart rendering */
  const prevRows = analyticsState.currentRows;
  const prevCols = analyticsState.currentCols;
  analyticsState.currentRows = rows;
  analyticsState.currentCols = cols;

  const grid = document.getElementById('an-view-charts');
  grid.innerHTML = '';

  const numCols = cols.filter(c => rows.slice(0,20).some(r => Number.isFinite(Number(r[c])) && r[c] !== ''));
  const lblCols = cols.filter(c => !numCols.includes(c));
  const xCol    = lblCols.find(c => !/^id$|_id$/i.test(c)) || lblCols[0] || cols[0] || '';
  const nonId   = numCols.filter(c => !/^id$|_id$/i.test(c));
  const yCol    = nonId[0] || numCols[0] || cols[1] || '';
  const dateCol = lblCols.find(c => /date|time|month|year|day|week/i.test(c));

  const charts = [{ type: 'bar', xCol, yCol }];
  if (dateCol && yCol)        charts.push({ type: 'area',    xCol: dateCol, yCol });
  else if (nonId.length >= 2) charts.push({ type: 'scatter', xCol: nonId[0], yCol: nonId[1] });

  charts.forEach(({ type, xCol, yCol }, i) => {
    const cid = `vc${i}`;
    const wrap = document.createElement('div');
    wrap.className = 'card an-view-chart-card';
    /* drawChartOnCanvas needs: canvas#c_{id}, div#cw_{id}, div#ct_{id}; renderChartLegend needs div#cl_{id} */
    wrap.innerHTML = `
      <div id="cw_${cid}" class="an-chart-canvas-wrap">
        <canvas id="c_${cid}"></canvas>
        <div id="ct_${cid}" class="analytics-tooltip hidden"></div>
      </div>
      <div id="cl_${cid}" class="analytics-legend"></div>`;
    grid.appendChild(wrap);

    const cfg = newChartCfg({ type, xCol, yCol });
    cfg.id = cid;
    requestAnimationFrame(() => {
      const pts = aggregateForChart(cfg, rows);
      const aggL = cfg.agg === 'count' ? 'COUNT(*)' : `${cfg.agg.toUpperCase()}(${cfg.yCol})`;
      drawChartOnCanvas(cid, pts, type, aggL, xCol);
      renderChartLegend(cid, pts, type);
    });
  });

  analyticsState.currentRows = prevRows;
  analyticsState.currentCols = prevCols;
}

async function deleteSavedResult(id) {
  if (!confirm('Удалить этот результат?')) return;
  try {
    await apiDelete(`/api/analytics/results/${id}`);
    loadSavedResults();
  } catch(e) { alert('Ошибка: ' + e.message); }
}


/* ═══════════════════════════════════════════════════
   API HELPERS
═══════════════════════════════════════════════════ */
async function apiFetch(path, method = 'GET', body = null) {
  const headers = {};
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, { method, headers, body });
  if (resp.status === 401) {
    // Unauthorized, redirect to login
    logout();
    return;
  }
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

async function apiGet(path) {
  const headers = {};
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, { method: 'GET', headers });
  if (resp.status === 401) { logout(); return; }
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

async function apiDelete(url) {
  const headers = {};
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const r = await fetch(API + url, { method: 'DELETE', headers });
  if (r.status === 401) { logout(); return; }
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiPost(path, body) {
  const headers = { 'Content-Type': 'application/json' };
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (resp.status === 401) { logout(); return; }
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  return resp.json();
}

async function apiPostRaw(path, body, contentType) {
  const headers = { 'Content-Type': contentType };
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, {
    method: 'POST',
    headers,
    body,
  });
  if (resp.status === 401) { logout(); return; }
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  return resp.json();
}


/* ═══════════════════════════════════════════════════
   FORMATTERS / UTILS
═══════════════════════════════════════════════════ */
function fmtNum(n)    { return Number(n).toLocaleString('ru'); }
function fmtBytes(n)  { if (n < 1024) return n+' Б'; if (n < 1048576) return (n/1024).toFixed(1)+' КБ'; return (n/1048576).toFixed(1)+' МБ'; }
function fmtUptime(s) { const h=Math.floor(s/3600),m=Math.floor((s%3600)/60); return `${h}ч ${m}м`; }
function pluralRows(n) {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return 'строка';
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'строки';
  return 'строк';
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

/* ═══════════════════════════════════════════════════
   AUTH
═══════════════════════════════════════════════════ */
function showLogin() {
  document.getElementById('login-screen').style.display = 'flex';
  document.getElementById('main-app').style.display = 'none';
}

function hideLogin() {
  document.getElementById('login-screen').style.display = 'none';
  document.getElementById('main-app').style.display = 'flex';
}

async function login(username, password) {
  try {
    const resp = await fetch(API + '/api/auth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    if (!resp.ok) throw new Error('Invalid credentials');
    const data = await resp.json();
    jwtToken = data.token;
    sessionStorage.setItem('dfo_jwt', jwtToken);
    isLoggedIn = true;
    hideLogin();
    initApp();
  } catch (e) {
    alert('Login failed: ' + e.message);
  }
}

function logout() {
  jwtToken = null;
  sessionStorage.removeItem('dfo_jwt');
  isLoggedIn = false;
  showLogin();
}

document.getElementById('login-form').addEventListener('submit', e => {
  e.preventDefault();
  const username = document.getElementById('login-username').value;
  const password = document.getElementById('login-password').value;
  login(username, password);
});

/* ═══════════════════════════════════════════════════
   INIT
═══════════════════════════════════════════════════ */
function initApp() {
  loadPrefs();
  applyPrefs();
  applyPrefsToSettingsForm();

  const hash = location.hash.replace('#', '') || 'ingest';
  switchView(hash, { pushState: false });

  connectWS();
  restartMetricsTimer(); /* clears any existing timer before starting a new one */
}

document.addEventListener('DOMContentLoaded', () => {
  if (isLoggedIn) {
    hideLogin();
    initApp();
  } else {
    showLogin();
  }
});
function escAttr(s) {
  return String(s).replace(/'/g,'&#39;').replace(/"/g,'&quot;').replace(/\n/g,'').replace(/\r/g,'');
}

function safeParse(str, def) {
  if (!str) return def;
  try { return JSON.parse(str); } catch (_) { return def; }
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

/* legacy compat */
function splitCSVLine(line) { return splitCSVLineDelim(line, ','); }


/* ═══════════════════════════════════════════════════
   MATERIALIZED VIEWS
═══════════════════════════════════════════════════ */
const MV_REFRESH = ['Вручную', 'По расписанию'];

async function loadMatviews() {
  const el = document.getElementById('matviews-list');
  try {
    const list = await apiFetch('/api/matviews');
    if (!list || !list.length) {
      el.innerHTML = '<div class="settings-loading">Нет представлений. Создайте первое.</div>';
      return;
    }
    el.innerHTML = list.map(mv => `
      <div class="settings-row">
        <div class="settings-row-label">
          <div class="settings-row-title">${escHtml(mv.name)}
            ${mv.is_stale ? '<span class="badge badge-warn" style="margin-left:.4rem;font-size:.7rem">устарело</span>' : ''}
          </div>
          <div class="settings-row-desc">
            ${MV_REFRESH[mv.refresh_mode] || 'Вручную'} ·
            ${fmtNum(mv.row_count || 0)} строк ·
            ${mv.last_refreshed_at ? 'обновлено ' + new Date(mv.last_refreshed_at * 1000).toLocaleString('ru') : 'ещё не обновлялось'}
          </div>
        </div>
        <div style="display:flex;gap:.4rem">
          <button class="btn btn-sm" onclick="queryMatview('${escAttr(mv.name)}')">Запрос</button>
          <button class="btn btn-sm" onclick="refreshMatview('${escAttr(mv.name)}')">↻ Обновить</button>
          <button class="btn btn-sm btn-danger" onclick="dropMatview('${escAttr(mv.name)}')">Удалить</button>
        </div>
      </div>`).join('');
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

function openCreateMatview() {
  document.getElementById('matview-create-form').style.display = '';
  document.getElementById('mv-name').focus();
}

function closeCreateMatview() {
  document.getElementById('matview-create-form').style.display = 'none';
  document.getElementById('mv-status').textContent = '';
}

document.addEventListener('DOMContentLoaded', () => {
  const sel = document.getElementById('mv-refresh-mode');
  if (sel) sel.addEventListener('change', () => {
    document.getElementById('mv-cron-group').style.display = sel.value === '1' ? '' : 'none';
  });
});

async function createMatview() {
  const name = document.getElementById('mv-name').value.trim();
  const sql  = document.getElementById('mv-sql').value.trim();
  const status = document.getElementById('mv-status');
  if (!name || !sql) { status.textContent = 'Укажите название и SQL'; return; }
  const sources = document.getElementById('mv-sources').value.split(',').map(s => s.trim()).filter(Boolean);
  const refresh_mode = parseInt(document.getElementById('mv-refresh-mode').value);
  const refresh_cron = document.getElementById('mv-cron').value.trim();
  try {
    await apiPost('/api/matviews', { name, definition_sql: sql, source_tables: sources, refresh_mode, refresh_cron });
    status.textContent = '';
    closeCreateMatview();
    loadMatviews();
    showToast(`Представление ${name} создано`, 'ok');
  } catch (err) {
    status.textContent = String(err);
  }
}

async function refreshMatview(name) {
  try {
    await apiPost(`/api/matviews/${encodeURIComponent(name)}/refresh`, {});
    showToast(`${name} обновлено`, 'ok');
    loadMatviews();
  } catch (err) {
    showToast(String(err), 'error');
  }
}

async function dropMatview(name) {
  if (!confirm(`Удалить представление "${name}"?`)) return;
  try {
    await apiFetch(`/api/matviews/${encodeURIComponent(name)}`, 'DELETE');
    showToast(`${name} удалено`, 'ok');
    loadMatviews();
  } catch (err) {
    showToast(String(err), 'error');
  }
}

function queryMatview(name) {
  switchView('query');
  setTimeout(() => {
    document.getElementById('sql-input').value = `SELECT * FROM ${name} LIMIT 100`;
    runQuery();
  }, 150);
}

/* ═══════════════════════════════════════════════════
   SECURITY — RBAC + AUDIT
═══════════════════════════════════════════════════ */
const RBAC_ROLES   = ['Admin', 'Analyst', 'Viewer'];
const RBAC_ACTIONS = ['', 'Чтение', 'Запись', 'Чтение + Запись'];
const AUDIT_TYPES  = ['', 'QUERY', 'INGEST', 'PIPELINE_RUN', 'AUTH_LOGIN', 'AUTH_FAIL', 'SCHEMA_CHANGE', 'POLICY_CHANGE'];

function switchSecTab(tab) {
  ['rbac','audit'].forEach(t => {
    document.getElementById(`sec-tab-${t}`).classList.toggle('active', t === tab);
    document.getElementById(`sec-pane-${t}`).style.display = t === tab ? '' : 'none';
  });
  if (tab === 'audit') loadAuditLog();
}

/* ── RBAC ── */
async function loadRbacPolicies() {
  const el = document.getElementById('rbac-policies-list');
  try {
    const list = await apiFetch('/api/rbac/policies');
    if (!list || !list.length) {
      el.innerHTML = '<div class="settings-loading">Политик нет. Добавьте первую.</div>';
      return;
    }
    el.innerHTML = `<table class="result-table" style="width:100%">
      <thead><tr><th>ID</th><th>Роль</th><th>Таблица (шаблон)</th><th>Права</th><th>Фильтр строк</th><th></th></tr></thead>
      <tbody>${list.map(p => `<tr>
        <td>${p.id}</td>
        <td>${RBAC_ROLES[p.role] ?? p.role}</td>
        <td><code>${escHtml(p.table_pattern)}</code></td>
        <td>${RBAC_ACTIONS[p.allowed_actions] ?? p.allowed_actions}</td>
        <td>${p.row_filter ? `<code>${escHtml(p.row_filter)}</code>` : '—'}</td>
        <td><button class="btn btn-sm btn-danger" onclick="deleteRbacPolicy(${p.id})">Удалить</button></td>
      </tr>`).join('')}</tbody></table>`;
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

async function addRbacPolicy() {
  const role    = parseInt(document.getElementById('rbac-new-role').value);
  const pattern = document.getElementById('rbac-new-pattern').value.trim();
  const actions = parseInt(document.getElementById('rbac-new-actions').value);
  const rf      = document.getElementById('rbac-new-rowfilter').value.trim();
  const status  = document.getElementById('rbac-status');
  if (!pattern) { status.textContent = 'Укажите шаблон таблицы'; return; }
  try {
    await apiPost('/api/rbac/policies', { role, table_pattern: pattern, allowed_actions: actions, row_filter: rf });
    document.getElementById('rbac-new-pattern').value = '';
    document.getElementById('rbac-new-rowfilter').value = '';
    status.textContent = '';
    loadRbacPolicies();
    showToast('Политика добавлена', 'ok');
  } catch (err) {
    status.textContent = String(err);
  }
}

async function deleteRbacPolicy(id) {
  if (!confirm('Удалить политику?')) return;
  try {
    await apiFetch(`/api/rbac/policies/${id}`, 'DELETE');
    loadRbacPolicies();
    showToast('Политика удалена', 'ok');
  } catch (err) {
    showToast(String(err), 'error');
  }
}

/* ── Audit ── */
async function loadAuditLog() {
  const el    = document.getElementById('audit-log-list');
  const user  = document.getElementById('audit-filter-user').value.trim();
  const limit = document.getElementById('audit-filter-limit').value || 50;
  const status = document.getElementById('audit-status');
  el.innerHTML = '<div class="settings-loading">Загрузка…</div>';
  try {
    let qs = `limit=${limit}`;
    if (user) qs += `&user_id=${encodeURIComponent(user)}`;
    const list = await apiFetch(`/api/audit?${qs}`);
    status.textContent = `${list.length} записей`;
    if (!list.length) {
      el.innerHTML = '<div class="settings-loading">Событий нет</div>';
      return;
    }
    el.innerHTML = `<table class="result-table" style="width:100%;font-size:.8rem">
      <thead><tr><th>Время</th><th>Тип</th><th>Пользователь</th><th>IP</th><th>Ресурс</th><th>Код</th><th>Детали</th></tr></thead>
      <tbody>${list.map(e => `<tr>
        <td style="white-space:nowrap">${new Date(e.ts * 1000).toLocaleString('ru')}</td>
        <td><span class="badge ${e.event_type === 5 ? 'badge-danger' : 'badge-ok'}">${AUDIT_TYPES[e.event_type] ?? e.event_type}</span></td>
        <td>${escHtml(e.user_id || '—')}</td>
        <td>${escHtml(e.client_ip || '—')}</td>
        <td>${escHtml(e.resource || '—')}</td>
        <td>${e.result_code === 0 ? '✓' : '<span style="color:var(--red)">✗ ' + e.result_code + '</span>'}</td>
        <td style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(e.action_detail||'')}">${escHtml(e.action_detail || '—')}</td>
      </tr>`).join('')}</tbody></table>`;
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
    status.textContent = '';
  }
}

/* ═══════════════════════════════════════════════════
   CLUSTER STATUS
═══════════════════════════════════════════════════ */
async function loadClusterStatus() {
  const el = document.getElementById('settings-cluster-status');
  if (!el) return;
  try {
    const s = await apiFetch('/api/cluster/status');
    if (!s.cluster_mode && !s.is_leader && !s.replica_count) {
      el.innerHTML = '<div class="settings-loading">Кластерный режим отключён</div>';
      return;
    }
    const replicaRows = (s.replicas || []).map(r =>
      `<div class="settings-row" style="padding:.3rem 0">
        <div class="settings-row-label">
          <div class="settings-row-title">${escHtml(r.host)}:${r.port}</div>
        </div>
        <span class="badge ${r.connected ? 'badge-ok' : 'badge-danger'}">${r.connected ? 'подключён' : 'недоступен'}</span>
      </div>`).join('');
    el.innerHTML = `
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Роль</div></div>
        <span class="badge ${s.is_leader ? 'badge-ok' : 'badge-warn'}">${s.is_leader ? 'Лидер' : 'Реплика'}</span>
      </div>
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Node ID</div></div>
        <code>${escHtml(s.node_id || '—')}</code>
      </div>
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Последний LSN</div></div>
        <span>${fmtNum(s.last_acked_lsn || 0)}</span>
      </div>
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Отставание (записей)</div></div>
        <span>${fmtNum(s.lag_count || 0)}</span>
      </div>
      ${replicaRows || '<div class="settings-loading">Реплик нет</div>'}`;
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

/* ═══════════════════════════════════════════════════
   BOOT
═══════════════════════════════════════════════════ */
loadPrefs();
applyPrefs();
connectWS();
restartMetricsTimer();

/* Restore view from URL hash, or default to tables */
(function bootView() {
  const hash = location.hash.replace('#', '').trim();
  const view = VALID_VIEWS.has(hash) ? hash : 'ingest';
  /* replace initial state so back-button doesn't exit the app */
  history.replaceState({ view }, '', '#' + view);
  switchView(view, { pushState: false });
})();
