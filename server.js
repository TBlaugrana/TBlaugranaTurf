'use strict';

// ═══════════════════════════════════════════════════════
//  TBlaugranaTurf — Serveur
//
//  Sert l'interface (public/index.html) et fait office de
//  proxy vers l'API PMU. Le CORS est une protection imposée
//  par le NAVIGATEUR : une requête serveur → serveur n'y est
//  pas soumise. C'est pourquoi --disable-web-security (utilisé
//  en local via le .bat) n'est plus nécessaire ici : le
//  navigateur ne parle qu'à CE serveur, qui lui-même relaie
//  la requête vers online.turfinfo.api.pmu.fr.
// ═══════════════════════════════════════════════════════

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const PMU_UPSTREAM = 'https://online.turfinfo.api.pmu.fr/rest';

// ── Proxy générique /pmu-api/* → online.turfinfo.api.pmu.fr/rest/* ──
// Transmet le If-None-Match du client et renvoie l'ETag / 304 du serveur
// PMU, pour conserver le mécanisme de cache utilisé par cotesLoop().
app.get('/pmu-api/*', async (req, res) => {
  const upstreamPath = req.path.replace(/^\/pmu-api/, '');
  const qs = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
  const upstreamUrl = `${PMU_UPSTREAM}${upstreamPath}${qs}`;

  try {
    const headers = {};
    if (req.headers['if-none-match']) {
      headers['If-None-Match'] = req.headers['if-none-match'];
    }

    const upstreamRes = await fetch(upstreamUrl, { headers });

    const etag = upstreamRes.headers.get('etag');
    if (etag) res.set('ETag', etag);
    res.set('Cache-Control', 'no-store');

    if (upstreamRes.status === 304) {
      res.status(304).end();
      return;
    }
    if (!upstreamRes.ok) {
      res.status(upstreamRes.status).end();
      return;
    }

    const data = await upstreamRes.json();
    res.json(data);
  } catch (err) {
    console.error('[proxy PMU] erreur :', err.message);
    res.status(502).json({ error: 'Erreur proxy PMU', detail: err.message });
  }
});

// ── Fichiers statiques (index.html, etc.) ──
app.use(express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`TBlaugranaTurf en écoute sur le port ${PORT}`);
});

// ═══════════════════════════════════════════════════════
//  ALERTES TELEGRAM AUTOMATIQUES (côté serveur)
//
//  Tourne en continu sur Railway, indépendamment du fait que
//  le navigateur soit ouvert ou non. Reprend la même logique
//  que le client (snapshot N s avant départ, puis détection de
//  chute de cote) mais en tâche de fond côté serveur.
//
//  Variables d'environnement (à définir sur Railway) :
//    TELEGRAM_BOT_TOKEN     token du bot (obligatoire)
//    TELEGRAM_CHAT_IDS      chat id(s), séparés par des virgules (obligatoire)
//    DROP_THRESHOLD         seuil de chute en %  (défaut 30)
//    SNAP_SECS              snapshot N s avant départ (défaut 180)
//    POST_DEPART_WINDOW     fenêtre d'alerte après départ, en s (défaut 120)
//    TG_MAX_COTE            cote max (snap ET finale) pour déclencher l'alerte (défaut : aucun filtre)
//    AUTO_ALERT_INTERVAL_MS intervalle de polling en ms (défaut 5000)
// ═══════════════════════════════════════════════════════

const TG_TOKEN           = process.env.TELEGRAM_BOT_TOKEN || '';
const TG_CHAT_IDS        = (process.env.TELEGRAM_CHAT_IDS || '').split(',').map(s => s.trim()).filter(Boolean);
const DROP_THRESHOLD     = parseFloat(process.env.DROP_THRESHOLD) || 30;
const SNAP_SECS          = parseInt(process.env.SNAP_SECS, 10) || 180;
const POST_DEPART_WINDOW = parseInt(process.env.POST_DEPART_WINDOW, 10) || 120;
const TG_MAX_COTE        = process.env.TG_MAX_COTE ? parseFloat(process.env.TG_MAX_COTE) : Infinity;
const AUTO_ALERT_INTERVAL_MS = parseInt(process.env.AUTO_ALERT_INTERVAL_MS, 10) || 5000;

function datePmuFmt(yyyymmdd) {
  // YYYYMMDD → DDMMYYYY
  return yyyymmdd.slice(6, 8) + yyyymmdd.slice(4, 6) + yyyymmdd.slice(0, 4);
}

function todayStr() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`;
}

// État interne du moteur d'alertes
const AE = {
  today: null,
  programme: [],
  lastProgFetch: 0,
  curRaceKey: null,
  snapDone: false,
  snapCotes: {},
  alerted: new Set(),
};

async function fetchProgramme() {
  AE.today = todayStr();
  const url = `${PMU_UPSTREAM}/client/61/programme/${datePmuFmt(AE.today)}?specialisation=OFFLINE`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`programme HTTP ${r.status}`);
  const data = await r.json();

  const races = [];
  for (const ru of (data?.programme?.reunions || [])) {
    const hip = ru.hippodrome?.libelleCourt || ru.hippodrome?.libelleLong || `R${ru.numOfficiel}`;
    for (const co of (ru.courses || [])) {
      races.push({
        reunion: ru.numOfficiel,
        course:  co.numOrdre,
        depart:  co.heureDepart,
        libelle: co.libelle || co.libelleCourt || `Course ${co.numOrdre}`,
        hip,
      });
    }
  }
  AE.programme = races.sort((a, b) => a.depart - b.depart);
  AE.lastProgFetch = Date.now();
}

function pickRace() {
  const now = Date.now();
  for (const race of AE.programme) {
    if (race.depart >= now - POST_DEPART_WINDOW * 1000) return race;
  }
  return AE.programme.length ? AE.programme[AE.programme.length - 1] : null;
}

async function fetchParticipants(race) {
  const url = `${PMU_UPSTREAM}/client/7/programme/${datePmuFmt(AE.today)}/R${race.reunion}/C${race.course}/participants?specialisation=OFFLINE`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`participants HTTP ${r.status}`);
  const data = await r.json();
  return data?.participants || [];
}

function formatDelay(secsLeft) {
  if (secsLeft > 0) return `⏱ ${secsLeft}s avant départ`;
  const elapsed = -secsLeft;
  const m = Math.floor(elapsed / 60);
  const s = elapsed % 60;
  return m > 0 ? `🏁 ${m}min ${s}s après départ` : `🏁 ${s}s après départ`;
}

async function sendTelegramAlert(p, snap, cur, drop, race, secsLeft) {
  if (!TG_TOKEN || !TG_CHAT_IDS.length) return;
  const raceLabel = `R${race.reunion}C${race.course}`;
  const text =
    `🆘 *ALERTE ${raceLabel}* 🆘\n` +
    `🐎 ${p.numPmu} — *${p.nom}*\n` +
    `${snap} ➡️ ${cur} (−${drop.toFixed(1)}%)\n` +
    `${formatDelay(secsLeft)}`;

  for (const chatId of TG_CHAT_IDS) {
    try {
      const r = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ chat_id: chatId, text, parse_mode: 'Markdown' }),
      });
      const json = await r.json();
      if (!r.ok || !json.ok) {
        console.error('[AutoAlert][TG] erreur', chatId, json.description || r.status);
      }
    } catch (e) {
      console.error('[AutoAlert][TG] exception', chatId, e.message);
    }
  }
}

async function autoAlertTick() {
  try {
    if (!AE.lastProgFetch || Date.now() - AE.lastProgFetch > 120_000 || AE.today !== todayStr()) {
      await fetchProgramme();
    }

    const race = pickRace();
    if (!race) return;

    const raceKey = `${race.reunion}-${race.course}`;
    if (raceKey !== AE.curRaceKey) {
      AE.curRaceKey  = raceKey;
      AE.snapDone    = false;
      AE.snapCotes   = {};
      AE.alerted.clear();
    }

    const now = Date.now();
    const secsLeft = Math.round((race.depart - now) / 1000);

    if (secsLeft > SNAP_SECS) return;            // trop tôt, pas encore dans la fenêtre
    if (secsLeft < -POST_DEPART_WINDOW) return;   // course trop ancienne, on arrête

    const participants = await fetchParticipants(race);

    // Snapshot : on mémorise les cotes au moment où l'on entre dans la fenêtre
    if (!AE.snapDone) {
      for (const p of participants) {
        if (p.statut === 'PARTANT' && p.dernierRapportDirect) {
          AE.snapCotes[p.numPmu] = p.dernierRapportDirect.rapport;
        }
      }
      AE.snapDone = true;
      console.log(`[AutoAlert] Snapshot R${race.reunion}C${race.course} — ${Object.keys(AE.snapCotes).length} chevaux`);
      return;
    }

    // Détection des chutes >= DROP_THRESHOLD %
    for (const p of participants) {
      if (p.statut !== 'PARTANT' || !p.dernierRapportDirect) continue;
      const snap = AE.snapCotes[p.numPmu];
      const cur  = p.dernierRapportDirect.rapport;
      if (!snap) continue;

      const drop = (snap - cur) / snap * 100;
      if (drop < DROP_THRESHOLD || AE.alerted.has(p.numPmu)) continue;

      // ── Filtre cote max : on ignore si la cote snapshot OU la cote finale dépasse TG_MAX_COTE ──
      if (snap > TG_MAX_COTE || cur > TG_MAX_COTE) continue;

      AE.alerted.add(p.numPmu);
      console.log(`[AutoAlert] 🔥 ${p.nom} ${snap} → ${cur} (-${drop.toFixed(1)}%)`);
      await sendTelegramAlert(p, snap, cur, drop, race, secsLeft);
    }
  } catch (e) {
    console.error('[AutoAlert] erreur tick :', e.message);
  }
}

if (TG_TOKEN && TG_CHAT_IDS.length) {
  setInterval(autoAlertTick, AUTO_ALERT_INTERVAL_MS);
  console.log(`[AutoAlert] activé — seuil ${DROP_THRESHOLD}% — intervalle ${AUTO_ALERT_INTERVAL_MS}ms`);
} else {
  console.log('[AutoAlert] désactivé — définir TELEGRAM_BOT_TOKEN et TELEGRAM_CHAT_IDS sur Railway pour l\'activer');
}
