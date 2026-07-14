'use strict';

/**
 * TBlaugranaTurf — serveur Railway
 * ─────────────────────────────────────────────────────────
 * - Sert le front (public/index.html)
 * - Fait proxy vers l'API PMU pour éviter le blocage CORS
 * - MOTEUR DU BOT (tourne en continu, indépendamment des
 *   visiteurs) :
 *     • charge le programme du jour, filtré aux courses
 *       FRANÇAISES uniquement (les réunions étrangères sont
 *       ignorées)
 *     • suit la prochaine course, prend un SNAPSHOT des cotes
 *       à l'heure de départ (T0), même si personne n'est
 *       connecté
 *     • détecte les chutes de cote >= 15% (cote finale entre
 *       1 et 15) et envoie une notification Telegram —
 *       UNE SEULE FOIS par cheval, peu importe le nombre de
 *       visiteurs connectés au même moment
 *     • bascule automatiquement sur la course suivante une
 *       fois les cotes figées après le départ
 * - /api/state expose l'état courant du bot (programme,
 *   course suivie, participants, snapshot, etc.) : un
 *   visiteur qui se connecte à T+23s récupère directement le
 *   snapshot pris par le serveur à T0.
 */

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const PMU_TARGETS = {
  '/api/pmu61': 'https://online.turfinfo.api.pmu.fr/rest/client/61',
  '/api/pmu7':  'https://online.turfinfo.api.pmu.fr/rest/client/7',
};

// ── Proxy générique vers l'API PMU (navigation manuelle côté front) ──
function makeProxy(prefix, target) {
  return async (req, res) => {
    const upstreamUrl = target + req.url;

    const headers = { 'Accept': 'application/json' };
    if (req.headers['if-none-match']) {
      headers['If-None-Match'] = req.headers['if-none-match'];
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);

    try {
      const upstreamRes = await fetch(upstreamUrl, {
        headers,
        signal: controller.signal,
      });
      clearTimeout(timeout);

      if (upstreamRes.status === 304) {
        res.status(304).end();
        return;
      }

      const etag = upstreamRes.headers.get('etag');
      if (etag) res.setHeader('ETag', etag);
      res.setHeader('Cache-Control', 'no-store');

      if (!upstreamRes.ok) {
        res.status(upstreamRes.status).json({
          error: `Upstream PMU HTTP ${upstreamRes.status}`,
        });
        return;
      }

      const data = await upstreamRes.json();
      res.status(200).json(data);
    } catch (e) {
      clearTimeout(timeout);
      const isAbort = e.name === 'AbortError';
      res.status(isAbort ? 504 : 502).json({
        error: isAbort ? 'Timeout upstream PMU' : `Erreur proxy: ${e.message}`,
      });
    }
  };
}

for (const [prefix, target] of Object.entries(PMU_TARGETS)) {
  app.use(prefix, makeProxy(prefix, target));
}

// ═══════════════════════════════════════════════════════════
//  MOTEUR DU BOT — tourne en continu côté serveur
// ═══════════════════════════════════════════════════════════

const PMU_PROG_BASE  = 'https://online.turfinfo.api.pmu.fr/rest/client/61';
const PMU_PARTS_BASE = 'https://online.turfinfo.api.pmu.fr/rest/client/7';

const BOT_CFG = {
  pollMs:               1000,    // rythme de surveillance des cotes
  progRefreshMs:        120000,  // rafraîchissement du programme complet
  raceRefreshMs:        15000,   // rafraîchissement ciblé de la course suivie (détecte un retard de départ)
  raceRefreshWindowMs:  600000,  // ne rafraîchit la course suivie que dans les 10 min autour du départ prévu
  oddsStableMs:         20000,   // durée sans changement pour confirmer le gel (après le VRAI départ)
  switchAfterFreezeMs:  240000,  // bascule N ms après le gel confirmé (4 min)
  maxWaitAfterDepartMs: 1200000, // sécurité anti-blocage (20 min)
  dropPctMin:           15,      // seuil de chute pour notifier (%)
  coteMin:              1,       // cote finale minimum pour notifier
  coteMax:              15,      // cote finale maximum pour notifier
  tgToken:   '8961502220:AAGlpLomYVMXRQgrJsPp5M4m-omFPJPBKoU',
  tgChatIds: ['625118343', '8288460384'],
};

// ── ÉTAT DU BOT (en mémoire, partagé par tous les visiteurs) ──
const bot = {
  today:            null,   // 'YYYYMMDD'
  programme:        [],     // courses françaises du jour, triées par départ
  curRaceIdx:       -1,
  participants:     [],
  snapCotes:        {},     // numPmu -> cote au moment du snapshot
  snapDone:         false,
  alertedDrop:      new Set(),
  lastOddsChangeAt: null,
  oddsFrozenAt:     null,
  lastPartsHash:    '',
  lastPoll:         null,
  lastRaceRefresh:  0,      // dernier rafraîchissement ciblé de l'heure de départ réelle
  raceStatut:       null,   // statut brut renvoyé par l'API pour la course suivie (diagnostic)
  log:              [],     // derniers événements { t, icon, msg }
};

function botLog(icon, msg) {
  bot.log.push({ t: Date.now(), icon, msg });
  if (bot.log.length > 30) bot.log.shift();
  console.log(`[BOT] ${icon} ${msg}`);
}

const pad = n => String(n).padStart(2, '0');
function dateStr(d) { return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`; }
function datePmu(yyyymmdd) { return yyyymmdd.slice(6, 8) + yyyymmdd.slice(4, 6) + yyyymmdd.slice(0, 4); }

async function fetchJson(url, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: ctrl.signal, headers: { Accept: 'application/json' } });
    clearTimeout(id);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    clearTimeout(id);
    throw e;
  }
}

function isAnnulee(obj) {
  const s = obj?.statut || obj?.statutCourse || obj?.statutReunion || '';
  return typeof s === 'string' && s.toUpperCase().includes('ANNULE');
}

// ⚠️ Filtre "courses françaises uniquement" — non vérifié en conditions
// réelles (pas d'accès direct à l'API PMU depuis l'environnement où ce
// code a été écrit). L'API expose normalement un champ `pays` sur chaque
// réunion (ex: { code: 'FRA', libelle: 'FRANCE' }). On teste plusieurs
// variantes possibles du champ pour rester robuste, et si le champ est
// absent on GARDE la réunion par défaut (mieux vaut une course étrangère
// affichée par erreur qu'une course française masquée par erreur).
// Si ça ne filtre pas correctement, ouvre /api/pmu61/programme/JJMMAAAA
// dans le navigateur et regarde le nom exact du champ pays sur une
// réunion étrangère connue, puis dis-le-moi pour ajuster.
function isFrench(reunion) {
  const p = reunion?.pays;
  if (!p) return true; // pas d'info → on ne masque pas
  const code = (p.code || '').toUpperCase();
  const lib  = (p.libelle || '').toUpperCase();
  if (['FRA', 'FR', 'FRANCE'].includes(code)) return true;
  if (lib === 'FRANCE') return true;
  return !code && !lib; // champ vide → on ne masque pas
}

async function loadProgrammeForDate(ds) {
  const url = `${PMU_PROG_BASE}/programme/${datePmu(ds)}?specialisation=OFFLINE`;
  const data = await fetchJson(url);
  const races = [];
  let nbEtrangeres = 0;
  for (const ru of (data?.programme?.reunions || [])) {
    if (isAnnulee(ru)) continue;
    if (!isFrench(ru)) { nbEtrangeres += (ru.courses || []).length; continue; }
    const hip = ru.hippodrome?.libelleCourt || ru.hippodrome?.libelleLong || `R${ru.numOfficiel}`;
    for (const co of (ru.courses || [])) {
      if (isAnnulee(co)) continue;
      races.push({
        reunion: ru.numOfficiel,
        course:  co.numOrdre,
        depart:  co.heureDepart,
        libelle: co.libelle || co.libelleCourt || `Course ${co.numOrdre}`,
        hip,
        disc: co.discipline || co.specialite || '',
        partants: co.nombreDeclaresPartants ?? null,
      });
    }
  }
  races.sort((a, b) => a.depart - b.depart);
  return { races, nbEtrangeres };
}

async function loadProgramme() {
  const today = dateStr(new Date());
  try {
    const { races, nbEtrangeres } = await loadProgrammeForDate(today);
    if (races.length > 0) {
      bot.today = today;
      bot.programme = races;
      botLog('📋', `Programme : ${races.length} courses FR (${nbEtrangeres} étrangère(s) masquée(s))`);
      return;
    }
  } catch (e) {
    botLog('⚠️', `Programme du jour : ${e.message}`);
  }

  // Pas de courses FR aujourd'hui → on cherche les 7 prochains jours
  for (let offset = 1; offset <= 7; offset++) {
    const d = new Date();
    d.setDate(d.getDate() + offset);
    const ds = dateStr(d);
    try {
      const { races } = await loadProgrammeForDate(ds);
      if (races.length > 0) {
        bot.today = ds;
        bot.programme = races;
        botLog('📅', `Pas de courses FR aujourd'hui — bascule sur ${ds} (${races.length} courses)`);
        return;
      }
    } catch (_) {}
  }
  botLog('⚠️', 'Aucune course française trouvée dans les 7 prochains jours');
}

function findNextRaceIdx() {
  const now = Date.now();
  for (let i = 0; i < bot.programme.length; i++) {
    if (bot.programme[i].depart >= now - 300_000) return i;
  }
  return Math.max(0, bot.programme.length - 1);
}

// ── Rafraîchissement ciblé de la course suivie ──────────────
// Le programme complet du jour n'est rechargé que toutes les 2 min
// (BOT_CFG.progRefreshMs), ce qui est trop rare pour détecter un retard
// de dernière minute annoncé par le PMU (heureDepart mise à jour côté
// API alors que le départ prévu initial est déjà dépassé). Sans ça, le
// bot peut croire la course "démarrée" (now >= depart) et déclarer un
// gel après une simple accalmie de mises, alors que le vrai départ n'a
// pas encore eu lieu.
// On interroge donc, uniquement autour du départ prévu (± raceRefreshWindowMs)
// et à un rythme plus soutenu (raceRefreshMs), le détail de CETTE course
// pour récupérer sa version la plus à jour de heureDepart (et son statut,
// à titre de diagnostic — le nom exact du champ n'ayant pas pu être vérifié
// en conditions réelles faute d'accès direct à l'API PMU depuis cet
// environnement de dev).
async function refreshCurrentRaceDepart() {
  if (bot.curRaceIdx < 0 || bot.curRaceIdx >= bot.programme.length) return;
  const race = bot.programme[bot.curRaceIdx];
  const now = Date.now();

  if (Math.abs(now - race.depart) > BOT_CFG.raceRefreshWindowMs) return;
  if (now - bot.lastRaceRefresh < BOT_CFG.raceRefreshMs) return;
  bot.lastRaceRefresh = now;

  const url = `${PMU_PROG_BASE}/programme/${datePmu(bot.today)}/R${race.reunion}/C${race.course}?specialisation=OFFLINE`;
  try {
    const data = await fetchJson(url, 5000);
    const co = data?.course || data;
    const newDepart = co?.heureDepart;
    const statut = co?.statut || co?.statutCourse || null;

    if (statut && statut !== bot.raceStatut) {
      bot.raceStatut = statut;
      botLog('ℹ️', `Statut course R${race.reunion}C${race.course} : ${statut}`);
    }

    if (typeof newDepart === 'number' && newDepart !== race.depart) {
      const deltaSec = Math.round((newDepart - race.depart) / 1000);
      botLog('⏰', `Départ R${race.reunion}C${race.course} mis à jour (${deltaSec >= 0 ? '+' : ''}${deltaSec}s) — retard pris en compte`);
      race.depart = newDepart;
      // Le départ a bougé : on redonne sa chance à la fenêtre de stabilité
      // (sinon un vieux lastOddsChangeAt antérieur au nouveau départ
      // pourrait immédiatement satisfaire le seuil de gel).
      if (bot.lastOddsChangeAt < newDepart) bot.lastOddsChangeAt = now;
    }
  } catch (_) {
    // échec silencieux, on retente au prochain tick
  }
}

function resetForNewRace() {
  bot.snapDone = false;
  bot.snapCotes = {};
  bot.alertedDrop.clear();
  bot.participants = [];
  bot.lastOddsChangeAt = Date.now();
  bot.oddsFrozenAt = null;
  bot.lastPartsHash = '';
  bot.lastRaceRefresh = 0;
  bot.raceStatut = null;
}

function autoSwitch() {
  if (bot.programme.length === 0) return;
  if (bot.curRaceIdx < 0 || bot.curRaceIdx >= bot.programme.length) {
    bot.curRaceIdx = findNextRaceIdx();
    resetForNewRace();
    return;
  }
  if (bot.curRaceIdx >= bot.programme.length - 1) return;

  const race = bot.programme[bot.curRaceIdx];
  const now = Date.now();
  let shouldSwitch = false;
  if (bot.oddsFrozenAt !== null) {
    shouldSwitch = (now - bot.oddsFrozenAt) >= BOT_CFG.switchAfterFreezeMs;
  } else if ((now - race.depart) >= BOT_CFG.maxWaitAfterDepartMs) {
    shouldSwitch = true;
    botLog('⚠️', 'Gel des cotes non détecté — bascule forcée (sécurité)');
  }
  if (shouldSwitch) {
    bot.curRaceIdx++;
    resetForNewRace();
    botLog('➡️', `Course suivante : R${bot.programme[bot.curRaceIdx].reunion}C${bot.programme[bot.curRaceIdx].course}`);
  }
}

function hashParts(parts) {
  return parts
    .filter(p => p.statut === 'PARTANT' && p.dernierRapportDirect)
    .map(p => `${p.numPmu}:${p.dernierRapportDirect.rapport}`)
    .join(',');
}

function takeSnapshot() {
  bot.snapDone = true;
  bot.snapCotes = {};
  for (const p of bot.participants) {
    if (p.statut === 'PARTANT' && p.dernierRapportDirect) {
      bot.snapCotes[p.numPmu] = p.dernierRapportDirect.rapport;
    }
  }
  bot.alertedDrop.clear();
  botLog('📸', `SNAPSHOT — ${Object.keys(bot.snapCotes).length} chevaux`);
}

async function sendTelegram(text) {
  const apiBase = `https://api.telegram.org/bot${BOT_CFG.tgToken}/sendMessage`;
  for (const chatId of BOT_CFG.tgChatIds) {
    try {
      const r = await fetch(apiBase, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'Markdown' }),
      });
      const json = await r.json();
      if (!r.ok || !json.ok) {
        botLog('❌', `TG erreur chat ${chatId} : ${json.description || r.status}`);
      }
    } catch (e) {
      botLog('❌', `TG exception chat ${chatId} : ${e.message}`);
    }
  }
}

function fmtElapsed(secs) {
  const s = Math.max(0, secs);
  if (s < 60) return `${s}s après le départ`;
  const min = Math.floor(s / 60);
  const rem = s % 60;
  return rem > 0 ? `${min}min${String(rem).padStart(2, '0')}s après le départ` : `${min}min après le départ`;
}

function checkDropsAndNotify(race) {
  if (!bot.snapDone) return;
  const secsLeft = Math.round((race.depart - Date.now()) / 1000);
  if (secsLeft > 0) return;      // snapshot pas encore pris (avant T0)
  // Pas de plafond fixe ici : la fenêtre d'alerte reste ouverte tant que
  // le bot suit cette course. Elle se ferme naturellement quand autoSwitch()
  // bascule sur la course suivante (gel confirmé + 4 min, ou sécurité
  // 20 min après l'heure prévue — voir autoSwitch()), ce qui réinitialise
  // snapDone/snapCotes/alertedDrop via resetForNewRace().

  for (const p of bot.participants) {
    if (p.statut !== 'PARTANT' || !p.dernierRapportDirect) continue;
    const snap = bot.snapCotes[p.numPmu];
    const cur = p.dernierRapportDirect.rapport;
    if (!snap || bot.alertedDrop.has(p.numPmu)) continue;

    const drop = (snap - cur) / snap * 100;
    if (drop < BOT_CFG.dropPctMin) continue;
    if (cur < BOT_CFG.coteMin || cur > BOT_CFG.coteMax) continue;

    bot.alertedDrop.add(p.numPmu);
    botLog('🔥', `${p.nom} — ${snap} → ${cur} (−${drop.toFixed(0)}%)`);

    const raceLabel = `R${race.reunion}C${race.course}`;
    const secsStr = `⏱ ${fmtElapsed(-secsLeft)}`;
    const text =
      `🚨 *ALERTE ${raceLabel}* 🚨\n` +
      `🐎 ${p.numPmu} — *${p.nom}*\n` +
      `${snap} ➡️ ${cur} (−${drop.toFixed(0)}%)\n` +
      `${secsStr}`;
    sendTelegram(text);
  }
}

async function pollCurrentRace() {
  if (bot.curRaceIdx < 0 || bot.curRaceIdx >= bot.programme.length) return;
  await refreshCurrentRaceDepart();
  const race = bot.programme[bot.curRaceIdx];
  const url = `${PMU_PARTS_BASE}/programme/${datePmu(bot.today)}/R${race.reunion}/C${race.course}/participants?specialisation=OFFLINE`;

  let parts;
  try {
    const data = await fetchJson(url, 5000);
    parts = data.participants || [];
  } catch (_) {
    return; // échec silencieux, on retente au prochain tick
  }

  const newHash = hashParts(parts);
  const unchanged = newHash === bot.lastPartsHash;
  bot.lastPartsHash = newHash;
  bot.participants = parts;
  bot.lastPoll = Date.now();

  const now = Date.now();
  if (!unchanged) {
    bot.lastOddsChangeAt = now;
  } else if (bot.oddsFrozenAt === null && now >= race.depart) {
    // Le compteur de stabilité ne peut jamais démarrer avant le départ :
    // si les cotes n'avaient pas bougé depuis un moment AVANT T0 (simple
    // creux entre deux mises, course pas encore partie), on ne veut pas
    // déclarer le gel dès l'instant du départ. On exige oddsStableMs de
    // stabilité mesurée à partir de race.depart au plus tôt.
    const stableSince = Math.max(bot.lastOddsChangeAt, race.depart);
    if ((now - stableSince) >= BOT_CFG.oddsStableMs) {
      bot.oddsFrozenAt = now;
      botLog('🧊', `Cotes figées R${race.reunion}C${race.course} — bascule dans ${Math.round(BOT_CFG.switchAfterFreezeMs / 1000)}s`);
    }
  }

  // Snapshot pris exactement à/après l'heure de départ (T0)
  const secsLeft = Math.round((race.depart - now) / 1000);
  if (!bot.snapDone && secsLeft <= 0) {
    takeSnapshot();
  }

  checkDropsAndNotify(race);
}

async function botTick() {
  try {
    if (bot.programme.length === 0 || !bot._lastProgLoad || (Date.now() - bot._lastProgLoad) >= BOT_CFG.progRefreshMs) {
      await loadProgramme();
      bot._lastProgLoad = Date.now();
    }
    autoSwitch();
    await pollCurrentRace();
  } catch (e) {
    botLog('⚠️', `Erreur boucle bot : ${e.message}`);
  }
}

function startBot() {
  botLog('🚀', 'Bot démarré — surveillance en continu');
  botTick();
  setInterval(botTick, BOT_CFG.pollMs);
}

// ── API : état courant du bot (consulté par le front) ──────
app.get('/api/state', (req, res) => {
  const race = (bot.curRaceIdx >= 0 && bot.curRaceIdx < bot.programme.length)
    ? bot.programme[bot.curRaceIdx]
    : null;
  const secsLeft = race ? Math.round((race.depart - Date.now()) / 1000) : null;

  const participants = bot.participants.map(p => {
    const snap = bot.snapCotes[p.numPmu] ?? null;
    const cur = p.dernierRapportDirect?.rapport ?? null;
    const dropPct = (snap != null && cur != null) ? (snap - cur) / snap * 100 : null;
    return {
      numPmu: p.numPmu,
      nom: p.nom,
      statut: p.statut,
      cote: cur,
      snap,
      dropPct,
      alerted: bot.alertedDrop.has(p.numPmu),
    };
  });

  res.json({
    ok: true,
    today: bot.today,
    programme: bot.programme,
    curRaceIdx: bot.curRaceIdx,
    race,
    secsLeft,
    participants,
    snapDone: bot.snapDone,
    oddsFrozenAt: bot.oddsFrozenAt,
    raceStatut: bot.raceStatut,
    lastPoll: bot.lastPoll,
    log: bot.log.slice(-15),
  });
});

// ── Fichiers statiques (le front) ───────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ── Fallback santé / racine ──────────────────────────────
app.get('/health', (req, res) => res.json({ ok: true, botToday: bot.today, curRaceIdx: bot.curRaceIdx }));

app.listen(PORT, () => {
  console.log(`TBlaugranaTurf en écoute sur le port ${PORT}`);
  startBot();
});
