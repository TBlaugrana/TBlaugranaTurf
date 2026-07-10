'use strict';

/**
 * TBlaugranaTurf — serveur Railway
 * ─────────────────────────────────────────────────────────
 * - Sert le front (public/index.html)
 * - Fait proxy vers l'API PMU pour éviter le blocage CORS
 *   (remplace le --disable-web-security du .bat local)
 * - Transmet ETag / If-None-Match pour garder le comportement
 *   304 "Not Modified" utilisé par le front pour détecter les
 *   cotes inchangées.
 */

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const PMU_TARGETS = {
  '/api/pmu61': 'https://online.turfinfo.api.pmu.fr/rest/client/61',
  '/api/pmu7':  'https://online.turfinfo.api.pmu.fr/rest/client/7',
};

// ── Proxy générique vers l'API PMU ──────────────────────
function makeProxy(prefix, target) {
  return async (req, res) => {
    const upstreamUrl = target + req.url; // req.url = ce qui suit le prefix (montage express)

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

      // 304 : on relaie tel quel, sans corps
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

// ── Fichiers statiques (le front) ───────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ── Fallback santé / racine ──────────────────────────────
app.get('/health', (req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`TBlaugranaTurf en écoute sur le port ${PORT}`);
});
