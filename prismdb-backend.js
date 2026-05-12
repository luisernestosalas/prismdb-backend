// ═══════════════════════════════════════════════════════════
//  PrismDB — Backend Node.js
//  Integraciones: Firecrawl · Jelou · Anthropic · MercadoPago
// ═══════════════════════════════════════════════════════════

import express from "express";
import cors from "cors";
import helmet from "helmet";
import dotenv from "dotenv";

dotenv.config();

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(helmet());
app.use(cors({ origin: process.env.FRONTEND_URL || "*" }));
app.use(express.json({ limit: "2mb" }));

// ── Health ───────────────────────────────────────────────
app.get("/health", (_req, res) => res.json({ ok: true, app: "prismdb", ts: Date.now() }));

// ════════════════════════════════════════════════════════════
//  1. PROSPECCIÓN — Buscar leads con Firecrawl + scoring Claude
// ════════════════════════════════════════════════════════════
app.post("/leads/search", async (req, res, next) => {
  try {
    const { query, sector = "", cargo = "", ciudad = "", scoreMin = 70, limit = 10 } = req.body;
    const searchQuery = `${cargo} ${sector} ${ciudad} ${query} WhatsApp contacto`.trim();

    const fcRes = await fetch("https://api.firecrawl.dev/v1/search", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}` },
      body: JSON.stringify({ query: searchQuery, limit, lang: "es", country: "co", scrapeOptions: { formats: ["markdown"] } }),
    });

    const fcData = await fcRes.json();
    const results = fcData.data || [];

    const scoredLeads = await Promise.all(
      results.map(async (item) => {
        const prompt = `Analiza este perfil y devuelve SOLO JSON:
{"nombre":"...","cargo":"...","empresa":"...","telefono":"...o null","email":"...o null","ciudad":"...","score":85,"razon":"por qué es buen prospecto en 10 palabras"}
Perfil: ${item.markdown?.slice(0, 800) || item.description || item.title}
Criterios: cargo=${cargo}, sector=${sector}, ciudad=${ciudad}`;
        try {
          const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
            method: "POST",
            headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
            body: JSON.stringify({ model: "claude-haiku-4-5-20251001", max_tokens: 300, messages: [{ role: "user", content: prompt }] }),
          });
          const aiData = await aiRes.json();
          const lead = JSON.parse(aiData.content?.[0]?.text?.replace(/```json|```/g, "").trim() || "{}");
          return { ...lead, url: item.url, source: "firecrawl" };
        } catch { return { nombre: item.title, url: item.url, score: 50, source: "firecrawl" }; }
      })
    );

    res.json({ leads: scoredLeads.filter(l => (l.score || 0) >= scoreMin), total: scoredLeads.length, query: searchQuery });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  2. MENSAJERÍA — WhatsApp via Jelou
// ════════════════════════════════════════════════════════════
async function getJelouToken() {
  const res = await fetch("https://api.jelou.ai/v1/auth/token", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client_id: process.env.JELOU_CLIENT_ID, client_secret: process.env.JELOU_CLIENT_SECRET, grant_type: "client_credentials" }),
  });
  return (await res.json()).access_token;
}

app.post("/messages/send", async (req, res, next) => {
  try {
    const { phone, message, leadName = "" } = req.body;
    if (!phone || !message) return res.status(400).json({ error: "phone y message requeridos" });
    const token = await getJelouToken();
    const data = await (await fetch(`https://api.jelou.ai/v1/bots/${process.env.JELOU_BOT_ID}/users/${phone}/messages`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
      body: JSON.stringify({ type: "TEXT", text: message }),
    })).json();
    res.json({ ok: true, messageId: data.id, lead: leadName });
  } catch (err) { next(err); }
});

app.post("/messages/bulk", async (req, res, next) => {
  try {
    const { leads, template } = req.body;
    if (!leads?.length) return res.status(400).json({ error: "leads requerido" });
    const token = await getJelouToken();
    const results = [];
    for (const lead of leads) {
      const msg = template.replace(/\{\{nombre\}\}/g, lead.name || lead.nombre || "").replace(/\{\{empresa\}\}/g, lead.empresa || "").replace(/\{\{ciudad\}\}/g, lead.ciudad || "");
      try {
        const d = await (await fetch(`https://api.jelou.ai/v1/bots/${process.env.JELOU_BOT_ID}/users/${lead.phone}/messages`, {
          method: "POST", headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
          body: JSON.stringify({ type: "TEXT", text: msg }),
        })).json();
        results.push({ phone: lead.phone, ok: true, id: d.id });
      } catch (e) { results.push({ phone: lead.phone, ok: false, error: e.message }); }
      await new Promise(r => setTimeout(r, 500));
    }
    res.json({ sent: results.filter(r => r.ok).length, failed: results.filter(r => !r.ok).length, results });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  3. IA — Mensajes y scoring con Claude
// ════════════════════════════════════════════════════════════
app.post("/ai/message", async (req, res, next) => {
  try {
    const { lead, prompt, businessContext = "" } = req.body;
    const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514", max_tokens: 400,
        system: `Eres experto en ventas B2B para LATAM. Genera mensajes WhatsApp personalizados, directos. Máximo 160 caracteres. Contexto: ${businessContext}`,
        messages: [{ role: "user", content: prompt || `Genera mensaje para: ${lead.nombre}, ${lead.cargo} en ${lead.empresa} (${lead.ciudad}).` }],
      }),
    });
    const data = await aiRes.json();
    res.json({ message: data.content?.[0]?.text || "", lead });
  } catch (err) { next(err); }
});

app.post("/ai/score", async (req, res, next) => {
  try {
    const { profile, criteria } = req.body;
    const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001", max_tokens: 300,
        messages: [{ role: "user", content: `Score 0-100 como prospecto. SOLO JSON: {"score":85,"nivel":"Alto","razones":[],"recomendacion":"..."}
Perfil: ${JSON.stringify(profile)} Criterios: ${JSON.stringify(criteria)}` }],
      }),
    });
    const data = await aiRes.json();
    res.json(JSON.parse(data.content?.[0]?.text?.replace(/```json|```/g, "").trim() || "{}"));
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  4. CRM Pipeline
// ════════════════════════════════════════════════════════════
const pipeline = { contacto: [], calificado: [], negociacion: [], cerrado: [] };

app.get("/crm/pipeline", (_req, res) => res.json(pipeline));
app.post("/crm/lead", (req, res) => {
  const lead = { ...req.body, id: Date.now().toString(), created_at: new Date().toISOString() };
  (pipeline[lead.stage || "contacto"] ||= []).push(lead);
  res.status(201).json(lead);
});
app.patch("/crm/lead/:id/move", (req, res) => {
  const { id } = req.params; const { to } = req.body;
  for (const stage of Object.keys(pipeline)) {
    const idx = pipeline[stage].findIndex(l => l.id === id);
    if (idx !== -1) {
      const [lead] = pipeline[stage].splice(idx, 1);
      (pipeline[to] ||= []).push({ ...lead, stage: to });
      return res.json({ ok: true, lead: { ...lead, stage: to } });
    }
  }
  res.status(404).json({ error: "Lead no encontrado" });
});

// ════════════════════════════════════════════════════════════
//  5. WEBHOOK Jelou
// ════════════════════════════════════════════════════════════
app.post("/webhook/jelou", (req, res) => {
  console.log(`[JELOU WEBHOOK] ${req.body.event}:`, JSON.stringify(req.body.data).slice(0, 200));
  res.sendStatus(200);
});

// ════════════════════════════════════════════════════════════
//  6. TALENT SCANNER
// ════════════════════════════════════════════════════════════
app.post("/talent/search", async (req, res, next) => {
  try {
    const { cargo, experiencia = "2+", skills = [], ubicacion = "Colombia", scoreMin = 70, limit = 10 } = req.body;
    const skillsStr = Array.isArray(skills) ? skills.join(", ") : skills;
    const queries = [
      `site:linkedin.com/in "${cargo}" "${ubicacion}" open to work`,
      `site:computrabajo.com.co "${cargo}" ${skillsStr}`,
    ];
    const allResults = [];
    for (const query of queries) {
      try {
        const data = await (await fetch("https://api.firecrawl.dev/v1/search", {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}` },
          body: JSON.stringify({ query, limit: Math.ceil(limit / 2), lang: "es", country: "co", scrapeOptions: { formats: ["markdown"] } }),
        })).json();
        if (data.data) allResults.push(...data.data);
      } catch (e) { console.error("[TALENT SEARCH]", e.message); }
    }
    const scored = await Promise.all(
      allResults.slice(0, limit).map(async (item) => {
        const prompt = `Analiza candidato. SOLO JSON:
{"nombre":"...","cargo_actual":"...","empresa_actual":"...","ubicacion":"...","telefono":null,"email":null,"anos_experiencia":5,"skills_detectados":[],"score":85,"resumen":"2 oraciones","senal_apertura":"..."}
Cargo: ${cargo} | Exp: ${experiencia} | Skills: ${skillsStr} | Loc: ${ubicacion}
Perfil: ${item.markdown?.slice(0, 1000) || item.description || item.title || ""}`;
        try {
          const aiData = await (await fetch("https://api.anthropic.com/v1/messages", {
            method: "POST",
            headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
            body: JSON.stringify({ model: "claude-haiku-4-5-20251001", max_tokens: 400, messages: [{ role: "user", content: prompt }] }),
          })).json();
          const candidate = JSON.parse(aiData.content?.[0]?.text?.replace(/```json|```/g, "").trim() || "{}");
          return { ...candidate, url: item.url, fuente: new URL(item.url).hostname };
        } catch { return { nombre: item.title, url: item.url, score: 50, fuente: "web" }; }
      })
    );
    res.json({ candidates: scored.filter(c => (c.score || 0) >= scoreMin), total: scored.length, cargo, ubicacion });
  } catch (err) { next(err); }
});

app.post("/talent/match", async (req, res, next) => {
  try {
    const { candidate, requirements } = req.body;
    const aiData = await (await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514", max_tokens: 600,
        messages: [{ role: "user", content: `Match candidato-cargo. SOLO JSON:
{"score":84,"nivel":"Alto","tiene":[],"falta":[],"destacados":[],"brechas_criticas":[],"recomendacion":"...","mensaje_personalizado":"120 chars max"}
CANDIDATO: ${JSON.stringify(candidate)}
REQUISITOS: ${JSON.stringify(requirements)}` }],
      }),
    })).json();
    const match = JSON.parse(aiData.content?.[0]?.text?.replace(/```json|```/g, "").trim() || "{}");
    res.json({ ...match, candidate: candidate.nombre, cargo: requirements.cargo });
  } catch (err) { next(err); }
});

app.post("/talent/contact", async (req, res, next) => {
  try {
    const { phone, candidateName, cargo, matchScore, tiene = [], falta = [], mensaje } = req.body;
    if (!phone) return res.status(400).json({ error: "phone requerido" });
    const token = await getJelouToken();
    const msg = mensaje || `Hola ${candidateName}, tu perfil hace ${matchScore}% match con ${cargo}. Tienes: ${tiene.slice(0,2).join(", ")}. ¿Te interesa saber más?`;
    const data = await (await fetch(`https://api.jelou.ai/v1/bots/${process.env.JELOU_BOT_ID}/users/${phone}/messages`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
      body: JSON.stringify({ type: "TEXT", text: msg }),
    })).json();
    res.json({ ok: true, messageId: data.id, candidate: candidateName, mensaje: msg });
  } catch (err) { next(err); }
});

const talentPipeline = { nuevo: [], contactado: [], respondio: [], entrevista: [], seleccionado: [] };
app.get("/talent/pipeline", (_req, res) => res.json(talentPipeline));
app.post("/talent/pipeline/add", (req, res) => {
  const c = { ...req.body, id: Date.now().toString(), created_at: new Date().toISOString() };
  talentPipeline.nuevo.push(c);
  res.status(201).json(c);
});
app.patch("/talent/pipeline/:id/move", (req, res) => {
  const { id } = req.params; const { to } = req.body;
  for (const stage of Object.keys(talentPipeline)) {
    const idx = talentPipeline[stage].findIndex(c => c.id === id);
    if (idx !== -1) {
      const [c] = talentPipeline[stage].splice(idx, 1);
      (talentPipeline[to] ||= []).push({ ...c, stage: to });
      return res.json({ ok: true, candidate: { ...c, stage: to } });
    }
  }
  res.status(404).json({ error: "Candidato no encontrado" });
});

// ════════════════════════════════════════════════════════════
//  7. MERCADO PAGO — Pagos y suscripciones
// ════════════════════════════════════════════════════════════

const MP_ACCESS_TOKEN = process.env.MP_ACCESS_TOKEN || 'TEST-748633123758950-051218-c76f920ae7147ecd648d1d9225667eba-503051039';
const FRONTEND_URL    = process.env.FRONTEND_URL || 'https://prismdb.netlify.app';

const PLANES_MP = {
  semilla:   { nombre: 'PrismDB Semilla',         precio: 99   },
  starter:   { nombre: 'PrismDB Starter',         precio: 249  },
  pro:       { nombre: 'PrismDB Pro',             precio: 599  },
  enterprise:{ nombre: 'PrismDB Enterprise',      precio: 1299 },
  talent:    { nombre: 'PrismDB Talent Scanner',  precio: 299  },
};

// POST /payment/preference — Crea link de pago (checkout externo MP)
app.post("/payment/preference", async (req, res, next) => {
  try {
    const { plan = 'starter', email = 'cliente@prismdb.co', amount, description } = req.body;
    const p = PLANES_MP[plan] || PLANES_MP.starter;

    const body = {
      items: [{
        id:          plan,
        title:       description || p.nombre,
        quantity:    1,
        unit_price:  amount || p.precio,
        currency_id: 'COP',
      }],
      payer: { email },
      back_urls: {
        success: `${FRONTEND_URL}?payment=success&plan=${plan}`,
        failure: `${FRONTEND_URL}?payment=failure`,
        pending: `${FRONTEND_URL}?payment=pending`,
      },
      auto_return:          'approved',
      notification_url:     `${process.env.BACKEND_URL || 'https://prismdb-backend-production.up.railway.app'}/payment/webhook`,
      statement_descriptor: 'PRISMDB',
      expires:              false,
    };

    const mpRes = await fetch('https://api.mercadopago.com/checkout/preferences', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` },
      body:    JSON.stringify(body),
    });

    const data = await mpRes.json();
    if (data.error) throw new Error(data.message || data.error);

    res.json({
      preference_id: data.id,
      init_point:    data.init_point,       // producción
      sandbox_url:   data.sandbox_init_point, // pruebas
    });
  } catch (err) { next(err); }
});

// POST /payment/create — Procesa token de tarjeta (cardForm)
app.post("/payment/create", async (req, res, next) => {
  try {
    const {
      token, paymentMethodId, issuerId, installments = 1,
      identificationNumber, identificationType = 'CC',
      email, plan = 'starter', amount, description
    } = req.body;

    if (!token) return res.status(400).json({ error: 'token requerido' });

    const p = PLANES_MP[plan] || PLANES_MP.starter;

    const body = {
      transaction_amount: amount || p.precio,
      token,
      description:        description || p.nombre,
      installments:       Number(installments) || 1,
      payment_method_id:  paymentMethodId,
      issuer_id:          issuerId,
      payer: {
        email,
        identification: { type: identificationType, number: identificationNumber },
      },
      metadata: { plan, prismdb: true },
    };

    const mpRes = await fetch('https://api.mercadopago.com/v1/payments', {
      method:  'POST',
      headers: {
        'Content-Type':   'application/json',
        'Authorization':  `Bearer ${MP_ACCESS_TOKEN}`,
        'X-Idempotency-Key': `${plan}-${email}-${Date.now()}`,
      },
      body: JSON.stringify(body),
    });

    const data = await mpRes.json();

    if (data.error) throw new Error(data.message || data.error);

    // Guardar suscripción en memoria (en prod: guardar en Supabase)
    console.log(`[PAYMENT] ${data.status} — ${email} — Plan ${plan} — $${p.precio}`);

    res.json({
      id:            data.id,
      status:        data.status,         // approved | in_process | rejected
      status_detail: data.status_detail,
      plan,
      email,
    });
  } catch (err) { next(err); }
});

// POST /payment/webhook — Notificaciones de MercadoPago
app.post("/payment/webhook", async (req, res) => {
  try {
    const { type, data } = req.body;
    if (type === 'payment' && data?.id) {
      const mpRes  = await fetch(`https://api.mercadopago.com/v1/payments/${data.id}`, {
        headers: { 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` },
      });
      const payment = await mpRes.json();
      console.log(`[WEBHOOK MP] ${payment.status} — ${payment.payer?.email} — $${payment.transaction_amount}`);
      // TODO: activar cuenta en Supabase según payment.metadata.plan
    }
    res.sendStatus(200);
  } catch (err) {
    console.error('[WEBHOOK MP ERROR]', err.message);
    res.sendStatus(200); // siempre 200 para MP
  }
});

// GET /payment/status/:id — Consultar estado de un pago
app.get("/payment/status/:id", async (req, res, next) => {
  try {
    const mpRes = await fetch(`https://api.mercadopago.com/v1/payments/${req.params.id}`, {
      headers: { 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` },
    });
    const data = await mpRes.json();
    res.json({ id: data.id, status: data.status, status_detail: data.status_detail, plan: data.metadata?.plan });
  } catch (err) { next(err); }
});

// ── Error handler ────────────────────────────────────────
app.use((err, _req, res, _next) => {
  console.error("[ERROR]", err.message);
  res.status(500).json({ error: err.message || "Error interno" });
});

app.listen(PORT, () => {
  console.log(`✅ PrismDB backend corriendo en http://localhost:${PORT}`);
  console.log(`   Integraciones: Firecrawl · Jelou · Anthropic · MercadoPago`);
});
