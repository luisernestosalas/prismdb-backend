// ═══════════════════════════════════════════════════════════
//  PrismDB — Backend Node.js
//  Integraciones: Firecrawl · Twilio WhatsApp · Anthropic · MercadoPago
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
//  TWILIO — WhatsApp
// ════════════════════════════════════════════════════════════
const TWILIO_SID   = process.env.TWILIO_ACCOUNT_SID  || 'AC9171340052e334043fe1805126b2ca60';
const TWILIO_TOKEN = process.env.TWILIO_AUTH_TOKEN    || '3632808f81f739c76a0287ce31e00df9';
const TWILIO_FROM  = process.env.TWILIO_WHATSAPP_FROM || 'whatsapp:+14155238886';

async function sendWhatsApp(to, message) {
  const phone = to.startsWith('whatsapp:') ? to : `whatsapp:+57${to.replace(/\D/g,'')}`;
  const credentials = Buffer.from(`${TWILIO_SID}:${TWILIO_TOKEN}`).toString('base64');
  const res = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`, {
    method: 'POST',
    headers: { 'Authorization': `Basic ${credentials}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ From: TWILIO_FROM, To: phone, Body: message }),
  });
  return await res.json();
}

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

    const results = (await fcRes.json()).data || [];

    const scoredLeads = await Promise.all(
      results.map(async (item) => {
        const prompt = `Analiza este perfil y devuelve SOLO JSON:
{"nombre":"...","cargo":"...","empresa":"...","telefono":"...o null","email":"...o null","ciudad":"...","score":85,"razon":"por qué es buen prospecto en 10 palabras"}
Perfil: ${item.markdown?.slice(0, 800) || item.description || item.title}
Criterios: cargo=${cargo}, sector=${sector}, ciudad=${ciudad}`;
        try {
          const aiData = await (await fetch("https://api.anthropic.com/v1/messages", {
            method: "POST",
            headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
            body: JSON.stringify({ model: "claude-haiku-4-5-20251001", max_tokens: 300, messages: [{ role: "user", content: prompt }] }),
          })).json();
          const lead = JSON.parse(aiData.content?.[0]?.text?.replace(/```json|```/g, "").trim() || "{}");
          return { ...lead, url: item.url, source: "firecrawl" };
        } catch { return { nombre: item.title, url: item.url, score: 50, source: "firecrawl" }; }
      })
    );

    res.json({ leads: scoredLeads.filter(l => (l.score || 0) >= scoreMin), total: scoredLeads.length, query: searchQuery });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  2. MENSAJERÍA — WhatsApp via Twilio
// ════════════════════════════════════════════════════════════

// POST /messages/send
app.post("/messages/send", async (req, res, next) => {
  try {
    const { phone, message, leadName = "" } = req.body;
    if (!phone || !message) return res.status(400).json({ error: "phone y message requeridos" });
    const data = await sendWhatsApp(phone, message);
    if (data.error_code) throw new Error(data.message);
    res.json({ ok: true, messageId: data.sid, lead: leadName });
  } catch (err) { next(err); }
});

// POST /messages/bulk
app.post("/messages/bulk", async (req, res, next) => {
  try {
    const { leads, template } = req.body;
    if (!leads?.length) return res.status(400).json({ error: "leads requerido" });
    const results = [];
    for (const lead of leads) {
      const msg = template
        .replace(/\{\{nombre\}\}/g, lead.name || lead.nombre || "")
        .replace(/\{\{empresa\}\}/g, lead.empresa || "")
        .replace(/\{\{ciudad\}\}/g, lead.ciudad || "");
      try {
        const d = await sendWhatsApp(lead.phone, msg);
        results.push({ phone: lead.phone, ok: !d.error_code, id: d.sid, error: d.message });
      } catch (e) { results.push({ phone: lead.phone, ok: false, error: e.message }); }
      await new Promise(r => setTimeout(r, 1000)); // 1 msg/seg para evitar rate limit
    }
    res.json({ sent: results.filter(r => r.ok).length, failed: results.filter(r => !r.ok).length, results });
  } catch (err) { next(err); }
});

// POST /messages/test — enviar mensaje de prueba
app.post("/messages/test", async (req, res, next) => {
  try {
    const { phone, message = "¡Hola desde PrismDB! 🚀 Tu plataforma de prospección y ventas está funcionando." } = req.body;
    if (!phone) return res.status(400).json({ error: "phone requerido (sin +57, ej: 3001234567)" });
    const data = await sendWhatsApp(phone, message);
    if (data.error_code) throw new Error(`Twilio error ${data.error_code}: ${data.message}`);
    res.json({ ok: true, messageId: data.sid, to: data.to, status: data.status });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  3. IA — Mensajes y scoring con Claude
// ════════════════════════════════════════════════════════════
app.post("/ai/message", async (req, res, next) => {
  try {
    const { lead, prompt, businessContext = "" } = req.body;
    const aiData = await (await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514", max_tokens: 400,
        system: `Eres experto en ventas B2B para LATAM. Genera mensajes WhatsApp personalizados, directos. Máximo 160 caracteres. Contexto: ${businessContext}`,
        messages: [{ role: "user", content: prompt || `Genera mensaje para: ${lead.nombre}, ${lead.cargo} en ${lead.empresa} (${lead.ciudad}).` }],
      }),
    })).json();
    res.json({ message: aiData.content?.[0]?.text || "", lead });
  } catch (err) { next(err); }
});

app.post("/ai/score", async (req, res, next) => {
  try {
    const { profile, criteria } = req.body;
    const aiData = await (await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001", max_tokens: 300,
        messages: [{ role: "user", content: `Score 0-100. SOLO JSON: {"score":85,"nivel":"Alto","razones":[],"recomendacion":"..."}
Perfil: ${JSON.stringify(profile)} Criterios: ${JSON.stringify(criteria)}` }],
      }),
    })).json();
    res.json(JSON.parse(aiData.content?.[0]?.text?.replace(/```json|```/g, "").trim() || "{}"));
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
//  5. WEBHOOK Twilio — recibir mensajes entrantes
// ════════════════════════════════════════════════════════════
app.post("/webhook/twilio", express.urlencoded({ extended: false }), (req, res) => {
  const { From, Body, ProfileName } = req.body;
  console.log(`[TWILIO WEBHOOK] ${ProfileName || From}: ${Body}`);
  // TODO: procesar respuestas y mover leads en el pipeline
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');
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
    const msg = mensaje || `Hola ${candidateName}, tu perfil hace ${matchScore}% match con ${cargo}. Tienes: ${tiene.slice(0,2).join(", ")}. ¿Te interesa saber más?`;
    const data = await sendWhatsApp(phone, msg);
    if (data.error_code) throw new Error(`Twilio error: ${data.message}`);
    res.json({ ok: true, messageId: data.sid, candidate: candidateName, mensaje: msg });
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
//  7. MERCADO PAGO
// ════════════════════════════════════════════════════════════
const MP_ACCESS_TOKEN = process.env.MP_ACCESS_TOKEN || 'TEST-748633123758950-051218-c76f920ae7147ecd648d1d9225667eba-503051039';
const FRONTEND_URL    = process.env.FRONTEND_URL || 'https://prismdb.netlify.app';

const PLANES_MP = {
  semilla:   { nombre: 'PrismDB Semilla',        precio: 99   },
  starter:   { nombre: 'PrismDB Starter',        precio: 249  },
  pro:       { nombre: 'PrismDB Pro',            precio: 599  },
  enterprise:{ nombre: 'PrismDB Enterprise',     precio: 1299 },
  talent:    { nombre: 'PrismDB Talent Scanner', precio: 299  },
};

app.post("/payment/preference", async (req, res, next) => {
  try {
    const { plan = 'starter', email = 'cliente@prismdb.co', amount, description } = req.body;
    const p = PLANES_MP[plan] || PLANES_MP.starter;
    const body = {
      items: [{ id: plan, title: description || p.nombre, quantity: 1, unit_price: amount || p.precio, currency_id: 'COP' }],
      payer: { email },
      back_urls: { success: `${FRONTEND_URL}?payment=success&plan=${plan}`, failure: `${FRONTEND_URL}?payment=failure`, pending: `${FRONTEND_URL}?payment=pending` },
      auto_return: 'approved',
      notification_url: `${process.env.BACKEND_URL || 'https://prismdb-backend-production.up.railway.app'}/payment/webhook`,
      statement_descriptor: 'PRISMDB',
    };
    const data = await (await fetch('https://api.mercadopago.com/checkout/preferences', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` },
      body: JSON.stringify(body),
    })).json();
    if (data.error) throw new Error(data.message || data.error);
    res.json({ preference_id: data.id, init_point: data.init_point, sandbox_url: data.sandbox_init_point });
  } catch (err) { next(err); }
});

app.post("/payment/create", async (req, res, next) => {
  try {
    const { token, paymentMethodId, issuerId, installments = 1, identificationNumber, identificationType = 'CC', email, plan = 'starter', amount, description } = req.body;
    if (!token) return res.status(400).json({ error: 'token requerido' });
    const p = PLANES_MP[plan] || PLANES_MP.starter;
    const data = await (await fetch('https://api.mercadopago.com/v1/payments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${MP_ACCESS_TOKEN}`, 'X-Idempotency-Key': `${plan}-${email}-${Date.now()}` },
      body: JSON.stringify({ transaction_amount: amount || p.precio, token, description: description || p.nombre, installments: Number(installments) || 1, payment_method_id: paymentMethodId, issuer_id: issuerId, payer: { email, identification: { type: identificationType, number: identificationNumber } }, metadata: { plan } }),
    })).json();
    if (data.error) throw new Error(data.message || data.error);
    console.log(`[PAYMENT] ${data.status} — ${email} — Plan ${plan}`);
    res.json({ id: data.id, status: data.status, status_detail: data.status_detail, plan, email });
  } catch (err) { next(err); }
});

app.post("/payment/webhook", async (req, res) => {
  try {
    const { type, data } = req.body;
    if (type === 'payment' && data?.id) {
      const payment = await (await fetch(`https://api.mercadopago.com/v1/payments/${data.id}`, { headers: { 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` } })).json();
      console.log(`[WEBHOOK MP] ${payment.status} — ${payment.payer?.email} — $${payment.transaction_amount}`);
    }
    res.sendStatus(200);
  } catch { res.sendStatus(200); }
});

app.get("/payment/status/:id", async (req, res, next) => {
  try {
    const data = await (await fetch(`https://api.mercadopago.com/v1/payments/${req.params.id}`, { headers: { 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` } })).json();
    res.json({ id: data.id, status: data.status, status_detail: data.status_detail, plan: data.metadata?.plan });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  8. CATÁLOGO — Extraer producto desde redes sociales
// ════════════════════════════════════════════════════════════

// POST /catalog/extract
// Body: { url } — URL de post de Instagram, Facebook, TikTok
app.post("/catalog/extract", async (req, res, next) => {
  try {
    const { url } = req.body;
    if (!url) return res.status(400).json({ error: "url requerida" });

    // 1. Firecrawl scrape the URL
    const fcRes = await fetch("https://api.firecrawl.dev/v1/scrape", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}` },
      body: JSON.stringify({ url, formats: ["markdown", "screenshot"] }),
    });
    const fcData = await fcRes.json();
    const content = fcData.data?.markdown || fcData.markdown || "";

    // 2. Claude extrae nombre, precio, descripción
    const prompt = `Analiza este contenido de redes sociales y extrae la información del producto. Devuelve SOLO JSON:
{
  "nombre": "nombre del producto",
  "precio": "precio con símbolo de moneda o null",
  "descripcion": "descripción del producto en 1-2 oraciones",
  "imagen_url": "url de imagen si la detectas o null"
}

Contenido: ${content.slice(0, 2000)}`;

    const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model: "claude-haiku-4-5-20251001", max_tokens: 300, messages: [{ role: "user", content: prompt }] }),
    });
    const aiData = await aiRes.json();
    const text = aiData.content?.[0]?.text || "{}";
    const product = JSON.parse(text.replace(/```json|```/g, "").trim());

    res.json({ ...product, url, fuente: new URL(url).hostname });
  } catch (err) { next(err); }
});

// ── Error handler ────────────────────────────────────────
app.use((err, _req, res, _next) => {
  console.error("[ERROR]", err.message);
  res.status(500).json({ error: err.message || "Error interno" });
});

app.listen(PORT, () => {
  console.log(`✅ PrismDB backend corriendo en http://localhost:${PORT}`);
  console.log(`   Integraciones: Firecrawl · Twilio WhatsApp · Anthropic · MercadoPago`);
});
