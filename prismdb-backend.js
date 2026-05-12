// ═══════════════════════════════════════════════════════════
//  PrismDB — Backend Node.js
//  Generado por IntégraHub
//  Integraciones: Firecrawl · Jelou · Anthropic · Supabase
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

// POST /leads/search
// Body: { query, sector, cargo, ciudad, scoreMin, limit }
app.post("/leads/search", async (req, res, next) => {
  try {
    const { query, sector = "", cargo = "", ciudad = "", scoreMin = 70, limit = 10 } = req.body;

    const searchQuery = `${cargo} ${sector} ${ciudad} ${query} WhatsApp contacto`.trim();

    // 1. Firecrawl — buscar leads
    const fcRes = await fetch("https://api.firecrawl.dev/v1/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}`,
      },
      body: JSON.stringify({
        query: searchQuery,
        limit,
        lang: "es",
        country: "co",
        scrapeOptions: { formats: ["markdown"] },
      }),
    });

    const fcData = await fcRes.json();
    const results = fcData.data || [];

    // 2. Claude — score cada lead
    const scoredLeads = await Promise.all(
      results.map(async (item) => {
        const prompt = `Analiza este perfil y devuelve SOLO JSON:
{"nombre":"...","cargo":"...","empresa":"...","telefono":"...o null","email":"...o null","ciudad":"...","score":85,"razon":"por qué es buen prospecto en 10 palabras"}

Perfil: ${item.markdown?.slice(0, 800) || item.description || item.title}
Criterios: cargo=${cargo}, sector=${sector}, ciudad=${ciudad}`;

        try {
          const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "x-api-key": process.env.ANTHROPIC_API_KEY,
              "anthropic-version": "2023-06-01",
            },
            body: JSON.stringify({
              model: "claude-haiku-4-5-20251001",
              max_tokens: 300,
              messages: [{ role: "user", content: prompt }],
            }),
          });
          const aiData = await aiRes.json();
          const text   = aiData.content?.[0]?.text || "{}";
          const clean  = text.replace(/```json|```/g, "").trim();
          const lead   = JSON.parse(clean);
          return { ...lead, url: item.url, source: "firecrawl" };
        } catch {
          return { nombre: item.title, url: item.url, score: 50, source: "firecrawl" };
        }
      })
    );

    // Filtrar por score mínimo
    const filtered = scoredLeads.filter(l => (l.score || 0) >= scoreMin);

    res.json({ leads: filtered, total: filtered.length, query: searchQuery });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  2. MENSAJERÍA — Enviar WhatsApp via Jelou
// ════════════════════════════════════════════════════════════

// Obtener token Jelou
async function getJelouToken() {
  const res = await fetch("https://api.jelou.ai/v1/auth/token", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      client_id:     process.env.JELOU_CLIENT_ID,
      client_secret: process.env.JELOU_CLIENT_SECRET,
      grant_type:    "client_credentials",
    }),
  });
  const data = await res.json();
  return data.access_token;
}

// POST /messages/send
// Body: { phone, message, leadName }
app.post("/messages/send", async (req, res, next) => {
  try {
    const { phone, message, leadName = "" } = req.body;
    if (!phone || !message) return res.status(400).json({ error: "phone y message requeridos" });

    const token = await getJelouToken();

    const jelouRes = await fetch(`https://api.jelou.ai/v1/bots/${process.env.JELOU_BOT_ID}/users/${phone}/messages`, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({ type: "TEXT", text: message }),
    });

    const data = await jelouRes.json();
    res.json({ ok: true, messageId: data.id, lead: leadName });
  } catch (err) { next(err); }
});

// POST /messages/bulk
// Body: { leads: [{phone, name}], template, variables }
app.post("/messages/bulk", async (req, res, next) => {
  try {
    const { leads, template, variables = {} } = req.body;
    if (!leads?.length) return res.status(400).json({ error: "leads requerido" });

    const token = await getJelouToken();
    const results = [];

    for (const lead of leads) {
      const msg = template.replace(/\{\{nombre\}\}/g, lead.name || lead.nombre || "")
                          .replace(/\{\{empresa\}\}/g, lead.empresa || "")
                          .replace(/\{\{ciudad\}\}/g, lead.ciudad || "");
      try {
        const r = await fetch(`https://api.jelou.ai/v1/bots/${process.env.JELOU_BOT_ID}/users/${lead.phone}/messages`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
          body: JSON.stringify({ type: "TEXT", text: msg }),
        });
        const d = await r.json();
        results.push({ phone: lead.phone, ok: true, id: d.id });
      } catch (e) {
        results.push({ phone: lead.phone, ok: false, error: e.message });
      }
      // Rate limiting: 1 mensaje cada 500ms
      await new Promise(r => setTimeout(r, 500));
    }

    const sent   = results.filter(r => r.ok).length;
    const failed = results.filter(r => !r.ok).length;
    res.json({ sent, failed, results });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  3. IA — Generar mensajes personalizados con Claude
// ════════════════════════════════════════════════════════════

// POST /ai/message
// Body: { lead, prompt, businessContext }
app.post("/ai/message", async (req, res, next) => {
  try {
    const { lead, prompt, businessContext = "" } = req.body;

    const systemPrompt = `Eres un experto en ventas B2B para LATAM. 
Genera mensajes de WhatsApp personalizados, directos y que generen respuesta.
Máximo 160 caracteres. Sin emojis excesivos. Habla de tú.
Contexto del negocio: ${businessContext}`;

    const userPrompt = prompt || 
      `Genera un mensaje de prospección para: ${lead.nombre}, ${lead.cargo} en ${lead.empresa} (${lead.ciudad}).
Su posible dolor: ${lead.razon || "necesita optimizar sus procesos comerciales"}.`;

    const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 400,
        system: systemPrompt,
        messages: [{ role: "user", content: userPrompt }],
      }),
    });

    const data = await aiRes.json();
    res.json({ message: data.content?.[0]?.text || "", lead });
  } catch (err) { next(err); }
});

// POST /ai/score
// Body: { profile, criteria }
app.post("/ai/score", async (req, res, next) => {
  try {
    const { profile, criteria } = req.body;
    const prompt = `Analiza este perfil y dale un score de 0-100 como prospecto de ventas.
Devuelve SOLO JSON: {"score":85,"nivel":"Alto","razones":["razón 1","razón 2"],"recomendacion":"acción sugerida"}

Perfil: ${JSON.stringify(profile)}
Criterios ideales: ${JSON.stringify(criteria)}`;

    const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 300,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const data  = await aiRes.json();
    const text  = data.content?.[0]?.text || "{}";
    const clean = text.replace(/```json|```/g, "").trim();
    res.json(JSON.parse(clean));
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  4. CRM — Pipeline de ventas
// ════════════════════════════════════════════════════════════

// En memoria para demo — en producción conectar a Supabase
const pipeline = {
  contacto:    [],
  calificado:  [],
  negociacion: [],
  cerrado:     [],
};

app.get("/crm/pipeline", (_req, res) => res.json(pipeline));

app.post("/crm/lead", (req, res) => {
  const lead = { ...req.body, id: Date.now().toString(), created_at: new Date().toISOString() };
  const stage = lead.stage || "contacto";
  if (!pipeline[stage]) pipeline[stage] = [];
  pipeline[stage].push(lead);
  res.status(201).json(lead);
});

app.patch("/crm/lead/:id/move", (req, res) => {
  const { id } = req.params;
  const { to }  = req.body;
  for (const stage of Object.keys(pipeline)) {
    const idx = pipeline[stage].findIndex(l => l.id === id);
    if (idx !== -1) {
      const [lead] = pipeline[stage].splice(idx, 1);
      if (!pipeline[to]) pipeline[to] = [];
      pipeline[to].push({ ...lead, stage: to });
      return res.json({ ok: true, lead: { ...lead, stage: to } });
    }
  }
  res.status(404).json({ error: "Lead no encontrado" });
});

// ════════════════════════════════════════════════════════════
//  5. WEBHOOK — Recibir mensajes entrantes de Jelou
// ════════════════════════════════════════════════════════════
app.post("/webhook/jelou", (req, res) => {
  const { event, data } = req.body;
  console.log(`[JELOU WEBHOOK] ${event}:`, JSON.stringify(data).slice(0, 200));
  // TODO: procesar respuestas y mover leads en el pipeline
  res.sendStatus(200);
});

// ── Error handler ────────────────────────────────────────
app.use((err, _req, res, _next) => {
  console.error("[ERROR]", err.message);
  res.status(500).json({ error: err.message || "Error interno" });
});

app.listen(PORT, () => {
  console.log(`✅ PrismDB backend corriendo en http://localhost:${PORT}`);
  console.log(`   Integraciones: Firecrawl · Jelou · Anthropic`);
});
