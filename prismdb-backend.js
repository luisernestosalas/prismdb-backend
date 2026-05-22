// ═══════════════════════════════════════════════════════════
//  PrismDB — Backend v2.1 — Autonomous Enterprise OS™
//  Memory Layer · AI Router · Event Bus · 4 Módulos IA
// ═══════════════════════════════════════════════════════════

import express from "express";
import cors from "cors";
import helmet from "helmet";
import dotenv from "dotenv";
import pg from "pg";
import { processEvent, startEventBus, EVENT_TYPES, autonomyDecision, classifyEvent } from "./router.js";

dotenv.config();

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(helmet());
app.use(cors({ origin: process.env.FRONTEND_URL || "*" }));
app.use(express.json({ limit: "2mb" }));

// ══════════════════════════════════════════════════════════
//  DATABASE — PostgreSQL (Supabase)
// ══════════════════════════════════════════════════════════
let db = null;
let memoryMode = "in-memory";

if (process.env.DATABASE_URL) {
  const { Pool } = pg;
  db = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 5,
  });

  db.connect()
    .then(async (client) => {
      // Crear tablas si no existen
      await client.query(`
        CREATE TABLE IF NOT EXISTS memory (
          id          SERIAL PRIMARY KEY,
          entity_id   TEXT NOT NULL,
          entity_type TEXT NOT NULL DEFAULT 'contact',
          data        JSONB NOT NULL DEFAULT '{}',
          created_at  TIMESTAMPTZ DEFAULT NOW(),
          updated_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS memory_entity_id_idx ON memory(entity_id);

        CREATE TABLE IF NOT EXISTS events (
          id          SERIAL PRIMARY KEY,
          type        TEXT NOT NULL,
          entity_id   TEXT,
          payload     JSONB NOT NULL DEFAULT '{}',
          created_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS pipeline (
          id          SERIAL PRIMARY KEY,
          entity_id   TEXT NOT NULL UNIQUE,
          stage       TEXT NOT NULL DEFAULT 'contacto',
          data        JSONB NOT NULL DEFAULT '{}',
          revenue     NUMERIC DEFAULT 0,
          probability NUMERIC DEFAULT 0,
          updated_at  TIMESTAMPTZ DEFAULT NOW()
        );
      `);
      client.release();
      memoryMode = "postgresql";
      console.log("✅ PostgreSQL conectado — Memory Layer activa");
      startEventBus(db, 60000);
    })
    .catch((err) => {
      console.error("⚠️  PostgreSQL error:", err.message);
      memoryMode = "in-memory";
    });
}

// In-memory fallback
const memoryStore = {};
const eventsStore = [];

// ── Health ────────────────────────────────────────────────
app.get("/health", (_req, res) =>
  res.json({ ok: true, app: "prismdb", version: "2.1", memory: memoryMode, event_bus: memoryMode === "postgresql" ? "active" : "standby", ts: Date.now() })
);

// ══════════════════════════════════════════════════════════
//  HELPERS
// ══════════════════════════════════════════════════════════
const TWILIO_SID   = process.env.TWILIO_ACCOUNT_SID  || 'AC9171340052e334043fe1805126b2ca60';
const TWILIO_TOKEN = process.env.TWILIO_AUTH_TOKEN    || '3632808f81f739c76a0287ce31e00df9';
const TWILIO_FROM  = process.env.TWILIO_WHATSAPP_FROM || 'whatsapp:+14155238886';

async function sendWhatsApp(to, message) {
  const phone = to.startsWith('whatsapp:') ? to : `whatsapp:+57${to.replace(/\D/g, '')}`;
  const credentials = Buffer.from(`${TWILIO_SID}:${TWILIO_TOKEN}`).toString('base64');
  const res = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`, {
    method: 'POST',
    headers: { 'Authorization': `Basic ${credentials}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ From: TWILIO_FROM, To: phone, Body: message }),
  });
  return await res.json();
}

async function claudeChat(system, userContent, model = "claude-haiku-4-5-20251001", maxTokens = 500) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
    body: JSON.stringify({ model, max_tokens: maxTokens, system, messages: [{ role: "user", content: userContent }] }),
  });
  const data = await res.json();
  return data.content?.[0]?.text || "";
}

async function logEvent(type, entityId, payload) {
  if (db && memoryMode === "postgresql") {
    await db.query(
      "INSERT INTO events (type, entity_id, payload) VALUES ($1, $2, $3)",
      [type, entityId, payload]
    ).catch(() => {});
  } else {
    eventsStore.push({ type, entity_id: entityId, payload, created_at: new Date().toISOString() });
  }
}

// ══════════════════════════════════════════════════════════
//  MEMORY LAYER
// ══════════════════════════════════════════════════════════

// GET /memory/:entityId
app.get("/memory/:entityId", async (req, res, next) => {
  try {
    const { entityId } = req.params;
    if (db && memoryMode === "postgresql") {
      const result = await db.query("SELECT * FROM memory WHERE entity_id = $1", [entityId]);
      return res.json(result.rows[0] || { entity_id: entityId, data: {} });
    }
    res.json({ entity_id: entityId, data: memoryStore[entityId] || {} });
  } catch (err) { next(err); }
});

// POST /memory/:entityId
app.post("/memory/:entityId", async (req, res, next) => {
  try {
    const { entityId } = req.params;
    const { data, entity_type = "contact" } = req.body;

    if (db && memoryMode === "postgresql") {
      const result = await db.query(`
        INSERT INTO memory (entity_id, entity_type, data)
        VALUES ($1, $2, $3)
        ON CONFLICT (entity_id) DO UPDATE
          SET data = memory.data || $3, updated_at = NOW()
        RETURNING *
      `, [entityId, entity_type, data]);
      return res.json(result.rows[0]);
    }
    memoryStore[entityId] = { ...(memoryStore[entityId] || {}), ...data };
    res.json({ entity_id: entityId, data: memoryStore[entityId] });
  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  EVENTS
// ══════════════════════════════════════════════════════════
app.get("/events", async (_req, res, next) => {
  try {
    if (db && memoryMode === "postgresql") {
      const result = await db.query("SELECT * FROM events ORDER BY created_at DESC LIMIT 100");
      return res.json({ events: result.rows });
    }
    res.json({ events: eventsStore.slice(-100).reverse() });
  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  AGENTS — Claude decide qué agente actúa
// ══════════════════════════════════════════════════════════

// POST /agents/route — Claude clasifica el mensaje y decide qué agente actúa
app.post("/agents/route", async (req, res, next) => {
  try {
    const { message, entityId, context = {} } = req.body;

    // Leer memoria del contacto
    let memory = {};
    if (db && memoryMode === "postgresql") {
      const r = await db.query("SELECT data FROM memory WHERE entity_id = $1", [entityId]);
      memory = r.rows[0]?.data || {};
    } else {
      memory = memoryStore[entityId] || {};
    }

    const decision = await claudeChat(
      `Eres el router del Revenue OS de PrismDB. Analiza el mensaje y decide qué agente debe responder.
Agentes disponibles:
- sdr: prospección, primer contacto, calificación de leads
- revenue: ventas, negociación, cierre, follow-up de oportunidades
- talent: reclutamiento, candidatos, vacantes
- none: mensaje no relevante para ningún agente

Responde SOLO JSON: {"agent":"sdr|revenue|talent|none","reason":"una frase","priority":"high|medium|low"}`,
      `Mensaje: "${message}"
Historial del contacto: ${JSON.stringify(memory)}
Contexto adicional: ${JSON.stringify(context)}`
    );

    const parsed = JSON.parse(decision.replace(/```json|```/g, "").trim());
    await logEvent("agent_routed", entityId, { message, ...parsed });
    res.json({ ...parsed, entityId, message });
  } catch (err) { next(err); }
});

// POST /agents/sdr — Agente de Prospección
app.post("/agents/sdr", async (req, res, next) => {
  try {
    const { message, entityId, leadData = {} } = req.body;

    // Leer memoria
    let memory = {};
    if (db && memoryMode === "postgresql") {
      const r = await db.query("SELECT data FROM memory WHERE entity_id = $1", [entityId]);
      memory = r.rows[0]?.data || {};
    } else {
      memory = memoryStore[entityId] || {};
    }

    const response = await claudeChat(
      `Eres el Agente SDR de PrismDB — experto en prospección B2B para LATAM.
Tu objetivo: calificar leads, generar interés, conseguir una reunión o demo.
Tono: directo, cálido, profesional. Mensajes cortos (max 160 chars para WhatsApp).
Recuerda siempre el historial del contacto y personaliza cada mensaje.`,
      `Mensaje recibido: "${message}"
Historial del contacto: ${JSON.stringify(memory)}
Datos del lead: ${JSON.stringify(leadData)}

Responde con JSON:
{"respuesta":"mensaje para enviar por WhatsApp","accion":"calificar|agendar|nutrir|cerrar","stage":"nuevo|contactado|calificado|negociacion","notas":"observaciones internas"}`
    );

    const parsed = JSON.parse(response.replace(/```json|```/g, "").trim());

    // Actualizar memoria
    const newMemory = {
      ...memory,
      last_message: message,
      last_response: parsed.respuesta,
      stage: parsed.stage,
      last_agent: "sdr",
      updated_at: new Date().toISOString(),
    };

    if (db && memoryMode === "postgresql") {
      await db.query(`
        INSERT INTO memory (entity_id, entity_type, data)
        VALUES ($1, 'lead', $2)
        ON CONFLICT (entity_id) DO UPDATE SET data = $2, updated_at = NOW()
      `, [entityId, newMemory]);
    } else {
      memoryStore[entityId] = newMemory;
    }

    await logEvent("sdr_response", entityId, { message, response: parsed });
    res.json({ ...parsed, entityId, agent: "sdr" });
  } catch (err) { next(err); }
});

// POST /agents/revenue — Agente de Ventas
app.post("/agents/revenue", async (req, res, next) => {
  try {
    const { message, entityId, dealData = {} } = req.body;

    let memory = {};
    if (db && memoryMode === "postgresql") {
      const r = await db.query("SELECT data FROM memory WHERE entity_id = $1", [entityId]);
      memory = r.rows[0]?.data || {};
    } else {
      memory = memoryStore[entityId] || {};
    }

    const response = await claudeChat(
      `Eres el Agente Revenue de PrismDB — experto en cierre de ventas B2B.
Tu objetivo: avanzar oportunidades al cierre, manejar objeciones, generar urgencia.
Tienes acceso al historial completo del contacto. Usa esa información para personalizar.
Mensajes WhatsApp: máximo 200 chars, directos y con valor claro.`,
      `Mensaje: "${message}"
Historial: ${JSON.stringify(memory)}
Deal: ${JSON.stringify(dealData)}

JSON: {"respuesta":"...","objecion_detectada":"...o null","probabilidad_cierre":75,"siguiente_accion":"...","revenue_esperado":0}`
    );

    const parsed = JSON.parse(response.replace(/```json|```/g, "").trim());

    const newMemory = {
      ...memory,
      last_message: message,
      revenue_probability: parsed.probabilidad_cierre,
      expected_revenue: parsed.revenue_esperado,
      last_agent: "revenue",
      updated_at: new Date().toISOString(),
    };

    if (db && memoryMode === "postgresql") {
      await db.query(`
        INSERT INTO memory (entity_id, entity_type, data)
        VALUES ($1, 'deal', $2)
        ON CONFLICT (entity_id) DO UPDATE SET data = $2, updated_at = NOW()
      `, [entityId, newMemory]);
    } else {
      memoryStore[entityId] = newMemory;
    }

    await logEvent("revenue_response", entityId, { message, response: parsed });
    res.json({ ...parsed, entityId, agent: "revenue" });
  } catch (err) { next(err); }
});

// POST /agents/talent — Agente de Talento
app.post("/agents/talent", async (req, res, next) => {
  try {
    const { message, entityId, candidateData = {} } = req.body;

    let memory = {};
    if (db && memoryMode === "postgresql") {
      const r = await db.query("SELECT data FROM memory WHERE entity_id = $1", [entityId]);
      memory = r.rows[0]?.data || {};
    } else {
      memory = memoryStore[entityId] || {};
    }

    const response = await claudeChat(
      `Eres el Agente Talent de PrismDB — especialista en reclutamiento y employer branding.
Tu objetivo: identificar candidatos, generar interés en vacantes, avanzar en el proceso.
Tono: empático, motivador, claro sobre la oportunidad.`,
      `Mensaje: "${message}"
Historial: ${JSON.stringify(memory)}
Candidato: ${JSON.stringify(candidateData)}

JSON: {"respuesta":"...","interes_detectado":"alto|medio|bajo","etapa":"nuevo|contactado|interesado|entrevista","notas":"..."}`
    );

    const parsed = JSON.parse(response.replace(/```json|```/g, "").trim());

    const newMemory = {
      ...memory,
      last_message: message,
      talent_stage: parsed.etapa,
      interest_level: parsed.interes_detectado,
      last_agent: "talent",
      updated_at: new Date().toISOString(),
    };

    if (db && memoryMode === "postgresql") {
      await db.query(`
        INSERT INTO memory (entity_id, entity_type, data)
        VALUES ($1, 'candidate', $2)
        ON CONFLICT (entity_id) DO UPDATE SET data = $2, updated_at = NOW()
      `, [entityId, newMemory]);
    } else {
      memoryStore[entityId] = newMemory;
    }

    await logEvent("talent_response", entityId, { message, response: parsed });
    res.json({ ...parsed, entityId, agent: "talent" });
  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  FINANCE AI
// ══════════════════════════════════════════════════════════

// GET /finance/pipeline — pipeline con revenue probable
app.get("/finance/pipeline", async (_req, res, next) => {
  try {
    if (db && memoryMode === "postgresql") {
      const result = await db.query(`
        SELECT
          stage,
          COUNT(*) as deals,
          SUM(revenue) as total_revenue,
          AVG(probability) as avg_probability,
          SUM(revenue * probability / 100) as weighted_revenue
        FROM pipeline
        GROUP BY stage
        ORDER BY stage
      `);
      const total = await db.query("SELECT SUM(revenue * probability / 100) as forecast FROM pipeline");
      return res.json({
        stages: result.rows,
        forecast_30d: parseFloat(total.rows[0]?.forecast || 0),
        currency: "USD"
      });
    }
    // Fallback in-memory
    res.json({ stages: [], forecast_30d: 0, currency: "USD", mode: "in-memory" });
  } catch (err) { next(err); }
});

// POST /finance/predict — predicción 30 días con IA
app.post("/finance/predict", async (req, res, next) => {
  try {
    const { historicalData = [], currentPipeline = [] } = req.body;

    const prediction = await claudeChat(
      `Eres un analista financiero especializado en revenue forecasting para SaaS B2B LATAM.
Analiza el pipeline y genera predicciones realistas de revenue a 30, 60 y 90 días.
Identifica riesgos y oportunidades. Responde SOLO JSON.`,
      `Pipeline actual: ${JSON.stringify(currentPipeline)}
Histórico: ${JSON.stringify(historicalData)}

JSON:
{
  "forecast_30d": {"amount": 0, "confidence": 75, "deals_expected": 0},
  "forecast_60d": {"amount": 0, "confidence": 60, "deals_expected": 0},
  "forecast_90d": {"amount": 0, "confidence": 45, "deals_expected": 0},
  "risks": ["riesgo 1", "riesgo 2"],
  "opportunities": ["oportunidad 1"],
  "recommendation": "acción recomendada"
}`,
      "claude-sonnet-4-20250514",
      800
    );

    const parsed = JSON.parse(prediction.replace(/```json|```/g, "").trim());
    await logEvent("finance_prediction", "system", parsed);
    res.json(parsed);
  } catch (err) { next(err); }
});

// POST /finance/alert — detecta clientes en riesgo
app.post("/finance/alert", async (req, res, next) => {
  try {
    let contacts = [];

    if (db && memoryMode === "postgresql") {
      const result = await db.query(`
        SELECT entity_id, data, updated_at
        FROM memory
        WHERE updated_at < NOW() - INTERVAL '7 days'
           OR (data->>'revenue_probability')::numeric < 30
        LIMIT 20
      `);
      contacts = result.rows;
    } else {
      contacts = Object.entries(memoryStore)
        .map(([id, data]) => ({ entity_id: id, data }))
        .filter(c => (c.data.revenue_probability || 100) < 30)
        .slice(0, 20);
    }

    if (!contacts.length) {
      return res.json({ alerts: [], message: "No hay clientes en riesgo detectados" });
    }

    const analysis = await claudeChat(
      `Analiza estos contactos/deals y detecta cuáles están en riesgo de perderse.
Para cada uno indica el riesgo y la acción inmediata recomendada. SOLO JSON.`,
      `Contactos: ${JSON.stringify(contacts.slice(0, 10))}

JSON: {"alerts":[{"entity_id":"...","risk_level":"high|medium","reason":"...","action":"mensaje o acción concreta"}]}`
    );

    const parsed = JSON.parse(analysis.replace(/```json|```/g, "").trim());
    res.json(parsed);
  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  WEBHOOK — WhatsApp entrante activa agentes automáticamente
// ══════════════════════════════════════════════════════════
app.post("/webhook/twilio", express.urlencoded({ extended: false }), async (req, res) => {
  const { From, Body, ProfileName } = req.body;
  const entityId = From?.replace('whatsapp:+57', '').replace('whatsapp:', '') || 'unknown';

  console.log(`[WEBHOOK] ${ProfileName || From}: ${Body}`);

  try {
    // 1. Leer memoria del contacto
    let memory = {};
    if (db && memoryMode === "postgresql") {
      const r = await db.query("SELECT data FROM memory WHERE entity_id = $1", [entityId]);
      memory = r.rows[0]?.data || {};
    } else {
      memory = memoryStore[entityId] || {};
    }

    // 2. Claude decide qué agente actúa
    let agent = "sdr";
    try {
      const decision = await claudeChat(
        `Eres el router de PrismDB. Responde SOLO con JSON válido, sin texto adicional:\n{"agent":"sdr","reason":"..."}\nOpciones: sdr=prospección, revenue=ventas/compra, talent=empleo, none=irrelevante.`,
        `Mensaje: "${Body}"\nHistorial: ${JSON.stringify(memory)}`
      );
      const cleaned = (decision || "").replace(/\`\`\`json|\`\`\`/g, "").trim();
      const match = cleaned.match(/\{[^}]+\}/);
      if (match) {
        const parsed = JSON.parse(match[0]);
        agent = parsed.agent || "sdr";
      }
    } catch (parseErr) {
      console.log("[WEBHOOK] parse fallback → agente sdr");
    }

    // 3. Agente genera respuesta
    if (agent !== "none") {
      const agentResponse = await claudeChat(
        `Agente ${agent.toUpperCase()} de PrismDB. Responde por WhatsApp. Máx 160 chars. Directo y personalizado.`,
        `Mensaje: "${Body}"\nHistorial: ${JSON.stringify(memory)}\nDevuelve SOLO el mensaje a enviar, sin JSON.`
      );

      // 4. Enviar respuesta por WhatsApp
      if (agentResponse.trim()) {
        await sendWhatsApp(From, agentResponse.trim()).catch(console.error);
      }

      // 5. Guardar en memoria
      const newMemory = {
        ...memory,
        last_message: Body,
        last_agent: agent,
        last_contact: new Date().toISOString(),
        name: ProfileName || memory.name,
      };

      if (db && memoryMode === "postgresql") {
        await db.query(`
          INSERT INTO memory (entity_id, entity_type, data)
          VALUES ($1, 'contact', $2)
          ON CONFLICT (entity_id) DO UPDATE SET data = $2, updated_at = NOW()
        `, [entityId, newMemory]).catch(console.error);
      } else {
        memoryStore[entityId] = newMemory;
      }

      await logEvent("whatsapp_handled", entityId, { message: Body, agent, response: agentResponse });
    }
  } catch (e) {
    console.error("[WEBHOOK ERROR]", e.message);
  }

  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');
});

// ══════════════════════════════════════════════════════════
//  RUTAS ORIGINALES v1 (mantenidas para compatibilidad)
// ══════════════════════════════════════════════════════════

// Prospección de leads
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
    const scoredLeads = await Promise.all(results.map(async (item) => {
      const prompt = `Analiza este perfil. SOLO JSON: {"nombre":"...","cargo":"...","empresa":"...","telefono":null,"email":null,"ciudad":"...","score":85,"razon":"..."}
Perfil: ${item.markdown?.slice(0, 800) || item.description || item.title}
Criterios: cargo=${cargo}, sector=${sector}, ciudad=${ciudad}`;
      try {
        const text = await claudeChat("", prompt, "claude-haiku-4-5-20251001", 300);
        const lead = JSON.parse(text.replace(/```json|```/g, "").trim() || "{}");
        return { ...lead, url: item.url, source: "firecrawl" };
      } catch { return { nombre: item.title, url: item.url, score: 50 }; }
    }));
    res.json({ leads: scoredLeads.filter(l => (l.score || 0) >= scoreMin), total: scoredLeads.length });
  } catch (err) { next(err); }
});

// Mensajería
app.post("/messages/send", async (req, res, next) => {
  try {
    const { phone, message, leadName = "" } = req.body;
    if (!phone || !message) return res.status(400).json({ error: "phone y message requeridos" });
    const data = await sendWhatsApp(phone, message);
    if (data.error_code) throw new Error(data.message);
    res.json({ ok: true, messageId: data.sid, lead: leadName });
  } catch (err) { next(err); }
});

app.post("/messages/test", async (req, res, next) => {
  try {
    const { phone, message = "¡Hola desde PrismDB! 🚀 Tu Revenue OS está funcionando." } = req.body;
    if (!phone) return res.status(400).json({ error: "phone requerido" });
    const data = await sendWhatsApp(phone, message);
    if (data.error_code) throw new Error(`Twilio error ${data.error_code}: ${data.message}`);
    res.json({ ok: true, messageId: data.sid, to: data.to, status: data.status });
  } catch (err) { next(err); }
});

// AI messages
app.post("/ai/message", async (req, res, next) => {
  try {
    const { lead, prompt, businessContext = "" } = req.body;
    const msg = await claudeChat(
      `Experto en ventas B2B LATAM. Mensajes WhatsApp max 160 chars. Contexto: ${businessContext}`,
      prompt || `Genera mensaje para: ${lead.nombre}, ${lead.cargo} en ${lead.empresa} (${lead.ciudad}).`,
      "claude-sonnet-4-20250514", 400
    );
    res.json({ message: msg, lead });
  } catch (err) { next(err); }
});

// CRM Pipeline (in-memory, compatible con v1)
const crmPipeline = { contacto: [], calificado: [], negociacion: [], cerrado: [] };
app.get("/crm/pipeline", (_req, res) => res.json(crmPipeline));
app.post("/crm/lead", (req, res) => {
  const lead = { ...req.body, id: Date.now().toString(), created_at: new Date().toISOString() };
  (crmPipeline[lead.stage || "contacto"] ||= []).push(lead);
  res.status(201).json(lead);
});
app.patch("/crm/lead/:id/move", (req, res) => {
  const { id } = req.params; const { to } = req.body;
  for (const stage of Object.keys(crmPipeline)) {
    const idx = crmPipeline[stage].findIndex(l => l.id === id);
    if (idx !== -1) {
      const [lead] = crmPipeline[stage].splice(idx, 1);
      (crmPipeline[to] ||= []).push({ ...lead, stage: to });
      return res.json({ ok: true, lead: { ...lead, stage: to } });
    }
  }
  res.status(404).json({ error: "Lead no encontrado" });
});

// Talent
app.post("/talent/search", async (req, res, next) => {
  try {
    const { cargo, experiencia = "2+", skills = [], ubicacion = "Colombia", scoreMin = 70, limit = 10 } = req.body;
    const skillsStr = Array.isArray(skills) ? skills.join(", ") : skills;
    const allResults = [];
    for (const query of [`site:linkedin.com/in "${cargo}" "${ubicacion}" open to work`, `site:computrabajo.com.co "${cargo}" ${skillsStr}`]) {
      try {
        const data = await (await fetch("https://api.firecrawl.dev/v1/search", {
          method: "POST", headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}` },
          body: JSON.stringify({ query, limit: Math.ceil(limit / 2), lang: "es", country: "co", scrapeOptions: { formats: ["markdown"] } }),
        })).json();
        if (data.data) allResults.push(...data.data);
      } catch (e) { console.error("[TALENT SEARCH]", e.message); }
    }
    const scored = await Promise.all(allResults.slice(0, limit).map(async (item) => {
      const text = await claudeChat("", `Analiza candidato. SOLO JSON:
{"nombre":"...","cargo_actual":"...","empresa_actual":"...","ubicacion":"...","telefono":null,"email":null,"anos_experiencia":5,"skills_detectados":[],"score":85,"resumen":"..."}
Cargo: ${cargo} | Skills: ${skillsStr} | Loc: ${ubicacion}
Perfil: ${item.markdown?.slice(0, 1000) || item.description || item.title || ""}`,
        "claude-haiku-4-5-20251001", 400);
      try {
        const candidate = JSON.parse(text.replace(/```json|```/g, "").trim() || "{}");
        return { ...candidate, url: item.url };
      } catch { return { nombre: item.title, url: item.url, score: 50 }; }
    }));
    res.json({ candidates: scored.filter(c => (c.score || 0) >= scoreMin), total: scored.length });
  } catch (err) { next(err); }
});

// MercadoPago
const MP_ACCESS_TOKEN = process.env.MP_ACCESS_TOKEN || 'TEST-748633123758950-051218-c76f920ae7147ecd648d1d9225667eba-503051039';
const PLANES_MP = {
  starter: { nombre: 'PrismDB Starter', precio: 249 },
  business: { nombre: 'PrismDB Business', precio: 599 },
  aifirst: { nombre: 'PrismDB AI-First OS', precio: 1499 },
  enterprise: { nombre: 'PrismDB Enterprise', precio: 3000 },
};

app.post("/payment/preference", async (req, res, next) => {
  try {
    const { plan = 'starter', email = 'cliente@prismdb.co', amount, description } = req.body;
    const p = PLANES_MP[plan] || PLANES_MP.starter;
    const FRONTEND = process.env.FRONTEND_URL || 'https://prismdb-chi.vercel.app';
    const data = await (await fetch('https://api.mercadopago.com/checkout/preferences', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` },
      body: JSON.stringify({
        items: [{ id: plan, title: description || p.nombre, quantity: 1, unit_price: amount || p.precio, currency_id: 'COP' }],
        payer: { email },
        back_urls: { success: `${FRONTEND}?payment=success&plan=${plan}`, failure: `${FRONTEND}?payment=failure`, pending: `${FRONTEND}?payment=pending` },
        auto_return: 'approved',
        statement_descriptor: 'PRISMDB',
      }),
    })).json();
    if (data.error) throw new Error(data.message || data.error);
    res.json({ preference_id: data.id, init_point: data.init_point, sandbox_url: data.sandbox_init_point });
  } catch (err) { next(err); }
});

app.post("/payment/webhook", async (req, res) => {
  try {
    const { type, data } = req.body;
    if (type === 'payment' && data?.id) {
      const payment = await (await fetch(`https://api.mercadopago.com/v1/payments/${data.id}`, { headers: { 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` } })).json();
      console.log(`[PAYMENT] ${payment.status} — ${payment.payer?.email} — $${payment.transaction_amount}`);
      await logEvent("payment", payment.payer?.email, { status: payment.status, amount: payment.transaction_amount });
    }
    res.sendStatus(200);
  } catch { res.sendStatus(200); }
});

// Catálogo
app.post("/catalog/extract", async (req, res, next) => {
  try {
    const { url } = req.body;
    if (!url) return res.status(400).json({ error: "url requerida" });
    const fcData = await (await fetch("https://api.firecrawl.dev/v1/scrape", {
      method: "POST", headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}` },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    })).json();
    const content = fcData.data?.markdown || "";
    const text = await claudeChat("", `Extrae producto. SOLO JSON: {"nombre":"...","precio":null,"descripcion":"...","imagen_url":null}
Contenido: ${content.slice(0, 2000)}`, "claude-haiku-4-5-20251001", 300);
    const product = JSON.parse(text.replace(/```json|```/g, "").trim());
    res.json({ ...product, url, fuente: new URL(url).hostname });
  } catch (err) { next(err); }
});


// ══════════════════════════════════════════════════════════
//  AI ROUTER — RUTAS HTTP
// ══════════════════════════════════════════════════════════

// POST /router/event — procesar cualquier evento manualmente
app.post("/router/event", async (req, res, next) => {
  try {
    const event = req.body;
    if (!event.type) return res.status(400).json({ error: "event.type requerido" });
    if (!event.entityId) return res.status(400).json({ error: "event.entityId requerido" });
    const result = await processEvent(event, db);
    res.json(result);
  } catch (err) { next(err); }
});

// POST /router/classify — solo clasificar, sin ejecutar
app.post("/router/classify", async (req, res, next) => {
  try {
    const { event, memory = {} } = req.body;
    if (!event) return res.status(400).json({ error: "event requerido" });
    const classification = await classifyEvent(event, memory);
    const autonomy = autonomyDecision(classification);
    res.json({ classification, autonomy, event_types: Object.values(EVENT_TYPES) });
  } catch (err) { next(err); }
});

// GET /router/events — historial de eventos procesados por el router
app.get("/router/events", async (_req, res, next) => {
  try {
    if (db && memoryMode === "postgresql") {
      const result = await db.query(`
        SELECT * FROM events
        WHERE type = 'router.processed'
        ORDER BY created_at DESC
        LIMIT 50
      `);
      return res.json({ events: result.rows, total: result.rowCount });
    }
    res.json({ events: eventsStore.filter(e => e.type === "router.processed").slice(-50), mode: "in-memory" });
  } catch (err) { next(err); }
});

// GET /router/event-types — todos los tipos de eventos disponibles
app.get("/router/event-types", (_req, res) => {
  res.json({
    event_types: EVENT_TYPES,
    total: Object.keys(EVENT_TYPES).length,
    categories: {
      whatsapp: ["whatsapp.inbound", "whatsapp.no_reply"],
      sales: ["lead.new", "lead.qualified", "deal.stalled", "deal.at_risk", "deal.closed"],
      payments: ["payment.success", "payment.failed", "payment.overdue"],
      campaigns: ["campaign.trigger", "client.inactive", "catalog.request"],
      talent: ["candidate.replied", "candidate.ghosted", "vacancy.opened"],
      finance: ["revenue.alert", "forecast.ready"],
      system: ["system.scheduled", "system.manual"],
    },
  });
});

// POST /router/test — probar el router con un evento simulado
app.post("/router/test", async (req, res, next) => {
  try {
    const { event_type, entity_id = "test_001", extra = {} } = req.body;
    const testEvent = {
      type: event_type || EVENT_TYPES.WHATSAPP_INBOUND,
      entityId: entity_id,
      entityType: "contact",
      message: extra.message || "Hola, me interesa saber más",
      phone: extra.phone,
      amount: extra.amount,
      data: extra,
      triggeredBy: "manual_test",
    };
    const result = await processEvent(testEvent, db);
    res.json({ ok: true, test: true, result });
  } catch (err) { next(err); }
});

//  PrismDB — Módulo Ventas Activas
//  Carga de bases · Catálogos · Campañas · Clientes inactivos
//
//  AGREGAR AL FINAL DE prismdb-backend.js
//  (antes del error handler)
// ═══════════════════════════════════════════════════════════

// ══════════════════════════════════════════════════════════
//  HELPERS INTERNOS
// ══════════════════════════════════════════════════════════

// Parsear CSV simple (sin librerías)
function parseCSV(text) {
  const lines = text.trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, '').toLowerCase());
  return lines.slice(1).map(line => {
    const vals = line.split(',').map(v => v.trim().replace(/"/g, ''));
    const obj = {};
    headers.forEach((h, i) => obj[h] = vals[i] || '');
    return obj;
  }).filter(r => Object.values(r).some(v => v));
}

// Normalizar campos de contacto (acepta columnas con distintos nombres)
function normalizeContact(raw) {
  const find = (...keys) => {
    for (const k of keys) {
      if (raw[k] !== undefined && raw[k] !== '') return raw[k];
    }
    return '';
  };
  return {
    nombre:   find('nombre', 'name', 'cliente', 'razon_social', 'empresa'),
    telefono: find('telefono', 'phone', 'celular', 'movil', 'whatsapp', 'tel'),
    email:    find('email', 'correo', 'mail'),
    empresa:  find('empresa', 'company', 'negocio', 'razon_social'),
    ciudad:   find('ciudad', 'city', 'municipio'),
    segmento: find('segmento', 'segment', 'categoria', 'tipo', 'grupo'),
    notas:    find('notas', 'notes', 'observaciones', 'comentarios'),
    ...raw,
  };
}

// In-memory store para bases de datos cargadas (fallback si no hay DB)
const activaStore = {
  bases: {},       // id → { nombre, contactos[], created_at }
  catalogos: {},   // id → { nombre, productos[], created_at }
  campanas: {},    // id → { config, resultados[], created_at }
};

// ══════════════════════════════════════════════════════════
//  1. BASES DE DATOS
// ══════════════════════════════════════════════════════════

// POST /activa/base/upload — subir base de datos (CSV text o JSON array)
app.post("/activa/base/upload", async (req, res, next) => {
  try {
    const { nombre = "Base sin nombre", formato = "json", data, csv_text } = req.body;

    let contactos = [];

    if (formato === "csv" && csv_text) {
      const raw = parseCSV(csv_text);
      contactos = raw.map(normalizeContact);
    } else if (Array.isArray(data)) {
      contactos = data.map(normalizeContact);
    } else {
      return res.status(400).json({ error: "Envía 'data' (array JSON) o 'csv_text' con formato:'csv'" });
    }

    if (!contactos.length) return res.status(400).json({ error: "No se encontraron contactos válidos" });

    const baseId = `base_${Date.now()}`;
    const base = {
      id: baseId,
      nombre,
      total: contactos.length,
      contactos,
      con_telefono: contactos.filter(c => c.telefono).length,
      con_email: contactos.filter(c => c.email).length,
      segmentos: [...new Set(contactos.map(c => c.segmento).filter(Boolean))],
      created_at: new Date().toISOString(),
    };

    // Guardar en PostgreSQL si disponible
    if (db && memoryMode === "postgresql") {
      for (const contacto of contactos) {
        const entityId = contacto.telefono || contacto.email || `${baseId}_${Math.random().toString(36).slice(2,8)}`;
        await db.query(`
          INSERT INTO memory (entity_id, entity_type, data)
          VALUES ($1, 'activa_contact', $2)
          ON CONFLICT (entity_id) DO UPDATE
            SET data = memory.data || $2, updated_at = NOW()
        `, [entityId, { ...contacto, base_id: baseId, base_nombre: nombre, loaded_at: new Date().toISOString() }])
        .catch(() => {});
      }
    }

    activaStore.bases[baseId] = base;

    // Análisis IA de la base
    const analisis = await claudeChat(
      "Analista de bases de datos comerciales. Responde en JSON.",
      `Analiza esta base de contactos y da recomendaciones. SOLO JSON:
{"calidad":"alta|media|baja","completitud":85,"segmentos_detectados":[],"recomendacion_campana":"...","mensaje_tipo":"...","mejor_horario":"..."}
Muestra (primeros 5): ${JSON.stringify(contactos.slice(0,5))}
Total: ${contactos.length} | Con teléfono: ${base.con_telefono} | Segmentos: ${base.segmentos.join(', ')||'sin segmentar'}`,
      "claude-haiku-4-5-20251001", 400
    ).catch(() => "{}");

    let analisisObj = {};
    try { analisisObj = JSON.parse(analisis.replace(/```json|```/g, "").trim()); } catch {}

    res.status(201).json({ ok: true, base_id: baseId, ...base, analisis: analisisObj });
  } catch (err) { next(err); }
});

// GET /activa/base — listar todas las bases
app.get("/activa/base", async (_req, res, next) => {
  try {
    const bases = Object.values(activaStore.bases).map(b => ({
      id: b.id, nombre: b.nombre, total: b.total,
      con_telefono: b.con_telefono, segmentos: b.segmentos, created_at: b.created_at,
    }));
    res.json({ bases, total: bases.length });
  } catch (err) { next(err); }
});

// GET /activa/base/:id — detalle de una base
app.get("/activa/base/:id", (req, res) => {
  const base = activaStore.bases[req.params.id];
  if (!base) return res.status(404).json({ error: "Base no encontrada" });
  res.json(base);
});

// ══════════════════════════════════════════════════════════
//  2. CATÁLOGOS
// ══════════════════════════════════════════════════════════

// POST /activa/catalogo — crear catálogo de productos
app.post("/activa/catalogo", async (req, res, next) => {
  try {
    const { nombre = "Catálogo", productos = [], descripcion_empresa = "" } = req.body;

    if (!productos.length) return res.status(400).json({ error: "productos[] requerido" });

    const catalogoId = `cat_${Date.now()}`;

    // Claude genera descripción optimizada para WhatsApp de cada producto
    const productosOptimizados = await Promise.all(
      productos.slice(0, 20).map(async (p) => {
        const msg = await claudeChat(
          "Experto en ventas por WhatsApp. Crea descripciones cortas, atractivas y con emoji. Máx 120 chars.",
          `Producto: ${p.nombre} | Precio: ${p.precio || 'consultar'} | Descripción: ${p.descripcion || ''}
Contexto empresa: ${descripcion_empresa}
Devuelve SOLO el mensaje WhatsApp optimizado.`
        ).catch(() => `${p.nombre} - $${p.precio || 'Consultar'}`);
        return { ...p, mensaje_wa: msg.trim() };
      })
    );

    const catalogo = {
      id: catalogoId,
      nombre,
      descripcion_empresa,
      productos: productosOptimizados,
      total_productos: productosOptimizados.length,
      created_at: new Date().toISOString(),
    };

    activaStore.catalogos[catalogoId] = catalogo;
    res.status(201).json({ ok: true, catalogo_id: catalogoId, ...catalogo });
  } catch (err) { next(err); }
});

// GET /activa/catalogo — listar catálogos
app.get("/activa/catalogo", (_req, res) => {
  const catalogos = Object.values(activaStore.catalogos).map(c => ({
    id: c.id, nombre: c.nombre, total_productos: c.total_productos, created_at: c.created_at,
  }));
  res.json({ catalogos, total: catalogos.length });
});

// POST /activa/catalogo/extract — extraer catálogo desde URL (Instagram, web, etc.)
app.post("/activa/catalogo/extract", async (req, res, next) => {
  try {
    const { url, nombre = "Catálogo extraído" } = req.body;
    if (!url) return res.status(400).json({ error: "url requerida" });

    const fcData = await (await fetch("https://api.firecrawl.dev/v1/scrape", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}` },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    })).json();

    const content = fcData.data?.markdown || fcData.markdown || "";

    const extraction = await claudeChat(
      "Extrae productos de contenido web/redes sociales. SOLO JSON.",
      `Extrae todos los productos que encuentres. JSON:
{"productos":[{"nombre":"...","precio":"...","descripcion":"...","disponible":true}]}
Contenido: ${content.slice(0, 3000)}`
    );

    let productos = [];
    try {
      const parsed = JSON.parse(extraction.replace(/```json|```/g, "").trim());
      productos = parsed.productos || [];
    } catch {}

    const catalogoId = `cat_${Date.now()}`;
    activaStore.catalogos[catalogoId] = {
      id: catalogoId, nombre, productos, total_productos: productos.length,
      fuente: url, created_at: new Date().toISOString(),
    };

    res.json({ ok: true, catalogo_id: catalogoId, productos, total: productos.length, fuente: url });
  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  3. CAMPAÑAS
// ══════════════════════════════════════════════════════════

// POST /activa/campana/preview — previsualizar mensajes antes de enviar
app.post("/activa/campana/preview", async (req, res, next) => {
  try {
    const {
      base_id, catalogo_id, tipo = "catalogo",
      template, segmento, limit = 5,
      promocion, contexto_empresa = ""
    } = req.body;

    if (!base_id) return res.status(400).json({ error: "base_id requerido" });

    const base = activaStore.bases[base_id];
    if (!base) return res.status(404).json({ error: "Base no encontrada" });

    let contactos = base.contactos;
    if (segmento) contactos = contactos.filter(c => c.segmento === segmento);
    const muestra = contactos.slice(0, limit);

    const catalogo = catalogo_id ? activaStore.catalogos[catalogo_id] : null;

    const previews = await Promise.all(muestra.map(async (contacto) => {
      let mensaje;

      if (template) {
        // Template con variables
        mensaje = template
          .replace(/\{\{nombre\}\}/gi, contacto.nombre || "estimado cliente")
          .replace(/\{\{empresa\}\}/gi, contacto.empresa || "")
          .replace(/\{\{ciudad\}\}/gi, contacto.ciudad || "")
          .replace(/\{\{segmento\}\}/gi, contacto.segmento || "");
      } else if (tipo === "catalogo" && catalogo) {
        const productos_texto = catalogo.productos.slice(0, 3)
          .map(p => p.mensaje_wa || `• ${p.nombre} - $${p.precio}`).join('\n');
        mensaje = await claudeChat(
          "Ventas por WhatsApp LATAM. Mensaje personalizado, cálido, máx 200 chars con emoji.",
          `Cliente: ${contacto.nombre} | Empresa: ${contacto.empresa} | Ciudad: ${contacto.ciudad}
Catálogo: ${catalogo.nombre}
Productos destacados:\n${productos_texto}
Contexto: ${contexto_empresa}
Genera el mensaje personalizado para este cliente.`
        );
      } else if (tipo === "promocion" && promocion) {
        mensaje = await claudeChat(
          "Ventas por WhatsApp LATAM. Mensaje de promoción personalizado, máx 160 chars, con urgencia.",
          `Cliente: ${contacto.nombre} | Empresa: ${contacto.empresa}
Promoción: ${promocion}
Contexto: ${contexto_empresa}
Genera el mensaje.`
        );
      } else {
        mensaje = `Hola ${contacto.nombre}, tenemos novedades para ti. ¿Te interesa conocerlas?`;
      }

      return { contacto: { nombre: contacto.nombre, telefono: contacto.telefono, empresa: contacto.empresa }, mensaje: mensaje.trim() };
    }));

    res.json({
      ok: true, tipo, base_id, total_en_base: contactos.length,
      segmento: segmento || "todos", previews,
      estimado_envio: contactos.filter(c => c.telefono).length,
    });
  } catch (err) { next(err); }
});

// POST /activa/campana/send — enviar campaña
app.post("/activa/campana/send", async (req, res, next) => {
  try {
    const {
      base_id, catalogo_id, tipo = "catalogo",
      template, segmento, limite_envios = 50,
      promocion, contexto_empresa = "",
      delay_ms = 1500, // delay entre mensajes para evitar spam
    } = req.body;

    if (!base_id) return res.status(400).json({ error: "base_id requerido" });

    const base = activaStore.bases[base_id];
    if (!base) return res.status(404).json({ error: "Base no encontrada" });

    let contactos = base.contactos.filter(c => c.telefono);
    if (segmento) contactos = contactos.filter(c => c.segmento === segmento);
    contactos = contactos.slice(0, limite_envios);

    if (!contactos.length) return res.status(400).json({ error: "No hay contactos con teléfono en esta base/segmento" });

    const campanaId = `camp_${Date.now()}`;
    const catalogo = catalogo_id ? activaStore.catalogos[catalogo_id] : null;

    // Enviar en background
    const resultados = [];
    let enviados = 0, fallidos = 0;

    // Respuesta inmediata con el ID de campaña
    res.json({
      ok: true, campana_id: campanaId,
      total_a_enviar: contactos.length,
      status: "sending",
      message: `Campaña iniciada. Enviando ${contactos.length} mensajes con delay de ${delay_ms}ms entre cada uno.`,
    });

    // Proceso de envío en background
    (async () => {
      for (const contacto of contactos) {
        try {
          let mensaje;

          if (template) {
            mensaje = template
              .replace(/\{\{nombre\}\}/gi, contacto.nombre || "estimado cliente")
              .replace(/\{\{empresa\}\}/gi, contacto.empresa || "")
              .replace(/\{\{ciudad\}\}/gi, contacto.ciudad || "")
              .replace(/\{\{segmento\}\}/gi, contacto.segmento || "");
          } else if (tipo === "catalogo" && catalogo) {
            const productos_texto = catalogo.productos.slice(0, 3)
              .map(p => p.mensaje_wa || `• ${p.nombre} - $${p.precio}`).join('\n');
            mensaje = await claudeChat(
              "Ventas WhatsApp. Mensaje corto personalizado máx 200 chars.",
              `Cliente: ${contacto.nombre} | Catálogo: ${catalogo.nombre}\nProductos: ${productos_texto}\nContexto: ${contexto_empresa}\nGenera mensaje.`
            );
          } else if (tipo === "promocion") {
            mensaje = await claudeChat(
              "Ventas WhatsApp. Mensaje de promoción personalizado máx 160 chars.",
              `Cliente: ${contacto.nombre} | Empresa: ${contacto.empresa}\nPromoción: ${promocion}\nGenera mensaje.`
            );
          } else {
            mensaje = `Hola ${contacto.nombre || ""}! Tenemos novedades. ¿Te cuento? 🚀`;
          }

          // Enviar WhatsApp
          const data = await sendWhatsApp(contacto.telefono, mensaje.trim());
          const ok = !data.error_code;

          resultados.push({ telefono: contacto.telefono, nombre: contacto.nombre, ok, sid: data.sid, mensaje: mensaje.trim() });
          if (ok) enviados++; else fallidos++;

          // Guardar en memoria
          if (db && memoryMode === "postgresql" && contacto.telefono) {
            await db.query(`
              INSERT INTO memory (entity_id, entity_type, data)
              VALUES ($1, 'activa_contact', $2)
              ON CONFLICT (entity_id) DO UPDATE SET data = memory.data || $2, updated_at = NOW()
            `, [contacto.telefono, {
              nombre: contacto.nombre, last_campana: campanaId,
              last_mensaje_enviado: mensaje.trim(), last_contact: new Date().toISOString(),
            }]).catch(() => {});
          }

          await new Promise(r => setTimeout(r, delay_ms));
        } catch (e) {
          resultados.push({ telefono: contacto.telefono, nombre: contacto.nombre, ok: false, error: e.message });
          fallidos++;
        }
      }

      activaStore.campanas[campanaId] = {
        id: campanaId, base_id, tipo, enviados, fallidos,
        total: contactos.length, resultados,
        completed_at: new Date().toISOString(),
      };

      console.log(`[CAMPAÑA ${campanaId}] Completada: ${enviados} enviados, ${fallidos} fallidos`);
    })();

  } catch (err) { next(err); }
});

// GET /activa/campana/:id — resultado de una campaña
app.get("/activa/campana/:id", (req, res) => {
  const campana = activaStore.campanas[req.params.id];
  if (!campana) return res.json({ status: "sending", message: "Campaña en proceso..." });
  res.json({ status: "completed", ...campana });
});

// GET /activa/campana — listar campañas
app.get("/activa/campana", (_req, res) => {
  const campanas = Object.values(activaStore.campanas).map(c => ({
    id: c.id, base_id: c.base_id, tipo: c.tipo,
    enviados: c.enviados, fallidos: c.fallidos,
    total: c.total, completed_at: c.completed_at,
  }));
  res.json({ campanas, total: campanas.length });
});

// ══════════════════════════════════════════════════════════
//  4. CLIENTES INACTIVOS
// ══════════════════════════════════════════════════════════

// GET /activa/inactivos — detectar clientes sin contacto reciente
app.get("/activa/inactivos", async (req, res, next) => {
  try {
    const { dias = 14 } = req.query;

    if (db && memoryMode === "postgresql") {
      const result = await db.query(`
        SELECT entity_id, data, updated_at
        FROM memory
        WHERE entity_type IN ('contact', 'activa_contact')
          AND updated_at < NOW() - INTERVAL '${parseInt(dias)} days'
        ORDER BY updated_at ASC
        LIMIT 50
      `);
      return res.json({
        inactivos: result.rows.map(r => ({
          id: r.entity_id,
          nombre: r.data?.nombre,
          telefono: r.data?.telefono || r.entity_id,
          empresa: r.data?.empresa,
          ultimo_contacto: r.updated_at,
          dias_inactivo: Math.floor((Date.now() - new Date(r.updated_at)) / 86400000),
        })),
        total: result.rowCount,
        dias_umbral: parseInt(dias),
      });
    }

    res.json({ inactivos: [], total: 0, dias_umbral: parseInt(dias), mode: "in-memory" });
  } catch (err) { next(err); }
});

// POST /activa/inactivos/reactivar — enviar campaña de reactivación
app.post("/activa/inactivos/reactivar", async (req, res, next) => {
  try {
    const { dias = 14, mensaje_template, limite = 20, contexto_empresa = "" } = req.body;

    if (!db || memoryMode !== "postgresql") {
      return res.status(400).json({ error: "Requiere PostgreSQL activo" });
    }

    const result = await db.query(`
      SELECT entity_id, data FROM memory
      WHERE entity_type IN ('contact', 'activa_contact')
        AND updated_at < NOW() - INTERVAL '${parseInt(dias)} days'
        AND (data->>'reactivacion_enviada') IS DISTINCT FROM 'true'
      LIMIT $1
    `, [limite]);

    if (!result.rows.length) {
      return res.json({ ok: true, message: "No hay clientes inactivos para reactivar", enviados: 0 });
    }

    const resultados = [];
    res.json({ ok: true, total: result.rows.length, status: "sending", message: `Reactivando ${result.rows.length} clientes...` });

    (async () => {
      for (const row of result.rows) {
        const contacto = row.data || {};
        const telefono = contacto.telefono || row.entity_id;
        if (!telefono || telefono.startsWith('base_')) continue;

        try {
          const mensaje = mensaje_template
            ? mensaje_template.replace(/\{\{nombre\}\}/gi, contacto.nombre || "estimado cliente")
            : await claudeChat(
                "Ventas WhatsApp. Mensaje de reactivación cálido, sin ser invasivo. Máx 140 chars.",
                `Cliente: ${contacto.nombre || "cliente"} | Empresa: ${contacto.empresa || ""}
Lleva ${Math.floor((Date.now() - new Date(row.updated_at))/86400000)} días sin contacto.
Contexto: ${contexto_empresa}
Genera mensaje de reactivación.`
              );

          await sendWhatsApp(telefono, mensaje.trim());
          await db.query(
            `UPDATE memory SET data = data || '{"reactivacion_enviada":"true","reactivacion_fecha":"${new Date().toISOString()}"}', updated_at = NOW() WHERE entity_id = $1`,
            [row.entity_id]
          ).catch(() => {});

          resultados.push({ id: row.entity_id, nombre: contacto.nombre, ok: true });
          await new Promise(r => setTimeout(r, 1500));
        } catch (e) {
          resultados.push({ id: row.entity_id, nombre: contacto.nombre, ok: false, error: e.message });
        }
      }
      console.log(`[REACTIVACIÓN] ${resultados.filter(r=>r.ok).length}/${resultados.length} enviados`);
    })();

  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  5. SEGMENTACIÓN IA
// ══════════════════════════════════════════════════════════

// POST /activa/segmentar — Claude segmenta una base automáticamente
app.post("/activa/segmentar", async (req, res, next) => {
  try {
    const { base_id, criterios = "" } = req.body;
    if (!base_id) return res.status(400).json({ error: "base_id requerido" });

    const base = activaStore.bases[base_id];
    if (!base) return res.status(404).json({ error: "Base no encontrada" });

    const muestra = base.contactos.slice(0, 30);

    const segmentacion = await claudeChat(
      "Analista de datos comerciales. Segmenta contactos para campañas. SOLO JSON.",
      `Analiza estos ${base.total} contactos y propón segmentos para campañas.
Muestra: ${JSON.stringify(muestra)}
Criterios adicionales: ${criterios}

JSON: {"segmentos":[{"nombre":"...","descripcion":"...","criterio":"...","tamaño_estimado":0,"prioridad":"alta|media|baja","mensaje_recomendado":"..."}]}`,
      "claude-sonnet-4-20250514", 800
    );

    let segmentos = [];
    try {
      const parsed = JSON.parse(segmentacion.replace(/```json|```/g, "").trim());
      segmentos = parsed.segmentos || [];
    } catch {}

    res.json({ ok: true, base_id, total_contactos: base.total, segmentos });
  } catch (err) { next(err); }
});
// ═══════════════════════════════════════════════════════════
//  PrismDB — Enterprise Graph
//  Relaciones entre entidades del sistema operativo
//
//  AGREGAR AL FINAL DE prismdb-backend.js
//  (antes del error handler)
//
//  Entidades: cliente, vendedor, factura, conversacion,
//             campana, pedido, pago, candidato, vacante,
//             entrevista, deal
//
//  Relaciones: COMPRÓ, ASIGNADO_A, GENERÓ, PARTICIPÓ_EN,
//              RECIBIÓ, APLICÓ_A, ENTREVISTADO_POR, etc.
// ═══════════════════════════════════════════════════════════

// ── SCHEMA DEL GRAFO en PostgreSQL ──────────────────────
// Se crean las tablas al iniciar si no existen

async function initGraphTables() {
  if (!db || memoryMode !== "postgresql") return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS graph_nodes (
      id          TEXT PRIMARY KEY,
      type        TEXT NOT NULL,
      label       TEXT NOT NULL,
      properties  JSONB NOT NULL DEFAULT '{}',
      created_at  TIMESTAMPTZ DEFAULT NOW(),
      updated_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS graph_edges (
      id          SERIAL PRIMARY KEY,
      from_id     TEXT NOT NULL REFERENCES graph_nodes(id) ON DELETE CASCADE,
      to_id       TEXT NOT NULL REFERENCES graph_nodes(id) ON DELETE CASCADE,
      relation    TEXT NOT NULL,
      weight      NUMERIC DEFAULT 1,
      properties  JSONB NOT NULL DEFAULT '{}',
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS graph_edges_from ON graph_edges(from_id);
    CREATE INDEX IF NOT EXISTS graph_edges_to   ON graph_edges(to_id);
    CREATE INDEX IF NOT EXISTS graph_nodes_type ON graph_nodes(type);
  `).catch(e => console.error("[GRAPH INIT]", e.message));
  console.log("✅ Enterprise Graph tables ready");
}

// Inicializar tablas al arrancar
setTimeout(initGraphTables, 3000);

// ── TIPOS DE NODOS ───────────────────────────────────────
const NODE_TYPES = {
  cliente:       { color: "#3b82f6", icon: "👤", label: "Cliente"      },
  vendedor:      { color: "#22c55e", icon: "💼", label: "Vendedor"     },
  factura:       { color: "#eab308", icon: "🧾", label: "Factura"      },
  conversacion:  { color: "#a855f7", icon: "💬", label: "Conversación" },
  campana:       { color: "#f97316", icon: "📣", label: "Campaña"      },
  pedido:        { color: "#14b8a6", icon: "📦", label: "Pedido"       },
  pago:          { color: "#22c55e", icon: "💰", label: "Pago"         },
  candidato:     { color: "#6366f1", icon: "🎓", label: "Candidato"    },
  vacante:       { color: "#ec4899", icon: "📋", label: "Vacante"      },
  entrevista:    { color: "#8b5cf6", icon: "🤝", label: "Entrevista"   },
  deal:          { color: "#ef4444", icon: "🎯", label: "Deal"         },
  empresa:       { color: "#64748b", icon: "🏢", label: "Empresa"      },
};

// ── TIPOS DE RELACIONES ──────────────────────────────────
const RELATION_TYPES = {
  COMPRO:           { label: "Compró",          color: "#22c55e" },
  ASIGNADO_A:       { label: "Asignado a",      color: "#3b82f6" },
  GENERO:           { label: "Generó",           color: "#eab308" },
  PARTICIPO_EN:     { label: "Participó en",    color: "#a855f7" },
  RECIBIO:          { label: "Recibió",          color: "#f97316" },
  APLICO_A:         { label: "Aplicó a",        color: "#6366f1" },
  ENTREVISTADO_POR: { label: "Entrevistado por", color: "#8b5cf6" },
  PAGO:             { label: "Pagó",            color: "#22c55e" },
  TIENE_DEAL:       { label: "Tiene deal",      color: "#ef4444" },
  PERTENECE_A:      { label: "Pertenece a",     color: "#64748b" },
  ENVIO_A:          { label: "Envió a",         color: "#f97316" },
  CONTIENE:         { label: "Contiene",        color: "#14b8a6" },
  SELECCIONADO_EN:  { label: "Seleccionado en", color: "#ec4899" },
};

// ── HELPERS ──────────────────────────────────────────────
async function upsertNode(id, type, label, properties = {}) {
  if (!db) return;
  await db.query(`
    INSERT INTO graph_nodes (id, type, label, properties)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (id) DO UPDATE
      SET label = $3, properties = graph_nodes.properties || $4, updated_at = NOW()
  `, [id, type, label, properties]);
}

async function upsertEdge(fromId, toId, relation, properties = {}, weight = 1) {
  if (!db) return;
  // Evitar duplicados de la misma relación entre los mismos nodos
  const existing = await db.query(
    "SELECT id FROM graph_edges WHERE from_id=$1 AND to_id=$2 AND relation=$3",
    [fromId, toId, relation]
  );
  if (existing.rows.length) {
    await db.query(
      "UPDATE graph_edges SET weight = weight + $1, properties = properties || $2 WHERE from_id=$3 AND to_id=$4 AND relation=$5",
      [weight, properties, fromId, toId, relation]
    );
  } else {
    await db.query(
      "INSERT INTO graph_edges (from_id, to_id, relation, weight, properties) VALUES ($1,$2,$3,$4,$5)",
      [fromId, toId, relation, weight, properties]
    );
  }
}

// ══════════════════════════════════════════════════════════
//  RUTAS — NODOS
// ══════════════════════════════════════════════════════════

// POST /graph/node — crear o actualizar nodo
app.post("/graph/node", async (req, res, next) => {
  try {
    const { id, type, label, properties = {} } = req.body;
    if (!id || !type || !label) return res.status(400).json({ error: "id, type y label requeridos" });
    if (!NODE_TYPES[type]) return res.status(400).json({ error: `Tipo inválido. Válidos: ${Object.keys(NODE_TYPES).join(", ")}` });

    await upsertNode(id, type, label, properties);
    res.status(201).json({ ok: true, id, type, label, properties });
  } catch (err) { next(err); }
});

// GET /graph/nodes — todos los nodos (con filtro opcional por tipo)
app.get("/graph/nodes", async (req, res, next) => {
  try {
    const { type, search, limit = 100 } = req.query;
    if (!db || memoryMode !== "postgresql") return res.json({ nodes: [], types: NODE_TYPES });

    let query = "SELECT * FROM graph_nodes WHERE 1=1";
    const params = [];
    if (type) { params.push(type); query += ` AND type = $${params.length}`; }
    if (search) { params.push(`%${search}%`); query += ` AND label ILIKE $${params.length}`; }
    params.push(parseInt(limit));
    query += ` ORDER BY updated_at DESC LIMIT $${params.length}`;

    const result = await db.query(query, params);
    res.json({ nodes: result.rows, total: result.rowCount, types: NODE_TYPES });
  } catch (err) { next(err); }
});

// GET /graph/node/:id — nodo con todas sus relaciones
app.get("/graph/node/:id", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.status(503).json({ error: "Requiere PostgreSQL" });

    const { id } = req.params;
    const [node, outgoing, incoming] = await Promise.all([
      db.query("SELECT * FROM graph_nodes WHERE id = $1", [id]),
      db.query(`
        SELECT e.*, n.type as to_type, n.label as to_label, n.properties as to_properties
        FROM graph_edges e JOIN graph_nodes n ON e.to_id = n.id
        WHERE e.from_id = $1 ORDER BY e.created_at DESC
      `, [id]),
      db.query(`
        SELECT e.*, n.type as from_type, n.label as from_label, n.properties as from_properties
        FROM graph_edges e JOIN graph_nodes n ON e.from_id = n.id
        WHERE e.to_id = $1 ORDER BY e.created_at DESC
      `, [id]),
    ]);

    if (!node.rows.length) return res.status(404).json({ error: "Nodo no encontrado" });

    // Análisis IA del nodo
    const analysis = await claudeChat(
      "Analista de relaciones empresariales. SOLO JSON.",
      `Analiza este nodo y sus relaciones. Devuelve insights accionables.
Nodo: ${JSON.stringify(node.rows[0])}
Relaciones salientes: ${JSON.stringify(outgoing.rows.slice(0,10))}
Relaciones entrantes: ${JSON.stringify(incoming.rows.slice(0,10))}

JSON: {"resumen":"2 oraciones","insights":["insight 1","insight 2"],"riesgo":"alto|medio|bajo|ninguno","accion_recomendada":"...","valor_estimado":0}`
    ).catch(() => "{}");

    let analysisObj = {};
    try { analysisObj = JSON.parse(analysis.replace(/```json|```/g, "").trim()); } catch {}

    res.json({
      node: node.rows[0],
      outgoing: outgoing.rows,
      incoming: incoming.rows,
      total_relations: outgoing.rowCount + incoming.rowCount,
      analysis: analysisObj,
    });
  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  RUTAS — RELACIONES (EDGES)
// ══════════════════════════════════════════════════════════

// POST /graph/edge — crear relación entre nodos
app.post("/graph/edge", async (req, res, next) => {
  try {
    const { from_id, to_id, relation, properties = {}, weight = 1 } = req.body;
    if (!from_id || !to_id || !relation) return res.status(400).json({ error: "from_id, to_id y relation requeridos" });

    // Auto-crear nodos si no existen (con tipo inferido del ID)
    if (db) {
      const fromExists = await db.query("SELECT id FROM graph_nodes WHERE id=$1", [from_id]);
      if (!fromExists.rows.length) await upsertNode(from_id, "cliente", from_id, {});
      const toExists = await db.query("SELECT id FROM graph_nodes WHERE id=$1", [to_id]);
      if (!toExists.rows.length) await upsertNode(to_id, "cliente", to_id, {});
    }

    await upsertEdge(from_id, to_id, relation, properties, weight);
    res.status(201).json({ ok: true, from_id, to_id, relation, weight });
  } catch (err) { next(err); }
});

// GET /graph/edges — todas las relaciones
app.get("/graph/edges", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.json({ edges: [], relations: RELATION_TYPES });
    const { relation, limit = 200 } = req.query;
    let query = "SELECT e.*, fn.label as from_label, fn.type as from_type, tn.label as to_label, tn.type as to_type FROM graph_edges e JOIN graph_nodes fn ON e.from_id=fn.id JOIN graph_nodes tn ON e.to_id=tn.id WHERE 1=1";
    const params = [];
    if (relation) { params.push(relation); query += ` AND e.relation=$${params.length}`; }
    params.push(parseInt(limit));
    query += ` ORDER BY e.created_at DESC LIMIT $${params.length}`;
    const result = await db.query(query, params);
    res.json({ edges: result.rows, total: result.rowCount, relations: RELATION_TYPES });
  } catch (err) { next(err); }
});

// ══════════════════════════════════════════════════════════
//  RUTAS — GRAFO COMPLETO
// ══════════════════════════════════════════════════════════

// GET /graph — grafo completo para visualización
app.get("/graph", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") {
      return res.json({ nodes: [], edges: [], stats: {}, node_types: NODE_TYPES, relation_types: RELATION_TYPES });
    }

    const { limit_nodes = 150, type } = req.query;
    let nodeQuery = "SELECT * FROM graph_nodes";
    const params = [];
    if (type) { params.push(type); nodeQuery += " WHERE type=$1"; }
    params.push(parseInt(limit_nodes));
    nodeQuery += ` ORDER BY updated_at DESC LIMIT $${params.length}`;

    const [nodes, edges, stats] = await Promise.all([
      db.query(nodeQuery, params),
      db.query(`
        SELECT e.*, fn.type as from_type, tn.type as to_type
        FROM graph_edges e
        JOIN graph_nodes fn ON e.from_id=fn.id
        JOIN graph_nodes tn ON e.to_id=tn.id
        ORDER BY e.weight DESC LIMIT 500
      `),
      db.query(`
        SELECT
          (SELECT COUNT(*) FROM graph_nodes) as total_nodes,
          (SELECT COUNT(*) FROM graph_edges) as total_edges,
          (SELECT COUNT(DISTINCT type) FROM graph_nodes) as entity_types,
          (SELECT COUNT(DISTINCT relation) FROM graph_edges) as relation_types
      `),
    ]);

    res.json({
      nodes: nodes.rows,
      edges: edges.rows,
      stats: stats.rows[0],
      node_types: NODE_TYPES,
      relation_types: RELATION_TYPES,
    });
  } catch (err) { next(err); }
});

// POST /graph/search — búsqueda semántica en el grafo con IA
app.post("/graph/search", async (req, res, next) => {
  try {
    const { query, limit = 20 } = req.body;
    if (!query) return res.status(400).json({ error: "query requerido" });
    if (!db || memoryMode !== "postgresql") return res.json({ results: [] });

    // Búsqueda en nodos
    const nodes = await db.query(`
      SELECT n.*, COUNT(e.id) as degree
      FROM graph_nodes n
      LEFT JOIN graph_edges e ON n.id=e.from_id OR n.id=e.to_id
      WHERE n.label ILIKE $1 OR n.properties::text ILIKE $1
      GROUP BY n.id ORDER BY degree DESC LIMIT $2
    `, [`%${query}%`, parseInt(limit)]);

    // IA interpreta la búsqueda
    if (nodes.rows.length) {
      const interpretation = await claudeChat(
        "Analista de grafos empresariales. Interpreta resultados de búsqueda. SOLO JSON.",
        `Query: "${query}"
Resultados encontrados: ${JSON.stringify(nodes.rows.slice(0,5))}
JSON: {"interpretacion":"qué encontró","patron_detectado":"...","sugerencia":"qué explorar siguiente"}`
      ).catch(() => "{}");
      let interp = {};
      try { interp = JSON.parse(interpretation.replace(/```json|```/g, "").trim()); } catch {}
      return res.json({ results: nodes.rows, total: nodes.rowCount, ai_interpretation: interp });
    }

    res.json({ results: [], total: 0, message: "Sin resultados para esa búsqueda" });
  } catch (err) { next(err); }
});

// POST /graph/sync — sincronizar memoria existente al grafo
app.post("/graph/sync", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.status(503).json({ error: "Requiere PostgreSQL" });

    const memory = await db.query("SELECT * FROM memory LIMIT 200");
    let created = 0;

    for (const row of memory.rows) {
      const data = row.data || {};
      const type = row.entity_type === "candidate" ? "candidato"
        : row.entity_type === "deal" ? "deal"
        : row.entity_type === "activa_contact" ? "cliente"
        : "cliente";

      const label = data.nombre || data.name || row.entity_id;

      await upsertNode(row.entity_id, type, label, {
        telefono: data.telefono,
        empresa: data.empresa,
        ciudad: data.ciudad,
        stage: data.stage || data.talent_stage,
        last_agent: data.last_agent,
        revenue: data.expected_revenue,
      });
      created++;

      // Crear relación con vendedor/agente si existe
      if (data.last_agent) {
        await upsertNode(`agent_${data.last_agent}`, "vendedor", `Agente ${data.last_agent.toUpperCase()}`, { type: "ai_agent" });
        await upsertEdge(row.entity_id, `agent_${data.last_agent}`, "ASIGNADO_A", { via: "memory_sync" });
      }

      // Crear relación con empresa si existe
      if (data.empresa) {
        const empresaId = `emp_${data.empresa.toLowerCase().replace(/\s+/g, '_').slice(0,30)}`;
        await upsertNode(empresaId, "empresa", data.empresa, {});
        await upsertEdge(row.entity_id, empresaId, "PERTENECE_A", {});
      }
    }

    res.json({ ok: true, synced: created, message: `${created} entidades sincronizadas al grafo` });
  } catch (err) { next(err); }
});

// GET /graph/stats — estadísticas del grafo
app.get("/graph/stats", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.json({ stats: {} });

    const [counts, topNodes, topRelations] = await Promise.all([
      db.query(`
        SELECT type, COUNT(*) as count FROM graph_nodes GROUP BY type ORDER BY count DESC
      `),
      db.query(`
        SELECT n.id, n.label, n.type, COUNT(e.id) as connections
        FROM graph_nodes n LEFT JOIN graph_edges e ON n.id=e.from_id OR n.id=e.to_id
        GROUP BY n.id ORDER BY connections DESC LIMIT 10
      `),
      db.query(`
        SELECT relation, COUNT(*) as count FROM graph_edges GROUP BY relation ORDER BY count DESC
      `),
    ]);

    res.json({
      node_counts: counts.rows,
      top_connected: topNodes.rows,
      top_relations: topRelations.rows,
    });
  } catch (err) { next(err); }
});

// POST /graph/bulk — cargar múltiples nodos y relaciones a la vez
app.post("/graph/bulk", async (req, res, next) => {
  try {
    const { nodes = [], edges = [] } = req.body;
    let n = 0, e = 0;
    for (const node of nodes) {
      if (node.id && node.type && node.label) {
        await upsertNode(node.id, node.type, node.label, node.properties || {});
        n++;
      }
    }
    for (const edge of edges) {
      if (edge.from_id && edge.to_id && edge.relation) {
        await upsertEdge(edge.from_id, edge.to_id, edge.relation, edge.properties || {}, edge.weight || 1);
        e++;
      }
    }
    res.json({ ok: true, nodes_created: n, edges_created: e });
  } catch (err) { next(err); }
});

// ── Error handler ─────────────────────────────────────────
app.use((err, _req, res, _next) => {
  console.error("[ERROR]", err.message);
  res.status(500).json({ error: err.message || "Error interno" });
});

app.listen(PORT, () => {
  console.log(`✅ PrismDB v2.1 corriendo en http://localhost:${PORT}`);
  console.log(`   Memory: ${memoryMode} | AI Router activo | Event Bus iniciando...`);
  console.log(`   Módulos: SDR · Revenue · Ventas Activas · Talent · Finance · Enterprise Graph`);
});
