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

async function claudeChat(system, userContent, model = "gemini-1.5-flash", maxTokens = 500) {
  try {
    const geminiKey = process.env.GEMINI_API_KEY || process.env.ANTHROPIC_API_KEY;
    const prompt = system ? system + "\n\n" + userContent : userContent;
    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiKey}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: { maxOutputTokens: maxTokens, temperature: 0.7 }
      }),
    });
    const data = await res.json();
    return data.candidates?.[0]?.content?.parts?.[0]?.text || "";
  } catch(e) {
    console.error("[GEMINI ERROR]", e.message);
    return "";
  }
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
      const geminiKey = process.env.GEMINI_API_KEY || process.env.ANTHROPIC_API_KEY;
      const routerRes = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiKey}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ parts: [{ text: "Clasifica este mensaje de WhatsApp para PrismDB. Responde SOLO el JSON: {agent} donde agent es: sdr (interes/prospecto), revenue (compra/precio/plan), talent (empleo), none (irrelevante).\n\nMensaje: " + Body }] }],
          generationConfig: { maxOutputTokens: 100, temperature: 0.1 }
        })
      });
      const routerData = await routerRes.json();
      console.log("[GEMINI RAW]", JSON.stringify(routerData).slice(0, 300));
      const routerText = routerData.candidates?.[0]?.content?.parts?.[0]?.text || routerData.candidates?.[0]?.output || "";
      const match = routerText.match(/"agent"\s*:\s*"(\w+)"/);
      if (match) agent = match[1];
      console.log("[WEBHOOK] Router decision:", agent, "| Text:", routerText.slice(0,60));
    } catch (routerErr) {
      console.log("[WEBHOOK] Router fallback sdr:", routerErr.message);
    }

    // 3. Agente genera respuesta
    if (agent !== "none") {
      let agentResponse = "";
      try {
        const geminiKey2 = process.env.GEMINI_API_KEY || process.env.ANTHROPIC_API_KEY;
        const agentPrompt = "Eres el agente " + agent.toUpperCase() + " de PrismDB, el Sistema Operativo Empresarial para LATAM. Responde mensajes de WhatsApp de forma cálida, directa y personalizada. Máximo 160 caracteres. Solo el mensaje, sin explicaciones.\n\nMensaje recibido: " + Body + "\nContexto: " + JSON.stringify(memory) + "\nGenera el mensaje de respuesta.";
        const agentRes = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiKey2}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            contents: [{ parts: [{ text: agentPrompt }] }],
            generationConfig: { maxOutputTokens: 200, temperature: 0.8 }
          })
        });
        const agentData = await agentRes.json();
        agentResponse = agentData.candidates?.[0]?.content?.parts?.[0]?.text || "";
        console.log("[WEBHOOK] Agent " + agent + " response:", agentResponse.slice(0,80));
      } catch(agentErr) {
        console.log("[WEBHOOK] Agent error:", agentErr.message);
        agentResponse = "Hola! Gracias por escribirnos. Un asesor de PrismDB te contactará pronto.";
      }

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
// ═══════════════════════════════════════════════════════════
//  PrismDB — Audit Layer
//  Registro completo de cada decisión de la infraestructura
//
//  Registra:
//  - POR QUÉ la IA hizo algo
//  - QUÉ información usó
//  - QUIÉN aprobó
//  - QUÉ riesgo detectó
//  - QUÉ acción ejecutó
//  - QUÉ resultado obtuvo
//
//  AGREGAR AL FINAL DE prismdb-backend.js
//  (antes del error handler)
// ═══════════════════════════════════════════════════════════

// ── INICIALIZAR TABLA DE AUDITORÍA ───────────────────────
async function initAuditTable() {
  if (!db || memoryMode !== "postgresql") return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id            SERIAL PRIMARY KEY,
      trace_id      TEXT NOT NULL,
      timestamp     TIMESTAMPTZ DEFAULT NOW(),

      -- Qué pasó
      event_type    TEXT NOT NULL,
      action        TEXT NOT NULL,
      outcome       TEXT NOT NULL DEFAULT 'success',

      -- Quién / qué entidad
      entity_id     TEXT,
      entity_type   TEXT,
      agent         TEXT,

      -- Por qué — razonamiento de la IA
      reasoning     TEXT,
      confidence    TEXT,
      risk_level    TEXT,

      -- Qué información usó
      context_used  JSONB DEFAULT '{}',
      memory_keys   TEXT[],

      -- Qué ejecutó
      action_taken  TEXT,
      message_sent  TEXT,
      whatsapp_to   TEXT,

      -- Resultado
      result        JSONB DEFAULT '{}',
      duration_ms   INTEGER,

      -- Aprobación humana
      autonomy_mode TEXT,
      approved_by   TEXT,
      approved_at   TIMESTAMPTZ,

      -- Metadata
      source        TEXT DEFAULT 'system',
      version       TEXT DEFAULT '2.1'
    );

    CREATE INDEX IF NOT EXISTS audit_entity_idx    ON audit_log(entity_id);
    CREATE INDEX IF NOT EXISTS audit_timestamp_idx ON audit_log(timestamp DESC);
    CREATE INDEX IF NOT EXISTS audit_agent_idx     ON audit_log(agent);
    CREATE INDEX IF NOT EXISTS audit_trace_idx     ON audit_log(trace_id);
  `).catch(e => console.error("[AUDIT INIT]", e.message));
  console.log("✅ Audit Layer activo");
}

setTimeout(initAuditTable, 4000);

// ── FUNCIÓN PRINCIPAL DE AUDITORÍA ──────────────────────
async function auditLog({
  trace_id,
  event_type,
  action,
  outcome = "success",
  entity_id,
  entity_type,
  agent,
  reasoning,
  confidence,
  risk_level,
  context_used = {},
  memory_keys = [],
  action_taken,
  message_sent,
  whatsapp_to,
  result = {},
  duration_ms,
  autonomy_mode,
  approved_by,
  source = "system",
}) {
  if (!db || memoryMode !== "postgresql") return;

  await db.query(`
    INSERT INTO audit_log (
      trace_id, event_type, action, outcome,
      entity_id, entity_type, agent,
      reasoning, confidence, risk_level,
      context_used, memory_keys,
      action_taken, message_sent, whatsapp_to,
      result, duration_ms, autonomy_mode, approved_by, source
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
      $11,$12,$13,$14,$15,$16,$17,$18,$19,$20
    )
  `, [
    trace_id || `trace_${Date.now()}`,
    event_type, action, outcome,
    entity_id, entity_type, agent,
    reasoning, confidence, risk_level,
    context_used, memory_keys,
    action_taken, message_sent, whatsapp_to,
    result, duration_ms, autonomy_mode, approved_by, source,
  ]).catch(e => console.error("[AUDIT ERROR]", e.message));
}

// ── WRAPPER DEL ROUTER CON AUDITORÍA ────────────────────
// Envuelve processEvent para registrar cada decisión
async function auditedProcessEvent(event, dbConn) {
  const traceId = `trace_${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
  const startTime = Date.now();

  try {
    const result = await processEvent(event, dbConn);
    const duration = Date.now() - startTime;

    await auditLog({
      trace_id: traceId,
      event_type: event.type,
      action: result.classification?.action || "event_processed",
      outcome: result.error ? "error" : "success",
      entity_id: event.entityId,
      entity_type: event.entityType,
      agent: result.classification?.agent,
      reasoning: result.classification?.reason,
      confidence: result.classification?.confidence,
      risk_level: result.classification?.risk,
      context_used: { event_data: event.data, memory_snapshot: {} },
      action_taken: result.actions_taken?.join(", "),
      message_sent: result.agent_response?.slice(0, 500),
      autonomy_mode: result.autonomy,
      result: { actions: result.actions_taken, memory_updated: result.memory_updated },
      duration_ms: duration,
      source: event.triggeredBy || "system",
    });

    return { ...result, trace_id: traceId };
  } catch (err) {
    await auditLog({
      trace_id: traceId,
      event_type: event.type,
      action: "event_processing_failed",
      outcome: "error",
      entity_id: event.entityId,
      reasoning: err.message,
      duration_ms: Date.now() - startTime,
    });
    throw err;
  }
}

// ══════════════════════════════════════════════════════════
//  RUTAS DE AUDITORÍA
// ══════════════════════════════════════════════════════════

// GET /audit — log completo paginado
app.get("/audit", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.json({ logs: [], total: 0 });

    const {
      limit = 50, offset = 0,
      agent, entity_id, outcome,
      from, to, event_type,
    } = req.query;

    let query = "SELECT * FROM audit_log WHERE 1=1";
    const params = [];

    if (agent)      { params.push(agent);      query += ` AND agent = $${params.length}`; }
    if (entity_id)  { params.push(entity_id);  query += ` AND entity_id = $${params.length}`; }
    if (outcome)    { params.push(outcome);     query += ` AND outcome = $${params.length}`; }
    if (event_type) { params.push(event_type);  query += ` AND event_type = $${params.length}`; }
    if (from)       { params.push(from);        query += ` AND timestamp >= $${params.length}`; }
    if (to)         { params.push(to);          query += ` AND timestamp <= $${params.length}`; }

    const countResult = await db.query(
      query.replace("SELECT *", "SELECT COUNT(*)"), params
    );

    params.push(parseInt(limit));
    params.push(parseInt(offset));
    query += ` ORDER BY timestamp DESC LIMIT $${params.length - 1} OFFSET $${params.length}`;

    const result = await db.query(query, params);

    res.json({
      logs: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset),
    });
  } catch (err) { next(err); }
});

// GET /audit/:traceId — detalle completo de una decisión
app.get("/audit/:traceId", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.status(503).json({ error: "Requiere PostgreSQL" });

    const result = await db.query(
      "SELECT * FROM audit_log WHERE trace_id = $1 ORDER BY timestamp ASC",
      [req.params.traceId]
    );

    if (!result.rows.length) return res.status(404).json({ error: "Trace no encontrado" });

    // Análisis IA del trace
    const analysis = await claudeChat(
      "Analista de auditoría de sistemas IA. Explica en lenguaje simple qué hizo el sistema. SOLO JSON.",
      `Analiza estas entradas de auditoría y explica qué pasó:
${JSON.stringify(result.rows)}

JSON: {
  "resumen": "qué pasó en 2 oraciones simples",
  "decision_correcta": true,
  "riesgo_detectado": "...",
  "informacion_usada": ["..."],
  "accion_ejecutada": "...",
  "resultado": "...",
  "recomendacion": "..."
}`
    ).catch(() => "{}");

    let analysisObj = {};
    try { analysisObj = JSON.parse(analysis.replace(/```json|```/g, "").trim()); } catch {}

    res.json({
      trace_id: req.params.traceId,
      entries: result.rows,
      analysis: analysisObj,
    });
  } catch (err) { next(err); }
});

// GET /audit/entity/:entityId — historial de auditoría de una entidad
app.get("/audit/entity/:entityId", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.json({ logs: [] });

    const result = await db.query(
      "SELECT * FROM audit_log WHERE entity_id = $1 ORDER BY timestamp DESC LIMIT 100",
      [req.params.entityId]
    );

    // Timeline de la entidad
    const timeline = result.rows.map(r => ({
      timestamp: r.timestamp,
      agent: r.agent,
      action: r.action,
      outcome: r.outcome,
      autonomy: r.autonomy_mode,
      message: r.message_sent,
      duration_ms: r.duration_ms,
    }));

    res.json({
      entity_id: req.params.entityId,
      total_interactions: result.rowCount,
      timeline,
      agents_involved: [...new Set(result.rows.map(r => r.agent).filter(Boolean))],
      last_interaction: result.rows[0]?.timestamp,
    });
  } catch (err) { next(err); }
});

// GET /audit/stats — estadísticas globales de auditoría
app.get("/audit/stats", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.json({ stats: {} });

    const [totals, byAgent, byOutcome, byAutonomy, recent] = await Promise.all([
      db.query("SELECT COUNT(*) as total, AVG(duration_ms) as avg_duration FROM audit_log"),
      db.query("SELECT agent, COUNT(*) as count FROM audit_log WHERE agent IS NOT NULL GROUP BY agent ORDER BY count DESC"),
      db.query("SELECT outcome, COUNT(*) as count FROM audit_log GROUP BY outcome"),
      db.query("SELECT autonomy_mode, COUNT(*) as count FROM audit_log WHERE autonomy_mode IS NOT NULL GROUP BY autonomy_mode"),
      db.query("SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT 5"),
    ]);

    res.json({
      total_decisions: parseInt(totals.rows[0].total),
      avg_duration_ms: Math.round(parseFloat(totals.rows[0].avg_duration) || 0),
      by_agent: byAgent.rows,
      by_outcome: byOutcome.rows,
      by_autonomy: byAutonomy.rows,
      recent_decisions: recent.rows,
    });
  } catch (err) { next(err); }
});

// POST /audit/explain — Claude explica una decisión en lenguaje simple
app.post("/audit/explain", async (req, res, next) => {
  try {
    const { trace_id, entity_id, context = "" } = req.body;
    if (!trace_id && !entity_id) return res.status(400).json({ error: "trace_id o entity_id requerido" });

    const query = trace_id
      ? "SELECT * FROM audit_log WHERE trace_id = $1 ORDER BY timestamp DESC LIMIT 10"
      : "SELECT * FROM audit_log WHERE entity_id = $1 ORDER BY timestamp DESC LIMIT 10";

    const result = await db.query(query, [trace_id || entity_id]);
    if (!result.rows.length) return res.json({ explanation: "No se encontraron registros de auditoría." });

    const explanation = await claudeChat(
      "Eres un auditor de sistemas IA que explica decisiones en lenguaje claro para ejecutivos no técnicos.",
      `Explica qué hizo PrismDB con esta entidad/decisión.
Registros: ${JSON.stringify(result.rows.slice(0, 5))}
Contexto adicional: ${context}

Responde en español, de forma clara y directa. Máximo 200 palabras.
Incluye: qué pasó, por qué, qué información usó, qué hizo, y si fue la decisión correcta.`,
      "claude-haiku-4-5-20251001", 400
    );

    res.json({
      trace_id,
      entity_id,
      explanation,
      records_analyzed: result.rowCount,
    });
  } catch (err) { next(err); }
});

// POST /audit/log — registrar manualmente una entrada de auditoría
app.post("/audit/log", async (req, res, next) => {
  try {
    await auditLog({ ...req.body, source: "manual" });
    res.json({ ok: true, message: "Entrada de auditoría registrada" });
  } catch (err) { next(err); }
});

// GET /audit/export — exportar auditoría como CSV
app.get("/audit/export", async (req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.status(503).json({ error: "Requiere PostgreSQL" });

    const { from, to, agent } = req.query;
    let query = "SELECT trace_id, timestamp, event_type, action, outcome, entity_id, agent, reasoning, action_taken, message_sent, autonomy_mode, duration_ms FROM audit_log WHERE 1=1";
    const params = [];

    if (from)  { params.push(from);  query += ` AND timestamp >= $${params.length}`; }
    if (to)    { params.push(to);    query += ` AND timestamp <= $${params.length}`; }
    if (agent) { params.push(agent); query += ` AND agent = $${params.length}`; }

    query += " ORDER BY timestamp DESC LIMIT 1000";
    const result = await db.query(query, params);

    const headers = ["trace_id","timestamp","event_type","action","outcome","entity_id","agent","reasoning","action_taken","message_sent","autonomy_mode","duration_ms"];
    const csv = [
      headers.join(","),
      ...result.rows.map(r =>
        headers.map(h => `"${(r[h] || "").toString().replace(/"/g, '""')}"`).join(",")
      )
    ].join("\n");

    res.setHeader("Content-Type", "text/csv");
    res.setHeader("Content-Disposition", `attachment; filename="prismdb-audit-${Date.now()}.csv"`);
    res.send(csv);
  } catch (err) { next(err); }
});
// ═══════════════════════════════════════════════════════════
//  PrismDB — pgvector / Memoria Semántica
//  Embeddings con Gemini text-embedding-004
//  Búsqueda por similitud semántica en toda la memoria
//
//  AGREGAR AL FINAL DE prismdb-backend.js
//  (antes del error handler)
// ═══════════════════════════════════════════════════════════

// ── INICIALIZAR TABLAS DE EMBEDDINGS ────────────────────
async function initVectorTables() {
  if (!db || memoryMode !== "postgresql") return;
  await db.query(`
    -- Activar extensión pgvector
    CREATE EXTENSION IF NOT EXISTS vector;

    -- Tabla de chunks de conocimiento con embeddings
    CREATE TABLE IF NOT EXISTS knowledge_chunks (
      id          SERIAL PRIMARY KEY,
      entity_id   TEXT,
      entity_type TEXT,
      content     TEXT NOT NULL,
      summary     TEXT,
      embedding   vector(512),
      metadata    JSONB DEFAULT '{}',
      agent       TEXT,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );

    -- Índice para búsqueda por similitud coseno
    CREATE INDEX IF NOT EXISTS knowledge_embedding_idx
      ON knowledge_chunks
      USING ivfflat (embedding vector_cosine_ops)
      WITH (lists = 100);

    -- Índice por entidad
    CREATE INDEX IF NOT EXISTS knowledge_entity_idx
      ON knowledge_chunks(entity_id);

    -- Tabla de patrones detectados entre entidades
    CREATE TABLE IF NOT EXISTS patterns (
      id          SERIAL PRIMARY KEY,
      pattern     TEXT NOT NULL,
      description TEXT,
      embedding   vector(512),
      examples    JSONB DEFAULT '[]',
      confidence  NUMERIC DEFAULT 0,
      agent       TEXT,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );
  `).catch(e => console.error("[VECTOR INIT]", e.message));
  console.log("✅ pgvector activo — Memoria semántica lista");
}

setTimeout(initVectorTables, 5000);

// ══════════════════════════════════════════════════════════
//  GENERAR EMBEDDINGS CON GEMINI
// ══════════════════════════════════════════════════════════
async function generateEmbedding(text) {
  try {
    const voyageKey = process.env.VOYAGE_API_KEY;
    if (!voyageKey) { console.error("[EMBEDDING] VOYAGE_API_KEY no configurada"); return null; }
    const res = await fetch("https://api.voyageai.com/v1/embeddings", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${voyageKey}` },
      body: JSON.stringify({
        model: "voyage-3-lite",
        input: text.slice(0, 4000),
        input_type: "document"
      })
    });
    const data = await res.json();
    if (data.error) { console.error("[EMBEDDING ERROR]", data.error); return null; }
    return data.data?.[0]?.embedding || null;
  } catch (e) {
    console.error("[EMBEDDING ERROR]", e.message);
    return null;
  }
}

// ══════════════════════════════════════════════════════════
//  GUARDAR CHUNK EN MEMORIA SEMÁNTICA
// ══════════════════════════════════════════════════════════
async function saveKnowledge(entityId, entityType, content, metadata = {}, agent = "system") {
  if (!db || memoryMode !== "postgresql") return null;

  const embedding = await generateEmbedding(content);
  if (!embedding) return null;

  // Generar resumen corto con Gemini
  let summary = content.slice(0, 100);
  try {
    const geminiKey = process.env.GEMINI_API_KEY || process.env.ANTHROPIC_API_KEY;
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ parts: [{ text: `Resume en máximo 20 palabras: ${content}` }] }],
          generationConfig: { maxOutputTokens: 50 }
        })
      }
    );
    const data = await res.json();
    summary = data.candidates?.[0]?.content?.parts?.[0]?.text || summary;
  } catch {}

  const result = await db.query(
    `INSERT INTO knowledge_chunks (entity_id, entity_type, content, summary, embedding, metadata, agent)
     VALUES ($1, $2, $3, $4, $5::vector, $6, $7) RETURNING id`,
    [entityId, entityType, content, summary.trim(), JSON.stringify(embedding), metadata, agent]
  ).catch(e => { console.error("[SAVE KNOWLEDGE]", e.message); return null; });

  return result?.rows?.[0]?.id;
}

// ══════════════════════════════════════════════════════════
//  BÚSQUEDA SEMÁNTICA
// ══════════════════════════════════════════════════════════
async function semanticSearch(query, options = {}) {
  if (!db || memoryMode !== "postgresql") return [];

  const {
    limit = 5,
    threshold = 0.7,
    entityType,
    agent,
  } = options;

  const embedding = await generateEmbedding(query);
  if (!embedding) return [];

  let sql = `
    SELECT
      id, entity_id, entity_type, content, summary, metadata, agent, created_at,
      1 - (embedding <=> $1::vector) AS similarity
    FROM knowledge_chunks
    WHERE 1 - (embedding <=> $1::vector) > $2
  `;
  const params = [JSON.stringify(embedding), threshold];

  if (entityType) { params.push(entityType); sql += ` AND entity_type = $${params.length}`; }
  if (agent)      { params.push(agent);      sql += ` AND agent = $${params.length}`; }

  params.push(limit);
  sql += ` ORDER BY similarity DESC LIMIT $${params.length}`;

  const result = await db.query(sql, params).catch(e => {
    console.error("[SEMANTIC SEARCH]", e.message);
    return { rows: [] };
  });

  return result.rows;
}

// ══════════════════════════════════════════════════════════
//  RUTAS HTTP — pgvector
// ══════════════════════════════════════════════════════════

// POST /memory/embed — guardar texto en memoria semántica
app.post("/semantic/embed", async (req, res, next) => {
  try {
    const { entity_id, entity_type = "contact", content, metadata = {}, agent = "system" } = req.body;
    if (!entity_id || !content) return res.status(400).json({ error: "entity_id y content requeridos" });

    const id = await saveKnowledge(entity_id, entity_type, content, metadata, agent);
    if (!id) return res.status(503).json({ error: "pgvector no disponible o embedding falló" });

    res.json({ ok: true, id, entity_id, summary: content.slice(0, 80) + "..." });
  } catch (err) { next(err); }
});

// POST /memory/search — búsqueda semántica
app.post("/semantic/search", async (req, res, next) => {
  try {
    const { query, limit = 5, threshold = 0.6, entity_type, agent } = req.body;
    if (!query) return res.status(400).json({ error: "query requerido" });

    const results = await semanticSearch(query, { limit, threshold, entityType: entity_type, agent });

    // IA interpreta los resultados
    let interpretation = null;
    if (results.length) {
      const geminiKey = process.env.GEMINI_API_KEY || process.env.ANTHROPIC_API_KEY;
      try {
        const aiRes = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiKey}`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              contents: [{ parts: [{ text: `Analiza estos resultados de búsqueda semántica y da un insight en 2 oraciones.
Query: "${query}"
Resultados: ${JSON.stringify(results.map(r => ({ summary: r.summary, similarity: r.similarity, entity: r.entity_id })))}
Responde directamente sin preámbulo.` }] }],
              generationConfig: { maxOutputTokens: 150 }
            })
          }
        );
        const aiData = await aiRes.json();
        interpretation = aiData.candidates?.[0]?.content?.parts?.[0]?.text || null;
      } catch {}
    }

    res.json({
      query,
      results,
      total: results.length,
      interpretation,
    });
  } catch (err) { next(err); }
});

// POST /memory/similar — encontrar entidades similares a una dada
app.post("/semantic/similar", async (req, res, next) => {
  try {
    const { entity_id, limit = 5 } = req.body;
    if (!entity_id) return res.status(400).json({ error: "entity_id requerido" });

    if (!db || memoryMode !== "postgresql") return res.json({ similar: [] });

    // Obtener embeddings de esta entidad
    const entity = await db.query(
      "SELECT content FROM knowledge_chunks WHERE entity_id = $1 ORDER BY created_at DESC LIMIT 1",
      [entity_id]
    );

    if (!entity.rows.length) return res.json({ similar: [], message: "Entidad sin embeddings aún" });

    // Buscar similares excluyendo la misma entidad
    const embedding = await generateEmbedding(entity.rows[0].content);
    if (!embedding) return res.json({ similar: [] });

    const result = await db.query(`
      SELECT
        entity_id, entity_type, summary,
        1 - (embedding <=> $1::vector) AS similarity
      FROM knowledge_chunks
      WHERE entity_id != $2
        AND 1 - (embedding <=> $1::vector) > 0.6
      GROUP BY entity_id, entity_type, summary, embedding
      ORDER BY similarity DESC
      LIMIT $3
    `, [JSON.stringify(embedding), entity_id, limit]).catch(() => ({ rows: [] }));

    res.json({ entity_id, similar: result.rows });
  } catch (err) { next(err); }
});

// POST /memory/patterns — detectar patrones entre entidades
app.post("/semantic/patterns", async (req, res, next) => {
  try {
    const { context = "ventas", limit = 10 } = req.body;
    if (!db || memoryMode !== "postgresql") return res.json({ patterns: [] });

    // Obtener chunks recientes
    const chunks = await db.query(
      "SELECT entity_id, content, summary, agent FROM knowledge_chunks ORDER BY created_at DESC LIMIT $1",
      [limit * 3]
    );

    if (!chunks.rows.length) return res.json({ patterns: [], message: "Sin datos suficientes aún" });

    // IA detecta patrones
    const geminiKey = process.env.GEMINI_API_KEY || process.env.ANTHROPIC_API_KEY;
    const aiRes = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ parts: [{ text: `Analiza estos registros de memoria empresarial y detecta patrones útiles para ${context}.
Datos: ${JSON.stringify(chunks.rows.slice(0, 20))}

Responde SOLO JSON:
{"patterns":[{"patron":"descripción del patrón","frecuencia":"alta|media|baja","insight":"qué significa para el negocio","accion":"qué hacer con este patrón"}]}` }] }],
          generationConfig: { maxOutputTokens: 600 }
        })
      }
    );
    const aiData = await aiRes.json();
    const text = aiData.candidates?.[0]?.content?.parts?.[0]?.text || "{}";

    let parsed = { patterns: [] };
    try { parsed = JSON.parse(text.replace(/```json|```/g, "").trim()); } catch {}

    res.json({ context, patterns: parsed.patterns || [], total_analyzed: chunks.rowCount });
  } catch (err) { next(err); }
});

// GET /memory/stats/vector — estadísticas de la memoria semántica
app.get("/semantic/stats", async (_req, res, next) => {
  try {
    if (!db || memoryMode !== "postgresql") return res.json({ stats: {}, mode: "in-memory" });

    const [total, byType, byAgent, recent] = await Promise.all([
      db.query("SELECT COUNT(*) as total FROM knowledge_chunks"),
      db.query("SELECT entity_type, COUNT(*) as count FROM knowledge_chunks GROUP BY entity_type ORDER BY count DESC"),
      db.query("SELECT agent, COUNT(*) as count FROM knowledge_chunks GROUP BY agent ORDER BY count DESC"),
      db.query("SELECT entity_id, summary, created_at FROM knowledge_chunks ORDER BY created_at DESC LIMIT 5"),
    ]);

    res.json({
      total_chunks: parseInt(total.rows[0].total),
      by_type: byType.rows,
      by_agent: byAgent.rows,
      recent: recent.rows,
    });
  } catch (err) { next(err); }
});

// Exportar funciones para uso interno
globalThis.saveKnowledge = saveKnowledge;
globalThis.semanticSearch = semanticSearch;
globalThis.generateEmbedding = generateEmbedding;
