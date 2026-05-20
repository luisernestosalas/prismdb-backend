// ═══════════════════════════════════════════════════════════
//  PrismDB — Backend v2.0 · WhatsApp Revenue OS™
//  Stack: Firecrawl · Twilio · Anthropic · MercadoPago
//  NEW:   Memory Layer · Agent Coordination · Finance AI
// ═══════════════════════════════════════════════════════════

import express from "express";
import cors from "cors";
import helmet from "helmet";
import dotenv from "dotenv";
import pg from "pg";

dotenv.config();

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(helmet());
app.use(cors({ origin: process.env.FRONTEND_URL || "*" }));
app.use(express.json({ limit: "2mb" }));

// ════════════════════════════════════════════════════════════
//  POSTGRESQL — Memory Layer
//  Railway provee DATABASE_URL automáticamente al agregar
//  el plugin de PostgreSQL al proyecto
// ════════════════════════════════════════════════════════════
const db = process.env.DATABASE_URL
  ? new pg.Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
  : null;

// Inicializar tablas de memoria
async function initMemory() {
  if (!db) {
    console.log("⚠️  Sin PostgreSQL — Memory Layer en modo in-memory");
    return;
  }
  try {
    await db.query(`
      -- Entidades: clientes, prospectos, candidatos
      CREATE TABLE IF NOT EXISTS entities (
        id          TEXT PRIMARY KEY,
        tipo        TEXT[] DEFAULT '{}',        -- ['prospecto','cliente','candidato']
        nombre      TEXT,
        empresa     TEXT,
        cargo       TEXT,
        telefono    TEXT,
        email       TEXT,
        ciudad      TEXT,
        sector      TEXT,
        score_wrp   INTEGER DEFAULT 0,
        agente      TEXT DEFAULT 'sdr',         -- sdr | revenue | talent
        stage       TEXT DEFAULT 'nuevo',
        metadata    JSONB DEFAULT '{}',
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        updated_at  TIMESTAMPTZ DEFAULT NOW()
      );

      -- Memoria de conversaciones por entidad
      CREATE TABLE IF NOT EXISTS memory (
        id          SERIAL PRIMARY KEY,
        entity_id   TEXT REFERENCES entities(id) ON DELETE CASCADE,
        canal       TEXT DEFAULT 'whatsapp',    -- whatsapp | system | agent
        rol         TEXT DEFAULT 'user',         -- user | assistant | system
        contenido   TEXT NOT NULL,
        agente      TEXT,                        -- qué agente procesó esto
        metadata    JSONB DEFAULT '{}',
        created_at  TIMESTAMPTZ DEFAULT NOW()
      );

      -- Eventos del Revenue OS
      CREATE TABLE IF NOT EXISTS events (
        id          SERIAL PRIMARY KEY,
        tipo        TEXT NOT NULL,               -- mensaje_recibido | pedido | prospecto_nuevo | etc
        entity_id   TEXT,
        payload     JSONB DEFAULT '{}',
        procesado   BOOLEAN DEFAULT FALSE,
        agente      TEXT,
        created_at  TIMESTAMPTZ DEFAULT NOW()
      );

      -- Pipeline de revenue (reemplaza el in-memory)
      CREATE TABLE IF NOT EXISTS pipeline (
        id          SERIAL PRIMARY KEY,
        entity_id   TEXT REFERENCES entities(id),
        modulo      TEXT DEFAULT 'ventas',       -- ventas | prospección | talento
        stage       TEXT DEFAULT 'contacto',
        valor       NUMERIC DEFAULT 0,
        probabilidad INTEGER DEFAULT 50,
        fecha_cierre_estimada DATE,
        notas       TEXT,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        updated_at  TIMESTAMPTZ DEFAULT NOW()
      );

      -- Índices para rendimiento
      CREATE INDEX IF NOT EXISTS idx_memory_entity ON memory(entity_id);
      CREATE INDEX IF NOT EXISTS idx_events_tipo ON events(tipo);
      CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_id);
      CREATE INDEX IF NOT EXISTS idx_pipeline_modulo ON pipeline(modulo);
      CREATE INDEX IF NOT EXISTS idx_entities_tipo ON entities USING GIN(tipo);
    `);
    console.log("✅ Memory Layer PostgreSQL inicializado");
  } catch (err) {
    console.error("[MEMORY INIT ERROR]", err.message);
  }
}

// ─── Helpers de memoria ─────────────────────────────────────
async function getOrCreateEntity(data) {
  if (!db) return { id: `mem_${Date.now()}`, ...data };
  const { telefono, nombre, empresa, tipo = ["prospecto"] } = data;
  const key = telefono || `${nombre}_${empresa}`.toLowerCase().replace(/\s/g, "_");
  const { rows } = await db.query(
    `INSERT INTO entities (id, tipo, nombre, empresa, cargo, telefono, email, ciudad, sector, score_wrp, agente)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT (id) DO UPDATE SET
       nombre = COALESCE(EXCLUDED.nombre, entities.nombre),
       updated_at = NOW()
     RETURNING *`,
    [key, tipo, data.nombre, data.empresa, data.cargo, data.telefono,
     data.email, data.ciudad, data.sector, data.score_wrp || 0, data.agente || "sdr"]
  );
  return rows[0];
}

async function saveMemory(entityId, rol, contenido, agente = "system", metadata = {}) {
  if (!db) return;
  await db.query(
    `INSERT INTO memory (entity_id, rol, contenido, agente, metadata) VALUES ($1,$2,$3,$4,$5)`,
    [entityId, rol, contenido, agente, JSON.stringify(metadata)]
  );
}

async function getMemory(entityId, limit = 10) {
  if (!db) return [];
  const { rows } = await db.query(
    `SELECT * FROM memory WHERE entity_id = $1 ORDER BY created_at DESC LIMIT $2`,
    [entityId, limit]
  );
  return rows.reverse();
}

async function emitEvent(tipo, entityId, payload = {}, agente = null) {
  if (!db) return;
  await db.query(
    `INSERT INTO events (tipo, entity_id, payload, agente) VALUES ($1,$2,$3,$4)`,
    [tipo, entityId, JSON.stringify(payload), agente]
  );
}

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
//  CLAUDE — Helper central
// ════════════════════════════════════════════════════════════
async function claude(messages, system = "", model = "claude-sonnet-4-20250514", maxTokens = 600) {
  const body = { model, max_tokens: maxTokens, messages };
  if (system) body.system = system;
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": process.env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  return data.content?.[0]?.text || "";
}

async function claudeJSON(prompt, model = "claude-haiku-4-5-20251001", maxTokens = 400) {
  const text = await claude([{ role: "user", content: prompt }], "", model, maxTokens);
  try {
    return JSON.parse(text.replace(/```json|```/g, "").trim());
  } catch {
    return {};
  }
}

// ── Health ───────────────────────────────────────────────────
app.get("/health", async (_req, res) => {
  const dbOk = db ? await db.query("SELECT 1").then(() => true).catch(() => false) : false;
  res.json({ ok: true, app: "prismdb-v2", memory: dbOk ? "postgresql" : "in-memory", ts: Date.now() });
});

// ════════════════════════════════════════════════════════════
//  MEMORY LAYER — API
// ════════════════════════════════════════════════════════════

// GET /memory/:entityId — obtener historial de una entidad
app.get("/memory/:entityId", async (req, res, next) => {
  try {
    const history = await getMemory(req.params.entityId, 20);
    if (!db) return res.json({ entity_id: req.params.entityId, history: [], note: "PostgreSQL no configurado" });
    const { rows: [entity] } = await db.query("SELECT * FROM entities WHERE id = $1", [req.params.entityId]);
    res.json({ entity: entity || null, history, total: history.length });
  } catch (err) { next(err); }
});

// GET /memory/entity/:phone — buscar entidad por teléfono
app.get("/memory/entity/:phone", async (req, res, next) => {
  try {
    if (!db) return res.json({ entity: null });
    const phone = req.params.phone.replace(/\D/g, "");
    const { rows } = await db.query(
      "SELECT * FROM entities WHERE telefono LIKE $1",
      [`%${phone}%`]
    );
    const entity = rows[0] || null;
    if (entity) {
      const history = await getMemory(entity.id, 10);
      return res.json({ entity, history });
    }
    res.json({ entity: null });
  } catch (err) { next(err); }
});

// POST /memory/save — guardar mensaje manualmente
app.post("/memory/save", async (req, res, next) => {
  try {
    const { entity_id, rol, contenido, agente, metadata } = req.body;
    await saveMemory(entity_id, rol, contenido, agente, metadata);
    res.json({ ok: true });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  AGENT COORDINATION — El cerebro del Revenue OS
// ════════════════════════════════════════════════════════════

// POST /agents/route — decide qué agente debe actuar
app.post("/agents/route", async (req, res, next) => {
  try {
    const { message, phone, entityId } = req.body;

    // Recuperar memoria de la entidad
    const history = entityId ? await getMemory(entityId, 8) : [];
    const historyStr = history.map(m => `${m.rol}: ${m.contenido}`).join("\n");

    const decision = await claudeJSON(`
Eres el coordinador del Revenue OS de PrismDB.
Analiza el mensaje y el historial. Decide qué agente debe actuar.

AGENTES DISPONIBLES:
- sdr: prospectar nuevos clientes, primer contacto, calificar leads
- revenue: atender clientes activos, pedidos, catálogo, seguimiento de ventas
- talent: buscar candidatos, calificar talento, contactar para vacantes
- finance: análisis de pipeline, predicciones de revenue, reportes
- coordinator: si necesitas coordinar múltiples agentes

Responde SOLO JSON:
{
  "agente": "revenue",
  "accion": "atender_pedido",
  "prioridad": "alta",
  "razon": "cliente activo haciendo pedido",
  "contexto_relevante": "último pedido hace 3 días"
}

HISTORIAL: ${historyStr || "Sin historial previo"}
MENSAJE ACTUAL: ${message}
`, "claude-haiku-4-5-20251001", 300);

    res.json({ ...decision, entity_id: entityId, phone });
  } catch (err) { next(err); }
});

// POST /agents/sdr — Agente de Prospección con memoria
app.post("/agents/sdr", async (req, res, next) => {
  try {
    const { lead, businessContext = "", accion = "generar_mensaje" } = req.body;

    // Crear/recuperar entidad en memoria
    const entity = await getOrCreateEntity({
      ...lead,
      tipo: ["prospecto"],
      agente: "sdr"
    });

    // Recuperar historial
    const history = await getMemory(entity.id, 6);
    const historyStr = history.map(m => `${m.rol}: ${m.contenido}`).join("\n");

    let resultado = {};

    if (accion === "generar_mensaje") {
      const mensaje = await claude([{
        role: "user",
        content: `Genera un mensaje WhatsApp de prospección personalizado.
LEAD: ${JSON.stringify(lead)}
HISTORIAL PREVIO: ${historyStr || "Sin contacto previo"}
CONTEXTO NEGOCIO: ${businessContext}

Reglas:
- Máximo 160 caracteres
- Personalizado con nombre y empresa
- Si hay historial, referencia la conversación anterior
- Directo, sin relleno
- En español LATAM`
      }],
      "Eres un SDR experto en ventas B2B para LATAM. Generas mensajes que convierten.",
      "claude-sonnet-4-20250514", 200);

      // Guardar en memoria
      await saveMemory(entity.id, "assistant", mensaje, "sdr", { accion: "mensaje_generado", lead });
      await emitEvent("mensaje_sdr_generado", entity.id, { lead, mensaje }, "sdr");

      resultado = { mensaje, entity_id: entity.id, historial_msgs: history.length };
    }

    if (accion === "calificar") {
      const scoring = await claudeJSON(`
Califica este prospecto con contexto histórico. SOLO JSON:
{"score":85,"nivel":"Alto","razones":[],"siguiente_accion":"...","mejor_momento_contacto":"..."}
LEAD: ${JSON.stringify(lead)}
HISTORIAL: ${historyStr || "Ninguno"}
`);
      await saveMemory(entity.id, "system", `Score WRP: ${scoring.score}`, "sdr", scoring);
      if (db && scoring.score) {
        await db.query("UPDATE entities SET score_wrp = $1 WHERE id = $2", [scoring.score, entity.id]);
      }
      resultado = { ...scoring, entity_id: entity.id };
    }

    res.json(resultado);
  } catch (err) { next(err); }
});

// POST /agents/revenue — Agente de Ventas con memoria
app.post("/agents/revenue", async (req, res, next) => {
  try {
    const { cliente, mensaje_entrante, accion = "responder" } = req.body;

    const entity = await getOrCreateEntity({
      ...cliente,
      tipo: ["cliente"],
      agente: "revenue"
    });

    const history = await getMemory(entity.id, 10);
    const historyStr = history.map(m => `${m.rol}: ${m.contenido}`).join("\n");

    // Guardar mensaje entrante en memoria
    if (mensaje_entrante) {
      await saveMemory(entity.id, "user", mensaje_entrante, "revenue");
      await emitEvent("mensaje_recibido_wa", entity.id, { mensaje: mensaje_entrante, cliente }, "revenue");
    }

    let resultado = {};

    if (accion === "responder") {
      const respuesta = await claude([{
        role: "user",
        content: `Cliente envió: "${mensaje_entrante}"
HISTORIAL DE CONVERSACIÓN:
${historyStr || "Sin historial"}

PERFIL CLIENTE: ${JSON.stringify(cliente)}

Genera la respuesta ideal para WhatsApp. Máximo 200 caracteres.
Si está haciendo un pedido, confírmalo con detalles.
Si tiene una queja, resuelve con empatía.
Si pregunta por productos, responde directamente.`
      }],
      "Eres el agente de Revenue de PrismDB. Atiendes clientes activos por WhatsApp. Eres ágil, amable y orientado al cierre.",
      "claude-sonnet-4-20250514", 250);

      await saveMemory(entity.id, "assistant", respuesta, "revenue");
      resultado = { respuesta, entity_id: entity.id, historial_msgs: history.length + 1 };
    }

    if (accion === "analizar_riesgo") {
      const analisis = await claudeJSON(`
Analiza el riesgo de abandono de este cliente. SOLO JSON:
{"riesgo":"alto|medio|bajo","probabilidad_compra_proximos_7_dias":75,
"señales_abandono":[],"accion_recomendada":"...","mensaje_reactivacion":"..."}
CLIENTE: ${JSON.stringify(cliente)}
HISTORIAL: ${historyStr || "Sin historial"}
`);
      await saveMemory(entity.id, "system", `Análisis riesgo: ${analisis.riesgo}`, "revenue", analisis);
      resultado = { ...analisis, entity_id: entity.id };
    }

    res.json(resultado);
  } catch (err) { next(err); }
});

// POST /agents/talent — Agente de Talento con memoria
app.post("/agents/talent", async (req, res, next) => {
  try {
    const { candidato, vacante, accion = "calificar" } = req.body;

    const entity = await getOrCreateEntity({
      ...candidato,
      tipo: ["candidato"],
      agente: "talent"
    });

    const history = await getMemory(entity.id, 6);
    const historyStr = history.map(m => `${m.rol}: ${m.contenido}`).join("\n");

    let resultado = {};

    if (accion === "calificar") {
      const match = await claudeJSON(`
Evalúa el match candidato-vacante con contexto histórico. SOLO JSON:
{"score":84,"nivel":"Alto","tiene":[],"falta":[],"recomendacion":"...",
"mensaje_personalizado":"máx 150 chars","señal_apertura":"...","momento_ideal_contacto":"..."}
CANDIDATO: ${JSON.stringify(candidato)}
VACANTE: ${JSON.stringify(vacante)}
HISTORIAL PREVIO: ${historyStr || "Primer contacto"}
`, "claude-sonnet-4-20250514", 500);

      await saveMemory(entity.id, "system", `Match score: ${match.score}% para ${vacante?.cargo}`, "talent", match);
      resultado = { ...match, entity_id: entity.id };
    }

    if (accion === "verificar_cruce") {
      // Verificar si el candidato también es prospecto de ventas
      let cruce = { es_prospecto: false, es_cliente: false };
      if (db && candidato.empresa) {
        const { rows } = await db.query(
          `SELECT * FROM entities WHERE empresa ILIKE $1 AND $2 = ANY(tipo)`,
          [`%${candidato.empresa}%`, "prospecto"]
        );
        cruce.es_prospecto = rows.length > 0;
        cruce.empresa_en_pipeline = rows[0] || null;
      }
      resultado = { ...cruce, entity_id: entity.id, candidato: candidato.nombre };
    }

    res.json(resultado);
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  FINANCE AI — Predicción de Revenue
// ════════════════════════════════════════════════════════════

// GET /finance/pipeline — pipeline con predicciones
app.get("/finance/pipeline", async (_req, res, next) => {
  try {
    if (!db) return res.json({ error: "PostgreSQL requerido para Finance AI" });

    const { rows: deals } = await db.query(`
      SELECT p.*, e.nombre, e.empresa, e.telefono, e.score_wrp,
             e.tipo, e.agente
      FROM pipeline p
      LEFT JOIN entities e ON p.entity_id = e.id
      ORDER BY p.updated_at DESC
      LIMIT 100
    `);

    const totalPipeline = deals.reduce((s, d) => s + Number(d.valor || 0), 0);
    const probPonderado = deals.reduce((s, d) => s + (Number(d.valor || 0) * (d.probabilidad || 50) / 100), 0);

    res.json({
      total_pipeline: totalPipeline,
      revenue_probable: Math.round(probPonderado),
      deals_activos: deals.length,
      por_modulo: {
        ventas: deals.filter(d => d.modulo === "ventas").length,
        prospeccion: deals.filter(d => d.modulo === "prospeccion").length,
        talento: deals.filter(d => d.modulo === "talento").length,
      },
      deals
    });
  } catch (err) { next(err); }
});

// POST /finance/predict — predicción de revenue con IA
app.post("/finance/predict", async (req, res, next) => {
  try {
    const { periodo = "30_dias", modulo = "todos" } = req.body;

    let contextData = {};
    if (db) {
      const { rows: eventos } = await db.query(`
        SELECT tipo, COUNT(*) as cantidad, DATE(created_at) as fecha
        FROM events
        WHERE created_at > NOW() - INTERVAL '30 days'
        GROUP BY tipo, DATE(created_at)
        ORDER BY fecha DESC
      `);
      const { rows: entities } = await db.query(`
        SELECT agente, COUNT(*) as total, AVG(score_wrp) as score_promedio
        FROM entities GROUP BY agente
      `);
      contextData = { eventos: eventos.slice(0, 20), entities };
    }

    const prediccion = await claudeJSON(`
Eres el Finance AI de PrismDB. Analiza los datos y genera predicciones de revenue.
SOLO JSON:
{
  "revenue_estimado_30_dias": 4200000,
  "confianza": 78,
  "tendencia": "creciente",
  "clientes_en_riesgo": 3,
  "oportunidades_detectadas": 5,
  "recomendaciones": ["acción 1", "acción 2"],
  "alerta": "mensaje importante si hay algo crítico o null",
  "wrp_score_promedio": 84
}

DATOS ACTUALES: ${JSON.stringify(contextData)}
PERÍODO: ${periodo}
MÓDULO: ${modulo}
`, "claude-sonnet-4-20250514", 500);

    res.json({ ...prediccion, generado_en: new Date().toISOString(), periodo });
  } catch (err) { next(err); }
});

// POST /finance/alert — detectar clientes en riesgo
app.post("/finance/alert", async (req, res, next) => {
  try {
    if (!db) return res.json({ alertas: [] });

    // Clientes sin actividad reciente
    const { rows: inactivos } = await db.query(`
      SELECT e.*, MAX(m.created_at) as ultimo_contacto
      FROM entities e
      LEFT JOIN memory m ON e.id = m.entity_id
      WHERE 'cliente' = ANY(e.tipo)
      GROUP BY e.id
      HAVING MAX(m.created_at) < NOW() - INTERVAL '14 days'
         OR MAX(m.created_at) IS NULL
      LIMIT 20
    `);

    const alertas = inactivos.map(c => ({
      entity_id: c.id,
      nombre: c.nombre,
      empresa: c.empresa,
      telefono: c.telefono,
      dias_sin_contacto: c.ultimo_contacto
        ? Math.floor((Date.now() - new Date(c.ultimo_contacto)) / 86400000)
        : 999,
      accion: "reactivar_con_revenue_agent",
      prioridad: c.score_wrp > 80 ? "alta" : "media"
    }));

    res.json({ alertas, total: alertas.length, accion_sugerida: "Activar Revenue Agent para reactivación automática" });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  WEBHOOK TWILIO — Event-Driven con Memory Layer
// ════════════════════════════════════════════════════════════
app.post("/webhook/twilio", express.urlencoded({ extended: false }), async (req, res) => {
  const { From, Body, ProfileName } = req.body;
  console.log(`[WEBHOOK WA] ${ProfileName || From}: ${Body}`);

  try {
    const phone = From.replace("whatsapp:+57", "").replace("whatsapp:+", "");

    // 1. Buscar o crear entidad en memoria
    const entity = await getOrCreateEntity({
      telefono: phone,
      nombre: ProfileName || `Usuario ${phone}`,
      tipo: ["cliente"],
      agente: "revenue"
    });

    // 2. Guardar mensaje en memoria
    await saveMemory(entity.id, "user", Body, "webhook");

    // 3. Emitir evento
    await emitEvent("mensaje_recibido_wa", entity.id, { mensaje: Body, from: From, nombre: ProfileName }, "webhook");

    // 4. Rutear al agente correcto
    const routeRes = await fetch(`http://localhost:${PORT}/agents/route`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: Body, phone, entityId: entity.id })
    });
    const route = await routeRes.json();

    console.log(`[AGENT ROUTE] → ${route.agente} · ${route.accion}`);

    // 5. Si el agente de revenue debe responder automáticamente
    if (route.agente === "revenue" && route.accion === "responder") {
      const agentRes = await fetch(`http://localhost:${PORT}/agents/revenue`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          cliente: { telefono: phone, nombre: ProfileName },
          mensaje_entrante: Body,
          accion: "responder"
        })
      });
      const agentData = await agentRes.json();
      if (agentData.respuesta) {
        await sendWhatsApp(From, agentData.respuesta);
        console.log(`[REVENUE AGENT] Respondió: ${agentData.respuesta}`);
      }
    }
  } catch (err) {
    console.error("[WEBHOOK ERROR]", err.message);
  }

  res.set("Content-Type", "text/xml");
  res.send("<Response></Response>");
});

// ════════════════════════════════════════════════════════════
//  1. PROSPECCIÓN — Buscar leads (original + memoria)
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
        const lead = await claudeJSON(`Analiza este perfil y devuelve SOLO JSON:
{"nombre":"...","cargo":"...","empresa":"...","telefono":"...o null","email":"...o null","ciudad":"...","score":85,"razon":"por qué es buen prospecto en 10 palabras"}
Perfil: ${item.markdown?.slice(0, 800) || item.description || item.title}
Criterios: cargo=${cargo}, sector=${sector}, ciudad=${ciudad}`, "claude-haiku-4-5-20251001", 300);

        // Guardar en memoria si tiene datos suficientes
        if (lead.nombre && lead.nombre !== "...") {
          await getOrCreateEntity({ ...lead, tipo: ["prospecto"], agente: "sdr" });
          await emitEvent("prospecto_detectado", lead.nombre, { lead, query: searchQuery }, "sdr");
        }

        return { ...lead, url: item.url, source: "firecrawl" };
      })
    );

    res.json({ leads: scoredLeads.filter(l => (l.score || 0) >= scoreMin), total: scoredLeads.length, query: searchQuery });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  2. MENSAJERÍA — WhatsApp con memoria
// ════════════════════════════════════════════════════════════
app.post("/messages/send", async (req, res, next) => {
  try {
    const { phone, message, leadName = "" } = req.body;
    if (!phone || !message) return res.status(400).json({ error: "phone y message requeridos" });

    const data = await sendWhatsApp(phone, message);
    if (data.error_code) throw new Error(data.message);

    // Guardar en memoria
    const entity = await getOrCreateEntity({ telefono: phone, nombre: leadName, tipo: ["prospecto"] });
    await saveMemory(entity.id, "assistant", message, "sdr", { messageId: data.sid });
    await emitEvent("mensaje_enviado_wa", entity.id, { phone, message, messageId: data.sid }, "sdr");

    res.json({ ok: true, messageId: data.sid, lead: leadName, entity_id: entity.id });
  } catch (err) { next(err); }
});

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
        if (!d.error_code) {
          const entity = await getOrCreateEntity({ telefono: lead.phone, nombre: lead.name || lead.nombre, tipo: ["cliente"] });
          await saveMemory(entity.id, "assistant", msg, "revenue", { messageId: d.sid, bulk: true });
        }
        results.push({ phone: lead.phone, ok: !d.error_code, id: d.sid, error: d.message });
      } catch (e) { results.push({ phone: lead.phone, ok: false, error: e.message }); }
      await new Promise(r => setTimeout(r, 1000));
    }
    res.json({ sent: results.filter(r => r.ok).length, failed: results.filter(r => !r.ok).length, results });
  } catch (err) { next(err); }
});

app.post("/messages/test", async (req, res, next) => {
  try {
    const { phone, message = "¡Hola desde PrismDB v2! 🚀 WhatsApp Revenue OS™ activo." } = req.body;
    if (!phone) return res.status(400).json({ error: "phone requerido" });
    const data = await sendWhatsApp(phone, message);
    if (data.error_code) throw new Error(`Twilio error ${data.error_code}: ${data.message}`);
    res.json({ ok: true, messageId: data.sid, to: data.to, status: data.status });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  3. IA — Mensajes y scoring (original)
// ════════════════════════════════════════════════════════════
app.post("/ai/message", async (req, res, next) => {
  try {
    const { lead, prompt, businessContext = "" } = req.body;
    const message = await claude(
      [{ role: "user", content: prompt || `Genera mensaje para: ${lead.nombre}, ${lead.cargo} en ${lead.empresa} (${lead.ciudad}).` }],
      `Eres experto en ventas B2B para LATAM. Genera mensajes WhatsApp personalizados, directos. Máximo 160 caracteres. Contexto: ${businessContext}`
    );
    res.json({ message, lead });
  } catch (err) { next(err); }
});

app.post("/ai/score", async (req, res, next) => {
  try {
    const { profile, criteria } = req.body;
    const result = await claudeJSON(`Score 0-100. SOLO JSON: {"score":85,"nivel":"Alto","razones":[],"recomendacion":"..."}
Perfil: ${JSON.stringify(profile)} Criterios: ${JSON.stringify(criteria)}`, "claude-haiku-4-5-20251001", 300);
    res.json(result);
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  4. CRM Pipeline (persistente con PostgreSQL)
// ════════════════════════════════════════════════════════════
const pipelineInMemory = { contacto: [], calificado: [], negociacion: [], cerrado: [] };

app.get("/crm/pipeline", async (_req, res, next) => {
  try {
    if (!db) return res.json(pipelineInMemory);
    const { rows } = await db.query(`
      SELECT p.*, e.nombre, e.empresa, e.telefono, e.score_wrp
      FROM pipeline p LEFT JOIN entities e ON p.entity_id = e.id
      WHERE p.modulo = 'ventas' ORDER BY p.updated_at DESC
    `);
    const byStage = { contacto: [], calificado: [], negociacion: [], cerrado: [] };
    rows.forEach(r => { (byStage[r.stage] ||= []).push(r); });
    res.json(byStage);
  } catch (err) { next(err); }
});

app.post("/crm/lead", async (req, res, next) => {
  try {
    const lead = { ...req.body, id: Date.now().toString(), created_at: new Date().toISOString() };
    if (db) {
      const entity = await getOrCreateEntity({ nombre: lead.nombre, empresa: lead.empresa, telefono: lead.telefono, tipo: ["prospecto"] });
      await db.query(
        `INSERT INTO pipeline (entity_id, modulo, stage, valor, probabilidad) VALUES ($1,'ventas',$2,$3,$4)`,
        [entity.id, lead.stage || "contacto", lead.valor || 0, lead.probabilidad || 50]
      );
    } else {
      (pipelineInMemory[lead.stage || "contacto"] ||= []).push(lead);
    }
    res.status(201).json(lead);
  } catch (err) { next(err); }
});

app.patch("/crm/lead/:id/move", async (req, res, next) => {
  try {
    const { id } = req.params; const { to } = req.body;
    if (db) {
      await db.query("UPDATE pipeline SET stage = $1, updated_at = NOW() WHERE id = $2", [to, id]);
      return res.json({ ok: true, id, stage: to });
    }
    for (const stage of Object.keys(pipelineInMemory)) {
      const idx = pipelineInMemory[stage].findIndex(l => l.id === id);
      if (idx !== -1) {
        const [lead] = pipelineInMemory[stage].splice(idx, 1);
        (pipelineInMemory[to] ||= []).push({ ...lead, stage: to });
        return res.json({ ok: true, lead: { ...lead, stage: to } });
      }
    }
    res.status(404).json({ error: "Lead no encontrado" });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  5. TALENT SCANNER (original + memoria)
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
        const candidate = await claudeJSON(`Analiza candidato. SOLO JSON:
{"nombre":"...","cargo_actual":"...","empresa_actual":"...","ubicacion":"...","telefono":null,"email":null,"anos_experiencia":5,"skills_detectados":[],"score":85,"resumen":"2 oraciones","senal_apertura":"..."}
Cargo: ${cargo} | Exp: ${experiencia} | Skills: ${skillsStr} | Loc: ${ubicacion}
Perfil: ${item.markdown?.slice(0, 1000) || item.description || item.title || ""}`, "claude-haiku-4-5-20251001", 400);

        // Guardar en memoria
        if (candidate.nombre && candidate.nombre !== "...") {
          await getOrCreateEntity({ ...candidate, nombre: candidate.nombre, telefono: candidate.telefono, tipo: ["candidato"], agente: "talent" });
        }

        return { ...candidate, url: item.url, fuente: new URL(item.url).hostname };
      })
    );
    res.json({ candidates: scored.filter(c => (c.score || 0) >= scoreMin), total: scored.length, cargo, ubicacion });
  } catch (err) { next(err); }
});

app.post("/talent/match", async (req, res, next) => {
  try {
    const { candidate, requirements } = req.body;
    const match = await claudeJSON(`Match candidato-cargo. SOLO JSON:
{"score":84,"nivel":"Alto","tiene":[],"falta":[],"destacados":[],"brechas_criticas":[],"recomendacion":"...","mensaje_personalizado":"120 chars max"}
CANDIDATO: ${JSON.stringify(candidate)}
REQUISITOS: ${JSON.stringify(requirements)}`, "claude-sonnet-4-20250514", 600);
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

    const entity = await getOrCreateEntity({ nombre: candidateName, telefono: phone, tipo: ["candidato"] });
    await saveMemory(entity.id, "assistant", msg, "talent", { matchScore, cargo });
    await emitEvent("candidato_contactado", entity.id, { phone, cargo, matchScore }, "talent");

    res.json({ ok: true, messageId: data.sid, candidate: candidateName, mensaje: msg, entity_id: entity.id });
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
//  6. MERCADO PAGO (original)
// ════════════════════════════════════════════════════════════
const MP_ACCESS_TOKEN = process.env.MP_ACCESS_TOKEN || 'TEST-748633123758950-051218-c76f920ae7147ecd648d1d9225667eba-503051039';
const FRONTEND_URL    = process.env.FRONTEND_URL || 'https://prismdb-chi.vercel.app';

const PLANES_MP = {
  semilla:    { nombre: 'PrismDB Semilla',        precio: 99   },
  starter:    { nombre: 'PrismDB Starter',        precio: 249  },
  pro:        { nombre: 'PrismDB Pro',            precio: 599  },
  enterprise: { nombre: 'PrismDB Enterprise',     precio: 1299 },
  talent:     { nombre: 'PrismDB Talent Scanner', precio: 299  },
  business:   { nombre: 'PrismDB Business',       precio: 599  },
  aifirst:    { nombre: 'PrismDB AI-First OS',    precio: 1499 },
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
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${MP_ACCESS_TOKEN}` },
      body: JSON.stringify(body),
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
      console.log(`[WEBHOOK MP] ${payment.status} — ${payment.payer?.email} — $${payment.transaction_amount}`);
      await emitEvent("pago_recibido", payment.payer?.email, { plan: payment.metadata?.plan, monto: payment.transaction_amount, status: payment.status }, "finance");
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
//  7. CATÁLOGO (original)
// ════════════════════════════════════════════════════════════
app.post("/catalog/extract", async (req, res, next) => {
  try {
    const { url } = req.body;
    if (!url) return res.status(400).json({ error: "url requerida" });
    const fcRes = await fetch("https://api.firecrawl.dev/v1/scrape", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.FIRECRAWL_API_KEY}` },
      body: JSON.stringify({ url, formats: ["markdown", "screenshot"] }),
    });
    const fcData = await fcRes.json();
    const content = fcData.data?.markdown || fcData.markdown || "";
    const product = await claudeJSON(`Analiza contenido de redes sociales y extrae el producto. SOLO JSON:
{"nombre":"nombre del producto","precio":"precio con moneda o null","descripcion":"1-2 oraciones","imagen_url":"url o null"}
Contenido: ${content.slice(0, 2000)}`, "claude-haiku-4-5-20251001", 300);
    res.json({ ...product, url, fuente: new URL(url).hostname });
  } catch (err) { next(err); }
});

// ════════════════════════════════════════════════════════════
//  8. EVENTS — Ver eventos del Revenue OS
// ════════════════════════════════════════════════════════════
app.get("/events", async (req, res, next) => {
  try {
    if (!db) return res.json({ events: [], note: "PostgreSQL requerido" });
    const limit = parseInt(req.query.limit) || 50;
    const { rows } = await db.query(
      "SELECT * FROM events ORDER BY created_at DESC LIMIT $1", [limit]
    );
    res.json({ events: rows, total: rows.length });
  } catch (err) { next(err); }
});

// ── Error handler ─────────────────────────────────────────
app.use((err, _req, res, _next) => {
  console.error("[ERROR]", err.message);
  res.status(500).json({ error: err.message || "Error interno" });
});

// ── Start ─────────────────────────────────────────────────
initMemory().then(() => {
  app.listen(PORT, () => {
    console.log(`✅ PrismDB v2.0 — WhatsApp Revenue OS™`);
    console.log(`   Puerto: ${PORT}`);
    console.log(`   Memory: ${db ? "PostgreSQL ✅" : "In-Memory ⚠️"}`);
    console.log(`   Agentes: SDR · Revenue · Talent · Finance ✅`);
    console.log(`   Rutas: Firecrawl · Twilio · Anthropic · MercadoPago ✅`);
  });
});
