// ═══════════════════════════════════════════════════════════
//  PrismDB — AI Router v1.0
//  Event Bus · Autonomy Matrix · Agent Orchestration
//  
//  Flujo:
//  Evento → Clasificación → Matriz autonomía → Agente → Memoria
// ═══════════════════════════════════════════════════════════

import pg from "pg";

// ── CONFIG ────────────────────────────────────────────────
const ANTHROPIC_KEY  = process.env.ANTHROPIC_API_KEY;
const TWILIO_SID     = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_TOKEN   = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_FROM    = process.env.TWILIO_WHATSAPP_FROM || "whatsapp:+14155238886";
const NOTIFY_PHONE   = process.env.NOTIFY_PHONE; // número del responsable humano

// ══════════════════════════════════════════════════════════
//  TIPOS DE EVENTOS
//  Cada fuente tiene su schema de entrada
// ══════════════════════════════════════════════════════════
export const EVENT_TYPES = {
  // WhatsApp
  WHATSAPP_INBOUND:    "whatsapp.inbound",       // cliente escribió
  WHATSAPP_NO_REPLY:   "whatsapp.no_reply",      // no respondió en X horas

  // Ventas / CRM
  LEAD_NEW:            "lead.new",               // lead nuevo cargado
  LEAD_QUALIFIED:      "lead.qualified",         // SDR calificó lead
  DEAL_STALLED:        "deal.stalled",           // deal sin movimiento
  DEAL_AT_RISK:        "deal.at_risk",           // probabilidad bajó
  DEAL_CLOSED:         "deal.closed",            // deal cerrado

  // Pagos
  PAYMENT_SUCCESS:     "payment.success",        // pago exitoso
  PAYMENT_FAILED:      "payment.failed",         // pago fallido
  PAYMENT_OVERDUE:     "payment.overdue",        // pago vencido

  // Ventas activas
  CAMPAIGN_TRIGGER:    "campaign.trigger",       // campaña programada
  CLIENT_INACTIVE:     "client.inactive",        // cliente inactivo +14d
  CATALOG_REQUEST:     "catalog.request",        // pidió catálogo

  // Talento / RRHH
  CANDIDATE_REPLIED:   "candidate.replied",      // candidato respondió
  CANDIDATE_GHOSTED:   "candidate.ghosted",      // candidato no respondió
  VACANCY_OPENED:      "vacancy.opened",         // nueva vacante

  // Finanzas
  REVENUE_ALERT:       "revenue.alert",          // revenue por debajo de meta
  FORECAST_READY:      "forecast.ready",         // predicción generada

  // Sistema
  SCHEDULED_TASK:      "system.scheduled",       // tarea programada
  MANUAL_TRIGGER:      "system.manual",          // trigger manual por humano
};

// ══════════════════════════════════════════════════════════
//  MATRIZ DE AUTONOMÍA
//  Define si el router actúa solo, actúa+notifica, o espera
//
//  Dimensiones:
//  - urgency:    high / medium / low
//  - confidence: high / medium / low
//  - risk:       high / medium / low  (riesgo económico o reputacional)
//
//  Resultado:
//  - "auto"     → agente actúa solo
//  - "notify"   → agente actúa + notifica al humano
//  - "approve"  → notifica y espera aprobación antes de actuar
// ══════════════════════════════════════════════════════════
export function autonomyDecision({ urgency, confidence, risk, amount = 0 }) {
  // Riesgo económico alto → siempre pedir aprobación
  if (amount > 2000)            return "approve";
  if (risk === "high")          return "approve";

  // Alta urgencia + alta confianza → actúa solo
  if (urgency === "high" && confidence === "high" && risk === "low")
                                return "auto";

  // Alta urgencia pero baja confianza → actúa + notifica
  if (urgency === "high" && confidence === "low")
                                return "notify";

  // Confianza alta, urgencia normal → actúa solo
  if (confidence === "high" && risk === "low")
                                return "auto";

  // Confianza media → actúa + notifica
  if (confidence === "medium")  return "notify";

  // Baja confianza o riesgo medio → notifica, espera
  return "approve";
}

// ══════════════════════════════════════════════════════════
//  CLASIFICADOR DE EVENTOS
//  Claude analiza el evento y decide:
//  - qué agente actúa
//  - urgencia / confianza / riesgo
//  - acción recomendada
// ══════════════════════════════════════════════════════════
export async function classifyEvent(event, memory = {}) {
  const prompt = `Eres el AI Router de PrismDB — el cerebro central del sistema operativo empresarial.

Analiza este evento y decide cómo responder. Devuelve SOLO JSON válido.

AGENTES DISPONIBLES:
- sdr: prospección, primer contacto, calificación de leads nuevos
- revenue: ventas activas, seguimiento, manejo de objeciones, cierre, deals en riesgo
- talent: reclutamiento, candidatos, vacantes
- finance: alertas financieras, forecasts, pagos, revenue
- campaign: campañas masivas, catálogos, clientes inactivos
- none: evento informativo, no requiere acción de agente

REGLAS:
- Si hay riesgo económico >$1000, sube el risk a "high"
- Si el cliente ya compró antes, sube confidence
- Si es primer contacto, baja confidence
- Pagos fallidos siempre son urgency "high"
- Deals cerrados siempre notifican a humano

EVENTO:
${JSON.stringify(event, null, 2)}

MEMORIA DEL CONTACTO:
${JSON.stringify(memory, null, 2)}

RESPONDE CON ESTE JSON EXACTO:
{
  "agent": "sdr|revenue|talent|finance|campaign|none",
  "urgency": "high|medium|low",
  "confidence": "high|medium|low",
  "risk": "high|medium|low",
  "amount": 0,
  "action": "descripción de qué debe hacer el agente en una frase",
  "reason": "por qué este agente y no otro",
  "context_needed": ["lista", "de", "datos", "relevantes", "de", "memoria"],
  "notify_message": "mensaje corto para notificar al humano si aplica"
}`;

  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 600,
      messages: [{ role: "user", content: prompt }],
    }),
  });

  const data = await res.json();
  const text = data.content?.[0]?.text || "{}";
  return JSON.parse(text.replace(/```json|```/g, "").trim());
}

// ══════════════════════════════════════════════════════════
//  EJECUTORES DE AGENTES
//  Cada agente recibe el evento + contexto completo
// ══════════════════════════════════════════════════════════
async function runAgent(agentName, event, memory, classification) {
  const systemPrompts = {
    sdr: `Eres el Agente SDR de PrismDB. Especialista en prospección B2B para LATAM.
Objetivo: calificar leads, generar interés, conseguir reunión o demo.
Tono: directo, cálido, profesional. WhatsApp: máximo 160 chars.
Usa el historial del contacto para personalizar cada mensaje.`,

    revenue: `Eres el Agente Revenue de PrismDB. Especialista en cierre de ventas B2B.
Objetivo: avanzar deals al cierre, manejar objeciones, generar urgencia.
Tienes el historial completo. Cada mensaje debe tener valor claro.
WhatsApp: máximo 200 chars.`,

    talent: `Eres el Agente Talent de PrismDB. Especialista en reclutamiento.
Objetivo: identificar candidatos, generar interés en vacantes.
Tono: empático, motivador, claro sobre la oportunidad.
WhatsApp: máximo 180 chars.`,

    finance: `Eres el Agente Finance de PrismDB. Especialista en análisis financiero.
Objetivo: generar alertas, forecasts y recomendaciones de revenue.
Tono: preciso, ejecutivo, orientado a acción.
Responde con análisis claro y acción recomendada.`,

    campaign: `Eres el Agente Campaign de PrismDB. Especialista en ventas activas y campañas.
Objetivo: reactivar clientes, enviar catálogos personalizados, gestionar pedidos.
Tono: comercial, personalizado, con propuesta de valor clara.
WhatsApp: máximo 200 chars.`,
  };

  const userContent = `EVENTO: ${JSON.stringify(event)}
HISTORIAL: ${JSON.stringify(memory)}
ACCIÓN REQUERIDA: ${classification.action}
CONTEXTO CLAVE: ${classification.context_needed?.join(", ")}

Genera la respuesta/acción. Si es un mensaje WhatsApp, devuelve SOLO el texto del mensaje.
Si es un análisis o reporte, devuelve el contenido completo.`;

  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: agentName === "finance" ? "claude-sonnet-4-20250514" : "claude-haiku-4-5-20251001",
      max_tokens: agentName === "finance" ? 1000 : 400,
      system: systemPrompts[agentName] || systemPrompts.sdr,
      messages: [{ role: "user", content: userContent }],
    }),
  });

  const data = await res.json();
  return data.content?.[0]?.text || "";
}

// ══════════════════════════════════════════════════════════
//  NOTIFICADOR HUMANO
//  Avisa al responsable cuando el router decide notificar
// ══════════════════════════════════════════════════════════
async function notifyHuman(message, event, classification) {
  if (!NOTIFY_PHONE) return { ok: false, reason: "NOTIFY_PHONE no configurado" };

  const text = `🤖 *PrismDB Router*\n\n` +
    `*Evento:* ${event.type}\n` +
    `*Entidad:* ${event.entityId || "—"}\n` +
    `*Agente:* ${classification.agent.toUpperCase()}\n` +
    `*Urgencia:* ${classification.urgency}\n\n` +
    `${classification.notify_message || message}\n\n` +
    `_${new Date().toLocaleString("es-CO")}_`;

  const credentials = Buffer.from(`${TWILIO_SID}:${TWILIO_TOKEN}`).toString("base64");
  const phone = NOTIFY_PHONE.startsWith("whatsapp:") ? NOTIFY_PHONE : `whatsapp:+57${NOTIFY_PHONE.replace(/\D/g, "")}`;

  const res = await fetch(
    `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`,
    {
      method: "POST",
      headers: { Authorization: `Basic ${credentials}`, "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ From: TWILIO_FROM, To: phone, Body: text }),
    }
  );
  return await res.json();
}

// ══════════════════════════════════════════════════════════
//  SEND WHATSAPP
// ══════════════════════════════════════════════════════════
async function sendWhatsApp(to, message) {
  const phone = to.startsWith("whatsapp:") ? to : `whatsapp:+57${to.replace(/\D/g, "")}`;
  const credentials = Buffer.from(`${TWILIO_SID}:${TWILIO_TOKEN}`).toString("base64");
  const res = await fetch(
    `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`,
    {
      method: "POST",
      headers: { Authorization: `Basic ${credentials}`, "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ From: TWILIO_FROM, To: phone, Body: message }),
    }
  );
  return await res.json();
}

// ══════════════════════════════════════════════════════════
//  AI ROUTER — FUNCIÓN PRINCIPAL
//  Orquesta todo el flujo completo
// ══════════════════════════════════════════════════════════
export async function processEvent(event, db = null) {
  const startTime = Date.now();
  const result = {
    event_id: `evt_${Date.now()}`,
    event_type: event.type,
    entity_id: event.entityId,
    timestamp: new Date().toISOString(),
    classification: null,
    autonomy: null,
    agent_response: null,
    actions_taken: [],
    memory_updated: false,
    duration_ms: 0,
  };

  try {
    // 1. Leer memoria del contacto
    let memory = {};
    if (db) {
      const r = await db.query("SELECT data FROM memory WHERE entity_id = $1", [event.entityId]).catch(() => ({ rows: [] }));
      memory = r.rows[0]?.data || {};
    }

    // 2. Clasificar el evento con Claude
    result.classification = await classifyEvent(event, memory);
    const cls = result.classification;

    // Si no hay agente que actúe, registrar y salir
    if (cls.agent === "none") {
      result.actions_taken.push("event_logged");
      result.duration_ms = Date.now() - startTime;
      await logRouterEvent(db, result);
      return result;
    }

    // 3. Decidir autonomía
    result.autonomy = autonomyDecision({
      urgency: cls.urgency,
      confidence: cls.confidence,
      risk: cls.risk,
      amount: cls.amount || 0,
    });

    // 4. Si necesita aprobación → solo notificar y esperar
    if (result.autonomy === "approve") {
      await notifyHuman(
        `Evento requiere tu aprobación: ${cls.action}`,
        event,
        cls
      ).catch(() => {});
      result.actions_taken.push("human_notified_awaiting_approval");
      result.duration_ms = Date.now() - startTime;
      await logRouterEvent(db, result);
      return result;
    }

    // 5. Ejecutar el agente
    result.agent_response = await runAgent(cls.agent, event, memory, cls);

    // 6. Acciones según tipo de evento
    const whatsappAgents = ["sdr", "revenue", "talent", "campaign"];
    const shouldSendWA = whatsappAgents.includes(cls.agent) &&
      (event.type.startsWith("whatsapp.") || event.phone || event.entityId?.includes("57"));

    if (shouldSendWA && result.agent_response) {
      const phone = event.phone || event.entityId;
      if (phone) {
        await sendWhatsApp(phone, result.agent_response).catch(() => {});
        result.actions_taken.push("whatsapp_sent");
      }
    }

    // 7. Notificar al humano si autonomy === "notify"
    if (result.autonomy === "notify") {
      await notifyHuman(result.agent_response, event, cls).catch(() => {});
      result.actions_taken.push("human_notified");
    }

    // 8. Actualizar memoria
    if (db && event.entityId) {
      const newMemory = {
        ...memory,
        last_event: event.type,
        last_agent: cls.agent,
        last_action: cls.action,
        last_response: result.agent_response?.slice(0, 200),
        [`${cls.agent}_last_run`]: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };

      // Métricas específicas por tipo de evento
      if (event.type === "payment.success")  newMemory.total_paid = (memory.total_paid || 0) + (event.amount || 0);
      if (event.type === "deal.closed")      newMemory.deals_closed = (memory.deals_closed || 0) + 1;
      if (event.type === "whatsapp.inbound") newMemory.messages_received = (memory.messages_received || 0) + 1;

      await db.query(
        `INSERT INTO memory (entity_id, entity_type, data)
         VALUES ($1, $2, $3)
         ON CONFLICT (entity_id) DO UPDATE SET data = $3, updated_at = NOW()`,
        [event.entityId, event.entityType || "contact", newMemory]
      ).catch(() => {});

      result.memory_updated = true;
    }

    result.actions_taken.push(`agent_${cls.agent}_executed`);
    result.duration_ms = Date.now() - startTime;
    await logRouterEvent(db, result);
    return result;

  } catch (err) {
    result.error = err.message;
    result.duration_ms = Date.now() - startTime;
    await logRouterEvent(db, result).catch(() => {});
    throw err;
  }
}

// ══════════════════════════════════════════════════════════
//  LOGGER DE EVENTOS DEL ROUTER
// ══════════════════════════════════════════════════════════
async function logRouterEvent(db, result) {
  if (!db) return;
  await db.query(
    "INSERT INTO events (type, entity_id, payload) VALUES ($1, $2, $3)",
    ["router.processed", result.entity_id, result]
  ).catch(() => {});
}

// ══════════════════════════════════════════════════════════
//  EVENT BUS — SCHEDULER INTERNO
//  Genera eventos proactivos periódicamente
// ══════════════════════════════════════════════════════════
export function startEventBus(db, intervalMs = 60000) {
  console.log("🚌 Event Bus iniciado — revisando cada", intervalMs / 1000, "segundos");

  setInterval(async () => {
    if (!db) return;
    const now = new Date();

    try {
      // 1. Detectar clientes inactivos (sin contacto en 14 días)
      const inactive = await db.query(`
        SELECT entity_id, data FROM memory
        WHERE entity_type = 'contact'
          AND (data->>'last_event') IS NOT NULL
          AND updated_at < NOW() - INTERVAL '14 days'
          AND (data->>'client_inactive_notified') IS DISTINCT FROM 'true'
        LIMIT 10
      `).catch(() => ({ rows: [] }));

      for (const row of inactive.rows) {
        await processEvent({
          type: EVENT_TYPES.CLIENT_INACTIVE,
          entityId: row.entity_id,
          entityType: "contact",
          phone: row.data?.phone,
          data: row.data,
          triggeredBy: "event_bus",
        }, db).catch(console.error);

        // Marcar como notificado para no repetir
        await db.query(
          `UPDATE memory SET data = data || '{"client_inactive_notified":"true"}', updated_at = NOW()
           WHERE entity_id = $1`, [row.entity_id]
        ).catch(() => {});
      }

      // 2. Detectar deals estancados (sin movimiento en 5 días)
      const stalled = await db.query(`
        SELECT entity_id, data FROM memory
        WHERE entity_type IN ('lead', 'deal')
          AND data->>'stage' IN ('calificado', 'negociacion')
          AND updated_at < NOW() - INTERVAL '5 days'
          AND (data->>'stalled_notified') IS DISTINCT FROM 'true'
        LIMIT 10
      `).catch(() => ({ rows: [] }));

      for (const row of stalled.rows) {
        await processEvent({
          type: EVENT_TYPES.DEAL_STALLED,
          entityId: row.entity_id,
          entityType: "deal",
          phone: row.data?.phone,
          data: row.data,
          triggeredBy: "event_bus",
        }, db).catch(console.error);

        await db.query(
          `UPDATE memory SET data = data || '{"stalled_notified":"true"}', updated_at = NOW()
           WHERE entity_id = $1`, [row.entity_id]
        ).catch(() => {});
      }

      // 3. Detectar candidatos sin respuesta (48h)
      const ghosted = await db.query(`
        SELECT entity_id, data FROM memory
        WHERE entity_type = 'candidate'
          AND data->>'talent_stage' = 'contactado'
          AND updated_at < NOW() - INTERVAL '48 hours'
          AND (data->>'ghosted_notified') IS DISTINCT FROM 'true'
        LIMIT 10
      `).catch(() => ({ rows: [] }));

      for (const row of ghosted.rows) {
        await processEvent({
          type: EVENT_TYPES.CANDIDATE_GHOSTED,
          entityId: row.entity_id,
          entityType: "candidate",
          phone: row.data?.phone,
          data: row.data,
          triggeredBy: "event_bus",
        }, db).catch(console.error);

        await db.query(
          `UPDATE memory SET data = data || '{"ghosted_notified":"true"}', updated_at = NOW()
           WHERE entity_id = $1`, [row.entity_id]
        ).catch(() => {});
      }

      // 4. Log del ciclo del bus
      if (inactive.rows.length + stalled.rows.length + ghosted.rows.length > 0) {
        console.log(`[EVENT BUS] ${now.toISOString()} — inactivos:${inactive.rows.length} stalled:${stalled.rows.length} ghosted:${ghosted.rows.length}`);
      }

    } catch (err) {
      console.error("[EVENT BUS ERROR]", err.message);
    }
  }, intervalMs);
}

export default { processEvent, classifyEvent, autonomyDecision, startEventBus, EVENT_TYPES };
