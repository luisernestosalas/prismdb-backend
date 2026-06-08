// ═══════════════════════════════════════════════════════════
//  PrismIA — AI Proxy v1.0
//  Todas las llamadas a Claude pasan por aquí.
//  El frontend NUNCA toca Anthropic directamente.
// ═══════════════════════════════════════════════════════════

import { claudeChat } from "./utils.js";

// ── Rate limiting simple en memoria ─────────────────────────
const rateLimiter = new Map(); // ip → { count, resetAt }

function checkRateLimit(ip, maxPerMinute = 20) {
  const now = Date.now();
  const entry = rateLimiter.get(ip) || { count: 0, resetAt: now + 60_000 };
  if (now > entry.resetAt) { entry.count = 0; entry.resetAt = now + 60_000; }
  entry.count++;
  rateLimiter.set(ip, entry);
  return entry.count <= maxPerMinute;
}

// ── Endpoints permitidos — whitelist de operaciones ─────────
// Cada endpoint define qué puede pedir el frontend.
// El frontend envía datos, el backend construye el prompt.
// Nunca se expone el system prompt ni la key.

const ENDPOINTS = {

  // Módulo: Prospección — generar prospectos calificados
  "prospectos/generar": async ({ query, cargo, ciudad, scoreMin = 70 }) => {
    if (!query || !cargo || !ciudad) throw new Error("query, cargo y ciudad son requeridos");
    const prompt = `Eres un experto en prospección B2B en Colombia y LATAM.
Genera exactamente 6 prospectos calificados para:
- Sector/tipo de empresa: "${query}"
- Cargo del decisor: "${cargo}"
- Ciudad: "${ciudad}"
- Score mínimo ICP: ${scoreMin}

Para cada prospecto incluye datos realistas y creíbles para empresas colombianas.
Responde SOLO con JSON válido, sin texto adicional, sin backticks:
[{
  "nombre": "Nombre completo real",
  "cargo": "${cargo}",
  "empresa": "Nombre empresa real del sector",
  "ciudad": "${ciudad}",
  "score": 85,
  "telefono": "300XXXXXXX",
  "email": "nombre@empresa.com",
  "insight": "Señal de compra específica detectada",
  "signals": [{"cls":"sig-job","txt":"Oferta de empleo activa"}]
}]
Solo incluye prospectos con score >= ${scoreMin}.`;
    return await claudeChat("Experto en prospección B2B Colombia.", prompt, "claude-haiku-4-5-20251001", 1500);
  },

  // Módulo: Ventas — generar mensaje de contacto personalizado
  "ventas/mensaje": async ({ nombre, empresa, cargo, ciudad, insight, tono = "consultivo", contexto = "", canal = "whatsapp" }) => {
    if (!nombre || !empresa) throw new Error("nombre y empresa son requeridos");
    const tonoDesc = { consultivo: "educativo y de valor", directo: "al grano sin rodeos", cercano: "amigable y casual" }[tono] || "consultivo";
    const maxChars = canal === "whatsapp" ? 160 : canal === "sms" ? 140 : 300;
    const prompt = `Eres un SDR experto en ventas B2B colombiano.
Crea un mensaje de ${canal} para:
- Nombre: ${nombre} | Cargo: ${cargo} | Empresa: ${empresa} | Ciudad: ${ciudad}
- Señal detectada: ${insight || "empresa en crecimiento"}
- Contexto de lo que vendemos: ${contexto || "Prisma OS automatiza la operación completa desde $599/mes"}
- Tono: ${tonoDesc}
- Máximo ${maxChars} caracteres. Termina con pregunta cerrada (sí/no).
Responde SOLO el mensaje, sin explicaciones ni comillas.`;
    return await claudeChat("SDR experto en ventas B2B.", prompt, "claude-haiku-4-5-20251001", 300);
  },

  // Módulo: PrismDB — SDR Agent, prospectos con señal de dolor
  "prismdb/buscar": async ({ sector, ciudad, dolor, canal = "WhatsApp" }) => {
    if (!sector || !ciudad) throw new Error("sector y ciudad son requeridos");
    const prompt = `Eres PrismDB, el motor de prospección de Prisma OS.
Genera exactamente 5 prospectos realistas para:
- Sector: ${sector} | Ciudad: ${ciudad}
- Señal de dolor a detectar: ${dolor || "caos operativo"}
- Canal de contacto: ${canal}

Responde SOLO el array JSON sin markdown:
[{
  "empresa": "Nombre S.A.S.",
  "contacto": "Nombre Apellido",
  "cargo": "Gerente Comercial",
  "score": 87,
  "senal": "Señal de dolor específica detectada en actividad pública",
  "mensaje": "Mensaje de ${canal} personalizado máx 120 chars con el dolor detectado",
  "ciudad": "${ciudad}"
}]`;
    return await claudeChat("Motor de prospección B2B inteligente.", prompt, "claude-haiku-4-5-20251001", 1200);
  },

  // Módulo: Onboarding — generar MVV de la empresa
  "onboarding/mvv": async ({ empresa, sector, ciudad, tamano, pain }) => {
    if (!empresa) throw new Error("empresa es requerido");
    const prompt = `Genera la Misión, Visión y Valores para esta empresa:
Empresa: ${empresa} | Sector: ${sector || "servicios"} | Ciudad: ${ciudad || "Colombia"}
Tamaño: ${tamano || "pyme"} | Dolor principal: ${pain || "operación desorganizada"}

Responde SOLO en este formato JSON exacto sin markdown:
{
  "mision": "texto máx 2 líneas para qué existe y a quién sirve",
  "vision": "texto máx 2 líneas dónde quiere estar en 5 años",
  "valores": "3 o 4 valores separados por ' · ' específicos y no genéricos"
}`;
    return await claudeChat("Consultor estratégico empresarial.", prompt, "claude-haiku-4-5-20251001", 400);
  },

  // Módulo: Ventas masivas — generar mensaje de campaña para catálogo
  "campanas/mensaje": async ({ producto, precio, descripcion, segmento, tono = "cercano", contextoEmpresa = "" }) => {
    if (!producto) throw new Error("producto es requerido");
    const prompt = `Experto en ventas por WhatsApp. Crea mensaje de campaña masiva.
Producto: ${producto} | Precio: ${precio || "consultar"} | Descripción: ${descripcion || ""}
Segmento objetivo: ${segmento || "clientes generales"}
Contexto empresa: ${contextoEmpresa}
Tono: ${tono}. Máximo 120 caracteres. Con emoji. Termina con CTA claro.
Responde SOLO el mensaje.`;
    return await claudeChat("Experto en ventas por WhatsApp y campañas masivas.", prompt, "claude-haiku-4-5-20251001", 200);
  },

  // Módulo: CEO — análisis inteligente de KPIs
  "ceo/analisis": async ({ ventas, gastos, leads, empleados, periodo = "este mes" }) => {
    const prompt = `Eres el CFO/CEO de una empresa colombiana. Analiza estos KPIs:
- Ventas: ${ventas || "sin datos"} | Gastos: ${gastos || "sin datos"}
- Leads activos: ${leads || "sin datos"} | Empleados: ${empleados || "sin datos"}
- Periodo: ${periodo}

Responde SOLO en JSON:
{
  "salud": "verde|amarillo|rojo",
  "resumen": "máx 2 líneas del estado general",
  "alerta_principal": "el problema más urgente o null",
  "accion_inmediata": "qué hacer hoy",
  "proyeccion": "tendencia a 30 días"
}`;
    return await claudeChat("CFO y CEO experto en empresas colombianas.", prompt, "claude-haiku-4-5-20251001", 400);
  },
};

// ── Middleware de autenticación simple ───────────────────────
// Por ahora valida un header X-PrismIA-Client.
// En producción: JWT o session token.
function authMiddleware(req, res, next) {
  const clientHeader = req.headers["x-prismia-client"];
  if (!clientHeader || clientHeader !== "prismia-frontend-v1") {
    return res.status(401).json({ error: "No autorizado" });
  }
  next();
}

// ── Registro de llamadas (audit) ─────────────────────────────
async function logAICall(db, { endpoint, ip, tokens, ok }) {
  if (!db) return;
  await db.query(`
    INSERT INTO audit_log (trace_id, event_type, action, outcome, source, context_used)
    VALUES ($1, 'ai_proxy', $2, $3, 'frontend', $4)
  `, [
    `proxy_${Date.now()}`,
    endpoint,
    ok ? "success" : "error",
    { ip, tokens },
  ]).catch(() => {});
}

// ── Setup de rutas ───────────────────────────────────────────
export function setupAIProxyRoutes(app, db) {

  // POST /ai/:endpoint
  app.post("/ai/:module/:action", authMiddleware, async (req, res, next) => {
    const ip = req.ip || req.connection?.remoteAddress || "unknown";
    const endpoint = `${req.params.module}/${req.params.action}`;

    // Rate limit
    if (!checkRateLimit(ip, 30)) {
      return res.status(429).json({ error: "Demasiadas solicitudes. Espera 1 minuto." });
    }

    // Endpoint existe?
    const handler = ENDPOINTS[endpoint];
    if (!handler) {
      return res.status(404).json({ error: `Endpoint IA '${endpoint}' no existe` });
    }

    try {
      const raw = await handler(req.body || {});

      // Intentar parsear JSON si la respuesta lo parece
      let result;
      try {
        const clean = raw.replace(/```json|```/g, "").trim();
        result = JSON.parse(clean);
      } catch {
        result = raw; // devolver como texto si no es JSON
      }

      await logAICall(db, { endpoint, ip, tokens: raw.length, ok: true });
      res.json({ ok: true, data: result });

    } catch (err) {
      await logAICall(db, { endpoint, ip, tokens: 0, ok: false });
      console.error(`[AI PROXY ERROR] ${endpoint}:`, err.message);
      res.status(500).json({ error: err.message || "Error en el agente IA" });
    }
  });

  // GET /ai/endpoints — lista los endpoints disponibles (solo dev)
  if (process.env.NODE_ENV !== "production") {
    app.get("/ai/endpoints", (_req, res) => {
      res.json({ endpoints: Object.keys(ENDPOINTS) });
    });
  }

  console.log("🔒 AI Proxy activo — /ai/:module/:action");
}
