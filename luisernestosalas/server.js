import express from "express";
import cors from "cors";
import helmet from "helmet";
import dotenv from "dotenv";
import pg from "pg";
import { processEvent, startEventBus } from "./src/server/router.js";
import { initAuditTable } from "./src/server/audit-layer.js";
import { initGraphTables } from "./src/server/enterprise-graph.js";
import { initVectorTables } from "./src/server/pgvector.js";
import { setupVentasActivasRoutes } from "./src/server/ventas-activas.js";
import { setupRRHHRoutes } from "./src/server/rrhh.js";
import { setupAIProxyRoutes } from "./src/server/ai-proxy.js";   // ← NUEVO
import { claudeChat, sendWhatsApp } from "./src/server/utils.js";

dotenv.config();

const app  = express();
const PORT = parseInt(process.env.PORT || "3000", 10);

// ── Seguridad ────────────────────────────────────────────────
app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
}));

// Solo acepta peticiones del frontend — cambia en Railway por tu dominio real
const ALLOWED_ORIGINS = (process.env.FRONTEND_URL || "*").split(",").map(s => s.trim());
app.use(cors({
  origin: (origin, cb) => {
    if (!origin || ALLOWED_ORIGINS.includes("*") || ALLOWED_ORIGINS.includes(origin)) {
      cb(null, true);
    } else {
      cb(new Error(`CORS: origen no permitido — ${origin}`));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "X-PrismIA-Client", "Authorization"],
}));

app.use(express.json({ limit: "2mb" }));

// ── Base de datos ────────────────────────────────────────────
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
      memoryMode = "postgresql";
      await initAuditTable(db, memoryMode);
      await initGraphTables(db, memoryMode);
      await initVectorTables(db, memoryMode);

      // Rutas de negocio
      setupVentasActivasRoutes(app, db, memoryMode, claudeChat, sendWhatsApp);
      setupRRHHRoutes(app, db);

      client.release();
      console.log("✅ PostgreSQL conectado");
      startEventBus(db, 60000);
    })
    .catch((err) => {
      console.error("⚠️ PostgreSQL error:", err.message);
    });
}

// ── AI Proxy — debe ir SIEMPRE, con o sin DB ─────────────────
setupAIProxyRoutes(app, db);

// ── Health check ─────────────────────────────────────────────
app.get("/health", (_req, res) =>
  res.json({ ok: true, app: "prismia", version: "3.0", memory: memoryMode, ts: Date.now() })
);

// ── Error handler global ──────────────────────────────────────
app.use((err, _req, res, _next) => {
  console.error("[ERROR]", err.message);
  res.status(err.status || 500).json({ error: err.message || "Error interno" });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ PrismIA v3.0 corriendo en http://localhost:${PORT}`);
  console.log(`🔒 AI Proxy activo — las keys nunca salen del servidor`);
});
