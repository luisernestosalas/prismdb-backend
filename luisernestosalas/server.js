const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const cors = require('cors');

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ─── HEALTH ──────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'Prisma OS Backend v1.0', ok: true }));

// ═══════════════════════════════════════════════════════════
// VENTAS — PROSPECTOS / PIPELINE
// ═══════════════════════════════════════════════════════════

// GET todos los deals del pipeline
app.get('/api/deals', async (req, res) => {
  const { data, error } = await supabase
    .from('deals')
    .select('*')
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// GET un deal por ID
app.get('/api/deals/:id', async (req, res) => {
  const { data, error } = await supabase
    .from('deals')
    .select('*')
    .eq('id', req.params.id)
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// POST crear nuevo prospecto/deal
app.post('/api/deals', async (req, res) => {
  const {
    empresa, contacto, email, telefono, cargo,
    valor_mensual, moneda, etapa, canal_origen,
    notas, score_mvv, ciudad
  } = req.body;

  const { data, error } = await supabase
    .from('deals')
    .insert([{
      empresa, contacto, email, telefono, cargo,
      valor_mensual: valor_mensual || 0,
      moneda: moneda || 'USD',
      etapa: etapa || 'prospecto',
      canal_origen: canal_origen || 'manual',
      notas, score_mvv: score_mvv || 50,
      ciudad,
      probabilidad: calcProbabilidad(etapa || 'prospecto')
    }])
    .select()
    .single();

  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json(data);
});

// PATCH actualizar etapa / datos del deal
app.patch('/api/deals/:id', async (req, res) => {
  const updates = { ...req.body, updated_at: new Date().toISOString() };
  if (updates.etapa) updates.probabilidad = calcProbabilidad(updates.etapa);

  const { data, error } = await supabase
    .from('deals')
    .update(updates)
    .eq('id', req.params.id)
    .select()
    .single();

  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// DELETE eliminar deal
app.delete('/api/deals/:id', async (req, res) => {
  const { error } = await supabase.from('deals').delete().eq('id', req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// GET métricas del pipeline (MRR, pipeline total, por etapa)
app.get('/api/deals/metrics/summary', async (req, res) => {
  const { data, error } = await supabase.from('deals').select('*');
  if (error) return res.status(500).json({ error: error.message });

  const clientes = data.filter(d => d.etapa === 'cliente');
  const pipeline = data.filter(d => d.etapa !== 'cliente' && d.etapa !== 'perdido');

  const mrr = clientes.reduce((s, d) => s + (d.valor_mensual || 0), 0);
  const pipelineTotal = pipeline.reduce((s, d) => s + (d.valor_mensual || 0), 0);

  const porEtapa = {};
  data.forEach(d => {
    porEtapa[d.etapa] = (porEtapa[d.etapa] || 0) + 1;
  });

  res.json({ mrr, pipeline_total: pipelineTotal, total_deals: data.length, por_etapa: porEtapa, clientes: clientes.length });
});

// POST registrar actividad/nota en un deal
app.post('/api/deals/:id/actividades', async (req, res) => {
  const { tipo, descripcion, resultado } = req.body;
  const { data, error } = await supabase
    .from('actividades')
    .insert([{ deal_id: req.params.id, tipo, descripcion, resultado }])
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json(data);
});

// GET actividades de un deal
app.get('/api/deals/:id/actividades', async (req, res) => {
  const { data, error } = await supabase
    .from('actividades')
    .select('*')
    .eq('deal_id', req.params.id)
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ═══════════════════════════════════════════════════════════
// CONTACTOS
// ═══════════════════════════════════════════════════════════

app.get('/api/contacts', async (req, res) => {
  const { data, error } = await supabase
    .from('contacts')
    .select('*')
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post('/api/contacts', async (req, res) => {
  const { data, error } = await supabase
    .from('contacts')
    .insert([req.body])
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json(data);
});

// ═══════════════════════════════════════════════════════════
// RRHH — EQUIPO
// ═══════════════════════════════════════════════════════════

app.get('/api/equipo', async (req, res) => {
  const { data, error } = await supabase
    .from('equipo')
    .select('*')
    .order('nombre');
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post('/api/equipo', async (req, res) => {
  const { data, error } = await supabase
    .from('equipo')
    .insert([req.body])
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json(data);
});

app.patch('/api/equipo/:id', async (req, res) => {
  const { data, error } = await supabase
    .from('equipo')
    .update({ ...req.body, updated_at: new Date().toISOString() })
    .eq('id', req.params.id)
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ═══════════════════════════════════════════════════════════
// TAREAS / ACCIONES REQUERIDAS
// ═══════════════════════════════════════════════════════════

app.get('/api/tareas', async (req, res) => {
  const { data, error } = await supabase
    .from('tareas')
    .select('*')
    .order('prioridad', { ascending: false })
    .order('fecha_vence');
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post('/api/tareas', async (req, res) => {
  const { data, error } = await supabase
    .from('tareas')
    .insert([req.body])
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json(data);
});

app.patch('/api/tareas/:id', async (req, res) => {
  const { data, error } = await supabase
    .from('tareas')
    .update(req.body)
    .eq('id', req.params.id)
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ═══════════════════════════════════════════════════════════
// ALERTAS
// ═══════════════════════════════════════════════════════════

app.get('/api/alertas', async (req, res) => {
  const { data, error } = await supabase
    .from('alertas')
    .select('*')
    .eq('activa', true)
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.patch('/api/alertas/:id/resolver', async (req, res) => {
  const { data, error } = await supabase
    .from('alertas')
    .update({ activa: false, resuelta_at: new Date().toISOString() })
    .eq('id', req.params.id)
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ─── HELPER ──────────────────────────────────────────────
function calcProbabilidad(etapa) {
  const map = {
    prospecto: 10, contactado: 25, calificado: 40,
    propuesta: 60, negociacion: 75, cierre: 90,
    cliente: 100, perdido: 0
  };
  return map[etapa] || 20;
}

// ─── START ────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Prisma OS Backend corriendo en puerto ${PORT}`));
