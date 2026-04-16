import http from 'node:http'
import { URL } from 'node:url'
import { Pool } from 'pg'

const PORT = Number(process.env.PORT ?? 3001)
const DSN = process.env.SUPABASE_DSN

if (!DSN) {
  throw new Error('SUPABASE_DSN is required.')
}

const pool = new Pool({
  connectionString: DSN,
  ssl: { rejectUnauthorized: false },
})

// ── Helpers ──────────────────────────────────────────────────────────

function writeJson(res, status, data) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,PATCH,DELETE,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  })
  res.end(JSON.stringify(data))
}

function normalizePath(pathname) {
  const p = pathname.replace(/\/+$/, '') || '/'
  if (p === '/api') return '/'
  if (p.startsWith('/api/')) return p.slice(4)
  return p
}

async function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    req.on('data', c => {
      size += c.length
      if (size > 2 * 1024 * 1024) { reject(new Error('too_large')); req.destroy(); return }
      chunks.push(c)
    })
    req.on('end', () => {
      if (!chunks.length) { resolve({}); return }
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))) }
      catch { reject(new Error('invalid_json')) }
    })
    req.on('error', reject)
  })
}

function toNum(v) { const n = Number(v); return Number.isFinite(n) ? n : 0 }
function toInt(v) { const n = parseInt(v, 10); return Number.isFinite(n) ? n : 0 }

async function auditLog(client, action, entityType, entityId, warehouseCode, productId, qtyChange, balanceAfter, notes) {
  await client.query(
    `INSERT INTO audit_log (action, entity_type, entity_id, warehouse_code, product_id, qty_change, balance_after, notes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [action, entityType, entityId, warehouseCode, productId, qtyChange, balanceAfter, notes]
  )
}

async function auditLogSimple(action, entityType, entityId, warehouseCode, productId, qtyChange, balanceAfter, notes) {
  await pool.query(
    `INSERT INTO audit_log (action, entity_type, entity_id, warehouse_code, product_id, qty_change, balance_after, notes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [action, entityType, entityId, warehouseCode, productId, qtyChange, balanceAfter, notes]
  )
}

// ── Route handler ────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  const method = req.method ?? 'GET'
  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`)
  const path = normalizePath(url.pathname)

  // CORS preflight
  if (method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,PATCH,DELETE,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    })
    res.end()
    return
  }

  try {
    // ── Health ──
    if (method === 'GET' && path === '/health') {
      return writeJson(res, 200, { ok: true })
    }

    // ── Stock (Part 1) ──
    if (method === 'GET' && path === '/stock') {
      const { rows } = await pool.query(
        'SELECT * FROM stock ORDER BY brand, title'
      )
      return writeJson(res, 200, rows)
    }

    // ── Warehouses ──
    if (method === 'GET' && path === '/warehouses') {
      const { rows } = await pool.query(`
        SELECT w.*,
          coalesce(s.total_products, 0)::int as total_products,
          coalesce(s.total_units, 0)::int as total_units,
          coalesce(s.total_value, 0)::numeric(12,2) as stock_value_gbp,
          coalesce(s.low_stock_count, 0)::int as low_stock_count
        FROM warehouses w
        LEFT JOIN LATERAL (
          SELECT
            count(*)::int as total_products,
            coalesce(sum(ws.qty_on_hand), 0)::int as total_units,
            coalesce(sum(ws.qty_on_hand * p.cost_price_gbp), 0)::numeric(12,2) as total_value,
            count(*) FILTER (WHERE ws.qty_on_hand <= ws.reorder_point)::int as low_stock_count
          FROM warehouse_stock ws
          JOIN products p ON p.id = ws.product_id
          WHERE ws.warehouse = w.code
        ) s ON true
        ORDER BY w.code
      `)
      return writeJson(res, 200, rows)
    }

    // ── Warehouse Stock ──
    const warehouseStockMatch = path.match(/^\/warehouses\/([^/]+)\/stock$/)
    if (method === 'GET' && warehouseStockMatch) {
      const code = warehouseStockMatch[1]
      const search = url.searchParams.get('search') || ''
      const filter = url.searchParams.get('filter') || ''
      const { rows } = await pool.query(`
        SELECT
          ws.id as stock_id,
          ws.qty_on_hand,
          ws.qty_reserved,
          ws.qty_on_hand - ws.qty_reserved as qty_available,
          ws.reorder_point,
          ws.max_stock,
          p.id as product_id,
          p.name,
          p.sku,
          p.barcode,
          p.brand,
          p.cost_price_gbp,
          p.sell_price_gbp,
          p.case_pack,
          p.moq,
          CASE
            WHEN ws.qty_on_hand = 0 THEN 'out_of_stock'
            WHEN ws.qty_on_hand <= ws.reorder_point THEN 'low'
            ELSE 'healthy'
          END as stock_status
        FROM warehouse_stock ws
        JOIN products p ON p.id = ws.product_id
        WHERE ws.warehouse = $1
          AND ($2 = '' OR p.name ILIKE $3 OR p.sku ILIKE $3 OR p.brand ILIKE $3)
          AND ($4 = '' OR
            ($4 = 'low' AND ws.qty_on_hand <= ws.reorder_point AND ws.qty_on_hand > 0) OR
            ($4 = 'out' AND ws.qty_on_hand = 0) OR
            ($4 = 'healthy' AND ws.qty_on_hand > ws.reorder_point))
        ORDER BY
          CASE WHEN ws.qty_on_hand = 0 THEN 0 WHEN ws.qty_on_hand <= ws.reorder_point THEN 1 ELSE 2 END,
          p.brand, p.name
      `, [code, search, `%${search}%`, filter])
      return writeJson(res, 200, rows)
    }

    // ── Inventory (legacy) ──
    if (method === 'GET' && path === '/inventory') {
      const { rows } = await pool.query(`
        SELECT coalesce(json_agg(row_to_json(r) ORDER BY r.name), '[]'::json) as inventory
        FROM (
          SELECT p.id, p.name, p.sku, p.barcode, p.brand, p.cost_price_gbp, p.sell_price_gbp,
            coalesce(json_agg(json_build_object(
              'warehouse', ws.warehouse, 'qty_on_hand', ws.qty_on_hand,
              'qty_reserved', ws.qty_reserved, 'reorder_point', ws.reorder_point
            ) ORDER BY ws.warehouse) FILTER (WHERE ws.id IS NOT NULL), '[]'::json) as warehouse_stock
          FROM products p
          LEFT JOIN warehouse_stock ws ON ws.product_id = p.id
          GROUP BY p.id
        ) r
      `)
      return writeJson(res, 200, rows[0]?.inventory ?? [])
    }

    // ── Suppliers ──
    if (method === 'GET' && path === '/suppliers') {
      const { rows } = await pool.query(`
        SELECT s.*,
          coalesce(pc.product_count, 0)::int as product_count
        FROM suppliers s
        LEFT JOIN LATERAL (
          SELECT count(*)::int as product_count FROM products WHERE supplier_id = s.id
        ) pc ON true
        ORDER BY s.name
      `)
      return writeJson(res, 200, rows)
    }

    if (method === 'POST' && path === '/suppliers') {
      const body = await readBody(req)
      const name = String(body.name ?? '').trim()
      if (!name) return writeJson(res, 400, { error: 'name_required' })
      const { rows } = await pool.query(
        `INSERT INTO suppliers (name, contact_email, lead_time_days, currency, notes)
         VALUES ($1,$2,$3,$4,$5) RETURNING *`,
        [name, body.contactEmail || null, toInt(body.leadTimeDays) || 14, body.currency || 'GBP', body.notes || null]
      )
      return writeJson(res, 201, rows[0])
    }

    // ── Purchase Orders ──
    if (method === 'GET' && path === '/purchase-orders') {
      const status = url.searchParams.get('status') || ''
      const { rows } = await pool.query(`
        SELECT po.*,
          s.name as supplier_name,
          coalesce(lc.line_count, 0)::int as line_count,
          coalesce(lc.total_ordered, 0)::int as total_ordered,
          coalesce(lc.total_received, 0)::int as total_received,
          coalesce(lc.total_cost, 0)::numeric(12,2) as total_cost_gbp
        FROM purchase_orders po
        LEFT JOIN suppliers s ON s.id = po.supplier_id
        LEFT JOIN LATERAL (
          SELECT count(*)::int as line_count,
            coalesce(sum(qty_ordered), 0)::int as total_ordered,
            coalesce(sum(qty_received), 0)::int as total_received,
            coalesce(sum(qty_ordered * unit_cost_gbp), 0)::numeric(12,2) as total_cost
          FROM po_lines WHERE po_id = po.id
        ) lc ON true
        WHERE ($1 = '' OR po.status = $1)
        ORDER BY po.created_at DESC
      `, [status])
      return writeJson(res, 200, rows)
    }

    if (method === 'POST' && path === '/purchase-orders') {
      const body = await readBody(req)
      const supplierId = body.supplierId
      const warehouseCode = body.warehouseCode
      const lines = Array.isArray(body.lines) ? body.lines : []

      if (!supplierId) return writeJson(res, 400, { error: 'supplier_id_required' })
      if (!warehouseCode) return writeJson(res, 400, { error: 'warehouse_code_required' })
      if (!lines.length) return writeJson(res, 400, { error: 'lines_required' })

      const client = await pool.connect()
      try {
        await client.query('BEGIN')

        // Generate PO ref
        const year = new Date().getUTCFullYear()
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`po:${year}`])
        const { rows: seqRows } = await client.query(
          `SELECT coalesce(max((substring(po_ref from 'PO-\\d{4}-(\\d+)'))::int), 0) as last_seq
           FROM purchase_orders WHERE po_ref LIKE $1`,
          [`PO-${year}-%`]
        )
        const nextSeq = (seqRows[0]?.last_seq ?? 0) + 1
        const poRef = `PO-${year}-${String(nextSeq).padStart(4, '0')}`

        const { rows: poRows } = await client.query(
          `INSERT INTO purchase_orders (po_ref, supplier_id, warehouse_code, status, notes)
           VALUES ($1,$2,$3,'draft',$4) RETURNING *`,
          [poRef, supplierId, warehouseCode, body.notes || null]
        )
        const po = poRows[0]

        for (const line of lines) {
          await client.query(
            `INSERT INTO po_lines (po_id, product_id, qty_ordered, unit_cost_gbp)
             VALUES ($1,$2,$3,$4)`,
            [po.id, line.productId, toInt(line.qtyOrdered), toNum(line.unitCost)]
          )
        }

        await auditLog(client, 'po_created', 'purchase_order', po.id, warehouseCode, null, null, null,
          `PO ${poRef} created for ${lines.length} line items`)

        await client.query('COMMIT')
        return writeJson(res, 201, { ...po, lineCount: lines.length })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // Delete PO (draft only)
    const poDeleteMatch = path.match(/^\/purchase-orders\/([^/]+)$/)
    if (method === 'DELETE' && poDeleteMatch) {
      const { rows } = await pool.query(
        `DELETE FROM purchase_orders WHERE id = $1 AND status = 'draft' RETURNING id, po_ref`, [poDeleteMatch[1]])
      if (!rows.length) return writeJson(res, 400, { error: 'cannot_delete_non_draft' })
      return writeJson(res, 200, { deleted: rows[0].po_ref })
    }

    // PO detail
    const poDetailMatch = path.match(/^\/purchase-orders\/([^/]+)$/)
    if (method === 'GET' && poDetailMatch) {
      const poId = poDetailMatch[1]
      const { rows: poRows } = await pool.query(
        `SELECT po.*, s.name as supplier_name FROM purchase_orders po
         LEFT JOIN suppliers s ON s.id = po.supplier_id WHERE po.id = $1`, [poId]
      )
      if (!poRows.length) return writeJson(res, 404, { error: 'not_found' })
      const { rows: lines } = await pool.query(
        `SELECT pl.*, p.name as product_name, p.sku, p.brand
         FROM po_lines pl JOIN products p ON p.id = pl.product_id
         WHERE pl.po_id = $1 ORDER BY p.brand, p.name`, [poId]
      )
      return writeJson(res, 200, { ...poRows[0], lines })
    }

    // Submit PO
    const poSubmitMatch = path.match(/^\/purchase-orders\/([^/]+)\/submit$/)
    if (method === 'PATCH' && poSubmitMatch) {
      const poId = poSubmitMatch[1]
      const { rows } = await pool.query(
        `UPDATE purchase_orders SET status = 'submitted', updated_at = now()
         WHERE id = $1 AND status = 'draft' RETURNING *`, [poId]
      )
      if (!rows.length) return writeJson(res, 400, { error: 'cannot_submit' })
      await pool.query('UPDATE purchase_orders SET submitted_at = now() WHERE id = $1', [rows[0].id])
      await auditLogSimple('po_submitted', 'purchase_order', rows[0].id, rows[0].warehouse_code, null, null, null, `PO ${rows[0].po_ref} submitted`)
      return writeJson(res, 200, rows[0])
    }

    // Receive PO goods
    const poReceiveMatch = path.match(/^\/purchase-orders\/([^/]+)\/receive$/)
    if (method === 'POST' && poReceiveMatch) {
      const poId = poReceiveMatch[1]
      const body = await readBody(req)
      const receivedLines = Array.isArray(body.lines) ? body.lines : []

      const client = await pool.connect()
      try {
        await client.query('BEGIN')

        const { rows: poRows } = await client.query(
          `SELECT * FROM purchase_orders WHERE id = $1 AND status IN ('submitted','partial') FOR UPDATE`, [poId]
        )
        if (!poRows.length) { await client.query('ROLLBACK'); return writeJson(res, 400, { error: 'cannot_receive' }) }
        const po = poRows[0]

        for (const rl of receivedLines) {
          const qtyReceiving = toInt(rl.qtyReceived)
          if (qtyReceiving <= 0) continue

          // Update PO line
          const { rows: plRows } = await client.query(
            `UPDATE po_lines SET qty_received = qty_received + $1
             WHERE id = $2 AND po_id = $3 RETURNING *, (qty_received) as new_received`,
            [qtyReceiving, rl.lineId, poId]
          )
          if (!plRows.length) continue

          // Update warehouse stock
          const { rows: wsRows } = await client.query(
            `UPDATE warehouse_stock SET qty_on_hand = qty_on_hand + $1
             WHERE product_id = $2 AND warehouse = $3
             RETURNING qty_on_hand`,
            [qtyReceiving, plRows[0].product_id, po.warehouse_code]
          )

          const newBalance = wsRows[0]?.qty_on_hand ?? qtyReceiving

          await auditLog(client, 'stock_received', 'purchase_order', po.id,
            po.warehouse_code, plRows[0].product_id, qtyReceiving, newBalance,
            `Received ${qtyReceiving} units via ${po.po_ref}`)
        }

        // Check if fully received
        const { rows: statusCheck } = await client.query(
          `SELECT bool_and(qty_received >= qty_ordered) as fully_received,
                  bool_or(qty_received > 0) as any_received
           FROM po_lines WHERE po_id = $1`, [poId]
        )

        const newStatus = statusCheck[0]?.fully_received ? 'received'
          : statusCheck[0]?.any_received ? 'partial' : po.status

        const receivedNow = newStatus === 'received'
        await client.query(
          `UPDATE purchase_orders SET status = $1, updated_at = now()${receivedNow ? ', received_at = now()' : ''} WHERE id = $2`,
          [newStatus, poId]
        )

        await client.query('COMMIT')

        // Trigger backorder auto-fulfilment for the warehouse that just received stock
        if (receivedNow || newStatus === 'partial') {
          try {
            const { rows: bof } = await pool.query(`
              SELECT ol.id as line_id, ol.order_id, ol.product_id, ol.qty_backordered,
                o.order_ref, o.warehouse_code, ws.qty_on_hand - ws.qty_reserved as qty_available
              FROM order_lines ol
              JOIN orders o ON o.id = ol.order_id
              JOIN warehouse_stock ws ON ws.product_id = ol.product_id AND ws.warehouse = o.warehouse_code
              WHERE ol.fulfilment_status IN ('backordered','partial') AND ol.qty_backordered > 0
                AND o.warehouse_code = $1 AND ws.qty_on_hand - ws.qty_reserved > 0
              ORDER BY o.created_at ASC LIMIT 50
            `, [po.warehouse_code])
            if (bof.length) {
              console.log(`PO ${po.po_ref} received — ${bof.length} backorder lines eligible for auto-fulfilment`)
            }
          } catch (_) { /* non-critical */ }
        }

        return writeJson(res, 200, { status: newStatus })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Stock Transfers ──
    if (method === 'GET' && path === '/transfers') {
      const { rows } = await pool.query(`
        SELECT st.*,
          coalesce(lc.line_count, 0)::int as line_count,
          coalesce(lc.total_units, 0)::int as total_units
        FROM stock_transfers st
        LEFT JOIN LATERAL (
          SELECT count(*)::int as line_count, coalesce(sum(qty),0)::int as total_units
          FROM transfer_lines WHERE transfer_id = st.id
        ) lc ON true
        ORDER BY st.created_at DESC
      `)
      return writeJson(res, 200, rows)
    }

    if (method === 'POST' && path === '/transfers') {
      const body = await readBody(req)
      const from = body.fromWarehouse
      const to = body.toWarehouse
      const lines = Array.isArray(body.lines) ? body.lines : []

      if (!from || !to) return writeJson(res, 400, { error: 'warehouses_required' })
      if (from === to) return writeJson(res, 400, { error: 'same_warehouse' })
      if (!lines.length) return writeJson(res, 400, { error: 'lines_required' })

      const client = await pool.connect()
      try {
        await client.query('BEGIN')

        const year = new Date().getUTCFullYear()
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`xfer:${year}`])
        const { rows: seqRows } = await client.query(
          `SELECT coalesce(max((substring(transfer_ref from 'TXF-\\d{4}-(\\d+)'))::int), 0) as last_seq
           FROM stock_transfers WHERE transfer_ref LIKE $1`,
          [`TXF-${year}-%`]
        )
        const nextSeq = (seqRows[0]?.last_seq ?? 0) + 1
        const ref = `TXF-${year}-${String(nextSeq).padStart(4, '0')}`

        const { rows: tRows } = await client.query(
          `INSERT INTO stock_transfers (transfer_ref, from_warehouse, to_warehouse, status, notes)
           VALUES ($1,$2,$3,'requested',$4) RETURNING *`,
          [ref, from, to, body.notes || null]
        )

        for (const line of lines) {
          await client.query(
            `INSERT INTO transfer_lines (transfer_id, product_id, qty)
             VALUES ($1,$2,$3)`,
            [tRows[0].id, line.productId, toInt(line.qty)]
          )
        }

        await auditLog(client, 'transfer_created', 'stock_transfer', tRows[0].id, from, null, null, null,
          `Transfer ${ref}: ${from} → ${to}, ${lines.length} items`)

        await client.query('COMMIT')
        return writeJson(res, 201, { ...tRows[0], lineCount: lines.length })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // Transfer detail
    const transferDetailMatch = path.match(/^\/transfers\/([^/]+)$/)
    if (method === 'GET' && transferDetailMatch) {
      const id = transferDetailMatch[1]
      const { rows: tRows } = await pool.query('SELECT * FROM stock_transfers WHERE id = $1', [id])
      if (!tRows.length) return writeJson(res, 404, { error: 'not_found' })
      const { rows: lines } = await pool.query(
        `SELECT tl.*, p.name as product_name, p.sku, p.brand
         FROM transfer_lines tl JOIN products p ON p.id = tl.product_id
         WHERE tl.transfer_id = $1 ORDER BY p.brand, p.name`, [id]
      )
      return writeJson(res, 200, { ...tRows[0], lines })
    }

    // Dispatch transfer
    const transferDispatchMatch = path.match(/^\/transfers\/([^/]+)\/dispatch$/)
    if (method === 'PATCH' && transferDispatchMatch) {
      const id = transferDispatchMatch[1]
      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const { rows: tRows } = await client.query(
          `SELECT * FROM stock_transfers WHERE id = $1 AND status = 'requested' FOR UPDATE`, [id]
        )
        if (!tRows.length) { await client.query('ROLLBACK'); return writeJson(res, 400, { error: 'cannot_dispatch' }) }
        const t = tRows[0]

        const { rows: lines } = await client.query(
          'SELECT * FROM transfer_lines WHERE transfer_id = $1', [id]
        )

        for (const line of lines) {
          const { rows: wsRows } = await client.query(
            `UPDATE warehouse_stock SET qty_on_hand = qty_on_hand - $1
             WHERE product_id = $2 AND warehouse = $3 AND qty_on_hand >= $1
             RETURNING qty_on_hand`,
            [line.qty, line.product_id, t.from_warehouse]
          )
          if (!wsRows.length) {
            await client.query('ROLLBACK')
            return writeJson(res, 400, { error: 'insufficient_stock', productId: line.product_id })
          }
          await auditLog(client, 'stock_transferred_out', 'stock_transfer', t.id,
            t.from_warehouse, line.product_id, -line.qty, wsRows[0].qty_on_hand,
            `Dispatched ${line.qty} units via ${t.transfer_ref} → ${t.to_warehouse}`)
        }

        await client.query(
          `UPDATE stock_transfers SET status = 'in_transit', updated_at = now() WHERE id = $1`, [id]
        )
        await client.query('COMMIT')
        return writeJson(res, 200, { status: 'in_transit' })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // Receive transfer
    const transferReceiveMatch = path.match(/^\/transfers\/([^/]+)\/receive$/)
    if (method === 'PATCH' && transferReceiveMatch) {
      const id = transferReceiveMatch[1]
      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const { rows: tRows } = await client.query(
          `SELECT * FROM stock_transfers WHERE id = $1 AND status = 'in_transit' FOR UPDATE`, [id]
        )
        if (!tRows.length) { await client.query('ROLLBACK'); return writeJson(res, 400, { error: 'cannot_receive' }) }
        const t = tRows[0]

        const { rows: lines } = await client.query(
          'SELECT * FROM transfer_lines WHERE transfer_id = $1', [id]
        )

        for (const line of lines) {
          const { rows: wsRows } = await client.query(
            `UPDATE warehouse_stock SET qty_on_hand = qty_on_hand + $1
             WHERE product_id = $2 AND warehouse = $3
             RETURNING qty_on_hand`,
            [line.qty, line.product_id, t.to_warehouse]
          )
          // If no row exists for this warehouse, create it
          if (!wsRows.length) {
            await client.query(
              `INSERT INTO warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
               VALUES ($1,$2,$3,0,5)`,
              [line.product_id, t.to_warehouse, line.qty]
            )
          }
          const newBal = wsRows[0]?.qty_on_hand ?? line.qty
          await auditLog(client, 'stock_transferred_in', 'stock_transfer', t.id,
            t.to_warehouse, line.product_id, line.qty, newBal,
            `Received ${line.qty} units via ${t.transfer_ref} from ${t.from_warehouse}`)
        }

        await client.query(
          `UPDATE stock_transfers SET status = 'received', updated_at = now() WHERE id = $1`, [id]
        )
        await client.query('COMMIT')
        return writeJson(res, 200, { status: 'received' })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Order Fulfilment ──
    if (method === 'GET' && path === '/fulfilment') {
      const status = url.searchParams.get('status') || ''
      const { rows } = await pool.query(`
        SELECT o.id, o.order_ref, o.customer_name, o.status, o.fulfilment_status,
          o.warehouse_code, o.currency, o.total_gbp, o.created_at,
          coalesce(lc.line_count, 0)::int as line_count,
          coalesce(lc.total_units, 0)::int as total_units
        FROM orders o
        LEFT JOIN LATERAL (
          SELECT count(*)::int as line_count, coalesce(sum(qty_ordered),0)::int as total_units
          FROM order_lines WHERE order_id = o.id
        ) lc ON true
        WHERE ($1 = '' OR o.fulfilment_status = $1)
        ORDER BY
          CASE o.fulfilment_status
            WHEN 'confirmed' THEN 1 WHEN 'picking' THEN 2 WHEN 'picked' THEN 3
            WHEN 'packing' THEN 4 WHEN 'packed' THEN 5 WHEN 'shipped' THEN 6
            ELSE 7 END,
          o.created_at ASC
      `, [status])
      return writeJson(res, 200, rows)
    }

    // Progress order through fulfilment stages
    const fulfilProgressMatch = path.match(/^\/fulfilment\/([^/]+)\/(pick|pack|ship)$/)
    if (method === 'PATCH' && fulfilProgressMatch) {
      const orderId = fulfilProgressMatch[1]
      const action = fulfilProgressMatch[2]

      const transitions = {
        pick: { from: 'confirmed', to: 'picking' },
        pack: { from: ['picking', 'picked'], to: 'packing' },
        ship: { from: ['packing', 'packed'], to: 'shipped' },
      }
      const t = transitions[action]
      const fromArr = Array.isArray(t.from) ? t.from : [t.from]

      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const { rows } = await client.query(
          `UPDATE orders SET fulfilment_status = $1
           WHERE id = $2 AND fulfilment_status = ANY($3) RETURNING *`,
          [t.to, orderId, fromArr]
        )
        if (!rows.length) { await client.query('ROLLBACK'); return writeJson(res, 400, { error: 'invalid_transition' }) }
        const order = rows[0]

        // On ship: deduct stock
        if (action === 'ship' && order.warehouse_code) {
          const { rows: oLines } = await client.query(
            'SELECT * FROM order_lines WHERE order_id = $1', [orderId]
          )
          for (const ol of oLines) {
            if (!ol.product_id) continue
            const { rows: wsRows } = await client.query(
              `UPDATE warehouse_stock SET qty_on_hand = qty_on_hand - $1
               WHERE product_id = $2 AND warehouse = $3
               RETURNING qty_on_hand`,
              [ol.qty_ordered, ol.product_id, order.warehouse_code]
            )
            await auditLog(client, 'stock_shipped', 'order', order.id,
              order.warehouse_code, ol.product_id, -ol.qty_ordered,
              wsRows[0]?.qty_on_hand ?? 0,
              `Shipped ${ol.qty_ordered} units for ${order.order_ref}`)
          }
        }

        await auditLog(client, `order_${action}`, 'order', order.id,
          order.warehouse_code, null, null, null,
          `Order ${order.order_ref} → ${t.to}`)

        await client.query('COMMIT')
        return writeJson(res, 200, { ...order, fulfilment_status: t.to })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Wholesale Pricing (tiers) ──
    if (method === 'GET' && path === '/wholesale/pricing') {
      const productId = url.searchParams.get('productId') || ''
      if (productId) {
        const { rows } = await pool.query(
          `SELECT pt.*, p.name as product_name, p.sku, p.sell_price_gbp, p.moq
           FROM price_tiers pt JOIN products p ON p.id = pt.product_id
           WHERE pt.product_id = $1 ORDER BY pt.min_qty`, [productId])
        return writeJson(res, 200, rows)
      }
      // Return all tiers grouped by product
      const { rows } = await pool.query(`
        SELECT p.id as product_id, p.name, p.sku, p.brand, p.sell_price_gbp, p.moq,
          coalesce(json_agg(json_build_object(
            'min_qty', pt.min_qty, 'discount_pct', pt.discount_pct
          ) ORDER BY pt.min_qty) FILTER (WHERE pt.id IS NOT NULL), '[]'::json) as tiers
        FROM products p
        LEFT JOIN price_tiers pt ON pt.product_id = p.id
        GROUP BY p.id
        HAVING count(pt.id) > 0
        ORDER BY p.brand, p.name
      `)
      return writeJson(res, 200, rows)
    }

    // ── Fulfilment Review (partial stock) ──
    if (method === 'GET' && path === '/fulfilment/review') {
      const { rows } = await pool.query(`
        SELECT o.id, o.order_ref, o.customer_name, o.warehouse_code, o.currency,
          o.total_gbp, o.created_at, o.notes,
          c.company_name,
          json_agg(json_build_object(
            'line_id', ol.id, 'product_id', ol.product_id, 'product_name', ol.product_name,
            'sku', ol.sku, 'qty_requested', coalesce(ol.qty_requested, ol.qty_ordered),
            'qty_ordered', ol.qty_ordered, 'unit_price_gbp', ol.unit_price_gbp,
            'fulfilment_decision', ol.fulfilment_decision,
            'qty_to_ship', ol.qty_to_ship, 'qty_to_backorder', ol.qty_to_backorder
          ) ORDER BY ol.product_name) as lines
        FROM orders o
        JOIN order_lines ol ON ol.order_id = o.id
        LEFT JOIN customers c ON c.id = o.customer_id
        WHERE o.requires_review = true
        GROUP BY o.id, o.order_ref, o.customer_name, o.warehouse_code, o.currency,
          o.total_gbp, o.created_at, o.notes, c.company_name
        ORDER BY o.created_at ASC
      `)

      // Enrich with current stock availability
      for (const order of rows) {
        for (const line of order.lines) {
          if (line.product_id) {
            const { rows: ws } = await pool.query(
              `SELECT qty_on_hand - qty_reserved as avail FROM warehouse_stock WHERE product_id = $1 AND warehouse = $2`,
              [line.product_id, order.warehouse_code])
            line.qty_available = ws[0]?.avail ?? 0
          } else {
            line.qty_available = 0
          }
        }
      }

      return writeJson(res, 200, rows)
    }

    // Process fulfilment review decisions
    const reviewMatch = path.match(/^\/fulfilment\/([^/]+)\/review$/)
    if (method === 'POST' && reviewMatch) {
      const orderId = reviewMatch[1]
      const body = await readBody(req)
      const decisions = Array.isArray(body.lines) ? body.lines : []

      if (!decisions.length) return writeJson(res, 400, { error: 'lines_required' })

      const client = await pool.connect()
      try {
        await client.query('BEGIN')

        const { rows: oRows } = await client.query(
          `SELECT * FROM orders WHERE id = $1 AND requires_review = true FOR UPDATE`, [orderId])
        if (!oRows.length) { await client.query('ROLLBACK'); return writeJson(res, 400, { error: 'not_reviewable' }) }
        const order = oRows[0]

        let totalToShipValue = 0

        for (const dec of decisions) {
          const { rows: lineRows } = await client.query(
            `SELECT * FROM order_lines WHERE id = $1 AND order_id = $2`, [dec.lineId, orderId])
          if (!lineRows.length) continue
          const line = lineRows[0]

          const qtyRequested = line.qty_requested || line.qty_ordered
          let qtyToShip = 0, qtyToBackorder = 0

          if (dec.decision === 'ship_available') {
            // Get current availability
            const { rows: ws } = await client.query(
              `SELECT qty_on_hand - qty_reserved as avail FROM warehouse_stock
               WHERE product_id = $1 AND warehouse = $2`,
              [line.product_id, order.warehouse_code])
            const avail = ws[0]?.avail ?? 0
            qtyToShip = Math.min(qtyRequested, avail)
            qtyToBackorder = qtyRequested - qtyToShip
          } else if (dec.decision === 'custom') {
            qtyToShip = Math.max(0, toInt(dec.customQty))
            qtyToBackorder = qtyRequested - qtyToShip
          } else if (dec.decision === 'hold') {
            qtyToShip = 0
            qtyToBackorder = qtyRequested
          } else {
            continue
          }

          // Reserve stock for qty_to_ship
          if (qtyToShip > 0 && line.product_id) {
            await client.query(
              `UPDATE warehouse_stock SET qty_reserved = qty_reserved + $1
               WHERE product_id = $2 AND warehouse = $3`,
              [qtyToShip, line.product_id, order.warehouse_code])
          }

          const newStatus = qtyToBackorder > 0 ? (qtyToShip > 0 ? 'partial' : 'backordered') : 'unmatched'

          await client.query(
            `UPDATE order_lines SET fulfilment_decision = $1, qty_to_ship = $2, qty_to_backorder = $3,
             qty_ordered = $4, qty_fulfilled = 0, qty_backordered = $5, fulfilment_status = $6
             WHERE id = $7`,
            [dec.decision, qtyToShip, qtyToBackorder, qtyToShip, qtyToBackorder, newStatus, dec.lineId])

          totalToShipValue += qtyToShip * Number(line.unit_price_gbp)
        }

        // Update order: clear review flag, update total to reflect what's actually shipping
        await client.query(
          `UPDATE orders SET requires_review = false, total_gbp = $1 WHERE id = $2`,
          [totalToShipValue.toFixed(2), orderId])

        await auditLog(client, 'order_reviewed', 'order', orderId, order.warehouse_code, null, null, null,
          `Order ${order.order_ref} reviewed: £${totalToShipValue.toFixed(2)} to ship, ${decisions.length} lines decided`)

        await client.query('COMMIT')
        return writeJson(res, 200, { status: 'reviewed', totalToShip: Number(totalToShipValue.toFixed(2)) })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Customers ──
    if (method === 'GET' && path === '/customers') {
      const search = url.searchParams.get('search') || ''
      const { rows } = await pool.query(`
        SELECT c.*, coalesce(count(o.id),0)::int as order_count,
          coalesce(round(sum(o.total_gbp)::numeric,2),0)::numeric(12,2) as total_spend_gbp,
          CASE WHEN c.credit_limit_gbp > 0
            THEN round((c.credit_limit_gbp - c.credit_used_gbp)::numeric, 2)
            ELSE NULL END as credit_available_gbp
        FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
        WHERE ($1 = '' OR c.company_name ILIKE $2)
        GROUP BY c.id ORDER BY c.company_name
      `, [search, `%${search}%`])
      return writeJson(res, 200, rows)
    }

    if (method === 'POST' && path === '/customers') {
      const body = await readBody(req)
      const name = String(body.companyName ?? '').trim()
      if (!name) return writeJson(res, 400, { error: 'company_name_required' })
      const { rows } = await pool.query(
        `INSERT INTO customers (company_name, contact_name, email, billing_address, currency_preference,
          vat_number, notes, region, customer_type, payment_terms, credit_limit_gbp, default_discount_pct)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
        [name, body.contactName||null, body.email||null, body.billingAddress||null,
         body.currencyPreference||'GBP', body.vatNumber||null, body.notes||null, body.region||null,
         body.customerType||'retail', body.paymentTerms||'prepaid',
         toNum(body.creditLimitGbp)||0, toNum(body.defaultDiscountPct)||0]
      )
      return writeJson(res, 201, rows[0])
    }

    const custMatch = path.match(/^\/customers\/([^/]+)$/)
    if (method === 'GET' && custMatch) {
      const { rows } = await pool.query(
        `SELECT c.*, coalesce(count(o.id),0)::int as order_count,
          coalesce(round(sum(o.total_gbp)::numeric,2),0)::numeric(12,2) as total_spend_gbp,
          CASE WHEN c.credit_limit_gbp > 0
            THEN round((c.credit_limit_gbp - c.credit_used_gbp)::numeric, 2)
            ELSE NULL END as credit_available_gbp
         FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
         WHERE c.id = $1 GROUP BY c.id`, [custMatch[1]]
      )
      return rows.length ? writeJson(res, 200, rows[0]) : writeJson(res, 404, { error: 'not_found' })
    }

    // ── Orders (create) — with MOQ, tiered pricing, credit check ──
    if (method === 'POST' && path === '/orders') {
      const body = await readBody(req)
      const customerId = body.customerId
      const customerName = String(body.customerName ?? '').trim()
      const lines = Array.isArray(body.lines) ? body.lines : []
      const warehouseCode = body.warehouseCode || 'uk'
      const skipWholesaleChecks = body.skipWholesaleChecks === true

      if (!customerId) return writeJson(res, 400, { error: 'customer_id_required' })
      if (!lines.length) return writeJson(res, 400, { error: 'lines_required' })

      // Fetch customer for wholesale checks
      const { rows: custCheck } = await pool.query(
        'SELECT customer_type, payment_terms, credit_limit_gbp, credit_used_gbp, default_discount_pct FROM customers WHERE id = $1', [customerId])
      const cust = custCheck[0]

      if (!skipWholesaleChecks && cust) {
        // MOQ enforcement
        const productIds = lines.map(l => l.productId).filter(Boolean)
        if (productIds.length) {
          const { rows: moqRows } = await pool.query(
            `SELECT id, name, moq FROM products WHERE id = ANY($1) AND moq > 1`, [productIds])
          const moqMap = Object.fromEntries(moqRows.map(r => [r.id, r]))
          const moqErrors = []
          for (const line of lines) {
            const prod = moqMap[line.productId]
            if (prod && toInt(line.qtyOrdered) < prod.moq) {
              moqErrors.push({ productId: line.productId, productName: prod.name, ordered: toInt(line.qtyOrdered), moq: prod.moq })
            }
          }
          if (moqErrors.length) {
            return writeJson(res, 400, { error: 'moq_not_met', details: moqErrors })
          }
        }

        // Apply tiered pricing — compute effective unit price per line
        for (const line of lines) {
          if (!line.productId) continue
          const qty = toInt(line.qtyOrdered)
          const { rows: tiers } = await pool.query(
            `SELECT min_qty, discount_pct FROM price_tiers WHERE product_id = $1 AND min_qty <= $2 ORDER BY min_qty DESC LIMIT 1`,
            [line.productId, qty])
          let discount = Number(cust.default_discount_pct) || 0
          if (tiers.length && Number(tiers[0].discount_pct) > discount) {
            discount = Number(tiers[0].discount_pct)
          }
          if (discount > 0) {
            const basePrice = toNum(line.unitPriceGbp)
            line.unitPriceGbp = Number((basePrice * (1 - discount / 100)).toFixed(2))
            line._discount = discount
          }
        }

        // Credit limit check (only for non-prepaid customers)
        if (cust.payment_terms !== 'prepaid' && Number(cust.credit_limit_gbp) > 0) {
          const orderTotal = lines.reduce((s, l) => s + toInt(l.qtyOrdered) * toNum(l.unitPriceGbp), 0)
          const creditAvailable = Number(cust.credit_limit_gbp) - Number(cust.credit_used_gbp)
          if (orderTotal > creditAvailable) {
            return writeJson(res, 400, {
              error: 'credit_limit_exceeded',
              orderTotal: Number(orderTotal.toFixed(2)),
              creditAvailable: Number(creditAvailable.toFixed(2)),
              creditLimit: Number(cust.credit_limit_gbp),
              creditUsed: Number(cust.credit_used_gbp)
            })
          }
        }
      }

      // Check stock availability per line to determine if review is needed
      let needsReview = false
      const lineAvailability = []
      for (const line of lines) {
        if (!line.productId) { lineAvailability.push(null); continue }
        const { rows: wsRows } = await pool.query(
          `SELECT qty_on_hand - qty_reserved as avail FROM warehouse_stock WHERE product_id = $1 AND warehouse = $2`,
          [line.productId, warehouseCode])
        const avail = wsRows[0]?.avail ?? 0
        const qtyOrdered = toInt(line.qtyOrdered)
        lineAvailability.push(avail)
        if (qtyOrdered > avail) needsReview = true
      }

      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const year = new Date().getUTCFullYear()
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`orders:${year}`])
        const { rows: seqRows } = await client.query(
          `SELECT coalesce(max((substring(order_ref from 'ORD-\\d{4}-(\\d+)'))::int), 0) as last_seq
           FROM orders WHERE order_ref LIKE $1`, [`ORD-${year}-%`]
        )
        const nextSeq = (seqRows[0]?.last_seq ?? 0) + 1
        const orderRef = `ORD-${year}-${String(nextSeq).padStart(4, '0')}`

        const totalGbp = lines.reduce((s, l) => s + toInt(l.qtyOrdered) * toNum(l.unitPriceGbp), 0)

        const { rows: oRows } = await client.query(
          `INSERT INTO orders (order_ref, customer_id, customer_name, status, fulfilment_status, warehouse_code, currency, fx_rate, total_gbp, notes, requires_review)
           VALUES ($1,$2,$3,'confirmed','confirmed',$4,$5,1.0,$6,$7,$8) RETURNING *`,
          [orderRef, customerId, customerName, warehouseCode, body.currency||'GBP', totalGbp.toFixed(2), body.notes||null, needsReview]
        )

        for (let i = 0; i < lines.length; i++) {
          const line = lines[i]
          const avail = lineAvailability[i]
          const qtyOrdered = toInt(line.qtyOrdered)
          await client.query(
            `INSERT INTO order_lines (order_id, product_id, product_name, sku, qty_ordered, qty_requested, unit_price_gbp, warehouse_allocated, fulfilment_status)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
            [oRows[0].id, line.productId, line.productName||'', line.sku||'', qtyOrdered, qtyOrdered,
             toNum(line.unitPriceGbp), warehouseCode, needsReview ? 'pending_review' : 'unmatched']
          )
        }

        // Update credit used for non-prepaid customers
        if (cust && cust.payment_terms !== 'prepaid' && Number(cust.credit_limit_gbp) > 0) {
          await client.query(
            `UPDATE customers SET credit_used_gbp = credit_used_gbp + $1 WHERE id = $2`,
            [totalGbp.toFixed(2), customerId])
        }

        const reviewNote = needsReview ? ' (requires review — partial stock)' : ''
        await auditLog(client, 'order_created', 'order', oRows[0].id, warehouseCode, null, null, null,
          `Order ${orderRef} created: ${lines.length} lines, £${totalGbp.toFixed(2)}${reviewNote}`)

        await client.query('COMMIT')
        return writeJson(res, 201, { ...oRows[0], requires_review: needsReview })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Audit Log ──
    if (method === 'GET' && path === '/audit-log') {
      const warehouse = url.searchParams.get('warehouse') || ''
      const action = url.searchParams.get('action') || ''
      const limit = toInt(url.searchParams.get('limit')) || 100
      const { rows } = await pool.query(`
        SELECT al.*,
          p.name as product_name, p.sku as product_sku
        FROM audit_log al
        LEFT JOIN products p ON p.id = al.product_id
        WHERE ($1 = '' OR al.warehouse_code = $1)
          AND ($2 = '' OR al.action = $2)
        ORDER BY al.created_at DESC
        LIMIT $3
      `, [warehouse, action, limit])
      return writeJson(res, 200, rows)
    }

    // ── Low Stock Alerts ──
    if (method === 'GET' && path === '/alerts/low-stock') {
      const { rows } = await pool.query(`
        SELECT
          ws.warehouse,
          ws.qty_on_hand,
          ws.qty_reserved,
          ws.qty_on_hand - ws.qty_reserved as qty_available,
          ws.reorder_point,
          p.id as product_id,
          p.name,
          p.sku,
          p.brand,
          p.cost_price_gbp,
          p.case_pack,
          s.name as supplier_name,
          s.lead_time_days,
          CASE WHEN ws.qty_on_hand = 0 THEN 'out_of_stock' ELSE 'low_stock' END as alert_type,
          -- Check if other warehouses have surplus
          (SELECT json_agg(json_build_object(
            'warehouse', ws2.warehouse, 'qty_available', ws2.qty_on_hand - ws2.qty_reserved
          )) FROM warehouse_stock ws2
          WHERE ws2.product_id = p.id AND ws2.warehouse != ws.warehouse
            AND ws2.qty_on_hand - ws2.qty_reserved > ws2.reorder_point
          ) as surplus_elsewhere
        FROM warehouse_stock ws
        JOIN products p ON p.id = ws.product_id
        LEFT JOIN suppliers s ON s.id = p.supplier_id
        WHERE ws.qty_on_hand <= ws.reorder_point
        ORDER BY ws.qty_on_hand ASC, ws.warehouse, p.brand, p.name
      `)
      return writeJson(res, 200, rows)
    }

    // ── Dashboard Chart Data ──
    if (method === 'GET' && path === '/dashboard/chart-data') {
      const [byBrand, byWarehouse, byMonth, topProducts] = await Promise.all([
        pool.query(`
          SELECT p.brand, sum(ws.qty_on_hand)::int as units, sum(ws.qty_on_hand * p.cost_price_gbp)::numeric(12,2) as value
          FROM warehouse_stock ws JOIN products p ON p.id = ws.product_id
          GROUP BY p.brand ORDER BY sum(ws.qty_on_hand) DESC
        `),
        pool.query(`
          SELECT w.code as warehouse, w.name,
            sum(ws.qty_on_hand)::int as units,
            sum(ws.qty_on_hand * p.cost_price_gbp)::numeric(12,2) as value
          FROM warehouses w
          LEFT JOIN warehouse_stock ws ON ws.warehouse = w.code
          LEFT JOIN products p ON p.id = ws.product_id
          GROUP BY w.code, w.name ORDER BY w.code
        `),
        pool.query(`
          SELECT to_char(o.created_at, 'YYYY-MM') as month,
            count(*)::int as order_count,
            coalesce(sum(o.total_gbp),0)::numeric(12,2) as revenue
          FROM orders o
          WHERE o.created_at >= now() - interval '6 months'
          GROUP BY to_char(o.created_at, 'YYYY-MM')
          ORDER BY month
        `),
        pool.query(`
          SELECT p.name, p.sku, coalesce(sum(ol.qty_ordered),0)::int as total_ordered
          FROM products p
          LEFT JOIN order_lines ol ON ol.product_id = p.id
          GROUP BY p.id, p.name, p.sku
          ORDER BY coalesce(sum(ol.qty_ordered),0) DESC
          LIMIT 10
        `)
      ])
      return writeJson(res, 200, {
        stockByBrand: byBrand.rows,
        stockByWarehouse: byWarehouse.rows,
        ordersByMonth: byMonth.rows,
        topProducts: topProducts.rows
      })
    }

    // ── Dashboard KPIs ──
    if (method === 'GET' && path === '/dashboard/kpis') {
      const [inventory, orders, transfers, alerts] = await Promise.all([
        pool.query(`
          SELECT
            w.code as warehouse,
            w.name,
            w.currency,
            coalesce(sum(ws.qty_on_hand), 0)::int as total_units,
            coalesce(sum(ws.qty_on_hand * p.cost_price_gbp), 0)::numeric(12,2) as stock_value_gbp,
            count(*) FILTER (WHERE ws.qty_on_hand <= ws.reorder_point)::int as low_stock_items,
            count(*) FILTER (WHERE ws.qty_on_hand = 0)::int as out_of_stock_items
          FROM warehouses w
          LEFT JOIN warehouse_stock ws ON ws.warehouse = w.code
          LEFT JOIN products p ON p.id = ws.product_id
          GROUP BY w.code, w.name, w.currency
          ORDER BY w.code
        `),
        pool.query(`
          SELECT
            coalesce(count(*) FILTER (WHERE fulfilment_status = 'confirmed'), 0)::int as pending,
            coalesce(count(*) FILTER (WHERE fulfilment_status IN ('picking','picked','packing','packed')), 0)::int as in_progress,
            coalesce(count(*) FILTER (WHERE fulfilment_status = 'shipped'), 0)::int as shipped,
            coalesce(sum(total_gbp) FILTER (WHERE fulfilment_status = 'shipped'), 0)::numeric(12,2) as revenue_shipped
          FROM orders
        `),
        pool.query(`
          SELECT
            coalesce(count(*) FILTER (WHERE status = 'in_transit'), 0)::int as in_transit,
            coalesce(count(*) FILTER (WHERE status = 'requested'), 0)::int as pending,
            coalesce(sum(tl.qty) FILTER (WHERE st.status = 'in_transit'), 0)::int as units_in_transit
          FROM stock_transfers st
          LEFT JOIN transfer_lines tl ON tl.transfer_id = st.id
        `),
        pool.query(`
          SELECT count(*)::int as total_alerts
          FROM warehouse_stock ws
          WHERE ws.qty_on_hand <= ws.reorder_point
        `)
      ])

      return writeJson(res, 200, {
        warehouses: inventory.rows,
        orders: orders.rows[0],
        transfers: transfers.rows[0],
        lowStockAlerts: alerts.rows[0].total_alerts,
        generatedAt: new Date().toISOString()
      })
    }

    // ── Products list (for dropdowns) ──
    if (method === 'GET' && path === '/products') {
      const supplier = url.searchParams.get('supplier') || ''
      const warehouse = url.searchParams.get('warehouse') || ''
      const { rows } = await pool.query(`
        SELECT p.id, p.name, p.sku, p.brand, p.cost_price_gbp, p.sell_price_gbp, p.case_pack,
          p.supplier_id, s.name as supplier_name
          ${warehouse ? `, ws.qty_on_hand, ws.qty_available` : ''}
        FROM products p
        LEFT JOIN suppliers s ON s.id = p.supplier_id
        ${warehouse ? `LEFT JOIN LATERAL (
          SELECT qty_on_hand, qty_on_hand - qty_reserved as qty_available
          FROM warehouse_stock WHERE product_id = p.id AND warehouse = '${warehouse}'
        ) ws ON true` : ''}
        WHERE ($1 = '' OR p.supplier_id::text = $1)
        ORDER BY p.brand, p.name
      `, [supplier])
      return writeJson(res, 200, rows)
    }

    // ── Price History ──
    const priceHistMatch = path.match(/^\/products\/([^/]+)\/price-history$/)
    if (method === 'GET' && priceHistMatch) {
      const productId = priceHistMatch[1]
      const { rows } = await pool.query(`
        SELECT ph.*, p.name as product_name, p.sku
        FROM price_history ph
        JOIN products p ON p.id = ph.product_id
        WHERE ph.product_id = $1
        ORDER BY ph.changed_at DESC
      `, [productId])
      return writeJson(res, 200, rows)
    }

    // Bulk price trends for inventory page (latest change per product)
    if (method === 'GET' && path === '/price-trends') {
      const { rows } = await pool.query(`
        SELECT DISTINCT ON (ph.product_id)
          ph.product_id, ph.field, ph.old_value, ph.new_value, ph.changed_at
        FROM price_history ph
        WHERE ph.field = 'buy_cost'
        ORDER BY ph.product_id, ph.changed_at DESC
      `)
      return writeJson(res, 200, rows)
    }

    // ── Stock Adjustment ──
    if (method === 'POST' && path === '/stock-adjustments') {
      const body = await readBody(req)
      const { productId, warehouseCode, adjustment, reason } = body
      if (!productId || !warehouseCode || adjustment === undefined) {
        return writeJson(res, 400, { error: 'missing_fields' })
      }
      const adj = toInt(adjustment)
      const { rows } = await pool.query(
        `UPDATE warehouse_stock SET qty_on_hand = GREATEST(0, qty_on_hand + $1)
         WHERE product_id = $2 AND warehouse = $3 RETURNING qty_on_hand`,
        [adj, productId, warehouseCode]
      )
      if (!rows.length) return writeJson(res, 404, { error: 'stock_record_not_found' })
      await auditLogSimple('stock_adjusted', 'warehouse_stock', null, warehouseCode, productId,
        adj, rows[0].qty_on_hand, reason || `Manual adjustment: ${adj > 0 ? '+' : ''}${adj}`)
      return writeJson(res, 200, { qty_on_hand: rows[0].qty_on_hand })
    }

    // ── Returns / RMA ──
    if (method === 'GET' && path === '/returns') {
      const { rows } = await pool.query(`
        SELECT r.*, p.name as product_name, p.sku, p.brand,
          o.order_ref, c.company_name as customer_name
        FROM returns r
        LEFT JOIN products p ON p.id = r.product_id
        LEFT JOIN orders o ON o.id = r.order_id
        LEFT JOIN customers c ON c.id = r.customer_id
        ORDER BY r.created_at DESC
      `)
      return writeJson(res, 200, rows)
    }

    if (method === 'POST' && path === '/returns') {
      const body = await readBody(req)
      const { orderId, customerId, productId, warehouseCode, qty, reason, action } = body
      if (!productId || !warehouseCode || !qty) return writeJson(res, 400, { error: 'missing_fields' })

      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const year = new Date().getUTCFullYear()
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`rma:${year}`])
        const { rows: seqRows } = await client.query(
          `SELECT coalesce(max((substring(rma_ref from 'RMA-\\d{4}-(\\d+)'))::int), 0) as s FROM returns WHERE rma_ref LIKE $1`, [`RMA-${year}-%`])
        const rmaRef = `RMA-${year}-${String((seqRows[0]?.s ?? 0) + 1).padStart(4, '0')}`

        const restockAction = action || 'restock'
        const { rows } = await client.query(
          `INSERT INTO returns (rma_ref, order_id, customer_id, product_id, warehouse_code, qty, reason, action, status)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending') RETURNING *`,
          [rmaRef, orderId||null, customerId||null, productId, warehouseCode, toInt(qty), reason||null, restockAction])

        await auditLog(client, 'return_created', 'return', rows[0].id, warehouseCode, productId, null, null,
          `RMA ${rmaRef}: ${toInt(qty)} units, action: ${restockAction}`)
        await client.query('COMMIT')
        return writeJson(res, 201, rows[0])
      } catch (e) { await client.query('ROLLBACK'); throw e }
      finally { client.release() }
    }

    const returnProcessMatch = path.match(/^\/returns\/([^/]+)\/process$/)
    if (method === 'PATCH' && returnProcessMatch) {
      const id = returnProcessMatch[1]
      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const { rows: rRows } = await client.query(
          `SELECT * FROM returns WHERE id = $1 AND status = 'pending' FOR UPDATE`, [id])
        if (!rRows.length) { await client.query('ROLLBACK'); return writeJson(res, 400, { error: 'cannot_process' }) }
        const ret = rRows[0]

        if (ret.action === 'restock') {
          const { rows: wsRows } = await client.query(
            `UPDATE warehouse_stock SET qty_on_hand = qty_on_hand + $1
             WHERE product_id = $2 AND warehouse = $3 RETURNING qty_on_hand`,
            [ret.qty, ret.product_id, ret.warehouse_code])
          await auditLog(client, 'stock_returned', 'return', ret.id, ret.warehouse_code, ret.product_id,
            ret.qty, wsRows[0]?.qty_on_hand ?? ret.qty, `Restocked ${ret.qty} units via ${ret.rma_ref}`)
        } else {
          await auditLog(client, 'stock_written_off', 'return', ret.id, ret.warehouse_code, ret.product_id,
            0, null, `Written off ${ret.qty} units via ${ret.rma_ref}`)
        }

        await client.query(`UPDATE returns SET status = 'processed' WHERE id = $1`, [id])
        await client.query('COMMIT')
        return writeJson(res, 200, { status: 'processed', action: ret.action })
      } catch (e) { await client.query('ROLLBACK'); throw e }
      finally { client.release() }
    }

    // ── Auto-Reorder (generate PO drafts from low stock) ──
    if (method === 'POST' && path === '/auto-reorder') {
      const body = await readBody(req)
      const warehouseCode = body.warehouseCode || 'uk'

      const { rows: lowItems } = await pool.query(`
        SELECT ws.product_id, ws.qty_on_hand, ws.reorder_point, ws.max_stock,
          p.supplier_id, p.cost_price_gbp, p.case_pack, s.name as supplier_name
        FROM warehouse_stock ws
        JOIN products p ON p.id = ws.product_id
        LEFT JOIN suppliers s ON s.id = p.supplier_id
        WHERE ws.warehouse = $1 AND ws.qty_on_hand <= ws.reorder_point AND ws.reorder_point > 0
          AND p.supplier_id IS NOT NULL
      `, [warehouseCode])

      if (!lowItems.length) return writeJson(res, 200, { message: 'no_items_below_reorder', pos: [] })

      // Group by supplier
      const bySupplier = {}
      for (const item of lowItems) {
        if (!bySupplier[item.supplier_id]) bySupplier[item.supplier_id] = { supplierName: item.supplier_name, lines: [] }
        const orderQty = Math.max(item.max_stock - item.qty_on_hand, item.case_pack || 1)
        bySupplier[item.supplier_id].lines.push({
          productId: item.product_id, qtyOrdered: orderQty, unitCost: Number(item.cost_price_gbp)
        })
      }

      const createdPOs = []
      for (const [supplierId, data] of Object.entries(bySupplier)) {
        const res2 = await pool.connect()
        try {
          await res2.query('BEGIN')
          const year = new Date().getUTCFullYear()
          await res2.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`po:${year}`])
          const { rows: seqRows } = await res2.query(
            `SELECT coalesce(max((substring(po_ref from 'PO-\\d{4}-(\\d+)'))::int), 0) as s FROM purchase_orders WHERE po_ref LIKE $1`, [`PO-${year}-%`])
          const poRef = `PO-${year}-${String((seqRows[0]?.s ?? 0) + 1).padStart(4, '0')}`

          const { rows: poRows } = await res2.query(
            `INSERT INTO purchase_orders (po_ref, supplier_id, warehouse_code, status, notes)
             VALUES ($1,$2,$3,'draft',$4) RETURNING *`,
            [poRef, supplierId, warehouseCode, `Auto-generated reorder for ${data.lines.length} low-stock items`])

          for (const line of data.lines) {
            await res2.query(
              `INSERT INTO po_lines (po_id, product_id, qty_ordered, unit_cost_gbp) VALUES ($1,$2,$3,$4)`,
              [poRows[0].id, line.productId, line.qtyOrdered, line.unitCost])
          }

          await auditLog(res2, 'po_auto_created', 'purchase_order', poRows[0].id, warehouseCode, null, null, null,
            `Auto-reorder PO ${poRef}: ${data.lines.length} items for ${data.supplierName}`)
          await res2.query('COMMIT')
          createdPOs.push({ poRef, supplier: data.supplierName, lineCount: data.lines.length })
        } catch (e) { await res2.query('ROLLBACK'); throw e }
        finally { res2.release() }
      }

      return writeJson(res, 201, { pos: createdPOs })
    }

    // ── Packing Slip ──
    const packingSlipMatch = path.match(/^\/orders\/([^/]+)\/packing-slip$/)
    if (method === 'GET' && packingSlipMatch) {
      const orderId = packingSlipMatch[1]
      const { rows: oRows } = await pool.query(
        `SELECT o.*, c.company_name, c.billing_address, c.email FROM orders o
         LEFT JOIN customers c ON c.id = o.customer_id WHERE o.id = $1`, [orderId])
      if (!oRows.length) return writeJson(res, 404, { error: 'not_found' })
      const { rows: lines } = await pool.query(
        `SELECT ol.*, p.brand FROM order_lines ol LEFT JOIN products p ON p.id = ol.product_id
         WHERE ol.order_id = $1 ORDER BY p.brand, ol.product_name`, [orderId])
      return writeJson(res, 200, { order: oRows[0], lines })
    }

    // ── Reports ──
    if (method === 'GET' && path === '/reports/stock-valuation') {
      const { rows } = await pool.query(`
        SELECT w.code as warehouse, w.name as warehouse_name, w.currency,
          p.brand, count(*)::int as sku_count,
          sum(ws.qty_on_hand)::int as total_units,
          sum(ws.qty_on_hand * p.cost_price_gbp)::numeric(12,2) as cost_value,
          sum(ws.qty_on_hand * p.sell_price_gbp)::numeric(12,2) as retail_value
        FROM warehouse_stock ws
        JOIN products p ON p.id = ws.product_id
        JOIN warehouses w ON w.code = ws.warehouse
        WHERE ws.qty_on_hand > 0
        GROUP BY GROUPING SETS ((w.code, w.name, w.currency, p.brand), (w.code, w.name, w.currency))
        ORDER BY w.code, p.brand NULLS FIRST
      `)
      return writeJson(res, 200, rows)
    }

    if (method === 'GET' && path === '/reports/demand') {
      const { rows } = await pool.query(`
        SELECT p.id, p.name, p.sku, p.brand, p.cost_price_gbp, p.sell_price_gbp,
          coalesce(ord.total_ordered, 0)::int as total_ordered,
          coalesce(ord.order_count, 0)::int as order_count,
          coalesce(ord.revenue, 0)::numeric(12,2) as revenue,
          CASE WHEN coalesce(ord.total_ordered, 0) = 0 THEN 'dead_stock'
               WHEN coalesce(ord.total_ordered, 0) < 5 THEN 'slow_mover'
               WHEN coalesce(ord.total_ordered, 0) < 20 THEN 'steady'
               ELSE 'fast_mover' END as velocity,
          json_agg(json_build_object('warehouse', ws.warehouse, 'qty', ws.qty_on_hand)) as stock_by_warehouse
        FROM products p
        LEFT JOIN warehouse_stock ws ON ws.product_id = p.id
        LEFT JOIN LATERAL (
          SELECT sum(ol.qty_ordered)::int as total_ordered, count(DISTINCT ol.order_id)::int as order_count,
            sum(ol.qty_ordered * ol.unit_price_gbp)::numeric(12,2) as revenue
          FROM order_lines ol WHERE ol.product_id = p.id
        ) ord ON true
        GROUP BY p.id, p.name, p.sku, p.brand, p.cost_price_gbp, p.sell_price_gbp, ord.total_ordered, ord.order_count, ord.revenue
        ORDER BY coalesce(ord.total_ordered, 0) DESC
      `)
      return writeJson(res, 200, rows)
    }

    // ── Multi-currency pricing ──
    if (method === 'GET' && path === '/pricing') {
      const gbpUsd = 1.34
      const gbpEur = 1.17
      const { rows } = await pool.query(`
        SELECT p.id, p.name, p.sku, p.brand, p.cost_price_gbp, p.sell_price_gbp,
          p.cost_price_gbp * ${gbpUsd} as cost_usd, p.sell_price_gbp * ${gbpUsd} as sell_usd,
          p.cost_price_gbp * ${gbpEur} as cost_eur, p.sell_price_gbp * ${gbpEur} as sell_eur,
          round((p.sell_price_gbp - p.cost_price_gbp) / NULLIF(p.sell_price_gbp, 0) * 100, 1) as margin_pct
        FROM products p ORDER BY p.brand, p.name
      `)
      return writeJson(res, 200, { rates: { gbpUsd, gbpEur }, products: rows })
    }

    // ── Customer sub-routes (legacy) ──
    const custOrdersMatch = path.match(/^\/customers\/([^/]+)\/orders$/)
    if (method === 'GET' && custOrdersMatch) {
      const { rows } = await pool.query(`
        SELECT o.id, o.order_ref, o.status, o.currency, o.total_gbp, o.created_at,
          coalesce(sum(ol.qty_ordered),0)::int as total_units, count(ol.id)::int as line_count
        FROM orders o LEFT JOIN order_lines ol ON ol.order_id = o.id
        WHERE o.customer_id = $1 GROUP BY o.id ORDER BY o.created_at DESC
      `, [custOrdersMatch[1]])
      return writeJson(res, 200, rows)
    }

    const custInvoicesMatch = path.match(/^\/customers\/([^/]+)\/invoices$/)
    if (method === 'GET' && custInvoicesMatch) {
      return writeJson(res, 200, [])
    }

    if (method === 'PATCH' && custMatch) {
      const body = await readBody(req)
      const fields = [['companyName','company_name'],['contactName','contact_name'],['email','email'],['billingAddress','billing_address'],['currencyPreference','currency_preference'],['vatNumber','vat_number'],['notes','notes'],['region','region'],['customerType','customer_type'],['paymentTerms','payment_terms'],['creditLimitGbp','credit_limit_gbp'],['creditUsedGbp','credit_used_gbp'],['defaultDiscountPct','default_discount_pct']]
      const updates = []; const vals = []
      for (const [input, col] of fields) {
        if (body[input] !== undefined) { vals.push(body[input]); updates.push(`${col} = $${vals.length}`) }
      }
      if (!updates.length) { const c = await pool.query('SELECT * FROM customers WHERE id=$1',[custMatch[1]]); return writeJson(res,200,c.rows[0]) }
      vals.push(custMatch[1])
      const { rows } = await pool.query(`UPDATE customers SET ${updates.join(', ')} WHERE id = $${vals.length} RETURNING *`, vals)
      return rows.length ? writeJson(res, 200, rows[0]) : writeJson(res, 404, { error: 'not_found' })
    }

    // ── Invoices (legacy — supports existing React app) ──
    if (method === 'POST' && path === '/invoices') {
      const body = await readBody(req)
      const customerName = String(body.customerName ?? '').trim()
      const customerRegion = String(body.customerRegion ?? '').toLowerCase()
      const orderNumber = String(body.orderNumber ?? '').trim()
      const orderDate = body.orderDate || null
      const invoiceDate = body.invoiceDate || new Date().toISOString().slice(0, 10)
      const sourceFilename = String(body.sourceFilename ?? '').trim()
      const lines = Array.isArray(body.lines) ? body.lines : []

      if (!customerName) return writeJson(res, 400, { error: 'customer_name_required' })
      if (!lines.length) return writeJson(res, 400, { error: 'lines_required' })

      const validRegions = new Set(['uk', 'us', 'eu'])
      const safeRegion = validRegions.has(customerRegion) ? customerRegion : null

      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const { rows: custRows } = await client.query(
          `INSERT INTO customers (company_name, region) VALUES ($1,$2)
           ON CONFLICT (company_name) DO UPDATE SET region = coalesce(excluded.region, customers.region) RETURNING id`,
          [customerName, safeRegion])
        const customerId = custRows[0].id

        let totalUnits = 0, totalValue = 0
        for (const l of lines) { totalUnits += toInt(l.quantity); totalValue += toNum(l.subtotal) }

        const { rows: invRows } = await client.query(
          `INSERT INTO invoices (customer_id, order_number, order_date, invoice_date, total_units, total_value_gbp, source_filename)
           VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
          [customerId, orderNumber||null, orderDate||null, invoiceDate, totalUnits, totalValue.toFixed(2), sourceFilename||null])
        const invoiceId = invRows[0].id

        const whForRegion = safeRegion === 'us' ? 'us' : 'uk'
        const counts = { fulfilled: 0, partial: 0, backordered: 0 }

        for (const line of lines) {
          const sku = String(line.sku ?? '').trim()
          const qty = toInt(line.quantity)
          const { rows: pRows } = await client.query('SELECT id FROM products WHERE sku=$1 LIMIT 1', [sku])
          const productId = pRows[0]?.id ?? null
          let status = 'backordered', qf = 0, qb = qty

          if (productId) {
            const { rows: sRows } = await client.query(
              `SELECT id, qty_on_hand, qty_reserved FROM warehouse_stock WHERE product_id=$1 AND warehouse=$2 FOR UPDATE`, [productId, whForRegion])
            if (sRows.length) {
              const avail = sRows[0].qty_on_hand - sRows[0].qty_reserved
              if (avail >= qty) { qf=qty; qb=0; status='fulfilled'; await client.query('UPDATE warehouse_stock SET qty_reserved=qty_reserved+$1 WHERE id=$2',[qty,sRows[0].id]) }
              else if (avail > 0) { qf=avail; qb=qty-avail; status='partial'; await client.query('UPDATE warehouse_stock SET qty_reserved=qty_reserved+$1 WHERE id=$2',[avail,sRows[0].id]) }
            }
          }
          counts[status]++
          await client.query(
            `INSERT INTO invoice_lines (invoice_id,product_id,sku,description,barcode,trade_price_gbp,quantity,subtotal_gbp,fulfilment_status,qty_fulfilled,qty_backordered)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
            [invoiceId, productId, sku, String(line.description??'').trim(), String(line.barcode??'').trim()||null,
             toNum(line.tradePrice).toFixed(2), qty, toNum(line.subtotal).toFixed(2), status, qf, qb])
        }
        await client.query('COMMIT')
        return writeJson(res, 201, { invoiceId, lineCount: lines.length, fulfilmentSummary: counts })
      } catch (e) {
        await client.query('ROLLBACK')
        if (e.code === '23505') return writeJson(res, 409, { error: 'duplicate_invoice' })
        throw e
      } finally { client.release() }
    }

    // ── Backorders (legacy) ──
    if (method === 'GET' && path === '/backorders') {
      const { rows } = await pool.query(`
        SELECT il.id as line_id, il.sku, il.description, il.quantity, il.qty_fulfilled, il.qty_backordered,
          il.fulfilment_status, il.trade_price_gbp, il.created_at as line_created_at,
          i.id as invoice_id, i.order_number, i.invoice_date,
          c.id as customer_id, c.company_name, c.region,
          coalesce(loyalty.total_spend,0)::numeric(12,2) as customer_total_spend
        FROM invoice_lines il JOIN invoices i ON il.invoice_id = i.id
        LEFT JOIN customers c ON i.customer_id = c.id
        LEFT JOIN LATERAL (SELECT sum(inv.total_value_gbp) as total_spend FROM invoices inv WHERE inv.customer_id = c.id) loyalty ON true
        WHERE il.fulfilment_status IN ('backordered','partial')
        ORDER BY il.sku, coalesce(loyalty.total_spend,0) DESC
      `)
      return writeJson(res, 200, rows)
    }

    // ── Stretch Summary (legacy) ──
    if (method === 'GET' && path === '/stretch/summary') {
      const [totals, top, bottom, monthly, custs, mismatch] = await Promise.all([
        pool.query(`SELECT count(*)::int as total_invoices, coalesce(sum(total_value_gbp),0)::numeric(12,2) as total_revenue,
          coalesce(sum(total_units),0)::int as total_units, min(invoice_date) as earliest_date, max(invoice_date) as latest_date FROM invoices`),
        pool.query(`SELECT sku,description,sum(quantity)::int as total_units,sum(subtotal_gbp)::numeric(12,2) as total_revenue,
          count(distinct invoice_id)::int as invoice_count FROM invoice_lines GROUP BY sku,description ORDER BY sum(quantity) DESC LIMIT 10`),
        pool.query(`SELECT sku,description,sum(quantity)::int as total_units,sum(subtotal_gbp)::numeric(12,2) as total_revenue,
          count(distinct invoice_id)::int as invoice_count FROM invoice_lines GROUP BY sku,description ORDER BY sum(quantity) ASC LIMIT 10`),
        pool.query(`SELECT to_char(invoice_date,'YYYY-MM') as month,sum(total_value_gbp)::numeric(12,2) as revenue,
          sum(total_units)::int as units,count(*)::int as invoice_count FROM invoices GROUP BY to_char(invoice_date,'YYYY-MM') ORDER BY month`),
        pool.query(`SELECT c.company_name,c.region,count(i.id)::int as invoice_count,
          coalesce(sum(i.total_value_gbp),0)::numeric(12,2) as total_revenue,coalesce(sum(i.total_units),0)::int as total_units
          FROM customers c INNER JOIN invoices i ON i.customer_id=c.id GROUP BY c.id,c.company_name,c.region ORDER BY sum(i.total_value_gbp) DESC`),
        pool.query(`SELECT DISTINCT il.sku,il.description,c.region as customer_region,
          coalesce(ws_uk.qty_on_hand,0)::int as uk_stock,coalesce(ws_us.qty_on_hand,0)::int as us_stock
          FROM invoice_lines il JOIN invoices i ON il.invoice_id=i.id JOIN customers c ON i.customer_id=c.id
          LEFT JOIN products p ON il.sku=p.sku
          LEFT JOIN warehouse_stock ws_uk ON p.id=ws_uk.product_id AND ws_uk.warehouse='uk'
          LEFT JOIN warehouse_stock ws_us ON p.id=ws_us.product_id AND ws_us.warehouse='us'
          WHERE c.region IS NOT NULL AND ((c.region='us' AND coalesce(ws_us.qty_on_hand,0)=0) OR (c.region='uk' AND coalesce(ws_uk.qty_on_hand,0)=0))
          ORDER BY il.sku`)
      ])
      const t = totals.rows[0]
      return writeJson(res, 200, {
        totalInvoices: t.total_invoices, totalRevenue: toNum(t.total_revenue), totalUnits: t.total_units,
        dateRange: { earliest: t.earliest_date, latest: t.latest_date },
        topProducts: top.rows, bottomProducts: bottom.rows, revenueByMonth: monthly.rows,
        customerBreakdown: custs.rows, warehouseMismatch: mismatch.rows
      })
    }

    // ── Intelligence: Stockout Predictions ──
    if (method === 'GET' && path === '/intelligence/stockout-predictions') {
      const { rows } = await pool.query(`
        SELECT
          ws.warehouse, ws.qty_on_hand, ws.reorder_point,
          p.id as product_id, p.name, p.sku, p.brand, p.cost_price_gbp,
          s.name as supplier_name, s.lead_time_days,
          coalesce(demand.daily_rate, 0) as burn_rate_per_day,
          CASE
            WHEN coalesce(demand.daily_rate, 0) <= 0 THEN NULL
            ELSE round(ws.qty_on_hand / demand.daily_rate, 1)
          END as days_until_stockout,
          CASE
            WHEN coalesce(demand.daily_rate, 0) <= 0 THEN 'no_demand'
            WHEN ws.qty_on_hand = 0 THEN 'no_demand'
            WHEN ws.qty_on_hand / demand.daily_rate < 14 THEN 'critical'
            WHEN ws.qty_on_hand / demand.daily_rate < 30 THEN 'warning'
            ELSE 'healthy'
          END as severity
        FROM warehouse_stock ws
        JOIN products p ON p.id = ws.product_id
        LEFT JOIN suppliers s ON s.id = p.supplier_id
        LEFT JOIN LATERAL (
          SELECT coalesce(sum(ol.qty_ordered)::numeric / GREATEST(extract(epoch from now() - min(o.created_at)) / 86400, 1), 0) as daily_rate
          FROM order_lines ol
          JOIN orders o ON o.id = ol.order_id
          WHERE ol.product_id = p.id AND o.warehouse_code = ws.warehouse
        ) demand ON true
        WHERE ws.qty_on_hand > 0
        ORDER BY
          CASE
            WHEN coalesce(demand.daily_rate, 0) <= 0 THEN 4
            WHEN ws.qty_on_hand / demand.daily_rate < 14 THEN 1
            WHEN ws.qty_on_hand / demand.daily_rate < 30 THEN 2
            ELSE 3
          END,
          coalesce(ws.qty_on_hand / NULLIF(demand.daily_rate, 0), 9999)
      `)
      return writeJson(res, 200, rows)
    }

    // ── Intelligence: Route Order ──
    const routeMatch = path === '/intelligence/route-order'
    if (method === 'GET' && routeMatch) {
      const customerId = url.searchParams.get('customerId')
      const productIds = (url.searchParams.get('productIds') || '').split(',').filter(Boolean)
      const qtys = (url.searchParams.get('qtys') || '').split(',').map(Number)

      if (!customerId || !productIds.length) return writeJson(res, 400, { error: 'customerId and productIds required' })

      const { rows: custRows } = await pool.query('SELECT * FROM customers WHERE id = $1', [customerId])
      const customer = custRows[0]
      if (!customer) return writeJson(res, 404, { error: 'customer_not_found' })

      const region = customer.region || 'uk'
      const preferred = region === 'us' ? 'us' : region === 'eu' ? 'eu' : 'uk'
      const whNames = { uk: 'London (UK)', us: 'Charleston (US)', eu: 'Hanover (DE)' }
      const shippingCost = { uk: { uk: 0, us: 2, eu: 1 }, us: { uk: 2, us: 0, eu: 2 }, eu: { uk: 1, us: 2, eu: 0 } }

      const options = []
      for (const wh of ['uk', 'us', 'eu']) {
        let score = 50
        const breakdown = []
        const warnings = []
        let canFulfill = true

        // Check stock availability
        let allInStock = true
        for (let i = 0; i < productIds.length; i++) {
          const { rows } = await pool.query(
            'SELECT qty_on_hand - qty_reserved as available FROM warehouse_stock WHERE product_id = $1 AND warehouse = $2',
            [productIds[i], wh]
          )
          const avail = rows[0]?.available ?? 0
          if (avail < (qtys[i] || 1)) { allInStock = false; canFulfill = false }
        }

        if (allInStock) { score += 30; breakdown.push({ factor: 'Full stock', points: 30 }) }
        else { score -= 20; breakdown.push({ factor: 'Partial stock', points: -20 }); warnings.push('Cannot fulfill all items from this warehouse') }

        // Proximity to customer
        const dist = shippingCost[region]?.[wh] ?? 2
        if (dist === 0) { score += 20; breakdown.push({ factor: 'Local warehouse', points: 20 }) }
        else if (dist === 1) { score += 10; breakdown.push({ factor: 'Near region', points: 10 }) }
        else { score -= 5; breakdown.push({ factor: 'Cross-region', points: -5 }); warnings.push('Cross-region shipping adds cost and transit time') }

        options.push({ warehouse: wh, warehouseName: whNames[wh], score, canFulfill, breakdown, warnings })
      }

      options.sort((a, b) => b.score - a.score)
      const recommended = options[0].warehouse

      return writeJson(res, 200, { customerRegion: region, preferredWarehouse: preferred, recommended, options })
    }

    // ── Intelligence: Reorder Recommendations ──
    if (method === 'GET' && path === '/intelligence/reorder-recommendations') {
      const warehouseCode = url.searchParams.get('warehouseCode') || 'uk'
      const { rows } = await pool.query(`
        SELECT
          ws.product_id, ws.qty_on_hand as current_stock, ws.reorder_point, ws.max_stock,
          p.name, p.sku, p.brand, p.cost_price_gbp, p.case_pack,
          p.supplier_id, s.name as supplier_name, s.lead_time_days, s.currency as supplier_currency,
          coalesce(demand.daily_rate, 0) as burn_rate_per_day,
          CASE
            WHEN coalesce(demand.daily_rate, 0) <= 0 THEN NULL
            ELSE round(ws.qty_on_hand / demand.daily_rate, 1)
          END as days_until_stockout
        FROM warehouse_stock ws
        JOIN products p ON p.id = ws.product_id
        LEFT JOIN suppliers s ON s.id = p.supplier_id
        LEFT JOIN LATERAL (
          SELECT coalesce(sum(ol.qty_ordered)::numeric / GREATEST(extract(epoch from now() - min(o.created_at)) / 86400, 1), 0) as daily_rate
          FROM order_lines ol JOIN orders o ON o.id = ol.order_id
          WHERE ol.product_id = p.id AND o.warehouse_code = ws.warehouse
        ) demand ON true
        WHERE ws.warehouse = $1
          AND p.supplier_id IS NOT NULL
          AND ws.qty_on_hand > 0
          AND (
            (ws.qty_on_hand <= ws.reorder_point AND ws.reorder_point > 0)
            OR (
              coalesce(demand.daily_rate, 0) > 0
              AND ws.qty_on_hand / demand.daily_rate < coalesce(s.lead_time_days, 14)
            )
          )
        ORDER BY coalesce(ws.qty_on_hand / NULLIF(demand.daily_rate, 0), 9999), p.brand, p.name
      `, [warehouseCode])

      // Group by supplier
      const supplierMap = {}
      for (const r of rows) {
        if (!supplierMap[r.supplier_id]) {
          supplierMap[r.supplier_id] = {
            supplierId: r.supplier_id, supplierName: r.supplier_name,
            leadTimeDays: r.lead_time_days, supplierCurrency: r.supplier_currency || 'GBP',
            products: [], totalItems: 0, totalCost: 0
          }
        }
        const sg = supplierMap[r.supplier_id]
        const recommendedQty = Math.max(r.max_stock - r.current_stock, r.case_pack || 1)
        const estimatedCost = recommendedQty * Number(r.cost_price_gbp)
        const burnRate = Number(r.burn_rate_per_day)
        const daysLeft = r.days_until_stockout
        const urgency = daysLeft !== null && daysLeft < 7 ? 'critical' : daysLeft !== null && daysLeft < 14 ? 'urgent'
          : r.current_stock <= r.reorder_point ? 'reorder' : 'preventive'

        sg.products.push({
          productId: r.product_id, name: r.name, sku: r.sku,
          currentStock: r.current_stock, reorderPoint: r.reorder_point,
          burnRatePerDay: burnRate, daysUntilStockout: daysLeft,
          recommendedQty, costPerUnit: Number(r.cost_price_gbp), estimatedCost, urgency
        })
        sg.totalItems++
        sg.totalCost += estimatedCost
      }

      const suppliers = Object.values(supplierMap).sort((a, b) => b.totalCost - a.totalCost)
      return writeJson(res, 200, { totalSuppliers: suppliers.length, totalProducts: rows.length, suppliers })
    }

    // ── Automation: Backorder Auto-Fulfilment ──
    // When called after stock receipt, finds backordered order lines that can now be fulfilled
    if (method === 'POST' && path === '/automation/backorder-fulfil') {
      const body = await readBody(req)
      const warehouseCode = body.warehouseCode || ''

      // Find backordered/partial order lines where stock is now available
      const { rows: backorders } = await pool.query(`
        SELECT ol.id as line_id, ol.order_id, ol.product_id, ol.qty_backordered,
          ol.product_name, ol.sku, o.order_ref, o.warehouse_code,
          ws.qty_on_hand - ws.qty_reserved as qty_available
        FROM order_lines ol
        JOIN orders o ON o.id = ol.order_id
        JOIN warehouse_stock ws ON ws.product_id = ol.product_id AND ws.warehouse = o.warehouse_code
        WHERE ol.fulfilment_status IN ('backordered', 'partial')
          AND ol.qty_backordered > 0
          AND ws.qty_on_hand - ws.qty_reserved > 0
          AND ($1 = '' OR o.warehouse_code = $1)
        ORDER BY o.created_at ASC
      `, [warehouseCode])

      if (!backorders.length) {
        return writeJson(res, 200, { message: 'no_backorders_to_fulfil', fulfilled: [] })
      }

      const client = await pool.connect()
      const fulfilled = []
      try {
        await client.query('BEGIN')

        for (const bo of backorders) {
          // Re-check availability inside transaction
          const { rows: wsCheck } = await client.query(
            `SELECT qty_on_hand - qty_reserved as avail FROM warehouse_stock
             WHERE product_id = $1 AND warehouse = $2 FOR UPDATE`,
            [bo.product_id, bo.warehouse_code])
          const avail = wsCheck[0]?.avail ?? 0
          if (avail <= 0) continue

          const canFulfil = Math.min(bo.qty_backordered, avail)

          // Reserve stock
          await client.query(
            `UPDATE warehouse_stock SET qty_reserved = qty_reserved + $1
             WHERE product_id = $2 AND warehouse = $3`,
            [canFulfil, bo.product_id, bo.warehouse_code])

          // Update order line
          const newBackordered = bo.qty_backordered - canFulfil
          const newStatus = newBackordered <= 0 ? 'fulfilled' : 'partial'
          await client.query(
            `UPDATE order_lines SET qty_fulfilled = qty_fulfilled + $1, qty_backordered = $2, fulfilment_status = $3
             WHERE id = $4`,
            [canFulfil, newBackordered, newStatus, bo.line_id])

          await auditLog(client, 'backorder_fulfilled', 'order', bo.order_id,
            bo.warehouse_code, bo.product_id, canFulfil, null,
            `Auto-fulfilled ${canFulfil} of ${bo.qty_backordered} backordered units for ${bo.order_ref}`)

          fulfilled.push({
            orderRef: bo.order_ref, productName: bo.product_name, sku: bo.sku,
            qtyFulfilled: canFulfil, remaining: newBackordered, status: newStatus
          })
        }

        await client.query('COMMIT')
        return writeJson(res, 200, { fulfilled, totalFulfilled: fulfilled.length })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Automation: Returns Auto-Disposition ──
    // Applies rule-based decisions to pending returns
    if (method === 'POST' && path === '/automation/returns-disposition') {
      // Rules: damaged/expired → write_off, everything else → restock
      const WRITE_OFF_REASONS = ['damaged', 'defective', 'expired', 'quality_issue']

      const { rows: pending } = await pool.query(
        `SELECT r.*, p.name as product_name, p.sku, p.cost_price_gbp
         FROM returns r LEFT JOIN products p ON p.id = r.product_id
         WHERE r.status = 'pending'`)

      if (!pending.length) {
        return writeJson(res, 200, { message: 'no_pending_returns', processed: [] })
      }

      const client = await pool.connect()
      const processed = []
      try {
        await client.query('BEGIN')

        for (const ret of pending) {
          // Determine reason code from free-text reason field if not set
          let reasonCode = ret.reason_code
          if (!reasonCode && ret.reason) {
            const lower = ret.reason.toLowerCase()
            if (lower.includes('damage') || lower.includes('broken')) reasonCode = 'damaged'
            else if (lower.includes('defect') || lower.includes('faulty')) reasonCode = 'defective'
            else if (lower.includes('wrong') || lower.includes('incorrect')) reasonCode = 'wrong_item'
            else if (lower.includes('expired') || lower.includes('expir')) reasonCode = 'expired'
            else if (lower.includes('quality')) reasonCode = 'quality_issue'
            else if (lower.includes('changed mind') || lower.includes('unwanted')) reasonCode = 'customer_change'
            else reasonCode = 'customer_change'
          }
          if (!reasonCode) reasonCode = 'customer_change'

          // Update reason_code
          await client.query('UPDATE returns SET reason_code = $1 WHERE id = $2', [reasonCode, ret.id])

          // Apply disposition rule
          const action = WRITE_OFF_REASONS.includes(reasonCode) ? 'write_off' : 'restock'
          await client.query('UPDATE returns SET action = $1 WHERE id = $2', [action, ret.id])

          // Process the return
          if (action === 'restock') {
            const { rows: wsRows } = await client.query(
              `UPDATE warehouse_stock SET qty_on_hand = qty_on_hand + $1
               WHERE product_id = $2 AND warehouse = $3 RETURNING qty_on_hand`,
              [ret.qty, ret.product_id, ret.warehouse_code])
            await auditLog(client, 'stock_returned', 'return', ret.id, ret.warehouse_code, ret.product_id,
              ret.qty, wsRows[0]?.qty_on_hand ?? ret.qty,
              `Auto-restocked ${ret.qty} units via ${ret.rma_ref} (${reasonCode})`)
          } else {
            await auditLog(client, 'stock_written_off', 'return', ret.id, ret.warehouse_code, ret.product_id,
              0, null, `Auto-written off ${ret.qty} units via ${ret.rma_ref} (${reasonCode})`)
          }

          await client.query(`UPDATE returns SET status = 'processed' WHERE id = $1`, [ret.id])

          processed.push({
            rmaRef: ret.rma_ref, productName: ret.product_name, sku: ret.sku,
            qty: ret.qty, reasonCode, action, warehouse: ret.warehouse_code
          })
        }

        await client.query('COMMIT')
        return writeJson(res, 200, { processed, totalProcessed: processed.length,
          restocked: processed.filter(p => p.action === 'restock').length,
          writtenOff: processed.filter(p => p.action === 'write_off').length })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Automation: ABC Classification & Cycle Count Scheduling ──
    if (method === 'POST' && path === '/automation/abc-classify') {
      // Classify products by revenue contribution (ABC analysis)
      const { rows: ranked } = await pool.query(`
        WITH product_revenue AS (
          SELECT ol.product_id, coalesce(sum(ol.qty_ordered * ol.unit_price_gbp), 0)::numeric as revenue
          FROM order_lines ol
          JOIN orders o ON o.id = ol.order_id
          WHERE o.created_at >= now() - interval '90 days'
          GROUP BY ol.product_id
        ),
        total AS (
          SELECT coalesce(sum(revenue), 1) as total_revenue FROM product_revenue
        ),
        classified AS (
          SELECT p.id, p.name, p.sku, p.brand, coalesce(pr.revenue, 0) as revenue,
            sum(coalesce(pr.revenue, 0)) OVER (ORDER BY coalesce(pr.revenue, 0) DESC) / t.total_revenue as cumulative_pct
          FROM products p
          LEFT JOIN product_revenue pr ON pr.product_id = p.id
          CROSS JOIN total t
        )
        SELECT id, name, sku, brand, revenue,
          CASE
            WHEN cumulative_pct <= 0.8 THEN 'A'
            WHEN cumulative_pct <= 0.95 THEN 'B'
            ELSE 'C'
          END as abc_class
        FROM classified
        ORDER BY revenue DESC
      `)

      // Update classifications
      const counts = { A: 0, B: 0, C: 0 }
      for (const r of ranked) {
        await pool.query('UPDATE products SET abc_class = $1 WHERE id = $2', [r.abc_class, r.id])
        counts[r.abc_class]++
      }

      return writeJson(res, 200, {
        classified: ranked.length,
        counts,
        summary: `A-class: ${counts.A} SKUs (count weekly), B-class: ${counts.B} (count monthly), C-class: ${counts.C} (count quarterly)`,
        products: ranked.slice(0, 20).map(r => ({ name: r.name, sku: r.sku, brand: r.brand, revenue: Number(r.revenue), class: r.abc_class }))
      })
    }

    // Auto-generate cycle count tasks based on ABC classification
    if (method === 'POST' && path === '/automation/schedule-cycle-counts') {
      const body = await readBody(req)
      const warehouseCode = body.warehouseCode || 'uk'

      // Determine which class needs counting based on schedule:
      // A = weekly, B = monthly (every 4 weeks), C = quarterly (every 13 weeks)
      const dayOfYear = Math.floor((Date.now() - new Date(new Date().getFullYear(), 0, 0)) / 86400000)
      const weekNum = Math.floor(dayOfYear / 7)

      const classesToCount = ['A'] // A is always counted
      if (weekNum % 4 === 0) classesToCount.push('B')
      if (weekNum % 13 === 0) classesToCount.push('C')

      // Check if there's already an active count for this warehouse this week
      const { rows: existing } = await pool.query(
        `SELECT id FROM cycle_counts WHERE warehouse_code = $1 AND status IN ('planned','in_progress')
         AND created_at >= now() - interval '5 days'`, [warehouseCode])
      if (existing.length) {
        return writeJson(res, 200, { message: 'active_count_exists', existingId: existing[0].id })
      }

      const client = await pool.connect()
      try {
        await client.query('BEGIN')

        const year = new Date().getUTCFullYear()
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`cc:${year}`])
        const { rows: seqRows } = await client.query(
          `SELECT coalesce(max((substring(count_ref from 'CC-\\d{4}-(\\d+)'))::int), 0) as s
           FROM cycle_counts WHERE count_ref LIKE $1`, [`CC-${year}-%`])
        const countRef = `CC-${year}-${String((seqRows[0]?.s ?? 0) + 1).padStart(4, '0')}`

        const scope = classesToCount.length === 3 ? 'full' : 'brand'
        const scopeFilter = classesToCount.join(',')

        const { rows: ccRows } = await client.query(
          `INSERT INTO cycle_counts (count_ref, warehouse_code, scope, scope_filter, notes, planned_date, status)
           VALUES ($1, $2, $3, $4, $5, CURRENT_DATE, 'planned') RETURNING *`,
          [countRef, warehouseCode, scope, scopeFilter,
           `Auto-scheduled ABC count: classes ${classesToCount.join(', ')} (week ${weekNum})`])
        const cc = ccRows[0]

        // Add lines for products in the target classes
        const { rows: stockRows } = await client.query(`
          SELECT ws.id as ws_id, ws.product_id, ws.qty_on_hand, ws.location
          FROM warehouse_stock ws
          JOIN products p ON p.id = ws.product_id
          WHERE ws.warehouse = $1 AND p.abc_class = ANY($2)
          ORDER BY p.abc_class, ws.location, p.brand, p.name
        `, [warehouseCode, classesToCount])

        for (const s of stockRows) {
          await client.query(
            `INSERT INTO cycle_count_lines (cycle_count_id, product_id, warehouse_stock_id, expected_qty, location)
             VALUES ($1,$2,$3,$4,$5)`,
            [cc.id, s.product_id, s.ws_id, s.qty_on_hand, s.location])
        }

        await auditLog(client, 'cycle_count_created', 'cycle_count', cc.id, warehouseCode, null, null, null,
          `Auto-scheduled ${countRef}: ABC classes ${classesToCount.join(',')} (${stockRows.length} items)`)

        await client.query('COMMIT')
        return writeJson(res, 201, {
          countRef, warehouseCode, classes: classesToCount,
          lineCount: stockRows.length, weekNumber: weekNum
        })
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
      }
    }

    // ── Automation: Supplier Lead Time Auto-Update ──
    if (method === 'POST' && path === '/automation/update-lead-times') {
      // Calculate actual lead times from PO submit → receive dates
      const { rows: stats } = await pool.query(`
        SELECT po.supplier_id, s.name as supplier_name, s.lead_time_days as promised_days,
          count(*)::int as po_count,
          round(avg(EXTRACT(DAY FROM po.received_at - po.submitted_at)), 1) as avg_actual_days,
          round(min(EXTRACT(DAY FROM po.received_at - po.submitted_at)), 1) as min_actual_days,
          round(max(EXTRACT(DAY FROM po.received_at - po.submitted_at)), 1) as max_actual_days
        FROM purchase_orders po
        JOIN suppliers s ON s.id = po.supplier_id
        WHERE po.status = 'received'
          AND po.submitted_at IS NOT NULL AND po.received_at IS NOT NULL
          AND po.received_at > po.submitted_at
        GROUP BY po.supplier_id, s.name, s.lead_time_days
        HAVING count(*) >= 1
      `)

      const updates = []
      for (const s of stats) {
        const actualDays = Number(s.avg_actual_days)
        const promisedDays = s.promised_days

        // Update actual_lead_time_days
        await pool.query('UPDATE suppliers SET actual_lead_time_days = $1 WHERE id = $2',
          [actualDays, s.supplier_id])

        // Auto-adjust promised lead time if actual consistently differs by >20%
        let adjusted = false
        if (s.po_count >= 2 && Math.abs(actualDays - promisedDays) / promisedDays > 0.2) {
          // Adjust to a weighted average: 70% actual + 30% promised (rounded up for safety)
          const newLeadTime = Math.ceil(actualDays * 0.7 + promisedDays * 0.3)
          if (newLeadTime !== promisedDays) {
            await pool.query('UPDATE suppliers SET lead_time_days = $1 WHERE id = $2',
              [newLeadTime, s.supplier_id])
            adjusted = true

            await auditLogSimple('lead_time_adjusted', 'supplier', s.supplier_id, null, null, null, null,
              `${s.supplier_name}: lead time adjusted from ${promisedDays}d to ${newLeadTime}d (actual avg: ${actualDays}d across ${s.po_count} POs)`)
          }
        }

        updates.push({
          supplierName: s.supplier_name, supplierId: s.supplier_id,
          promisedDays, actualAvgDays: actualDays, minDays: Number(s.min_actual_days),
          maxDays: Number(s.max_actual_days), poCount: s.po_count, adjusted,
          newLeadTime: adjusted ? Math.ceil(actualDays * 0.7 + promisedDays * 0.3) : promisedDays
        })
      }

      return writeJson(res, 200, {
        suppliers: updates, totalAnalyzed: updates.length,
        totalAdjusted: updates.filter(u => u.adjusted).length
      })
    }

    // ── Automation: Status/Summary ──
    if (method === 'GET' && path === '/automation/status') {
      const [backorders, pendingReturns, lastClassification, leadTimeStats] = await Promise.all([
        pool.query(`
          SELECT count(*)::int as count, coalesce(sum(ol.qty_backordered),0)::int as total_units
          FROM order_lines ol WHERE ol.fulfilment_status IN ('backordered','partial') AND ol.qty_backordered > 0
        `),
        pool.query(`SELECT count(*)::int as count FROM returns WHERE status = 'pending'`),
        pool.query(`
          SELECT abc_class, count(*)::int as count FROM products GROUP BY abc_class ORDER BY abc_class
        `),
        pool.query(`
          SELECT s.name, s.lead_time_days as promised, s.actual_lead_time_days as actual
          FROM suppliers s ORDER BY s.name
        `)
      ])

      return writeJson(res, 200, {
        backorderFulfilment: {
          pendingLines: backorders.rows[0].count,
          pendingUnits: backorders.rows[0].total_units,
          action: 'POST /api/automation/backorder-fulfil'
        },
        returnsDisposition: {
          pendingReturns: pendingReturns.rows[0].count,
          action: 'POST /api/automation/returns-disposition'
        },
        abcClassification: {
          classes: Object.fromEntries(lastClassification.rows.map(r => [r.abc_class, r.count])),
          action: 'POST /api/automation/abc-classify'
        },
        supplierLeadTimes: {
          suppliers: leadTimeStats.rows.map(s => ({
            name: s.name, promisedDays: s.promised,
            actualDays: s.actual ? Number(s.actual) : null
          })),
          action: 'POST /api/automation/update-lead-times'
        }
      })
    }

    // ── Fallback ──
    writeJson(res, 404, { error: 'not_found' })
  } catch (err) {
    console.error('Request error:', err)
    if (err.message === 'invalid_json') return writeJson(res, 400, { error: 'invalid_json' })
    if (err.message === 'too_large') return writeJson(res, 413, { error: 'too_large' })
    writeJson(res, 500, { error: 'internal_error' })
  }
})

server.listen(PORT, () => {
  console.log(`Telemachus WMS API listening on :${PORT}`)
})

process.on('SIGINT', () => { server.close(); pool.end(); process.exit(0) })
process.on('SIGTERM', () => { server.close(); pool.end(); process.exit(0) })
