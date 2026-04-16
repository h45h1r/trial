# Fox & Fable WMS — System Overview

A fully functional Warehouse Management System built in one day for Fox & Fable Toy Sellers — a wholesale and retail toy business operating across three international warehouses in London (UK), Charleston (US), and Hanover (Germany).

**Live:** https://trial.foxandfabletoysellers.com/dashboard.html

---

## Architecture

| Layer | Technology |
|-------|-----------|
| API Server | Node.js (ES modules, node:http, pg) on DigitalOcean |
| Database | PostgreSQL via Supabase — 16 tables, referential integrity, check constraints |
| Frontend | Standalone HTML pages, Inter font, shared CSS design system |
| Process Manager | PM2 with ecosystem config |
| Web Server | Nginx with SSL (Let's Encrypt) |
| Domain | trial.foxandfabletoysellers.com |

---

## Systems Built

### Core Warehouse Operations

**1. Multi-Warehouse Inventory Management**
Real-time stock visibility across London, Charleston, and Hanover. Per-warehouse stock levels with search, status filtering (healthy/low/out of stock), and manual adjustments with reason tracking. Every stock change is audited.

**2. Bin-Level Location Tracking**
Every stock record has a warehouse location in Aisle-Rack-Shelf format (e.g. A-01-03). Editable from the inventory page, changes audited. Enables zone-based cycle counting and reduces pick times.

**3. Purchase Order Lifecycle**
Create POs to suppliers, submit, receive goods (partial or full). Stock increases automatically on receipt. Products filtered by supplier — prevents cross-supplier ordering. Double-click protection prevents duplicate submissions. Draft POs are deletable.

**4. Inter-Warehouse Stock Transfers**
Request, dispatch, and receive transfers between any two warehouses. Stock deducted from source on dispatch, added to destination on receipt. Validates stock availability before dispatch. In-transit visibility.

**5. Order Fulfilment Pipeline**
Kanban-style pipeline: Confirmed → Picking → Packing → Shipped. Orders allocated to specific warehouses. Each stage progression enforced via state machine — the API prevents invalid transitions. Stock deducted on ship.

**6. Partial Fulfilment Review Workflow**
When an order exceeds available stock, it's flagged for review instead of being rejected. Warehouse operators see a review queue and choose per line item: ship what's available, enter a custom quantity, or hold for full stock. Stock is only reserved after the review decision. The B2B portal shows customers a "pending review" status.

**7. Returns / RMA Processing**
Create return authorisations with reason and action (restock, write off, or replace). Processing a restock return adds units back to inventory. Every return has a reason recorded.

**8. Cycle Counting**
Stock verification tasks scoped by warehouse zone, brand, or random sample. Count lines track expected vs actual quantities. Completing a count auto-adjusts stock for discrepancies.

---

### Commercial & Wholesale

**9. Wholesale Operations**
Full wholesale commercial mechanics:
- **Customer types:** retail, wholesale, distributor — each with different terms
- **Payment terms:** prepaid, net-15, net-30, net-60, net-90 per customer
- **Credit limits:** enforced at order placement, with visual credit usage tracking
- **Minimum order quantities:** per product, enforced at checkout
- **Volume tiered pricing:** 268 price tiers across all brands (e.g. Games Workshop: 5+ → 3% off, 10+ → 5%, 25+ → 8%). System auto-applies the best available discount
- **Customer default discount:** blanket percentage for preferred accounts

**10. Multi-Currency Pricing**
All 98 products displayed with cost and sell prices in GBP, USD, and EUR. Margin percentage calculated and colour-coded. Exchange rates configurable.

**11. B2B Customer Portal**
Self-service ordering for wholesale customers. Customer selector with account type badges, credit status display, stock availability grid filtered by warehouse region. Cart shows per-item discounts, total savings, and credit remaining. Backorder warnings when ordering above available stock.

Retail orders flow through the same fulfilment pipeline as wholesale — the distinction is handled at the customer type level. Retail customers have no MOQs, no credit terms, and pay RRP rather than trade price. This means a single system serves both channels without duplicating workflows.

**12. Historical Price Tracking**
230 historical price entries across 93 products. Inventory page shows trend arrows (red ▲ increase, green ▼ decrease) next to each cost. Hover popover shows full timeline of price changes with dates, old→new values, and percentage change.

**13. Customer & Supplier Management**
Customer database with region, currency preference, VAT number, order history, and total spend. Supplier directory with lead times, currency, and expandable product catalogues.

---

### Intelligence & Automation

**14. Smart Order Routing**
Automatically recommends the optimal warehouse for each order. Scores based on: customer region match (+100), stock availability (+50), impact on reorder points (-10 per item), and surplus balance (+20). When warehouse is not specified on order creation, the system auto-selects.

**15. Stockout Prediction**
Calculates daily burn rate per product per warehouse from shipped order history over 90 days. Projects days until stockout. Classifies items as critical (<14 days), warning (<30 days), healthy, or no demand. Sorted by urgency.

**16. Intelligent Reorder Recommendations**
For products below reorder point or predicted to stockout within supplier lead time: calculates recommended order quantity based on burn rate × (lead time + 14 days safety buffer) − current stock. Rounds up to nearest case pack size. Groups by supplier with total estimated cost. One-click PO generation.

**17. Backorder Auto-Fulfilment**
When stock is received via PO, automatically finds backordered order lines that can now be fulfilled. Re-checks availability in a transaction, reserves stock, updates line statuses. No manual checking required.

**18. Returns Auto-Disposition**
Rule-based engine that parses return reason text into structured codes and auto-routes: damaged/defective/expired → write off, wrong item/customer change → restock. Processes stock and logs automatically.

**19. ABC Cycle Count Scheduling**
Classifies products by revenue contribution: A-class (top 80%, 47 SKUs) counted weekly, B-class (next 15%, 23 SKUs) monthly, C-class (remaining 5%, 28 SKUs) quarterly. Auto-generates count tasks on schedule.

**20. Supplier Lead Time Auto-Update**
Analyses actual PO delivery times vs promised lead times. If actual consistently differs by >20% across 2+ POs, auto-adjusts using weighted average (70% actual + 30% promised). Reorder calculations then use realistic lead times.

**21. Auto-Reorder Generation**
Scans a warehouse for items below reorder point, calculates order quantities to reach max stock, groups by supplier, generates draft POs. One-click replenishment.

---

### Reporting & Visibility

**22. Dashboard with Charts**
KPI cards, warehouse overview with stock level meters, low stock alerts with transfer/reorder recommendations, recent activity feed. Four Chart.js visualisations: stock distribution by brand, stock value by warehouse, order volume by month, top products by orders. Auto-refreshes every 30 seconds.

**23. Full Audit Trail**
Every inventory mutation logged: receives, shipments, transfers, adjustments, returns. Each entry records timestamp, action type, warehouse, product, quantity change, balance after, and notes. Filterable by warehouse and action type.

**24. Reporting & Analytics**
Stock valuation by warehouse and brand with cost and retail totals. Demand velocity analysis scoring every product as fast mover, steady, slow, or dead stock. Warehouse comparison showing side-by-side stock across all three locations with imbalance detection.

**25. Packing Slips**
Printable document per order with company header, warehouse-specific address, customer details, line items with picker checkboxes, and signature lines. Print-optimised CSS.

**26. Order Form Import**
Ingested the actual Fox & Fable order form Excel provided in the brief. Parsed 12 Games Workshop product orders (130 units, £3,700.60 total), matched against the product catalogue, and created a real order (ORD-2026-0037) in the system. Demonstrates real-world data ingestion capability.

---

## Feature Comparison vs Industry

| Feature | Fox & Fable WMS | Manhattan Active | Deposco | Hopstack | Cin7 |
|---------|:-:|:-:|:-:|:-:|:-:|
| Multi-warehouse inventory | ✓ | ✓ | ✓ | ✓ | ✓ |
| Purchase order lifecycle | ✓ | ✓ | ✓ | ✓ | ✓ |
| Inter-warehouse transfers | ✓ | ✓ | ✓ | ✓ | ✓ |
| Order fulfilment pipeline | ✓ | ✓ | ✓ | ✓ | ✓ |
| Partial fulfilment review | ✓ | ✓ | ✓ | ~ | — |
| Returns / RMA | ✓ | ✓ | ✓ | ✓ | ✓ |
| Cycle counting | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bin-level locations | ✓ | ✓ | ✓ | ✓ | ~ |
| Multi-currency pricing | ✓ | ✓ | ✓ | ~ | ✓ |
| Full audit trail | ✓ | ✓ | ✓ | ✓ | ✓ |
| B2B customer portal | ✓ | ✓ | ✓ | — | ~ |
| Auto-reorder generation | ✓ | ✓ | ✓ | ✓ | ✓ |
| Wholesale MOQs | ✓ | ✓ | ✓ | — | ~ |
| Volume tiered pricing | ✓ | ✓ | ✓ | — | ~ |
| Credit limits & terms | ✓ | ✓ | ✓ | — | ✓ |
| AI order routing | ✓ | ✓ | ✓ | ✓ | — |
| Stockout prediction | ✓ | ✓ | ✓ | ✓ | — |
| Intelligent reorder qty | ✓ | ✓ | ✓ | ✓ | — |
| Backorder auto-fulfilment | ✓ | ✓ | ✓ | ✓ | — |
| Returns auto-disposition | ✓ | ✓ | ✓ | — | — |
| ABC cycle count scheduling | ✓ | ✓ | ✓ | ~ | — |
| Supplier lead time learning | ✓ | ✓ | ~ | — | — |
| Historical price tracking | ✓ | ✓ | ~ | — | — |
| Demand velocity analysis | ✓ | ✓ | ✓ | ✓ | ~ |
| Dashboard with charts | ✓ | ✓ | ✓ | ✓ | ✓ |
| Packing slips | ✓ | ✓ | ✓ | ✓ | ✓ |
| Order form import | ✓ | ✓ | ✓ | ~ | ~ |

✓ = supported, ~ = partial, — = not available

**What enterprise systems offer that this does not** (and why):

| Feature | Reason |
|---------|--------|
| Barcode scanning / handheld devices | Requires warehouse hardware |
| Voice picking | Requires warehouse hardware |
| Carrier rate shopping / shipping labels | Requires carrier API integrations (FedEx, UPS, DHL) |
| EDI integration | Requires trading partner setup |
| Mobile native app | Responsive web covers the use case for a demo |
| Wave/batch pick optimisation | Needs higher order volume to be meaningful |

---

## Database

16 tables with referential integrity, check constraints, and indexes:

| Table | Rows | Purpose |
|-------|------|---------|
| warehouses | 3 | London, Charleston, Hanover |
| suppliers | 4 | Games Workshop, Smartgames, Mantic Games, Leder Games |
| products | 98 | Full product catalogue with supplier links, MOQs, ABC class |
| warehouse_stock | 294 | 98 products × 3 warehouses, with bin locations |
| price_tiers | 268 | Volume discount tiers by brand |
| price_history | 230 | Historical buy cost and sell price changes |
| customers | 16 | UK, US, EU mix with wholesale terms |
| orders | 37+ | Full fulfilment lifecycle |
| order_lines | 130+ | Per-line fulfilment tracking |
| purchase_orders | 15+ | Inbound procurement |
| po_lines | 48+ | PO line items |
| stock_transfers | 8+ | Inter-warehouse movements |
| transfer_lines | 18+ | Transfer line items |
| returns | 7+ | RMA processing |
| cycle_counts | Active | Stock verification tasks |
| audit_log | 200+ | Complete mutation history |
| stock | 98 | Part 1 raw import |

---

## Engineering Qualities

- **Transactional integrity** — all multi-step operations use BEGIN/COMMIT/ROLLBACK
- **Concurrent safety** — advisory locks on sequence generation (PO refs, order refs, transfer refs, RMA refs)
- **State machines** — enforced valid transitions across orders, POs, transfers, and returns
- **Referential integrity** — foreign keys, check constraints, unique constraints throughout
- **Audit by default** — every inventory mutation logged with full context
- **Input validation** — all endpoints validate required fields with clear error messages
- **Double-click protection** — create buttons disable after first click
- **Supplier-product filtering** — prevents ordering products from the wrong supplier
- **Credit limit enforcement** — blocks orders exceeding customer credit at the API level
- **MOQ enforcement** — rejects orders below minimum quantity with specific error messages
