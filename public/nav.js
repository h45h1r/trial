// Fox & Fable WMS — shared nav + base styles
(function() {
  const font = document.createElement('link');
  font.rel = 'stylesheet';
  font.href = 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap';
  document.head.appendChild(font);

  const style = document.createElement('style');
  style.textContent = `
    body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important; -webkit-font-smoothing: antialiased; }
  `;
  document.head.appendChild(style);
})();

function renderNav(activePage) {
  const pages = [
    { id: 'dashboard', label: 'Dashboard', href: '/dashboard.html' },
    { id: 'inventory', label: 'Inventory', href: '/inventory.html' },
    { id: 'purchase-orders', label: 'POs', href: '/purchase-orders.html' },
    { id: 'transfers', label: 'Transfers', href: '/transfers.html' },
    { id: 'fulfilment', label: 'Fulfilment', href: '/fulfilment.html' },
    { id: 'cycle-counts', label: 'Cycle Counts', href: '/cycle-counts.html' },
    { id: 'suppliers', label: 'Suppliers', href: '/suppliers.html' },
    { id: 'customers', label: 'Customers', href: '/customers.html' },
    { id: 'returns', label: 'Returns', href: '/returns.html' },
    { id: 'pricing', label: 'Pricing', href: '/pricing.html' },
    { id: 'reports', label: 'Reports', href: '/reports.html' },
    { id: 'intelligence', label: 'Intelligence', href: '/intelligence.html' },
    { id: 'audit', label: 'Audit', href: '/audit.html' },
    { id: 'stock', label: 'Stock', href: '/stock.html' },
    { id: 'portal', label: 'Portal', href: '/portal.html' },
  ];

  const nav = document.createElement('nav');
  nav.innerHTML = `
    <div style="background:#0f172a">
      <div style="max-width:1440px;margin:0 auto;padding:0 1.5rem;display:flex;align-items:center;overflow-x:auto;scrollbar-width:none">
        <a href="/dashboard.html" style="color:#fff;font-weight:700;font-size:.8125rem;text-decoration:none;padding:.75rem 0;margin-right:1.5rem;white-space:nowrap;letter-spacing:-.01em;flex-shrink:0">Fox & Fable</a>
        ${pages.map(p => {
          const active = p.id === activePage;
          return `<a href="${p.href}" style="color:${active ? '#fff' : '#64748b'};text-decoration:none;font-size:.75rem;font-weight:${active ? '600' : '500'};padding:.6875rem .5rem;border-bottom:2px solid ${active ? '#fff' : 'transparent'};transition:color 120ms;white-space:nowrap;flex-shrink:0"
            onmouseover="this.style.color='#cbd5e1'" onmouseout="if(!${active})this.style.color='#64748b'">${p.label}</a>`;
        }).join('')}
      </div>
    </div>
  `;

  // Hide scrollbar
  const s = document.createElement('style');
  s.textContent = 'nav div:first-child>div{-ms-overflow-style:none;scrollbar-width:none}nav div:first-child>div::-webkit-scrollbar{display:none}';
  document.head.appendChild(s);

  document.body.prepend(nav);
}
