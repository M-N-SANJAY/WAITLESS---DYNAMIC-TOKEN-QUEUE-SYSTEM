async function requireRole(role) {
  const r = await fetch('/api/me');
  if (!r.ok) { location.href = '/login.html'; throw new Error('unauth'); }
  const me = await r.json();
  if (me.role !== role) {
    const t = { customer: '/customer.html', staff: '/staff.html', admin: '/admin.html' }[me.role];
    location.href = t || '/login.html';
    throw new Error('wrong role');
  }
  return me;
}
function fmtMinutes(ms) { return Math.max(0, Math.round(ms / 60000)) + 'm'; }
function fmtSeconds(ms) { return Math.max(0, Math.floor(ms / 1000)) + 's'; }
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c =>
    ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
}