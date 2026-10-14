const express = require('express');
const http = require('http');
const crypto = require('crypto');
const cookieParser = require('cookie-parser');
const bcrypt = require('bcryptjs');
const path = require('path');
const { Server } = require('socket.io');
const { connect, Counter, User, Token, ServiceEvent } = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

const ML_URL = process.env.ML_URL || 'http://127.0.0.1:8000';

// ---------------- SESSIONS (in-memory) ----------------
// { sid -> { role, userId?, counterId?, name, tokenId? } }
const sessions = new Map();

// Auto-guest middleware — every visitor gets a session on first request.
// Customers are BORN with role='customer'. Staff/admin must call /api/login to upgrade.
app.use((req, res, next) => {
  let sid = req.cookies?.sid;
  let session = sid && sessions.get(sid);

  if (!session) {
    sid = crypto.randomBytes(24).toString('hex');
    session = { role: 'customer', tokenId: null, createdAt: Date.now() };
    sessions.set(sid, session);
    res.cookie('sid', sid, {
      httpOnly: true,
      sameSite: 'lax',
      maxAge: 30 * 24 * 60 * 60 * 1000, // 30 days
    });
  }
  req.session = { sid, ...session };
  next();
});

function requireRole(...roles) {
  return (req, res, next) => {
    if (!roles.includes(req.session.role)) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    next();
  };
}

// ---------------- TOKEN NUMBER COUNTER (in-memory) ----------------
let nextNumber = 1;

// ---------------- ESTIMATION ----------------
async function recentAvgMs(serviceType, window = 20) {
  const events = await ServiceEvent.find({ serviceType })
    .sort('-endedAt').limit(window).lean();
  if (events.length < 3) return 5 * 60 * 1000;
  return events.reduce((s, e) => s + e.durationMs, 0) / events.length;
}

async function mlPredict({ hour, dayOfWeek, ahead }) {
  try {
    const r = await fetch(`${ML_URL}/predict`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ hour, dayOfWeek, ahead }),
      signal: AbortSignal.timeout(2000),
    });
    if (!r.ok) throw new Error('ml fail');
    return await r.json();
  } catch { return null; }
}

async function naiveEstimate(ahead) {
  let total = 0;
  for (const a of ahead) {
    const avg = await recentAvgMs(a.serviceType);
    if (a.state === 'serving') total += Math.max(0, avg - a.elapsedMs);
    else total += avg;
  }
  const spread = total * 0.35;
  return {
    etaMinutes: total / 60000,
    p10Minutes: Math.max(0, (total - spread) / 60000),
    p90Minutes: (total + spread) / 60000,
    source: 'naive',
  };
}

async function estimateWaitForToken(token) {
  const ahead = await Token.find({
    status: { $in: ['waiting', 'serving'] },
    issuedAt: { $lt: token.issuedAt },
    _id: { $ne: token._id },
  }).sort('issuedAt').lean();

  if (ahead.length === 0) {
    return { etaMinutes: 0, p10Minutes: 0, p90Minutes: 0, source: 'none' };
  }

  const now = new Date();
  const payload = ahead.map(t => ({
    serviceType: t.serviceType,
    state: t.status,
    elapsedMs: t.status === 'serving' ? Date.now() - new Date(t.startedAt).getTime() : 0,
  }));

  const ml = await mlPredict({
    hour: now.getHours(),
    dayOfWeek: now.getDay(),
    ahead: payload,
  });
  if (ml) return { ...ml, source: 'ml' };
  return naiveEstimate(payload);
}

// ---------------- VIEWS ----------------
async function customerView(session) {
  if (!session.tokenId) return { token: null };
  const token = await Token.findById(session.tokenId).lean();
  if (!token) return { token: null };
  const eta = token.status === 'waiting' ? await estimateWaitForToken(token) : null;
  const aheadCount = await Token.countDocuments({
    status: { $in: ['waiting', 'serving'] },
    issuedAt: { $lt: token.issuedAt },
    _id: { $ne: token._id },
  });
  return { token, eta, aheadCount, now: Date.now() };
}

async function staffView(session) {
  const now = Date.now();
  const counter = session.counterId
    ? await Counter.findById(session.counterId).lean()
    : null;

  const serving = await Token.find({
    status: 'serving',
    counterId: session.counterId,
  }).lean();

  const waiting = await Token.find({ status: 'waiting' })
    .sort('issuedAt').limit(30).lean();

  // compute rough wait per waiting token (naive, cheap — ML is for customer view)
  const waitingWithEta = await Promise.all(waiting.map(async t => {
    const ahead = await Token.find({
      status: { $in: ['waiting', 'serving'] },
      issuedAt: { $lt: t.issuedAt },
    }).lean();
    let total = 0;
    for (const a of ahead) total += await recentAvgMs(a.serviceType);
    return { ...t, estimatedWaitMs: total };
  }));

  return {
    now,
    counter,
    serving: serving.map(t => ({
      ...t,
      elapsedMs: now - new Date(t.startedAt).getTime(),
    })),
    waiting: waitingWithEta,
    totalWaiting: await Token.countDocuments({ status: 'waiting' }),
  };
}

async function adminView() {
  const [totalServed, waiting, serving, counters] = await Promise.all([
    ServiceEvent.countDocuments(),
    Token.countDocuments({ status: 'waiting' }),
    Token.countDocuments({ status: 'serving' }),
    Counter.find().lean(),
  ]);

  // per-service stats
  const stats = await ServiceEvent.aggregate([
    { $group: {
        _id: '$serviceType',
        count: { $sum: 1 },
        avgMs: { $avg: '$durationMs' },
    }},
    { $sort: { _id: 1 } },
  ]);

  // per-counter stats
  const counterStats = await Promise.all(counters.map(async c => {
    const s = await ServiceEvent.aggregate([
      { $match: { counterId: c._id } },
      { $group: { _id: null, count: { $sum: 1 }, avgMs: { $avg: '$durationMs' } } },
    ]);
    const staff = await User.findOne({ counterId: c._id, role: 'staff' }).lean();
    const current = await Token.findOne({ counterId: c._id, status: 'serving' }).lean();
    return {
      counterName: c.name,
      staffName: staff?.name || '—',
      served: s[0]?.count || 0,
      avgMs: s[0]?.avgMs || 0,
      current: current ? { number: current.number, name: current.customerName } : null,
    };
  }));

  const recent = await ServiceEvent.find().sort('-endedAt').limit(10).lean();

  return {
    now: Date.now(),
    totalServed, waiting, serving,
    stats: stats.map(x => ({ serviceType: x._id, count: x.count, avgMs: x.avgMs })),
    counterStats,
    recent,
  };
}

// ---------------- BROADCAST (per-room) ----------------
async function broadcastAll() {
  // each customer gets only their own view
  const custJobs = [];
  for (const [sid, session] of sessions) {
    if (session.role === 'customer' && session.tokenId) {
      custJobs.push(
        customerView({ sid, ...session })
          .then(v => io.to('cust_' + sid).emit('state', v))
          .catch(() => {})
      );
    }
  }

  // staff: each staff room is shared by role — but we want per-counter views.
  // Simplest: emit to a room 'counter_<id>' and each staff socket joins that room.
  const staffJobs = [];
  for (const [sid, session] of sessions) {
    if (session.role === 'staff' && session.counterId) {
      staffJobs.push(
        staffView({ sid, ...session })
          .then(v => io.to('counter_' + session.counterId).emit('state', v))
          .catch(() => {})
      );
    }
  }

  const adminJob = adminView().then(v => io.to('admin').emit('state', v)).catch(() => {});

  await Promise.all([...custJobs, ...staffJobs, adminJob]);
}

// ---------------- AUTH ----------------
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'Missing fields' });

  const user = await User.findOne({ username }).lean();
  if (!user) return res.status(401).json({ error: 'Invalid credentials' });

  const ok = await bcrypt.compare(password, user.passwordHash);
  if (!ok) return res.status(401).json({ error: 'Invalid credentials' });

  // burn the old session, create a fresh one with the new role
  const oldSid = req.cookies?.sid;
  if (oldSid) sessions.delete(oldSid);

  const sid = crypto.randomBytes(24).toString('hex');
  sessions.set(sid, {
    role: user.role,
    userId: user._id.toString(),
    counterId: user.counterId ? user.counterId.toString() : null,
    name: user.name,
    tokenId: null,
    createdAt: Date.now(),
  });
  res.cookie('sid', sid, {
    httpOnly: true,
    sameSite: 'lax',
    maxAge: 30 * 24 * 60 * 60 * 1000,
  });

  const redirect = user.role === 'admin' ? '/admin.html' : '/staff.html';
  res.json({ ok: true, role: user.role, name: user.name, redirect });
});

app.post('/api/logout', (req, res) => {
  const sid = req.cookies?.sid;
  if (sid) sessions.delete(sid);
  res.clearCookie('sid');
  res.json({ ok: true });
});

app.get('/api/me', (req, res) => {
  res.json({
    role: req.session.role,
    name: req.session.name || null,
    hasToken: !!req.session.tokenId,
  });
});

// ---------------- CUSTOMER ----------------
app.post('/api/token', requireRole('customer'), async (req, res) => {
  const { customerName, serviceType } = req.body || {};
  if (!customerName || !serviceType) {
    return res.status(400).json({ error: 'Name and service type required' });
  }

  // already have an active token?
  if (req.session.tokenId) {
    const existing = await Token.findById(req.session.tokenId);
    if (existing && (existing.status === 'waiting' || existing.status === 'serving')) {
      return res.status(400).json({ error: 'You already have an active token' });
    }
  }

  const token = await Token.create({
    number: nextNumber++,
    customerName,
    serviceType,
    sessionId: req.session.sid,
    status: 'waiting',
    issuedAt: new Date(),
  });

  // attach to session
  const s = sessions.get(req.session.sid);
  if (s) s.tokenId = token._id.toString();

  await broadcastAll();
  res.json(token);
});

// ---------------- STAFF ----------------
app.post('/api/start/:id', requireRole('staff'), async (req, res) => {
  // atomic: only flip if still waiting
  const token = await Token.findOneAndUpdate(
    { _id: req.params.id, status: 'waiting' },
    {
      status: 'serving',
      startedAt: new Date(),
      counterId: req.session.counterId,
      staffId: req.session.userId,
    },
    { new: true }
  );
  if (!token) return res.status(409).json({ error: 'Token not waiting or already taken' });
  await broadcastAll();
  res.json(token);
});

app.post('/api/done/:id', requireRole('staff'), async (req, res) => {
  const token = await Token.findOneAndUpdate(
    { _id: req.params.id, status: 'serving', counterId: req.session.counterId },
    { status: 'done', endedAt: new Date() },
    { new: true }
  );
  if (!token) return res.status(409).json({ error: 'Not your token or not serving' });

  await ServiceEvent.create({
    tokenId: token._id,
    serviceType: token.serviceType,
    counterId: token.counterId,
    staffId: token.staffId,
    durationMs: new Date(token.endedAt) - new Date(token.startedAt),
    endedAt: token.endedAt,
  });

  await broadcastAll();
  res.json(token);
});

// ---------------- ADMIN ----------------
app.post('/api/admin/retrain', requireRole('admin'), async (req, res) => {
  try {
    const r = await fetch(`${ML_URL}/retrain`, { method: 'POST' });
    res.json(await r.json());
  } catch {
    res.status(500).json({ error: 'ML service unavailable' });
  }
});

// ---------------- SOCKET.IO ----------------
io.use((socket, next) => {
  const raw = socket.handshake.headers.cookie || '';
  const parsed = Object.fromEntries(
    raw.split(';').map(c => {
      const [k, ...v] = c.trim().split('=');
      return [k, decodeURIComponent(v.join('='))];
    })
  );
  const session = parsed.sid ? sessions.get(parsed.sid) : null;
  if (!session) return next(new Error('unauthorized'));
  socket.sid = parsed.sid;
  socket.role = session.role;
  socket.counterId = session.counterId;
  next();
});

io.on('connection', async (socket) => {
  const session = sessions.get(socket.sid);
  if (!session) return socket.disconnect();

  if (socket.role === 'customer') {
    socket.join('cust_' + socket.sid);
    socket.emit('state', await customerView({ sid: socket.sid, ...session }));
  } else if (socket.role === 'staff' && socket.counterId) {
    socket.join('counter_' + socket.counterId);
    socket.emit('state', await staffView({ sid: socket.sid, ...session }));
  } else if (socket.role === 'admin') {
    socket.join('admin');
    socket.emit('state', await adminView());
  }
});

// ---------------- TICK + BOOT ----------------
app.get('/', (req, res) => res.redirect('/customer.html'));

async function boot() {
  await connect();

  // recover token number
  const last = await Token.findOne().sort('-number').lean();
  nextNumber = (last?.number || 0) + 1;

  setInterval(broadcastAll, 3000);

  server.listen(3000, () => console.log('>>> http://localhost:3000'));
}

boot().catch(e => { console.error(e); process.exit(1); });