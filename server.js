const express = require('express');
const http = require('http');
const crypto = require('crypto');
const cookieParser = require('cookie-parser');
const bcrypt = require('bcryptjs');
const path = require('path');
const { Server } = require('socket.io');
const { connect, Counter, User, Token, ServiceEvent, Feedback } = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

const ML_URL = process.env.ML_URL || 'http://127.0.0.1:8000';

// ---------------- SESSIONS ----------------
const sessions = new Map();

app.use((req, res, next) => {
  let sid = req.cookies?.sid;
  let session = sid && sessions.get(sid);
  if (!session) {
    sid = crypto.randomBytes(24).toString('hex');
    session = { role: 'customer', tokenId: null, createdAt: Date.now() };
    sessions.set(sid, session);
    res.cookie('sid', sid, { httpOnly: true, sameSite: 'lax', maxAge: 30 * 24 * 60 * 60 * 1000 });
  }
  req.session = { sid, ...session };
  next();
});

function requireRole(...roles) {
  return (req, res, next) => {
    if (!roles.includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
    next();
  };
}

let nextNumber = 1;

// ---------------- HELPERS ----------------
async function recentAvgMs(serviceType, window = 20) {
  const events = await ServiceEvent.find({ serviceType }).sort('-endedAt').limit(window).lean();
  if (events.length < 3) return 5 * 60 * 1000;
  return events.reduce((s, e) => s + e.durationMs, 0) / events.length;
}

async function getDurationStats(serviceTypes, when = new Date()) {
  try {
    const r = await fetch(`${ML_URL}/duration_stats`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        serviceTypes: [...new Set(serviceTypes)],
        hour: when.getHours(),
        dayOfWeek: when.getDay(),
      }),
      signal: AbortSignal.timeout(2500),
    });
    if (!r.ok) throw new Error('ml failed');
    return { stats: await r.json(), source: 'ml' };
  } catch {
    const out = {};
    for (const st of new Set(serviceTypes)) {
      const avgMin = (await recentAvgMs(st)) / 60000;
      out[st] = { p10: avgMin * 0.7, p50: avgMin, p90: avgMin * 1.3 };
    }
    return { stats: out, source: 'naive' };
  }
}

// Box-Muller sample from a distribution defined by p10/p50/p90 (minutes)
function sampleDurationMin(stats) {
  const sigma = Math.max(0.3, (stats.p90 - stats.p10) / (2 * 1.2816));
  const u1 = Math.random() || 1e-9;
  const u2 = Math.random();
  const z = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  return Math.max(0.15, stats.p50 + z * sigma);
}

// ---------------- MULTI-COUNTER SIMULATOR ----------------
// The heart of the correct wait-time prediction.
// Simulates the shared pool: at each step, the earliest-free eligible counter
// picks the next waiting token. This is why token 3 assigned to staff 1 can be
// served by staff 2 if staff 2 becomes free first.
async function simulateWaitMs(targetToken, opts = {}) {
  const nSims = opts.nSims || 300;
  const now = Date.now();

  const counters = await Counter.find({ isActive: true }).lean();
  if (counters.length === 0) {
    return { etaMinutes: 0, p10Minutes: 0, p90Minutes: 0, source: 'no-counters', blocked: true };
  }

  const serving = await Token.find({ status: 'serving' }).lean();
  const servingByCounter = {};
  for (const s of serving) {
    if (s.counterId) servingByCounter[s.counterId.toString()] = s;
  }

  const waitingBefore = await Token.find({
    status: 'waiting',
    issuedAt: { $lt: targetToken.issuedAt },
    _id: { $ne: targetToken._id },
  }).sort('issuedAt').lean();

  // Precompute duration distributions once for all service types involved
  const allTypes = new Set([
    targetToken.serviceType,
    ...waitingBefore.map(t => t.serviceType),
    ...serving.map(t => t.serviceType),
  ]);
  const { stats, source } = await getDurationStats([...allTypes]);

  const samples = [];

  for (let sim = 0; sim < nSims; sim++) {
    // Each counter starts with the time it will next become free
    const nextFree = counters.map(c => {
      let t = now;
      if (c.pausedUntil && new Date(c.pausedUntil).getTime() > now) {
        t = new Date(c.pausedUntil).getTime();
      }
      const s = servingByCounter[c._id.toString()];
      if (s) {
        const elapsed = now - new Date(s.startedAt).getTime();
        const dur = sampleDurationMin(stats[s.serviceType] || { p10: 3, p50: 5, p90: 8 }) * 60000;
        t = Math.max(t, now + Math.max(0, dur - elapsed));
      }
      return t;
    });

    let blocked = false;

    // Serve each waiting token, in issue order, at whichever eligible counter is free first
    for (const t of waitingBefore) {
      let bestIdx = -1;
      let bestTime = Infinity;
      for (let i = 0; i < counters.length; i++) {
        if (!counters[i].serviceTypes.includes(t.serviceType)) continue;
        if (nextFree[i] < bestTime) { bestTime = nextFree[i]; bestIdx = i; }
      }
      if (bestIdx === -1) { blocked = true; break; }
      const dur = sampleDurationMin(stats[t.serviceType] || { p10: 3, p50: 5, p90: 8 }) * 60000;
      nextFree[bestIdx] += dur;
    }

    if (blocked) { samples.push(Infinity); continue; }

    // When does our target get served?
    let bestIdx = -1;
    let bestTime = Infinity;
    for (let i = 0; i < counters.length; i++) {
      if (!counters[i].serviceTypes.includes(targetToken.serviceType)) continue;
      if (nextFree[i] < bestTime) { bestTime = nextFree[i]; bestIdx = i; }
    }
    if (bestIdx === -1) { samples.push(Infinity); continue; }

    samples.push(Math.max(0, nextFree[bestIdx] - now));
  }

  const finite = samples.filter(x => isFinite(x));
  if (finite.length === 0) {
    return {
      etaMinutes: 0, p10Minutes: 0, p90Minutes: 0,
      source, blocked: true, blockRate: 1,
    };
  }
  finite.sort((a, b) => a - b);
  const q = p => finite[Math.min(finite.length - 1, Math.floor(finite.length * p))];

  return {
    etaMinutes: q(0.5) / 60000,
    p10Minutes: q(0.1) / 60000,
    p90Minutes: q(0.9) / 60000,
    source,
    blocked: false,
    blockRate: 1 - finite.length / samples.length,
  };
}

async function estimateWaitForToken(token) {
  if (!token || token.status !== 'waiting') return null;
  return simulateWaitMs(token);
}

// ---------------- AUTO-ASSIGN ----------------
async function leastBusyCounter(serviceType, excludeIds = []) {
  const counters = await Counter.find({
    isActive: true,
    serviceTypes: serviceType,
    _id: { $nin: excludeIds },
  }).lean();

  const now = Date.now();
  const available = counters.filter(c =>
    !c.pausedUntil || new Date(c.pausedUntil).getTime() <= now
  );
  if (available.length === 0) return null;

  const scored = await Promise.all(available.map(async c => {
    const serving = await Token.countDocuments({ counterId: c._id, status: 'serving' });
    const waiting = await Token.countDocuments({ counterId: c._id, status: 'waiting' });
    return { counter: c, score: serving * 2 + waiting };
  }));
  scored.sort((a, b) => a.score - b.score);
  return scored[0].counter;
}

// ---------------- VIEWS ----------------
async function customerView(session) {
  if (!session.tokenId) return { token: null };
  const token = await Token.findById(session.tokenId).lean();
  if (!token) return { token: null };

  const eta = token.status === 'waiting' ? await estimateWaitForToken(token) : null;

  // total waiting tokens (system-wide) issued before this one
  const aheadCount = await Token.countDocuments({
    status: { $in: ['waiting', 'serving'] },
    issuedAt: { $lt: token.issuedAt },
    _id: { $ne: token._id },
  });

  let counter = null;
  if (token.counterId) {
    const c = await Counter.findById(token.counterId).lean();
    if (c) {
      const isPaused = c.pausedUntil && new Date(c.pausedUntil).getTime() > Date.now();
      // Is there any OTHER active counter that could serve this token?
      const alt = await Counter.countDocuments({
        isActive: true,
        serviceTypes: token.serviceType,
        _id: { $ne: c._id },
        $or: [
          { pausedUntil: null },
          { pausedUntil: { $lte: new Date() } },
        ],
      });
      counter = {
        name: c.name,
        paused: !!isPaused,
        pausedUntil: c.pausedUntil,
        pauseReason: c.pauseReason,
        hasAlternate: alt > 0,
      };
    }
  }

  return { token, eta, aheadCount, counter, now: Date.now() };
}

async function staffView(session) {
  const now = Date.now();
  const counter = session.counterId
    ? await Counter.findById(session.counterId).lean()
    : null;

  const serving = await Token.find({ status: 'serving', counterId: session.counterId }).lean();

  const waiting = await Token.find({
    status: 'waiting',
    counterId: session.counterId,
  }).sort('issuedAt').limit(30).lean();

  const waitingWithEta = await Promise.all(waiting.map(async t => {
    const eta = await simulateWaitMs(t, { nSims: 80 });
    return { ...t, estimatedWaitMs: eta.etaMinutes * 60000 };
  }));

  return {
    now,
    counter: counter ? {
      _id: counter._id,
      name: counter.name,
      pausedUntil: counter.pausedUntil,
      pauseReason: counter.pauseReason,
      isPaused: !!(counter.pausedUntil && new Date(counter.pausedUntil).getTime() > now),
    } : null,
    serving: serving.map(t => ({ ...t, elapsedMs: now - new Date(t.startedAt).getTime() })),
    waiting: waitingWithEta,
    totalWaiting: await Token.countDocuments({ status: 'waiting' }),
    blockedCount: await Token.countDocuments({
      counterId: session.counterId,
      status: 'waiting',
      blocked: true,
    }),
  };
}

async function adminView() {
  const now = Date.now();
  const [totalServed, waiting, serving, counters] = await Promise.all([
    ServiceEvent.countDocuments(),
    Token.countDocuments({ status: 'waiting' }),
    Token.countDocuments({ status: 'serving' }),
    Counter.find().lean(),
  ]);

  const stats = await ServiceEvent.aggregate([
    { $group: { _id: '$serviceType', count: { $sum: 1 }, avgMs: { $avg: '$durationMs' } } },
    { $sort: { _id: 1 } },
  ]);

  const counterStats = await Promise.all(counters.map(async c => {
    const s = await ServiceEvent.aggregate([
      { $match: { counterId: c._id } },
      { $group: { _id: null, count: { $sum: 1 }, avgMs: { $avg: '$durationMs' } } },
    ]);
    const staff = await User.findOne({ counterId: c._id, role: 'staff' }).lean();
    const current = await Token.findOne({ counterId: c._id, status: 'serving' }).lean();
    const queueLen = await Token.countDocuments({ counterId: c._id, status: 'waiting' });
    const fb = await Feedback.aggregate([
      { $match: { counterId: c._id } },
      { $group: { _id: null, avg: { $avg: '$rating' }, count: { $sum: 1 } } },
    ]);
    return {
      counterName: c.name,
      staffName: staff?.name || '—',
      served: s[0]?.count || 0,
      avgMs: s[0]?.avgMs || 0,
      ratingAvg: fb[0]?.avg || 0,
      ratingCount: fb[0]?.count || 0,
      queueLen,
      isPaused: !!(c.pausedUntil && new Date(c.pausedUntil).getTime() > now),
      current: current ? { number: current.number, name: current.customerName } : null,
    };
  }));

  const recent = await ServiceEvent.find().sort('-endedAt').limit(10).lean();
  const recentFeedback = await Feedback.find().sort('-createdAt').limit(10)
    .populate('counterId', 'name').lean();

  return {
    now,
    totalServed, waiting, serving,
    stats: stats.map(x => ({ serviceType: x._id, count: x.count, avgMs: x.avgMs })),
    counterStats,
    recent,
    recentFeedback: recentFeedback.map(f => ({
      rating: f.rating,
      comment: f.comment || '',
      counterName: f.counterId?.name || '—',
      at: f.createdAt,
    })),
  };
}

// ---------------- BROADCAST ----------------
async function broadcastAll() {
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
  res.cookie('sid', sid, { httpOnly: true, sameSite: 'lax', maxAge: 30 * 24 * 60 * 60 * 1000 });
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
  res.json({ role: req.session.role, name: req.session.name || null, hasToken: !!req.session.tokenId });
});

// ---------------- CUSTOMER ----------------
app.post('/api/token', requireRole('customer'), async (req, res) => {
  const { customerName, serviceType } = req.body || {};
  if (!customerName || !serviceType) {
    return res.status(400).json({ error: 'Name and service type required' });
  }
  if (req.session.tokenId) {
    const existing = await Token.findById(req.session.tokenId);
    if (existing && (existing.status === 'waiting' || existing.status === 'serving')) {
      return res.status(400).json({ error: 'You already have an active token' });
    }
  }

  const counter = await leastBusyCounter(serviceType);

  const token = await Token.create({
    number: nextNumber++,
    customerName,
    serviceType,
    sessionId: req.session.sid,
    status: 'waiting',
    issuedAt: new Date(),
    counterId: counter?._id || null,
  });

  const s = sessions.get(req.session.sid);
  if (s) s.tokenId = token._id.toString();

  await broadcastAll();
  res.json(token);
});

app.post('/api/withdraw', requireRole('customer'), async (req, res) => {
  if (!req.session.tokenId) return res.status(400).json({ error: 'No active token' });
  const token = await Token.findById(req.session.tokenId);
  if (!token) return res.status(404).json({ error: 'Token not found' });
  if (token.status === 'serving') {
    return res.status(400).json({ error: 'Cannot withdraw while being served' });
  }
  if (token.status !== 'waiting') {
    return res.status(400).json({ error: 'Token already finished' });
  }

  token.status = 'cancelled';
  token.endedAt = new Date();
  await token.save();

  const s = sessions.get(req.session.sid);
  if (s) s.tokenId = null;

  await broadcastAll();
  res.json({ ok: true });
});

app.post('/api/feedback', requireRole('customer'), async (req, res) => {
  const { tokenId, rating, comment } = req.body || {};
  if (!tokenId || !rating) return res.status(400).json({ error: 'tokenId and rating required' });
  const token = await Token.findById(tokenId).lean();
  if (!token) return res.status(404).json({ error: 'Token not found' });
  const existing = await Feedback.findOne({ tokenId });
  if (existing) return res.status(400).json({ error: 'Feedback already submitted' });

  await Feedback.create({
    tokenId, rating, comment: comment || '',
    staffId: token.staffId, counterId: token.counterId,
  });
  await broadcastAll();
  res.json({ ok: true });
});

// ---------------- STAFF ----------------
app.post('/api/start/:id', requireRole('staff'), async (req, res) => {
  const counter = await Counter.findById(req.session.counterId).lean();
  if (counter?.pausedUntil && new Date(counter.pausedUntil).getTime() > Date.now()) {
    return res.status(409).json({ error: 'Counter is paused' });
  }

  // Allow staff to serve any waiting token assigned to them OR unassigned
  const token = await Token.findOneAndUpdate(
    {
      _id: req.params.id,
      status: 'waiting',
      $or: [
        { counterId: req.session.counterId },
        { counterId: null },
      ],
    },
    { status: 'serving', startedAt: new Date(), staffId: req.session.userId, counterId: req.session.counterId },
    { new: true }
  );
  if (!token) return res.status(409).json({ error: 'Not your counter or not waiting' });
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

app.post('/api/pause', requireRole('staff'), async (req, res) => {
  const minutes = Math.max(1, Math.min(120, req.body?.minutes || 10));
  const reason = (req.body?.reason || 'Break').slice(0, 60);
  const until = new Date(Date.now() + minutes * 60 * 1000);
  const myCounterId = req.session.counterId;

  await Counter.findByIdAndUpdate(myCounterId, {
    pausedUntil: until,
    pauseReason: reason,
  });

  // Redistribute waiting tokens from this counter
  const orphaned = await Token.find({
    counterId: myCounterId,
    status: 'waiting',
  }).lean();

  let moved = 0;
  let blocked = 0;
  for (const token of orphaned) {
    const alt = await leastBusyCounter(token.serviceType, [myCounterId]);
    if (alt) {
      await Token.findByIdAndUpdate(token._id, { counterId: alt._id, blocked: false });
      moved++;
    } else {
      // No alternate available — mark blocked so customer/staff know
      await Token.findByIdAndUpdate(token._id, { blocked: true });
      blocked++;
    }
  }

  // Also mark all remaining tokens on this counter as blocked
  await Token.updateMany(
    { counterId: myCounterId, status: 'waiting' },
    { blocked: true }
  );

  await broadcastAll();
  res.json({ ok: true, until, reassigned: moved, blocked });
});

app.post('/api/resume', requireRole('staff'), async (req, res) => {
  await Counter.findByIdAndUpdate(req.session.counterId, {
    $set: { pausedUntil: null, pauseReason: null },
  });

  // Unblock tokens on this counter
  await Token.updateMany(
    { counterId: req.session.counterId, status: 'waiting' },
    { blocked: false }
  );

  await broadcastAll();
  res.json({ ok: true });
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

app.get('/api/admin/timeseries', requireRole('admin'), async (req, res) => {
  const data = await ServiceEvent.aggregate([
    { $group: { _id: { $hour: '$endedAt' }, avgDuration: { $avg: '$durationMs' }, count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]);
  res.json(data.map(d => ({ hour: d._id, avgMin: +(d.avgDuration / 60000).toFixed(2), count: d.count })));
});

app.get('/api/admin/heatmap', requireRole('admin'), async (req, res) => {
  const data = await ServiceEvent.aggregate([
    { $group: { _id: { day: { $dayOfWeek: '$endedAt' }, hour: { $hour: '$endedAt' } }, count: { $sum: 1 } } },
  ]);
  res.json(data);
});

// ---------------- SOCKET ----------------
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

// ---------------- BOOT ----------------
app.get('/', (req, res) => res.redirect('/customer.html'));

async function boot() {
  await connect();
  const last = await Token.findOne().sort('-number').lean();
  nextNumber = (last?.number || 0) + 1;
  setInterval(broadcastAll, 3000);
  server.listen(3000, () => console.log('>>> http://localhost:3000'));
}

boot().catch(e => { console.error(e); process.exit(1); });