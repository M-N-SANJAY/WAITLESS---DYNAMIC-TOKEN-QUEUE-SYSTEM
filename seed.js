const bcrypt = require('bcryptjs');
const mongoose = require('mongoose');
const { connect, Counter, User, Token, ServiceEvent, Feedback } = require('./db');

async function run() {
  await connect();

  console.log('[seed] wiping existing data…');
  await Promise.all([
    Counter.deleteMany({}),
    User.deleteMany({}),
    Token.deleteMany({}),
    ServiceEvent.deleteMany({}),
    Feedback.deleteMany({}),
  ]);

  console.log('[seed] creating counters…');
  const counters = await Counter.insertMany([
    { name: 'Counter 1', serviceTypes: ['general', 'billing', 'consultation'], pausedUntil: null, pauseReason: null },
    { name: 'Counter 2', serviceTypes: ['general', 'billing', 'consultation'], pausedUntil: null, pauseReason: null },
    { name: 'Counter 3', serviceTypes: ['general', 'billing', 'consultation'], pausedUntil: null, pauseReason: null },
  ]);

  const staffHash = await bcrypt.hash('staff123', 10);
  const adminHash = await bcrypt.hash('admin123', 10);

  console.log('[seed] creating staff + admin…');
  await User.insertMany([
    { username: 'staff1', passwordHash: staffHash, role: 'staff', name: 'Priya', counterId: counters[0]._id },
    { username: 'staff2', passwordHash: staffHash, role: 'staff', name: 'Arjun', counterId: counters[1]._id },
    { username: 'staff3', passwordHash: staffHash, role: 'staff', name: 'Neha',  counterId: counters[2]._id },
    { username: 'admin',  passwordHash: adminHash, role: 'admin', name: 'Admin' },
  ]);

  console.log('\n=== SEED COMPLETE ===');
  console.log('Staff:  staff1 / staff2 / staff3   password: staff123');
  console.log('Admin:  admin                       password: admin123');
  console.log('Customers: no login — just visit /\n');

  await mongoose.disconnect();
}

run().catch(e => { console.error(e); process.exit(1); });