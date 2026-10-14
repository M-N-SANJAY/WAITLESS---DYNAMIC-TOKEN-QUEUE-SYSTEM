const mongoose = require('mongoose');

const MONGO_URL = process.env.MONGO_URL || 'mongodb://127.0.0.1:27017/waittless';

async function connect() {
  await mongoose.connect(MONGO_URL);
  console.log('[db] connected to', MONGO_URL);
}

const CounterSchema = new mongoose.Schema({
  name: { type: String, required: true },
  serviceTypes: [String],
  isActive: { type: Boolean, default: true },
});

const UserSchema = new mongoose.Schema({
  username: { type: String, unique: true, required: true },
  passwordHash: { type: String, required: true },
  role: { type: String, enum: ['staff', 'admin'], required: true },
  name: String,
  counterId: { type: mongoose.Schema.Types.ObjectId, ref: 'Counter' },
});

const TokenSchema = new mongoose.Schema({
  number: { type: Number, required: true, index: true },
  customerName: String,
  serviceType: String,
  sessionId: { type: String, index: true },
  status: {
    type: String,
    enum: ['waiting', 'serving', 'done', 'cancelled'],
    default: 'waiting',
    index: true,
  },
  issuedAt: { type: Date, default: Date.now, index: true },
  startedAt: Date,
  endedAt: Date,
  counterId: { type: mongoose.Schema.Types.ObjectId, ref: 'Counter' },
  staffId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
});

const ServiceEventSchema = new mongoose.Schema({
  tokenId: mongoose.Schema.Types.ObjectId,
  serviceType: String,
  counterId: mongoose.Schema.Types.ObjectId,
  staffId: mongoose.Schema.Types.ObjectId,
  durationMs: Number,
  endedAt: { type: Date, default: Date.now, index: true },
});

module.exports = {
  connect,
  Counter: mongoose.model('Counter', CounterSchema),
  User: mongoose.model('User', UserSchema),
  Token: mongoose.model('Token', TokenSchema),
  ServiceEvent: mongoose.model('ServiceEvent', ServiceEventSchema),
};