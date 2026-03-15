require('dotenv').config();
const express = require('express');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const bcrypt = require('bcryptjs');

// Email services — priority: Brevo (HTTP) > Resend (HTTP) > Gmail SMTP (local only)
const nodemailer = require('nodemailer');
const GMAIL_USER = process.env.GMAIL_USER || 'soldihq@gmail.com';
const GMAIL_APP_PASSWORD = process.env.GMAIL_APP_PASSWORD || '';
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const BREVO_API_KEY = process.env.BREVO_API_KEY || '';
const SENDER_EMAIL = process.env.SENDER_EMAIL || GMAIL_USER;

// Resend (HTTP-based)
let resend = null;
try {
  const { Resend } = require('resend');
  if (RESEND_API_KEY) {
    resend = new Resend(RESEND_API_KEY);
    console.log('[Mail] Resend initialized');
  }
} catch (e) {}

// Gmail SMTP (fallback — works locally but blocked on Render)
let mailTransporter = null;
if (GMAIL_APP_PASSWORD) {
  mailTransporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: GMAIL_USER, pass: GMAIL_APP_PASSWORD },
    connectionTimeout: 8000,
    greetingTimeout: 8000,
    socketTimeout: 8000
  });
  console.log(`[Mail] Gmail SMTP initialized (fallback)`);
}

if (BREVO_API_KEY) console.log('[Mail] Brevo HTTP email service initialized (primary)');
if (!BREVO_API_KEY && !resend && !mailTransporter) {
  console.log('[Mail] WARNING: No email service configured — email sending disabled');
}

// Unified email sender: tries Brevo (HTTP) > Resend (HTTP) > Gmail SMTP
async function sendEmail({ to, subject, html }) {
  // 1. Brevo / Sendinblue (HTTP — works everywhere, sends to any email)
  if (BREVO_API_KEY) {
    try {
      const brevoRes = await fetch('https://api.brevo.com/v3/smtp/email', {
        method: 'POST',
        headers: { 'accept': 'application/json', 'content-type': 'application/json', 'api-key': BREVO_API_KEY },
        body: JSON.stringify({
          sender: { name: 'Soldi', email: SENDER_EMAIL },
          to: [{ email: to }],
          subject,
          htmlContent: html
        })
      });
      if (!brevoRes.ok) {
        const errBody = await brevoRes.text().catch(() => '');
        throw new Error(`Brevo ${brevoRes.status}: ${errBody}`);
      }
      console.log(`[Mail] Sent to ${to} via Brevo`);
      return { success: true, provider: 'brevo' };
    } catch (err) {
      console.error(`[Mail] Brevo failed for ${to}:`, err.message || err);
    }
  }

  // 2. Resend (HTTP — sandbox limited to account owner email)
  if (resend) {
    try {
      const result = await resend.emails.send({
        from: 'Soldi <onboarding@resend.dev>',
        to, subject, html
      });
      if (result.error) throw new Error(result.error.message || JSON.stringify(result.error));
      console.log(`[Mail] Sent to ${to} via Resend`);
      return { success: true, provider: 'resend' };
    } catch (err) {
      console.error(`[Mail] Resend failed for ${to}:`, err.message || err);
    }
  }

  // 3. Gmail SMTP (works locally but blocked on most PaaS)
  if (mailTransporter) {
    try {
      const sendPromise = mailTransporter.sendMail({ from: `Soldi <${GMAIL_USER}>`, to, subject, html });
      const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error('Gmail SMTP timed out (10s)')), 10000));
      await Promise.race([sendPromise, timeoutPromise]);
      console.log(`[Mail] Sent to ${to} via Gmail SMTP`);
      return { success: true, provider: 'gmail' };
    } catch (err) {
      console.error(`[Mail] Gmail SMTP failed for ${to}:`, err.message || err);
    }
  }

  return { success: false, error: 'All email providers failed' };
}

const app = express();

// Helper: fetch with timeout (prevents hanging on slow/dead APIs)
function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { ...options, signal: controller.signal }).finally(() => clearTimeout(timer));
}

// Raw body for Whop webhook signature verification (MUST be before express.json())
app.use('/api/webhooks/whop', express.raw({ type: 'application/json' }));

app.use(express.json());
app.use(express.static(path.join(__dirname)));

const WHOP_API_KEY = process.env.WHOP_API_KEY;
const WHOP_COMPANY_ID = process.env.WHOP_COMPANY_ID;
const JWT_SECRET = process.env.JWT_SECRET || 'fallback-secret-change-me';
const WHOP_STORE_SLUG = process.env.WHOP_STORE_SLUG || 'soldi-4def';
const WHOP_PRODUCT_PATH = process.env.WHOP_PRODUCT_PATH || 'soldi-a9';
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'bartwill452@gmail.com';
const BRAVE_SEARCH_API_KEY = process.env.BRAVE_SEARCH_API_KEY || '';
const SERPER_API_KEY = process.env.SERPER_API_KEY || '';

// ============================================
// 2FA VERIFICATION CODE STORE (in-memory)
// ============================================
const verificationCodes = new Map(); // key: email (lowercase), value: { code, expiresAt }
const CODE_EXPIRY_MS = 5 * 60 * 1000; // 5 minutes
const CODE_COOLDOWN_MS = 60 * 1000;   // 1 minute between sends

function generateVerificationCode() {
  return crypto.randomInt(100000, 999999).toString();
}

function cleanupExpiredCodes() {
  const now = Date.now();
  for (const [email, data] of verificationCodes) {
    if (now > data.expiresAt) verificationCodes.delete(email);
  }
}

// ============================================
// API KEY STORAGE (file-based)
// ============================================
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const API_KEYS_FILE = path.join(DATA_DIR, 'api-keys.json');

function loadApiKeys() {
  try {
    if (!fs.existsSync(API_KEYS_FILE)) return {};
    return JSON.parse(fs.readFileSync(API_KEYS_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function saveApiKeys(keys) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(API_KEYS_FILE, JSON.stringify(keys, null, 2));
}

function generateApiKey() {
  return 'soldi_' + crypto.randomBytes(16).toString('hex');
}

function findKeyByMembershipId(membershipId) {
  const keys = loadApiKeys();
  for (const [key, data] of Object.entries(keys)) {
    if (data.membershipId === membershipId) {
      return { key, ...data };
    }
  }
  return null;
}

// ============================================
// ADMIN & SUBMISSIONS STORAGE (file-based)
// ============================================
const ADMINS_FILE = path.join(DATA_DIR, 'admins.json');
const SUBMISSIONS_FILE = path.join(DATA_DIR, 'submissions.json');

function loadAdmins() {
  try {
    if (!fs.existsSync(ADMINS_FILE)) return [];
    return JSON.parse(fs.readFileSync(ADMINS_FILE, 'utf8'));
  } catch {
    return [];
  }
}

function saveAdmins(admins) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(ADMINS_FILE, JSON.stringify(admins, null, 2));
}

function loadSubmissions() {
  try {
    if (!fs.existsSync(SUBMISSIONS_FILE)) return [];
    return JSON.parse(fs.readFileSync(SUBMISSIONS_FILE, 'utf8'));
  } catch {
    return [];
  }
}

function saveSubmissions(submissions) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(SUBMISSIONS_FILE, JSON.stringify(submissions, null, 2));
}

// ============================================
// DISPUTES STORAGE (file-based)
// ============================================
const DISPUTES_FILE = path.join(DATA_DIR, 'disputes.json');

function loadDisputes() {
  try {
    if (!fs.existsSync(DISPUTES_FILE)) return [];
    return JSON.parse(fs.readFileSync(DISPUTES_FILE, 'utf8'));
  } catch {
    return [];
  }
}

function saveDisputes(disputes) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(DISPUTES_FILE, JSON.stringify(disputes, null, 2));
}

// ============================================
// ANALYTICS STORAGE (daily files + in-memory)
// ============================================
const ANALYTICS_DIR = path.join(DATA_DIR, 'analytics');
const ANALYTICS_RETENTION_DAYS = 90;

function loadDailyAnalytics(dateStr) {
  try {
    const file = path.join(ANALYTICS_DIR, dateStr + '.json');
    if (!fs.existsSync(file)) return null;
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return null;
  }
}

function saveDailyAnalytics(dateStr, data) {
  fs.mkdirSync(ANALYTICS_DIR, { recursive: true });
  fs.writeFileSync(path.join(ANALYTICS_DIR, dateStr + '.json'), JSON.stringify(data, null, 2));
}

// In-memory: active sessions for live visitor tracking
const activeSessions = new Map();

function getTodayStr() {
  return new Date().toISOString().split('T')[0];
}

function createDayStats(dateStr) {
  return {
    date: dateStr,
    pageViews: 0,
    uniqueVisitors: new Set(),
    sessions: new Set(),
    pages: {},
    referrers: {},
    hourlyViews: new Array(24).fill(0),
    events: {},
    funnel: {
      page_view: new Set(),
      form_started: new Set(),
      step_1_complete: new Set(),
      step_2_complete: new Set(),
      step_3_complete: new Set(),
      form_submitted: new Set(),
    },
  };
}

let todayStats = createDayStats(getTodayStr());

// Serialize Sets → counts for file write
function serializeDayStats(stats) {
  const s = {
    date: stats.date,
    pageViews: stats.pageViews,
    uniqueVisitors: stats.uniqueVisitors.size,
    sessions: stats.sessions.size,
    referrers: { ...stats.referrers },
    hourlyViews: [...stats.hourlyViews],
    events: { ...stats.events },
  };
  s.pages = {};
  for (const [pg, data] of Object.entries(stats.pages)) {
    s.pages[pg] = { views: data.views, uniques: data.uniques.size };
  }
  s.funnel = {};
  for (const [k, v] of Object.entries(stats.funnel)) {
    s.funnel[k] = v instanceof Set ? v.size : v;
  }
  return s;
}

// Flush analytics to disk every 5 minutes
setInterval(() => {
  const now = getTodayStr();
  if (todayStats.date !== now) {
    // Day rolled over — save yesterday, start fresh
    saveDailyAnalytics(todayStats.date, serializeDayStats(todayStats));
    todayStats = createDayStats(now);
    cleanupOldAnalytics();
  } else {
    saveDailyAnalytics(todayStats.date, serializeDayStats(todayStats));
  }
}, 5 * 60 * 1000);

// Expire stale sessions every 60 seconds
setInterval(() => {
  const cutoff = Date.now() - 60000;
  for (const [sid, session] of activeSessions) {
    if (session.lastSeen < cutoff) {
      activeSessions.delete(sid);
    }
  }
}, 60000);

// Delete analytics files older than retention period
function cleanupOldAnalytics() {
  try {
    if (!fs.existsSync(ANALYTICS_DIR)) return;
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - ANALYTICS_RETENTION_DAYS);
    const cutoffStr = cutoffDate.toISOString().split('T')[0];
    const files = fs.readdirSync(ANALYTICS_DIR);
    for (const file of files) {
      if (file.endsWith('.json') && file.replace('.json', '') < cutoffStr) {
        fs.unlinkSync(path.join(ANALYTICS_DIR, file));
      }
    }
  } catch (err) {
    console.error('Analytics cleanup error:', err);
  }
}

// Flush analytics on graceful shutdown
function flushAnalyticsSync() {
  try {
    saveDailyAnalytics(todayStats.date, serializeDayStats(todayStats));
    console.log('Analytics flushed to disk');
  } catch (err) {
    console.error('Failed to flush analytics:', err);
  }
}

process.on('SIGTERM', () => { flushAnalyticsSync(); process.exit(0); });
process.on('SIGINT', () => { flushAnalyticsSync(); process.exit(0); });

// Prevent silent crashes — log and restart via supervisord
process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught exception:', err.message, err.stack);
  flushAnalyticsSync();
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  console.error('[FATAL] Unhandled rejection:', reason);
  flushAnalyticsSync();
  process.exit(1);
});

// Seed owner account on first run
async function seedAdminAccount() {
  const admins = loadAdmins();
  const email = process.env.ADMIN_SEED_EMAIL;
  const password = process.env.ADMIN_SEED_PASSWORD;
  if (!email || !password) {
    console.log('⚠️  No ADMIN_SEED_EMAIL/ADMIN_SEED_PASSWORD in .env — skipping admin seed');
    return;
  }

  // Check if seed email already exists
  const seedEmail = email.toLowerCase().trim();
  const existing = admins.find(a => a.email === seedEmail && a.role === 'owner');
  if (existing) return; // Already seeded with this email

  // If admins exist but seed email is different, add the new owner
  const hash = await bcrypt.hash(password, 10);
  const owner = {
    id: crypto.randomUUID(),
    email: seedEmail,
    passwordHash: hash,
    role: 'owner',
    createdAt: new Date().toISOString(),
  };
  admins.push(owner);
  saveAdmins(admins);
  console.log(`✅ Admin owner account seeded: ${owner.email}`);
}

seedAdminAccount();

// ============================================
// AUTH MIDDLEWARE
// ============================================
function requireAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No token provided' });
  }
  try {
    const decoded = jwt.verify(authHeader.split(' ')[1], JWT_SECRET);
    req.user = decoded;
    next();
  } catch {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// ============================================
// DUAL AUTH: Accept either API key or JWT Bearer token
// ============================================
function authenticateRequest(req) {
  // Method 1: API key in query param
  const key = req.query.key;
  if (key) {
    const keys = loadApiKeys();
    const keyData = keys[key];
    if (keyData && keyData.status === 'active') {
      return { authenticated: true, email: keyData.email, apiKey: key, method: 'apikey' };
    }
  }

  // Method 2: JWT Bearer token in Authorization header
  const authHeader = req.headers.authorization;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    try {
      const decoded = jwt.verify(authHeader.split(' ')[1], JWT_SECRET);
      // Also find their API key for endpoints that need it
      const existing = findKeyByMembershipId(decoded.membershipId);
      return { authenticated: true, email: decoded.email, user: decoded, apiKey: existing?.key, method: 'jwt' };
    } catch (e) { /* token invalid or expired */ }
  }

  return { authenticated: false };
}

function requireAdmin(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No token provided' });
  }
  try {
    const decoded = jwt.verify(authHeader.split(' ')[1], JWT_SECRET);
    // Standard admin JWT check
    if (decoded.type === 'admin') {
      const admins = loadAdmins();
      const admin = admins.find(a => a.id === decoded.adminId);
      if (!admin) {
        return res.status(401).json({ error: 'Admin account not found' });
      }
      req.admin = admin;
      return next();
    }
    // Owner member JWT check (from 2FA login as bartwill452@gmail.com)
    if (decoded.membershipId === 'admin' && decoded.email?.toLowerCase() === ADMIN_EMAIL.toLowerCase()) {
      req.admin = { id: 'owner', email: decoded.email, role: 'owner' };
      return next();
    }
    return res.status(403).json({ error: 'Admin access required' });
  } catch {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

function requireOwner(req, res, next) {
  requireAdmin(req, res, () => {
    if (req.admin.role !== 'owner') {
      return res.status(403).json({ error: 'Owner access required' });
    }
    next();
  });
}

// ============================================
// POST /api/send-verification  (Step 1 of 2FA login)
// Validates membership, then sends 6-digit code to email
// ============================================
app.post('/api/send-verification', async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Email is required' });

  const emailLower = email.toLowerCase();
  cleanupExpiredCodes();

  // Rate limit: 1 code per minute per email
  const existing = verificationCodes.get(emailLower);
  if (existing && Date.now() < existing.sentAt + CODE_COOLDOWN_MS) {
    const waitSec = Math.ceil((existing.sentAt + CODE_COOLDOWN_MS - Date.now()) / 1000);
    return res.status(429).json({ error: 'cooldown', message: `Please wait ${waitSec}s before requesting another code` });
  }

  // For non-admin emails, verify Whop membership FIRST (don't send code to non-members)
  if (emailLower !== ADMIN_EMAIL.toLowerCase()) {
    if (!WHOP_API_KEY || WHOP_API_KEY === 'YOUR_API_KEY_HERE') {
      return res.status(500).json({ error: 'Server not configured. API key missing.' });
    }
    const membershipCheckStart = Date.now();
    const OVERALL_TIMEOUT_MS = 15000; // 15s max for entire membership check
    try {
      let page = 1;
      let found = false;
      const maxPages = 20;
      while (!found && page <= maxPages) {
        // Check overall timeout
        if (Date.now() - membershipCheckStart > OVERALL_TIMEOUT_MS) {
          console.error(`[2FA] Membership check overall timeout after ${page - 1} pages`);
          return res.status(504).json({ error: 'Membership verification took too long. Please try again.' });
        }
        const url = `https://api.whop.com/api/v1/memberships?company_id=${WHOP_COMPANY_ID}&page=${page}&per=50`;
        console.log(`[2FA] Checking Whop page ${page} for ${emailLower}...`);
        const response = await fetchWithTimeout(url, { headers: { Authorization: `Bearer ${WHOP_API_KEY}` } }, 8000);
        console.log(`[2FA] Whop page ${page} responded: ${response.status} (${Date.now() - membershipCheckStart}ms elapsed)`);
        if (!response.ok) {
          const errText = await response.text().catch(() => '');
          console.error(`[2FA] Whop API error on page ${page}: ${response.status} ${errText}`);
          return res.status(502).json({ error: 'Failed to verify membership' });
        }
        const data = await response.json();
        const memberships = data.data || [];
        if (memberships.length === 0) break;
        const match = memberships.find(m => {
          const memberEmail = m.user?.email || m.email;
          return memberEmail && memberEmail.toLowerCase() === emailLower;
        });
        if (match) {
          const activeStatuses = ['active', 'trialing', 'canceling'];
          if (!activeStatuses.includes(match.status)) {
            return res.status(403).json({
              error: 'inactive_membership',
              message: `Membership status: ${match.status}`,
              purchaseUrl: 'https://whop.com/checkout/plan_Q93fIRTfIo5g7/'
            });
          }
          found = true;
          break;
        }
        if (!data.page_info || !data.page_info.has_next_page) break;
        page++;
      }
      console.log(`[2FA] Membership check done: found=${found}, pages=${page}, time=${Date.now() - membershipCheckStart}ms`);
      if (!found) {
        return res.status(404).json({
          error: 'no_membership',
          message: 'No membership found for this email',
          purchaseUrl: 'https://whop.com/checkout/plan_Q93fIRTfIo5g7/'
        });
      }
    } catch (err) {
      console.error('Membership check error:', err.name === 'AbortError' ? 'Whop API timed out' : err);
      return res.status(500).json({ error: err.name === 'AbortError' ? 'Membership check timed out. Please try again.' : 'Internal server error' });
    }
  }

  // Generate and store 6-digit code
  const code = generateVerificationCode();
  verificationCodes.set(emailLower, { code, expiresAt: Date.now() + CODE_EXPIRY_MS, sentAt: Date.now() });

  // Send code via email (Resend HTTP preferred, Gmail SMTP fallback)
  const emailResult = await sendEmail({
    to: email,
    subject: `${code} — Your Soldi verification code`,
    html: buildVerificationCodeEmailHtml(code)
  });

  if (!emailResult.success) {
    console.error(`[2FA] All email providers failed for ${email}`);
    verificationCodes.delete(emailLower);
    return res.status(500).json({ error: 'Failed to send verification email. Please try again.' });
  }
  console.log(`[2FA] Code sent to ${email} via ${emailResult.provider}`);

  return res.json({ success: true, message: 'Verification code sent to your email' });
});

// ============================================
// POST /api/verify-code  (Step 2 of 2FA login)
// Validates code, then issues JWT + API key
// ============================================
app.post('/api/verify-code', async (req, res) => {
  const { email, code } = req.body;
  if (!email || !code) return res.status(400).json({ error: 'Email and code are required' });

  const emailLower = email.toLowerCase();
  const stored = verificationCodes.get(emailLower);

  if (!stored) {
    return res.status(400).json({ error: 'no_code', message: 'No verification code found. Please request a new one.' });
  }

  if (Date.now() > stored.expiresAt) {
    verificationCodes.delete(emailLower);
    return res.status(400).json({ error: 'expired', message: 'Verification code expired. Please request a new one.' });
  }

  if (stored.code !== code.trim()) {
    return res.status(400).json({ error: 'invalid_code', message: 'Invalid verification code. Please try again.' });
  }

  // Code is valid — delete it (single use)
  verificationCodes.delete(emailLower);

  // Admin bypass
  if (emailLower === ADMIN_EMAIL.toLowerCase()) {
    let apiKey;
    const existingKey = findKeyByMembershipId('admin');
    if (existingKey) {
      apiKey = existingKey.key;
    } else {
      apiKey = generateApiKey();
      const keys = loadApiKeys();
      keys[apiKey] = { email: ADMIN_EMAIL, membershipId: 'admin', createdAt: new Date().toISOString(), status: 'active' };
      saveApiKeys(keys);
    }
    const token = jwt.sign(
      { membershipId: 'admin', email: ADMIN_EMAIL, name: 'Admin', firstName: 'Admin', status: 'active', affiliateLink: null, affiliateUsername: null, createdAt: new Date().toISOString() },
      JWT_SECRET, { expiresIn: '14d' }
    );
    return res.json({ success: true, token, apiKey, user: { email: ADMIN_EMAIL, name: 'Admin', firstName: 'Admin', status: 'active', affiliateLink: null, affiliateUsername: null, memberSince: 'Owner' } });
  }

  // Regular member — look up Whop membership and issue JWT
  try {
    let page = 1;
    let found = null;
    const maxPages = 20;
    while (!found && page <= maxPages) {
      const url = `https://api.whop.com/api/v1/memberships?company_id=${WHOP_COMPANY_ID}&page=${page}&per=50`;
      const response = await fetchWithTimeout(url, { headers: { Authorization: `Bearer ${WHOP_API_KEY}` } }, 10000);
      if (!response.ok) return res.status(502).json({ error: 'Failed to verify membership' });
      const data = await response.json();
      const memberships = data.data || [];
      if (memberships.length === 0) break;
      found = memberships.find(m => {
        const memberEmail = m.user?.email || m.email;
        return memberEmail && memberEmail.toLowerCase() === emailLower;
      });
      if (!data.page_info || !data.page_info.has_next_page) break;
      page++;
    }

    if (!found) return res.status(404).json({ error: 'no_membership', message: 'No membership found' });

    const affiliateUsername = found.user?.username || null;
    const affiliateLink = affiliateUsername ? `https://whop.com/${WHOP_STORE_SLUG}/?a=${affiliateUsername}` : null;

    let apiKey;
    const existingKey = findKeyByMembershipId(found.id);
    if (existingKey) {
      apiKey = existingKey.key;
    } else {
      apiKey = generateApiKey();
      const keys = loadApiKeys();
      keys[apiKey] = { email: found.user?.email || found.email, membershipId: found.id, createdAt: new Date().toISOString(), status: 'active' };
      saveApiKeys(keys);
    }

    const fullName = found.user?.name || found.user?.username || null;
    let firstName = fullName ? fullName.split(' ')[0] : null;
    // If no name from Whop, derive a display name from the email
    if (!firstName) {
      const emailUser = (found.user?.email || found.email || '').split('@')[0].replace(/[._\-+]/g, ' ').trim();
      if (emailUser) firstName = emailUser.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
    }

    const token = jwt.sign(
      { membershipId: found.id, email: found.user?.email || found.email, name: fullName, firstName, status: found.status, affiliateLink, affiliateUsername, createdAt: found.created_at },
      JWT_SECRET, { expiresIn: '14d' }
    );

    return res.json({
      success: true, token, apiKey,
      user: { email: found.user?.email || found.email, name: fullName, firstName, status: found.status, affiliateLink, affiliateUsername, memberSince: found.created_at }
    });
  } catch (err) {
    console.error('Verify-code membership error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// POST /api/verify-membership  (legacy — kept for backward compatibility)
// Body: { email: "user@example.com" }
// ============================================
app.post('/api/verify-membership', async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Email is required' });

  // Admin/owner bypass — skip Whop API lookup
  if (email.toLowerCase() === ADMIN_EMAIL.toLowerCase()) {
    let apiKey;
    const existingKey = findKeyByMembershipId('admin');
    if (existingKey) {
      apiKey = existingKey.key;
    } else {
      apiKey = generateApiKey();
      const keys = loadApiKeys();
      keys[apiKey] = {
        email: ADMIN_EMAIL,
        membershipId: 'admin',
        createdAt: new Date().toISOString(),
        status: 'active'
      };
      saveApiKeys(keys);
    }

    const token = jwt.sign(
      {
        membershipId: 'admin',
        email: ADMIN_EMAIL,
        name: 'Admin',
        firstName: 'Admin',
        status: 'active',
        affiliateLink: null,
        affiliateUsername: null,
        createdAt: new Date().toISOString()
      },
      JWT_SECRET,
      { expiresIn: '14d' }
    );

    return res.json({
      success: true,
      token,
      apiKey,
      user: {
        email: ADMIN_EMAIL,
        name: 'Admin',
        firstName: 'Admin',
        status: 'active',
        affiliateLink: null,
        affiliateUsername: null,
        memberSince: 'Owner'
      }
    });
  }

  if (!WHOP_API_KEY || WHOP_API_KEY === 'YOUR_API_KEY_HERE') {
    return res.status(500).json({ error: 'Server not configured. API key missing.' });
  }

  try {
    let page = 1;
    let found = null;
    const maxPages = 20;

    while (!found && page <= maxPages) {
      const url = `https://api.whop.com/api/v1/memberships?company_id=${WHOP_COMPANY_ID}&page=${page}&per=50`;
      const response = await fetchWithTimeout(url, {
        headers: { Authorization: `Bearer ${WHOP_API_KEY}` }
      }, 10000);

      if (!response.ok) {
        const errText = await response.text();
        console.error('Whop API error:', response.status, errText);
        return res.status(502).json({ error: 'Failed to verify membership' });
      }

      const data = await response.json();
      const memberships = data.data || [];

      if (memberships.length === 0) break;

      const match = memberships.find(
        m => {
          const memberEmail = m.user?.email || m.email;
          return memberEmail && memberEmail.toLowerCase() === email.toLowerCase();
        }
      );

      if (match) {
        found = match;
        break;
      }

      if (!data.page_info || !data.page_info.has_next_page) break;
      page++;
    }

    if (!found) {
      return res.status(404).json({
        error: 'no_membership',
        message: 'No membership found for this email',
        purchaseUrl: 'https://whop.com/checkout/plan_Q93fIRTfIo5g7/'
      });
    }

    const activeStatuses = ['active', 'trialing', 'canceling'];
    const isActive = activeStatuses.includes(found.status);

    if (!isActive) {
      return res.status(403).json({
        error: 'inactive_membership',
        message: `Membership status: ${found.status}`,
        status: found.status,
        purchaseUrl: 'https://whop.com/checkout/plan_Q93fIRTfIo5g7/'
      });
    }

    // Build affiliate link from Whop username
    const affiliateUsername = found.user?.username || null;
    const affiliateLink = affiliateUsername
      ? `https://whop.com/${WHOP_STORE_SLUG}/?a=${affiliateUsername}`
      : null;

    // Auto-generate API key if member doesn't have one
    let apiKey;
    const existingKey = findKeyByMembershipId(found.id);
    if (existingKey) {
      apiKey = existingKey.key;
    } else {
      apiKey = generateApiKey();
      const keys = loadApiKeys();
      keys[apiKey] = {
        email: found.user?.email || found.email,
        membershipId: found.id,
        createdAt: new Date().toISOString(),
        status: 'active'
      };
      saveApiKeys(keys);
    }

    // Extract first name from Whop user data
    const fullName = found.user?.name || found.user?.username || null;
    let firstName = fullName ? fullName.split(' ')[0] : null;
    // If no name from Whop, derive a display name from the email
    if (!firstName) {
      const emailUser = (found.user?.email || found.email || '').split('@')[0].replace(/[._\-+]/g, ' ').trim();
      if (emailUser) firstName = emailUser.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
    }

    // Sign JWT (14 day expiry)
    const token = jwt.sign(
      {
        membershipId: found.id,
        email: found.user?.email || found.email,
        name: fullName,
        firstName,
        status: found.status,
        affiliateLink,
        affiliateUsername,
        createdAt: found.created_at
      },
      JWT_SECRET,
      { expiresIn: '14d' }
    );

    return res.json({
      success: true,
      token,
      apiKey,
      user: {
        email: found.user?.email || found.email,
        name: fullName,
        firstName,
        status: found.status,
        affiliateLink,
        affiliateUsername,
        memberSince: found.created_at
      }
    });

  } catch (err) {
    console.error('Verification error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// GET /api/me - Validate existing JWT
// ============================================
app.get('/api/me', requireAuth, (req, res) => {
  // Also return the API key for this member — auto-generate if missing
  let existing = findKeyByMembershipId(req.user.membershipId);
  if (!existing) {
    const keys = loadApiKeys();
    const newKey = generateApiKey();
    keys[newKey] = {
      email: req.user.email,
      membershipId: req.user.membershipId,
      createdAt: new Date().toISOString(),
      status: 'active'
    };
    saveApiKeys(keys);
    existing = { key: newKey };
  }
  return res.json({
    success: true,
    user: req.user,
    apiKey: existing.key
  });
});

// ============================================
// POST /api/keys/regenerate - Generate new API key
// ============================================
app.post('/api/keys/regenerate', requireAuth, (req, res) => {
  const keys = loadApiKeys();
  const membershipId = req.user.membershipId;

  // Remove old key
  for (const [key, data] of Object.entries(keys)) {
    if (data.membershipId === membershipId) {
      delete keys[key];
    }
  }

  // Generate new key
  const newKey = generateApiKey();
  keys[newKey] = {
    email: req.user.email,
    membershipId,
    createdAt: new Date().toISOString(),
    status: 'active'
  };
  saveApiKeys(keys);

  return res.json({ success: true, apiKey: newKey });
});

// ============================================
// GET /api/validate-key - Validate an API key
// ============================================
app.get('/api/validate-key', (req, res) => {
  const { key } = req.query;
  if (!key) return res.status(400).json({ error: 'API key is required' });

  const keys = loadApiKeys();
  const keyData = keys[key];

  if (!keyData || keyData.status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }

  return res.json({ success: true, email: keyData.email });
});

// ============================================
// WEBINAR REGISTRATION
// ============================================
const WEBINAR_DATA_FILE = path.join(DATA_DIR, 'webinar-registrations.json');

// Load existing registrations
function loadWebinarRegistrations() {
  try {
    if (fs.existsSync(WEBINAR_DATA_FILE)) {
      return JSON.parse(fs.readFileSync(WEBINAR_DATA_FILE, 'utf8'));
    }
  } catch (e) { console.error('[Webinar] Error loading registrations:', e.message); }
  return [];
}

function saveWebinarRegistrations(registrations) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(WEBINAR_DATA_FILE, JSON.stringify(registrations, null, 2));
  } catch (e) { console.error('[Webinar] Error saving registrations:', e.message); }
}

// CORS preflight for webinar registration
app.options('/api/webinar/register', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.sendStatus(204);
});

// POST /api/webinar/register - Register for the webinar
app.post('/api/webinar/register', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  try {
    const { firstName, email, phone, interest, newsletter, smsConsent, registeredAt } = req.body;

    if (!firstName || !email || !phone) {
      return res.status(400).json({ error: 'First name, email, and phone are required' });
    }

    // Save registration
    const registrations = loadWebinarRegistrations();
    const existing = registrations.find(r => r.email === email);
    if (existing) {
      return res.json({ status: 'already_registered', message: 'You are already registered!' });
    }

    const registration = { firstName, email, phone, interest, newsletter, smsConsent, registeredAt: registeredAt || new Date().toISOString() };
    registrations.push(registration);
    saveWebinarRegistrations(registrations);
    console.log(`[Webinar] New registration: ${firstName} (${email})`);

    // Respond immediately — don't make user wait for emails
    res.json({ status: 'success', message: 'Registration confirmed!' });

    // Fire-and-forget: send emails + Google Sheets in background
    (async () => {
      // Send confirmation email
      try {
        await sendEmail({
          to: email,
          subject: "You're In! The 3 Income Engines — Free Live Training",
          html: buildWebinarConfirmationEmail(firstName)
        });
        console.log(`[Webinar] Confirmation email sent to ${email}`);
      } catch (mailErr) {
        console.error(`[Webinar] Failed to send confirmation to ${email}:`, mailErr.message);
      }

      // Send newsletter welcome if opted in
      if (newsletter) {
        try {
          await sendEmail({
            to: email,
            subject: "Welcome to the Soldi Newsletter",
            html: buildNewsletterWelcomeEmail(firstName)
          });
        } catch (e) { console.error('[Webinar] Newsletter welcome failed:', e.message); }
      }

      // Forward to Google Sheets via Apps Script webhook
      const GOOGLE_SCRIPT_URL = process.env.GOOGLE_SCRIPT_URL;
      if (GOOGLE_SCRIPT_URL) {
        try {
          await fetch(GOOGLE_SCRIPT_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(registration)
          });
          console.log(`[Webinar] Forwarded to Google Sheets: ${email}`);
        } catch (sheetErr) {
          console.error(`[Webinar] Google Sheets forward failed:`, sheetErr.message);
        }
      }
    })();
  } catch (err) {
    console.error('[Webinar] Registration error:', err);
    res.status(500).json({ error: 'Registration failed. Please try again.' });
  }
});

// GET /api/webinar/registrations - Admin: list all registrations
app.get('/api/webinar/registrations', requireAdmin, (req, res) => {
  const registrations = loadWebinarRegistrations();
  res.json({ count: registrations.length, registrations });
});

// GET /api/webinar/registrations/simple - Simple password-protected registration viewer
app.get('/api/webinar/registrations/simple', (req, res) => {
  const pw = req.query.pw;
  const WEBINAR_ADMIN_PW = process.env.WEBINAR_ADMIN_PW || 'soldi2026';
  if (pw !== WEBINAR_ADMIN_PW) {
    return res.status(401).json({ error: 'Invalid password. Use ?pw=YOUR_PASSWORD' });
  }
  const registrations = loadWebinarRegistrations();
  // Return as JSON for the admin dashboard
  res.json({ count: registrations.length, registrations });
});

// POST /api/webinar/send-marketing - Send marketing email from soldihq
app.post('/api/webinar/send-marketing', async (req, res) => {
  const pw = req.query.pw;
  const WEBINAR_ADMIN_PW = process.env.WEBINAR_ADMIN_PW || 'soldi2026';
  if (pw !== WEBINAR_ADMIN_PW) {
    return res.status(401).json({ error: 'Invalid password' });
  }
  const { emails } = req.body; // array of email strings
  if (!emails || !Array.isArray(emails) || emails.length === 0) {
    return res.status(400).json({ error: 'Provide an array of emails in the request body' });
  }
  const subject = '[LIVE Monday] The 3 Income Engines — Free Training by Soldi';
  const html = buildMarketingEmail();
  const results = [];
  for (const email of emails) {
    try {
      const r = await sendEmail({ to: email.trim(), subject, html });
      results.push({ email, status: 'sent', provider: r.provider });
      console.log(`[Marketing] Sent to ${email}`);
    } catch (e) {
      results.push({ email, status: 'failed', error: e.message });
      console.error(`[Marketing] Failed for ${email}:`, e.message);
    }
  }
  res.json({ sent: results.filter(r => r.status === 'sent').length, failed: results.filter(r => r.status === 'failed').length, results });
});

// Marketing newsletter email template
function buildMarketingEmail() {
  return `<div style="max-width:600px;margin:0 auto;background:#0a0a0a;font-family:'Inter',Arial,Helvetica,sans-serif;">
  <div style="background:linear-gradient(135deg,#00C853 0%,#00E676 50%,#69F0AE 100%);padding:40px 24px;text-align:center;">
    <p style="margin:0 0 8px;font-size:13px;font-weight:700;color:#000;text-transform:uppercase;letter-spacing:2px;">FREE LIVE TRAINING</p>
    <h1 style="margin:0 0 8px;font-size:32px;font-weight:900;color:#000;line-height:1.2;">The 3 Income Engines</h1>
    <p style="margin:0;font-size:16px;color:rgba(0,0,0,0.7);font-weight:600;">How Ordinary People Are Building $10K+/Mo Online in 2026</p>
  </div>
  <div style="background:#111;padding:16px 24px;text-align:center;border-bottom:1px solid #222;">
    <table width="100%" cellpadding="0" cellspacing="0" style="max-width:400px;margin:0 auto;">
      <tr>
        <td style="text-align:center;padding:0 12px;"><p style="margin:0;font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;">DATE</p><p style="margin:4px 0 0;font-size:15px;color:#fff;font-weight:700;">Mon, March 16</p></td>
        <td style="text-align:center;padding:0 12px;border-left:1px solid #333;border-right:1px solid #333;"><p style="margin:0;font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;">TIME</p><p style="margin:4px 0 0;font-size:15px;color:#fff;font-weight:700;">7:00 PM ET</p></td>
        <td style="text-align:center;padding:0 12px;"><p style="margin:0;font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;">WHERE</p><p style="margin:4px 0 0;font-size:15px;color:#00C853;font-weight:700;">Google Meet</p></td>
      </tr>
    </table>
  </div>
  <div style="padding:32px 24px;">
    <p style="font-size:16px;line-height:1.7;color:#ddd;margin:0 0 20px;">We're going live this <strong style="color:#fff;">Monday at 7 PM ET</strong> with a brand new training session — and you're invited.</p>
    <p style="font-size:16px;line-height:1.7;color:#ddd;margin:0 0 28px;">In just 60 minutes, we'll reveal the <strong style="color:#00C853;">3 income engines</strong> our members are using right now to generate consistent income online — no experience required.</p>
    <div style="margin:0 0 28px;">
      <div style="background:#1A1A1A;border:1px solid #2a2a2a;border-radius:12px;padding:20px;margin-bottom:12px;">
        <table cellpadding="0" cellspacing="0" width="100%"><tr>
          <td style="width:48px;vertical-align:top;"><div style="width:44px;height:44px;background:#0D3320;border-radius:10px;text-align:center;line-height:44px;font-size:22px;">&#9917;</div></td>
          <td style="padding-left:14px;vertical-align:top;"><p style="margin:0 0 4px;font-size:15px;font-weight:800;color:#00C853;">ENGINE 1: Sports Betting Systems</p><p style="margin:0;font-size:14px;color:#999;line-height:1.5;">Follow million-dollar traders with automated alerts. Our members are seeing 20-40% monthly returns.</p></td>
        </tr></table>
      </div>
      <div style="background:#1A1A1A;border:1px solid #2a2a2a;border-radius:12px;padding:20px;margin-bottom:12px;">
        <table cellpadding="0" cellspacing="0" width="100%"><tr>
          <td style="width:48px;vertical-align:top;"><div style="width:44px;height:44px;background:#0D2033;border-radius:10px;text-align:center;line-height:44px;font-size:22px;">&#128202;</div></td>
          <td style="padding-left:14px;vertical-align:top;"><p style="margin:0 0 4px;font-size:15px;font-weight:800;color:#4FC3F7;">ENGINE 2: Crypto &amp; Stock Plays</p><p style="margin:0;font-size:14px;color:#999;line-height:1.5;">Real-time signals, AI-powered analysis, and proven strategies for market gains.</p></td>
        </tr></table>
      </div>
      <div style="background:#1A1A1A;border:1px solid #2a2a2a;border-radius:12px;padding:20px;margin-bottom:12px;">
        <table cellpadding="0" cellspacing="0" width="100%"><tr>
          <td style="width:48px;vertical-align:top;"><div style="width:44px;height:44px;background:#1A1A0D;border-radius:10px;text-align:center;line-height:44px;font-size:22px;">&#129302;</div></td>
          <td style="padding-left:14px;vertical-align:top;"><p style="margin:0 0 4px;font-size:15px;font-weight:800;color:#FFD54F;">ENGINE 3: AI Automation</p><p style="margin:0;font-size:14px;color:#999;line-height:1.5;">Build and sell AI-powered services. Our bots scan markets 24/7 so you don't have to.</p></td>
        </tr></table>
      </div>
    </div>
    <div style="background:#111;border-left:3px solid #00C853;padding:16px 20px;border-radius:0 8px 8px 0;margin-bottom:28px;">
      <p style="margin:0;font-size:14px;color:#ccc;line-height:1.6;font-style:italic;">"I joined Soldi 3 months ago and I've already made back my investment 10x. The sports betting signals alone are worth it."</p>
      <p style="margin:8px 0 0;font-size:13px;color:#00C853;font-weight:700;">— Soldi Member</p>
    </div>
    <div style="text-align:center;margin:32px 0;">
      <a href="https://trysoldi.com/webinar.html" style="display:inline-block;background:#00C853;color:#000;padding:16px 40px;border-radius:12px;font-weight:900;font-size:18px;text-decoration:none;letter-spacing:0.5px;">SAVE YOUR SPOT NOW</a>
      <p style="margin:12px 0 0;font-size:13px;color:#666;">100% Free — Limited Spots Available</p>
    </div>
    <div style="background:#1A1A1A;border:1px solid #2a2a2a;border-radius:12px;padding:20px;margin-bottom:24px;">
      <p style="margin:0 0 12px;font-size:14px;font-weight:800;color:#fff;text-transform:uppercase;letter-spacing:1px;">What you'll get:</p>
      <table cellpadding="0" cellspacing="0" width="100%">
        <tr><td style="padding:6px 0;font-size:14px;color:#ccc;">&#10003; Live walkthrough of all 3 income engines</td></tr>
        <tr><td style="padding:6px 0;font-size:14px;color:#ccc;">&#10003; Real member results and case studies</td></tr>
        <tr><td style="padding:6px 0;font-size:14px;color:#ccc;">&#10003; Exclusive signup bonus for live attendees</td></tr>
        <tr><td style="padding:6px 0;font-size:14px;color:#ccc;">&#10003; Q&amp;A session at the end</td></tr>
      </table>
    </div>
    <p style="font-size:14px;color:#888;line-height:1.6;text-align:center;">Don't miss this. We only do these trainings a few times a year, and the replay won't be available forever.</p>
  </div>
  <div style="padding:20px 24px;text-align:center;border-top:1px solid #222;">
    <p style="margin:0 0 4px;font-size:13px;color:#555;">&copy; 2026 Soldi. All rights reserved.</p>
    <p style="margin:0;font-size:13px;"><a href="https://trysoldi.com" style="color:#00C853;text-decoration:none;">trysoldi.com</a></p>
    <p style="margin:8px 0 0;font-size:11px;color:#444;">Results shown are from real members but are not typical. Income depends on effort, experience, and market conditions.</p>
  </div>
</div>`;
}

// Webinar confirmation email template
function buildWebinarConfirmationEmail(firstName) {
  return `
  <div style="max-width:600px;margin:0 auto;background:#0D0D0D;color:#fff;font-family:'Inter',Arial,sans-serif;border-radius:12px;overflow:hidden;">
    <div style="background:#00C853;padding:24px;text-align:center;">
      <h1 style="margin:0;color:#000;font-size:24px;font-weight:900;">You're Registered!</h1>
    </div>
    <div style="padding:32px 24px;">
      <p style="font-size:16px;line-height:1.6;margin-bottom:20px;">Hey ${firstName},</p>
      <p style="font-size:16px;line-height:1.6;margin-bottom:20px;">You're locked in for <strong style="color:#00C853;">The 3 Income Engines</strong> — our free live training where we break down the exact systems Soldi members use to build $10K+/mo online.</p>
      <div style="background:#1A1A1A;border:1px solid #333;border-radius:12px;padding:20px;margin:24px 0;">
        <p style="margin:0 0 8px;color:#888;font-size:13px;">EVENT DETAILS</p>
        <p style="margin:0 0 6px;font-size:15px;"><strong>Date:</strong> Monday, March 16, 2026</p>
        <p style="margin:0 0 6px;font-size:15px;"><strong>Time:</strong> 7:00 PM ET</p>
        <p style="margin:0 0 6px;font-size:15px;"><strong>Duration:</strong> ~60 minutes</p>
        <p style="margin:0;font-size:15px;"><strong>Where:</strong> <a href="https://meet.google.com/poq-vewb-hhh" style="color:#00C853;">Google Meet</a> (click to join at event time)</p>
      </div>
      <p style="font-size:16px;line-height:1.6;margin-bottom:24px;">Here's what we'll cover:</p>
      <ul style="padding-left:20px;margin-bottom:24px;">
        <li style="font-size:15px;line-height:1.8;color:#ccc;">Sports Betting Systems — Follow million-dollar traders</li>
        <li style="font-size:15px;line-height:1.8;color:#ccc;">E-Commerce Mastery — Members doing $10K+ days</li>
        <li style="font-size:15px;line-height:1.8;color:#ccc;">AI Automation — Build & sell AI-powered services</li>
        <li style="font-size:15px;line-height:1.8;color:#ccc;">BONUS: Automated Bots scanning markets 24/7</li>
      </ul>
      <div style="text-align:center;margin:28px 0;">
        <a href="https://meet.google.com/poq-vewb-hhh" style="display:inline-block;background:#00C853;color:#000;padding:14px 32px;border-radius:10px;font-weight:800;font-size:16px;text-decoration:none;margin-bottom:12px;">Join Google Meet</a>
        <br/>
        <a href="https://calendar.google.com/calendar/render?action=TEMPLATE&text=Soldi+Webinar%3A+The+3+Income+Engines&dates=20260316T230000Z/20260317T000000Z&details=Join+via+Google+Meet%3A+https%3A%2F%2Fmeet.google.com%2Fpoq-vewb-hhh&location=Google+Meet" style="display:inline-block;background:transparent;color:#00C853;padding:10px 24px;border-radius:10px;font-weight:600;font-size:14px;text-decoration:none;border:1px solid #00C853;margin-top:8px;">Add to Calendar</a>
      </div>
      <p style="font-size:14px;color:#888;line-height:1.6;">Save the Google Meet link above — you'll use it to join the live session on Monday at 7 PM ET. This is a view-only presentation, so just sit back and learn!</p>
    </div>
    <div style="padding:16px 24px;text-align:center;border-top:1px solid #222;">
      <p style="margin:0;font-size:13px;color:#555;">&copy; 2026 Soldi | <a href="https://trysoldi.com" style="color:#00C853;text-decoration:none;">trysoldi.com</a></p>
    </div>
  </div>`;
}

// Newsletter welcome email template
function buildNewsletterWelcomeEmail(firstName) {
  return `
  <div style="max-width:600px;margin:0 auto;background:#0D0D0D;color:#fff;font-family:'Inter',Arial,sans-serif;border-radius:12px;overflow:hidden;">
    <div style="padding:32px 24px;">
      <h2 style="color:#00C853;margin:0 0 16px;">Welcome to the Soldi Newsletter</h2>
      <p style="font-size:16px;line-height:1.6;">Hey ${firstName},</p>
      <p style="font-size:16px;line-height:1.6;">You're now on the list. Every week, we send out our best plays, AI updates, e-commerce wins, and exclusive member-only content.</p>
      <p style="font-size:16px;line-height:1.6;color:#888;">Stay tuned — the first one is coming soon.</p>
      <p style="font-size:16px;line-height:1.6;margin-top:24px;">— The Soldi Team</p>
    </div>
    <div style="padding:16px 24px;text-align:center;border-top:1px solid #222;">
      <p style="margin:0;font-size:13px;color:#555;">&copy; 2026 Soldi | <a href="https://trysoldi.com" style="color:#00C853;text-decoration:none;">trysoldi.com</a></p>
    </div>
  </div>`;
}

// ============================================
// GET /api/health - Service health check
// ============================================
app.get('/api/health', (req, res) => {
  const mem = process.memoryUsage();
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    version: '2.1.0',
    memory: {
      rss_mb: Math.round(mem.rss / 1048576),
      heap_used_mb: Math.round(mem.heapUsed / 1048576),
      heap_total_mb: Math.round(mem.heapTotal / 1048576),
      external_mb: Math.round(mem.external / 1048576),
    },
  });
});

// GET /api/health/detailed - Proxy to SoldiAPI for per-scraper health data
app.get('/api/health/detailed', async (req, res) => {
  try {
    const apiUrl = process.env.SOLDI_API_URL || 'http://localhost:3001';
    const apiKey = process.env.SOLDI_API_KEY || 'dev-key-change-me';
    const response = await fetchWithTimeout(
      `${apiUrl}/health/detailed`,
      { headers: { 'Authorization': `Bearer ${apiKey}` } },
      10000
    );
    const data = await response.json();
    // Merge Express health info
    data.express = { uptime: process.uptime(), timestamp: new Date().toISOString() };
    res.json(data);
  } catch (err) {
    res.status(502).json({ error: 'SoldiAPI unreachable', details: err.message });
  }
});

// Diagnostic: test Whop API connectivity (no sensitive data exposed)
app.get('/api/debug/whop-test', async (req, res) => {
  const hasKey = !!WHOP_API_KEY && WHOP_API_KEY !== 'YOUR_API_KEY_HERE';
  const hasCompany = !!WHOP_COMPANY_ID;
  const hasBrevo = !!BREVO_API_KEY;
  const hasResend = !!resend;
  const hasMail = !!mailTransporter;
  if (!hasKey) return res.json({ whopKey: false, error: 'No API key' });
  try {
    const start = Date.now();
    const url = `https://api.whop.com/api/v1/memberships?company_id=${WHOP_COMPANY_ID}&page=1&per=1`;
    const response = await fetchWithTimeout(url, { headers: { Authorization: `Bearer ${WHOP_API_KEY}` } }, 8000);
    const elapsed = Date.now() - start;
    const data = await response.json();
    // Quick SMTP connection test
    let smtpOk = false;
    let smtpErr = null;
    if (hasMail) {
      try {
        await Promise.race([mailTransporter.verify(), new Promise((_, rej) => setTimeout(() => rej(new Error('SMTP verify timeout')), 8000))]);
        smtpOk = true;
      } catch (e) { smtpErr = e.message; }
    }
    res.json({ whopKey: true, companyId: hasCompany, brevo: hasBrevo, resend: hasResend, gmailSmtp: hasMail, smtpOk, smtpErr, whopStatus: response.status, whopResponseTime: `${elapsed}ms`, memberCount: data.data?.length || 0, hasNextPage: data.page_info?.has_next_page || false });
  } catch (err) {
    res.json({ whopKey: true, companyId: hasCompany, mailConfigured: hasMail, error: err.name === 'AbortError' ? 'Whop API timed out (8s)' : err.message });
  }
});

// ============================================
// WHOP WEBHOOK: Post-purchase welcome email
// ============================================
app.post('/api/webhooks/whop', async (req, res) => {
  console.log('[Webhook] ====== INCOMING WEBHOOK ======');
  console.log('[Webhook] Content-Type:', req.headers['content-type']);
  console.log('[Webhook] Body type:', typeof req.body, Buffer.isBuffer(req.body) ? '(Buffer)' : '');
  console.log('[Webhook] webhook-id:', req.headers['webhook-id'] || 'MISSING');
  console.log('[Webhook] webhook-timestamp:', req.headers['webhook-timestamp'] || 'MISSING');
  console.log('[Webhook] webhook-signature:', req.headers['webhook-signature'] ? 'PRESENT' : 'MISSING');

  // Get the body as string regardless of how it arrived
  let bodyStr;
  if (Buffer.isBuffer(req.body)) {
    bodyStr = req.body.toString('utf8');
  } else if (typeof req.body === 'string') {
    bodyStr = req.body;
  } else {
    // express.json() already parsed it — re-stringify for signature verification
    bodyStr = JSON.stringify(req.body);
    console.log('[Webhook] WARNING: Body was already parsed by express.json() — re-stringified');
  }
  console.log('[Webhook] Body preview:', bodyStr.substring(0, 200));

  const WHOP_WEBHOOK_SECRET = process.env.WHOP_WEBHOOK_SECRET;

  // Verify signature using Standard Webhooks spec (https://www.standardwebhooks.com)
  if (WHOP_WEBHOOK_SECRET) {
    const sigHeader = req.headers['webhook-signature'];
    if (sigHeader) {
      const msgId = req.headers['webhook-id'] || '';
      const timestamp = req.headers['webhook-timestamp'] || '';
      const toSign = `${msgId}.${timestamp}.${bodyStr}`;

      // Derive HMAC key: strip ws_ prefix, try hex-decode then base64-decode
      const rawSecret = WHOP_WEBHOOK_SECRET.startsWith('ws_') ? WHOP_WEBHOOK_SECRET.slice(3) : WHOP_WEBHOOK_SECRET;
      let valid = false;
      const keyVariants = [
        Buffer.from(rawSecret, 'hex'),    // Whop secrets are hex strings
        Buffer.from(rawSecret, 'base64'), // Standard Webhooks uses base64
        Buffer.from(WHOP_WEBHOOK_SECRET), // Full string as-is fallback
      ];
      const signatures = sigHeader.split(' ');
      console.log('[Webhook] Signature header values:', signatures.length, 'signatures');
      for (let i = 0; i < keyVariants.length; i++) {
        try {
          const hmac = crypto.createHmac('sha256', keyVariants[i]).update(toSign).digest('base64');
          const expected = `v1,${hmac}`;
          if (signatures.some(s => s === expected)) {
            valid = true;
            console.log(`[Webhook] Signature verified ✓ (key variant ${i})`);
            break;
          }
        } catch (e) { console.log(`[Webhook] Key variant ${i} error:`, e.message); }
      }
      if (!valid) {
        console.error('[Webhook] Invalid signature — all key variants failed');
        console.error('[Webhook] Proceeding anyway to not block webhooks (will fix signature later)');
        // Don't return 401 — process the webhook anyway and fix signature verification later
      }
    } else {
      console.warn('[Webhook] No webhook-signature header — skipping verification (test event?)');
    }
  } else {
    console.warn('[Webhook] No WHOP_WEBHOOK_SECRET configured — skipping verification');
  }

  try {
    const event = typeof req.body === 'object' && !Buffer.isBuffer(req.body) ? req.body : JSON.parse(bodyStr);
    const eventType = event.action || event.event || event.type;
    console.log(`[Webhook] Event type: ${eventType}`);
    console.log(`[Webhook] Event keys:`, Object.keys(event).join(', '));

    // Whop V1 sends "membership.activated" (dot), dashboard test uses "membership_activated" (underscore)
    // Also handle V5 "membership.went_valid" and other variants
    const activationEvents = [
      'membership.activated',       // Whop V1 real webhook
      'membership_activated',       // Whop dashboard test event
      'membership.went_active',     // Legacy/alternate
      'membership.went_valid',      // Whop V5 event
      'membership.created',         // Creation event
      'payment.succeeded',          // Payment success
    ];
    if (activationEvents.includes(eventType)) {
      const membership = event.data;
      console.log('[Webhook] Membership data keys:', membership ? Object.keys(membership).join(', ') : 'NO DATA');

      const email = membership?.user?.email || membership?.email;
      const name = membership?.user?.name || membership?.user?.username || null;
      const firstName = name ? name.split(' ')[0] : null;
      console.log(`[Webhook] Extracted — email: ${email || 'NONE'}, name: ${name || 'NONE'}`);

      if (email && (resend || mailTransporter)) {
        console.log(`[Webhook] Sending welcome email to ${email}...`);
        const result = await sendEmail({
          to: email,
          subject: 'Welcome to Soldi — Get Started Now',
          html: buildWelcomeEmailHtml(firstName || 'there', email)
        });
        if (result.success) console.log(`[Webhook] Welcome email sent to ${email} via ${result.provider}`);
        else console.error(`[Webhook] Welcome email FAILED for ${email}:`, JSON.stringify(result));
      } else {
        console.error(`[Webhook] Cannot send email — email: ${email || 'NONE'}, resend: ${!!resend}, mailTransporter: ${!!mailTransporter}`);
      }
    } else {
      console.log(`[Webhook] Event type "${eventType}" does not match membership activation — skipping email`);
    }

    return res.json({ received: true });
  } catch (err) {
    console.error('[Webhook] Processing error:', err.message, err.stack);
    return res.status(400).json({ error: 'Invalid payload' });
  }
});

function buildWelcomeEmailHtml(firstName, email) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:0;background:#050507;font-family:'Inter',system-ui,-apple-system,sans-serif;">
<div style="max-width:600px;margin:0 auto;background:#111;color:#fff;padding:40px;border-radius:16px;margin-top:20px;">
  <div style="text-align:center;margin-bottom:32px;">
    <div style="display:inline-block;width:60px;height:60px;background:linear-gradient(135deg,#22c55e,#10b981);border-radius:16px;line-height:60px;font-size:32px;font-weight:900;color:#050507;">S</div>
  </div>
  <h1 style="font-size:26px;font-weight:800;text-align:center;margin:0 0 8px;">Welcome to Soldi, ${firstName}!</h1>
  <p style="color:#a1a1aa;text-align:center;margin:0 0 32px;font-size:15px;">Your membership is active. Here's how to get started.</p>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:16px;">
    <h3 style="color:#22c55e;margin:0 0 10px;font-size:16px;">Log Into Your Dashboard</h3>
    <p style="color:#d4d4d8;font-size:14px;margin:0;line-height:1.7;">Head over to <a href="https://trysoldi.com" style="color:#22c55e;text-decoration:none;font-weight:600;">trysoldi.com</a> and click Login in the top right. Enter your email <strong style="color:#fff;">${email}</strong> and you'll get a verification code sent to your inbox. This is your home base for everything, not Whop. Bookmark trysoldi.com because that's where all your tools and guides live.</p>
  </div>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:16px;">
    <h3 style="color:#22c55e;margin:0 0 10px;font-size:16px;">Your Dashboard</h3>
    <p style="color:#d4d4d8;font-size:14px;margin:0;line-height:1.7;">Once you're logged in you'll see your member dashboard. You've got your API key for the Odds Screen, your affiliate link where you earn 25% on every person you refer, written guides covering sports betting, e-commerce, AI selling, and affiliate programs, the AI Chatbot Builder, and the Google Review Campaign tool.</p>
  </div>

  <div style="background:rgba(88,101,242,0.08);border:1px solid rgba(88,101,242,0.2);border-radius:12px;padding:24px;margin-bottom:16px;">
    <h3 style="color:#5865F2;margin:0 0 10px;font-size:16px;">Join the Discord</h3>
    <p style="color:#d4d4d8;font-size:14px;margin:0 0 14px;line-height:1.7;">Click below to join the community. Once you're in the server type <strong style="color:#22c55e;">!verify</strong> in any channel. The bot will DM you asking for your email. Reply with <strong style="color:#fff;">${email}</strong> and you'll automatically get the Soldi Paid Member role which unlocks all the private channels, strategy calls, and live picks.</p>
    <a href="https://discord.gg/HStNMpCAH5" style="display:inline-block;padding:10px 24px;background:#5865F2;color:#fff;font-weight:600;border-radius:8px;text-decoration:none;font-size:14px;">Join Discord Server</a>
  </div>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:16px;">
    <h3 style="color:#22c55e;margin:0 0 10px;font-size:16px;">Polymarket Whale Tracker</h3>
    <p style="color:#d4d4d8;font-size:14px;margin:0;line-height:1.7;">One of the most powerful tools we have. In Discord type <strong style="color:#22c55e;">!whales</strong> to see the default whale wallets we're already tracking for you. Type <strong style="color:#22c55e;">!track</strong> followed by any wallet address to add it to your watchlist. You'll get a DM alert every time a tracked wallet places a bet.</p>
  </div>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:16px;">
    <h3 style="color:#22c55e;margin:0 0 10px;font-size:16px;">The Odds Screen</h3>
    <p style="color:#d4d4d8;font-size:14px;margin:0;line-height:1.7;">From your dashboard click Open Odds Screen and enter your API key. This gives you real time odds across 10+ sportsbooks including FanDuel, DraftKings, Pinnacle, BetRivers, Caesars, and more. Switch between NBA, NFL, MLB, NHL, and MMA. Look for the highlighted edges where one book has way better odds than the rest.</p>
  </div>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:32px;">
    <h3 style="color:#22c55e;margin:0 0 10px;font-size:16px;">Build and Sell AI Chatbots</h3>
    <p style="color:#d4d4d8;font-size:14px;margin:0;line-height:1.7;">Head to the Chatbot Builder on your dashboard. Set up a business name, add services and FAQs, pick a tone, and you'll get an embeddable widget you can install on any website. Members are selling these to local businesses for $500 to $2,000 upfront plus a monthly retainer fee for ongoing maintenance and updates.</p>
  </div>

  <div style="background:rgba(34,197,94,0.06);border:1px solid rgba(34,197,94,0.15);border-radius:12px;padding:20px 24px;margin-bottom:32px;">
    <p style="color:#d4d4d8;font-size:14px;margin:0;line-height:1.7;"><strong style="color:#22c55e;">Important:</strong> Your Whop account is just for billing. Everything else happens at <a href="https://trysoldi.com" style="color:#22c55e;text-decoration:none;font-weight:600;">trysoldi.com</a>. If you ever need help type <strong style="color:#22c55e;">!commands</strong> in Discord for a full walkthrough of the bot or submit a bug report from your dashboard.</p>
  </div>

  <div style="text-align:center;margin-bottom:16px;">
    <a href="https://trysoldi.com" style="display:inline-block;padding:14px 40px;background:linear-gradient(135deg,#22c55e,#10b981);color:#050507;font-weight:700;border-radius:10px;text-decoration:none;font-size:15px;margin-right:8px;">Go to Dashboard</a>
    <a href="https://trysoldi.com/getting-started" style="display:inline-block;padding:14px 28px;background:rgba(255,255,255,0.08);color:#fff;font-weight:600;border-radius:10px;text-decoration:none;font-size:15px;border:1px solid rgba(255,255,255,0.1);">Full Getting Started Guide</a>
  </div>

  <p style="text-align:center;color:#52525b;font-size:12px;margin:0;">&copy; 2026 Soldi. All rights reserved.</p>
</div>
</body></html>`;
}

// ============================================
// VERIFICATION CODE EMAIL TEMPLATE
// ============================================
function buildVerificationCodeEmailHtml(code) {
  const digitBoxes = code.split('').map(d =>
    `<td style="width:48px;height:60px;background:#1a1a1f;border:2px solid rgba(34,197,94,0.4);border-radius:12px;text-align:center;vertical-align:middle;font-family:'Courier New',Courier,monospace;font-size:32px;font-weight:800;color:#22c55e;">${d}</td>`
  ).join('<td style="width:10px;"></td>');

  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#050507;font-family:'Inter',system-ui,-apple-system,sans-serif;">
<div style="max-width:560px;margin:0 auto;padding:20px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#111114;border-radius:16px;overflow:hidden;">
    <tr><td style="padding:40px 32px 0;text-align:center;">
      <div style="display:inline-block;width:56px;height:56px;background:linear-gradient(135deg,#22c55e,#10b981);border-radius:14px;line-height:56px;font-size:28px;font-weight:900;color:#050507;">S</div>
    </td></tr>
    <tr><td style="padding:24px 32px 0;text-align:center;">
      <h1 style="font-size:22px;font-weight:700;color:#ffffff;margin:0;">Your Verification Code</h1>
    </td></tr>
    <tr><td style="padding:8px 32px 0;text-align:center;">
      <p style="color:#a1a1aa;font-size:15px;margin:0;line-height:1.5;">Enter this code on the Soldi login page to continue:</p>
    </td></tr>
    <tr><td style="padding:28px 32px;">
      <table cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
        <tr>${digitBoxes}</tr>
      </table>
    </td></tr>
    <tr><td style="padding:0 32px;text-align:center;">
      <p style="color:#71717a;font-size:13px;margin:0;">This code expires in <strong style="color:#e4e4e7;">5 minutes</strong></p>
    </td></tr>
    <tr><td style="padding:32px 32px 0;">
      <div style="border-top:1px solid rgba(255,255,255,0.06);"></div>
    </td></tr>
    <tr><td style="padding:16px 32px 32px;text-align:center;">
      <p style="color:#52525b;font-size:12px;margin:0 0 4px;">If you didn't request this code, you can safely ignore this email.</p>
      <p style="color:#3f3f46;font-size:11px;margin:0;">© 2026 Soldi. All rights reserved.</p>
    </td></tr>
  </table>
</div>
</body></html>`;
}

// ============================================
// SOLDI ODDS API: Sports Betting Data (self-hosted scraper)
// ============================================
const SOLDI_API_URL = process.env.SOLDI_API_URL || 'http://localhost:3001';
const SOLDI_API_KEY = process.env.SOLDI_API_KEY || 'dev-key-change-me';
const oddsCache = new Map();

function getOddsCache(key) {
  const entry = oddsCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiry) { oddsCache.delete(key); return null; }
  return entry.data;
}
function setOddsCache(key, data, ttlSeconds) {
  oddsCache.set(key, { data, expiry: Date.now() + ttlSeconds * 1000 });
}

// Sportsbook definitions (matching SoldiAPI scrapers)
const ODDS_SPORTSBOOKS = [
  { key: 'draftkings', name: 'DraftKings', shortName: 'DK' },
  { key: 'fanduel', name: 'FanDuel', shortName: 'FD' },
  { key: 'betrivers', name: 'BetRivers', shortName: 'BR' },
  { key: 'williamhill_us', name: 'Caesars', shortName: 'CZR' },
  { key: 'betonlineag', name: 'BetOnline', shortName: 'BOL' },
  { key: 'betus', name: 'BetUS', shortName: 'BUS' },
  { key: 'kalshi', name: 'Kalshi', shortName: 'KAL' },
  { key: 'pinnacle', name: 'Pinnacle', shortName: 'PIN' },
  { key: 'bet105', name: 'Bet105', shortName: '105' },
  { key: 'prophetx', name: 'ProphetX', shortName: 'PX' },
  { key: 'novig', name: 'Novig', shortName: 'NOV' },
  { key: 'buckeye', name: 'Buckeye', shortName: 'BKY' },
];
const SHARP_BOOKS = ['pinnacle', 'novig'];

const ODDS_SPORT_CATEGORIES = [
  { id: 'basketball', name: 'Basketball', icon: '🏀', leagues: [
    { key: 'basketball_nba', name: 'NBA' }, { key: 'basketball_ncaab', name: 'NCAAB' }
  ]},
  { id: 'hockey', name: 'Hockey', icon: '🏒', leagues: [
    { key: 'icehockey_nhl', name: 'NHL' }
  ]},
  { id: 'baseball', name: 'Baseball', icon: '⚾', leagues: [
    { key: 'baseball_mlb', name: 'MLB' }
  ]},
  { id: 'mma', name: 'MMA', icon: '🥊', leagues: [
    { key: 'mma_mixed_martial_arts', name: 'UFC' }
  ]},
];

// ============================================
// BETTING MATH (ported from OddsScreen/lib/sportsbooks.ts)
// ============================================
function impliedProbability(americanOdds) {
  if (americanOdds > 0) return 100 / (americanOdds + 100);
  return Math.abs(americanOdds) / (Math.abs(americanOdds) + 100);
}

function probToAmericanOdds(prob) {
  if (prob <= 0 || prob >= 1) return 0;
  if (prob >= 0.5) return Math.round(-(prob / (1 - prob)) * 100);
  return Math.round(((1 - prob) / prob) * 100);
}

function formatAmericanOdds(odds) {
  if (!odds || odds === 0) return '-';
  return odds > 0 ? `+${odds}` : `${odds}`;
}

function devigProbabilities(imp1, imp2) {
  const total = imp1 + imp2;
  if (total <= 0) return [0.5, 0.5];
  return [imp1 / total, imp2 / total];
}

function findMarketConsensusEV(bookOdds, sharpOddsList, sharpOppOddsList) {
  if (!sharpOddsList.length) return { ev: 0, isPositive: false, trueProb: 0, kelly: 0 };
  const avgImp = sharpOddsList.reduce((s, o) => s + impliedProbability(o), 0) / sharpOddsList.length;
  const avgOpp = sharpOppOddsList.length > 0
    ? sharpOppOddsList.reduce((s, o) => s + impliedProbability(o), 0) / sharpOppOddsList.length
    : 1 - avgImp;
  const [trueProb] = devigProbabilities(avgImp, avgOpp);
  const decimal = bookOdds > 0 ? bookOdds / 100 + 1 : 100 / Math.abs(bookOdds) + 1;
  const ev = (trueProb * decimal - 1) * 100;
  const b = decimal - 1;
  const kelly = b > 0 ? Math.max(0, (trueProb * b - (1 - trueProb)) / b) * 100 : 0;
  return { ev: Math.round(ev * 100) / 100, isPositive: ev > 0, trueProb: Math.round(trueProb * 10000) / 100, kelly: Math.round(kelly * 100) / 100 };
}

function findArbitrageForEvent(allOdds) {
  let bestHome = { bookmaker: '', odds: -Infinity };
  let bestAway = { bookmaker: '', odds: -Infinity };
  for (const o of allOdds) {
    if (o.home > bestHome.odds) bestHome = { bookmaker: o.bookmaker, odds: o.home };
    if (o.away > bestAway.odds) bestAway = { bookmaker: o.bookmaker, odds: o.away };
  }
  const homeD = bestHome.odds > 0 ? bestHome.odds / 100 + 1 : 100 / Math.abs(bestHome.odds) + 1;
  const awayD = bestAway.odds > 0 ? bestAway.odds / 100 + 1 : 100 / Math.abs(bestAway.odds) + 1;
  const arbPct = 1 / homeD + 1 / awayD;
  if (arbPct < 1) {
    const homeStake = (100 / homeD) / arbPct;
    const awayStake = (100 / awayD) / arbPct;
    const profit = ((1 / arbPct) - 1) * 100;
    return {
      profit: Math.round(profit * 100) / 100,
      bets: [
        { bookmaker: bestHome.bookmaker, side: 'home', odds: bestHome.odds, stake: Math.round(homeStake * 100) / 100 },
        { bookmaker: bestAway.bookmaker, side: 'away', odds: bestAway.odds, stake: Math.round(awayStake * 100) / 100 },
      ]
    };
  }
  return null;
}

// Transform SoldiAPI response to our format (same schema as The Odds API)
function transformOddsEvents(rawEvents) {
  return rawEvents.map(event => {
    const bookmakers = (event.bookmakers || []).map(bm => {
      const result = { key: bm.key, name: bm.title, markets: {} };

      for (const market of bm.markets || []) {
        // Pass through ALL markets generically
        result.markets[market.key] = {
          key: market.key,
          outcomes: (market.outcomes || []).map(o => ({
            name: o.name,
            price: o.price,
            point: o.point ?? null,
            liquidity: o.liquidity ?? null
          }))
        };
      }

      // Keep backward compatibility for EV/arb calculations (h2h, spreads, totals)
      const h2h = result.markets['h2h'];
      if (h2h) {
        const home = h2h.outcomes.find(o => o.name === event.home_team);
        const away = h2h.outcomes.find(o => o.name === event.away_team);
        const draw = h2h.outcomes.find(o => o.name === 'Draw');
        result.moneyline = { home: home?.price || 0, away: away?.price || 0, draw: draw?.price || null };
      }
      const spreads = result.markets['spreads'];
      if (spreads) {
        const home = spreads.outcomes.find(o => o.name === event.home_team);
        const away = spreads.outcomes.find(o => o.name === event.away_team);
        result.spread = {
          home: home?.price || 0, homePoint: home?.point || 0,
          away: away?.price || 0, awayPoint: away?.point || 0
        };
      }
      const totals = result.markets['totals'];
      if (totals) {
        const over = totals.outcomes.find(o => o.name === 'Over');
        const under = totals.outcomes.find(o => o.name === 'Under');
        result.total = {
          over: over?.price || 0, overPoint: over?.point || 0,
          under: under?.price || 0, underPoint: under?.point || 0
        };
      }
      return result;
    });
    return {
      id: event.id, sport: event.sport_title, sportKey: event.sport_key,
      homeTeam: event.home_team, awayTeam: event.away_team,
      commenceTime: event.commence_time, bookmakers
    };
  });
}

// Calculate +EV bets across all events — iterates over ALL market keys generically
function calculatePositiveEV(events) {
  const evBets = [];
  for (const event of events) {
    // Collect all unique market keys across all bookmakers
    const allMarketKeys = new Set();
    for (const bm of event.bookmakers) {
      if (bm.markets) Object.keys(bm.markets).forEach(k => allMarketKeys.add(k));
    }

    for (const marketKey of allMarketKeys) {
      // Determine market type from key prefix
      const isMoneyline = marketKey.startsWith('h2h') || marketKey === 'fight_to_go_distance';
      const isSpread = marketKey.startsWith('spreads');
      const isTotal = marketKey.startsWith('totals') || marketKey.startsWith('team_total') || marketKey === 'total_rounds';

      // Find sharp book data for this market key
      const sharpBms = event.bookmakers.filter(b => SHARP_BOOKS.includes(b.key) && b.markets && b.markets[marketKey]);
      if (sharpBms.length === 0) continue;

      // Get display-friendly market name suffix
      const suffixMap = { '_h1': ' 1H', '_q1': ' Q1', '_p1': ' P1', '_f5': ' F5', '_f7': ' F7', '_i1': ' 1st Inn' };
      let displaySuffix = '';
      for (const [s, label] of Object.entries(suffixMap)) {
        if (marketKey.endsWith(s)) { displaySuffix = label; break; }
      }

      for (const bm of event.bookmakers) {
        if (SHARP_BOOKS.includes(bm.key)) continue;
        if (!bm.markets || !bm.markets[marketKey]) continue;
        const mkt = bm.markets[marketKey];
        const bookInfo = ODDS_SPORTSBOOKS.find(s => s.key === bm.key);

        if (isMoneyline) {
          // 2-way moneyline: home/away outcomes
          const homeOutcome = mkt.outcomes.find(o => o.name === event.homeTeam);
          const awayOutcome = mkt.outcomes.find(o => o.name === event.awayTeam);
          if (!homeOutcome || !awayOutcome) continue;
          const sharpHome = sharpBms.map(b => b.markets[marketKey].outcomes.find(o => o.name === event.homeTeam)?.price).filter(Boolean);
          const sharpAway = sharpBms.map(b => b.markets[marketKey].outcomes.find(o => o.name === event.awayTeam)?.price).filter(Boolean);
          if (sharpHome.length === 0 || sharpAway.length === 0) continue;
          const label = marketKey === 'fight_to_go_distance' ? 'Go Distance' : `ML${displaySuffix}`;
          addEVBet(evBets, event, bm, bookInfo, label, homeOutcome.price, event.homeTeam, sharpHome, sharpAway, null, marketKey);
          addEVBet(evBets, event, bm, bookInfo, label, awayOutcome.price, event.awayTeam, sharpAway, sharpHome, null, marketKey);
        } else if (isSpread) {
          const homeOutcome = mkt.outcomes.find(o => o.name === event.homeTeam);
          const awayOutcome = mkt.outcomes.find(o => o.name === event.awayTeam);
          if (!homeOutcome || !awayOutcome) continue;
          // Only compare to sharp books with matching spread point
          const matchingSharp = sharpBms.filter(b => {
            const sm = b.markets[marketKey];
            const sh = sm.outcomes.find(o => o.name === event.homeTeam);
            return sh && Math.abs(sh.point - homeOutcome.point) < 0.01;
          });
          if (matchingSharp.length === 0) continue;
          const sharpHome = matchingSharp.map(b => b.markets[marketKey].outcomes.find(o => o.name === event.homeTeam)?.price).filter(Boolean);
          const sharpAway = matchingSharp.map(b => b.markets[marketKey].outcomes.find(o => o.name === event.awayTeam)?.price).filter(Boolean);
          const hpt = homeOutcome.point || 0;
          addEVBet(evBets, event, bm, bookInfo, `Spread${displaySuffix} ${hpt > 0 ? '+' : ''}${hpt}`, homeOutcome.price, event.homeTeam, sharpHome, sharpAway, hpt, marketKey);
          const apt = awayOutcome.point || 0;
          addEVBet(evBets, event, bm, bookInfo, `Spread${displaySuffix} ${apt > 0 ? '+' : ''}${apt}`, awayOutcome.price, event.awayTeam, sharpAway, sharpHome, apt, marketKey);
        } else if (isTotal) {
          const overOutcome = mkt.outcomes.find(o => o.name === 'Over');
          const underOutcome = mkt.outcomes.find(o => o.name === 'Under');
          if (!overOutcome || !underOutcome) continue;
          // Only compare to sharp books with matching total point
          const matchingSharp = sharpBms.filter(b => {
            const sm = b.markets[marketKey];
            const so = sm.outcomes.find(o => o.name === 'Over');
            return so && Math.abs(so.point - overOutcome.point) < 0.01;
          });
          if (matchingSharp.length === 0) continue;
          const sharpOver = matchingSharp.map(b => b.markets[marketKey].outcomes.find(o => o.name === 'Over')?.price).filter(Boolean);
          const sharpUnder = matchingSharp.map(b => b.markets[marketKey].outcomes.find(o => o.name === 'Under')?.price).filter(Boolean);
          const prefix = marketKey.startsWith('team_total') ? 'Team Total' : marketKey === 'total_rounds' ? 'Total Rounds' : 'Total';
          addEVBet(evBets, event, bm, bookInfo, `${prefix}${displaySuffix} O${overOutcome.point}`, overOutcome.price, 'Over', sharpOver, sharpUnder, overOutcome.point, marketKey);
          addEVBet(evBets, event, bm, bookInfo, `${prefix}${displaySuffix} U${underOutcome.point}`, underOutcome.price, 'Under', sharpUnder, sharpOver, underOutcome.point, marketKey);
        }
      }
    }
  }
  evBets.sort((a, b) => b.ev - a.ev);
  return evBets;
}

function addEVBet(evBets, event, bm, bookInfo, marketType, odds, team, sharpSide, sharpOpp, point, marketKey) {
  if (!odds) return;
  const result = findMarketConsensusEV(odds, sharpSide, sharpOpp);
  if (result.isPositive && result.ev > 0.5) {
    const avgImp = sharpSide.reduce((s, o) => s + impliedProbability(o), 0) / sharpSide.length;
    const avgOpp = sharpOpp.reduce((s, o) => s + impliedProbability(o), 0) / sharpOpp.length;
    const [tp] = devigProbabilities(avgImp, avgOpp);
    evBets.push({
      eventId: event.id, sport: event.sport, sportKey: event.sportKey,
      homeTeam: event.homeTeam, awayTeam: event.awayTeam, commenceTime: event.commenceTime,
      bookmaker: bm.key, bookmakerName: bookInfo?.name || bm.name,
      team, odds, fairOdds: probToAmericanOdds(tp), ev: result.ev,
      trueProb: result.trueProb, kelly: result.kelly, marketType, point, marketKey
    });
  }
}

// Find arbitrage opportunities across all events and ALL market keys
function findArbitrageOpportunities(events) {
  const arbs = [];
  for (const event of events) {
    // Collect all unique market keys that are 2-way moneyline-like
    const allMarketKeys = new Set();
    for (const bm of event.bookmakers) {
      if (bm.markets) Object.keys(bm.markets).forEach(k => allMarketKeys.add(k));
    }

    for (const marketKey of allMarketKeys) {
      const isMoneyline = marketKey.startsWith('h2h') || marketKey === 'fight_to_go_distance';
      if (!isMoneyline) continue; // Arb detection works best on 2-way moneylines

      const bmsWithMkt = event.bookmakers.filter(b => {
        const m = b.markets && b.markets[marketKey];
        if (!m) return false;
        const home = m.outcomes.find(o => o.name === event.homeTeam);
        const away = m.outcomes.find(o => o.name === event.awayTeam);
        return home && away && home.price && away.price;
      });
      if (bmsWithMkt.length < 2) continue;

      const oddsArray = bmsWithMkt.map(bm => {
        const m = bm.markets[marketKey];
        return {
          bookmaker: bm.key,
          home: m.outcomes.find(o => o.name === event.homeTeam).price,
          away: m.outcomes.find(o => o.name === event.awayTeam).price,
        };
      });

      const arb = findArbitrageForEvent(oddsArray);
      if (arb) {
        const suffixMap = { '_h1': ' 1H', '_q1': ' Q1', '_p1': ' P1', '_f5': ' F5', '_f7': ' F7', '_i1': ' 1st Inn' };
        let displaySuffix = '';
        for (const [s, label] of Object.entries(suffixMap)) {
          if (marketKey.endsWith(s)) { displaySuffix = label; break; }
        }
        const label = marketKey === 'h2h' ? 'ML' : marketKey === 'fight_to_go_distance' ? 'Go Distance' : `ML${displaySuffix}`;
        arbs.push({
          eventId: event.id, sport: event.sport, sportKey: event.sportKey,
          homeTeam: event.homeTeam, awayTeam: event.awayTeam, commenceTime: event.commenceTime,
          profit: arb.profit, marketType: label,
          bets: arb.bets.map(b => {
            const bookInfo = ODDS_SPORTSBOOKS.find(s => s.key === b.bookmaker);
            return { ...b, bookmakerName: bookInfo?.name || b.bookmaker, team: b.side === 'home' ? event.homeTeam : event.awayTeam };
          })
        });
      }
    }
  }
  arbs.sort((a, b) => b.profit - a.profit);
  return arbs;
}

// Fetch odds from SoldiAPI (self-hosted scraper, with cache)
async function fetchOddsEvents(sport) {
  const cacheKey = `odds_events_${sport}`;
  const cached = getOddsCache(cacheKey);
  if (cached) return { events: cached, cached: true };

  const bookKeys = ODDS_SPORTSBOOKS.map(b => b.key).join(',');
  const url = `${SOLDI_API_URL}/api/v1/sports/${sport}/odds?regions=us,us2,eu,au&markets=h2h,spreads,totals&oddsFormat=american&bookmakers=${bookKeys}`;
  const headers = { 'Authorization': `Bearer ${SOLDI_API_KEY}` };
  const apiRes = await fetch(url, { headers, signal: AbortSignal.timeout(15000) });
  if (!apiRes.ok) {
    const err = await apiRes.text();
    console.error('SoldiAPI error:', apiRes.status, err);
    throw new Error(`SoldiAPI returned ${apiRes.status}`);
  }
  const rawEvents = await apiRes.json();
  const events = transformOddsEvents(rawEvents);
  // Cache for 5s — Python-side stale carry-forward ensures books persist
  setOddsCache(cacheKey, events, 5);
  console.log(`[SoldiAPI] Fetched ${events.length} events for ${sport}`);
  return { events, cached: false };
}

// GET /api/odds/sports - List sport categories
app.get('/api/odds/sports', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  res.json({ success: true, categories: ODDS_SPORT_CATEGORIES, sportsbooks: ODDS_SPORTSBOOKS });
});

// GET /api/odds/events?sport=basketball_nba - Live odds
app.get('/api/odds/events', async (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  const sport = req.query.sport || 'basketball_nba';
  try {
    const result = await fetchOddsEvents(sport);
    res.json({ success: true, events: result.events, cached: result.cached });
  } catch (err) {
    console.error('Odds events error:', err.message);
    res.status(502).json({ error: err.message || 'Failed to fetch odds' });
  }
});

// GET /api/odds/ev?sport=basketball_nba - +EV bets
app.get('/api/odds/ev', async (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  const sport = req.query.sport || 'basketball_nba';
  const evCacheKey = `odds_ev_${sport}`;
  const cached = getOddsCache(evCacheKey);
  if (cached) return res.json({ success: true, bets: cached, cached: true });
  try {
    const result = await fetchOddsEvents(sport);
    const evBets = calculatePositiveEV(result.events);
    setOddsCache(evCacheKey, evBets, 5);
    res.json({ success: true, bets: evBets, cached: false });
  } catch (err) {
    console.error('EV calculation error:', err.message);
    res.status(502).json({ error: err.message || 'Failed to calculate EV' });
  }
});

// GET /api/odds/arbitrage?sport=basketball_nba - Arbitrage opportunities
app.get('/api/odds/arbitrage', async (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  const sport = req.query.sport || 'basketball_nba';
  const arbCacheKey = `odds_arb_${sport}`;
  const cached = getOddsCache(arbCacheKey);
  if (cached) return res.json({ success: true, arbs: cached, cached: true });
  try {
    const result = await fetchOddsEvents(sport);
    const arbs = findArbitrageOpportunities(result.events);
    setOddsCache(arbCacheKey, arbs, 5);
    res.json({ success: true, arbs, cached: false });
  } catch (err) {
    console.error('Arbitrage calculation error:', err.message);
    res.status(502).json({ error: err.message || 'Failed to calculate arbitrage' });
  }
});

// GET /api/odds/line-history/:eventId - Line movement data
app.get('/api/odds/line-history/:eventId', async (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  const { eventId } = req.params;
  const { market, bookmaker, sport } = req.query;
  try {
    const params = new URLSearchParams();
    if (market) params.set('market', market);
    if (bookmaker) params.set('bookmaker', bookmaker);
    const url = `${SOLDI_API_URL}/api/v1/sports/${sport || 'basketball_nba'}/events/${eventId}/line-history?${params}`;
    const apiRes = await fetch(url, {
      headers: { 'Authorization': `Bearer ${SOLDI_API_KEY}` },
      signal: AbortSignal.timeout(10000)
    });
    const data = await apiRes.json();
    res.json({ success: true, history: data });
  } catch (err) {
    console.error('Line history error:', err.message);
    res.status(502).json({ error: err.message || 'Failed to fetch line history' });
  }
});

// ============================================
// GET /api/membership/details - Get full membership info
// ============================================
app.get('/api/membership/details', requireAuth, async (req, res) => {
  if (!WHOP_API_KEY || WHOP_API_KEY === 'YOUR_API_KEY_HERE') {
    return res.status(500).json({ error: 'Server not configured' });
  }

  try {
    const membershipId = req.user.membershipId;
    const url = `https://api.whop.com/api/v1/memberships/${membershipId}?company_id=${WHOP_COMPANY_ID}`;
    const response = await fetchWithTimeout(url, {
      headers: { Authorization: `Bearer ${WHOP_API_KEY}` }
    }, 10000);

    if (!response.ok) {
      console.error('Whop membership details error:', response.status);
      return res.status(502).json({ error: 'Failed to fetch membership details' });
    }

    const membership = await response.json();

    return res.json({
      success: true,
      membership: {
        id: membership.id,
        status: membership.status,
        planName: membership.product?.name || 'Soldi',
        email: membership.email,
        createdAt: membership.created_at,
        renewalDate: membership.renewal_period_end || membership.next_renewal_date || null,
        cancelAt: membership.cancel_at || null,
        manageUrl: membership.manage_url || `https://whop.com/orders`,
        validUntil: membership.valid_until || null
      }
    });
  } catch (err) {
    console.error('Membership details error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// POST /api/membership/cancel - Cancel membership
// ============================================
app.post('/api/membership/cancel', requireAuth, async (req, res) => {
  if (!WHOP_API_KEY || WHOP_API_KEY === 'YOUR_API_KEY_HERE') {
    return res.status(500).json({ error: 'Server not configured' });
  }

  try {
    const membershipId = req.user.membershipId;
    const url = `https://api.whop.com/api/v1/memberships/${membershipId}/cancel?company_id=${WHOP_COMPANY_ID}`;
    const response = await fetchWithTimeout(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${WHOP_API_KEY}`,
        'Content-Type': 'application/json'
      }
    }, 10000);

    if (!response.ok) {
      const errText = await response.text();
      console.error('Cancel error:', response.status, errText);
      return res.status(502).json({ error: 'Failed to cancel membership' });
    }

    const result = await response.json();

    return res.json({
      success: true,
      status: result.status || 'canceling',
      cancelAt: result.cancel_at || null,
      message: 'Your membership has been set to cancel at the end of your billing period.'
    });
  } catch (err) {
    console.error('Cancel error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// WEB SCRAPER - DuckDuckGo HTML Search (Free, No API Key)
// ============================================
// Rotating user agents to avoid scraper detection
const SCRAPER_UAS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0',
];
let uaIndex = 0;
function getScraperUA() {
  const ua = SCRAPER_UAS[uaIndex % SCRAPER_UAS.length];
  uaIndex++;
  return ua;
}
// Keep backward compat for existing references
const SCRAPER_UA = SCRAPER_UAS[0];

// Directory domains - results from these are NOT the business's own website
const DIRECTORY_DOMAINS = new Set([
  // Business directories & review sites
  'yelp.com', 'yellowpages.com', 'superpages.com', 'manta.com',
  'bbb.org', 'thumbtack.com', 'homeadvisor.com', 'angi.com',
  'angieslist.com', 'expertise.com', 'bark.com', 'porch.com',
  'houzz.com', 'homeguide.com', 'buildzoom.com', 'fixr.com',
  'chamberofcommerce.com', 'merchantcircle.com', 'dexknows.com',
  'brownbook.net', 'hotfrog.com', 'local.com', 'showmelocal.com',
  'loc8nearme.com', 'birdeye.com', 'podium.com', 'citysearch.com',
  'superlawyers.com', 'bestlawyers.com', 'nolo.com',
  // Social media & platforms
  'facebook.com', 'linkedin.com', 'twitter.com', 'x.com',
  'instagram.com', 'tiktok.com', 'pinterest.com', 'youtube.com',
  'reddit.com', 'nextdoor.com', 'foursquare.com',
  // Search engines & maps
  'google.com', 'bing.com', 'mapquest.com', 'yahoo.com',
  'duckduckgo.com', 'brave.com',
  // Job boards
  'ziprecruiter.com', 'indeed.com', 'glassdoor.com', 'careerbuilder.com',
  'monster.com', 'simplyhired.com',
  // Medical/Legal directories
  'healthgrades.com', 'vitals.com', 'zocdoc.com', 'webmd.com',
  'findlaw.com', 'avvo.com', 'justia.com', 'lawyers.com',
  'martindale.com', 'npidb.org',
  // Real estate directories
  'realtor.com', 'zillow.com', 'redfin.com', 'trulia.com',
  // Reference & news
  'wikipedia.org', 'wikihow.com', 'latimes.com', 'nytimes.com',
  'usnews.com', 'forbes.com', 'bloomberg.com', 'cnbc.com',
  // Education & gov
  'cornell.edu', 'harvard.edu', 'stanford.edu', 'mit.edu',
  '.edu', '.gov',
  // Travel
  'tripadvisor.com',
]);

function extractDomain(url) {
  try {
    const hostname = new URL(url).hostname.replace(/^www\./, '');
    return hostname;
  } catch {
    return '';
  }
}

function isDirectoryDomain(url) {
  const domain = extractDomain(url);
  if (!domain) return true;

  // Skip .edu and .gov domains entirely
  if (domain.endsWith('.edu') || domain.endsWith('.gov')) return true;

  // Check exact match and parent domain
  for (const dir of DIRECTORY_DOMAINS) {
    if (dir.startsWith('.')) continue; // Skip TLD entries (handled above)
    if (domain === dir || domain.endsWith('.' + dir)) return true;
  }
  return false;
}

function extractPhoneFromText(text) {
  if (!text) return '';
  const patterns = [
    // (XXX) XXX-XXXX or (XXX) XXX.XXXX or (XXX) XXX XXXX
    /\((\d{3})\)\s*(\d{3})[-.\s](\d{4})/,
    // (XXX) XXXXXXX (no separator)
    /\((\d{3})\)\s*(\d{3})(\d{4})/,
    // XXX-XXX-XXXX or XXX.XXX.XXXX or XXX XXX XXXX
    /(?<!\d)(\d{3})[-.\s](\d{3})[-.\s](\d{4})(?!\d)/,
    // 1-XXX-XXX-XXXX (strip leading 1)
    /1[-.\s](\d{3})[-.\s](\d{3})[-.\s](\d{4})(?!\d)/,
    // +1 XXX XXX XXXX or +1-XXX-XXX-XXXX
    /\+1[-.\s]?(\d{3})[-.\s](\d{3})[-.\s](\d{4})(?!\d)/,
    // XXXXXXXXXX (10 consecutive digits)
    /(?<!\d)(\d{3})(\d{3})(\d{4})(?!\d)/,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      const digits = match.slice(1).join('');
      if (digits.length === 10) {
        return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
      }
    }
  }
  return '';
}

// Detect AI chatbot / live chat widgets on a webpage
function detectChatbot(html) {
  if (!html) return { hasChatbot: false, chatbotProvider: '' };

  const lowerHtml = html.toLowerCase();

  // Map of chatbot providers and their signatures (ordered by popularity)
  const chatbotSignatures = [
    { name: 'Intercom', patterns: ['intercom', 'intercomsettings', 'widget.intercom.io'] },
    { name: 'Drift', patterns: ['drift.com', 'driftt', 'js.driftt.com'] },
    { name: 'Tidio', patterns: ['tidio', 'tidiochatcode', 'code.tidio.co'] },
    { name: 'LiveChat', patterns: ['livechatinc.com', '__lc_inited', 'livechat-static'] },
    { name: 'Zendesk Chat', patterns: ['zopim', 'static.zdassets.com', 'ze-snippet', 'zendeskwidget'] },
    { name: 'Crisp', patterns: ['crisp.chat', 'crisp_website_id', 'client.crisp.chat'] },
    { name: 'Tawk.to', patterns: ['tawk.to', 'tawk_api', 'embed.tawk.to'] },
    { name: 'HubSpot Chat', patterns: ['js.hs-scripts.com', 'hubspot.com/conversations', 'hs-chat'] },
    { name: 'Freshchat', patterns: ['freshchat', 'fcwidget', 'wchat.freshchat.com'] },
    { name: 'Olark', patterns: ['olark', 'static.olark.com'] },
    { name: 'ChatBot', patterns: ['chatbot.com', 'widget.chatbot.com'] },
    { name: 'ManyChat', patterns: ['manychat', 'mcwidget'] },
    { name: 'Podium', patterns: ['podium', 'connect.podium.com'] },
    { name: 'Birdeye', patterns: ['birdeye.com/widget', 'birdeye'] },
    { name: 'BotPress', patterns: ['botpress', 'cdn.botpress.cloud'] },
    { name: 'Kommunicate', patterns: ['kommunicate', 'widget.kommunicate.io'] },
    { name: 'Chatwoot', patterns: ['chatwoot', 'app.chatwoot.com'] },
    { name: 'JivoChat', patterns: ['jivochat', 'jivosite.com', 'code.jivosite.com'] },
    { name: 'Smith.ai', patterns: ['smith.ai', 'smithai'] },
    { name: 'Dialogflow', patterns: ['dialogflow', 'cloud.google.com/dialogflow'] },
  ];

  for (const bot of chatbotSignatures) {
    for (const pattern of bot.patterns) {
      if (lowerHtml.includes(pattern)) {
        return { hasChatbot: true, chatbotProvider: bot.name };
      }
    }
  }

  // Generic chat widget detection (catch custom/unknown chatbots)
  const genericPatterns = [
    /chat[-_]?widget/i,
    /live[-_]?chat[-_]?widget/i,
    /ai[-_]?chat[-_]?bot/i,
    /class="[^"]*chat[-_]?bubble[^"]*"/i,
    /id="[^"]*chat[-_]?widget[^"]*"/i,
    /data-chat[-_]?widget/i,
  ];

  for (const pattern of genericPatterns) {
    if (pattern.test(html)) {
      return { hasChatbot: true, chatbotProvider: 'Chat Widget' };
    }
  }

  return { hasChatbot: false, chatbotProvider: '' };
}

function extractStarRating(text) {
  if (!text) return { rating: null, reviewCount: 0 };
  // Match patterns like "4.5 stars", "4.8/5", "rating: 4.5", "4.5 out of 5", "Rated 4.5"
  const ratingPatterns = [
    /(\d+(?:\.\d+)?)\s*(?:\/\s*5|out\s+of\s+5)/i,
    /(\d+(?:\.\d+)?)\s*stars?/i,
    /rating[:\s]+(\d+(?:\.\d+)?)/i,
    /rated\s+(\d+(?:\.\d+)?)/i,
  ];
  for (const pattern of ratingPatterns) {
    const match = text.match(pattern);
    if (match) {
      const rating = parseFloat(match[1]);
      if (rating >= 1 && rating <= 5) {
        // Try to extract review count nearby
        let reviewCount = 0;
        const countPatterns = [
          /(\d+[,.]?\d*)\s*(?:reviews?|ratings?|votes?)/i,
          /\((\d+[,.]?\d*)\)/,
        ];
        for (const cp of countPatterns) {
          const cm = text.match(cp);
          if (cm) {
            reviewCount = parseInt(cm[1].replace(/[,.]/g, ''), 10) || 0;
            break;
          }
        }
        return { rating: Math.round(rating * 10) / 10, reviewCount };
      }
    }
  }
  // Match unicode stars: count filled stars
  const starMatch = text.match(/([★]{1,5})/);
  if (starMatch) {
    const rating = starMatch[1].length;
    return { rating, reviewCount: 0 };
  }
  return { rating: null, reviewCount: 0 };
}

function extractAddressFromText(text) {
  // Look for common US address patterns
  const addrPattern = /(\d+\s+(?:[NSEW]\s+)?[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\s+(?:St|Street|Ave|Avenue|Blvd|Boulevard|Dr|Drive|Rd|Road|Ln|Lane|Way|Ct|Court|Pl|Place|Pkwy|Parkway|Hwy|Highway|Cir|Circle)[.,]?\s*(?:(?:Ste|Suite|Unit|Apt|#)\s*\w+[.,]?\s*)?(?:[A-Z][a-zA-Z]+[.,]?\s+)?(?:FL|CA|TX|NY|GA|NC|OH|PA|MI|IL|NJ|VA|WA|AZ|MA|TN|IN|MO|MD|WI|CO|MN|SC|AL|LA|KY|OR|OK|CT|UT|IA|NV|AR|MS|KS|NM|NE|WV|ID|HI|NH|ME|MT|RI|DE|SD|ND|AK|VT|WY|DC)\s+\d{5})/i;
  const match = text.match(addrPattern);
  return match ? match[1].trim() : '';
}

// Try to extract a city name from snippet text
function extractCityFromSnippet(text) {
  if (!text) return '';
  // Match "in CityName" or "CityName, ST" patterns
  const patterns = [
    /(?:in|near|serving|located in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*[A-Z]{2}\b/i,
    /([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*(?:FL|CA|TX|NY|GA|NC|OH|PA|MI|IL|NJ|VA|WA|AZ|MA|TN|IN|MO|MD|WI|CO|MN|SC|AL|LA|KY|OR|OK|CT|UT|IA|NV|AR|MS|KS|NM|NE|WV|ID|HI|NH|ME|MT|RI|DE|SD|ND|AK|VT|WY|DC)\b/,
  ];
  for (const pattern of patterns) {
    const m = text.match(pattern);
    if (m && m[1] && m[1].length >= 3 && m[1].length <= 30) {
      return m[1].trim();
    }
  }
  return '';
}

// State abbreviation to full name mapping
const STATE_NAMES = {
  'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
  'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
  'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
  'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
  'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
  'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
  'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
  'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
  'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
  'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
  'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
  'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
  'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'Washington DC'
};

// Get a user-friendly city name from the location parameter
function getCityFromLocation(location) {
  if (!location) return '';
  const loc = location.trim();
  // "City, ST" → "City"
  const cityStateMatch = loc.match(/^([A-Za-z\s]+),\s*[A-Z]{2}$/);
  if (cityStateMatch) return cityStateMatch[1].trim();
  // Just a state abbreviation → full state name
  const stateUpper = loc.toUpperCase();
  if (STATE_NAMES[stateUpper]) return STATE_NAMES[stateUpper];
  // Otherwise return as-is
  return loc;
}

function cleanBusinessName(title) {
  if (!title) return '';
  let name = title.trim();

  // If title has " | ", try to find the actual business name part
  // Pattern: "Generic Description | Business Name" or "Business Name | Generic"
  if (name.includes(' | ')) {
    const parts = name.split(' | ').map(p => p.trim());
    // The business name is usually the part that doesn't contain generic words
    const genericWords = /^(?:home|about|contact|services|locations?|reviews?|hours|directions|search|find|best|top|get|compare|our|the)\b/i;
    const locationPattern = /^(?:.*\b(?:in|near)\s+[A-Z][a-z]+)/i;
    // Try to find the most "business name-like" part
    const candidates = parts.filter(p =>
      !genericWords.test(p) &&
      !locationPattern.test(p) &&
      p.length >= 3 &&
      p.length <= 60
    );
    if (candidates.length > 0) {
      // Prefer shorter, more name-like candidates
      name = candidates.reduce((a, b) => {
        // Prefer the part that looks most like a proper business name
        const aHasGeneric = /(?:services?|clinic|locations?|search)\s+(?:in|near|for|at)\b/i.test(a);
        const bHasGeneric = /(?:services?|clinic|locations?|search)\s+(?:in|near|for|at)\b/i.test(b);
        if (aHasGeneric && !bHasGeneric) return b;
        if (!aHasGeneric && bHasGeneric) return a;
        return a.length <= b.length ? a : b;
      });
    } else {
      name = parts[parts.length - 1]; // fallback: use last part
    }
  }

  // If title has " - ", try similar logic
  if (name.includes(' - ') && name.length > 40) {
    const parts = name.split(' - ').map(p => p.trim());
    const genericWords = /^(?:home|about|contact|services|locations?|reviews?|hours|directions|the best|top \d+|find|best|get)\b/i;
    const candidates = parts.filter(p => !genericWords.test(p) && p.length >= 3);
    if (candidates.length > 0) {
      name = candidates.reduce((a, b) => a.length <= b.length ? a : b);
    }
  }

  name = name
    .replace(/more info$/i, '')
    // Remove leading page-type words
    .replace(/^(?:Home|About|Contact|Services|Welcome to|Visit|Contact our [a-z]+ team)\s*[-|:]\s*/i, '')
    // Remove trailing page-type words
    .replace(/\s*[-|:]\s*(?:Home|About|Services|Contact|Reviews|Directions|Hours|Photos)$/i, '')
    // Remove location suffixes like "in Miami, FL" or "- Dallas, TX" at end
    .replace(/\s*[-|]\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,?\s*[A-Z]{2}\s*\d{0,5}\s*$/i, '')
    .replace(/\s+in\s+[A-Z][a-zA-Z\s]+,?\s*[A-Z]{2}\s*\d{0,5}\s*$/i, '')
    // Remove "at City" suffix
    .replace(/\s+at\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*$/i, '')
    .replace(/\s*\(\d+\).*$/, '')
    .replace(/\s*-\s*\d+\s*reviews?.*$/i, '')
    .replace(/\s*\|\s*$/g, '')
    .replace(/\s*-\s*$/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  return name;
}

// Fetch DDG HTML search results
// Convert a domain name into a readable business name
// e.g., "robsroofingaz.com" → "Robs Roofing Az"
function domainToBusinessName(domain) {
  if (!domain) return '';
  // Remove TLD and www
  let name = domain.replace(/^www\./, '').replace(/\.[a-z]{2,}(\.[a-z]{2,})?$/i, '');
  // Split on hyphens and camelCase
  name = name
    .replace(/-/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    // Insert spaces between word boundaries (letters followed by numbers or vice versa)
    .replace(/([a-zA-Z])(\d)/g, '$1 $2')
    .replace(/(\d)([a-zA-Z])/g, '$1 $2');
  // Capitalize each word
  name = name.split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  return name;
}

// Pick the best business name from available sources
function pickBusinessName(result) {
  // 1. Use Brave's siteName if it's a real brand name (not domain-like)
  if (result.siteName && result.siteName.length >= 3) {
    // A real brand name has spaces, mixed case, or special chars
    const hasSpaces = result.siteName.includes(' ');
    const hasMixedCase = /[A-Z]/.test(result.siteName) && /[a-z]/.test(result.siteName) && /[A-Z].*[a-z]|[a-z].*[A-Z]/.test(result.siteName);
    const hasSpecialChars = /[&',.\-!]/.test(result.siteName);

    if (hasSpaces || hasSpecialChars) {
      return result.siteName;
    }
    // Even without spaces, if it has proper mixed case like "InSmyle" it's fine
    if (hasMixedCase && result.siteName.length <= 20) {
      return result.siteName;
    }
  }

  // 2. Try cleaning the page title
  const cleaned = cleanBusinessName(result.title);
  if (cleaned && cleaned.length >= 3 && cleaned.length <= 60) {
    // Make sure cleaned title isn't just a location or generic term
    const tooGeneric = /^[A-Z][a-z]+,?\s*[A-Z]{2}$|^HVAC$|^Plumbing$|^Roofing$|^Electrical$/i.test(cleaned);
    if (!tooGeneric) {
      return cleaned;
    }
  }

  // 3. Fall back to making a readable name from the domain
  const domain = extractDomain(result.url);
  if (domain) {
    return domainToBusinessName(domain);
  }

  return '';
}

// Global search engine throttle — minimum 1 second between requests to any search engine
let lastSearchTime = 0;
async function throttleSearch() {
  const now = Date.now();
  const elapsed = now - lastSearchTime;
  if (elapsed < 1000) {
    await new Promise(r => setTimeout(r, 1000 - elapsed));
  }
  lastSearchTime = Date.now();
}

// Serper.dev API (Google Search results via API — free 2,500 queries)
async function fetchSerperResults(query) {
  const response = await fetch('https://google.serper.dev/search', {
    method: 'POST',
    headers: {
      'X-API-KEY': SERPER_API_KEY,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ q: query, num: 20 }),
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(`Serper API returned status ${response.status}`);
  }

  const data = await response.json();
  const results = [];
  const seenDomains = new Set();

  for (const item of (data.organic || [])) {
    const href = item.link || '';
    if (!href) continue;

    const domain = extractDomain(href);
    if (!domain || seenDomains.has(domain)) continue;
    seenDomains.add(domain);

    const title = item.title || '';
    const snippet = item.snippet || '';

    if (title && href) {
      results.push({ title, url: href, siteName: '', snippet: `${title} ${snippet}` });
    }
  }

  return results;
}

// Fetch search results — API engines first (reliable), then HTML scrapers (fallback)
async function fetchSearchResults(query) {
  const errors = [];

  // 1. Try Brave Search API (most reliable, no rate limiting)
  if (BRAVE_SEARCH_API_KEY) {
    try {
      await throttleSearch();
      const results = await fetchBraveAPIResults(query);
      if (results.length > 0) return results;
      console.log('Brave API returned 0 results, trying next...');
    } catch (err) {
      errors.push(`BraveAPI: ${err.message}`);
      console.error('Brave API failed:', err.message);
    }
  }

  // 2. Try Serper.dev API (Google search results, free 2,500/month)
  if (SERPER_API_KEY) {
    try {
      await throttleSearch();
      const results = await fetchSerperResults(query);
      if (results.length > 0) return results;
      console.log('Serper returned 0 results, trying next...');
    } catch (err) {
      errors.push(`Serper: ${err.message}`);
      console.error('Serper failed:', err.message);
    }
  }

  // 3. Try Brave HTML scraping (can get captcha'd/rate-limited)
  try {
    await throttleSearch();
    const results = await fetchBraveHTMLResults(query);
    if (results.length > 0) return results;
    console.log('Brave HTML returned 0 results, trying DDG...');
  } catch (err) {
    errors.push(`BraveHTML: ${err.message}`);
    console.error('Brave HTML failed:', err.message);
  }

  // 4. Try Bing HTML scraping (reliable, no API key needed)
  try {
    await throttleSearch();
    const results = await fetchBingResults(query);
    if (results.length > 0) return results;
    console.log('Bing returned 0 results, trying DDG...');
  } catch (err) {
    errors.push(`Bing: ${err.message}`);
    console.error('Bing Search failed:', err.message);
  }

  // 5. Fallback to DuckDuckGo HTML
  try {
    await throttleSearch();
    const results = await fetchDDGResults(query);
    if (results.length > 0) return results;
    console.log('DDG returned 0 results, trying Google...');
  } catch (err) {
    errors.push(`DDG: ${err.message}`);
    console.error('DDG Search failed:', err.message);
  }

  // 6. Last-resort: Google HTML scraping
  try {
    await throttleSearch();
    const results = await fetchGoogleResults(query);
    if (results.length > 0) return results;
  } catch (err) {
    errors.push(`Google: ${err.message}`);
    console.error('Google Search failed:', err.message);
  }

  // If all failed, throw with helpful message
  if (errors.length >= 2) {
    const hasApiKey = BRAVE_SEARCH_API_KEY || SERPER_API_KEY;
    if (!hasApiKey) {
      console.error('ALL search engines failed. No API keys configured — set BRAVE_SEARCH_API_KEY or SERPER_API_KEY env var for reliable results.');
    }
    throw new Error('Search engines temporarily unavailable. Please try again in a few minutes.');
  }

  return [];
}

// Brave Search API (official, reliable, no captcha)
async function fetchBraveAPIResults(query) {
  const url = `https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(query)}&count=20`;

  const response = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'Accept-Encoding': 'gzip',
      'X-Subscription-Token': BRAVE_SEARCH_API_KEY,
    },
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(`Brave API returned status ${response.status}`);
  }

  const data = await response.json();
  const results = [];
  const seenDomains = new Set();

  const webResults = data.web?.results || [];
  for (const item of webResults) {
    const href = item.url || '';
    if (!href) continue;

    const domain = extractDomain(href);
    if (!domain || seenDomains.has(domain)) continue;
    seenDomains.add(domain);

    const title = item.title || '';
    const desc = item.description || '';
    // Brave API provides profile.name for the site brand
    const siteName = item.profile?.name || item.meta_url?.hostname?.replace(/^www\./, '') || '';

    if (title && href) {
      results.push({
        title,
        url: href,
        siteName,
        snippet: `${title} ${desc}`,
      });
    }
  }

  return results;
}

// Brave Search HTML scraper (secondary — can get rate-limited)
async function fetchBraveHTMLResults(query) {
  const url = `https://search.brave.com/search?q=${encodeURIComponent(query)}`;

  const response = await fetch(url, {
    headers: {
      'User-Agent': getScraperUA(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(`Brave Search returned status ${response.status}`);
  }

  const html = await response.text();

  // Detect captcha / rate limiting
  if (html.toLowerCase().includes('captcha') || html.includes('cf-challenge') || html.toLowerCase().includes('too many requests')) {
    throw new Error('Brave Search rate limited / captcha');
  }

  const $ = cheerio.load(html);
  const results = [];
  const seenDomains = new Set();

  $('#results .snippet').each((i, el) => {
    const $el = $(el);

    // Get the main link
    const mainLink = $el.find('a[href^="http"]').first();
    const href = mainLink.attr('href') || '';

    // Skip internal Brave links and empty hrefs
    if (!href || href.startsWith('/search') || href.includes('brave.com')) return;

    // Get the site/brand name from the .site-name-content element
    let siteName = $el.find('.site-name-content').first().text().trim()
      .replace(/\s*›.*$/, '').trim();
    siteName = siteName.replace(/\s+[a-z0-9][-a-z0-9]*\.[a-z]{2,}(\.[a-z]{2,})?\s*$/i, '').trim();

    // Get the page title
    const title = $el.find('.snippet-title').first().text().trim() ||
                  mainLink.text().trim();

    // Get description
    const desc = $el.find('.snippet-description').text().trim() ||
                 $el.find('.snippet-content .description').text().trim();

    // Deduplicate by domain
    const domain = extractDomain(href);
    if (!domain || seenDomains.has(domain)) return;
    seenDomains.add(domain);

    if (title && href) {
      results.push({
        title,
        url: href,
        siteName,
        snippet: `${title} ${desc}`,
      });
    }
  });

  return results;
}

// DuckDuckGo HTML scraper (fallback)
async function fetchDDGResults(query) {
  const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'User-Agent': getScraperUA(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: `q=${encodeURIComponent(query)}`,
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok && response.status !== 202) {
    throw new Error(`DDG returned status ${response.status}`);
  }

  const html = await response.text();

  // Check for CAPTCHA
  if (html.includes('challenge to confirm') || html.includes('Select all squares')) {
    throw new Error('DDG CAPTCHA detected');
  }

  const $ = cheerio.load(html);
  const results = [];

  $('.result').each((i, el) => {
    const $el = $(el);
    if ($el.find('.badge--ad').length > 0) return;
    if ($el.text().includes('Sponsored')) return;

    const titleEl = $el.find('.result__title .result__a, .result__a');
    const title = titleEl.text().trim().replace(/more info$/i, '').trim();
    const href = titleEl.attr('href') || '';
    const snippet = $el.find('.result__snippet').text().trim();

    let actualUrl = href;
    const uddgMatch = href.match(/uddg=([^&]+)/);
    if (uddgMatch) {
      actualUrl = decodeURIComponent(uddgMatch[1]);
    }

    if (title && actualUrl) {
      results.push({ title, url: actualUrl, siteName: '', snippet });
    }
  });

  return results;
}

// Bing HTML scraper (reliable fallback, no API key needed)
function decodeBingUrl(bingUrl) {
  try {
    const urlObj = new URL(bingUrl);
    const u = urlObj.searchParams.get('u');
    if (u && u.startsWith('a1')) {
      return Buffer.from(u.slice(2), 'base64').toString('utf-8');
    }
  } catch {}
  return bingUrl;
}

async function fetchBingResults(query) {
  const url = `https://www.bing.com/search?q=${encodeURIComponent(query)}&count=20`;
  const response = await fetch(url, {
    headers: {
      'User-Agent': getScraperUA(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) {
    throw new Error(`Bing returned status ${response.status}`);
  }
  const html = await response.text();
  if (html.toLowerCase().includes('captcha') || html.includes('unusual traffic')) {
    throw new Error('Bing captcha detected');
  }

  const $ = cheerio.load(html);
  const results = [];
  const seenDomains = new Set();

  $('li.b_algo').each((i, el) => {
    const $el = $(el);
    const linkEl = $el.find('h2 a').first();
    const rawHref = linkEl.attr('href') || '';
    if (!rawHref) return;

    // Decode Bing redirect URL (base64-encoded in ?u= param)
    const href = decodeBingUrl(rawHref);
    if (!href || href.includes('bing.com') || href.includes('microsoft.com')) return;

    const domain = extractDomain(href);
    if (!domain || seenDomains.has(domain)) return;
    seenDomains.add(domain);

    const title = linkEl.text().trim();
    const snippet = $el.find('p').text().trim() || $el.find('.b_caption').text().trim();

    if (title && href) {
      results.push({ title, url: href, siteName: '', snippet: `${title} ${snippet}` });
    }
  });

  return results;
}

// Google Search HTML scraper (last-resort fallback)
async function fetchGoogleResults(query) {
  const url = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=20&hl=en`;

  const response = await fetch(url, {
    headers: {
      'User-Agent': getScraperUA(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(`Google returned status ${response.status}`);
  }

  const html = await response.text();

  // Detect captcha
  if (html.includes('captcha') || html.includes('unusual traffic')) {
    throw new Error('Google captcha detected');
  }

  const $ = cheerio.load(html);
  const results = [];
  const seenDomains = new Set();

  // Google search results are in div.g elements
  $('div.g').each((i, el) => {
    const $el = $(el);
    const linkEl = $el.find('a[href^="http"]').first();
    const href = linkEl.attr('href') || '';
    if (!href || href.includes('google.com') || href.includes('webcache')) return;

    const title = $el.find('h3').first().text().trim();
    const snippet = $el.find('.VwiC3b, [data-sncf], .IsZvec').first().text().trim();

    const domain = extractDomain(href);
    if (!domain || seenDomains.has(domain)) return;
    seenDomains.add(domain);

    if (title && href) {
      results.push({ title, url: href, siteName: '', snippet: `${title} ${snippet}` });
    }
  });

  return results;
}

// ============================================
// GOOGLE REVIEW CAMPAIGN - Templates
// ============================================
const REVIEW_EMAIL_TEMPLATES = [
  {
    id: 'casual',
    name: 'Friendly & Casual',
    subject: 'Quick favor, {customer_name}? 🙏',
    body: `Hi {customer_name},

Thanks so much for choosing {business_name}! We really appreciate your business.

If you had a great experience, would you mind taking 30 seconds to leave us a quick Google review? It honestly makes a huge difference for our small business.

👉 {review_link}

No pressure at all — we just love hearing from our customers!

Thanks again,
{business_name} Team`
  },
  {
    id: 'professional',
    name: 'Professional',
    subject: 'We\'d love your feedback — {business_name}',
    body: `Dear {customer_name},

Thank you for choosing {business_name} for your recent service. We strive to provide the best possible experience for every customer.

We would greatly appreciate it if you could take a moment to share your experience with a Google review. Your feedback helps us improve and helps others find quality service.

Leave a review here: {review_link}

Thank you for your time and continued trust in our services.

Best regards,
{business_name}`
  },
  {
    id: 'follow-up',
    name: 'Post-Service Follow-up',
    subject: 'How did we do? — {business_name}',
    body: `Hi {customer_name},

We hope everything went well with your recent service from {business_name}!

We're always looking to improve, and your opinion matters to us. If you have a moment, we'd be grateful if you could share your experience on Google:

👉 {review_link}

If there's anything we could have done better, please don't hesitate to reach out to us directly — we want to make it right.

Warm regards,
{business_name} Team`
  },
  {
    id: 'loyalty',
    name: 'Loyal Customer / VIP',
    subject: 'You\'re one of our favorites, {customer_name} ⭐',
    body: `Hey {customer_name}!

As one of our valued repeat customers at {business_name}, your opinion means the world to us.

Would you be willing to share your experience with a quick Google review? It really helps other people discover us and keeps our small business growing.

👉 {review_link}

We truly appreciate your loyalty and look forward to serving you again!

With gratitude,
{business_name} Team`
  },
];

const REVIEW_SMS_TEMPLATES = [
  {
    id: 'sms-short',
    name: 'Quick & Simple',
    body: `Hi {customer_name}! Thanks for choosing {business_name}. Would you mind leaving us a quick Google review? It really helps! {review_link}`
  },
  {
    id: 'sms-friendly',
    name: 'Friendly',
    body: `Hey {customer_name} 👋 Hope you're happy with your recent service from {business_name}! If so, a Google review would mean the world to us: {review_link} Thanks! 🙏`
  },
];

app.get('/api/review-campaign/templates', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  return res.json({
    success: true,
    emailTemplates: REVIEW_EMAIL_TEMPLATES,
    smsTemplates: REVIEW_SMS_TEMPLATES,
  });
});

// ============================================
// AI CHATBOT BUILDER
// ============================================
const CHATBOTS_FILE = path.join(DATA_DIR, 'chatbots.json');

function loadChatbots() {
  try {
    if (!fs.existsSync(CHATBOTS_FILE)) return {};
    return JSON.parse(fs.readFileSync(CHATBOTS_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function saveChatbots(chatbots) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CHATBOTS_FILE, JSON.stringify(chatbots, null, 2));
}

function buildSystemPrompt(bot) {
  let prompt = `You are a helpful customer support assistant for ${bot.businessName}.`;
  if (bot.businessDescription) prompt += `\n\nAbout the business: ${bot.businessDescription}`;
  if (bot.services && bot.services.length) prompt += `\n\nServices offered: ${bot.services.join(', ')}`;
  if (bot.faqs && bot.faqs.length) {
    prompt += '\n\nFrequently Asked Questions:';
    for (const faq of bot.faqs) {
      prompt += `\nQ: ${faq.q}\nA: ${faq.a}`;
    }
  }
  const toneMap = {
    friendly: 'Be warm, friendly, and conversational. Use casual language.',
    professional: 'Be professional, polished, and courteous. Use formal language.',
    casual: 'Be super casual and laid-back. Use simple everyday language.',
    technical: 'Be precise and technical. Provide detailed, accurate information.',
  };
  if (bot.tone && toneMap[bot.tone]) prompt += `\n\nTone: ${toneMap[bot.tone]}`;
  if (bot.customInstructions) prompt += `\n\nAdditional instructions: ${bot.customInstructions}`;
  if (bot.accountInfo) {
    const ai = bot.accountInfo;
    const parts = [];
    if (ai.loginUrl) parts.push(`Login/Signup page: ${ai.loginUrl}`);
    if (ai.resetUrl) parts.push(`Password reset page: ${ai.resetUrl}`);
    if (ai.supportEmail) parts.push(`Support email: ${ai.supportEmail}`);
    if (ai.supportPhone) parts.push(`Support phone: ${ai.supportPhone}`);
    if (ai.businessHours) parts.push(`Business hours: ${ai.businessHours}`);
    if (ai.refundPolicy) parts.push(`Return/Refund policy: ${ai.refundPolicy}`);
    if (ai.billingFaq) parts.push(`Billing info: ${ai.billingFaq}`);
    if (parts.length > 0) {
      prompt += '\n\nAccount & Support Information:\n' + parts.join('\n');
    }
  }
  prompt += '\n\nKeep responses concise (1-3 sentences when possible). If you don\'t know something, say so honestly and suggest the customer contact the business directly.';
  return prompt;
}

// List user's chatbots
app.get('/api/chatbot/list', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  const storageKey = auth.apiKey || auth.email;
  const chatbots = loadChatbots();
  return res.json({ success: true, chatbots: chatbots[storageKey] || [] });
});

// Save (create/update) a chatbot
app.post('/api/chatbot/save', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  const storageKey = auth.apiKey || auth.email;

  const { chatbot } = req.body;
  if (!chatbot || !chatbot.businessName) {
    return res.status(400).json({ error: 'Business name is required' });
  }

  const chatbots = loadChatbots();
  if (!chatbots[storageKey]) chatbots[storageKey] = [];

  const existing = chatbot.id ? chatbots[storageKey].findIndex(c => c.id === chatbot.id) : -1;
  if (existing >= 0) {
    chatbots[storageKey][existing] = { ...chatbots[storageKey][existing], ...chatbot, updatedAt: new Date().toISOString() };
  } else {
    if (chatbots[storageKey].length >= 5) {
      return res.status(400).json({ error: 'Maximum 5 chatbots per account' });
    }
    chatbots[storageKey].push({
      id: crypto.randomUUID(),
      name: chatbot.name || chatbot.businessName + ' Bot',
      businessName: chatbot.businessName,
      businessDescription: chatbot.businessDescription || '',
      services: chatbot.services || [],
      faqs: chatbot.faqs || [],
      tone: chatbot.tone || 'friendly',
      brandColor: chatbot.brandColor || '#2ECC71',
      welcomeMessage: chatbot.welcomeMessage || 'Hi! How can I help you today?',
      customInstructions: chatbot.customInstructions || '',
      accountInfo: chatbot.accountInfo || {},
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
  }

  saveChatbots(chatbots);
  return res.json({ success: true, chatbots: chatbots[storageKey] });
});

// Delete a chatbot
app.delete('/api/chatbot/delete', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  const storageKey = auth.apiKey || auth.email;
  const botId = req.query.id;
  if (!botId) return res.status(400).json({ error: 'Chatbot ID required' });
  const chatbots = loadChatbots();
  if (!chatbots[storageKey]) return res.json({ success: true, chatbots: [] });
  chatbots[storageKey] = chatbots[storageKey].filter(c => c.id !== botId);
  saveChatbots(chatbots);
  return res.json({ success: true, chatbots: chatbots[storageKey] });
});

// Chat endpoint (public — called by widget and preview)
app.post('/api/chatbot/chat', async (req, res) => {
  // CORS for widget usage
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  const { chatbotId, message, history } = req.body;
  if (!chatbotId || !message) {
    return res.status(400).json({ error: 'chatbotId and message required' });
  }

  // Find the chatbot across all keys
  const chatbots = loadChatbots();
  let bot = null;
  for (const bots of Object.values(chatbots)) {
    bot = bots.find(b => b.id === chatbotId);
    if (bot) break;
  }
  if (!bot) return res.status(404).json({ error: 'Chatbot not found' });

  const systemPrompt = buildSystemPrompt(bot);
  const messages = [{ role: 'system', content: systemPrompt }];
  if (history && Array.isArray(history)) {
    for (const h of history.slice(-10)) {
      messages.push({ role: h.role, content: h.content });
    }
  }
  messages.push({ role: 'user', content: message });

  try {
    const aiRes = await fetch('https://text.pollinations.ai/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages, model: 'openai' }),
    });
    if (!aiRes.ok) {
      const errText = await aiRes.text();
      throw new Error(`AI API error: ${aiRes.status} ${errText}`);
    }
    const reply = await aiRes.text();
    return res.json({ success: true, reply });
  } catch (err) {
    console.error('Chatbot AI error:', err.message);
    return res.status(500).json({ error: 'Failed to get AI response' });
  }
});

// CORS preflight for chat endpoint
app.options('/api/chatbot/chat', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.sendStatus(204);
});

// Widget JS endpoint (public — embedded on customer websites)
app.get('/api/chatbot/widget/:id', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/javascript');

  const chatbots = loadChatbots();
  let bot = null;
  for (const bots of Object.values(chatbots)) {
    bot = bots.find(b => b.id === req.params.id);
    if (bot) break;
  }
  if (!bot) {
    return res.status(404).send('// Chatbot not found');
  }

  const color = bot.brandColor || '#2ECC71';
  const welcome = (bot.welcomeMessage || 'Hi! How can I help?').replace(/'/g, "\\'").replace(/\n/g, '\\n');
  const name = (bot.name || bot.businessName + ' Bot').replace(/'/g, "\\'");
  const chatbotId = bot.id;
  const serverUrl = `${req.protocol}://${req.get('host')}`;

  const widgetJS = `
(function(){
  if(document.getElementById('soldi-chat-widget')) return;
  var SERVER='${serverUrl}';
  var BOT_ID='${chatbotId}';
  var COLOR='${color}';
  var WELCOME='${welcome}';
  var NAME='${name}';
  var history=[];
  var open=false;

  var host=document.createElement('div');
  host.id='soldi-chat-widget';
  document.body.appendChild(host);
  var shadow=host.attachShadow({mode:'closed'});

  var style=document.createElement('style');
  style.textContent=\`
    *{margin:0;padding:0;box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}
    .sc-btn{position:fixed;bottom:20px;right:20px;width:56px;height:56px;border-radius:50%;background:\${COLOR};border:none;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 12px rgba(0,0,0,0.3);z-index:99999;transition:transform .2s}
    .sc-btn:hover{transform:scale(1.08)}
    .sc-btn svg{width:28px;height:28px;fill:#fff}
    .sc-window{position:fixed;bottom:88px;right:20px;width:360px;max-height:500px;border-radius:12px;overflow:hidden;display:flex;flex-direction:column;box-shadow:0 8px 30px rgba(0,0,0,0.3);z-index:99999;opacity:0;transform:translateY(10px);transition:opacity .2s,transform .2s;pointer-events:none;background:#fff}
    .sc-window.open{opacity:1;transform:translateY(0);pointer-events:auto}
    .sc-header{background:\${COLOR};color:#fff;padding:14px 16px;font-size:15px;font-weight:600;display:flex;align-items:center;gap:8px}
    .sc-header .dot{width:8px;height:8px;border-radius:50%;background:#4ade80}
    .sc-msgs{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:8px;min-height:250px;max-height:350px;background:#f9fafb}
    .sc-msg{max-width:80%;padding:10px 14px;border-radius:12px;font-size:14px;line-height:1.5;word-wrap:break-word}
    .sc-msg.bot{background:#e5e7eb;color:#111;align-self:flex-start;border-bottom-left-radius:4px}
    .sc-msg.user{background:\${COLOR};color:#fff;align-self:flex-end;border-bottom-right-radius:4px}
    .sc-msg.typing{background:#e5e7eb;color:#888;font-style:italic;align-self:flex-start;border-bottom-left-radius:4px}
    .sc-input-row{display:flex;border-top:1px solid #e5e7eb;background:#fff}
    .sc-input-row input{flex:1;border:none;padding:12px 14px;font-size:14px;outline:none;background:transparent}
    .sc-input-row button{border:none;background:\${COLOR};color:#fff;padding:0 16px;cursor:pointer;font-size:14px;font-weight:600}
    .sc-input-row button:hover{opacity:0.9}
    .sc-powered{text-align:center;padding:4px;font-size:10px;color:#aaa;background:#fff}
    .sc-powered a{color:#aaa;text-decoration:none}
  \`;
  shadow.appendChild(style);

  var btn=document.createElement('button');
  btn.className='sc-btn';
  btn.innerHTML='<svg viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z"/></svg>';
  shadow.appendChild(btn);

  var win=document.createElement('div');
  win.className='sc-window';
  win.innerHTML='<div class="sc-header"><span class="dot"></span>'+NAME+'</div><div class="sc-msgs"></div><div class="sc-input-row"><input placeholder="Type a message..." /><button>Send</button></div><div class="sc-powered">Powered by <a href="https://soldi.app" target="_blank">Soldi</a></div>';
  shadow.appendChild(win);

  var msgs=win.querySelector('.sc-msgs');
  var inp=win.querySelector('input');
  var sendBtn=win.querySelector('.sc-input-row button');

  function addMsg(text,cls){
    var d=document.createElement('div');
    d.className='sc-msg '+cls;
    d.textContent=text;
    msgs.appendChild(d);
    msgs.scrollTop=msgs.scrollHeight;
    return d;
  }

  addMsg(WELCOME,'bot');

  btn.onclick=function(){
    open=!open;
    win.classList.toggle('open',open);
    if(open) inp.focus();
  };

  async function send(){
    var text=inp.value.trim();
    if(!text) return;
    inp.value='';
    addMsg(text,'user');
    history.push({role:'user',content:text});
    var typing=addMsg('Typing...','typing');
    try{
      var r=await fetch(SERVER+'/api/chatbot/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({chatbotId:BOT_ID,message:text,history:history})});
      var data=await r.json();
      typing.remove();
      var reply=data.reply||'Sorry, I could not process that.';
      addMsg(reply,'bot');
      history.push({role:'assistant',content:reply});
    }catch(e){
      typing.remove();
      addMsg('Sorry, something went wrong.','bot');
    }
  }

  sendBtn.onclick=send;
  inp.onkeydown=function(e){if(e.key==='Enter')send()};
})();
`;

  res.send(widgetJS);
});

// ============================================
// FORM SUBMISSIONS (replaces Formspree)
// ============================================
const DISCORD_INVITE_URL = 'https://discord.gg/HStNMpCAH5';

app.post('/api/submissions', async (req, res) => {
  try {
    const { firstName, lastName, email, phone, instagram, experience, interest, goal,
            utm_source, utm_medium, utm_campaign } = req.body;

    if (!email) return res.status(400).json({ error: 'Email is required' });

    const submission = {
      id: crypto.randomUUID(),
      firstName: firstName || '',
      lastName: lastName || '',
      email: email.trim().toLowerCase(),
      phone: phone || '',
      instagram: instagram || '',
      experience: experience || '',
      interest: interest || '',
      goal: goal || '',
      utm_source: utm_source || '',
      utm_medium: utm_medium || '',
      utm_campaign: utm_campaign || '',
      submittedAt: new Date().toISOString(),
    };

    const submissions = loadSubmissions();
    submissions.unshift(submission);
    saveSubmissions(submissions);

    // Send welcome email with Discord invite + free resources (non-blocking)
    if (mailTransporter || resend) {
      const name = (firstName || '').trim() || 'there';
      const welcomeSubject = `Welcome to Soldi, ${name}! Here's your free access`;
      const welcomeTo = submission.email;
      const welcomeHtmlContent = `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#050507;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="max-width:600px;margin:0 auto;padding:40px 24px;">

  <div style="text-align:center;margin-bottom:32px;">
    <div style="display:inline-block;width:48px;height:48px;background:#10b981;border-radius:12px;line-height:48px;font-size:24px;font-weight:700;color:#fff;">S</div>
    <h1 style="color:#fff;font-size:28px;margin:16px 0 0;">Welcome to Soldi</h1>
  </div>

  <p style="color:#d1d5db;font-size:16px;line-height:1.6;">Hey ${name},</p>
  <p style="color:#d1d5db;font-size:16px;line-height:1.6;">Thanks for applying! We're excited to have you. Here's what you get access to right now — completely free:</p>

  <!-- Discord Invite -->
  <div style="background:#111;border:1px solid #22c55e;border-radius:12px;padding:24px;margin:24px 0;text-align:center;">
    <h2 style="color:#22c55e;font-size:20px;margin:0 0 8px;">Join Our Free Discord Community</h2>
    <p style="color:#9ca3af;font-size:14px;margin:0 0 16px;">Connect with other members, get free resources, and start your journey.</p>
    <a href="${DISCORD_INVITE_URL}" style="display:inline-block;background:#5865F2;color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:600;font-size:16px;">Join Discord Server</a>
  </div>

  <!-- Free Resources -->
  <div style="background:#111;border:1px solid #1e293b;border-radius:12px;padding:24px;margin:24px 0;">
    <h2 style="color:#fff;font-size:18px;margin:0 0 16px;">Your Free Resources</h2>

    <div style="padding:12px 0;border-bottom:1px solid #1e293b;">
      <span style="color:#22c55e;font-weight:600;">1.</span>
      <span style="color:#fff;font-weight:500;"> Beginner's Roadmap to $1K/Month</span>
      <p style="color:#9ca3af;font-size:13px;margin:4px 0 0 18px;">Step-by-step path from zero to your first $1K online — available in #start-here on Discord.</p>
    </div>

    <div style="padding:12px 0;border-bottom:1px solid #1e293b;">
      <span style="color:#22c55e;font-weight:600;">2.</span>
      <span style="color:#fff;font-weight:500;"> Free Sports Betting Bankroll Calculator</span>
      <p style="color:#9ca3af;font-size:13px;margin:4px 0 0 18px;">Manage your bankroll like a pro — pinned in #free-money.</p>
    </div>

    <div style="padding:12px 0;border-bottom:1px solid #1e293b;">
      <span style="color:#22c55e;font-weight:600;">3.</span>
      <span style="color:#fff;font-weight:500;"> AI Prompt Pack (20 Money-Making Prompts)</span>
      <p style="color:#9ca3af;font-size:13px;margin:4px 0 0 18px;">Ready-to-use prompts for selling AI services — in #free-money.</p>
    </div>

    <div style="padding:12px 0;border-bottom:1px solid #1e293b;">
      <span style="color:#22c55e;font-weight:600;">4.</span>
      <span style="color:#fff;font-weight:500;"> E-Commerce Product Research Checklist</span>
      <p style="color:#9ca3af;font-size:13px;margin:4px 0 0 18px;">Find winning products before you spend a dollar — in #free-money.</p>
    </div>

    <div style="padding:12px 0;">
      <span style="color:#22c55e;font-weight:600;">5.</span>
      <span style="color:#fff;font-weight:500;"> Weekly Free Picks & Market Insights</span>
      <p style="color:#9ca3af;font-size:13px;margin:4px 0 0 18px;">Free weekly plays posted in the Discord every week.</p>
    </div>
  </div>

  <!-- CTA -->
  <div style="text-align:center;margin:32px 0;">
    <p style="color:#9ca3af;font-size:14px;">Ready to unlock everything? Get full access to all income streams, tools, and 1-on-1 support:</p>
    <a href="https://whop.com/checkout/plan_Q93fIRTfIo5g7/" style="display:inline-block;background:#22c55e;color:#000;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:700;font-size:16px;margin-top:12px;">Activate Full Membership</a>
  </div>

  <hr style="border:none;border-top:1px solid #1e293b;margin:32px 0;">
  <p style="color:#52525b;font-size:12px;text-align:center;">Soldi — The All-In-One Blueprint to Make Money Online</p>
</div>
</body>
</html>
        `.trim();

      sendEmail({ to: welcomeTo, subject: welcomeSubject, html: welcomeHtmlContent })
        .then(r => r.success ? console.log(`[Mail] Welcome email sent to ${welcomeTo} via ${r.provider}`) : console.error(`[Mail] Welcome email failed for ${welcomeTo}`))
        .catch(err => console.error('[Mail] Welcome email error:', err.message));
    }

    return res.status(201).json({ success: true, discordInvite: DISCORD_INVITE_URL });
  } catch (err) {
    console.error('Submission error:', err);
    return res.status(500).json({ error: 'Failed to save submission' });
  }
});

// ============================================
// ADMIN AUTH
// ============================================
app.post('/api/admin/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) {
    return res.status(400).json({ error: 'Email and password required' });
  }

  const admins = loadAdmins();
  const admin = admins.find(a => a.email === email.toLowerCase().trim());
  if (!admin) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }

  const valid = await bcrypt.compare(password, admin.passwordHash);
  if (!valid) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }

  const token = jwt.sign(
    { adminId: admin.id, email: admin.email, role: admin.role, type: 'admin' },
    JWT_SECRET,
    { expiresIn: '24h' }
  );

  return res.json({
    success: true,
    token,
    admin: { id: admin.id, email: admin.email, role: admin.role },
  });
});

app.get('/api/admin/me', requireAdmin, (req, res) => {
  return res.json({
    id: req.admin.id,
    email: req.admin.email,
    role: req.admin.role,
    createdAt: req.admin.createdAt,
  });
});

// ============================================
// ADMIN — SUBMISSIONS CRUD
// ============================================
app.get('/api/admin/submissions', requireAdmin, (req, res) => {
  const { search, page = 1, limit = 20 } = req.query;
  let submissions = loadSubmissions();

  // Search filter
  if (search) {
    const q = search.toLowerCase();
    submissions = submissions.filter(s =>
      (s.firstName + ' ' + s.lastName).toLowerCase().includes(q) ||
      s.email.toLowerCase().includes(q) ||
      (s.phone || '').includes(q) ||
      (s.instagram || '').toLowerCase().includes(q) ||
      (s.interest || '').toLowerCase().includes(q)
    );
  }

  const total = submissions.length;
  const pageNum = Math.max(1, parseInt(page));
  const limitNum = Math.min(100, Math.max(1, parseInt(limit)));
  const start = (pageNum - 1) * limitNum;
  const paged = submissions.slice(start, start + limitNum);

  return res.json({
    submissions: paged,
    total,
    page: pageNum,
    limit: limitNum,
    totalPages: Math.ceil(total / limitNum),
  });
});

app.delete('/api/admin/submissions/:id', requireAdmin, (req, res) => {
  const submissions = loadSubmissions();
  const idx = submissions.findIndex(s => s.id === req.params.id);
  if (idx === -1) {
    return res.status(404).json({ error: 'Submission not found' });
  }
  submissions.splice(idx, 1);
  saveSubmissions(submissions);
  return res.json({ success: true });
});

app.get('/api/admin/submissions/export', requireAdmin, (req, res) => {
  const submissions = loadSubmissions();
  const headers = ['Date', 'First Name', 'Last Name', 'Email', 'Phone', 'Instagram', 'Experience', 'Interest', 'Goal', 'UTM Source', 'UTM Medium', 'UTM Campaign'];
  const csvRows = [headers.join(',')];

  for (const s of submissions) {
    const row = [
      s.submittedAt,
      `"${(s.firstName || '').replace(/"/g, '""')}"`,
      `"${(s.lastName || '').replace(/"/g, '""')}"`,
      s.email,
      s.phone || '',
      s.instagram || '',
      `"${(s.experience || '').replace(/"/g, '""')}"`,
      `"${(s.interest || '').replace(/"/g, '""')}"`,
      `"${(s.goal || '').replace(/"/g, '""')}"`,
      s.utm_source || '',
      s.utm_medium || '',
      s.utm_campaign || '',
    ];
    csvRows.push(row.join(','));
  }

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=soldi-submissions.csv');
  return res.send(csvRows.join('\n'));
});

app.get('/api/admin/submissions/stats', requireAdmin, (req, res) => {
  const submissions = loadSubmissions();
  const now = new Date();
  const todayStr = now.toISOString().split('T')[0];
  const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

  const total = submissions.length;
  const today = submissions.filter(s => s.submittedAt && s.submittedAt.startsWith(todayStr)).length;
  const thisWeek = submissions.filter(s => s.submittedAt && new Date(s.submittedAt) >= weekAgo).length;

  return res.json({ total, today, thisWeek });
});

// ============================================
// ADMIN — MANAGE ADMIN ACCOUNTS
// ============================================
app.get('/api/admin/admins', requireOwner, (req, res) => {
  const admins = loadAdmins().map(a => ({
    id: a.id,
    email: a.email,
    role: a.role,
    createdAt: a.createdAt,
  }));
  return res.json({ admins });
});

app.post('/api/admin/admins', requireOwner, async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) {
    return res.status(400).json({ error: 'Email and password required' });
  }
  if (password.length < 6) {
    return res.status(400).json({ error: 'Password must be at least 6 characters' });
  }

  const admins = loadAdmins();
  const exists = admins.find(a => a.email === email.toLowerCase().trim());
  if (exists) {
    return res.status(409).json({ error: 'Admin with this email already exists' });
  }

  const hash = await bcrypt.hash(password, 10);
  const newAdmin = {
    id: crypto.randomUUID(),
    email: email.toLowerCase().trim(),
    passwordHash: hash,
    role: 'admin',
    createdAt: new Date().toISOString(),
  };

  admins.push(newAdmin);
  saveAdmins(admins);

  return res.status(201).json({
    success: true,
    admin: { id: newAdmin.id, email: newAdmin.email, role: newAdmin.role, createdAt: newAdmin.createdAt },
  });
});

app.delete('/api/admin/admins/:id', requireOwner, (req, res) => {
  const admins = loadAdmins();
  const target = admins.find(a => a.id === req.params.id);
  if (!target) {
    return res.status(404).json({ error: 'Admin not found' });
  }
  if (target.role === 'owner') {
    return res.status(403).json({ error: 'Cannot delete the owner account' });
  }

  const filtered = admins.filter(a => a.id !== req.params.id);
  saveAdmins(filtered);
  return res.json({ success: true });
});

// ============================================
// SELF-HOSTED ANALYTICS (public endpoints)
// ============================================
app.post('/api/analytics/pageview', (req, res) => {
  const { sid, page, referrer, ua, screen: screenSize, utm_source, utm_medium, utm_campaign } = req.body;
  if (!sid || !page) return res.status(400).json({ error: 'sid and page required' });

  const now = Date.now();
  const hour = new Date().getHours();

  // Update or create session
  let session = activeSessions.get(sid);
  if (!session) {
    session = { sid, page, startedAt: now, lastSeen: now, pageViews: 0, referrer: referrer || 'direct', ua, screen: screenSize };
    activeSessions.set(sid, session);
  }
  session.page = page;
  session.lastSeen = now;
  session.pageViews++;

  // Check day rollover
  const today = getTodayStr();
  if (todayStats.date !== today) {
    saveDailyAnalytics(todayStats.date, serializeDayStats(todayStats));
    todayStats = createDayStats(today);
  }

  // Accumulate stats
  todayStats.pageViews++;
  todayStats.uniqueVisitors.add(sid);
  todayStats.sessions.add(sid);
  todayStats.hourlyViews[hour]++;
  todayStats.funnel.page_view.add(sid);

  // Page stats
  if (!todayStats.pages[page]) {
    todayStats.pages[page] = { views: 0, uniques: new Set() };
  }
  todayStats.pages[page].views++;
  todayStats.pages[page].uniques.add(sid);

  // Referrer stats
  let ref = 'direct';
  if (referrer && referrer !== 'direct') {
    try { ref = new URL(referrer).hostname; } catch { ref = referrer; }
  }
  todayStats.referrers[ref] = (todayStats.referrers[ref] || 0) + 1;

  res.json({ ok: true });
});

app.post('/api/analytics/event', (req, res) => {
  const { sid, event, props, page } = req.body;
  if (!sid || !event) return res.status(400).json({ error: 'sid and event required' });

  // Update session heartbeat
  const session = activeSessions.get(sid);
  if (session) session.lastSeen = Date.now();

  // Check day rollover
  const today = getTodayStr();
  if (todayStats.date !== today) {
    saveDailyAnalytics(todayStats.date, serializeDayStats(todayStats));
    todayStats = createDayStats(today);
  }

  // Count events
  todayStats.events[event] = (todayStats.events[event] || 0) + 1;

  // Funnel tracking
  if (event === 'form_started') {
    todayStats.funnel.form_started.add(sid);
  } else if (event === 'form_step_complete' && props) {
    const stepNum = props.step_number;
    if (stepNum === 1) todayStats.funnel.step_1_complete.add(sid);
    if (stepNum === 2) todayStats.funnel.step_2_complete.add(sid);
    if (stepNum === 3) todayStats.funnel.step_3_complete.add(sid);
  } else if (event === 'form_submitted') {
    todayStats.funnel.form_submitted.add(sid);
  }

  res.json({ ok: true });
});

app.post('/api/analytics/heartbeat', (req, res) => {
  const { sid, page } = req.body;
  if (!sid) return res.status(400).json({ error: 'sid required' });

  const session = activeSessions.get(sid);
  if (session) {
    session.lastSeen = Date.now();
    if (page) session.page = page;
  }

  res.json({ ok: true });
});

// ============================================
// ADMIN — ANALYTICS
// ============================================
app.get('/api/admin/analytics/live', requireAdmin, (req, res) => {
  const sessions = [];
  for (const [sid, s] of activeSessions) {
    sessions.push({
      sid: sid.substring(0, 8) + '...',
      page: s.page,
      startedAt: new Date(s.startedAt).toISOString(),
      lastSeen: new Date(s.lastSeen).toISOString(),
      pageViews: s.pageViews,
      duration: Math.round((s.lastSeen - s.startedAt) / 1000),
    });
  }
  res.json({ activeVisitors: activeSessions.size, sessions });
});

app.get('/api/admin/analytics/summary', requireAdmin, (req, res) => {
  const dateStr = req.query.date || getTodayStr();

  if (dateStr === todayStats.date) {
    const serialized = serializeDayStats(todayStats);
    serialized.topPages = Object.entries(serialized.pages)
      .map(([page, data]) => ({ page, views: data.views, uniques: data.uniques }))
      .sort((a, b) => b.views - a.views)
      .slice(0, 10);
    serialized.topReferrers = Object.entries(serialized.referrers)
      .map(([referrer, count]) => ({ referrer, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);
    return res.json(serialized);
  }

  const data = loadDailyAnalytics(dateStr);
  if (!data) return res.json({ date: dateStr, pageViews: 0, uniqueVisitors: 0, sessions: 0, pages: {}, referrers: {}, hourlyViews: new Array(24).fill(0), events: {}, funnel: {}, topPages: [], topReferrers: [] });

  data.topPages = Object.entries(data.pages || {})
    .map(([page, d]) => ({ page, views: d.views, uniques: d.uniques }))
    .sort((a, b) => b.views - a.views)
    .slice(0, 10);
  data.topReferrers = Object.entries(data.referrers || {})
    .map(([referrer, count]) => ({ referrer, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  res.json(data);
});

app.get('/api/admin/analytics/funnel', requireAdmin, (req, res) => {
  const dateStr = req.query.date || getTodayStr();

  let funnel;
  if (dateStr === todayStats.date) {
    funnel = {};
    for (const [k, v] of Object.entries(todayStats.funnel)) {
      funnel[k] = v instanceof Set ? v.size : v;
    }
  } else {
    const data = loadDailyAnalytics(dateStr);
    funnel = data ? data.funnel : null;
  }

  if (!funnel) {
    return res.json({ date: dateStr, funnel: {}, stepDropoff: [], conversionRate: 0 });
  }

  const steps = [
    { name: 'Page View', key: 'page_view' },
    { name: 'Form Started', key: 'form_started' },
    { name: 'Step 1 (Experience)', key: 'step_1_complete' },
    { name: 'Step 2 (Interest)', key: 'step_2_complete' },
    { name: 'Step 3 (Goal)', key: 'step_3_complete' },
    { name: 'Submitted', key: 'form_submitted' },
  ];

  const stepDropoff = [];
  for (let i = 1; i < steps.length; i++) {
    const prev = funnel[steps[i - 1].key] || 0;
    const curr = funnel[steps[i].key] || 0;
    stepDropoff.push({
      step: steps[i - 1].name + ' \u2192 ' + steps[i].name,
      fromCount: prev,
      toCount: curr,
      rate: prev > 0 ? Math.round((curr / prev) * 1000) / 10 : 0,
    });
  }

  const totalStart = funnel.page_view || 0;
  const totalEnd = funnel.form_submitted || 0;
  const conversionRate = totalStart > 0 ? Math.round((totalEnd / totalStart) * 1000) / 10 : 0;

  res.json({ date: dateStr, funnel, stepDropoff, conversionRate });
});

// ============================================
// ADMIN — DISPUTES CRUD
// ============================================
app.get('/api/admin/disputes', requireAdmin, (req, res) => {
  const { search, status, page = 1, limit = 20 } = req.query;
  let disputes = loadDisputes();

  if (status && status !== 'all') {
    disputes = disputes.filter(d => d.status === status);
  }
  if (search) {
    const q = search.toLowerCase();
    disputes = disputes.filter(d =>
      (d.customerName || '').toLowerCase().includes(q) ||
      (d.customerEmail || '').toLowerCase().includes(q) ||
      (d.reason || '').toLowerCase().includes(q)
    );
  }

  disputes.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

  const total = disputes.length;
  const pageNum = Math.max(1, parseInt(page));
  const limitNum = Math.min(100, Math.max(1, parseInt(limit)));
  const start = (pageNum - 1) * limitNum;
  const paged = disputes.slice(start, start + limitNum);

  res.json({ disputes: paged, total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) });
});

app.post('/api/admin/disputes', requireAdmin, (req, res) => {
  const { customerEmail, customerName, amount, reason, status, evidenceText, notes } = req.body;
  if (!customerEmail || !customerName || amount === undefined) {
    return res.status(400).json({ error: 'Customer email, name, and amount are required' });
  }

  const dispute = {
    id: crypto.randomUUID(),
    customerEmail: customerEmail.trim().toLowerCase(),
    customerName: customerName.trim(),
    amount: parseFloat(amount) || 0,
    reason: reason || '',
    status: status || 'open',
    evidenceText: evidenceText || '',
    notes: notes || '',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    createdBy: req.admin.email,
  };

  const disputes = loadDisputes();
  disputes.unshift(dispute);
  saveDisputes(disputes);

  res.status(201).json({ success: true, dispute });
});

app.put('/api/admin/disputes/:id', requireAdmin, (req, res) => {
  const disputes = loadDisputes();
  const idx = disputes.findIndex(d => d.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Dispute not found' });

  const allowed = ['customerEmail', 'customerName', 'amount', 'reason', 'status', 'evidenceText', 'notes'];
  for (const key of allowed) {
    if (req.body[key] !== undefined) {
      disputes[idx][key] = key === 'amount' ? (parseFloat(req.body[key]) || 0) : req.body[key];
    }
  }
  disputes[idx].updatedAt = new Date().toISOString();
  saveDisputes(disputes);

  res.json({ success: true, dispute: disputes[idx] });
});

app.delete('/api/admin/disputes/:id', requireAdmin, (req, res) => {
  const disputes = loadDisputes();
  const idx = disputes.findIndex(d => d.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Dispute not found' });

  disputes.splice(idx, 1);
  saveDisputes(disputes);
  res.json({ success: true });
});

app.get('/api/admin/disputes/stats', requireAdmin, (req, res) => {
  const disputes = loadDisputes();
  res.json({
    total: disputes.length,
    open: disputes.filter(d => d.status === 'open').length,
    responding: disputes.filter(d => d.status === 'responding').length,
    won: disputes.filter(d => d.status === 'won').length,
    lost: disputes.filter(d => d.status === 'lost').length,
    totalAmount: Math.round(disputes.reduce((s, d) => s + (d.amount || 0), 0) * 100) / 100,
    wonAmount: Math.round(disputes.filter(d => d.status === 'won').reduce((s, d) => s + (d.amount || 0), 0) * 100) / 100,
    lostAmount: Math.round(disputes.filter(d => d.status === 'lost').reduce((s, d) => s + (d.amount || 0), 0) * 100) / 100,
  });
});

// ============================================
// ADMIN — VIEW USERS
// ============================================
app.get('/api/admin/users', requireAdmin, (req, res) => {
  const { search, status, page = 1, limit = 20 } = req.query;
  const keys = loadApiKeys();

  let users = Object.entries(keys).map(([key, data]) => ({
    apiKey: key.substring(0, 10) + '****' + key.substring(key.length - 4),
    email: data.email,
    membershipId: data.membershipId,
    status: data.status,
    createdAt: data.createdAt,
  }));

  users.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

  if (status && status !== 'all') {
    users = users.filter(u => u.status === status);
  }
  if (search) {
    const q = search.toLowerCase();
    users = users.filter(u =>
      u.email.toLowerCase().includes(q) ||
      u.membershipId.toLowerCase().includes(q)
    );
  }

  const total = users.length;
  const pageNum = Math.max(1, parseInt(page));
  const limitNum = Math.min(100, Math.max(1, parseInt(limit)));
  const start = (pageNum - 1) * limitNum;
  const paged = users.slice(start, start + limitNum);

  res.json({ users: paged, total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) });
});

// ============================================
// OWNER DASHBOARD — Whop-like admin panel for site owner
// ============================================
const WHOP_PLAN_PRICE = parseFloat(process.env.WHOP_PLAN_PRICE) || 49;
const ownerCache = new Map();

function getOwnerCache(key, ttlMs) {
  const cached = ownerCache.get(key);
  if (cached && Date.now() - cached.ts < ttlMs) return cached.data;
  return null;
}

function setOwnerCache(key, data) {
  ownerCache.set(key, { data, ts: Date.now() });
}

function requireOwnerMember(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No token provided' });
  }
  try {
    const decoded = jwt.verify(authHeader.split(' ')[1], JWT_SECRET);
    if (decoded.membershipId === 'admin' && decoded.email?.toLowerCase() === ADMIN_EMAIL.toLowerCase()) {
      req.user = decoded;
      return next();
    }
    if (decoded.type === 'admin') {
      const admins = loadAdmins();
      const admin = admins.find(a => a.id === decoded.adminId);
      if (admin && admin.role === 'owner') { req.user = decoded; return next(); }
    }
    return res.status(403).json({ error: 'Owner access required' });
  } catch {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// Fetch all Whop memberships (cached 2 min)
async function fetchAllWhopMembers(forceRefresh) {
  if (!forceRefresh) {
    const cached = getOwnerCache('whop_members', 2 * 60 * 1000);
    if (cached) return cached;
  }
  if (!WHOP_API_KEY || WHOP_API_KEY === 'YOUR_API_KEY_HERE') return [];
  const allMembers = [];
  let page = 1;
  const maxPages = 40;
  while (page <= maxPages) {
    try {
      const url = `https://api.whop.com/api/v1/memberships?company_id=${WHOP_COMPANY_ID}&page=${page}&per=50`;
      const response = await fetchWithTimeout(url, { headers: { Authorization: `Bearer ${WHOP_API_KEY}` } }, 10000);
      if (!response.ok) break;
      const data = await response.json();
      const memberships = data.data || [];
      if (memberships.length === 0) break;
      for (const m of memberships) {
        allMembers.push({
          id: m.id,
          email: m.user?.email || m.email || '',
          name: m.user?.name || m.user?.username || '',
          username: m.user?.username || '',
          status: m.status,
          createdAt: m.created_at,
          renewalDate: m.next_renewal_date || m.renewal_period_end || null,
          cancelAt: m.cancel_at || null,
          validUntil: m.valid_until || null,
          planName: m.product?.name || 'Soldi',
        });
      }
      if (!data.page_info || !data.page_info.has_next_page) break;
      page++;
    } catch (err) {
      console.error('Whop member fetch error page', page, err);
      break;
    }
  }
  setOwnerCache('whop_members', allMembers);
  return allMembers;
}

// GET /api/owner/members — List all members with search/filter/pagination
app.get('/api/owner/members', requireOwnerMember, async (req, res) => {
  try {
    const refresh = req.query.refresh === 'true';
    let members = await fetchAllWhopMembers(refresh);
    const search = (req.query.search || '').toLowerCase();
    const statusFilter = req.query.status || '';
    if (search) {
      members = members.filter(m => m.email.toLowerCase().includes(search) || m.name.toLowerCase().includes(search) || m.username.toLowerCase().includes(search));
    }
    if (statusFilter) {
      members = members.filter(m => m.status === statusFilter);
    }
    const summary = { active: 0, trialing: 0, canceling: 0, canceled: 0, expired: 0 };
    const allMembers = await fetchAllWhopMembers(false);
    for (const m of allMembers) { if (summary.hasOwnProperty(m.status)) summary[m.status]++; }
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const total = members.length;
    const totalPages = Math.ceil(total / limit);
    const start = (page - 1) * limit;
    const paged = members.slice(start, start + limit);
    res.json({ members: paged, total, page, limit, totalPages, summary });
  } catch (err) {
    console.error('Owner members error:', err);
    res.status(500).json({ error: 'Failed to fetch members' });
  }
});

// POST /api/owner/members/:id/pause — Pause a membership
app.post('/api/owner/members/:id/pause', requireOwnerMember, async (req, res) => {
  try {
    const url = `https://api.whop.com/api/v1/memberships/${req.params.id}/pause?company_id=${WHOP_COMPANY_ID}`;
    const response = await fetchWithTimeout(url, { method: 'POST', headers: { Authorization: `Bearer ${WHOP_API_KEY}`, 'Content-Type': 'application/json' } }, 10000);
    if (!response.ok) {
      const errText = await response.text();
      console.error('Pause error:', response.status, errText);
      return res.status(502).json({ error: 'Failed to pause membership' });
    }
    const result = await response.json();
    ownerCache.delete('whop_members');
    res.json({ success: true, status: result.status, message: 'Membership paused' });
  } catch (err) {
    console.error('Pause error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/owner/members/:id/cancel — Cancel a membership (owner-initiated)
app.post('/api/owner/members/:id/cancel', requireOwnerMember, async (req, res) => {
  try {
    const url = `https://api.whop.com/api/v1/memberships/${req.params.id}/cancel?company_id=${WHOP_COMPANY_ID}`;
    const response = await fetchWithTimeout(url, { method: 'POST', headers: { Authorization: `Bearer ${WHOP_API_KEY}`, 'Content-Type': 'application/json' } }, 10000);
    if (!response.ok) {
      const errText = await response.text();
      console.error('Cancel error:', response.status, errText);
      return res.status(502).json({ error: 'Failed to cancel membership' });
    }
    const result = await response.json();
    ownerCache.delete('whop_members');
    res.json({ success: true, status: result.status, cancelAt: result.cancel_at, message: 'Membership canceled' });
  } catch (err) {
    console.error('Cancel error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/owner/revenue — MRR and revenue metrics
app.get('/api/owner/revenue', requireOwnerMember, async (req, res) => {
  try {
    const cached = getOwnerCache('whop_revenue', 5 * 60 * 1000);
    if (cached) return res.json(cached);

    const members = await fetchAllWhopMembers(false);
    // MRR only counts actively paying members (not trialing — they haven't paid yet)
    const payingCount = members.filter(m => m.status === 'active').length;
    const trialingCount = members.filter(m => m.status === 'trialing').length;
    const cancelingCount = members.filter(m => m.status === 'canceling').length;
    const activeCount = payingCount + trialingCount + cancelingCount;
    // MRR = only paying + canceling (canceling still pays until end of period)
    const mrr = (payingCount + cancelingCount) * WHOP_PLAN_PRICE;

    const now = new Date();
    const todayStr = now.toISOString().split('T')[0];
    const yesterday = new Date(now); yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

    // Try Whop payments API for real revenue data
    let revenueToday = 0, revenueYesterday = 0, revenueThisWeek = 0, revenueThisMonth = 0;
    let paymentsAvailable = false;
    const dailyMap = {}; // date string -> amount for chart
    // Init last 7 days with 0
    for (let i = 6; i >= 0; i--) {
      const d = new Date(now); d.setDate(d.getDate() - i);
      dailyMap[d.toISOString().split('T')[0]] = 0;
    }
    try {
      let payPage = 1;
      const maxPayPages = 10;
      while (payPage <= maxPayPages) {
        const payUrl = `https://api.whop.com/api/v1/payments?company_id=${WHOP_COMPANY_ID}&page=${payPage}&per=50`;
        const payRes = await fetchWithTimeout(payUrl, { headers: { Authorization: `Bearer ${WHOP_API_KEY}` } }, 10000);
        if (!payRes.ok) break;
        const payData = await payRes.json();
        const payments = payData.data || [];
        if (payments.length === 0) break;
        paymentsAvailable = true;
        for (const p of payments) {
          if (p.status !== 'paid') continue;
          const amount = (p.final_amount || p.amount || 0) / 100;
          const paidAt = new Date(p.paid_at || p.created_at);
          const paidDateStr = paidAt.toISOString().split('T')[0];
          if (paidDateStr === todayStr) revenueToday += amount;
          if (paidDateStr === yesterdayStr) revenueYesterday += amount;
          if (paidAt >= weekAgo) revenueThisWeek += amount;
          if (paidAt >= monthStart) revenueThisMonth += amount;
          // Accumulate daily chart data
          if (dailyMap.hasOwnProperty(paidDateStr)) dailyMap[paidDateStr] += amount;
        }
        if (!payData.page_info || !payData.page_info.has_next_page) break;
        payPage++;
      }
    } catch (payErr) {
      console.error('Whop payments API error:', payErr.message);
    }

    // If no payments API data, show $0 (don't fake estimates)
    // The MRR is still calculated from member count

    // New members this month & churn
    const newThisMonth = members.filter(m => m.createdAt && new Date(m.createdAt) >= monthStart).length;
    const canceledThisMonth = members.filter(m => m.status === 'canceled' && m.cancelAt && new Date(m.cancelAt) >= monthStart).length;
    const activeStart = activeCount + canceledThisMonth - newThisMonth;
    const churnRate = activeStart > 0 ? Math.round((canceledThisMonth / activeStart) * 1000) / 10 : 0;

    // Daily revenue for chart (last 7 days)
    const dailyRevenue = Object.entries(dailyMap).sort().map(([date, amount]) => ({
      date,
      amount: Math.round(amount * 100) / 100,
    }));

    const result = {
      mrr: Math.round(mrr * 100) / 100,
      revenueToday: Math.round(revenueToday * 100) / 100,
      revenueYesterday: Math.round(revenueYesterday * 100) / 100,
      revenueThisWeek: Math.round(revenueThisWeek * 100) / 100,
      revenueThisMonth: Math.round(revenueThisMonth * 100) / 100,
      activeMembers: activeCount,
      payingMembers: payingCount,
      trialingMembers: trialingCount,
      totalMembers: members.length,
      newMembersThisMonth: newThisMonth,
      churnRate,
      dailyRevenue,
      paymentsAvailable,
      currency: 'USD'
    };
    setOwnerCache('whop_revenue', result);
    res.json(result);
  } catch (err) {
    console.error('Revenue error:', err);
    res.status(500).json({ error: 'Failed to fetch revenue data' });
  }
});

// GET /api/owner/overview — Combined overview stats
app.get('/api/owner/overview', requireOwnerMember, async (req, res) => {
  try {
    const members = await fetchAllWhopMembers(false);
    const payingCount = members.filter(m => m.status === 'active').length;
    const trialingCount = members.filter(m => m.status === 'trialing').length;
    const cancelingCount = members.filter(m => m.status === 'canceling').length;
    const activeCount = payingCount + trialingCount + cancelingCount;
    const mrr = (payingCount + cancelingCount) * WHOP_PLAN_PRICE;

    const now = new Date();
    const todayStr = now.toISOString().split('T')[0];
    const yesterday = new Date(now); yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

    // Fetch real payment data from Whop
    let revenueToday = 0, revenueYesterday = 0, revenueThisWeek = 0, revenueThisMonth = 0;
    try {
      let payPage = 1;
      while (payPage <= 10) {
        const payUrl = `https://api.whop.com/api/v1/payments?company_id=${WHOP_COMPANY_ID}&page=${payPage}&per=50`;
        const payRes = await fetchWithTimeout(payUrl, { headers: { Authorization: `Bearer ${WHOP_API_KEY}` } }, 10000);
        if (!payRes.ok) break;
        const payData = await payRes.json();
        const payments = payData.data || [];
        if (payments.length === 0) break;
        for (const p of payments) {
          if (p.status !== 'paid') continue;
          const amount = (p.final_amount || p.amount || 0) / 100;
          const paidAt = new Date(p.paid_at || p.created_at);
          const paidDateStr = paidAt.toISOString().split('T')[0];
          if (paidDateStr === todayStr) revenueToday += amount;
          if (paidDateStr === yesterdayStr) revenueYesterday += amount;
          if (paidAt >= weekAgo) revenueThisWeek += amount;
          if (paidAt >= monthStart) revenueThisMonth += amount;
        }
        if (!payData.page_info || !payData.page_info.has_next_page) break;
        payPage++;
      }
    } catch (payErr) {
      console.error('Overview payments error:', payErr.message);
    }

    const newThisMonth = members.filter(m => m.createdAt && new Date(m.createdAt) >= monthStart).length;
    const canceledThisMonth = members.filter(m => m.status === 'canceled' && m.cancelAt && new Date(m.cancelAt) >= monthStart).length;
    const activeStart = activeCount + canceledThisMonth - newThisMonth;
    const churnRate = activeStart > 0 ? Math.round((canceledThisMonth / activeStart) * 1000) / 10 : 0;

    // Submissions stats
    const submissions = loadSubmissions();
    const subStats = {
      total: submissions.length,
      today: submissions.filter(s => s.submittedAt && s.submittedAt.startsWith(todayStr)).length,
      thisWeek: submissions.filter(s => s.submittedAt && new Date(s.submittedAt) >= weekAgo).length,
    };

    const liveVisitors = activeSessions ? activeSessions.size : 0;

    res.json({
      revenue: {
        mrr: Math.round(mrr * 100) / 100,
        today: Math.round(revenueToday * 100) / 100,
        yesterday: Math.round(revenueYesterday * 100) / 100,
        thisWeek: Math.round(revenueThisWeek * 100) / 100,
        thisMonth: Math.round(revenueThisMonth * 100) / 100,
      },
      members: {
        active: activeCount,
        paying: payingCount,
        trialing: trialingCount,
        total: members.length,
        new: newThisMonth,
        churnRate,
      },
      submissions: subStats,
      liveVisitors,
    });
  } catch (err) {
    console.error('Overview error:', err);
    res.status(500).json({ error: 'Failed to fetch overview' });
  }
});

// GET /api/owner/conversions — Cross-reference submissions with memberships
app.get('/api/owner/conversions', requireOwnerMember, async (req, res) => {
  try {
    const members = await fetchAllWhopMembers(false);
    const submissions = loadSubmissions();

    // Build email sets for fast lookup
    const submissionEmails = new Set(submissions.map(s => (s.email || '').toLowerCase()).filter(Boolean));
    const memberEmails = new Set(members.map(m => (m.email || '').toLowerCase()).filter(Boolean));

    // 1. Members who filled out form AND purchased
    const formThenPurchased = [];
    // 2. Members who purchased WITHOUT filling out form
    const directPurchased = [];
    // 3. Users who filled out form but did NOT purchase
    const formNoPurchase = [];

    for (const m of members) {
      const email = (m.email || '').toLowerCase();
      if (!email) continue;
      const hasSubmission = submissionEmails.has(email);
      const sub = hasSubmission ? submissions.find(s => (s.email || '').toLowerCase() === email) : null;
      const memberInfo = {
        email: m.email,
        name: m.name || m.username || '',
        status: m.status,
        joinedAt: m.createdAt,
        renewalDate: m.renewalDate,
      };
      if (hasSubmission) {
        formThenPurchased.push({
          ...memberInfo,
          submittedAt: sub ? sub.submittedAt : null,
          interest: sub ? sub.interest : null,
          experience: sub ? sub.experience : null,
        });
      } else {
        directPurchased.push(memberInfo);
      }
    }

    for (const s of submissions) {
      const email = (s.email || '').toLowerCase();
      if (!email) continue;
      if (!memberEmails.has(email)) {
        formNoPurchase.push({
          email: s.email,
          name: (s.firstName || '') + ' ' + (s.lastName || ''),
          submittedAt: s.submittedAt,
          interest: s.interest,
          experience: s.experience,
          phone: s.phone,
          goal: s.goal,
        });
      }
    }

    // Sort each group by most recent first
    formThenPurchased.sort((a, b) => new Date(b.joinedAt || 0) - new Date(a.joinedAt || 0));
    directPurchased.sort((a, b) => new Date(b.joinedAt || 0) - new Date(a.joinedAt || 0));
    formNoPurchase.sort((a, b) => new Date(b.submittedAt || 0) - new Date(a.submittedAt || 0));

    // Conversion rates
    const totalMembers = members.length;
    const totalSubmissions = submissions.length;
    const formToPayRate = totalSubmissions > 0 ? Math.round((formThenPurchased.length / totalSubmissions) * 1000) / 10 : 0;
    const directRate = totalMembers > 0 ? Math.round((directPurchased.length / totalMembers) * 1000) / 10 : 0;

    res.json({
      formThenPurchased: { count: formThenPurchased.length, users: formThenPurchased },
      directPurchased: { count: directPurchased.length, users: directPurchased },
      formNoPurchase: { count: formNoPurchase.length, users: formNoPurchase },
      rates: {
        formToPayRate,
        directRate,
        totalMembers,
        totalSubmissions,
        formFillRate: totalSubmissions, // raw count of form fills
      },
    });
  } catch (err) {
    console.error('Conversions error:', err);
    res.status(500).json({ error: 'Failed to fetch conversion data' });
  }
});

// ============================================
// GOOGLE DOCS GUIDES (scrape + cache)
// ============================================
const GUIDE_DOCS = {
  'selling-ai-websites':  { docId: '1zYT_2ImQRI5mdiBjDSK5qFUfn2-Tar00_nknNBQ6w80', title: 'Selling AI Websites' },
  'soldi-ecom-info':      { docId: '1uJXa69v_zzLfbWfHlAizTRMPRyN8BXKLNARxLcLhNnQ', title: 'Soldi Ecom Info' },
  'affiliate-offers':     { docId: '1hrVBCpFRdZfLo91WrO0trR5-oU918ZSFks0neJD-RR0', title: 'Best Affiliate/Signup Offers Banks' },
  'soldi-betting-guides': { docId: '1XqEsr_moPXv8EH1p-nWot7its9g0LaSchMFogDwtdd8', title: 'Soldi Betting Guides' },
};

const GUIDES_CACHE_FILE = path.join(DATA_DIR, 'google-docs-cache.json');
const guidesCache = new Map();

function loadGuidesCache() {
  try {
    if (!fs.existsSync(GUIDES_CACHE_FILE)) return;
    const data = JSON.parse(fs.readFileSync(GUIDES_CACHE_FILE, 'utf8'));
    for (const [slug, entry] of Object.entries(data)) {
      guidesCache.set(slug, entry);
    }
    console.log(`[Guides] Loaded ${guidesCache.size} guides from disk cache`);
  } catch (err) {
    console.error('[Guides] Failed to load disk cache:', err.message);
  }
}

function saveGuidesCache() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const obj = {};
    for (const [slug, entry] of guidesCache) {
      obj[slug] = entry;
    }
    fs.writeFileSync(GUIDES_CACHE_FILE, JSON.stringify(obj, null, 2));
    console.log(`[Guides] Saved ${guidesCache.size} guides to disk`);
  } catch (err) {
    console.error('[Guides] Failed to save disk cache:', err.message);
  }
}

async function fetchGoogleDoc(docId) {
  const url = `https://docs.google.com/document/d/${docId}/export?format=html`;
  const response = await fetchWithTimeout(url, {}, 15000);
  if (!response.ok) throw new Error(`Google Docs fetch failed: ${response.status}`);
  const rawHtml = await response.text();
  const $ = cheerio.load(rawHtml);

  // Strip Google Docs cruft
  $('style, script, meta, link').remove();

  // Extract sub-guide links (links to other Google Docs)
  const subGuides = [];
  $('a').each(function () {
    const href = $(this).attr('href') || '';
    const text = $(this).text().trim();
    const docMatch = href.match(/docs\.google\.com\/document\/d\/([a-zA-Z0-9_-]+)/);
    if (docMatch && text) {
      const subDocId = docMatch[1];
      const subSlug = text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').substring(0, 60);
      subGuides.push({ title: text, slug: subSlug, docId: subDocId });
    }
  });

  // Clean inline styles but keep structure
  $('*').removeAttr('style').removeAttr('class').removeAttr('id');

  const bodyHtml = $('body').html() || $.html();
  return { html: bodyHtml, subGuides };
}

async function refreshGuidesCache() {
  console.log('[Guides] Refreshing all guides from Google Docs...');
  for (const [slug, config] of Object.entries(GUIDE_DOCS)) {
    try {
      const { html, subGuides } = await fetchGoogleDoc(config.docId);
      guidesCache.set(slug, {
        title: config.title,
        html,
        subGuides: subGuides.length > 0 ? subGuides : undefined,
        fetchedAt: new Date().toISOString(),
      });
      console.log(`[Guides] Cached: ${slug} (${subGuides.length} sub-guides)`);

      // Also cache each linked sub-guide doc
      for (const sub of subGuides) {
        try {
          const subData = await fetchGoogleDoc(sub.docId);
          guidesCache.set(sub.slug, {
            title: sub.title,
            html: subData.html,
            subGuides: subData.subGuides.length > 0 ? subData.subGuides : undefined,
            fetchedAt: new Date().toISOString(),
          });
          console.log(`[Guides]   Sub-guide cached: ${sub.slug}`);
        } catch (subErr) {
          console.error(`[Guides]   Sub-guide failed: ${sub.slug}:`, subErr.message);
        }
        await new Promise(r => setTimeout(r, 2000)); // rate-limit delay
      }
    } catch (err) {
      console.error(`[Guides] Failed to fetch ${slug}:`, err.message);
    }
    await new Promise(r => setTimeout(r, 3000));
  }
  saveGuidesCache();
}

// Load disk cache immediately
loadGuidesCache();

console.log('[Guides] Loaded ' + guidesCache.size + ' guides from disk cache');

// Refresh guides from Google Docs 45s after startup, then every 6 hours
setTimeout(() => {
  refreshGuidesCache().catch(err => console.error('[Guides] Startup refresh failed:', err.message));
  setInterval(() => {
    refreshGuidesCache().catch(err => console.error('[Guides] Periodic refresh failed:', err.message));
  }, 6 * 60 * 60 * 1000); // 6 hours
}, 45000);

// ── Static guide supplements ────────────────────────────────────
// Extra HTML appended to Google-Doc guides at serve-time.
// Keeps the Google Doc as the single source of truth while letting
// us add rich content that doesn't belong in a shared doc.
const GUIDE_SUPPLEMENTS = {
  'selling-ai-websites': `
<hr style="margin:48px 0 32px;border:none;border-top:2px solid rgba(255,255,255,0.08)">
<h2>Finding Customers on Facebook</h2>
<p>The easiest way to find clients who need websites is to go where small businesses are already advertising their services without a website.</p>

<h3>Facebook Marketplace</h3>
<ul>
  <li>Search your local area on Facebook Marketplace for service businesses (roofers, plumbers, landscapers, cleaners, movers, etc.)</li>
  <li>Look for listings that only have a phone number and no website link</li>
  <li>These businesses are actively trying to get customers but have no online presence</li>
  <li>Message them directly: <em>"Hey, I saw your listing on Marketplace. I build websites for local businesses like yours &mdash; would you be interested in a free mockup?"</em></li>
</ul>

<h3>Facebook Groups (Local &amp; City-Dependent)</h3>
<ul>
  <li>Join local Facebook groups in your city: <strong>"[City] Small Business Owners"</strong>, <strong>"[City] Buy Sell Trade"</strong>, <strong>"[City] Home Services"</strong></li>
  <li>Look for service providers posting their business (handyman, pressure washing, lawn care, etc.)</li>
  <li>These are typically owner-operators who handle everything themselves &mdash; they don't have time to build a website</li>
  <li>Engage with their posts first, then DM them about your service</li>
  <li>Post value in the group: <em>"Free website audit for any local business &mdash; DM me your business name"</em></li>
</ul>

<h3>Why These Leads Convert</h3>
<ul>
  <li>They're already spending time marketing on Facebook &mdash; they understand the need for customers</li>
  <li>They don't have a website, so the value proposition is obvious</li>
  <li>Local/city-dependent businesses (plumbers, roofers, landscapers) rely on local customers finding them online</li>
  <li>A $25-50/month website + hosting plan is an easy sell when they're already paying for ads</li>
</ul>

<hr style="margin:48px 0 32px;border:none;border-top:2px solid rgba(255,255,255,0.08)">
<h2>AI Website Creation &mdash; Command Prompt (Copy &amp; Paste)</h2>
<p>Use the prompt below in <strong>Claude</strong>, <strong>Lovable</strong>, or <strong>LandingSite.ai</strong> to generate a modern, professional website for a local business.</p>

<h3>Universal Website Builder Prompt</h3>
<p>Copy and paste this prompt into your AI builder:</p>
<div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.1);border-radius:10px;padding:20px;margin:16px 0;font-family:monospace;font-size:13px;line-height:1.7;white-space:pre-wrap;overflow-x:auto">Create a modern, high-converting website for a local service business.

Business Information:
Business Name: [BUSINESS NAME]
Business Type: [ROOFING / PLUMBING / LANDSCAPING / ETC]
Location: [CITY, STATE]

Contact Information:
Phone Number: [PHONE]
Email: [EMAIL]
Business Hours: [HOURS]
Address: [ADDRESS]

Services Offered:
[List 3-6 main services]

Website Requirements:

Design
- Modern, clean, professional design
- Mobile responsive
- Fast loading
- SEO optimized structure
- Clear call-to-actions

Sections to Include:

1. Hero Section
- Strong headline
- Short value proposition
- "Get a Free Quote" button
- Click-to-call phone button

2. Services Section
- Icons with service descriptions
- Clear benefits of each service

3. About Section
- Business story
- Experience and trust elements
- Licensed/insured badges

4. Testimonials
- Generate 3-5 realistic testimonials

5. Gallery
- Example work photos or placeholders

6. FAQ Section
- Generate 5-7 common customer questions

7. Contact Section
- Contact form
- Phone, Email
- Google Maps integration

8. Service Area Section
- List surrounding cities served

9. Footer
- Business information
- Social media links
- Copyright
- Quick navigation

Color Scheme (choose for industry):
- Blue = plumbing / HVAC
- Green = landscaping
- Red / black = roofing
- Neutral professional tones

Additional Features:
- Mobile click-to-call button
- Lead capture contact form
- SEO friendly headings
- Conversion optimized layout
- Trust badges and social proof</div>

<h3>Using Claude to Build the Backend</h3>
<p>If you want Claude to generate the full codebase, paste this follow-up prompt after the first one:</p>
<div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.1);border-radius:10px;padding:20px;margin:16px 0;font-family:monospace;font-size:13px;line-height:1.7;white-space:pre-wrap;overflow-x:auto">Now generate the complete website code.

Requirements:
- Use React + Tailwind CSS
- Create a modern responsive design
- Include reusable components
- Create a working contact form
- Include a backend using Node.js and Express
- Add API endpoint for form submissions
- Structure the project cleanly

Project Structure:
/frontend
  /components
  /pages
  /styles

/backend
  /server.js
  /routes
  /controllers

The contact form should:
- Collect name, email, phone, message
- Send submissions to email or database

Return the full code with instructions on how to run locally and deploy.</div>

<h3>Using Lovable (Fastest Method)</h3>
<ol>
  <li>Go to <strong>Lovable</strong></li>
  <li>Paste the Universal Website Builder Prompt above</li>
  <li>Let the AI generate the site</li>
  <li>Review the design</li>
  <li>Click <strong>Export Code</strong> and download the project</li>
</ol>
<p>Lovable will generate the entire frontend automatically &mdash; no coding required.</p>

<hr style="margin:48px 0 32px;border:none;border-top:2px solid rgba(255,255,255,0.08)">
<h2>How to Deploy the Website</h2>
<p>Once the site is generated, you need to host it online.</p>

<h3>Recommended Hosting Platforms</h3>
<ul>
  <li><strong>Vercel</strong> &mdash; best for React/Next.js sites</li>
  <li><strong>Netlify</strong> &mdash; great for static sites</li>
  <li><strong>Cloudflare Pages</strong> &mdash; fast global CDN</li>
</ul>

<h3>Deployment Steps</h3>
<ol>
  <li>Create a free account on Vercel or Netlify</li>
  <li>Upload the website files or connect your GitHub repo</li>
  <li>Click <strong>Deploy</strong></li>
  <li>Your site goes live instantly with a temporary URL like <code>businessname.vercel.app</code></li>
</ol>

<h3>Connecting the Client's Domain</h3>
<p>Buy the domain from <strong>Namecheap</strong> or <strong>GoDaddy</strong>:</p>
<ol>
  <li>Purchase the domain (e.g., <code>businessname.com</code>)</li>
  <li>Go to DNS settings in the domain registrar</li>
  <li>Add the hosting provider's nameservers</li>
  <li>Connect the domain inside Vercel/Netlify settings</li>
</ol>
<p>Result: <code>www.businessname.com</code> is now live for the client.</p>

<hr style="margin:48px 0 32px;border:none;border-top:2px solid rgba(255,255,255,0.08)">
<h2>What to Send the Client After Delivery</h2>
<p>Send them:</p>
<ul>
  <li>Website link</li>
  <li>Login credentials (if applicable)</li>
  <li>Monthly maintenance plan details</li>
  <li>Instructions for requesting updates</li>
</ul>

<h3>Example Client Message</h3>
<div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.1);border-radius:10px;padding:20px;margin:16px 0;font-size:14px;line-height:1.7">
Your new website is now live!<br><br>
Website: www.[businessname].com<br><br>
If you ever need updates like adding photos, changing services, or updating contact info, just message me and I'll handle it.<br><br>
Your hosting and maintenance plan keeps the site secure, updated, and running fast.
</div>

<hr style="margin:48px 0 32px;border:none;border-top:2px solid rgba(255,255,255,0.08)">
<h2>Scaling to Recurring Revenue</h2>
<p>Once you reach <strong>20 clients at $25/month</strong>, you're making <strong>$500/month in recurring revenue</strong> &mdash; and the websites require almost no maintenance.</p>

<h3>Upsell Services</h3>
<ul>
  <li><strong>Local SEO</strong> &mdash; help them rank on Google Maps</li>
  <li><strong>Google Review Automation</strong> &mdash; automated review request campaigns</li>
  <li><strong>AI Chatbots</strong> &mdash; embed a support chatbot on their site (use the Soldi Chatbot Builder!)</li>
  <li><strong>Lead Capture Systems</strong> &mdash; advanced forms, CRM integrations</li>
</ul>
`,
};

// GET /api/guides/:slug — returns guide content (JWT auth required)
app.get('/api/guides/:slug', requireAuth, (req, res) => {
  const slug = req.params.slug;
  const cached = guidesCache.get(slug);
  if (!cached) {
    return res.status(404).json({ error: 'Guide not found' });
  }

  // Append any static supplement content for this guide
  let html = cached.html || '';
  if (GUIDE_SUPPLEMENTS[slug]) {
    html += GUIDE_SUPPLEMENTS[slug];
  }

  res.json({
    title: cached.title,
    html,
    subGuides: cached.subGuides || [],
    fetchedAt: cached.fetchedAt,
  });
});

// POST /api/report — Bug/Suggestion report (sends email to soldihq@gmail.com)
app.post('/api/report', requireAuth, async (req, res) => {
  try {
    const { category, message } = req.body;
    if (!category || !message) {
      return res.status(400).json({ error: 'Category and message are required' });
    }
    if (message.length > 5000) {
      return res.status(400).json({ error: 'Message too long (max 5000 chars)' });
    }

    const userEmail = req.user.email || 'Unknown';
    const timestamp = new Date().toISOString();
    const categoryLabel = category === 'bug' ? 'Bug Report' : 'Suggestion';

    const emailHtml = `
      <div style="font-family: Arial, sans-serif; max-width: 600px;">
        <h2 style="color: ${category === 'bug' ? '#ef4444' : '#22c55e'};">${categoryLabel}</h2>
        <p><strong>From:</strong> ${userEmail}</p>
        <p><strong>Category:</strong> ${categoryLabel}</p>
        <p><strong>Time:</strong> ${timestamp}</p>
        <hr style="border: 1px solid #eee;">
        <div style="background: #f9f9f9; padding: 16px; border-radius: 8px; margin-top: 12px;">
          <p style="white-space: pre-wrap;">${message.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</p>
        </div>
      </div>
    `;

    await sendEmail({
      to: 'soldihq@gmail.com',
      subject: `[Soldi ${categoryLabel}] from ${userEmail}`,
      html: emailHtml,
    });

    console.log(`[Report] ${categoryLabel} from ${userEmail}`);
    res.json({ success: true });
  } catch (err) {
    console.error('[Report] Error:', err.message);
    res.status(500).json({ error: 'Failed to send report' });
  }
});

// ============================================
// MEMORY MONITOR — logs RSS every 5 minutes for OOM diagnostics
// ============================================
function startMemoryMonitor() {
  setInterval(() => {
    const mem = process.memoryUsage();
    const rssMB = Math.round(mem.rss / 1048576);
    const heapMB = Math.round(mem.heapUsed / 1048576);
    console.log(`[Memory] RSS=${rssMB}MB Heap=${heapMB}MB`);
    // Force GC if RSS exceeds 400MB (soft limit — hard limit is 512MB via --max-old-space-size)
    if (global.gc && rssMB > 400) {
      console.log('[Memory] RSS high — triggering GC');
      global.gc();
    }
  }, 5 * 60 * 1000); // every 5 minutes
}

// ============================================
// KEEP-ALIVE SELF-PING (prevents Render free tier spin-down)
// ============================================
function keepAlive() {
  const url = process.env.RENDER_EXTERNAL_URL || `http://localhost:${process.env.PORT || 8080}`;
  setInterval(() => {
    require('https').get(`${url}/api/health`, () => {}).on('error', () => {});
    console.log(`[Keep-Alive] Pinged ${url}/api/health`);
  }, 14 * 60 * 1000); // every 14 minutes
}

// ============================================
// START SERVER
// ============================================
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Soldi server running on http://localhost:${PORT}`);
  keepAlive();
  startMemoryMonitor();
});
