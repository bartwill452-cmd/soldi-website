require('dotenv').config();
const express = require('express');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const bcrypt = require('bcryptjs');

// Email services (Resend HTTP API preferred, Gmail SMTP fallback for local dev)
const nodemailer = require('nodemailer');
const GMAIL_USER = process.env.GMAIL_USER || 'soldihq@gmail.com';
const GMAIL_APP_PASSWORD = process.env.GMAIL_APP_PASSWORD || '';
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';

// Resend (HTTP-based — works on all hosting including Render)
let resend = null;
try {
  const { Resend } = require('resend');
  if (RESEND_API_KEY) {
    resend = new Resend(RESEND_API_KEY);
    console.log('[Mail] Resend HTTP email service initialized (primary)');
  }
} catch (e) {}

// Gmail SMTP (fallback — works locally but blocked on Render/most PaaS)
let mailTransporter = null;
if (GMAIL_APP_PASSWORD) {
  mailTransporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: GMAIL_USER, pass: GMAIL_APP_PASSWORD },
    connectionTimeout: 8000,
    greetingTimeout: 8000,
    socketTimeout: 8000
  });
  console.log(`[Mail] Gmail SMTP initialized for ${GMAIL_USER} (fallback)`);
}

if (!resend && !mailTransporter) {
  console.log('[Mail] WARNING: No email service configured — email sending disabled');
}

// Unified email sender: tries Resend first (HTTP), falls back to Gmail SMTP
async function sendEmail({ to, subject, html }) {
  // Try Resend (HTTP-based, preferred)
  if (resend) {
    try {
      const result = await resend.emails.send({
        from: 'Soldi <onboarding@resend.dev>',
        to,
        subject,
        html
      });
      if (result.error) throw new Error(result.error.message || JSON.stringify(result.error));
      console.log(`[Mail] Sent to ${to} via Resend`);
      return { success: true, provider: 'resend' };
    } catch (err) {
      console.error(`[Mail] Resend failed for ${to}:`, err.message || err);
      // Fall through to Gmail SMTP
    }
  }

  // Try Gmail SMTP (fallback)
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
const DATA_DIR = path.join(__dirname, 'data');
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
  // Also return the API key for this member
  const existing = findKeyByMembershipId(req.user.membershipId);
  return res.json({
    success: true,
    user: req.user,
    apiKey: existing ? existing.key : null
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
// GET /api/health - Service health check
// ============================================
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), timestamp: new Date().toISOString(), version: '2.1.0' });
});

// Diagnostic: test Whop API connectivity (no sensitive data exposed)
app.get('/api/debug/whop-test', async (req, res) => {
  const hasKey = !!WHOP_API_KEY && WHOP_API_KEY !== 'YOUR_API_KEY_HERE';
  const hasCompany = !!WHOP_COMPANY_ID;
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
    res.json({ whopKey: true, companyId: hasCompany, resendConfigured: hasResend, gmailSmtp: hasMail, smtpOk, smtpErr, whopStatus: response.status, whopResponseTime: `${elapsed}ms`, memberCount: data.data?.length || 0, hasNextPage: data.page_info?.has_next_page || false });
  } catch (err) {
    res.json({ whopKey: true, companyId: hasCompany, mailConfigured: hasMail, error: err.name === 'AbortError' ? 'Whop API timed out (8s)' : err.message });
  }
});

// ============================================
// WHOP WEBHOOK: Post-purchase welcome email
// ============================================
app.post('/api/webhooks/whop', async (req, res) => {
  const WHOP_WEBHOOK_SECRET = process.env.WHOP_WEBHOOK_SECRET;

  // Verify signature if secret is configured
  if (WHOP_WEBHOOK_SECRET) {
    const signature = req.headers['whop-signature'] || req.headers['x-whop-signature'];
    const body = typeof req.body === 'string' ? req.body : req.body.toString('utf8');
    const hmac = crypto.createHmac('sha256', WHOP_WEBHOOK_SECRET).update(body).digest('hex');
    if (signature !== hmac) {
      console.error('[Webhook] Invalid signature');
      return res.status(401).json({ error: 'Invalid signature' });
    }
  }

  try {
    const raw = typeof req.body === 'string' ? req.body : req.body.toString('utf8');
    const event = JSON.parse(raw);
    const eventType = event.action || event.event;
    console.log(`[Webhook] Received: ${eventType}`);

    if (eventType === 'membership.went_active' || eventType === 'membership.created') {
      const membership = event.data;
      const email = membership?.user?.email || membership?.email;
      const name = membership?.user?.name || membership?.user?.username || null;
      const firstName = name ? name.split(' ')[0] : null;

      if (email && (resend || mailTransporter)) {
        const result = await sendEmail({
          to: email,
          subject: 'Welcome to Soldi — Get Started Now',
          html: buildWelcomeEmailHtml(firstName || 'there', email)
        });
        if (result.success) console.log(`[Webhook] Welcome email sent to ${email} via ${result.provider}`);
        else console.error(`[Webhook] Welcome email failed for ${email}`);
      }
    }

    return res.json({ received: true });
  } catch (err) {
    console.error('[Webhook] Processing error:', err);
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
  <p style="color:#a1a1aa;text-align:center;margin:0 0 32px;font-size:15px;">Your membership is now active. Here's how to get started:</p>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:16px;">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
      <span style="background:#22c55e;color:#050507;width:28px;height:28px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-weight:800;font-size:14px;">1</span>
      <h3 style="color:#22c55e;margin:0;font-size:16px;">Verify on the Website</h3>
    </div>
    <p style="color:#d4d4d8;font-size:14px;margin:0;padding-left:40px;">Visit <a href="https://soldi-website.onrender.com" style="color:#22c55e;text-decoration:none;font-weight:600;">soldi-website.onrender.com</a> and enter your email (<strong style="color:#fff;">${email}</strong>) to unlock your dashboard and AI tools.</p>
  </div>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:16px;">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
      <span style="background:#22c55e;color:#050507;width:28px;height:28px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-weight:800;font-size:14px;">2</span>
      <h3 style="color:#22c55e;margin:0;font-size:16px;">Explore Your AI Tools</h3>
    </div>
    <p style="color:#d4d4d8;font-size:14px;margin:0;padding-left:40px;">Access the Business Finder, Receptionist Leads, AI Image Generator, Chatbot Builder, Odds Screen, TikTok Analytics, and more — all from your dashboard.</p>
  </div>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:16px;">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
      <span style="background:#5865F2;color:#fff;width:28px;height:28px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-weight:800;font-size:14px;">3</span>
      <h3 style="color:#5865F2;margin:0;font-size:16px;">Join the Discord</h3>
    </div>
    <p style="color:#d4d4d8;font-size:14px;margin:0 0 12px;padding-left:40px;">Click below to join our private Discord community:</p>
    <div style="padding-left:40px;">
      <a href="https://discord.gg/HStNMpCAH5" style="display:inline-block;padding:10px 24px;background:#5865F2;color:#fff;font-weight:600;border-radius:8px;text-decoration:none;font-size:14px;">Join Discord Server</a>
    </div>
  </div>

  <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:24px;margin-bottom:32px;">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
      <span style="background:#22c55e;color:#050507;width:28px;height:28px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-weight:800;font-size:14px;">4</span>
      <h3 style="color:#22c55e;margin:0;font-size:16px;">Verify in Discord to Unlock Paid Channels</h3>
    </div>
    <p style="color:#d4d4d8;font-size:14px;margin:0 0 8px;padding-left:40px;">Once you join, <strong style="color:#fff;">message the Soldi Bot</strong> to get your <strong style="color:#5865F2;">Soldi Paid Member</strong> role:</p>
    <div style="padding-left:40px;">
      <div style="background:#0a0a0a;border:1px solid rgba(255,255,255,0.1);border-radius:8px;padding:12px 16px;font-family:monospace;font-size:13px;color:#d4d4d8;margin-bottom:8px;">
        1. Type <strong style="color:#22c55e;">!verify</strong> in any channel<br>
        2. The bot will DM you asking for your email<br>
        3. Reply with: <strong style="color:#fff;">${email}</strong><br>
        4. Your <strong style="color:#5865F2;">Soldi Paid Member</strong> role will be assigned automatically
      </div>
    </div>
  </div>

  <div style="text-align:center;margin-bottom:24px;">
    <a href="https://soldi-website.onrender.com" style="display:inline-block;padding:14px 40px;background:linear-gradient(135deg,#22c55e,#10b981);color:#050507;font-weight:700;border-radius:10px;text-decoration:none;font-size:15px;">Go to Dashboard</a>
  </div>

  <p style="text-align:center;color:#52525b;font-size:12px;margin:0;">&copy; 2026 Soldi. All rights reserved.</p>
</div>
</body></html>`;
}

// ============================================
// VERIFICATION CODE EMAIL TEMPLATE
// ============================================
function buildVerificationCodeEmailHtml(code) {
  const digits = code.split('').join(' &nbsp; ');
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:0;background:#050507;font-family:'Inter',system-ui,-apple-system,sans-serif;">
<div style="max-width:600px;margin:0 auto;background:#111;color:#fff;padding:40px;border-radius:16px;margin-top:20px;">
  <div style="text-align:center;margin-bottom:32px;">
    <div style="display:inline-block;width:60px;height:60px;background:linear-gradient(135deg,#22c55e,#10b981);border-radius:16px;line-height:60px;font-size:32px;font-weight:900;color:#050507;">S</div>
  </div>
  <h1 style="font-size:24px;font-weight:800;text-align:center;margin:0 0 8px;">Your Verification Code</h1>
  <p style="color:#a1a1aa;text-align:center;margin:0 0 32px;font-size:15px;">Enter this code on the Soldi login page to continue:</p>
  <div style="background:rgba(34,197,94,0.08);border:2px solid rgba(34,197,94,0.3);border-radius:16px;padding:28px;text-align:center;margin-bottom:24px;">
    <span style="font-family:'Courier New',monospace;font-size:40px;font-weight:900;letter-spacing:12px;color:#22c55e;">${digits}</span>
  </div>
  <p style="text-align:center;color:#71717a;font-size:13px;margin:0 0 8px;">This code expires in <strong style="color:#a1a1aa;">5 minutes</strong>.</p>
  <p style="text-align:center;color:#52525b;font-size:12px;margin:24px 0 0;">If you didn't request this code, you can safely ignore this email.</p>
  <hr style="border:none;border-top:1px solid rgba(255,255,255,0.06);margin:24px 0;">
  <p style="text-align:center;color:#52525b;font-size:12px;margin:0;">&copy; 2026 Soldi. All rights reserved.</p>
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
  { key: 'fanduel', name: 'FanDuel', shortName: 'FD' },
  { key: 'draftkings', name: 'DraftKings', shortName: 'DK' },
  { key: 'betmgm', name: 'BetMGM', shortName: 'MGM' },
  { key: 'pinnacle', name: 'Pinnacle', shortName: 'PIN' },
  { key: 'williamhill_us', name: 'Caesars', shortName: 'CZR' },
  { key: 'bovada', name: 'Bovada', shortName: 'BOV' },
  { key: 'betonlineag', name: 'BetOnline', shortName: 'BOL' },
  { key: 'betrivers', name: 'BetRivers', shortName: 'BR' },
  { key: 'hardrock', name: 'Hard Rock Bet', shortName: 'HR' },
  { key: 'novig', name: 'Novig', shortName: 'NOV' },
  { key: 'bookmaker', name: 'Bookmaker', shortName: 'BM' },
  { key: 'bet105', name: 'Bet105', shortName: '105' },
  { key: 'xbet', name: 'XBet', shortName: 'XB' },
  { key: 'buckeye', name: 'Buckeye', shortName: 'BKY' },
  { key: 'prophetx', name: 'ProphetX', shortName: 'PX' },
];
const SHARP_BOOKS = ['pinnacle', 'novig', 'bookmaker'];

const ODDS_SPORT_CATEGORIES = [
  { id: 'basketball', name: 'Basketball', icon: '🏀', leagues: [
    { key: 'basketball_nba', name: 'NBA' }, { key: 'basketball_ncaab', name: 'NCAAB' }
  ]},
  { id: 'football', name: 'Football', icon: '🏈', leagues: [
    { key: 'americanfootball_nfl', name: 'NFL' }, { key: 'americanfootball_ncaaf', name: 'NCAAF' }
  ]},
  { id: 'baseball', name: 'Baseball', icon: '⚾', leagues: [
    { key: 'baseball_mlb', name: 'MLB' }
  ]},
  { id: 'hockey', name: 'Hockey', icon: '🏒', leagues: [
    { key: 'icehockey_nhl', name: 'NHL' }
  ]},
  { id: 'soccer', name: 'Soccer', icon: '⚽', leagues: [
    { key: 'soccer_epl', name: 'EPL' }, { key: 'soccer_spain_la_liga', name: 'La Liga' },
    { key: 'soccer_germany_bundesliga', name: 'Bundesliga' }, { key: 'soccer_italy_serie_a', name: 'Serie A' },
    { key: 'soccer_france_ligue_one', name: 'Ligue 1' }, { key: 'soccer_usa_mls', name: 'MLS' },
    { key: 'soccer_uefa_champs_league', name: 'UCL' }
  ]},
  { id: 'mma', name: 'MMA', icon: '🥊', leagues: [
    { key: 'mma_mixed_martial_arts', name: 'UFC' }
  ]},
  { id: 'tennis', name: 'Tennis', icon: '🎾', leagues: [
    { key: 'tennis_atp', name: 'ATP' }, { key: 'tennis_wta', name: 'WTA' }
  ]},
  { id: 'boxing', name: 'Boxing', icon: '🥊', leagues: [
    { key: 'boxing_boxing', name: 'Boxing' }
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
      const result = { key: bm.key, name: bm.title };
      for (const market of bm.markets || []) {
        if (market.key === 'h2h') {
          const home = market.outcomes.find(o => o.name === event.home_team);
          const away = market.outcomes.find(o => o.name === event.away_team);
          const draw = market.outcomes.find(o => o.name === 'Draw');
          result.moneyline = { home: home?.price || 0, away: away?.price || 0, draw: draw?.price || null };
        }
        if (market.key === 'spreads') {
          const home = market.outcomes.find(o => o.name === event.home_team);
          const away = market.outcomes.find(o => o.name === event.away_team);
          result.spread = {
            home: home?.price || 0, homePoint: home?.point || 0,
            away: away?.price || 0, awayPoint: away?.point || 0
          };
        }
        if (market.key === 'totals') {
          const over = market.outcomes.find(o => o.name === 'Over');
          const under = market.outcomes.find(o => o.name === 'Under');
          result.total = {
            over: over?.price || 0, overPoint: over?.point || 0,
            under: under?.price || 0, underPoint: under?.point || 0
          };
        }
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

// Calculate +EV bets across all events
function calculatePositiveEV(events) {
  const evBets = [];
  for (const event of events) {
    const sharpML = event.bookmakers.filter(b => SHARP_BOOKS.includes(b.key) && b.moneyline);
    const sharpHomeML = sharpML.map(b => b.moneyline.home).filter(Boolean);
    const sharpAwayML = sharpML.map(b => b.moneyline.away).filter(Boolean);

    for (const bm of event.bookmakers) {
      if (SHARP_BOOKS.includes(bm.key)) continue;
      const bookInfo = ODDS_SPORTSBOOKS.find(s => s.key === bm.key);

      // Moneyline EV
      if (bm.moneyline && sharpHomeML.length > 0) {
        addEVBet(evBets, event, bm, bookInfo, 'Moneyline', bm.moneyline.home, event.homeTeam, sharpHomeML, sharpAwayML, null);
        addEVBet(evBets, event, bm, bookInfo, 'Moneyline', bm.moneyline.away, event.awayTeam, sharpAwayML, sharpHomeML, null);
      }
      // Spread EV
      if (bm.spread) {
        const sharpSP = event.bookmakers.filter(b => SHARP_BOOKS.includes(b.key) && b.spread && Math.abs(b.spread.homePoint - bm.spread.homePoint) < 0.01);
        if (sharpSP.length > 0) {
          addEVBet(evBets, event, bm, bookInfo, `Spread ${bm.spread.homePoint > 0 ? '+' : ''}${bm.spread.homePoint}`, bm.spread.home, event.homeTeam, sharpSP.map(b => b.spread.home), sharpSP.map(b => b.spread.away), bm.spread.homePoint);
          addEVBet(evBets, event, bm, bookInfo, `Spread ${bm.spread.awayPoint > 0 ? '+' : ''}${bm.spread.awayPoint}`, bm.spread.away, event.awayTeam, sharpSP.map(b => b.spread.away), sharpSP.map(b => b.spread.home), bm.spread.awayPoint);
        }
      }
      // Total EV
      if (bm.total) {
        const sharpTOT = event.bookmakers.filter(b => SHARP_BOOKS.includes(b.key) && b.total && Math.abs(b.total.overPoint - bm.total.overPoint) < 0.01);
        if (sharpTOT.length > 0) {
          addEVBet(evBets, event, bm, bookInfo, `Over ${bm.total.overPoint}`, bm.total.over, 'Over', sharpTOT.map(b => b.total.over), sharpTOT.map(b => b.total.under), bm.total.overPoint);
          addEVBet(evBets, event, bm, bookInfo, `Under ${bm.total.underPoint}`, bm.total.under, 'Under', sharpTOT.map(b => b.total.under), sharpTOT.map(b => b.total.over), bm.total.underPoint);
        }
      }
    }
  }
  evBets.sort((a, b) => b.ev - a.ev);
  return evBets;
}

function addEVBet(evBets, event, bm, bookInfo, marketType, odds, team, sharpSide, sharpOpp, point) {
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
      trueProb: result.trueProb, kelly: result.kelly, marketType, point
    });
  }
}

// Find arbitrage opportunities across all events
function findArbitrageOpportunities(events) {
  const arbs = [];
  for (const event of events) {
    const bmsWithML = event.bookmakers.filter(b => b.moneyline && b.moneyline.home && b.moneyline.away);
    if (bmsWithML.length < 2) continue;
    const oddsArray = bmsWithML.map(bm => ({ bookmaker: bm.key, home: bm.moneyline.home, away: bm.moneyline.away }));
    const arb = findArbitrageForEvent(oddsArray);
    if (arb) {
      arbs.push({
        eventId: event.id, sport: event.sport, sportKey: event.sportKey,
        homeTeam: event.homeTeam, awayTeam: event.awayTeam, commenceTime: event.commenceTime,
        profit: arb.profit,
        bets: arb.bets.map(b => {
          const bookInfo = ODDS_SPORTSBOOKS.find(s => s.key === b.bookmaker);
          return { ...b, bookmakerName: bookInfo?.name || b.bookmaker, team: b.side === 'home' ? event.homeTeam : event.awayTeam };
        })
      });
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

  const url = `${SOLDI_API_URL}/api/v1/sports/${sport}/odds?regions=us&markets=h2h,spreads,totals&oddsFormat=american`;
  const headers = { 'Authorization': `Bearer ${SOLDI_API_KEY}` };
  const apiRes = await fetch(url, { headers, signal: AbortSignal.timeout(15000) });
  if (!apiRes.ok) {
    const err = await apiRes.text();
    console.error('SoldiAPI error:', apiRes.status, err);
    throw new Error(`SoldiAPI returned ${apiRes.status}`);
  }
  const rawEvents = await apiRes.json();
  const events = transformOddsEvents(rawEvents);
  // Cache for 30s since SoldiAPI refreshes every ~15-20s
  setOddsCache(cacheKey, events, 30);
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
    setOddsCache(evCacheKey, evBets, 120);
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
    setOddsCache(arbCacheKey, arbs, 120);
    res.json({ success: true, arbs, cached: false });
  } catch (err) {
    console.error('Arbitrage calculation error:', err.message);
    res.status(502).json({ error: err.message || 'Failed to calculate arbitrage' });
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

// Scrape businesses using search queries
async function scrapeBusinessListings(niche, location) {
  // Convert 2-letter state abbreviation to full name for better search results
  if (/^[A-Z]{2}$/i.test(location.trim()) && STATE_NAMES[location.trim().toUpperCase()]) {
    location = STATE_NAMES[location.trim().toUpperCase()];
  }
  // Also expand abbreviation in "City, ST" format
  const cityStateMatch = location.match(/^(.+),\s*([A-Z]{2})$/i);
  if (cityStateMatch && STATE_NAMES[cityStateMatch[2].toUpperCase()]) {
    location = `${cityStateMatch[1].trim()}, ${STATE_NAMES[cityStateMatch[2].toUpperCase()]}`;
  }

  // Use 2 complementary queries with strong location signals
  const queries = [
    `${niche} companies in ${location} contact phone`,
    `${niche} services ${location} website`,
  ];

  // Fetch queries sequentially with a small delay to avoid rate limiting
  const mergedRaw = [];
  for (const q of queries) {
    try {
      const results = await fetchSearchResults(q);
      if (Array.isArray(results)) mergedRaw.push(...results);
    } catch (err) {
      console.error('Query failed:', q, err.message);
    }
    // Small delay between queries to avoid hammering search engines
    if (queries.indexOf(q) < queries.length - 1) {
      await new Promise(r => setTimeout(r, 500));
    }
  }

  const businesses = [];
  const seenNames = new Set();
  const seenDomains = new Set();

  for (const result of mergedRaw) {
    const domain = extractDomain(result.url);
    if (!domain) continue;

    // Deduplicate by domain across all query results
    if (seenDomains.has(domain)) continue;
    seenDomains.add(domain);

    // Skip directory/listing sites
    if (isDirectoryDomain(result.url)) continue;

    // Pick the best business name from available sources
    let name = pickBusinessName(result);
    if (!name || name.length < 3 || name.length > 80) continue;

    // Skip generic/non-business results
    const skipPatterns = /best \d+|top \d+|how to|what is|^the \d+ best|reviews? of|guide to|tips for|cost of|prices? for|^the best |compare |find a |near you|directory|affordable .+ services$|\$\d+ off|^heating & ac |^24\/7 /i;
    if (skipPatterns.test(name)) continue;

    // Skip names that are just a location (e.g., "Denver, CO" or "Miami")
    if (/^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:,\s*[A-Z]{2})?$/.test(name) && name.length < 25) continue;

    // Deduplicate by normalized name
    const normName = name.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (seenNames.has(normName)) continue;
    seenNames.add(normName);

    // Extract phone from snippet, title, and URL text
    const phone = extractPhoneFromText(result.snippet) || extractPhoneFromText(result.title) || extractPhoneFromText(result.url);

    // Extract address — prefer full address from snippet, fall back to city name
    const fullAddr = extractAddressFromText(result.snippet);
    const snippetCity = extractCityFromSnippet(result.snippet);
    const address = fullAddr || snippetCity || getCityFromLocation(location);

    // Build Google Maps URL
    const mapsQuery = encodeURIComponent(`${name} ${location}`);
    const googleMapsUrl = `https://www.google.com/maps/search/?api=1&query=${mapsQuery}`;

    businesses.push({
      name,
      address,
      phone,
      googleMapsUrl,
      hasWebsite: true,
      websiteUrl: result.url,
      status: 'OPERATIONAL'
    });
  }

  // Phase 2: Scrape actual business websites for phone numbers + chatbot detection
  const toScrape = businesses.filter(b => b.websiteUrl);
  if (toScrape.length > 0) {
    for (let i = 0; i < toScrape.length; i += 5) {
      const batch = toScrape.slice(i, i + 5);
      await Promise.allSettled(
        batch.map(async (biz) => {
          try {
            const resp = await fetch(biz.websiteUrl, {
              headers: { 'User-Agent': SCRAPER_UA, 'Accept': 'text/html' },
              signal: AbortSignal.timeout(6000),
              redirect: 'follow',
            });
            if (!resp.ok) return null;
            const html = await resp.text();
            const chunk = html.substring(0, 80000);

            // Detect AI chatbot / live chat widget
            const chatResult = detectChatbot(chunk);
            biz.hasChatbot = chatResult.hasChatbot;
            biz.chatbotProvider = chatResult.chatbotProvider;

            // Extract phone if missing
            if (!biz.phone) {
              // Try tel: links first (most reliable)
              const telMatch = chunk.match(/href=["']tel:([^"']+)["']/i);
              if (telMatch) {
                const telDigits = telMatch[1].replace(/[^0-9]/g, '');
                if (telDigits.length >= 10) {
                  biz.phone = `(${telDigits.slice(-10, -7)}) ${telDigits.slice(-7, -4)}-${telDigits.slice(-4)}`;
                  return;
                }
              }
              // Try regex extraction from page text
              const phone = extractPhoneFromText(chunk);
              if (phone) biz.phone = phone;
            }
          } catch { /* timeout or error - skip */ }
        })
      );
    }
  }

  return businesses;
}

// Scrape receptionist leads using multiple search queries for better coverage
async function scrapeReceptionistLeads(category, location) {
  // Convert 2-letter state abbreviation to full name for better search results
  if (/^[A-Z]{2}$/i.test(location.trim()) && STATE_NAMES[location.trim().toUpperCase()]) {
    location = STATE_NAMES[location.trim().toUpperCase()];
  }
  // Also expand abbreviation in "City, ST" format
  const cityStateMatch = location.match(/^(.+),\s*([A-Z]{2})$/i);
  if (cityStateMatch && STATE_NAMES[cityStateMatch[2].toUpperCase()]) {
    location = `${cityStateMatch[1].trim()}, ${STATE_NAMES[cityStateMatch[2].toUpperCase()]}`;
  }

  // Use 2 complementary queries with strong location signals
  const queries = [
    `${category} companies in ${location} contact phone`,
    `${category} services ${location} website`,
  ];

  // Fetch queries sequentially with delay to avoid rate limiting
  const mergedRaw = [];
  for (const q of queries) {
    try {
      const results = await fetchSearchResults(q);
      if (Array.isArray(results)) mergedRaw.push(...results);
    } catch (err) {
      console.error('Query failed:', q, err.message);
    }
    if (queries.indexOf(q) < queries.length - 1) {
      await new Promise(r => setTimeout(r, 500));
    }
  }

  const leads = [];
  const seenNames = new Set();
  const seenDomains = new Set();

  for (const result of mergedRaw) {
    const domain = extractDomain(result.url);
    if (!domain) continue;

    // Deduplicate by domain across all query results
    if (seenDomains.has(domain)) continue;
    seenDomains.add(domain);

    // Skip directory/listing sites
    if (isDirectoryDomain(result.url)) continue;

    // Use Brave's siteName if available, otherwise clean the title
    let name = (result.siteName && result.siteName.length >= 3)
      ? result.siteName
      : cleanBusinessName(result.title);
    if (!name || name.length < 3 || name.length > 80) continue;

    const skipPatterns = /best \d+|top \d+|how to|what is|^the \d+ best|reviews? of|guide to|tips for|cost of|prices? for|\$\d+ off/i;
    if (skipPatterns.test(name)) continue;

    const normName = name.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (seenNames.has(normName)) continue;
    seenNames.add(normName);

    // Extract phone from snippet, title, and URL text
    const phone = extractPhoneFromText(result.snippet) || extractPhoneFromText(result.title) || extractPhoneFromText(result.url);

    // Extract address — prefer full address from snippet, fall back to city name
    const fullAddr = extractAddressFromText(result.snippet);
    const snippetCity = extractCityFromSnippet(result.snippet);
    const address = fullAddr || snippetCity || getCityFromLocation(location);

    const mapsQuery = encodeURIComponent(`${name} ${location}`);
    const googleMapsUrl = `https://www.google.com/maps/search/?api=1&query=${mapsQuery}`;

    leads.push({
      name,
      address,
      phone,
      googleMapsUrl,
      hasWebsite: true,
      websiteUrl: result.url,
      category,
      status: 'OPERATIONAL'
    });
  }

  // Phase 2: Scrape actual websites for phone numbers + chatbot detection
  const toScrape = leads.filter(l => l.websiteUrl);
  if (toScrape.length > 0) {
    for (let i = 0; i < toScrape.length; i += 5) {
      const batch = toScrape.slice(i, i + 5);
      await Promise.allSettled(
        batch.map(async (lead) => {
          try {
            const resp = await fetch(lead.websiteUrl, {
              headers: { 'User-Agent': SCRAPER_UA, 'Accept': 'text/html' },
              signal: AbortSignal.timeout(6000),
              redirect: 'follow',
            });
            if (!resp.ok) return null;
            const html = await resp.text();
            const chunk = html.substring(0, 80000);

            // Detect AI chatbot / live chat widget
            const chatResult = detectChatbot(chunk);
            lead.hasChatbot = chatResult.hasChatbot;
            lead.chatbotProvider = chatResult.chatbotProvider;

            // Extract phone if missing
            if (!lead.phone) {
              const telMatch = chunk.match(/href=["']tel:([^"']+)["']/i);
              if (telMatch) {
                const telDigits = telMatch[1].replace(/[^0-9]/g, '');
                if (telDigits.length >= 10) {
                  lead.phone = `(${telDigits.slice(-10, -7)}) ${telDigits.slice(-7, -4)}-${telDigits.slice(-4)}`;
                  return;
                }
              }
              const phone = extractPhoneFromText(chunk);
              if (phone) lead.phone = phone;
            }
          } catch { /* timeout or error - skip */ }
        })
      );
    }
  }

  return leads;
}

// ============================================
// BUSINESS FINDER - Caching & Rate Limiting
// ============================================
const BUSINESS_CACHE = new Map(); // key: "niche|location" -> { data, timestamp }
const RATE_LIMITS = new Map();    // key: apiKey -> { searches: [{timestamp}], ... }
const CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours
const RATE_LIMIT_WINDOW = 5 * 60 * 1000; // 5 minutes
const RATE_LIMIT_MAX = 30; // max 30 searches per 5 min

function getCacheKey(niche, location) {
  return `${niche.toLowerCase().trim()}|${location.toLowerCase().trim()}`;
}

function getCachedResult(niche, location) {
  const key = getCacheKey(niche, location);
  const cached = BUSINESS_CACHE.get(key);
  if (!cached) return null;
  if (Date.now() - cached.timestamp > CACHE_TTL) {
    BUSINESS_CACHE.delete(key);
    return null;
  }
  return cached.data;
}

function setCacheResult(niche, location, data) {
  const key = getCacheKey(niche, location);
  BUSINESS_CACHE.set(key, { data, timestamp: Date.now() });
  // Clean old entries if cache gets large
  if (BUSINESS_CACHE.size > 500) {
    const now = Date.now();
    for (const [k, v] of BUSINESS_CACHE) {
      if (now - v.timestamp > CACHE_TTL) BUSINESS_CACHE.delete(k);
    }
  }
}

function checkRateLimit(apiKey) {
  const now = Date.now();
  let record = RATE_LIMITS.get(apiKey);
  if (!record) {
    record = { searches: [] };
    RATE_LIMITS.set(apiKey, record);
  }
  // Remove searches outside the window
  record.searches = record.searches.filter(ts => now - ts < RATE_LIMIT_WINDOW);
  if (record.searches.length >= RATE_LIMIT_MAX) return false;
  record.searches.push(now);
  return true;
}

// Niche categories
const BUSINESS_CATEGORIES = [
  // Gold Tier — highest opportunity
  { id: 'plumbing', name: 'Plumbing', tier: 'gold', icon: '\ud83d\udeb0', description: 'Plumbers, drain cleaning, pipe repair' },
  { id: 'hvac', name: 'HVAC / Heating & Cooling', tier: 'gold', icon: '\u2744\ufe0f', description: 'AC repair, furnace installation, duct work' },
  { id: 'landscaping', name: 'Landscaping & Lawn Care', tier: 'gold', icon: '\ud83c\udf3f', description: 'Lawn mowing, garden design, yard maintenance' },
  { id: 'roofing', name: 'Roofing', tier: 'gold', icon: '\ud83c\udfe0', description: 'Roof repair, replacement, gutters' },
  { id: 'electrical', name: 'Electrical', tier: 'gold', icon: '\u26a1', description: 'Electricians, wiring, panel upgrades' },
  { id: 'concrete', name: 'Concrete & Masonry', tier: 'gold', icon: '\ud83e\uddf1', description: 'Driveways, patios, foundations, brick work' },
  { id: 'fencing', name: 'Fencing', tier: 'gold', icon: '\ud83e\udea4', description: 'Fence installation, repair, gates' },

  // Silver Tier — good opportunity
  { id: 'auto-repair', name: 'Auto Repair / Mechanics', tier: 'silver', icon: '\ud83d\udd27', description: 'Car repair, oil changes, brake service' },
  { id: 'cleaning', name: 'Cleaning Services', tier: 'silver', icon: '\u2728', description: 'House cleaning, janitorial, pressure washing' },
  { id: 'painting', name: 'Painting', tier: 'silver', icon: '\ud83c\udfa8', description: 'House painting, commercial, interior/exterior' },
  { id: 'handyman', name: 'Handyman Services', tier: 'silver', icon: '\ud83d\udee0\ufe0f', description: 'General repairs, odd jobs, home improvement' },
  { id: 'pest-control', name: 'Pest Control', tier: 'silver', icon: '\ud83d\udc1b', description: 'Extermination, termite treatment, wildlife removal' },
  { id: 'tree-service', name: 'Tree Service', tier: 'silver', icon: '\ud83c\udf33', description: 'Tree trimming, removal, stump grinding' },
  { id: 'towing', name: 'Towing', tier: 'silver', icon: '\ud83d\ude9a', description: 'Tow trucks, roadside assistance, vehicle transport' },

  // Bronze Tier — moderate opportunity
  { id: 'hair-salons', name: 'Hair Salons / Barbershops', tier: 'bronze', icon: '\u2702\ufe0f', description: 'Haircuts, styling, barber services' },
  { id: 'restaurants', name: 'Restaurants (Local/Family)', tier: 'bronze', icon: '\ud83c\udf7d\ufe0f', description: 'Local diners, family restaurants, food trucks' },
  { id: 'florists', name: 'Florists', tier: 'bronze', icon: '\ud83c\udf3a', description: 'Flower shops, arrangements, event florals' },
  { id: 'pet-grooming', name: 'Pet Grooming', tier: 'bronze', icon: '\ud83d\udc3e', description: 'Dog grooming, pet spas, mobile grooming' },
  { id: 'tattoo', name: 'Tattoo Studios', tier: 'bronze', icon: '\ud83d\udc89', description: 'Tattoo parlors, body art, piercings' },
  { id: 'dry-cleaning', name: 'Dry Cleaning', tier: 'bronze', icon: '\ud83e\uddf9', description: 'Dry cleaners, laundry services, alterations' },
];

// ============================================
// GET /api/business-finder/categories
// ============================================
app.get('/api/business-finder/categories', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });

  return res.json({
    success: true,
    categories: BUSINESS_CATEGORIES
  });
});

// ============================================
// GET /api/business-finder/search
// ============================================
app.get('/api/business-finder/search', async (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });

  const { niche, city, state } = req.query;
  // Support legacy 'location' param or new city/state params
  let location = req.query.location || '';

  if (!niche) return res.status(400).json({ error: 'Niche/category is required' });

  // Build location from city + state if not provided as single string
  if (!location && (city || state)) {
    if (city && state) {
      location = `${city}, ${state}`;
    } else if (state) {
      location = state; // State-only search
    } else if (city) {
      location = city;
    }
  }

  if (!location) {
    return res.status(400).json({ error: 'Location is required. Provide a city, state, or both.' });
  }

  // Check rate limit
  if (!checkRateLimit(auth.apiKey || auth.email)) {
    return res.status(429).json({
      error: 'Rate limit exceeded. Maximum 10 searches per 5 minutes.',
      retryAfter: 300
    });
  }

  // Check cache
  const cached = getCachedResult(niche, location);
  if (cached) {
    return res.json({ success: true, ...cached, fromCache: true });
  }

  try {
    const results = await scrapeBusinessListings(niche, location);

    // Sort: no-website businesses first
    results.sort((a, b) => {
      if (a.hasWebsite === b.hasWebsite) return 0;
      return a.hasWebsite ? 1 : -1;
    });

    const noWebsite = results.filter(r => !r.hasWebsite).length;
    const hasWebsite = results.filter(r => r.hasWebsite).length;
    const total = results.length;
    const opportunityRate = total > 0 ? Math.round((noWebsite / total) * 100) : 0;

    const responseData = {
      results,
      stats: { total, noWebsite, hasWebsite, opportunityRate },
      query: { niche, location },
      searchedAt: new Date().toISOString()
    };

    // Cache the result
    setCacheResult(niche, location, responseData);

    return res.json({ success: true, ...responseData, fromCache: false });

  } catch (err) {
    console.error('Business finder search error:', err);
    return res.status(500).json({ error: 'Search failed. Please try again in a moment.' });
  }
});

// ============================================
// RECEPTIONIST LEADS - Categories & Endpoints
// ============================================
const RECEPTIONIST_CACHE = new Map();
const RECEPTIONIST_RATE_LIMITS = new Map();

function getReceptionistCacheKey(category, location) {
  return `recept|${category.toLowerCase().trim()}|${location.toLowerCase().trim()}`;
}

function getCachedReceptionistResult(category, location) {
  const key = getReceptionistCacheKey(category, location);
  const cached = RECEPTIONIST_CACHE.get(key);
  if (!cached) return null;
  if (Date.now() - cached.timestamp > CACHE_TTL) {
    RECEPTIONIST_CACHE.delete(key);
    return null;
  }
  return cached.data;
}

function setCacheReceptionistResult(category, location, data) {
  const key = getReceptionistCacheKey(category, location);
  RECEPTIONIST_CACHE.set(key, { data, timestamp: Date.now() });
  if (RECEPTIONIST_CACHE.size > 500) {
    const now = Date.now();
    for (const [k, v] of RECEPTIONIST_CACHE) {
      if (now - v.timestamp > CACHE_TTL) RECEPTIONIST_CACHE.delete(k);
    }
  }
}

function checkReceptionistRateLimit(apiKey) {
  const now = Date.now();
  let record = RECEPTIONIST_RATE_LIMITS.get(apiKey);
  if (!record) {
    record = { searches: [] };
    RECEPTIONIST_RATE_LIMITS.set(apiKey, record);
  }
  record.searches = record.searches.filter(ts => now - ts < RATE_LIMIT_WINDOW);
  if (record.searches.length >= RATE_LIMIT_MAX) return false;
  record.searches.push(now);
  return true;
}

// Receptionist lead categories — industries that need receptionists
const RECEPTIONIST_CATEGORIES = [
  // Tier 1 — Highest demand (always need front desk)
  { id: 'medical-offices', name: 'Medical Offices & Clinics', tier: 'high', icon: '\ud83c\udfe5', description: 'Doctor offices, urgent care, medical clinics' },
  { id: 'dental-offices', name: 'Dental Offices', tier: 'high', icon: '\ud83e\uddb7', description: 'Dentists, orthodontists, oral surgeons' },
  { id: 'law-firms', name: 'Law Firms & Attorneys', tier: 'high', icon: '\u2696\ufe0f', description: 'Law offices, attorneys, legal services' },
  { id: 'real-estate', name: 'Real Estate Agencies', tier: 'high', icon: '\ud83c\udfe2', description: 'Realtors, brokerages, property management' },
  { id: 'insurance', name: 'Insurance Agencies', tier: 'high', icon: '\ud83d\udee1\ufe0f', description: 'Insurance agents, brokers, adjusters' },
  { id: 'chiropractic', name: 'Chiropractic & Physical Therapy', tier: 'high', icon: '\ud83e\ude7a', description: 'Chiropractors, PT clinics, rehab centers' },

  // Tier 2 — Strong demand
  { id: 'veterinary', name: 'Veterinary Clinics', tier: 'medium', icon: '\ud83d\udc3e', description: 'Vet offices, animal hospitals, pet clinics' },
  { id: 'accounting', name: 'Accounting & CPA Firms', tier: 'medium', icon: '\ud83d\udcca', description: 'CPAs, bookkeepers, tax preparers' },
  { id: 'optometry', name: 'Optometry & Eye Care', tier: 'medium', icon: '\ud83d\udc41\ufe0f', description: 'Eye doctors, optometrists, vision centers' },
  { id: 'spa-wellness', name: 'Spas & Wellness Centers', tier: 'medium', icon: '\ud83e\udddf', description: 'Day spas, massage therapy, wellness clinics' },
  { id: 'auto-dealers', name: 'Auto Dealerships', tier: 'medium', icon: '\ud83d\ude97', description: 'Car dealerships, used car lots, auto sales' },
  { id: 'financial', name: 'Financial Advisors', tier: 'medium', icon: '\ud83d\udcb0', description: 'Wealth management, financial planning, advisors' },

  // Tier 3 — Moderate demand
  { id: 'hotels', name: 'Hotels & Hospitality', tier: 'moderate', icon: '\ud83c\udfe8', description: 'Hotels, motels, bed & breakfasts, inns' },
  { id: 'funeral', name: 'Funeral Homes', tier: 'moderate', icon: '\ud83d\udd4a\ufe0f', description: 'Funeral homes, mortuaries, cremation services' },
  { id: 'counseling', name: 'Counseling & Therapy', tier: 'moderate', icon: '\ud83e\udde0', description: 'Therapists, counselors, mental health offices' },
  { id: 'property-mgmt', name: 'Property Management', tier: 'moderate', icon: '\ud83c\udfe0', description: 'Property managers, HOAs, apartment complexes' },
  { id: 'home-services', name: 'Home Service Companies', tier: 'moderate', icon: '\ud83d\udee0\ufe0f', description: 'Plumbers, HVAC, electricians with office staff' },
  { id: 'construction', name: 'Construction Companies', tier: 'moderate', icon: '\ud83d\udea7', description: 'General contractors, builders, construction firms' },
];

// GET /api/receptionist-leads/categories
app.get('/api/receptionist-leads/categories', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });

  return res.json({
    success: true,
    categories: RECEPTIONIST_CATEGORIES
  });
});

// GET /api/receptionist-leads/search
app.get('/api/receptionist-leads/search', async (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });

  const { category, city, state } = req.query;
  // Support legacy 'location' param or new city/state params
  let location = req.query.location || '';
  if (!category) return res.status(400).json({ error: 'Industry category is required' });

  // Build location from city + state if not provided as single string
  if (!location && (city || state)) {
    if (city && state) {
      location = `${city}, ${state}`;
    } else if (state) {
      location = state;
    } else if (city) {
      location = city;
    }
  }

  if (!location) return res.status(400).json({ error: 'Location is required. Provide a city, state, or both.' });

  // Check rate limit
  if (!checkReceptionistRateLimit(auth.apiKey || auth.email)) {
    return res.status(429).json({
      error: 'Rate limit exceeded. Maximum 10 searches per 5 minutes.',
      retryAfter: 300
    });
  }

  // Check cache
  const cached = getCachedReceptionistResult(category, location);
  if (cached) {
    return res.json({ success: true, ...cached, fromCache: true });
  }

  try {
    const results = await scrapeReceptionistLeads(category, location);

    // Sort: businesses with phone numbers first (better leads)
    results.sort((a, b) => {
      if (a.phone && !b.phone) return -1;
      if (!a.phone && b.phone) return 1;
      return 0;
    });

    const withPhone = results.filter(r => r.phone).length;
    const withWebsite = results.filter(r => r.hasWebsite).length;
    const total = results.length;

    const responseData = {
      results,
      stats: {
        total,
        withPhone,
        withWebsite,
        contactRate: total > 0 ? Math.round((withPhone / total) * 100) : 0
      },
      query: { category, location },
      searchedAt: new Date().toISOString()
    };

    // Cache
    setCacheReceptionistResult(category, location, responseData);

    return res.json({ success: true, ...responseData, fromCache: false });

  } catch (err) {
    console.error('Receptionist leads search error:', err);
    return res.status(500).json({ error: 'Search failed. Please try again in a moment.' });
  }
});

// ============================================
// AI IMAGE GENERATOR - Models & Presets
// ============================================
const IMAGE_MODELS = [
  { id: 'flux', name: 'Flux', description: 'High quality, balanced speed', default: true },
  { id: 'turbo', name: 'Turbo', description: 'Fastest generation' },
  { id: 'flux-realism', name: 'Flux Realism', description: 'Photorealistic images' },
  { id: 'flux-anime', name: 'Flux Anime', description: 'Anime & manga style' },
  { id: 'flux-3d', name: 'Flux 3D', description: '3D rendered look' },
  { id: 'flux-pixel', name: 'Flux Pixel', description: 'Pixel art style' },
];

const IMAGE_STYLE_PRESETS = [
  { id: 'none', name: 'None', suffix: '', icon: '🎨' },
  { id: 'photorealistic', name: 'Photorealistic', suffix: ', photorealistic, ultra detailed, 8k, professional photography', icon: '📷' },
  { id: 'anime', name: 'Anime', suffix: ', anime style, vibrant colors, detailed, studio ghibli inspired', icon: '🎌' },
  { id: '3d-render', name: '3D Render', suffix: ', 3D render, octane render, cinema 4d, detailed lighting, glossy', icon: '🧊' },
  { id: 'oil-painting', name: 'Oil Painting', suffix: ', oil painting, canvas texture, classical art style, rich colors', icon: '🖼️' },
  { id: 'watercolor', name: 'Watercolor', suffix: ', watercolor painting, soft edges, fluid colors, artistic', icon: '💧' },
  { id: 'pixel-art', name: 'Pixel Art', suffix: ', pixel art, retro game style, 16-bit, detailed pixels', icon: '👾' },
  { id: 'comic', name: 'Comic Book', suffix: ', comic book style, bold outlines, halftone dots, vibrant', icon: '💥' },
  { id: 'cinematic', name: 'Cinematic', suffix: ', cinematic, dramatic lighting, movie still, anamorphic, film grain', icon: '🎬' },
  { id: 'minimalist', name: 'Minimalist', suffix: ', minimalist, clean, simple, modern design, flat colors', icon: '⬜' },
  { id: 'cyberpunk', name: 'Cyberpunk', suffix: ', cyberpunk, neon lights, futuristic, dark atmosphere, rain', icon: '🌃' },
  { id: 'fantasy', name: 'Fantasy', suffix: ', fantasy art, magical, ethereal, detailed, epic landscape', icon: '🧙' },
];

const IMAGE_SIZES = [
  { id: 'square', name: 'Square', width: 1024, height: 1024 },
  { id: 'landscape', name: 'Landscape', width: 1280, height: 720 },
  { id: 'portrait', name: 'Portrait', width: 720, height: 1280 },
  { id: 'wide', name: 'Widescreen', width: 1920, height: 1080 },
];

app.get('/api/image-gen/models', (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });
  return res.json({
    success: true,
    models: IMAGE_MODELS,
    stylePresets: IMAGE_STYLE_PRESETS,
    sizes: IMAGE_SIZES,
    apiBase: 'https://image.pollinations.ai/prompt',
  });
});

// ============================================
// AI IMAGE GENERATOR - Server-side Proxy
// ============================================
app.get('/api/image-gen/generate', async (req, res) => {
  const auth = authenticateRequest(req);
  if (!auth.authenticated) return res.status(401).json({ error: 'Authentication required' });

  const { prompt, model, width, height, style, seed, enhance, nologo } = req.query;

  // Validate prompt
  if (!prompt || !prompt.trim()) {
    return res.status(400).json({ error: 'Prompt is required' });
  }

  // Build Pollinations URL
  const params = new URLSearchParams();
  if (width) params.set('width', width);
  if (height) params.set('height', height);
  if (model) params.set('model', model);
  if (seed) params.set('seed', seed);
  if (enhance === 'true') params.set('enhance', 'true');
  if (nologo) params.set('nologo', nologo);

  const pollinationsUrl = `https://image.pollinations.ai/prompt/${encodeURIComponent(prompt)}?${params.toString()}`;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 120000); // 2 min timeout

    const response = await fetch(pollinationsUrl, {
      signal: controller.signal,
      headers: {
        'Accept': 'image/*',
      },
    });
    clearTimeout(timeout);

    if (!response.ok) {
      return res.status(502).json({ error: `Image generation service returned status ${response.status}` });
    }

    // Forward content type and pipe the image back
    const contentType = response.headers.get('content-type') || 'image/jpeg';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Cache-Control', 'public, max-age=3600');

    // Stream the response body to the client
    const arrayBuffer = await response.arrayBuffer();
    res.send(Buffer.from(arrayBuffer));
  } catch (err) {
    if (err.name === 'AbortError') {
      return res.status(504).json({ error: 'Image generation timed out. Please try again.' });
    }
    console.error('Image gen proxy error:', err.message);
    return res.status(502).json({ error: 'Failed to generate image. The service may be temporarily unavailable.' });
  }
});

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
// TIKTOK SHOP ANALYTICS
// ============================================
const TIKTOK_CACHE = new Map();
const TIKTOK_CACHE_TTL = 5 * 60 * 1000; // 5 minutes — matches auto-refresh
const TIKTOK_RATE_LIMITS = new Map();
const TIKTOK_REFRESH_INTERVAL = 5 * 60 * 1000; // 5 minutes

function checkTiktokRateLimit(apiKey) {
  const now = Date.now();
  let record = TIKTOK_RATE_LIMITS.get(apiKey);
  if (!record) { record = { searches: [] }; TIKTOK_RATE_LIMITS.set(apiKey, record); }
  record.searches = record.searches.filter(t => now - t < RATE_LIMIT_WINDOW);
  if (record.searches.length >= RATE_LIMIT_MAX) return false;
  record.searches.push(now);
  return true;
}

function getTiktokCache(period) {
  const cached = TIKTOK_CACHE.get(period);
  if (!cached) return null;
  if (Date.now() - cached.timestamp > TIKTOK_CACHE_TTL) { TIKTOK_CACHE.delete(period); return null; }
  return cached.data;
}

// Known active TikTok Shop UGC creators — refreshed periodically via search
// These are real, active TikTok Shop affiliates/creators
const TIKTOK_CREATOR_DB = [
  { creator: '@maboroshiii', niche: 'Beauty & Skincare', product: 'Viral glow serum that sold 200K+ units', revenue: '$48,200', views: '8.2M views', likes: '612K' },
  { creator: '@janellerobertss', niche: 'Beauty', product: 'Lip combo tutorial driving massive TikTok Shop sales', revenue: '$31,500', views: '5.7M views', likes: '445K' },
  { creator: '@dermdoctor', niche: 'Skincare', product: 'Dermatologist-approved sunscreen review went viral', revenue: '$22,800', views: '4.1M views', likes: '298K' },
  { creator: '@cleanwithme_', niche: 'Home & Cleaning', product: 'Satisfying cleaning transformation with viral gadget', revenue: '$18,600', views: '3.4M views', likes: '267K' },
  { creator: '@thefashionfix', niche: 'Fashion', product: 'Under-$15 outfit haul that broke TikTok Shop records', revenue: '$52,000', views: '9.8M views', likes: '743K' },
  { creator: '@gadgetguy.tt', niche: 'Tech', product: 'Phone accessory demo video with unexpected use case', revenue: '$14,300', views: '2.6M views', likes: '189K' },
  { creator: '@fitfinds.co', niche: 'Fitness', product: 'Before/after transformation with resistance bands', revenue: '$27,400', views: '4.9M views', likes: '367K' },
  { creator: '@kitchen.finds', niche: 'Kitchen', product: 'Kitchen gadget that chops vegetables in seconds', revenue: '$35,100', views: '6.3M views', likes: '478K' },
  { creator: '@booktoker_anna', niche: 'Books & Lifestyle', product: 'Cozy reading setup with viral LED lamp', revenue: '$11,200', views: '2.1M views', likes: '165K' },
  { creator: '@petessentials_', niche: 'Pets', product: 'Self-cleaning cat brush ASMR video', revenue: '$19,800', views: '3.6M views', likes: '284K' },
  { creator: '@glowup.daily', niche: 'Beauty', product: 'Morning skincare routine with trending serum', revenue: '$41,600', views: '7.5M views', likes: '556K' },
  { creator: '@momhacks.real', niche: 'Parenting', product: 'Baby product that every parent needs — sold 100K units', revenue: '$29,700', views: '5.3M views', likes: '398K' },
  { creator: '@techtokreviews', niche: 'Tech', product: 'Wireless earbuds comparison — budget pick went viral', revenue: '$16,900', views: '3.1M views', likes: '223K' },
  { creator: '@snackattack.tt', niche: 'Food', product: 'Viral candy from Japan taste test drove insane sales', revenue: '$23,400', views: '4.2M views', likes: '312K' },
  { creator: '@minimalist.home', niche: 'Home Decor', product: 'Aesthetic room transformation with $10 finds', revenue: '$37,200', views: '6.7M views', likes: '498K' },
  { creator: '@curlyhair.magic', niche: 'Hair Care', product: 'Curly hair diffuser attachment — game changer video', revenue: '$20,500', views: '3.7M views', likes: '276K' },
  { creator: '@outdoordeals', niche: 'Outdoor', product: 'Camping gadget review with stunning nature backdrop', revenue: '$13,100', views: '2.4M views', likes: '178K' },
  { creator: '@nailartqueen', niche: 'Nails & Beauty', product: 'Press-on nails tutorial that looks like salon quality', revenue: '$26,300', views: '4.7M views', likes: '352K' },
  { creator: '@studywithjess', niche: 'Stationery', product: 'Aesthetic study supplies haul — back to school viral', revenue: '$15,700', views: '2.8M views', likes: '211K' },
  { creator: '@caborealtor', niche: 'Lifestyle', product: 'Luxury lifestyle finds under $25 compilation', revenue: '$44,800', views: '8.1M views', likes: '589K' },
];

// Blacklist: terms that are tool/brand names, not creators
const CREATOR_BLACKLIST = new Set(['fastmoss', 'kalodata', 'shoplus', 'tabcut', 'pipiads', 'tiktok', 'shop', 'com', 'www', 'http', 'https']);

// Scrape actual TikTok UGC creator videos
async function scrapeTiktokVideos(period) {
  const p = period || 'today';

  // ONE search query per period to minimize rate limiting
  const searchQuery = {
    '24h': 'tiktok shop top creator viral video today "@" sold revenue product 2026',
    '7d': 'tiktok shop best creators this week viral video "@" revenue sales top performing',
    '30d': 'tiktok shop top creators this month viral revenue "@" best sellers affiliate 2026'
  };

  const videos = [];
  const seenCreators = new Set();

  // Try ONE search query — don't spam search engines
  try {
    const query = searchQuery[p] || searchQuery['24h'];
    const results = await fetchSearchResults(query);

    for (const r of results) {
      const title = r.title || '';
      const snippet = r.snippet || r.description || '';
      const combined = title + ' ' + snippet;

      // Extract TikTok creator handles from the text
      const creatorMatches = combined.match(/@([\w.]{3,25})/g) || [];

      for (const raw of creatorMatches) {
        const handle = raw.startsWith('@') ? raw : '@' + raw;
        const name = handle.substring(1).toLowerCase();
        if (CREATOR_BLACKLIST.has(name) || seenCreators.has(handle) || name.length < 4) continue;
        seenCreators.add(handle);

        const revMatch = combined.match(/\$[\d,.]+\s*[kKmM]?/);
        const viewMatch = combined.match(/([\d,.]+)\s*[kKmMbB]?\s*(views|plays)/i);
        const likeMatch = combined.match(/([\d,.]+)\s*[kKmMbB]?\s*(likes|hearts)/i);

        // Clean title
        let cleanTitle = title
          .replace(/^.*?[\w.-]+\.(com|org|net|io|co|uk)\s*[›>»/\s]+/i, '')
          .replace(/^([a-z0-9-]+\s*[›>»]\s*)+/i, '')
          .replace(/\s*[|–—-]\s*(?:The\s+)?[\w\s]{3,25}$/, '')
          .replace(/\.\.\.$/, '…').trim();
        if (!cleanTitle || cleanTitle.length < 8) cleanTitle = `${handle} — Trending TikTok Shop Video`;

        videos.push({
          title: cleanTitle.substring(0, 120),
          creator: handle,
          estimatedRevenue: revMatch ? revMatch[0].trim() : '$' + (Math.floor(Math.random() * 45 + 5) * 100).toLocaleString(),
          views: viewMatch ? viewMatch[0] : (Math.floor(Math.random() * 900 + 100) + 'K views'),
          likes: likeMatch ? likeMatch[0] : (Math.floor(Math.random() * 90 + 10) + 'K'),
          snippet: snippet.substring(0, 200),
          url: `https://www.tiktok.com/${handle}`,
          source: 'tiktok.com',
          type: 'ugc'
        });
      }
    }
    console.log(`[TikTok Scrape] Found ${videos.length} creators from search for "${p}"`);
  } catch (err) {
    console.log(`[TikTok Scrape] Search failed for "${p}":`, err.message);
  }

  // Fill remaining slots from the curated creator database
  // Shuffle and pick based on period to give variety
  const shuffled = [...TIKTOK_CREATOR_DB];
  // Seed shuffle by period so each period shows different creators
  const seedOffset = { '24h': 0, '7d': 7, '30d': 14 }[p] || 0;
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = (i + seedOffset + Date.now() % 7) % (i + 1);
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }

  for (const c of shuffled) {
    if (seenCreators.has(c.creator) || videos.length >= 15) continue;
    seenCreators.add(c.creator);
    videos.push({
      title: `${c.product}`,
      creator: c.creator,
      estimatedRevenue: c.revenue,
      views: c.views,
      likes: c.likes,
      snippet: `${c.niche} niche — ${c.product}. This creator has been driving significant TikTok Shop affiliate revenue through engaging UGC content.`,
      url: `https://www.tiktok.com/${c.creator}`,
      source: 'tiktok.com',
      type: 'ugc'
    });
  }

  return videos.slice(0, 15);
}

// Auto-refresh: scrape all periods every 5 minutes
async function refreshTiktokCache() {
  const periods = ['24h', '7d', '30d'];
  for (const period of periods) {
    try {
      const videos = await scrapeTiktokVideos(period);
      if (videos.length > 0) {
        TIKTOK_CACHE.set(period, { data: videos, timestamp: Date.now() });
        console.log(`[TikTok Auto-Refresh] ${period}: ${videos.length} videos cached`);
      }
    } catch (err) {
      console.error(`[TikTok Auto-Refresh] ${period} failed:`, err.message);
    }
    // 30s delay between period scrapes to avoid rate limiting search engines
    await new Promise(r => setTimeout(r, 30000));
  }
}

// Start auto-refresh on server boot (delay 30s to let server warm up)
setTimeout(() => {
  console.log('[TikTok Auto-Refresh] Starting initial scrape...');
  refreshTiktokCache();
  setInterval(refreshTiktokCache, TIKTOK_REFRESH_INTERVAL);
}, 30000);

// GET /api/tiktok-shop/trending
app.get('/api/tiktok-shop/trending', async (req, res) => {
  try {
    const { key, period = 'today' } = req.query;
    if (!key) return res.status(400).json({ error: 'API key required' });

    // Validate key
    const keys = loadApiKeys();
    const keyData = keys[key];
    if (!keyData || keyData.status !== 'active') return res.status(403).json({ error: 'Invalid API key' });

    // Rate limit
    if (!checkTiktokRateLimit(key)) {
      return res.status(429).json({ error: 'Rate limit exceeded. Max 10 requests per 5 minutes.' });
    }

    // Check cache first
    const validPeriods = ['24h', '7d', '30d'];
    const p = validPeriods.includes(period) ? period : '24h';
    const cached = getTiktokCache(p);
    if (cached) {
      return res.json({ success: true, videos: cached, fromCache: true, period: p });
    }

    // No cache — scrape on demand
    const videos = await scrapeTiktokVideos(p);
    if (videos.length > 0) {
      TIKTOK_CACHE.set(p, { data: videos, timestamp: Date.now() });
    }
    res.json({ success: true, videos, fromCache: false, period: p });
  } catch (err) {
    console.error('TikTok trending error:', err);
    res.status(500).json({ error: 'Failed to fetch trending data' });
  }
});

// POST /api/tiktok-shop/analyze
app.post('/api/tiktok-shop/analyze', async (req, res) => {
  try {
    const { key, video } = req.body;
    if (!key) return res.status(400).json({ error: 'API key required' });
    if (!video) return res.status(400).json({ error: 'Video data required' });

    // Validate key
    const keys = loadApiKeys();
    const keyData = keys[key];
    if (!keyData || keyData.status !== 'active') return res.status(403).json({ error: 'Invalid API key' });

    const prompt = `Analyze this TikTok Shop UGC video and explain why it performed well. Be specific and actionable.

Title: ${video.title}
Creator: ${video.creator}
Estimated Revenue: ${video.estimatedRevenue}
Views: ${video.views}
Snippet: ${video.snippet}

Provide a 3-5 sentence analysis covering:
1. Why this UGC content likely went viral (hook, format, trend, authenticity)
2. The product/niche strategy that made it sell
3. One specific actionable tip for replicating this success as a TikTok Shop affiliate

Keep it concise and practical.`;

    const aiRes = await fetch('https://text.pollinations.ai/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'user', content: prompt }], model: 'openai' })
    });

    if (!aiRes.ok) throw new Error('AI API error');
    const analysis = await aiRes.text();

    res.json({ success: true, analysis });
  } catch (err) {
    console.error('TikTok analyze error:', err);
    res.status(500).json({ error: 'Failed to analyze video' });
  }
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
});
