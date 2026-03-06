require('dotenv').config();
const express = require('express');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const bcrypt = require('bcryptjs');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname)));

const WHOP_API_KEY = process.env.WHOP_API_KEY;
const WHOP_COMPANY_ID = process.env.WHOP_COMPANY_ID;
const JWT_SECRET = process.env.JWT_SECRET || 'fallback-secret-change-me';
const WHOP_STORE_SLUG = process.env.WHOP_STORE_SLUG || 'soldi-4def';
const WHOP_PRODUCT_PATH = process.env.WHOP_PRODUCT_PATH || 'soldi-a9';

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
  if (admins.length > 0) return; // Already seeded

  const email = process.env.ADMIN_SEED_EMAIL;
  const password = process.env.ADMIN_SEED_PASSWORD;
  if (!email || !password) {
    console.log('⚠️  No ADMIN_SEED_EMAIL/ADMIN_SEED_PASSWORD in .env — skipping admin seed');
    return;
  }

  const hash = await bcrypt.hash(password, 10);
  const owner = {
    id: crypto.randomUUID(),
    email: email.toLowerCase().trim(),
    passwordHash: hash,
    role: 'owner',
    createdAt: new Date().toISOString(),
  };
  saveAdmins([owner]);
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

function requireAdmin(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No token provided' });
  }
  try {
    const decoded = jwt.verify(authHeader.split(' ')[1], JWT_SECRET);
    if (decoded.type !== 'admin') {
      return res.status(403).json({ error: 'Admin access required' });
    }
    const admins = loadAdmins();
    const admin = admins.find(a => a.id === decoded.adminId);
    if (!admin) {
      return res.status(401).json({ error: 'Admin account not found' });
    }
    req.admin = admin;
    next();
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
// POST /api/verify-membership
// Body: { email: "user@example.com" }
// ============================================
app.post('/api/verify-membership', async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Email is required' });

  if (!WHOP_API_KEY || WHOP_API_KEY === 'YOUR_API_KEY_HERE') {
    return res.status(500).json({ error: 'Server not configured. API key missing.' });
  }

  try {
    let page = 1;
    let found = null;
    const maxPages = 20;

    while (!found && page <= maxPages) {
      const url = `https://api.whop.com/api/v1/memberships?company_id=${WHOP_COMPANY_ID}&page=${page}&per=50`;
      const response = await fetch(url, {
        headers: { Authorization: `Bearer ${WHOP_API_KEY}` }
      });

      if (!response.ok) {
        const errText = await response.text();
        console.error('Whop API error:', response.status, errText);
        return res.status(502).json({ error: 'Failed to verify membership' });
      }

      const data = await response.json();
      const memberships = data.data || [];

      if (memberships.length === 0) break;

      const match = memberships.find(
        m => m.email && m.email.toLowerCase() === email.toLowerCase()
      );

      if (match) {
        found = match;
        break;
      }

      if (!data.pagination || page >= data.pagination.total_pages) break;
      page++;
    }

    if (!found) {
      return res.status(404).json({
        error: 'no_membership',
        message: 'No membership found for this email',
        purchaseUrl: `https://whop.com/${WHOP_STORE_SLUG}/${WHOP_PRODUCT_PATH}/`
      });
    }

    const activeStatuses = ['active', 'trialing', 'canceling'];
    const isActive = activeStatuses.includes(found.status);

    if (!isActive) {
      return res.status(403).json({
        error: 'inactive_membership',
        message: `Membership status: ${found.status}`,
        status: found.status,
        purchaseUrl: `https://whop.com/${WHOP_STORE_SLUG}/${WHOP_PRODUCT_PATH}/`
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
        email: found.email,
        membershipId: found.id,
        createdAt: new Date().toISOString(),
        status: 'active'
      };
      saveApiKeys(keys);
    }

    // Sign JWT (7 day expiry)
    const token = jwt.sign(
      {
        membershipId: found.id,
        email: found.email,
        status: found.status,
        affiliateLink,
        affiliateUsername,
        createdAt: found.created_at
      },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    return res.json({
      success: true,
      token,
      apiKey,
      user: {
        email: found.email,
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
// GET /api/odds-data - Get odds data (requires API key)
// ============================================
app.get('/api/odds-data', (req, res) => {
  const { key } = req.query;
  if (!key) return res.status(400).json({ error: 'API key is required' });

  const keys = loadApiKeys();
  const keyData = keys[key];

  if (!keyData || keyData.status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }

  // Mock odds data (replace with real data feed later)
  const now = new Date().toISOString();
  const odds = [
    { sport: 'NBA', game: 'Lakers vs Celtics', line: 'LAL -3.5', odds: '-110', book: 'FanDuel', edge: '3.2%', updated: now },
    { sport: 'NBA', game: 'Warriors vs Bucks', line: 'Over 228.5', odds: '-105', book: 'DraftKings', edge: '2.8%', updated: now },
    { sport: 'NBA', game: 'Nuggets vs 76ers', line: 'DEN ML', odds: '+145', book: 'BetMGM', edge: '4.1%', updated: now },
    { sport: 'NFL', game: 'Chiefs vs Bills', line: 'KC -2.5', odds: '-108', book: 'Caesars', edge: '2.5%', updated: now },
    { sport: 'NFL', game: 'Eagles vs Cowboys', line: 'Under 44.5', odds: '-112', book: 'FanDuel', edge: '1.9%', updated: now },
    { sport: 'MLB', game: 'Yankees vs Red Sox', line: 'NYY ML', odds: '-135', book: 'DraftKings', edge: '3.7%', updated: now },
    { sport: 'MLB', game: 'Dodgers vs Padres', line: 'Over 8.5', odds: '+100', book: 'BetMGM', edge: '2.1%', updated: now },
    { sport: 'NHL', game: 'Rangers vs Bruins', line: 'NYR ML', odds: '+120', book: 'FanDuel', edge: '3.5%', updated: now },
    { sport: 'NBA', game: 'Heat vs Knicks', line: 'NYK -5.5', odds: '-110', book: 'Caesars', edge: '2.3%', updated: now },
    { sport: 'NFL', game: 'Ravens vs Bengals', line: 'BAL -1.5', odds: '-102', book: 'DraftKings', edge: '4.6%', updated: now },
  ];

  return res.json({ success: true, odds, generatedAt: now });
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
    const response = await fetch(url, {
      headers: { Authorization: `Bearer ${WHOP_API_KEY}` }
    });

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
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${WHOP_API_KEY}`,
        'Content-Type': 'application/json'
      }
    });

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
const SCRAPER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

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
  const patterns = [
    /\((\d{3})\)\s*(\d{3})[-.](\d{4})/,
    /(\d{3})[-.](\d{3})[-.](\d{4})/,
    /(\d{3})\s+(\d{3})\s+(\d{4})/,
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

function extractAddressFromText(text) {
  // Look for common US address patterns
  const addrPattern = /(\d+\s+(?:[NSEW]\s+)?[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\s+(?:St|Street|Ave|Avenue|Blvd|Boulevard|Dr|Drive|Rd|Road|Ln|Lane|Way|Ct|Court|Pl|Place|Pkwy|Parkway|Hwy|Highway|Cir|Circle)[.,]?\s*(?:(?:Ste|Suite|Unit|Apt|#)\s*\w+[.,]?\s*)?(?:[A-Z][a-zA-Z]+[.,]?\s+)?(?:FL|CA|TX|NY|GA|NC|OH|PA|MI|IL|NJ|VA|WA|AZ|MA|TN|IN|MO|MD|WI|CO|MN|SC|AL|LA|KY|OR|OK|CT|UT|IA|NV|AR|MS|KS|NM|NE|WV|ID|HI|NH|ME|MT|RI|DE|SD|ND|AK|VT|WY|DC)\s+\d{5})/i;
  const match = text.match(addrPattern);
  return match ? match[1].trim() : '';
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

// Fetch search results from Brave Search (primary) with DDG fallback
async function fetchSearchResults(query) {
  const errors = [];

  // Try Brave Search first
  try {
    const results = await fetchBraveResults(query);
    if (results.length > 0) return results;
    console.log('Brave returned 0 results, trying DDG...');
  } catch (err) {
    errors.push(`Brave: ${err.message}`);
    console.error('Brave Search failed:', err.message);
  }

  // Fallback to DuckDuckGo HTML
  try {
    const results = await fetchDDGResults(query);
    if (results.length > 0) return results;
  } catch (err) {
    errors.push(`DDG: ${err.message}`);
    console.error('DDG Search failed:', err.message);
  }

  // If both failed, throw so the endpoint returns a proper error
  if (errors.length === 2) {
    throw new Error('Search engines temporarily unavailable. Please try again in a few minutes.');
  }

  return [];
}

// Brave Search HTML scraper
async function fetchBraveResults(query) {
  const url = `https://search.brave.com/search?q=${encodeURIComponent(query)}`;

  const response = await fetch(url, {
    headers: {
      'User-Agent': SCRAPER_UA,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(`Brave Search returned status ${response.status}`);
  }

  const html = await response.text();
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
    // Brave format: "Brand Name domain.com › breadcrumbs"
    let siteName = $el.find('.site-name-content').first().text().trim()
      .replace(/\s*›.*$/, '') // Remove breadcrumbs like " › home › locations"
      .trim();
    // Remove the domain suffix (e.g., "Roto-Rooter rotorooter.com" → "Roto-Rooter")
    siteName = siteName.replace(/\s+[a-z0-9][-a-z0-9]*\.[a-z]{2,}(\.[a-z]{2,})?\s*$/i, '').trim();

    // Get the page title
    const title = $el.find('.snippet-title').first().text().trim() ||
                  mainLink.text().trim();

    // Get description
    const desc = $el.find('.snippet-description').text().trim() ||
                 $el.find('.snippet-content .description').text().trim();

    // Deduplicate by domain — skip sub-pages of same domain
    const domain = extractDomain(href);
    if (!domain || seenDomains.has(domain)) return;
    seenDomains.add(domain);

    if (title && href) {
      results.push({
        title,
        url: href,
        siteName, // Clean brand name from Brave
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
      'User-Agent': SCRAPER_UA,
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

// Scrape businesses using DuckDuckGo HTML search (US only, free)
async function scrapeBusinessListings(niche, location) {
  const query = `${niche} in ${location} phone address`;
  const rawResults = await fetchSearchResults(query);

  const businesses = [];
  const seenNames = new Set();

  for (const result of rawResults) {
    const domain = extractDomain(result.url);
    if (!domain) continue;

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

    // Extract phone from snippet and title
    const phone = extractPhoneFromText(result.snippet) || extractPhoneFromText(result.title);

    // Extract address from snippet
    const address = extractAddressFromText(result.snippet) || location;

    // Build Google Maps URL
    const mapsQuery = encodeURIComponent(`${name} ${location}`);
    const googleMapsUrl = `https://www.google.com/maps/search/?api=1&query=${mapsQuery}`;

    businesses.push({
      name,
      address,
      phone,
      googleMapsUrl,
      rating: null,
      reviewCount: 0,
      hasWebsite: true,
      websiteUrl: result.url,
      status: 'OPERATIONAL'
    });
  }

  return businesses;
}

// Scrape receptionist leads using search engines
async function scrapeReceptionistLeads(category, location) {
  const query = `${category} in ${location} phone`;
  const rawResults = await fetchSearchResults(query);

  const leads = [];
  const seenNames = new Set();

  for (const result of rawResults) {
    const domain = extractDomain(result.url);
    if (!domain) continue;

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

    const phone = extractPhoneFromText(result.snippet) || extractPhoneFromText(result.title);
    const address = extractAddressFromText(result.snippet) || location;
    const mapsQuery = encodeURIComponent(`${name} ${location}`);
    const googleMapsUrl = `https://www.google.com/maps/search/?api=1&query=${mapsQuery}`;

    leads.push({
      name,
      address,
      phone,
      googleMapsUrl,
      rating: null,
      reviewCount: 0,
      hasWebsite: true,
      websiteUrl: result.url,
      category,
      status: 'OPERATIONAL'
    });
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
const RATE_LIMIT_MAX = 10; // max 10 searches per 5 min

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
  const { key } = req.query;
  if (!key) return res.status(400).json({ error: 'API key is required' });

  const keys = loadApiKeys();
  const keyData = keys[key];
  if (!keyData || keyData.status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }

  return res.json({
    success: true,
    categories: BUSINESS_CATEGORIES
  });
});

// ============================================
// GET /api/business-finder/search
// ============================================
app.get('/api/business-finder/search', async (req, res) => {
  const { key, niche, location } = req.query;

  if (!key) return res.status(400).json({ error: 'API key is required' });
  if (!niche) return res.status(400).json({ error: 'Niche/category is required' });
  if (!location) return res.status(400).json({ error: 'Location is required' });

  // Validate API key
  const keys = loadApiKeys();
  const keyData = keys[key];
  if (!keyData || keyData.status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }

  // Check rate limit
  if (!checkRateLimit(key)) {
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
  const { key } = req.query;
  if (!key) return res.status(400).json({ error: 'API key is required' });

  const keys = loadApiKeys();
  const keyData = keys[key];
  if (!keyData || keyData.status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }

  return res.json({
    success: true,
    categories: RECEPTIONIST_CATEGORIES
  });
});

// GET /api/receptionist-leads/search
app.get('/api/receptionist-leads/search', async (req, res) => {
  const { key, category, location } = req.query;

  if (!key) return res.status(400).json({ error: 'API key is required' });
  if (!category) return res.status(400).json({ error: 'Industry category is required' });
  if (!location) return res.status(400).json({ error: 'Location is required' });

  // Validate API key
  const keys = loadApiKeys();
  const keyData = keys[key];
  if (!keyData || keyData.status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }

  // Check rate limit
  if (!checkReceptionistRateLimit(key)) {
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
  const apiKey = req.query.key;
  if (!apiKey) return res.status(400).json({ error: 'API key required' });
  const keys = loadApiKeys();
  if (!keys[apiKey] || keys[apiKey].status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }
  return res.json({
    success: true,
    models: IMAGE_MODELS,
    stylePresets: IMAGE_STYLE_PRESETS,
    sizes: IMAGE_SIZES,
    apiBase: 'https://image.pollinations.ai/prompt',
  });
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
  const apiKey = req.query.key;
  if (!apiKey) return res.status(400).json({ error: 'API key required' });
  const keys = loadApiKeys();
  if (!keys[apiKey] || keys[apiKey].status !== 'active') {
    return res.status(401).json({ error: 'Invalid or inactive API key' });
  }
  return res.json({
    success: true,
    emailTemplates: REVIEW_EMAIL_TEMPLATES,
    smsTemplates: REVIEW_SMS_TEMPLATES,
  });
});

// ============================================
// FORM SUBMISSIONS (replaces Formspree)
// ============================================
app.post('/api/submissions', (req, res) => {
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

    return res.status(201).json({ success: true });
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
// START SERVER
// ============================================
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Soldi server running on http://localhost:${PORT}`);
});
