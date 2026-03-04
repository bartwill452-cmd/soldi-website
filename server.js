require('dotenv').config();
const express = require('express');
const jwt = require('jsonwebtoken');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname)));

const WHOP_API_KEY = process.env.WHOP_API_KEY;
const WHOP_COMPANY_ID = process.env.WHOP_COMPANY_ID;
const JWT_SECRET = process.env.JWT_SECRET || 'fallback-secret-change-me';
const WHOP_STORE_SLUG = process.env.WHOP_STORE_SLUG || 'soldi-4def';
const WHOP_PRODUCT_PATH = process.env.WHOP_PRODUCT_PATH || 'soldi-a9';

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
app.get('/api/me', (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No token' });
  }

  try {
    const decoded = jwt.verify(authHeader.split(' ')[1], JWT_SECRET);
    return res.json({ success: true, user: decoded });
  } catch {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
});

// ============================================
// START SERVER
// ============================================
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Soldi server running on http://localhost:${PORT}`);
});
