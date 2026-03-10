// ============================================
// SOLDI TWITTER/X NOTIFICATION BOT
// Monitors Twitter accounts and posts tweets
// to designated Discord channels via Soldi bot
// ============================================
//
// Monitored accounts:
//   @UnderdogNBA  → #nba-news
//   @UnderdogNFL  → #nfl-news
//   @UnderdogMLB  → #mlb-news
//   @GazeboCombat → #ufc-judging
//   @tennisgrinder1 → #tennis-injury
//
// Usage: node twitter-bot.js
//

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');
const { sendEmbed, createEmbed, COLORS } = require('./discord-utils');

// ============================================
// HTTP HELPER (uses https module — more
// compatible with Nitter than native fetch)
// ============================================
function httpsGet(url, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
      },
    }, (res) => {
      // Follow redirects (up to 3)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const location = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        res.resume();
        return httpsGet(location, timeoutMs).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        res.resume();
        return resolve({ status: res.statusCode, data: '', ok: false });
      }

      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, data, ok: true }));
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

// ============================================
// CONFIGURATION
// ============================================
const ACCOUNTS = [
  { handle: 'UnderdogNBA',   channelId: process.env.NBA_NEWS_CHANNEL_ID   || '1477003954511548457', sport: 'NBA',    emoji: '🏀' },
  { handle: 'UnderdogNFL',   channelId: process.env.NFL_NEWS_CHANNEL_ID   || '1477004189329526947', sport: 'NFL',    emoji: '🏈' },
  { handle: 'UnderdogMLB',   channelId: process.env.MLB_NEWS_CHANNEL_ID   || '1477004046857670686', sport: 'MLB',    emoji: '⚾' },
  { handle: 'GazeboCombat',  channelId: process.env.UFC_JUDGING_CHANNEL_ID || '1477003990251077745', sport: 'UFC',    emoji: '🥊' },
  { handle: 'tennisgrinder1', channelId: process.env.TENNIS_INJURY_CHANNEL_ID || '1477011631308275877', sport: 'Tennis', emoji: '🎾' },
];

const CHECK_INTERVAL = 2 * 60 * 1000; // 2 minutes between full cycles
const ACCOUNT_DELAY = 5000;            // 5s between account checks (avoid rate limits)
const MAX_TWEET_AGE = 10 * 60 * 1000;  // Only post tweets from the last 10 minutes
const STATE_FILE = path.join(__dirname, 'data', 'twitter-bot-state.json');

// RSS / scraping sources (tried in order)
const NITTER_INSTANCES = [
  'https://nitter.net',
  'https://nitter.privacydev.net',
  'https://nitter.poast.org',
  'https://nitter.1d4.us',
];

const RSSHUB_INSTANCES = [
  'https://rsshub.app',
  'https://rsshub.rssforever.com',
];

// ============================================
// STATE MANAGEMENT
// ============================================
function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return {};
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function saveState(state) {
  const dir = path.dirname(STATE_FILE);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// ============================================
// TWEET FETCHING — MULTIPLE STRATEGIES
// ============================================

// Strategy 1: Nitter RSS feed (uses https module for compatibility)
async function fetchFromNitter(handle) {
  for (const instance of NITTER_INSTANCES) {
    try {
      const url = `${instance}/${handle}/rss`;
      const res = await httpsGet(url);

      if (!res.ok) continue;
      if (!res.data.includes('<item>') && !res.data.includes('<entry>')) continue;

      const tweets = parseRSSXml(res.data, handle, instance);
      if (tweets.length > 0) {
        return tweets;
      }
    } catch {
      continue;
    }
  }
  return null;
}

// Strategy 2: RSSHub
async function fetchFromRSSHub(handle) {
  for (const instance of RSSHUB_INSTANCES) {
    try {
      const url = `${instance}/twitter/user/${handle}`;
      const res = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SoldiBot/1.0)' },
        signal: AbortSignal.timeout(10000),
      });

      if (!res.ok) continue;

      const contentType = res.headers.get('content-type') || '';

      if (contentType.includes('json')) {
        const data = await res.json();
        const items = data.items || [];
        return items.map(item => ({
          id: extractTweetId(item.url || item.id || ''),
          text: cleanHtml(item.content_html || item.content_text || item.title || ''),
          url: item.url || `https://x.com/${handle}`,
          date: item.date_published || null,
          author: handle,
          images: item.image ? [item.image] : [],
        })).filter(t => t.id);
      }

      // XML RSS
      const xml = await res.text();
      if (xml.includes('<item>') || xml.includes('<entry>')) {
        const tweets = parseRSSXml(xml, handle, instance);
        if (tweets.length > 0) return tweets;
      }
    } catch {
      continue;
    }
  }
  return null;
}

// Track syndication rate-limit state
let syndicationBackoffUntil = 0;

// Strategy 3: Twitter syndication (embedded timeline)
async function fetchFromSyndication(handle) {
  // Skip if we're in a backoff period
  if (Date.now() < syndicationBackoffUntil) return null;

  try {
    const url = `https://syndication.twitter.com/srv/timeline-profile/screen-name/${handle}`;
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
      },
      signal: AbortSignal.timeout(15000),
    });

    if (res.status === 429) {
      // Rate limited — back off for 5 minutes
      syndicationBackoffUntil = Date.now() + 5 * 60 * 1000;
      console.log('  [Syndication] Rate limited — backing off 5 min');
      return null;
    }

    if (!res.ok) return null;
    const html = await res.text();
    const $ = cheerio.load(html);

    const tweets = [];

    // Look for tweet articles/containers
    $('[data-tweet-id], .timeline-Tweet, article').each((_, el) => {
      const $el = $(el);
      const tweetId = $el.attr('data-tweet-id') || '';
      const text = $el.find('.timeline-Tweet-text, .tweet-text, [data-testid="tweetText"]').text().trim();
      const permalink = $el.find('a[href*="/status/"]').attr('href') || '';

      const id = tweetId || extractTweetId(permalink);
      if (!id) return;

      const images = [];
      $el.find('img[src*="pbs.twimg.com"]').each((_, img) => {
        images.push($(img).attr('src'));
      });

      tweets.push({
        id,
        text: text || '',
        url: `https://x.com/${handle}/status/${id}`,
        date: null,
        author: handle,
        images,
      });
    });

    // Also try parsing JSON-LD or embedded data
    $('script').each((_, script) => {
      try {
        const content = $(script).html() || '';
        if (content.includes('"tweet_results"') || content.includes('"full_text"')) {
          const data = JSON.parse(content);
          // Extract tweets from embedded JSON if available
          extractTweetsFromJson(data, handle, tweets);
        }
      } catch { /* ignore parsing errors */ }
    });

    return tweets.length > 0 ? tweets : null;
  } catch {
    return null;
  }
}

// Strategy 4: FxTwitter API
async function fetchFromFxTwitter(handle) {
  try {
    const url = `https://api.fxtwitter.com/${handle}`;
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SoldiBot/1.0)' },
      signal: AbortSignal.timeout(10000),
    });

    if (!res.ok) return null;
    const data = await res.json();

    if (data.tweets && Array.isArray(data.tweets)) {
      return data.tweets.map(t => ({
        id: t.id || extractTweetId(t.url || ''),
        text: t.text || '',
        url: t.url || `https://x.com/${handle}/status/${t.id}`,
        date: t.created_at || null,
        author: handle,
        images: (t.media?.photos || []).map(p => p.url).filter(Boolean),
      })).filter(t => t.id);
    }

    return null;
  } catch {
    return null;
  }
}

// ============================================
// PARSING HELPERS
// ============================================
function parseRSSXml(xml, handle, source) {
  const tweets = [];

  // RSS 2.0 format (<item>)
  const itemRegex = /<item>([\s\S]*?)<\/item>/g;
  let match;
  while ((match = itemRegex.exec(xml)) !== null) {
    const item = match[1];
    const link = item.match(/<link>(.*?)<\/link>/)?.[1]?.trim() ||
                 item.match(/<link[^>]*href="([^"]*)"[^>]*\/>/)?.[1]?.trim() || '';
    const title = item.match(/<title><!\[CDATA\[([\s\S]*?)\]\]><\/title>/)?.[1] ||
                  item.match(/<title>([\s\S]*?)<\/title>/)?.[1] || '';
    const description = item.match(/<description><!\[CDATA\[([\s\S]*?)\]\]><\/description>/)?.[1] ||
                        item.match(/<description>([\s\S]*?)<\/description>/)?.[1] || '';
    const pubDate = item.match(/<pubDate>(.*?)<\/pubDate>/)?.[1] || '';
    const creator = item.match(/<dc:creator>(.*?)<\/dc:creator>/)?.[1] || handle;

    const id = extractTweetId(link);
    if (!id) continue;

    // Extract images from description HTML
    const images = [];
    const imgRegex = /src="(https?:\/\/[^"]*(?:pbs\.twimg\.com|nitter)[^"]*)"/g;
    let imgMatch;
    while ((imgMatch = imgRegex.exec(description)) !== null) {
      let imgUrl = imgMatch[1];
      // Convert Nitter image URLs to Twitter CDN
      if (imgUrl.includes('/pic/')) {
        const encoded = imgUrl.split('/pic/')[1];
        if (encoded) {
          try {
            imgUrl = decodeURIComponent(encoded.replace(/%2F/g, '/'));
            if (!imgUrl.startsWith('http')) imgUrl = `https://${imgUrl}`;
          } catch { /* keep original */ }
        }
      }
      images.push(imgUrl);
    }

    const text = cleanHtml(title || description);

    tweets.push({
      id,
      text,
      url: cleanNitterUrl(link, handle),
      date: pubDate || null,
      author: handle,
      images,
    });
  }

  // Atom format (<entry>)
  if (tweets.length === 0) {
    const entryRegex = /<entry>([\s\S]*?)<\/entry>/g;
    while ((match = entryRegex.exec(xml)) !== null) {
      const entry = match[1];
      const link = entry.match(/<link[^>]*href="([^"]*)"[^>]*\/>/)?.[1]?.trim() ||
                   entry.match(/<link>(.*?)<\/link>/)?.[1]?.trim() || '';
      const title = entry.match(/<title[^>]*>([\s\S]*?)<\/title>/)?.[1] || '';
      const content = entry.match(/<content[^>]*>([\s\S]*?)<\/content>/)?.[1] || '';
      const published = entry.match(/<published>(.*?)<\/published>/)?.[1] || '';

      const id = extractTweetId(link);
      if (!id) continue;

      tweets.push({
        id,
        text: cleanHtml(title || content),
        url: cleanNitterUrl(link, handle),
        date: published || null,
        author: handle,
        images: [],
      });
    }
  }

  return tweets;
}

function extractTweetId(url) {
  if (!url) return null;
  // Match /status/1234567890 pattern
  const match = url.match(/\/status(?:es)?\/(\d+)/);
  if (match) return match[1];
  // If it's just a number
  if (/^\d{10,}$/.test(url)) return url;
  return null;
}

function cleanNitterUrl(url, handle) {
  if (!url) return `https://x.com/${handle}`;
  const tweetId = extractTweetId(url);
  if (tweetId) return `https://x.com/${handle}/status/${tweetId}`;
  return url;
}

function cleanHtml(html) {
  if (!html) return '';
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function extractTweetsFromJson(data, handle, tweets) {
  // Recursively search for tweet data in JSON structures
  if (!data || typeof data !== 'object') return;
  if (data.full_text && data.id_str) {
    if (!tweets.find(t => t.id === data.id_str)) {
      tweets.push({
        id: data.id_str,
        text: data.full_text,
        url: `https://x.com/${handle}/status/${data.id_str}`,
        date: data.created_at || null,
        author: handle,
        images: [],
      });
    }
    return;
  }
  for (const key of Object.keys(data)) {
    if (typeof data[key] === 'object') {
      extractTweetsFromJson(data[key], handle, tweets);
    }
  }
}

// ============================================
// MAIN FETCH (tries all strategies)
// ============================================
async function fetchTweets(handle) {
  // Try each strategy in order (Nitter RSS is most reliable)
  const strategies = [
    { name: 'Nitter', fn: () => fetchFromNitter(handle) },
    { name: 'Syndication', fn: () => fetchFromSyndication(handle) },
    { name: 'RSSHub', fn: () => fetchFromRSSHub(handle) },
  ];

  for (const strategy of strategies) {
    try {
      const tweets = await strategy.fn();
      if (tweets && tweets.length > 0) {
        return { tweets, source: strategy.name };
      }
    } catch {
      continue;
    }
  }

  return { tweets: [], source: 'none' };
}

// ============================================
// CHECK A SINGLE ACCOUNT
// ============================================
async function checkAccount(account, state) {
  const { handle, channelId, sport, emoji } = account;
  const key = handle.toLowerCase();

  if (!state[key]) {
    state[key] = { seenIds: [], lastCheck: null, source: null };
  }

  const { tweets, source } = await fetchTweets(handle);

  if (tweets.length === 0) {
    return 0;
  }

  state[key].source = source;
  state[key].lastCheck = new Date().toISOString();

  // First run: record existing tweets without posting
  if (state[key].seenIds.length === 0) {
    console.log(`  [${handle}] First run via ${source} — recording ${tweets.length} existing tweets`);
    state[key].seenIds = tweets.map(t => t.id);
    return 0;
  }

  // Find new tweets
  const seenSet = new Set(state[key].seenIds);
  const now = Date.now();
  const newTweets = tweets.filter(t => {
    if (seenSet.has(t.id)) return false;
    // Only post tweets that are recent (within MAX_TWEET_AGE)
    // This prevents posting old tweets if state is reset or IDs are lost
    if (t.date) {
      try {
        const tweetAge = now - new Date(t.date).getTime();
        if (tweetAge > MAX_TWEET_AGE) {
          // Old tweet — mark as seen but don't post
          state[key].seenIds.push(t.id);
          return false;
        }
      } catch { /* if date parsing fails, allow it through */ }
    }
    return true;
  });

  if (newTweets.length === 0) return 0;

  // Post new tweets (oldest first, max 5 per cycle)
  const toPost = newTweets.reverse().slice(0, 5);
  let posted = 0;

  for (const tweet of toPost) {
    const tweetText = tweet.text.length > 500
      ? tweet.text.substring(0, 497) + '...'
      : tweet.text;

    // Build description with tweet text + direct link to the tweet
    const tweetLink = tweet.url || `https://x.com/${handle}`;
    const descriptionWithLink = (tweetText || 'New tweet') + `\n\n🔗 [View Tweet](${tweetLink})`;

    const embed = createEmbed({
      author: {
        name: `@${handle}`,
        url: `https://x.com/${handle}`,
        icon_url: 'https://abs.twimg.com/responsive-web/client-web/icon-ios.77d25eba.png',
      },
      title: `${emoji} New ${sport} Tweet`,
      description: descriptionWithLink,
      url: tweetLink,
      color: COLORS.TWITTER,
      footer: { text: `Soldi • ${sport} ${emoji}` },
      image: tweet.images?.[0] || undefined,
    });

    if (tweet.date) {
      try { embed.timestamp = new Date(tweet.date).toISOString(); } catch { /* ignore */ }
    }

    const result = await sendEmbed(channelId, embed);
    if (result) {
      posted++;
      state[key].seenIds.push(tweet.id);
    }

    // Small delay between posts
    await sleep(300);
  }

  // Keep seenIds manageable
  if (state[key].seenIds.length > 500) {
    state[key].seenIds = state[key].seenIds.slice(-500);
  }

  return posted;
}

// ============================================
// CHECK ALL ACCOUNTS
// ============================================
async function checkAllAccounts() {
  const state = loadState();
  let totalNew = 0;

  for (const account of ACCOUNTS) {
    try {
      const count = await checkAccount(account, state);
      if (count > 0) {
        console.log(`  [${account.handle}] Posted ${count} new tweet(s) to #${account.sport.toLowerCase()}`);
        totalNew += count;
      }
    } catch (err) {
      console.error(`  [${account.handle}] Error: ${err.message}`);
    }

    // Delay between accounts
    await sleep(ACCOUNT_DELAY);
  }

  saveState(state);
  return totalNew;
}

// ============================================
// STARTUP & MAIN LOOP
// ============================================
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function main() {
  const BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;
  if (!BOT_TOKEN) {
    console.error('Missing DISCORD_BOT_TOKEN in .env');
    process.exit(1);
  }

  console.log('\n=== Soldi Twitter/X Notification Bot ===');
  console.log('Monitoring accounts:');
  for (const acc of ACCOUNTS) {
    console.log(`  ${acc.emoji} @${acc.handle} → #${acc.sport.toLowerCase()} (${acc.channelId})`);
  }
  console.log(`\nCheck interval: ${CHECK_INTERVAL / 1000}s`);
  console.log('Sources: Nitter RSS → Syndication → RSSHub\n');

  // Initial check
  console.log('[Startup] Running initial check...');
  const initialCount = await checkAllAccounts();
  if (initialCount > 0) {
    console.log(`[Startup] Posted ${initialCount} new tweet(s)`);
  } else {
    console.log('[Startup] All accounts initialized. No new tweets.');
  }

  console.log('\nBot running. Monitoring for new tweets...\n');

  // Main loop
  let cycle = 0;
  while (true) {
    await sleep(CHECK_INTERVAL);
    cycle++;

    const timestamp = new Date().toLocaleTimeString();
    process.stdout.write(`[${timestamp}] Cycle ${cycle}... `);

    const count = await checkAllAccounts();
    if (count > 0) {
      console.log(`${count} new tweet(s) posted`);
    } else {
      console.log('no new tweets');
    }
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[Twitter Bot] Shutting down...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n[Twitter Bot] Shutting down...');
  process.exit(0);
});

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
