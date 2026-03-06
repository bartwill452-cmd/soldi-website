// ============================================
// SOLDI POLYMARKET WHALE TRACKER
// Monitors whale wallets on Polymarket and
// posts alerts to Discord via the Soldi bot
// ============================================
//
// Usage: node polymarket-bot.js
//
// Tracks 37 whale addresses. When a whale places
// a new BUY bet, sends a rich embed to Discord
// with trader info, market, odds, price data.
//

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { sendNotification, createEmbed, COLORS } = require('./discord-utils');

// ============================================
// CONFIGURATION
// ============================================

const POLYMARKET_CHANNEL_ID = process.env.POLYMARKET_CHANNEL_ID || '';
const REFRESH_INTERVAL = 15; // seconds between full cycles (be kind to API)
const RATE_LIMIT_BACKOFF = 10; // seconds to wait if rate limited

// Polymarket APIs
const DATA_API = 'https://data-api.polymarket.com';
const CLOB_API = 'https://clob.polymarket.com';
const PROFILE_API = 'https://gamma-api.polymarket.com';

// 37 whale addresses to track
const TARGET_ADDRESSES = [
  '0xac44cb78be973ec7d91b69678c4bdfa7009afbd7',
  '0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee',
  '0xd25c72ac0928385610611c8148803dc717334d20',
  '0x94f199fb7789f1aef7fff6b758d6b375100f4c7a',
  '0x14964aefa2cd7caff7878b3820a690a03c5aa429',
  '0xd0b4c4c020abdc88ad9a884f999f3d8cff8ffed6',
  '0x13414a77a4be48988851c73dfd824d0168e70853',
  '0x507e52ef684ca2dd91f90a9d26d149dd3288beae',
  '0x2005d16a84ceefa912d4e380cd32e7ff827875ea',
  '0x93abbc022ce98d6f45d4444b594791cc4b7a9723',
  '0xd6a3f0ec6c4a8ad680d580610c82ca57ff139489',
  '0x91654fd592ea5339fc0b1b2f2b30bfffa5e75b98',
  '0x57cd939930fd119067ca9dc42b22b3e15708a0fb',
  '0x9cb990f1862568a63d8601efeebe0304225c32f2',
  '0xdb27bf2ac5d428a9c63dbc914611036855a6c56e',
  '0xe6a3778e5c3f93958534684ed7308b4625622f0d',
  '0xd7a58948a0aba3015f057ab4ecc5bef039e47c26',
  '0xaa075924e1dc7cff3b9fab67401126338c4d2125',
  '0x31a56e9e690c621ed21de08cb559e9524cdb8ed9',
  '0x7744bfd749a70020d16a1fcbac1d064761c9999e',
  '0x96489abcb9f583d6835c8ef95ffc923d05a86825',
  '0x05e26c775ecfe91b897d47f134c1bf5900ca6e12',
  '0x90ed5bffbffbfc344aa1195572d89719a398b5bc',
  '0xdbade4c82fb72780a0db9a38f821d8671aba9c95',
  '0x876426b52898c295848f56760dd24b55eda2604a',
  '0xccb290b1c145d1c95695d3756346bba9f1398586',
  '0x4bd74aef0ee5f1ec0718890f55c15f047e28373e',
  '0x72b40c0012682ef52228ad53ef955f9e4f177d67',
  '0xf1528f12e645462c344799b62b1b421a6a4c64aa',
  '0x2537fa3357f0e42fa283b8d0338390dda0b6bff9',
  '0x5c2bd19cb9bb241f864a057e4b2da6d2a3d62575',
  '0x6adcccb0ea0b93a66e67f0d7b2b625b135a8beba',
  '0xf705fa045201391d9632b7f3cde06a5e24453ca7',
  '0x57a8d63731277200ed26cfde9a8a830d94f36933',
  '0x44c58184f89a5c2f699dc8943009cb3d75a08d45',
  '0x6ade597c0e2b43c0bf3542cada8a5e330d73f5b0',
  '0xac75b6e590720a394364f2a1580b68a2fbe51319',
  '0xedc8b2023897dad9df5b2f47ce79b2cdf1b6cca9',
];

// ============================================
// STATE
// ============================================
const processedBets = new Set();
const traderNameCache = {};
const traderProfitCache = {};

// ============================================
// USER TRACKING (loaded from shared JSON file)
// ============================================
const USER_TRACKING_FILE = path.join(__dirname, 'data', 'user-tracking.json');
let userTrackingData = {};       // { discordUserId: { addresses: [...], dmChannelId: "..." } }
let addressToUsers = new Map();  // address → [{ userId, dmChannelId }]
let allAddresses = [];           // merged: defaults + custom (deduplicated)
let lastTrackingLoad = 0;
const TRACKING_RELOAD_INTERVAL = 5 * 60 * 1000; // reload every 5 minutes

function loadUserTrackingData() {
  try {
    if (!fs.existsSync(USER_TRACKING_FILE)) {
      userTrackingData = {};
    } else {
      userTrackingData = JSON.parse(fs.readFileSync(USER_TRACKING_FILE, 'utf8'));
    }
  } catch {
    userTrackingData = {};
  }

  // Build reverse map: address → [users]
  addressToUsers = new Map();
  for (const [userId, data] of Object.entries(userTrackingData)) {
    for (const addr of (data.addresses || [])) {
      if (!addressToUsers.has(addr)) addressToUsers.set(addr, []);
      addressToUsers.get(addr).push({ userId, dmChannelId: data.dmChannelId });
    }
  }

  // Build merged address list (defaults + custom, deduplicated)
  const customAddrs = [...addressToUsers.keys()];
  const allSet = new Set([...TARGET_ADDRESSES, ...customAddrs]);
  allAddresses = [...allSet];

  lastTrackingLoad = Date.now();
  return customAddrs.length;
}

function maybeReloadTracking() {
  if (Date.now() - lastTrackingLoad > TRACKING_RELOAD_INTERVAL) {
    const customCount = loadUserTrackingData();
    if (customCount > 0) {
      console.log(`  [Tracking] Reloaded: ${customCount} custom address(es) from ${Object.keys(userTrackingData).length} user(s)`);
    }
  }
}

// ============================================
// HELPERS
// ============================================

async function apiFetch(url, params = {}) {
  const qs = new URLSearchParams(params).toString();
  const fullUrl = qs ? `${url}?${qs}` : url;
  const res = await fetch(fullUrl, {
    headers: { 'User-Agent': 'SoldiBot/1.0' },
  });
  if (res.status === 429) {
    const err = new Error('Rate limited');
    err.status = 429;
    throw err;
  }
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

function decimalToAmericanOdds(price) {
  if (price <= 0 || price >= 1) return 'N/A';
  if (price >= 0.5) {
    return `${Math.round(-(price / (1 - price)) * 100)}`;
  }
  return `+${Math.round(((1 - price) / price) * 100)}`;
}

function getMarketUrl(bet) {
  const eventSlug = bet.eventSlug || '';
  const marketSlug = bet.slug || bet.marketSlug || '';
  if (eventSlug && marketSlug) return `https://polymarket.com/event/${eventSlug}/${marketSlug}`;
  if (eventSlug) return `https://polymarket.com/event/${eventSlug}`;
  if (marketSlug) return `https://polymarket.com/event/${marketSlug}`;
  return 'https://polymarket.com/markets';
}

function getTraderProfileUrl(addr) {
  return `https://polymarket.com/profile/${addr}`;
}

function getBetId(bet, traderAddr) {
  return `${traderAddr}_${bet.conditionId}_${bet.outcomeIndex}`;
}

function formatPriceChange(entryPrice, currentPrice) {
  const changeCents = (currentPrice - entryPrice) * 100;
  const changePct = entryPrice > 0 ? (changeCents / (entryPrice * 100)) * 100 : 0;
  const sign = changeCents > 0 ? '+' : '';
  const emoji = changeCents > 0 ? '\u{1F4C8}' : changeCents < 0 ? '\u{1F4C9}' : '\u{27A1}\u{FE0F}';
  return {
    changeCents, changePct, sign, emoji,
    formatted: `${emoji} ${sign}${changeCents.toFixed(1)}\u00A2 (${sign}${changePct.toFixed(1)}%)`,
  };
}

// ============================================
// POLYMARKET API CALLS
// ============================================

async function getProfileName(addr) {
  if (traderNameCache[addr]) return traderNameCache[addr];
  try {
    const profile = await apiFetch(`${PROFILE_API}/public-profile`, { address: addr });
    const name = profile.name || profile.pseudonym || `${addr.slice(0, 8)}...${addr.slice(-4)}`;
    traderNameCache[addr] = name;
    return name;
  } catch {
    const name = `${addr.slice(0, 8)}...${addr.slice(-4)}`;
    traderNameCache[addr] = name;
    return name;
  }
}

async function getTraderProfit(addr) {
  if (traderProfitCache[addr]) return traderProfitCache[addr];
  try {
    const profile = await apiFetch(`${PROFILE_API}/public-profile`, { address: addr });
    const pnl = parseFloat(profile.pnl || profile.profitLoss || profile.allTimePnl || profile.totalPnl);
    if (!isNaN(pnl)) {
      const formatted = pnl >= 0 ? `+$${pnl.toLocaleString('en-US', { minimumFractionDigits: 2 })}` : `-$${Math.abs(pnl).toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
      traderProfitCache[addr] = formatted;
      return formatted;
    }
    traderProfitCache[addr] = 'N/A';
    return 'N/A';
  } catch {
    traderProfitCache[addr] = 'N/A';
    return 'N/A';
  }
}

async function getCurrentMarketPrice(tokenId) {
  try {
    const data = await apiFetch(`${CLOB_API}/prices`, { token_id: tokenId });
    if (data[tokenId]) return parseFloat(data[tokenId]);
    return null;
  } catch {
    return null;
  }
}

async function getMarketImage(bet) {
  if (bet.image) return bet.image;
  if (bet.icon) return bet.icon;
  try {
    const slug = bet.marketSlug;
    if (slug) {
      const data = await apiFetch(`${DATA_API}/markets/${slug}`);
      return data.image || data.icon || null;
    }
  } catch { /* ignore */ }
  return null;
}

async function getLatestBet(addr, maxAgeMinutes = 0) {
  const data = await apiFetch(`${DATA_API}/activity`, { user: addr, limit: 20 });
  const now = Date.now();

  for (const activity of data) {
    if (activity.type !== 'TRADE' || activity.side !== 'BUY') continue;

    if (maxAgeMinutes > 0) {
      const rawTs = activity.timestamp || activity.createdAt || activity.updatedAt;
      if (rawTs) {
        let ts = parseFloat(rawTs);
        if (ts > 1e12) ts = ts / 1000;
        const ageMin = (now - ts * 1000) / 60000;
        if (ageMin > maxAgeMinutes) continue;
      }
    }
    return activity;
  }
  return null;
}

// ============================================
// CHECK A SINGLE TRADER
// ============================================
async function checkTrader(addr) {
  let latest;
  try {
    latest = await getLatestBet(addr, 2); // last 2 minutes
  } catch (err) {
    if (err.status === 429) {
      console.log(`\n  Rate limited! Cooling down ${RATE_LIMIT_BACKOFF}s...`);
      await new Promise(r => setTimeout(r, RATE_LIMIT_BACKOFF * 1000));
      return;
    }
    if (err.status === 404) return;
    throw err;
  }

  if (!latest) return;

  const betId = getBetId(latest, addr);
  if (processedBets.has(betId)) return;

  // New bet detected!
  const traderName = await getProfileName(addr);
  const traderProfit = await getTraderProfit(addr);
  const { title, outcome, size: targetSize, price, asset } = latest;
  const americanOdds = decimalToAmericanOdds(price);
  const marketUrl = getMarketUrl(latest);
  const traderUrl = getTraderProfileUrl(addr);

  // Trade time
  let tradeTimeStr = 'Unknown';
  const rawTs = latest.timestamp || latest.createdAt || latest.updatedAt;
  if (rawTs) {
    try {
      let tsVal = parseFloat(rawTs);
      if (tsVal > 1e12) tsVal = tsVal / 1000;
      const dt = new Date(tsVal * 1000);
      tradeTimeStr = dt.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit', timeZone: 'America/New_York' }) + ' ET';
    } catch { /* ignore */ }
  }

  // Current price comparison
  const currentPrice = await getCurrentMarketPrice(asset);
  let priceChange = null;
  if (currentPrice !== null) {
    priceChange = formatPriceChange(price, currentPrice);
  }

  // Market image
  const marketImage = await getMarketImage(latest);

  console.log(`\n${'='.repeat(60)}`);
  console.log(`  NEW BET: ${traderName}`);
  console.log(`  Market: ${title}`);
  console.log(`  Outcome: ${outcome} | Size: $${targetSize?.toFixed(2)} | Price: ${(price * 100).toFixed(1)}c`);
  if (currentPrice) console.log(`  Current: ${(currentPrice * 100).toFixed(1)}c | Odds: ${americanOdds}`);
  console.log(`${'='.repeat(60)}`);

  // Build embed fields
  const fields = [
    { name: '\u{1F464} Trader', value: `[${traderName}](${traderUrl}) (${traderProfit})\n\`${addr}\``, inline: false },
    { name: '\u{1F3AF} Market', value: (title || '').slice(0, 100), inline: false },
    { name: '\u{1F4CA} Outcome', value: outcome || '-', inline: true },
    { name: '\u{1F550} Trade Time', value: tradeTimeStr, inline: true },
  ];

  if (currentPrice !== null && priceChange) {
    fields.push(
      { name: '\u{1F4B5} Entry Price', value: `${(price * 100).toFixed(1)}\u00A2`, inline: true },
      { name: '\u{1F4B5} Current Price', value: `${(currentPrice * 100).toFixed(1)}\u00A2`, inline: true },
      { name: '\u{1F4CA} Price Change', value: priceChange.formatted, inline: true },
    );
  } else {
    fields.push({ name: '\u{1F4B5} Price', value: `${(price * 100).toFixed(1)}\u00A2`, inline: true });
  }

  fields.push({ name: '\u{1F3B2} Betting Odds', value: americanOdds, inline: true });

  // Action links
  const buttonText = `[\u{1F4CA} View Market](${marketUrl}) \u2022 [\u{1F464} Trader Profile](${traderUrl})`;
  fields.unshift({ name: '\u{1F517} Quick Actions', value: buttonText, inline: false });

  // Build the embed once
  const embed = createEmbed({
    title: '\u{1F514} New Whale Bet Detected!',
    description: `**${traderName}** placed a new bet${priceChange ? `\n${priceChange.formatted} since entry` : ''}`,
    color: COLORS.POLYMARKET,
    fields,
    thumbnail: marketImage || undefined,
    footer: { text: 'Soldi \u2022 Polymarket Whale Tracker' },
  });

  // Route notification: public channel for defaults, DM for custom, both if overlap
  const isDefault = TARGET_ADDRESSES.includes(addr);
  const trackedBy = addressToUsers.get(addr) || [];

  // Post to public channel if it's a default whale
  if (isDefault && POLYMARKET_CHANNEL_ID) {
    await sendNotification(POLYMARKET_CHANNEL_ID, null, [embed]);
  }

  // DM users who track this address
  for (const { dmChannelId } of trackedBy) {
    if (dmChannelId) {
      await sendNotification(dmChannelId, null, [embed]);
    }
  }

  processedBets.add(betId);

  // Keep set from growing unbounded
  if (processedBets.size > 5000) {
    const arr = [...processedBets];
    arr.splice(0, arr.length - 2000);
    processedBets.clear();
    arr.forEach(id => processedBets.add(id));
  }
}

// ============================================
// CHECK ALL TRADERS (defaults + user-tracked)
// ============================================
async function checkAllTraders() {
  // Periodically reload user tracking data
  maybeReloadTracking();

  for (const addr of allAddresses) {
    try {
      await checkTrader(addr);
    } catch (err) {
      if (err.status !== 404) {
        console.error(`  Error ${addr.slice(0, 10)}...: ${err.message}`);
      }
    }
    // Small delay between traders to avoid rate limits
    await new Promise(r => setTimeout(r, 200));
  }
}

// ============================================
// MAIN
// ============================================
async function main() {
  if (!POLYMARKET_CHANNEL_ID) {
    console.error('\nMissing POLYMARKET_CHANNEL_ID in .env');
    console.error('Add: POLYMARKET_CHANNEL_ID=your_channel_id_here\n');
    process.exit(1);
  }

  // Load user-customized tracking data
  const customCount = loadUserTrackingData();
  const userCount = Object.keys(userTrackingData).length;

  console.log('\n' + '='.repeat(60));
  console.log('  SOLDI POLYMARKET WHALE TRACKER');
  console.log('='.repeat(60));
  console.log(`  Default whales: ${TARGET_ADDRESSES.length}`);
  console.log(`  Custom tracked: ${customCount} address(es) from ${userCount} user(s)`);
  console.log(`  Total tracking: ${allAddresses.length} addresses`);
  console.log(`  Check interval: ${REFRESH_INTERVAL}s`);
  console.log(`  Discord channel: ${POLYMARKET_CHANNEL_ID}`);
  console.log('='.repeat(60));

  // Pre-load trader names (defaults only — custom will be cached on first check)
  console.log('\nLoading trader profiles...');
  for (let i = 0; i < TARGET_ADDRESSES.length; i++) {
    const name = await getProfileName(TARGET_ADDRESSES[i]);
    console.log(`  ${i + 1}. ${name}`);
    await new Promise(r => setTimeout(r, 150)); // gentle rate limit
  }

  // Pre-load existing bets to avoid spam on startup
  console.log('\nPre-loading existing bets...');
  let preloaded = 0;
  for (const addr of allAddresses) {
    try {
      const latest = await getLatestBet(addr);
      if (latest) {
        processedBets.add(getBetId(latest, addr));
        preloaded++;
      }
    } catch { /* ignore */ }
    await new Promise(r => setTimeout(r, 200));
  }
  console.log(`Pre-loaded ${preloaded} existing bets (will be skipped)\n`);

  // Startup notification
  const traderList = (await Promise.all(
    TARGET_ADDRESSES.slice(0, 10).map(async (addr, i) => `${i + 1}. ${await getProfileName(addr)}`)
  )).join('\n') + (TARGET_ADDRESSES.length > 10 ? `\n... and ${TARGET_ADDRESSES.length - 10} more` : '');

  const startupFields = [
    { name: '\u{1F4CA} Default Whales', value: traderList.slice(0, 1000), inline: false },
  ];
  if (customCount > 0) {
    startupFields.push({ name: '\u{1F464} Custom Tracked', value: `${customCount} address(es) from ${userCount} user(s)`, inline: false });
  }

  await sendNotification(POLYMARKET_CHANNEL_ID, null, [createEmbed({
    title: '\u{1F916} Whale Tracker Started',
    description: `Now monitoring **${allAddresses.length} total addresses**\nChecking every **${REFRESH_INTERVAL}s**`,
    color: COLORS.POLYMARKET,
    fields: startupFields,
    footer: { text: 'Soldi \u2022 Polymarket Whale Tracker' },
  })]);

  // Main loop
  console.log('Starting monitoring loop...');
  console.log(`Checking every ${REFRESH_INTERVAL} seconds`);
  console.log('Press Ctrl+C to stop\n');

  let checkCount = 0;
  const startTime = Date.now();

  const loop = async () => {
    while (true) {
      checkCount++;
      const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
      process.stdout.write(`[${ts}] Check #${checkCount} ... `);

      try {
        await checkAllTraders();
        console.log('done');
      } catch (err) {
        console.log(`error: ${err.message}`);
      }

      await new Promise(r => setTimeout(r, REFRESH_INTERVAL * 1000));
    }
  };

  // Graceful shutdown
  process.on('SIGINT', async () => {
    const runtime = ((Date.now() - startTime) / 1000).toFixed(0);
    console.log(`\n\nStopping whale tracker...`);
    console.log(`  Checks: ${checkCount} | Runtime: ${runtime}s`);

    await sendNotification(POLYMARKET_CHANNEL_ID, null, [createEmbed({
      title: '\u{1F6D1} Whale Tracker Stopped',
      description: 'Polymarket monitoring has been stopped',
      color: COLORS.RED,
      fields: [
        { name: 'Total Checks', value: String(checkCount), inline: true },
        { name: 'Runtime', value: `${runtime}s`, inline: true },
        { name: 'Addresses Tracked', value: String(allAddresses.length), inline: true },
      ],
      footer: { text: 'Soldi \u2022 Polymarket Whale Tracker' },
    })]);

    process.exit(0);
  });

  await loop();
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
