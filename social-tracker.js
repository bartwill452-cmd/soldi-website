// ============================================
// SOLDI SOCIAL MEDIA TRACKER
// Posts YouTube & Instagram updates to Discord
// ============================================
//
// Runs alongside discord-bot.js
// Usage: node social-tracker.js
//

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { sendNotification, sendEmbed, createEmbed, COLORS } = require('./discord-utils');

const BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;
const GUILD_ID = process.env.DISCORD_GUILD_ID;

// Channel IDs (created by setup)
const YT_CHANNEL_ID = '1479289498700353536';
const IG_CHANNEL_ID = '1479289504987877638';

// Social media sources
const YOUTUBE_CHANNEL_ID = 'UCx-1MD5R1foM1dJ37PtEyJg';
const YOUTUBE_RSS = `https://www.youtube.com/feeds/videos.xml?channel_id=${YOUTUBE_CHANNEL_ID}`;
const INSTAGRAM_USERNAME = 'willbart_4';

// Check intervals
const YT_CHECK_INTERVAL = 5 * 60 * 1000;  // 5 minutes
const IG_CHECK_INTERVAL = 10 * 60 * 1000; // 10 minutes

// State file to track what we've already posted
const STATE_FILE = path.join(__dirname, 'data', 'social-tracker-state.json');

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return { lastYTVideoId: null, lastIGPostId: null, postedYTIds: [], postedIGIds: [] };
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {
    return { lastYTVideoId: null, lastIGPostId: null, postedYTIds: [], postedIGIds: [] };
  }
}

function saveState(state) {
  const dir = path.dirname(STATE_FILE);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// Discord messaging now handled by discord-utils.js (sendNotification, sendEmbed)

// ============================================
// YOUTUBE TRACKER (RSS Feed)
// ============================================
async function checkYouTube() {
  try {
    const res = await fetch(YOUTUBE_RSS);
    if (!res.ok) {
      console.error('YouTube RSS error:', res.status);
      return;
    }

    const xml = await res.text();

    // Parse entries from XML
    const entries = [];
    const entryRegex = /<entry>([\s\S]*?)<\/entry>/g;
    let match;
    while ((match = entryRegex.exec(xml)) !== null) {
      const entry = match[1];
      const videoId = entry.match(/<yt:videoId>(.*?)<\/yt:videoId>/)?.[1];
      const title = entry.match(/<title>(.*?)<\/title>/)?.[1];
      const published = entry.match(/<published>(.*?)<\/published>/)?.[1];
      const channelName = entry.match(/<name>(.*?)<\/name>/)?.[1];
      const thumbnail = videoId ? `https://img.youtube.com/vi/${videoId}/maxresdefault.jpg` : null;

      if (videoId && title) {
        entries.push({ videoId, title, published, channelName, thumbnail });
      }
    }

    if (entries.length === 0) return;

    const state = loadState();
    const newVideos = entries.filter(e => !state.postedYTIds.includes(e.videoId));

    // On first run, just record existing videos without posting
    if (!state.lastYTVideoId && state.postedYTIds.length === 0) {
      console.log(`[YouTube] First run — recording ${entries.length} existing videos`);
      state.postedYTIds = entries.map(e => e.videoId);
      state.lastYTVideoId = entries[0]?.videoId;
      saveState(state);
      return;
    }

    for (const video of newVideos.reverse()) { // oldest first
      console.log(`[YouTube] New video: ${video.title}`);

      const embed = createEmbed({
        title: video.title,
        url: `https://www.youtube.com/watch?v=${video.videoId}`,
        color: COLORS.YT_RED,
        author: {
          name: video.channelName || 'Will Bart',
          url: `https://www.youtube.com/@WillBart_4`,
          icon_url: 'https://www.youtube.com/s/desktop/f506bd45/img/favicon_32x32.png'
        },
        image: video.thumbnail || undefined,
        footer: { text: 'Soldi • YouTube' },
      });
      if (video.published) embed.timestamp = video.published;

      await sendNotification(YT_CHANNEL_ID,
        `**New YouTube Video!** 🎬\nCheck out the latest upload from Will Bart:\nhttps://www.youtube.com/watch?v=${video.videoId}`,
        [embed]
      );

      state.postedYTIds.push(video.videoId);
      // Keep only last 100 IDs
      if (state.postedYTIds.length > 100) {
        state.postedYTIds = state.postedYTIds.slice(-100);
      }
    }

    if (newVideos.length > 0) {
      state.lastYTVideoId = entries[0].videoId;
      saveState(state);
    }

  } catch (err) {
    console.error('[YouTube] Check error:', err.message);
  }
}

// ============================================
// INSTAGRAM TRACKER (Public Profile Scraping)
// ============================================
async function checkInstagram() {
  try {
    // Use Instagram's public profile endpoint
    const res = await fetch(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${INSTAGRAM_USERNAME}`, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'X-IG-App-ID': '936619743392459',
        'X-Requested-With': 'XMLHttpRequest'
      }
    });

    if (!res.ok) {
      // Instagram blocks scraping frequently - try alternative approach
      console.log(`[Instagram] API returned ${res.status} — trying RSS bridge...`);
      await checkInstagramViaRSS();
      return;
    }

    const data = await res.json();
    const user = data?.data?.user;
    if (!user) return;

    const edges = user.edge_owner_to_timeline_media?.edges || [];
    if (edges.length === 0) return;

    const state = loadState();
    const posts = edges.map(e => ({
      id: e.node.shortcode,
      caption: e.node.edge_media_to_caption?.edges?.[0]?.node?.text || '',
      thumbnail: e.node.thumbnail_src || e.node.display_url,
      timestamp: e.node.taken_at_timestamp,
      isVideo: e.node.is_video
    }));

    const newPosts = posts.filter(p => !state.postedIGIds.includes(p.id));

    // First run — record existing
    if (!state.lastIGPostId && state.postedIGIds.length === 0) {
      console.log(`[Instagram] First run — recording ${posts.length} existing posts`);
      state.postedIGIds = posts.map(p => p.id);
      state.lastIGPostId = posts[0]?.id;
      saveState(state);
      return;
    }

    for (const post of newPosts.reverse()) {
      console.log(`[Instagram] New post: ${post.id}`);

      const caption = post.caption.length > 200 ? post.caption.substring(0, 200) + '...' : post.caption;
      const embed = createEmbed({
        title: post.isVideo ? 'New Instagram Reel' : 'New Instagram Post',
        url: `https://www.instagram.com/p/${post.id}/`,
        description: caption || undefined,
        color: COLORS.PINK,
        author: {
          name: '@willbart_4',
          url: 'https://www.instagram.com/willbart_4/',
          icon_url: 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a5/Instagram_icon.png/600px-Instagram_icon.png'
        },
        image: post.thumbnail || undefined,
        footer: { text: 'Soldi • Instagram' },
      });
      if (post.timestamp) embed.timestamp = new Date(post.timestamp * 1000).toISOString();

      await sendNotification(IG_CHANNEL_ID,
        `**New Instagram Post!** 📸\nCheck out the latest from @willbart_4:\nhttps://www.instagram.com/p/${post.id}/`,
        [embed]
      );

      state.postedIGIds.push(post.id);
      if (state.postedIGIds.length > 100) {
        state.postedIGIds = state.postedIGIds.slice(-100);
      }
    }

    if (newPosts.length > 0) {
      state.lastIGPostId = posts[0].id;
      saveState(state);
    }

  } catch (err) {
    console.error('[Instagram] Check error:', err.message);
    // Fallback to RSS bridge
    await checkInstagramViaRSS();
  }
}

// Fallback: Use RSS bridge services for Instagram
async function checkInstagramViaRSS() {
  try {
    // Try multiple RSS bridge services
    const bridges = [
      `https://rsshub.app/instagram/user/${INSTAGRAM_USERNAME}`,
      `https://rss.app/feeds/v1.1/instagram/${INSTAGRAM_USERNAME}.json`,
    ];

    for (const bridgeUrl of bridges) {
      try {
        const res = await fetch(bridgeUrl, {
          headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SoldiBot/1.0)' },
          signal: AbortSignal.timeout(10000)
        });

        if (!res.ok) continue;

        const contentType = res.headers.get('content-type') || '';

        if (contentType.includes('json')) {
          const data = await res.json();
          const items = data.items || [];
          if (items.length === 0) continue;

          const state = loadState();
          const newPosts = items.filter(item => {
            const id = item.id || item.url?.split('/p/')?.[1]?.replace('/', '') || item.title;
            return id && !state.postedIGIds.includes(id);
          });

          if (!state.lastIGPostId && state.postedIGIds.length === 0) {
            console.log(`[Instagram/RSS] First run — recording ${items.length} existing posts`);
            state.postedIGIds = items.map(item => item.id || item.url?.split('/p/')?.[1]?.replace('/', '') || item.title).filter(Boolean);
            state.lastIGPostId = state.postedIGIds[0];
            saveState(state);
            return;
          }

          for (const item of newPosts.reverse()) {
            const id = item.id || item.url?.split('/p/')?.[1]?.replace('/', '') || item.title;
            console.log(`[Instagram/RSS] New post: ${id}`);

            await sendNotification(IG_CHANNEL_ID,
              `**New Instagram Post!** 📸\nCheck out the latest from @willbart_4:\n${item.url || `https://www.instagram.com/willbart_4/`}`,
              [createEmbed({
                title: item.title || 'New Instagram Post',
                url: item.url || `https://www.instagram.com/willbart_4/`,
                description: item.content_text ? item.content_text.substring(0, 200) : undefined,
                color: COLORS.PINK,
                author: { name: '@willbart_4', url: 'https://www.instagram.com/willbart_4/' },
                image: item.image || undefined,
                footer: { text: 'Soldi • Instagram' },
              })]
            );

            state.postedIGIds.push(id);
          }

          if (newPosts.length > 0) saveState(state);
          return; // Success with this bridge
        }

        // XML RSS feed
        const xml = await res.text();
        const items = [];
        const itemRegex = /<item>([\s\S]*?)<\/item>/g;
        let m;
        while ((m = itemRegex.exec(xml)) !== null) {
          const itemXml = m[1];
          const link = itemXml.match(/<link>(.*?)<\/link>/)?.[1];
          const title = itemXml.match(/<title>(.*?)<\/title>/)?.[1];
          const id = link?.split('/p/')?.[1]?.replace('/', '') || title;
          items.push({ id, link, title });
        }

        if (items.length > 0) {
          const state = loadState();
          const newPosts = items.filter(item => item.id && !state.postedIGIds.includes(item.id));

          if (!state.lastIGPostId && state.postedIGIds.length === 0) {
            state.postedIGIds = items.map(i => i.id).filter(Boolean);
            state.lastIGPostId = state.postedIGIds[0];
            saveState(state);
            return;
          }

          for (const item of newPosts.reverse()) {
            await sendNotification(IG_CHANNEL_ID,
              `**New Instagram Post!** 📸\n${item.link || 'https://www.instagram.com/willbart_4/'}`
            );
            state.postedIGIds.push(item.id);
          }
          if (newPosts.length > 0) saveState(state);
          return;
        }

      } catch (bridgeErr) {
        continue; // Try next bridge
      }
    }

    console.log('[Instagram] All RSS bridges failed — will retry next cycle');

  } catch (err) {
    console.error('[Instagram/RSS] Error:', err.message);
  }
}

// ============================================
// MAIN LOOP
// ============================================
async function main() {
  if (!BOT_TOKEN || BOT_TOKEN === 'YOUR_DISCORD_BOT_TOKEN_HERE') {
    console.error('Missing DISCORD_BOT_TOKEN in .env');
    process.exit(1);
  }

  console.log('\n=== Soldi Social Media Tracker ===');
  console.log(`YouTube Channel: ${YOUTUBE_CHANNEL_ID}`);
  console.log(`Instagram User:  @${INSTAGRAM_USERNAME}`);
  console.log(`YT Check Every:  ${YT_CHECK_INTERVAL / 60000} minutes`);
  console.log(`IG Check Every:  ${IG_CHECK_INTERVAL / 60000} minutes`);
  console.log('');

  // Initial checks
  console.log('[YouTube] Running initial check...');
  await checkYouTube();

  console.log('[Instagram] Running initial check...');
  await checkInstagram();

  console.log('\nTracker running. Monitoring for new posts...\n');

  // Set up intervals
  setInterval(checkYouTube, YT_CHECK_INTERVAL);
  setInterval(checkInstagram, IG_CHECK_INTERVAL);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
