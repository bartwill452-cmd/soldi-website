// ============================================
// SOLDI DISCORD UTILITIES
// Shared module for all bots to send messages
// as "Soldi" with the company logo
// ============================================
//
// Usage:
//   const { sendNotification, sendEmbed, COLORS } = require('./discord-utils');
//
//   // Simple message
//   await sendNotification(channelId, 'Hello from Soldi!');
//
//   // Rich embed
//   await sendEmbed(channelId, {
//     title: 'New Alert',
//     description: 'Something happened!',
//     color: COLORS.GREEN,
//   });
//
//   // With webhook (custom name/avatar per message)
//   await sendWebhookMessage(webhookUrl, {
//     content: 'Hello!',
//     embeds: [{ title: 'Test' }]
//   });
//

require('dotenv').config();

const BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;

// Soldi brand colors
const COLORS = {
  GREEN: 0x10B981,     // Primary Soldi green
  BLUE: 0x3B82F6,      // Info blue
  PURPLE: 0xA855F7,    // AI tools purple
  RED: 0xEF4444,       // Error/alert red
  YELLOW: 0xEAB308,    // Warning yellow
  PINK: 0xE1306C,      // Instagram pink
  YT_RED: 0xFF0000,    // YouTube red
  TWITTER: 0x1DA1F2,   // Twitter/X blue
  POLYMARKET: 0x0066FF, // Polymarket blue
};

// ============================================
// SEND MESSAGE VIA BOT TOKEN
// All messages appear as "Soldi" bot with logo
// ============================================
async function sendNotification(channelId, content, embeds = null) {
  if (!BOT_TOKEN) {
    console.error('[discord-utils] Missing DISCORD_BOT_TOKEN in .env');
    return null;
  }

  const body = {};
  if (content) body.content = content;
  if (embeds) body.embeds = Array.isArray(embeds) ? embeds : [embeds];

  try {
    const res = await fetch(`https://discord.com/api/v10/channels/${channelId}/messages`, {
      method: 'POST',
      headers: {
        'Authorization': `Bot ${BOT_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      console.error(`[discord-utils] Send error (${res.status}):`, err);
      return null;
    }

    return await res.json();
  } catch (err) {
    console.error('[discord-utils] Send error:', err.message);
    return null;
  }
}

// ============================================
// SEND RICH EMBED (convenience wrapper)
// ============================================
async function sendEmbed(channelId, embed, content = null) {
  // Default footer to "Soldi" if not set
  if (!embed.footer) {
    embed.footer = { text: 'Soldi' };
  }
  // Default color to Soldi green if not set
  if (!embed.color) {
    embed.color = COLORS.GREEN;
  }
  // Default timestamp to now if not set
  if (!embed.timestamp) {
    embed.timestamp = new Date().toISOString();
  }

  return sendNotification(channelId, content, [embed]);
}

// ============================================
// SEND VIA WEBHOOK URL
// Allows custom username/avatar per message
// Use this for external webhooks
// ============================================
async function sendWebhookMessage(webhookUrl, { content, embeds, username, avatarUrl }) {
  const body = {};
  if (content) body.content = content;
  if (embeds) body.embeds = Array.isArray(embeds) ? embeds : [embeds];

  // Override webhook identity to Soldi branding
  body.username = username || 'Soldi';
  body.avatar_url = avatarUrl || null; // Will use webhook default if null

  try {
    const res = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      console.error(`[discord-utils] Webhook error (${res.status}):`, err);
      return null;
    }

    // Webhooks return 204 No Content on success
    if (res.status === 204) return { ok: true };
    return await res.json();
  } catch (err) {
    console.error('[discord-utils] Webhook error:', err.message);
    return null;
  }
}

// ============================================
// CREATE SOLDI-BRANDED EMBED
// Helper to build consistent embeds
// ============================================
function createEmbed({ title, description, url, color, fields, image, thumbnail, author, footer }) {
  const embed = {};
  if (title) embed.title = title;
  if (description) embed.description = description;
  if (url) embed.url = url;
  embed.color = color || COLORS.GREEN;
  if (fields) embed.fields = fields;
  if (image) embed.image = { url: image };
  if (thumbnail) embed.thumbnail = { url: thumbnail };
  if (author) embed.author = author;
  embed.footer = footer || { text: 'Soldi' };
  embed.timestamp = new Date().toISOString();
  return embed;
}

module.exports = {
  sendNotification,
  sendEmbed,
  sendWebhookMessage,
  createEmbed,
  COLORS,
  BOT_TOKEN,
};
