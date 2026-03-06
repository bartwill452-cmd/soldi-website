// ============================================
// SOLDI TWITTER/X NOTIFICATION BOT
// Posts Twitter/X updates to Discord
// ============================================
//
// PLACEHOLDER — Paste your Twitter notification bot code here.
// This file already imports the shared Soldi Discord utility
// so all notifications will be sent as "Soldi" with the logo.
//
// Usage: node twitter-bot.js
//

require('dotenv').config();
const { sendNotification, sendEmbed, createEmbed, COLORS } = require('./discord-utils');

// TODO: Add your Twitter notification channel ID here
const TWITTER_CHANNEL_ID = process.env.TWITTER_CHANNEL_ID || '';

// TODO: Paste your Twitter bot code below
// Use these helpers to send Discord messages:
//
//   // Simple message:
//   await sendNotification(TWITTER_CHANNEL_ID, 'New tweet alert!');
//
//   // Rich embed:
//   await sendEmbed(TWITTER_CHANNEL_ID, {
//     title: 'New Tweet from @willbart_4',
//     description: 'Tweet content here...',
//     url: 'https://x.com/willbart_4/status/123',
//     color: COLORS.TWITTER,
//     footer: { text: 'Soldi • Twitter/X' },
//   });
//

console.log('⚠️  Twitter bot placeholder — paste your code into twitter-bot.js');
