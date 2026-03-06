// ============================================
// SOLDI POLYMARKET BOT
// Posts Polymarket odds/alerts to Discord
// ============================================
//
// PLACEHOLDER — Paste your Polymarket bot code here.
// This file already imports the shared Soldi Discord utility
// so all notifications will be sent as "Soldi" with the logo.
//
// Usage: node polymarket-bot.js
//

require('dotenv').config();
const { sendNotification, sendEmbed, createEmbed, COLORS } = require('./discord-utils');

// TODO: Add your Polymarket channel ID here
const POLYMARKET_CHANNEL_ID = process.env.POLYMARKET_CHANNEL_ID || '';

// TODO: Paste your Polymarket bot code below
// Use these helpers to send Discord messages:
//
//   // Simple message:
//   await sendNotification(POLYMARKET_CHANNEL_ID, 'Market update!');
//
//   // Rich embed:
//   await sendEmbed(POLYMARKET_CHANNEL_ID, {
//     title: 'Market Alert',
//     description: 'BTC > $100k — 75% chance',
//     color: COLORS.POLYMARKET,
//     fields: [
//       { name: 'Yes', value: '75%', inline: true },
//       { name: 'No', value: '25%', inline: true },
//     ],
//     footer: { text: 'Soldi • Polymarket' },
//   });
//

console.log('⚠️  Polymarket bot placeholder — paste your code into polymarket-bot.js');
