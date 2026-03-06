// ============================================
// SET SOLDI BOT AVATAR & USERNAME
// Run once: node set-bot-avatar.js
// ============================================

require('dotenv').config();
const fs = require('fs');
const path = require('path');

const BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;

if (!BOT_TOKEN) {
  console.error('❌ Missing DISCORD_BOT_TOKEN in .env');
  process.exit(1);
}

async function setBotProfile() {
  // Read the Soldi logo
  const logoPath = path.join(__dirname, 'soldi-profile-400x400.png');
  if (!fs.existsSync(logoPath)) {
    console.error('❌ Logo not found at:', logoPath);
    console.error('   Make sure soldi-profile-400x400.png exists in the project root.');
    process.exit(1);
  }

  const imageBuffer = fs.readFileSync(logoPath);
  const base64Image = `data:image/png;base64,${imageBuffer.toString('base64')}`;

  console.log('📷 Setting bot avatar and username...\n');

  // Update bot profile via Discord API
  try {
    const res = await fetch('https://discord.com/api/v10/users/@me', {
      method: 'PATCH',
      headers: {
        'Authorization': `Bot ${BOT_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        username: 'Soldi',
        avatar: base64Image,
      }),
    });

    if (!res.ok) {
      const err = await res.json();
      if (err.retry_after) {
        console.error(`⏳ Rate limited. Try again in ${Math.ceil(err.retry_after)} seconds.`);
      } else {
        console.error('❌ Discord API error:', JSON.stringify(err, null, 2));
      }
      process.exit(1);
    }

    const user = await res.json();
    console.log('✅ Bot profile updated successfully!');
    console.log(`   Username: ${user.username}`);
    console.log(`   Avatar:   ${user.avatar ? 'Set ✓' : 'Not set'}`);
    console.log(`   ID:       ${user.id}`);
    console.log(`\n   Avatar URL: https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png?size=512\n`);

  } catch (err) {
    console.error('❌ Failed to update bot profile:', err.message);
    process.exit(1);
  }
}

setBotProfile();
