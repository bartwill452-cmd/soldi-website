// ============================================
// SOLDI DISCORD AUTO-ROLE BOT
// Assigns "Soldi Paid Member" role to verified Whop members
// ============================================
//
// SETUP INSTRUCTIONS:
// 1. Go to https://discord.com/developers/applications
// 2. Click "New Application" and name it "Soldi Bot"
// 3. Go to "Bot" tab, click "Add Bot"
// 4. Copy the Bot Token and paste it in .env as DISCORD_BOT_TOKEN
// 5. Enable these Privileged Gateway Intents:
//    - SERVER MEMBERS INTENT
//    - MESSAGE CONTENT INTENT
// 6. Go to OAuth2 > URL Generator
//    - Select scopes: bot
//    - Select permissions: Manage Roles, Read Messages/View Channels, Send Messages
//    - Copy the generated URL and open it to invite the bot to your server
// 7. In your Discord server:
//    - Create a role called "Soldi Paid Member" (or whatever you want)
//    - Make sure the bot's role is ABOVE "Soldi Paid Member" in the role hierarchy
//    - Copy the role ID (right-click role > Copy Role ID, enable Developer Mode in settings)
//    - Paste it in .env as DISCORD_ROLE_ID
// 8. Run: node discord-bot.js
//
// HOW IT WORKS:
// - When a user joins the server, the bot DMs them asking for their Whop email
// - User replies with their email
// - Bot checks the Whop API for an active membership
// - If verified, bot assigns "Soldi Paid Member" role
// - Users can also type !verify in any channel to start the process
//

require('dotenv').config();
const fs = require('fs');
const path = require('path');

// Check if discord.js is installed
try {
  require.resolve('discord.js');
} catch (e) {
  console.error('\n❌ discord.js is not installed. Run:\n');
  console.error('   npm install discord.js\n');
  console.error('Then run this bot again with: node discord-bot.js\n');
  process.exit(1);
}

const { Client, GatewayIntentBits, Partials } = require('discord.js');
const { sendEmbed, createEmbed, COLORS } = require('./discord-utils');

const DISCORD_TOKEN = process.env.DISCORD_BOT_TOKEN;
const ROLE_ID = process.env.DISCORD_ROLE_ID;
const GUILD_ID = process.env.DISCORD_GUILD_ID;
const WHOP_API_KEY = process.env.WHOP_API_KEY;
const WHOP_COMPANY_ID = process.env.WHOP_COMPANY_ID;

if (!DISCORD_TOKEN || DISCORD_TOKEN === 'YOUR_DISCORD_BOT_TOKEN_HERE') {
  console.error('❌ Missing DISCORD_BOT_TOKEN in .env');
  process.exit(1);
}
if (!ROLE_ID || ROLE_ID === 'YOUR_ROLE_ID_HERE') {
  console.error('❌ Missing DISCORD_ROLE_ID in .env');
  process.exit(1);
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.DirectMessages,
  ],
  partials: [Partials.Channel, Partials.Message],
});

// Track pending verifications: { discordUserId: { guildId, timestamp } }
const pendingVerifications = new Map();

// ============================================
// POLYMARKET USER TRACKING
// ============================================
const USER_TRACKING_FILE = path.join(__dirname, 'data', 'user-tracking.json');
const MAX_TRACKED_PER_USER = 50;

// Default whale addresses (same list as polymarket-bot.js)
const DEFAULT_WHALES = [
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

function loadUserTracking() {
  try {
    if (!fs.existsSync(USER_TRACKING_FILE)) return {};
    return JSON.parse(fs.readFileSync(USER_TRACKING_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function saveUserTracking(data) {
  const dir = path.dirname(USER_TRACKING_FILE);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(USER_TRACKING_FILE, JSON.stringify(data, null, 2));
}

function isValidEthAddress(addr) {
  return /^0x[a-fA-F0-9]{40}$/.test(addr);
}

client.once('ready', () => {
  console.log(`\n✅ Soldi Bot is online as ${client.user.tag}`);
  console.log(`   Watching for !verify commands and new member joins\n`);
});

// When a new member joins
const FREE_MEMBER_ROLE_ID = '1467955361309917478';

client.on('guildMemberAdd', async (member) => {
  console.log(`👋 New member joined: ${member.user.tag}`);

  // Auto-assign Free Member role (they'll upgrade to Paid Member after verification)
  try {
    if (!member.user.bot) {
      await member.roles.add(FREE_MEMBER_ROLE_ID, 'Auto-assigned on join');
      console.log(`  ✓ Assigned Free Member role to ${member.user.tag}`);
    }
  } catch (err) {
    console.log(`  Could not assign Free Member role: ${err.message}`);
  }

  try {
    const dm = await member.user.createDM();
    await dm.send(
      `**Welcome to Soldi!** 🎉\n\n` +
      `To get your **Soldi Paid Member** role, please reply with the email address you used to purchase your Whop membership.\n\n` +
      `Example: \`your@email.com\`\n\n` +
      `This will verify your membership and give you full access to all member channels.`
    );

    pendingVerifications.set(member.user.id, {
      guildId: member.guild.id,
      timestamp: Date.now(),
    });

    // Expire after 30 minutes
    setTimeout(() => {
      pendingVerifications.delete(member.user.id);
    }, 30 * 60 * 1000);

  } catch (err) {
    console.log(`  Could not DM ${member.user.tag} (DMs may be disabled)`);
  }
});

// Listen for messages (both DMs and guild)
client.on('messageCreate', async (message) => {
  if (message.author.bot) return;

  // Handle !verify command in a guild channel
  if (message.guild && message.content.toLowerCase() === '!verify') {
    try {
      const dm = await message.author.createDM();
      await dm.send(
        `**Soldi Membership Verification** 🔐\n\n` +
        `Please reply with the email address you used to purchase your Whop membership.\n\n` +
        `Example: \`your@email.com\``
      );

      pendingVerifications.set(message.author.id, {
        guildId: message.guild.id,
        timestamp: Date.now(),
      });

      setTimeout(() => {
        pendingVerifications.delete(message.author.id);
      }, 30 * 60 * 1000);

      await message.reply('📩 Check your DMs! I sent you a verification message.');
    } catch (err) {
      await message.reply('❌ I couldn\'t send you a DM. Please enable DMs from server members and try again.');
    }
    return;
  }

  // ============================================
  // POLYMARKET TRACKING COMMANDS (DMs)
  // ============================================
  if (!message.guild) {
    const content = message.content.trim();
    const lower = content.toLowerCase();

    // !track <address> — Add a wallet to track
    if (lower.startsWith('!track ')) {
      const addr = content.split(/\s+/)[1]?.toLowerCase();
      if (!addr || !isValidEthAddress(addr)) {
        await message.reply(
          '**Invalid address.** Please provide a valid Ethereum address.\n' +
          'Example: `!track 0x1234567890abcdef1234567890abcdef12345678`'
        );
        return;
      }

      const tracking = loadUserTracking();
      const userId = message.author.id;
      if (!tracking[userId]) {
        tracking[userId] = { addresses: [], dmChannelId: message.channel.id, updatedAt: new Date().toISOString() };
      }

      if (tracking[userId].addresses.includes(addr)) {
        await message.reply(`You're already tracking \`${addr.slice(0, 8)}...${addr.slice(-4)}\`.`);
        return;
      }

      if (tracking[userId].addresses.length >= MAX_TRACKED_PER_USER) {
        await message.reply(`You've reached the max of **${MAX_TRACKED_PER_USER}** tracked addresses. Remove one first with \`!untrack <address>\`.`);
        return;
      }

      tracking[userId].addresses.push(addr);
      tracking[userId].dmChannelId = message.channel.id;
      tracking[userId].updatedAt = new Date().toISOString();
      saveUserTracking(tracking);

      // Try to get trader name from Polymarket
      let traderName = `${addr.slice(0, 8)}...${addr.slice(-4)}`;
      try {
        const profile = await fetch(`https://gamma-api.polymarket.com/public-profile?address=${addr}`, {
          headers: { 'User-Agent': 'SoldiBot/1.0' },
        });
        if (profile.ok) {
          const data = await profile.json();
          if (data.name || data.pseudonym) traderName = data.name || data.pseudonym;
        }
      } catch { /* ignore */ }

      const isDefault = DEFAULT_WHALES.includes(addr);

      await sendEmbed(message.channel.id, {
        title: 'Address Tracked',
        description: `Now tracking **${traderName}**\n\`${addr}\`\n\n${isDefault ? 'This is also a default whale — you\'ll get DM alerts for their bets.' : 'You\'ll receive DM alerts when this address places a bet on Polymarket.'}`,
        color: COLORS.POLYMARKET,
        fields: [
          { name: 'Your Tracked', value: `${tracking[userId].addresses.length}/${MAX_TRACKED_PER_USER}`, inline: true },
        ],
        footer: { text: 'Soldi • Polymarket Tracker' },
      });

      console.log(`[Track] ${message.author.tag} added ${addr}`);
      return;
    }

    // !untrack <address> — Remove a tracked wallet
    if (lower.startsWith('!untrack ')) {
      const addr = content.split(/\s+/)[1]?.toLowerCase();
      if (!addr || !isValidEthAddress(addr)) {
        await message.reply('**Invalid address.** Example: `!untrack 0x1234...`');
        return;
      }

      const tracking = loadUserTracking();
      const userId = message.author.id;
      if (!tracking[userId] || !tracking[userId].addresses.includes(addr)) {
        await message.reply(`You're not tracking \`${addr.slice(0, 8)}...${addr.slice(-4)}\`.`);
        return;
      }

      tracking[userId].addresses = tracking[userId].addresses.filter(a => a !== addr);
      tracking[userId].updatedAt = new Date().toISOString();
      if (tracking[userId].addresses.length === 0) {
        delete tracking[userId];
      }
      saveUserTracking(tracking);

      await sendEmbed(message.channel.id, {
        title: 'Address Removed',
        description: `Stopped tracking \`${addr.slice(0, 8)}...${addr.slice(-4)}\``,
        color: COLORS.RED,
        footer: { text: 'Soldi • Polymarket Tracker' },
      });

      console.log(`[Untrack] ${message.author.tag} removed ${addr}`);
      return;
    }

    // !mytrackers — List user's tracked addresses
    if (lower === '!mytrackers') {
      const tracking = loadUserTracking();
      const userId = message.author.id;
      const userAddrs = tracking[userId]?.addresses || [];

      if (userAddrs.length === 0) {
        await message.reply(
          'You\'re not tracking any addresses yet.\n' +
          'Use `!track 0x...` to start tracking a Polymarket wallet.'
        );
        return;
      }

      const lines = userAddrs.map((addr, i) => {
        const isDefault = DEFAULT_WHALES.includes(addr);
        return `${i + 1}. \`${addr.slice(0, 8)}...${addr.slice(-4)}\`${isDefault ? ' (default whale)' : ''}`;
      });

      await sendEmbed(message.channel.id, {
        title: 'Your Tracked Addresses',
        description: lines.join('\n') + `\n\n**${userAddrs.length}/${MAX_TRACKED_PER_USER}** slots used`,
        color: COLORS.POLYMARKET,
        footer: { text: 'Soldi • Polymarket Tracker' },
      });
      return;
    }

    // !whales — Show default whale addresses
    if (lower === '!whales') {
      const lines = DEFAULT_WHALES.map((addr, i) =>
        `${i + 1}. \`${addr.slice(0, 8)}...${addr.slice(-4)}\``
      );

      // Split into 2 columns since there are 38 whales
      const half = Math.ceil(lines.length / 2);
      const col1 = lines.slice(0, half).join('\n');
      const col2 = lines.slice(half).join('\n');

      await sendEmbed(message.channel.id, {
        title: 'Default Whale Addresses',
        description: `**${DEFAULT_WHALES.length}** whale wallets are tracked by default in #polymarket-whale-tracker.`,
        color: COLORS.POLYMARKET,
        fields: [
          { name: 'Whales 1-' + half, value: col1, inline: true },
          { name: 'Whales ' + (half + 1) + '-' + lines.length, value: col2, inline: true },
        ],
        footer: { text: 'Soldi • Polymarket Tracker' },
      });
      return;
    }

    // !help — Show available commands
    if (lower === '!help') {
      await sendEmbed(message.channel.id, {
        title: 'Soldi Bot Commands',
        description: 'Send these commands via DM:',
        color: COLORS.GREEN,
        fields: [
          { name: '!verify', value: 'Verify your Whop membership (can also use in server)', inline: false },
          { name: '!track 0x...', value: 'Track a Polymarket wallet address (max 10)', inline: false },
          { name: '!untrack 0x...', value: 'Stop tracking an address', inline: false },
          { name: '!mytrackers', value: 'List your tracked addresses', inline: false },
          { name: '!whales', value: 'Show default whale addresses', inline: false },
        ],
        footer: { text: 'Soldi' },
      });
      return;
    }
  }

  // Handle DM replies (email verification)
  if (!message.guild && pendingVerifications.has(message.author.id)) {
    const email = message.content.trim().toLowerCase();

    // Basic email validation
    if (!email.match(/^[^\s@]+@[^\s@]+\.[^\s@]+$/)) {
      await message.reply('That doesn\'t look like a valid email. Please send just your email address (e.g., `your@email.com`).');
      return;
    }

    await message.reply('🔍 Checking your membership...');

    const result = await verifyWhopMembership(email);

    if (result.active) {
      const pending = pendingVerifications.get(message.author.id);
      const targetGuildId = pending?.guildId || GUILD_ID;

      try {
        const guild = await client.guilds.fetch(targetGuildId);
        const member = await guild.members.fetch(message.author.id);
        const role = guild.roles.cache.get(ROLE_ID);

        if (!role) {
          await message.reply('❌ Error: Could not find the member role. Please contact an admin.');
          console.error(`Role ${ROLE_ID} not found in guild ${targetGuildId}`);
          return;
        }

        await member.roles.add(role);
        // Remove Free Member role now that they're a Paid Member
        try { await member.roles.remove(FREE_MEMBER_ROLE_ID); } catch {}
        pendingVerifications.delete(message.author.id);

        await message.reply(
          `✅ **Verified!** Welcome to Soldi, ${message.author.username}!\n\n` +
          `Your **${role.name}** role has been assigned. You now have access to all member channels.\n\n` +
          `Head back to the server and check out the pinned posts to get started! 🚀`
        );

        console.log(`✅ Verified and assigned role to ${message.author.tag} (${email})`);
      } catch (err) {
        console.error('Role assignment error:', err.message);
        await message.reply('❌ Error assigning your role. Please contact an admin.');
      }
    } else {
      await message.reply(
        `❌ **No active membership found** for \`${email}\`.\n\n` +
        `${result.reason || 'Please make sure you\'re using the same email as your Whop purchase.'}\n\n` +
        `Not a member yet? Join here: https://whop.com/officialsoldi/officalsoldi/`
      );
    }
    return;
  }
});

// Verify membership against Whop API
async function verifyWhopMembership(email) {
  try {
    let page = 1;
    const maxPages = 20;

    while (page <= maxPages) {
      const url = `https://api.whop.com/api/v1/memberships?company_id=${WHOP_COMPANY_ID}&page=${page}&per=50`;
      const response = await fetch(url, {
        headers: { Authorization: `Bearer ${WHOP_API_KEY}` }
      });

      if (!response.ok) {
        console.error('Whop API error:', response.status);
        return { active: false, reason: 'Could not reach Whop API. Try again later.' };
      }

      const data = await response.json();
      const memberships = data.data || [];

      if (memberships.length === 0) break;

      const match = memberships.find(
        m => m.email && m.email.toLowerCase() === email.toLowerCase()
      );

      if (match) {
        const activeStatuses = ['active', 'trialing', 'canceling'];
        if (activeStatuses.includes(match.status)) {
          return { active: true };
        } else {
          return {
            active: false,
            reason: `Your membership status is **${match.status}**. Please reactivate to get verified.`
          };
        }
      }

      if (!data.pagination || page >= data.pagination.total_pages) break;
      page++;
    }

    return { active: false };
  } catch (err) {
    console.error('Verification error:', err);
    return { active: false, reason: 'Verification service error. Try again later.' };
  }
}

// Login
client.login(DISCORD_TOKEN);
