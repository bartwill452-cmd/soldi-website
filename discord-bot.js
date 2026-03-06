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

client.once('ready', () => {
  console.log(`\n✅ Soldi Bot is online as ${client.user.tag}`);
  console.log(`   Watching for !verify commands and new member joins\n`);
});

// When a new member joins
client.on('guildMemberAdd', async (member) => {
  console.log(`👋 New member joined: ${member.user.tag}`);

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
