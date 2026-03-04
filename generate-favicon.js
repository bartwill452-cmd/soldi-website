const { createCanvas } = require('canvas');
const fs = require('fs');
const path = require('path');

const DIR = __dirname;

function greenGrad(ctx, x, y, w, h) {
  const g = ctx.createLinearGradient(x, y, x + w, y + h);
  g.addColorStop(0, '#22c55e');
  g.addColorStop(0.5, '#10b981');
  g.addColorStop(1, '#06d6a0');
  return g;
}

function renderFavicon(size) {
  const canvas = createCanvas(size, size);
  const ctx = canvas.getContext('2d');

  // Transparent bg
  ctx.clearRect(0, 0, size, size);

  // Green rounded square
  const pad = size * 0.06;
  const r = size * 0.22;
  const x = pad, y = pad, w = size - pad * 2, h = size - pad * 2;

  ctx.fillStyle = greenGrad(ctx, x, y, w, h);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + r);
  ctx.lineTo(x + w, y + h - r);
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
  ctx.lineTo(x + r, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
  ctx.fill();

  // Shine
  ctx.save();
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + r);
  ctx.lineTo(x + w, y + h - r);
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
  ctx.lineTo(x + r, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
  ctx.clip();
  const shineG = ctx.createLinearGradient(x, y, x, y + h * 0.5);
  shineG.addColorStop(0, 'rgba(255,255,255,0.12)');
  shineG.addColorStop(1, 'rgba(255,255,255,0)');
  ctx.fillStyle = shineG;
  ctx.fillRect(x + w * 0.12, y + 2, w * 0.76, h * 0.4);
  ctx.restore();

  // S letter
  ctx.fillStyle = '#050507';
  ctx.font = `900 ${Math.round(size * 0.58)}px sans-serif`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('S', size / 2, size / 2 + size * 0.02);

  return canvas;
}

function renderOGImage() {
  console.log('  Rendering: OG Image (1200x630)...');
  const W = 1200, H = 630;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  // Dark bg
  ctx.fillStyle = '#050507';
  ctx.fillRect(0, 0, W, H);

  // Grid
  ctx.strokeStyle = 'rgba(34,197,94,0.04)';
  ctx.lineWidth = 1;
  for (let x = 0; x < W; x += 60) {
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, H); ctx.stroke();
  }
  for (let y = 0; y < H; y += 60) {
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
  }

  // Orbs
  const orb1 = ctx.createRadialGradient(200, 150, 0, 200, 150, 350);
  orb1.addColorStop(0, 'rgba(34,197,94,0.15)');
  orb1.addColorStop(1, 'rgba(34,197,94,0)');
  ctx.fillStyle = orb1;
  ctx.fillRect(0, 0, 600, 500);

  const orb2 = ctx.createRadialGradient(W - 200, H - 100, 0, W - 200, H - 100, 300);
  orb2.addColorStop(0, 'rgba(16,185,129,0.1)');
  orb2.addColorStop(1, 'rgba(16,185,129,0)');
  ctx.fillStyle = orb2;
  ctx.fillRect(W - 500, H - 400, 500, 400);

  // Logo mark
  const logoSize = 100;
  const logoX = W / 2;
  const logoY = 180;
  const lr = logoSize * 0.23;
  const lx = logoX - logoSize / 2;
  const ly = logoY - logoSize / 2;

  ctx.fillStyle = greenGrad(ctx, lx, ly, logoSize, logoSize);
  ctx.beginPath();
  ctx.moveTo(lx + lr, ly);
  ctx.lineTo(lx + logoSize - lr, ly);
  ctx.quadraticCurveTo(lx + logoSize, ly, lx + logoSize, ly + lr);
  ctx.lineTo(lx + logoSize, ly + logoSize - lr);
  ctx.quadraticCurveTo(lx + logoSize, ly + logoSize, lx + logoSize - lr, ly + logoSize);
  ctx.lineTo(lx + lr, ly + logoSize);
  ctx.quadraticCurveTo(lx, ly + logoSize, lx, ly + logoSize - lr);
  ctx.lineTo(lx, ly + lr);
  ctx.quadraticCurveTo(lx, ly, lx + lr, ly);
  ctx.closePath();
  ctx.fill();

  ctx.fillStyle = '#050507';
  ctx.font = '900 62px sans-serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('S', logoX, logoY + 2);

  // Title
  ctx.fillStyle = '#ffffff';
  ctx.font = '900 64px sans-serif';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText('Soldi', W / 2, 310);

  // Tagline
  ctx.font = '500 26px sans-serif';
  ctx.fillStyle = '#a1a1aa';
  ctx.fillText('The All-In-One Blueprint to', W / 2 - 120, 360);

  ctx.fillStyle = greenGrad(ctx, W / 2 + 60, 340, 300, 30);
  ctx.font = '700 26px sans-serif';
  ctx.fillText('Make Money Online', W / 2 + 180, 360);

  // Stats row
  const statsY = 440;
  const stats = [
    { num: '$10M+', label: 'Member Revenue' },
    { num: '5+', label: 'Income Streams' },
    { num: '$349.99', label: '+ $19.99/mo' },
  ];
  const statGap = 200;
  const startX = W / 2 - statGap;

  stats.forEach((s, i) => {
    const sx = startX + i * statGap;
    ctx.fillStyle = greenGrad(ctx, sx - 50, statsY - 20, 100, 40);
    ctx.font = '800 36px sans-serif';
    ctx.fillText(s.num, sx, statsY);
    ctx.fillStyle = '#71717a';
    ctx.font = '500 14px sans-serif';
    ctx.fillText(s.label, sx, statsY + 24);
  });

  // Accent line
  ctx.fillStyle = greenGrad(ctx, 0, H - 4, W, 4);
  ctx.fillRect(0, H - 4, W, 4);

  // URL at bottom
  ctx.fillStyle = '#52525b';
  ctx.font = '500 16px sans-serif';
  ctx.fillText('whop.com/soldi-4def', W / 2, H - 30);

  return canvas.toBuffer('image/png');
}

// Generate all sizes
console.log('Generating favicons and OG image...\n');

const sizes = [16, 32, 48, 180, 192, 512];
sizes.forEach(size => {
  const canvas = renderFavicon(size);
  const filePath = path.join(DIR, 'images', `favicon-${size}x${size}.png`);
  fs.writeFileSync(filePath, canvas.toBuffer('image/png'));
  console.log(`  ✅ favicon-${size}x${size}.png`);
});

// Apple touch icon (180px)
const apple = renderFavicon(180);
fs.writeFileSync(path.join(DIR, 'images', 'apple-touch-icon.png'), apple.toBuffer('image/png'));
console.log('  ✅ apple-touch-icon.png');

// OG Image
const ogBuffer = renderOGImage();
fs.writeFileSync(path.join(DIR, 'images', 'og-image.png'), ogBuffer);
const ogSize = Math.round(ogBuffer.length / 1024);
console.log(`  ✅ og-image.png (${ogSize} KB)`);

// Web manifest
const manifest = {
  name: 'Soldi',
  short_name: 'Soldi',
  icons: [
    { src: '/images/favicon-192x192.png', sizes: '192x192', type: 'image/png' },
    { src: '/images/favicon-512x512.png', sizes: '512x512', type: 'image/png' },
  ],
  theme_color: '#22c55e',
  background_color: '#050507',
  display: 'standalone',
};
fs.writeFileSync(path.join(DIR, 'site.webmanifest'), JSON.stringify(manifest, null, 2));
console.log('  ✅ site.webmanifest');

console.log('\n🎉 All favicons, OG image, and manifest generated!');
