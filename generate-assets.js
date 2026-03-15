const { createCanvas, loadImage } = require('canvas');
const fs = require('fs');
const path = require('path');

const DIR = __dirname;

// ============= SHARED HELPERS =============
function roundRect(ctx, x, y, w, h, r) {
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
}

function greenGrad(ctx, x, y, w, h) {
  const g = ctx.createLinearGradient(x, y, x + w, y + h);
  g.addColorStop(0, '#22c55e');
  g.addColorStop(0.5, '#10b981');
  g.addColorStop(1, '#06d6a0');
  return g;
}

function drawBG(ctx, W, H) {
  // Dark bg
  ctx.fillStyle = '#050507';
  ctx.fillRect(0, 0, W, H);

  // Grid lines
  ctx.strokeStyle = 'rgba(34,197,94,0.04)';
  ctx.lineWidth = 1;
  for (let x = 0; x < W; x += 60) {
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, H); ctx.stroke();
  }
  for (let y = 0; y < H; y += 60) {
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
  }

  // Orb top-left
  const orbG1 = ctx.createRadialGradient(200, 150, 0, 200, 150, 400);
  orbG1.addColorStop(0, 'rgba(34,197,94,0.12)');
  orbG1.addColorStop(1, 'rgba(34,197,94,0)');
  ctx.fillStyle = orbG1;
  ctx.fillRect(0, 0, 700, 700);

  // Orb bottom-right
  const orbG2 = ctx.createRadialGradient(W - 200, H - 150, 0, W - 200, H - 150, 350);
  orbG2.addColorStop(0, 'rgba(16,185,129,0.08)');
  orbG2.addColorStop(1, 'rgba(16,185,129,0)');
  ctx.fillStyle = orbG2;
  ctx.fillRect(W - 600, H - 600, 600, 600);
}

function drawAccentLine(ctx, W, H) {
  ctx.fillStyle = greenGrad(ctx, 0, H - 4, W, 4);
  ctx.fillRect(0, H - 4, W, 4);
}

function drawLogoMark(ctx, cx, cy, size) {
  // Green rounded square with S
  const r = size * 0.23;
  const x = cx - size / 2;
  const y = cy - size / 2;
  ctx.fillStyle = greenGrad(ctx, x, y, size, size);
  roundRect(ctx, x, y, size, size, r);
  ctx.fill();

  // Shine overlay
  ctx.save();
  roundRect(ctx, x, y, size, size, r);
  ctx.clip();
  const shineG = ctx.createLinearGradient(x, y, x, y + size * 0.5);
  shineG.addColorStop(0, 'rgba(255,255,255,0.1)');
  shineG.addColorStop(1, 'rgba(255,255,255,0)');
  ctx.fillStyle = shineG;
  ctx.fillRect(x + size * 0.15, y + 5, size * 0.7, size * 0.4);
  ctx.restore();

  // S letter
  ctx.fillStyle = '#050507';
  ctx.font = `900 ${Math.round(size * 0.62)}px sans-serif`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('S', cx, cy + 2);
}

function drawContained(ctx, img, x, y, bw, bh) {
  if (!img) return;
  const scale = Math.min(bw / img.width, bh / img.height);
  const dw = img.width * scale;
  const dh = img.height * scale;
  const dx = x + (bw - dw) / 2;
  const dy = y + (bh - dh) / 2;
  ctx.drawImage(img, dx, dy, dw, dh);
}

function drawCheckmark(ctx, x, y, size) {
  ctx.save();
  ctx.strokeStyle = '#22c55e';
  ctx.lineWidth = 2;
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  ctx.beginPath();
  ctx.moveTo(x + size * 0.17, y + size * 0.5);
  ctx.lineTo(x + size * 0.38, y + size * 0.71);
  ctx.lineTo(x + size * 0.83, y + size * 0.25);
  ctx.stroke();
  ctx.restore();
}

async function loadImg(src) {
  try {
    return await loadImage(path.join(DIR, src));
  } catch {
    return null;
  }
}

// ============= ASSET 1: PROFILE PICTURE (400x400) =============
function renderProfilePic() {
  console.log('  Rendering: Profile Picture (400x400)...');
  const W = 400, H = 400;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  // Dark bg
  ctx.fillStyle = '#050507';
  ctx.fillRect(0, 0, W, H);

  // Radial glow
  const glow = ctx.createRadialGradient(120, 120, 0, 200, 200, 300);
  glow.addColorStop(0, 'rgba(34,197,94,0.15)');
  glow.addColorStop(1, 'rgba(34,197,94,0)');
  ctx.fillStyle = glow;
  ctx.fillRect(0, 0, W, H);

  // Logo box shadow glow
  const shadowG = ctx.createRadialGradient(W / 2, H / 2, 0, W / 2, H / 2, 180);
  shadowG.addColorStop(0, 'rgba(34,197,94,0.3)');
  shadowG.addColorStop(1, 'rgba(34,197,94,0)');
  ctx.fillStyle = shadowG;
  ctx.fillRect(0, 0, W, H);

  // Logo mark
  drawLogoMark(ctx, W / 2, H / 2, 260);

  return canvas.toBuffer('image/png');
}

// ============= ASSET 2A: BANNER CLEAN (1920x1080) =============
function renderBannerClean() {
  console.log('  Rendering: Banner Clean (1920x1080)...');
  const W = 1920, H = 1080;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  drawBG(ctx, W, H);

  // Center orb
  const orbC = ctx.createRadialGradient(W * 0.65, H * 0.4, 0, W * 0.65, H * 0.4, 300);
  orbC.addColorStop(0, 'rgba(6,214,160,0.06)');
  orbC.addColorStop(1, 'rgba(6,214,160,0)');
  ctx.fillStyle = orbC;
  ctx.fillRect(W * 0.3, 0, W * 0.6, H);

  // Left side: Logo mark
  const logoX = 240;
  const logoY = H / 2;
  drawLogoMark(ctx, logoX, logoY, 240);

  // Right side: Text
  const textX = 440;
  ctx.textBaseline = 'alphabetic';

  // "Soldi" brand
  ctx.fillStyle = '#ffffff';
  ctx.font = '900 96px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText('Soldi', textX, H / 2 - 60);

  // Tagline
  ctx.font = '500 32px sans-serif';
  ctx.fillStyle = '#a1a1aa';
  ctx.fillText('The All-In-One Blueprint to', textX, H / 2 + 0);

  ctx.fillStyle = greenGrad(ctx, textX, H / 2 + 10, 400, 40);
  ctx.font = '700 32px sans-serif';
  ctx.fillText('Make Money Online', textX + ctx.measureText('The All-In-One Blueprint to ').width, H / 2 + 0);

  // Stats
  const statsY = H / 2 + 80;

  ctx.fillStyle = greenGrad(ctx, textX, statsY - 30, 120, 44);
  ctx.font = '800 40px sans-serif';
  ctx.fillText('$10M+', textX, statsY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 16px sans-serif';
  ctx.fillText('Member Revenue', textX, statsY + 26);

  ctx.fillStyle = greenGrad(ctx, textX + 200, statsY - 30, 80, 44);
  ctx.font = '800 40px sans-serif';
  ctx.fillText('5+', textX + 200, statsY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 16px sans-serif';
  ctx.fillText('Income Streams', textX + 200, statsY + 26);

  drawAccentLine(ctx, W, H);
  return canvas.toBuffer('image/png');
}

// ============= ASSET 2B: BANNER WITH SUCCESS SCREENSHOTS (1920x1080) =============
async function renderBannerSuccess(images) {
  console.log('  Rendering: Banner Success (1920x1080)...');
  const W = 1920, H = 1080;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  drawBG(ctx, W, H);

  // Left side (900px)
  const leftW = 900;

  // Logo + brand name
  drawLogoMark(ctx, 125, H / 2 - 80, 90);

  ctx.fillStyle = '#ffffff';
  ctx.font = '900 64px sans-serif';
  ctx.textAlign = 'left';
  ctx.textBaseline = 'middle';
  ctx.fillText('Soldi', 185, H / 2 - 80);

  // Tagline
  ctx.textBaseline = 'alphabetic';
  ctx.font = '500 26px sans-serif';
  ctx.fillStyle = '#a1a1aa';
  ctx.fillText('The All-In-One Blueprint to', 80, H / 2 + 0);

  ctx.fillStyle = greenGrad(ctx, 80, H / 2 - 10, 400, 36);
  ctx.font = '700 26px sans-serif';
  const taglineW = ctx.measureText('The All-In-One Blueprint to ').width;
  ctx.fillText('Make Money Online', 80 + taglineW, H / 2 + 0);

  // Stats
  const statsY = H / 2 + 70;
  ctx.fillStyle = greenGrad(ctx, 80, statsY - 24, 120, 36);
  ctx.font = '800 32px sans-serif';
  ctx.fillText('$10M+', 80, statsY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 14px sans-serif';
  ctx.fillText('Member Revenue', 80, statsY + 22);

  ctx.fillStyle = greenGrad(ctx, 240, statsY - 24, 80, 36);
  ctx.font = '800 32px sans-serif';
  ctx.fillText('5+', 240, statsY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 14px sans-serif';
  ctx.fillText('Income Streams', 240, statsY + 22);

  // Right side: 4 columns of success images
  const thumbW = 220, thumbH = 280, gap = 16;
  const cols = [
    ['betting-success/bet-success-1.png', 'betting-success/bet-success-5.png', 'ecom-success/ecom-success-1.jpg'],
    ['betting-success/bet-success-3.png', 'ecom-success/ecom-success-4.png', 'betting-success/bet-success-7.png'],
    ['ecom-success/ecom-success-2.jpg', 'betting-success/bet-success-10.png', 'betting-success/bet-success-12.png'],
    ['betting-success/bet-success-15.png', 'ecom-success/ecom-success-6.png', 'betting-success/bet-success-9.png'],
  ];

  const startXRight = leftW + 40;

  for (let ci = 0; ci < cols.length; ci++) {
    const cx = startXRight + ci * (thumbW + gap);
    const offsetY = ci % 2 === 1 ? -60 : 0;

    for (let ri = 0; ri < cols[ci].length; ri++) {
      const ty = (H / 2 - (3 * thumbH + 2 * gap) / 2) + ri * (thumbH + gap) + offsetY;

      // Card bg
      ctx.fillStyle = '#0a0a0f';
      roundRect(ctx, cx, ty, thumbW, thumbH, 14);
      ctx.fill();

      // Border
      ctx.strokeStyle = 'rgba(255,255,255,0.08)';
      ctx.lineWidth = 1;
      roundRect(ctx, cx, ty, thumbW, thumbH, 14);
      ctx.stroke();

      // Image
      const img = images[cols[ci][ri]];
      if (img) {
        ctx.save();
        roundRect(ctx, cx, ty, thumbW, thumbH, 14);
        ctx.clip();
        drawContained(ctx, img, cx, ty, thumbW, thumbH);
        ctx.restore();
      }
    }
  }

  // Fade edges on right panel
  const fadeL = ctx.createLinearGradient(leftW, 0, leftW + 80, 0);
  fadeL.addColorStop(0, '#050507');
  fadeL.addColorStop(1, 'rgba(5,5,7,0)');
  ctx.fillStyle = fadeL;
  ctx.fillRect(leftW, 0, 80, H);

  const fadeR = ctx.createLinearGradient(W - 80, 0, W, 0);
  fadeR.addColorStop(0, 'rgba(5,5,7,0)');
  fadeR.addColorStop(1, '#050507');
  ctx.fillStyle = fadeR;
  ctx.fillRect(W - 80, 0, 80, H);

  drawAccentLine(ctx, W, H);
  return canvas.toBuffer('image/png');
}

// ============= ASSET 3: PRODUCT CARD (1920x1080) =============
function renderProductCard() {
  console.log('  Rendering: Product Card (1920x1080)...');
  const W = 1920, H = 1080;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  drawBG(ctx, W, H);

  // Center orb
  const orbC = ctx.createRadialGradient(W / 2, H / 2, 0, W / 2, H / 2, 350);
  orbC.addColorStop(0, 'rgba(6,214,160,0.06)');
  orbC.addColorStop(1, 'rgba(6,214,160,0)');
  ctx.fillStyle = orbC;
  ctx.fillRect(0, 0, W, H);

  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';

  // Logo mark
  drawLogoMark(ctx, W / 2, 300, 140);

  // Title
  ctx.fillStyle = '#ffffff';
  ctx.font = '900 72px sans-serif';
  ctx.fillText('Soldi', W / 2, 440);

  // Subtitle
  ctx.fillStyle = '#a1a1aa';
  ctx.font = '500 28px sans-serif';
  const subLines = wrapText(ctx, 'The complete blueprint to build 5+ income streams with AI-powered tools, proven systems & a winning community.', 800);
  let subY = 500;
  subLines.forEach(line => {
    ctx.fillText(line, W / 2, subY);
    subY += 38;
  });

  // Feature chips
  const features = ['Sports Betting', 'E-Commerce', 'AI Automation', 'Affiliate Systems'];
  const chipGap = 24;
  const chipPadX = 28, chipPadY = 14;
  const chipFontSize = 18;
  ctx.font = `600 ${chipFontSize}px sans-serif`;

  // Measure total width
  let totalChipsW = 0;
  const chipWidths = features.map(f => {
    const tw = ctx.measureText('✓ ' + f).width + chipPadX * 2;
    totalChipsW += tw;
    return tw;
  });
  totalChipsW += (features.length - 1) * chipGap;

  let chipX = (W - totalChipsW) / 2;
  const chipY = subY + 40;

  features.forEach((feat, i) => {
    const cw = chipWidths[i];
    const ch = chipFontSize + chipPadY * 2;

    // Chip bg
    ctx.fillStyle = 'rgba(34,197,94,0.08)';
    roundRect(ctx, chipX, chipY, cw, ch, 100);
    ctx.fill();

    // Chip border
    ctx.strokeStyle = 'rgba(34,197,94,0.15)';
    ctx.lineWidth = 1;
    roundRect(ctx, chipX, chipY, cw, ch, 100);
    ctx.stroke();

    // Chip text
    ctx.fillStyle = '#22c55e';
    ctx.font = `600 ${chipFontSize}px sans-serif`;
    ctx.textAlign = 'center';
    ctx.fillText('✓ ' + feat, chipX + cw / 2, chipY + ch / 2 + chipFontSize * 0.35);

    chipX += cw + chipGap;
  });

  drawAccentLine(ctx, W, H);
  return canvas.toBuffer('image/png');
}

function wrapText(ctx, text, maxWidth) {
  const words = text.split(' ');
  const lines = [];
  let current = '';
  for (const word of words) {
    const test = current ? current + ' ' + word : word;
    if (ctx.measureText(test).width > maxWidth) {
      if (current) lines.push(current);
      current = word;
    } else {
      current = test;
    }
  }
  if (current) lines.push(current);
  return lines;
}

// ============= ASSET 4: SUCCESS COLLAGE (1920x1080) =============
async function renderSuccessCollage(images) {
  console.log('  Rendering: Success Collage (1920x1080)...');
  const W = 1920, H = 1080;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  drawBG(ctx, W, H);

  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';

  // Header
  drawLogoMark(ctx, W / 2, 80, 64);

  ctx.fillStyle = '#ffffff';
  ctx.font = '900 48px sans-serif';
  ctx.fillText('Real Members. ', W / 2 - 130, 170);

  ctx.fillStyle = greenGrad(ctx, W / 2 - 10, 130, 260, 50);
  ctx.font = '900 48px sans-serif';
  // Measure "Real Members. " to position "Real Money." correctly
  const rmW = ctx.measureText('Real Members. ').width;
  ctx.textAlign = 'left';
  ctx.fillText('Real Money.', W / 2 - 130 - rmW / 2 + rmW, 170);

  ctx.textAlign = 'center';
  ctx.fillStyle = '#71717a';
  ctx.font = '500 20px sans-serif';
  ctx.fillText('Verified wins from Soldi members', W / 2, 205);

  // Grid: 4 columns x 2 rows
  const gridTop = 230;
  const gridBottom = H - 140;
  const gridH = gridBottom - gridTop;
  const gridPadX = 40;
  const gridGap = 16;
  const numCols = 4, numRows = 2;
  const cellW = (W - gridPadX * 2 - (numCols - 1) * gridGap) / numCols;
  const cellH = (gridH - (numRows - 1) * gridGap) / numRows;

  const collageImages = [
    { src: 'betting-success/bet-success-1.png', tag: '+$34,931', feat: true },
    { src: 'ecom-success/ecom-success-1.jpg', tag: 'E-COM' },
    { src: 'betting-success/bet-success-5.png' },
    { src: 'ecom-success/ecom-success-4.png', tag: '$352K' },
    { src: 'betting-success/bet-success-10.png' },
    { src: 'betting-success/bet-success-3.png' },
    { src: 'ecom-success/ecom-success-2.jpg', tag: 'E-COM' },
  ];

  // Row 1: feat(span 2), normal, normal  => 4 cols used
  // Row 2: normal, normal, normal(span 2 for last cell? no, 3 cells)
  // Actually from HTML: grid-template-columns: repeat(4, 1fr); grid-template-rows: repeat(2, 1fr);
  // feat = grid-column span 2
  // Row 1: [feat(2col), cell, cell] = 4 cols
  // Row 2: [cell, cell, cell] = 3 of 4 cols (last cell is normal width)

  // Row 1
  // Featured cell (spans 2 columns)
  const featW = cellW * 2 + gridGap;
  let col = 0;

  // Image 0: Featured (+$34,931)
  {
    const cx = gridPadX;
    const cy = gridTop;
    ctx.fillStyle = '#0a0a0f';
    roundRect(ctx, cx, cy, featW, cellH, 14);
    ctx.fill();
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.lineWidth = 1;
    roundRect(ctx, cx, cy, featW, cellH, 14);
    ctx.stroke();
    const img = images[collageImages[0].src];
    if (img) {
      ctx.save();
      roundRect(ctx, cx, cy, featW, cellH, 14);
      ctx.clip();
      drawContained(ctx, img, cx, cy, featW, cellH);
      ctx.restore();
    }
    // Tag
    drawTag(ctx, cx + 10, cy + cellH - 44, collageImages[0].tag);
  }

  // Image 1: ecom-success-1 (E-COM)
  {
    const cx = gridPadX + featW + gridGap;
    const cy = gridTop;
    drawCollageCell(ctx, cx, cy, cellW, cellH, images[collageImages[1].src], collageImages[1].tag);
  }

  // Image 2: bet-success-5
  {
    const cx = gridPadX + featW + gridGap + cellW + gridGap;
    const cy = gridTop;
    drawCollageCell(ctx, cx, cy, cellW, cellH, images[collageImages[2].src]);
  }

  // Row 2
  // Image 3: ecom-success-4 ($352K)
  {
    const cx = gridPadX;
    const cy = gridTop + cellH + gridGap;
    drawCollageCell(ctx, cx, cy, cellW, cellH, images[collageImages[3].src], collageImages[3].tag);
  }

  // Image 4: bet-success-10
  {
    const cx = gridPadX + cellW + gridGap;
    const cy = gridTop + cellH + gridGap;
    drawCollageCell(ctx, cx, cy, cellW, cellH, images[collageImages[4].src]);
  }

  // Image 5: bet-success-3
  {
    const cx = gridPadX + 2 * (cellW + gridGap);
    const cy = gridTop + cellH + gridGap;
    drawCollageCell(ctx, cx, cy, cellW, cellH, images[collageImages[5].src]);
  }

  // Image 6: ecom-success-2 (E-COM)
  {
    const cx = gridPadX + 3 * (cellW + gridGap);
    const cy = gridTop + cellH + gridGap;
    drawCollageCell(ctx, cx, cy, cellW, cellH, images[collageImages[6].src], collageImages[6].tag);
  }

  // Footer stats
  const footY = H - 80;
  ctx.textAlign = 'center';

  ctx.fillStyle = greenGrad(ctx, W / 2 - 280, footY - 26, 120, 36);
  ctx.font = '800 32px sans-serif';
  ctx.fillText('$10M+', W / 2 - 220, footY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 13px sans-serif';
  ctx.fillText('Member Revenue', W / 2 - 220, footY + 20);

  ctx.fillStyle = greenGrad(ctx, W / 2 - 30, footY - 26, 60, 36);
  ctx.font = '800 32px sans-serif';
  ctx.fillText('5+', W / 2, footY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 13px sans-serif';
  ctx.fillText('Income Streams', W / 2, footY + 20);

  ctx.fillStyle = greenGrad(ctx, W / 2 + 150, footY - 26, 120, 36);
  ctx.font = '800 32px sans-serif';
  ctx.fillText('$49.99', W / 2 + 220, footY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 13px sans-serif';
  ctx.fillText('/month', W / 2 + 220, footY + 20);

  drawAccentLine(ctx, W, H);
  return canvas.toBuffer('image/png');
}

function drawCollageCell(ctx, x, y, w, h, img, tag) {
  ctx.fillStyle = '#0a0a0f';
  roundRect(ctx, x, y, w, h, 14);
  ctx.fill();
  ctx.strokeStyle = 'rgba(255,255,255,0.06)';
  ctx.lineWidth = 1;
  roundRect(ctx, x, y, w, h, 14);
  ctx.stroke();
  if (img) {
    ctx.save();
    roundRect(ctx, x, y, w, h, 14);
    ctx.clip();
    drawContained(ctx, img, x, y, w, h);
    ctx.restore();
  }
  if (tag) {
    drawTag(ctx, x + 10, y + h - 44, tag);
  }
}

function drawTag(ctx, x, y, text) {
  ctx.font = '800 14px sans-serif';
  const tw = ctx.measureText(text).width;
  const pw = tw + 28;
  const ph = 34;
  ctx.fillStyle = 'rgba(34,197,94,0.9)';
  roundRect(ctx, x, y, pw, ph, 8);
  ctx.fill();
  ctx.fillStyle = '#050507';
  ctx.textAlign = 'left';
  ctx.fillText(text, x + 14, y + 22);
}

// ============= ASSET 5: SOCIAL PROOF (1920x1080) =============
function renderSocialProof() {
  console.log('  Rendering: Social Proof (1920x1080)...');
  const W = 1920, H = 1080;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  drawBG(ctx, W, H);

  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';

  // Headline
  const headY = 320;
  ctx.fillStyle = '#ffffff';
  ctx.font = '900 64px sans-serif';
  ctx.fillText('Real Members. ', W / 2 - 155, headY);

  // "Real Results." in green
  const rmW = ctx.measureText('Real Members. ').width;
  ctx.fillStyle = greenGrad(ctx, W / 2 - 155 + rmW - rmW / 2, headY - 50, 350, 60);
  ctx.textAlign = 'left';
  ctx.fillText('Real Results.', W / 2 - 155 - rmW / 2 + rmW + 10, headY);

  ctx.textAlign = 'center';

  // Stats row
  const statsY = 470;
  const statGap = 200;

  // Stat 1: $10M+
  const stat1X = W / 2 - statGap - 80;
  ctx.fillStyle = greenGrad(ctx, stat1X - 80, statsY - 50, 160, 70);
  ctx.font = '900 72px sans-serif';
  ctx.fillText('$10M+', stat1X, statsY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 18px sans-serif';
  ctx.fillText('Total Member Revenue', stat1X, statsY + 32);

  // Divider 1
  const div1X = W / 2 - 40;
  ctx.strokeStyle = 'rgba(255,255,255,0.1)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(div1X, statsY - 55);
  ctx.lineTo(div1X, statsY + 40);
  ctx.stroke();

  // Stat 2: $42K
  const stat2X = W / 2 + 40;
  ctx.fillStyle = greenGrad(ctx, stat2X - 60, statsY - 50, 120, 70);
  ctx.font = '900 72px sans-serif';
  ctx.fillText('$42K', stat2X, statsY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 18px sans-serif';
  ctx.fillText('Top Monthly Profit', stat2X, statsY + 32);

  // Divider 2
  const div2X = W / 2 + statGap;
  ctx.strokeStyle = 'rgba(255,255,255,0.1)';
  ctx.beginPath();
  ctx.moveTo(div2X, statsY - 55);
  ctx.lineTo(div2X, statsY + 40);
  ctx.stroke();

  // Stat 3: $4.2M
  const stat3X = W / 2 + statGap + 80;
  ctx.fillStyle = greenGrad(ctx, stat3X - 80, statsY - 50, 160, 70);
  ctx.font = '900 72px sans-serif';
  ctx.fillText('$4.2M', stat3X, statsY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 18px sans-serif';
  ctx.fillText('Store Revenue', stat3X, statsY + 32);

  // Win chips
  const chips = ['+$34,931 Nov', '+$42,219 Monthly', '$352K Store Sales', '+$24,865 Verified', '$15K/mo AI Services'];
  const chipY = 580;
  const chipPadX = 24, chipPadY = 12;
  const chipFont = 18;
  ctx.font = `700 ${chipFont}px sans-serif`;

  let totalW = 0;
  const chipWidths = chips.map(c => {
    const w = ctx.measureText(c).width + chipPadX * 2;
    totalW += w;
    return w;
  });
  totalW += (chips.length - 1) * 16;

  let cx = (W - totalW) / 2;
  chips.forEach((chip, i) => {
    const cw = chipWidths[i];
    const ch = chipFont + chipPadY * 2;

    ctx.fillStyle = 'rgba(34,197,94,0.08)';
    roundRect(ctx, cx, chipY, cw, ch, 100);
    ctx.fill();

    ctx.strokeStyle = 'rgba(34,197,94,0.12)';
    ctx.lineWidth = 1;
    roundRect(ctx, cx, chipY, cw, ch, 100);
    ctx.stroke();

    ctx.fillStyle = '#22c55e';
    ctx.font = `700 ${chipFont}px sans-serif`;
    ctx.textAlign = 'center';
    ctx.fillText(chip, cx + cw / 2, chipY + ch / 2 + chipFont * 0.35);

    cx += cw + 16;
  });

  drawAccentLine(ctx, W, H);
  return canvas.toBuffer('image/png');
}

// ============= MAIN =============
async function main() {
  console.log('🎨 Generating Soldi PNG Assets...\n');

  // Load all success images into a map
  const imagePaths = [
    'betting-success/bet-success-1.png',
    'betting-success/bet-success-3.png',
    'betting-success/bet-success-5.png',
    'betting-success/bet-success-6.png',
    'betting-success/bet-success-7.png',
    'betting-success/bet-success-8.png',
    'betting-success/bet-success-9.png',
    'betting-success/bet-success-10.png',
    'betting-success/bet-success-12.png',
    'betting-success/bet-success-15.png',
    'betting-success/bet-success-16.png',
    'ecom-success/ecom-success-1.jpg',
    'ecom-success/ecom-success-2.jpg',
    'ecom-success/ecom-success-4.png',
    'ecom-success/ecom-success-5.png',
    'ecom-success/ecom-success-6.png',
    'ecom-success/ecom-success-8.jpg',
  ];

  console.log('Loading images...');
  const images = {};
  for (const p of imagePaths) {
    try {
      images[p] = await loadImage(path.join(DIR, 'images', p));
    } catch {
      console.log(`  ⚠ Could not load: ${p}`);
      images[p] = null;
    }
  }
  console.log(`Loaded ${Object.values(images).filter(Boolean).length}/${imagePaths.length} images.\n`);

  // Generate each asset
  const assets = [
    { name: 'soldi-profile-400x400.png', buffer: renderProfilePic() },
    { name: 'soldi-banner-clean-1920x1080.png', buffer: renderBannerClean() },
    { name: 'soldi-banner-success-1920x1080.png', buffer: await renderBannerSuccess(images) },
    { name: 'soldi-product-card-1920x1080.png', buffer: renderProductCard() },
    { name: 'soldi-success-collage-1920x1080.png', buffer: await renderSuccessCollage(images) },
    { name: 'soldi-social-proof-1920x1080.png', buffer: renderSocialProof() },
  ];

  console.log('\nSaving files...');
  for (const asset of assets) {
    const filePath = path.join(DIR, asset.name);
    fs.writeFileSync(filePath, asset.buffer);
    const sizeKB = Math.round(asset.buffer.length / 1024);
    console.log(`  ✅ ${asset.name} (${sizeKB} KB)`);
  }

  console.log('\n🎉 All 6 PNG assets generated successfully!');
}

main().catch(console.error);
