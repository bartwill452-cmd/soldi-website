const { createCanvas, loadImage } = require('canvas');
const fs = require('fs');
const path = require('path');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path;
ffmpeg.setFfmpegPath(ffmpegPath);

const W = 1920, H = 1080;
const FPS = 30;
const DURATION = 20; // seconds
const TOTAL_FRAMES = FPS * DURATION;
const FRAMES_DIR = path.join(__dirname, '_frames');
const OUTPUT = path.join(__dirname, 'soldi-success-video-1920x1080.mp4');

const imgSrcs = [
  'images/betting-success/bet-success-1.png',
  'images/ecom-success/ecom-success-1.jpg',
  'images/betting-success/bet-success-6.png',
  'images/betting-success/bet-success-3.png',
  'images/ecom-success/ecom-success-4.png',
  'images/betting-success/bet-success-8.png',
  'images/ecom-success/ecom-success-2.jpg',
  'images/betting-success/bet-success-10.png',
  'images/ecom-success/ecom-success-6.png',
  'images/betting-success/bet-success-5.png',
  'images/betting-success/bet-success-12.png',
  'images/betting-success/bet-success-15.png',
  'images/betting-success/bet-success-7.png',
  'images/ecom-success/ecom-success-8.jpg',
  'images/betting-success/bet-success-14.png',
  'images/betting-success/bet-success-9.png',
  'images/ecom-success/ecom-success-5.png',
  'images/betting-success/bet-success-16.png',
];

const colW = 260, colH = 340, gap = 24, colGap = 20;
const NUM_COLS = 6;
const totalColsW = NUM_COLS * colW + (NUM_COLS - 1) * colGap;
const startX = (W - totalColsW) / 2;

const colAnim = [
  { dir: -1, speed: 0.0031 },
  { dir: 1, speed: 0.0028 },
  { dir: -1, speed: 0.0035 },
  { dir: 1, speed: 0.0025 },
  { dir: -1, speed: 0.003 },
  { dir: 1, speed: 0.0033 },
];

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

function drawContained(ctx, img, x, y, bw, bh) {
  if (!img) return;
  const scale = Math.min(bw / img.width, bh / img.height);
  const dw = img.width * scale;
  const dh = img.height * scale;
  const dx = x + (bw - dw) / 2;
  const dy = y + (bh - dh) / 2;
  ctx.drawImage(img, dx, dy, dw, dh);
}

function greenGrad(ctx, x, y, w, h) {
  const g = ctx.createLinearGradient(x, y, x + w, y + h);
  g.addColorStop(0, '#22c55e');
  g.addColorStop(0.5, '#10b981');
  g.addColorStop(1, '#06d6a0');
  return g;
}

function renderFrame(ctx, cols, elapsed) {
  // BG
  ctx.fillStyle = '#050507';
  ctx.fillRect(0, 0, W, H);

  // Grid lines
  ctx.strokeStyle = 'rgba(34,197,94,0.04)';
  ctx.lineWidth = 1;
  for (let x = 0; x < W; x += 60) { ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, H); ctx.stroke(); }
  for (let y = 0; y < H; y += 60) { ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke(); }

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

  // === HEADER ===
  // Logo box
  const logoX = W / 2 - 40, logoY = 44;
  ctx.fillStyle = greenGrad(ctx, logoX, logoY, 80, 80);
  roundRect(ctx, logoX, logoY, 80, 80, 22);
  ctx.fill();
  // S letter
  ctx.fillStyle = '#050507';
  ctx.font = 'bold 54px sans-serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('S', logoX + 40, logoY + 43);

  // Title line 1
  ctx.fillStyle = '#ffffff';
  ctx.font = '900 54px sans-serif';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText('See Why Members', W / 2, 186);

  // Title line 2 (green)
  ctx.fillStyle = greenGrad(ctx, W / 2 - 180, 200, 360, 60);
  ctx.font = '900 54px sans-serif';
  ctx.fillText('Keep Winning', W / 2, 250);

  // Subtitle
  ctx.fillStyle = '#71717a';
  ctx.font = '500 21px sans-serif';
  ctx.fillText('Real results. Real screenshots. Real money.', W / 2, 290);

  // === ANIMATED COLUMNS ===
  const stageTop = 316, stageH = 560;
  ctx.save();
  ctx.beginPath();
  ctx.rect(0, stageTop, W, stageH);
  ctx.clip();

  cols.forEach((colImgs, ci) => {
    const anim = colAnim[ci];
    const offset = Math.sin(elapsed * anim.speed) * 140 * anim.dir;
    const cx = startX + ci * (colW + colGap);

    colImgs.forEach((img, ri) => {
      const baseY = stageTop + ri * (colH + gap) + (stageH - 3 * colH - 2 * gap) / 2;
      const iy = baseY + offset;

      // Card bg
      ctx.fillStyle = '#0a0a0f';
      roundRect(ctx, cx, iy, colW, colH, 16);
      ctx.fill();

      // Border
      ctx.strokeStyle = 'rgba(255,255,255,0.08)';
      ctx.lineWidth = 1;
      roundRect(ctx, cx, iy, colW, colH, 16);
      ctx.stroke();

      // Image
      ctx.save();
      roundRect(ctx, cx, iy, colW, colH, 16);
      ctx.clip();
      drawContained(ctx, img, cx, iy, colW, colH);
      ctx.restore();
    });
  });
  ctx.restore();

  // === BOTTOM STATS ===
  const bY = H - 126;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';

  ctx.fillStyle = greenGrad(ctx, W / 2 - 200, bY, 120, 44);
  ctx.font = '900 42px sans-serif';
  ctx.fillText('$10M+', W / 2 - 120, bY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 14px sans-serif';
  ctx.fillText('Member Revenue', W / 2 - 120, bY + 24);

  ctx.fillStyle = greenGrad(ctx, W / 2 + 60, bY, 120, 44);
  ctx.font = '900 42px sans-serif';
  ctx.fillText('5+', W / 2 + 120, bY);
  ctx.fillStyle = '#71717a';
  ctx.font = '500 14px sans-serif';
  ctx.fillText('Income Streams', W / 2 + 120, bY + 24);

  // === CTA BUTTON ===
  const btnW = 460, btnH = 58;
  const btnX = W / 2 - btnW / 2, btnY2 = H - 68;
  ctx.fillStyle = greenGrad(ctx, btnX, btnY2, btnW, btnH);
  roundRect(ctx, btnX, btnY2, btnW, btnH, 14);
  ctx.fill();
  ctx.fillStyle = '#050507';
  ctx.font = '800 23px sans-serif';
  ctx.fillText('Join Soldi | $49.99/mo  →', W / 2, btnY2 + 38);

  // === ACCENT LINE ===
  ctx.fillStyle = greenGrad(ctx, 0, H - 4, W, 4);
  ctx.fillRect(0, H - 4, W, 4);
}

async function main() {
  console.log('Loading images...');
  const images = [];
  for (const src of imgSrcs) {
    try {
      const img = await loadImage(path.join(__dirname, src));
      images.push(img);
    } catch {
      images.push(null);
    }
  }

  // Build columns
  const cols = [];
  for (let i = 0; i < NUM_COLS; i++) {
    cols.push(images.slice(i * 3, i * 3 + 3).filter(Boolean));
  }

  // Create frames dir
  if (!fs.existsSync(FRAMES_DIR)) fs.mkdirSync(FRAMES_DIR);

  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  console.log(`Rendering ${TOTAL_FRAMES} frames at ${FPS}fps...`);
  for (let f = 0; f < TOTAL_FRAMES; f++) {
    const elapsed = (f / FPS) * 1000; // ms
    renderFrame(ctx, cols, elapsed);

    const buf = canvas.toBuffer('image/png');
    const frameNum = String(f).padStart(5, '0');
    fs.writeFileSync(path.join(FRAMES_DIR, `frame_${frameNum}.png`), buf);

    if (f % 30 === 0) {
      process.stdout.write(`\r  Frame ${f}/${TOTAL_FRAMES} (${Math.round(f / TOTAL_FRAMES * 100)}%)`);
    }
  }
  console.log(`\r  Frame ${TOTAL_FRAMES}/${TOTAL_FRAMES} (100%)    `);

  // Stitch into MP4
  console.log('Encoding MP4...');
  return new Promise((resolve, reject) => {
    ffmpeg()
      .input(path.join(FRAMES_DIR, 'frame_%05d.png'))
      .inputFPS(FPS)
      .outputOptions([
        '-c:v libx264',
        '-pix_fmt yuv420p',
        '-crf 18',
        '-preset medium',
        '-movflags +faststart',
      ])
      .output(OUTPUT)
      .on('end', () => {
        console.log(`✅ Video saved: ${OUTPUT}`);
        // Cleanup frames
        const files = fs.readdirSync(FRAMES_DIR);
        files.forEach(f => fs.unlinkSync(path.join(FRAMES_DIR, f)));
        fs.rmdirSync(FRAMES_DIR);
        console.log('Cleaned up frame files.');
        resolve();
      })
      .on('error', (err) => {
        console.error('FFmpeg error:', err.message);
        reject(err);
      })
      .on('progress', (p) => {
        if (p.frames) process.stdout.write(`\r  Encoding frame ${p.frames}...`);
      })
      .run();
  });
}

main().catch(console.error);
