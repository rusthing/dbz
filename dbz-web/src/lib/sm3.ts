/**
 * SM3 摘要（纯 TypeScript，无第三方依赖）。
 *
 * 登录与删除二次确认都要求 `loginPswd = SM3(明文)` 的 64 位小写十六进制，
 * 口令不能在网络上以明文传输。这里只实现单向摘要，足够登录与口令校验使用。
 *
 * 参考：GB/T 32905-2016。
 */

const IV = [
  0x7380166f, 0x4914b2b9, 0x172442d7, 0xda8a0600, 0xa96f30bc, 0x163138aa, 0xe38dee4d,
  0xb0fb0e4e,
] as const;

function rotl(x: number, n: number): number {
  const s = n & 31;
  return ((x << s) | (x >>> (32 - s))) >>> 0;
}

function add(x: number, y: number): number {
  return (x + y) >>> 0;
}

function p0(x: number): number {
  return (x ^ rotl(x, 9) ^ rotl(x, 17)) >>> 0;
}

function p1(x: number): number {
  return (x ^ rotl(x, 15) ^ rotl(x, 23)) >>> 0;
}

function compress(v: number[], block: Uint8Array): void {
  const w = new Uint32Array(68);
  for (let i = 0; i < 16; i += 1) {
    const o = i * 4;
    w[i] = ((block[o] << 24) | (block[o + 1] << 16) | (block[o + 2] << 8) | block[o + 3]) >>> 0;
  }
  for (let j = 16; j < 68; j += 1) {
    w[j] =
      (p1((w[j - 16] ^ w[j - 9] ^ rotl(w[j - 3], 15)) >>> 0) ^ rotl(w[j - 13], 7) ^ w[j - 6]) >>>
      0;
  }

  const wp = new Uint32Array(64);
  for (let j = 0; j < 64; j += 1) {
    wp[j] = (w[j] ^ w[j + 4]) >>> 0;
  }

  let [a, b, c, d, e, f, g, h] = v;

  for (let j = 0; j < 64; j += 1) {
    const tj = j < 16 ? 0x79cc4519 : 0x7a879d8a;
    const ss1 = rotl(add(add(rotl(a, 12), e), rotl(tj, j)), 7);
    const ss2 = ss1 ^ rotl(a, 12);
    const ff = j < 16 ? (a ^ b ^ c) >>> 0 : ((a & b) | (a & c) | (b & c)) >>> 0;
    const gg = j < 16 ? (e ^ f ^ g) >>> 0 : ((e & f) | (~e & g)) >>> 0;
    const tt1 = add(add(add(ff, d), ss2), wp[j]);
    const tt2 = add(add(add(gg, h), ss1), w[j]);

    d = c;
    c = rotl(b, 9);
    b = a;
    a = tt1;
    h = g;
    g = rotl(f, 19);
    f = e;
    e = p0(tt2);
  }

  v[0] = (v[0] ^ a) >>> 0;
  v[1] = (v[1] ^ b) >>> 0;
  v[2] = (v[2] ^ c) >>> 0;
  v[3] = (v[3] ^ d) >>> 0;
  v[4] = (v[4] ^ e) >>> 0;
  v[5] = (v[5] ^ f) >>> 0;
  v[6] = (v[6] ^ g) >>> 0;
  v[7] = (v[7] ^ h) >>> 0;
}

/** 计算 SM3，返回 64 位小写十六进制。输入按 UTF-8 编码。 */
export function sm3(input: string): string {
  const bytes = new TextEncoder().encode(input);
  const bitLength = bytes.length * 8;

  // padding：1 bit + k 个 0，使 (len + 1 + k) % 64 === 56，末尾 8 字节是大端长度
  const paddedLength = Math.ceil((bytes.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(bytes);
  padded[bytes.length] = 0x80;

  // 长度按 64 位大端写入；JS 的安全整数不足以表达超过 2^53 位的消息，
  // 但实际口令远低于这个量级。
  const hi = Math.floor(bitLength / 0x1_0000_0000);
  const lo = bitLength >>> 0;
  padded[paddedLength - 8] = (hi >>> 24) & 0xff;
  padded[paddedLength - 7] = (hi >>> 16) & 0xff;
  padded[paddedLength - 6] = (hi >>> 8) & 0xff;
  padded[paddedLength - 5] = hi & 0xff;
  padded[paddedLength - 4] = (lo >>> 24) & 0xff;
  padded[paddedLength - 3] = (lo >>> 16) & 0xff;
  padded[paddedLength - 2] = (lo >>> 8) & 0xff;
  padded[paddedLength - 1] = lo & 0xff;

  const v = [...IV];
  for (let offset = 0; offset < paddedLength; offset += 64) {
    compress(v, padded.subarray(offset, offset + 64));
  }

  return v.map((word) => word.toString(16).padStart(8, '0')).join('');
}
