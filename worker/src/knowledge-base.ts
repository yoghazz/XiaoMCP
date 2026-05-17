import fs from 'node:fs';
import path from 'node:path';
import pdf from 'pdf-parse';
import { addKnowledgeBaseDoc, listKnowledgeBaseDocs, loadKnowledgeBaseIndex, saveKnowledgeBaseIndex, type KnowledgeBaseDoc } from '../../shared/knowledge-base-index';

const KB_DIR = process.env.KNOWLEDGE_BASE_DIR || '/home/yoga/.openclaw/workspace/XiaoMCP/knowledge-base';

export function parseKnowledgeBaseAdminText(text: string) {
  const lines = text.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  const command = lines[0]?.toLowerCase() || '';
  const data: Record<string, string> = {};
  for (const line of lines.slice(1)) {
    const i = line.indexOf(':');
    if (i === -1) continue;
    const key = line.slice(0, i).trim().toLowerCase();
    const value = line.slice(i + 1).trim();
    if (key) data[key] = value;
  }
  return { command, data };
}

async function extractPdfText(filePath: string) {
  const buf = fs.readFileSync(filePath);
  const parsed = await pdf(buf);
  return String(parsed.text || '');
}

async function extractDocText(doc: KnowledgeBaseDoc) {
  const resolved = path.isAbsolute(doc.path) ? doc.path : path.join(KB_DIR, doc.path);
  if (!fs.existsSync(resolved)) throw new Error(`File tidak ditemukan: ${resolved}`);
  if (doc.type === 'pdf') return extractPdfText(resolved);
  return fs.readFileSync(resolved, 'utf8');
}

function normalize(s: string) {
  return s
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s:/.-]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractTerms(query: string) {
  return Array.from(new Set(normalize(query).split(/[^\p{L}\p{N}_-]+/u).filter((x) => x.length >= 2)));
}

function chunkText(text: string, chunkSize = 1200, overlap = 200) {
  const clean = text.replace(/\r/g, '');
  const chunks: string[] = [];
  let i = 0;
  while (i < clean.length) {
    chunks.push(clean.slice(i, i + chunkSize));
    if (i + chunkSize >= clean.length) break;
    i += Math.max(1, chunkSize - overlap);
  }
  return chunks;
}

function extractFocusedExcerpt(query: string, chunk: string) {
  const q = normalize(query);
  const c = chunk.replace(/\r/g, ' ');
  const lc = normalize(c);

  const anchors: string[] = [];
  if (/ayat kursi/.test(q)) anchors.push('ayat kursi', '255', 'allah, tidak ada tuhan');
  if (/al baqarah 255|al-baqarah 255|2:255/.test(q)) anchors.push('255', 'al baqarah', 'allah, tidak ada tuhan');
  if (/administrasi kependudukan|adminduk/.test(q)) anchors.push('administrasi kependudukan');

  for (const anchor of anchors) {
    const idx = lc.indexOf(normalize(anchor));
    if (idx >= 0) {
      const start = Math.max(0, idx - 180);
      const end = Math.min(c.length, idx + 900);
      return c.slice(start, end).trim();
    }
  }

  return c.slice(0, 900).trim();
}

function scoreChunk(query: string, chunk: string, doc: KnowledgeBaseDoc) {
  const q = normalize(query);
  const c = normalize(chunk);
  const title = normalize(doc.title);
  const tags = (doc.tags || []).map(normalize);
  const desc = normalize(doc.description || '');
  const terms = extractTerms(query);
  let score = 0;

  if (!q) return score;

  if (title.includes(q)) score += 80;
  if (desc.includes(q)) score += 45;
  if (tags.some((tag) => tag.includes(q))) score += 60;
  if (c.includes(q)) score += 50;

  const queryHasDigits = /\d/.test(q);
  for (const term of terms) {
    const inChunk = c.includes(term);
    const inTitle = title.includes(term);
    const inTag = tags.some((tag) => tag.includes(term));
    const inDesc = desc.includes(term);
    if (inChunk) score += queryHasDigits && /\d/.test(term) ? 8 : 4;
    if (inTitle) score += queryHasDigits && /\d/.test(term) ? 14 : 7;
    if (inTag) score += queryHasDigits && /\d/.test(term) ? 12 : 6;
    if (inDesc) score += 4;
  }

  const matchedTerms = terms.filter((term) => c.includes(term)).length;
  score += matchedTerms * 2;

  if (/ayat kursi/.test(q) && /ayat kursi/.test(c)) score += 120;
  if (/al baqarah 255|al-baqarah 255|2:255/.test(q) && /255/.test(c)) score += 120;
  if (/administrasi kependudukan|adminduk/.test(q) && /administrasi kependudukan|adminduk/.test(c)) score += 90;

  return score;
}

export async function runKnowledgeBaseAdmin(text: string) {
  const { command, data } = parseKnowledgeBaseAdminText(text);
  const index = loadKnowledgeBaseIndex();

  if (command.includes('tambah') || command.includes('add')) {
    const id = data.id || data.slug || data.nama?.toLowerCase().replace(/[^a-z0-9]+/g, '-') || '';
    const title = data.title || data.nama || '';
    const filePath = data.path || data.file || '';
    const type = ((data.type || '').toLowerCase() === 'pdf' ? 'pdf' : 'text') as 'pdf' | 'text';
    if (!id || !title || !filePath) throw new Error('Format tambah KB butuh id/nama/title dan path/file.');
    if (index.docs.some((d) => d.id === id)) throw new Error(`ID KB sudah ada: ${id}`);
    const doc: KnowledgeBaseDoc = {
      id,
      title,
      path: filePath,
      type,
      tags: (data.tags || '').split(',').map((x) => x.trim()).filter(Boolean),
      description: data.description || data.deskripsi || '',
    };
    index.docs.push(doc);
    index.updatedAt = new Date().toISOString();
    saveKnowledgeBaseIndex(index);
    return `Knowledge base ditambahkan: ${doc.title} (${doc.id})`;
  }

  if (command.includes('list') || command.includes('daftar')) {
    if (!index.docs.length) return 'Belum ada dokumen knowledge base.';
    return index.docs.map((d, i) => `${i + 1}. ${d.title} [${d.id}] (${d.type})`).join('\n');
  }

  throw new Error('Perintah knowledge base admin belum dikenali. Gunakan add/tambah atau list/daftar.');
}

export { listKnowledgeBaseDocs, addKnowledgeBaseDoc };

export async function runKnowledgeBaseQuery(text: string) {
  const index = loadKnowledgeBaseIndex();
  if (!index.docs.length) return 'Knowledge base masih kosong.';

  const targetHint = normalize(text);
  const candidates = index.docs.filter((doc) => {
    const meta = [doc.id, doc.title, ...(doc.tags || []), doc.description || ''].join(' ');
    return !targetHint || normalize(meta).includes(targetHint) || scoreChunk(text, meta, doc) > 0;
  });
  const docs = candidates.length ? candidates : index.docs;

  const scored: Array<{ doc: KnowledgeBaseDoc; chunk: string; score: number }> = [];
  for (const doc of docs.slice(0, 8)) {
    const fullText = await extractDocText(doc);
    for (const chunk of chunkText(fullText)) {
      const score = scoreChunk(text, chunk, doc);
      if (score > 0) scored.push({ doc, chunk, score });
    }
  }

  scored.sort((a, b) => b.score - a.score);
  const top = scored.slice(0, 8);
  if (!top.length) {
    return 'Tidak ditemukan bagian yang relevan di knowledge base.';
  }

  const grouped = new Map<string, { title: string; excerpts: string[] }>();
  for (const item of top) {
    const key = item.doc.id;
    if (!grouped.has(key)) grouped.set(key, { title: item.doc.title, excerpts: [] });
    const bucket = grouped.get(key)!;
    if (bucket.excerpts.length >= 2) continue;
    bucket.excerpts.push(extractFocusedExcerpt(text, item.chunk));
  }

  const parts: string[] = [];
  for (const [docId, data] of grouped.entries()) {
    parts.push(`Dokumen: ${data.title} [${docId}]`);
    data.excerpts.forEach((excerpt, idx) => {
      parts.push(`Kutipan ${idx + 1}: ${excerpt}`);
    });
  }
  return parts.join('\n\n');
}
