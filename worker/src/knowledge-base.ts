import fs from 'node:fs';
import path from 'node:path';
import pdf from 'pdf-parse';

export type KnowledgeBaseDoc = {
  id: string;
  title: string;
  type: 'pdf' | 'text';
  path: string;
  tags?: string[];
  description?: string;
};

export type KnowledgeBaseIndex = {
  updatedAt: string;
  docs: KnowledgeBaseDoc[];
};

const KB_DIR = process.env.KNOWLEDGE_BASE_DIR || '/home/yoga/.openclaw/workspace/XiaoMCP/knowledge-base';
const KB_INDEX_PATH = process.env.KNOWLEDGE_BASE_INDEX || path.join(KB_DIR, 'index.json');

function ensureKbDir() {
  fs.mkdirSync(KB_DIR, { recursive: true });
}

function safeReadJson(filePath: string) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

export function loadKnowledgeBaseIndex(): KnowledgeBaseIndex {
  ensureKbDir();
  const parsed = safeReadJson(KB_INDEX_PATH);
  if (!parsed || !Array.isArray(parsed.docs)) {
    const empty: KnowledgeBaseIndex = { updatedAt: new Date().toISOString(), docs: [] };
    fs.writeFileSync(KB_INDEX_PATH, JSON.stringify(empty, null, 2));
    return empty;
  }
  return {
    updatedAt: typeof parsed.updatedAt === 'string' ? parsed.updatedAt : new Date().toISOString(),
    docs: parsed.docs,
  };
}

export function saveKnowledgeBaseIndex(index: KnowledgeBaseIndex) {
  ensureKbDir();
  fs.writeFileSync(KB_INDEX_PATH, JSON.stringify(index, null, 2));
}

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
  return s.toLowerCase().replace(/\s+/g, ' ').trim();
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

function scoreChunk(query: string, chunk: string, doc: KnowledgeBaseDoc) {
  const q = normalize(query);
  const c = normalize(chunk);
  let score = 0;
  const terms = Array.from(new Set(q.split(/[^\p{L}\p{N}_-]+/u).filter((x) => x.length >= 2)));
  for (const term of terms) {
    if (c.includes(term)) score += 3;
    if (normalize(doc.title).includes(term)) score += 5;
    if ((doc.tags || []).some((tag) => normalize(tag).includes(term))) score += 4;
  }
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
  const top = scored.slice(0, 5);
  if (!top.length) {
    return 'Tidak ditemukan bagian yang relevan di knowledge base.';
  }

  const grouped = new Map<string, { title: string; excerpts: string[] }>();
  for (const item of top) {
    const key = item.doc.id;
    if (!grouped.has(key)) grouped.set(key, { title: item.doc.title, excerpts: [] });
    grouped.get(key)!.excerpts.push(item.chunk.slice(0, 700).trim());
  }

  const parts: string[] = [];
  parts.push(`Hasil pencarian knowledge base untuk: ${text}`);
  for (const [docId, data] of grouped.entries()) {
    parts.push(`\nDokumen: ${data.title} [${docId}]`);
    data.excerpts.slice(0, 2).forEach((excerpt, idx) => {
      parts.push(`Kutipan ${idx + 1}: ${excerpt}`);
    });
  }
  return parts.join('\n');
}
