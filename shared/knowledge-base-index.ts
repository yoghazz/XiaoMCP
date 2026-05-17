import fs from 'node:fs';
import path from 'node:path';

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

export function listKnowledgeBaseDocs() {
  return loadKnowledgeBaseIndex().docs;
}

export async function addKnowledgeBaseDoc(input: {
  id: string;
  title: string;
  path: string;
  type: 'pdf' | 'text';
  tags?: string[];
  description?: string;
}) {
  const index = loadKnowledgeBaseIndex();
  if (!input.id || !input.title || !input.path) throw new Error('id, title, dan path wajib diisi.');
  if (index.docs.some((d) => d.id === input.id)) throw new Error(`ID KB sudah ada: ${input.id}`);
  index.docs.push({
    id: input.id,
    title: input.title,
    path: input.path,
    type: input.type,
    tags: input.tags || [],
    description: input.description || '',
  });
  index.updatedAt = new Date().toISOString();
  saveKnowledgeBaseIndex(index);
  return { ok: true };
}
