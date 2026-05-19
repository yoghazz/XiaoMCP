export async function runInternetSearch(query: string) {
  const q = String(query || '').trim();
  if (!q) return 'Query kosong.';
  return 'Pencarian internet sementara dinonaktifkan saat recovery worker. Workflow OpenClaw lain diprioritaskan pulih dulu.';
}
