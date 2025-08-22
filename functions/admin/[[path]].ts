// cloudflare-bbs/functions/admin/[[path]].ts

interface Env {
  MY_D1_DATABASE2: D1Database;
  MY_R2_BUCKET2: R2Bucket;
  ADMIN_API_KEY: string;
}

const corsHeaders = {
  'Access-Control-Allow-Origin': 'http://localhost:3000',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-Admin-API-Key',
};
const CHUNK_SIZE = 50;
const DELETED_MESSAGE = "この書き込みは削除されました";

const handleOptions = (request: Request) => new Response(null, { headers: corsHeaders });

export const onRequest: PagesFunction<Env> = async (context) => {
    const { request, env, waitUntil } = context;
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') return handleOptions(request);
    
    const apiKey = request.headers.get('X-Admin-API-Key');
    if (!apiKey || apiKey !== env.ADMIN_API_KEY) {
        return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: corsHeaders });
    }

    try {
        // --- 書き込み・削除系 ---
        if (method === 'POST') {
            const genreMatch = path.match(/^\/admin\/threads\/(\d+)\/genre$/);
            if (genreMatch) {
                const threadId = parseInt(genreMatch[1], 10);
                const { genre } = await request.json<{ genre: string }>();
                await env.MY_D1_DATABASE2.prepare("UPDATE threads_meta SET genre = ? WHERE id = ?").bind(genre, threadId).run();
                return new Response(JSON.stringify({ success: true }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
            }

            const purgeMatch = path.match(/^\/admin\/purge$/);
            if (purgeMatch) {
                const { urls } = await request.json<{ urls: string[] }>();
                const promises = urls.map(url => caches.default.delete(new Request(url)));
                await Promise.all(promises);
                return new Response(JSON.stringify({ success: true, purged_count: urls.length }), { headers: corsHeaders });
            }
        }
        
        if (method === 'DELETE') {
            const threadMatch = path.match(/^\/admin\/threads\/(\d+)$/);
            if (threadMatch) {
                const threadId = parseInt(threadMatch[1], 10);
                
                // 関連データを全て削除 (D1, R2)
                const threadInfo = await env.MY_D1_DATABASE2.prepare("SELECT archived_chunk_count FROM threads_meta WHERE id = ?").bind(threadId).first<{ archived_chunk_count: number}>();
                
                await env.MY_D1_DATABASE2.batch([
                    env.MY_D1_DATABASE2.prepare("DELETE FROM posts WHERE thread_id = ?").bind(threadId),
                    env.MY_D1_DATABASE2.prepare("DELETE FROM threads_meta WHERE id = ?").bind(threadId),
                    env.MY_D1_DATABASE2.prepare("DELETE FROM threads WHERE id = ?").bind(threadId),
                ]);

                if (threadInfo && threadInfo.archived_chunk_count > 0) {
                    const r2KeysToDelete = Array.from({ length: threadInfo.archived_chunk_count }, (_, i) => `thread/${threadId}/${i}.json`);
                    await env.MY_R2_BUCKET2.delete(r2KeysToDelete);
                }
                
                return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });
            }

            const postMatch = path.match(/^\/admin\/threads\/(\d+)\/posts\/(\d+)$/);
            if (postMatch) {
                const threadId = parseInt(postMatch[1], 10);
                const postNumber = parseInt(postMatch[2], 10);

                // D1上のレスを更新
                await env.MY_D1_DATABASE2.prepare("UPDATE posts SET body = ?, author = NULL WHERE thread_id = ? AND post_number = ?")
                    .bind(DELETED_MESSAGE, threadId, postNumber).run();

                // R2上のアーカイブを更新
                const chunkIndex = Math.floor((postNumber - 1) / CHUNK_SIZE);
                const r2Key = `thread/${threadId}/${chunkIndex}.json`;
                const r2Object = await env.MY_R2_BUCKET2.get(r2Key);
                if (r2Object) {
                    const posts = await r2Object.json<any[]>();
                    const targetPost = posts.find(p => p.post_number === postNumber);
                    if (targetPost) {
                        targetPost.body = DELETED_MESSAGE;
                        targetPost.author = null;
                        await env.MY_R2_BUCKET2.put(r2Key, JSON.stringify(posts));
                    }
                }
                return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });
            }
        }
        
        // --- 読み取り系はパブリックAPIを使うので、管理者APIには不要 ---
        // GET /admin/threads や /admin/threads/:id/all-posts は削除

        return new Response(JSON.stringify({ error: 'Admin API endpoint not found' }), { status: 404, headers: corsHeaders });
    } catch (e: any) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
    }
}