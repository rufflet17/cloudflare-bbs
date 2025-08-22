// cloudflare-bbs/functions/admin/[[path]].ts

interface Env {
  MY_D1_DATABASE2: D1Database;
  MY_R2_BUCKET2: R2Bucket;
  ADMIN_API_KEY: string; // 環境変数として設定するAPIキー
}

// CORSヘッダー: localhostからのアクセスを許可
const corsHeaders = {
  'Access-Control-Allow-Origin': 'http://localhost:3000',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-control-Allow-Headers': 'Content-Type, X-Admin-API-Key',
};

const handleOptions = (request: Request) => {
    return new Response(null, { headers: corsHeaders });
}

export const onRequest: PagesFunction<Env> = async (context) => {
    const { request, env } = context;
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') {
        return handleOptions(request);
    }
    
    // === セキュリティチェック ===
    const apiKey = request.headers.get('X-Admin-API-Key');
    if (!apiKey || apiKey !== env.ADMIN_API_KEY) {
        return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401 });
    }

    try {
        if (method === 'GET') {
            if (path === '/admin/threads') {
                const { results } = await env.MY_D1_DATABASE2.prepare(
                    "SELECT id, title, genre FROM threads_meta ORDER BY id DESC"
                ).all();
                return new Response(JSON.stringify(results ?? []), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
            }
            
            const allPostsMatch = path.match(/^\/admin\/threads\/(\d+)\/all-posts$/);
            if (allPostsMatch) {
                const threadId = parseInt(allPostsMatch[1], 10);
                const threadInfo = await env.MY_D1_DATABASE2.prepare(
                    "SELECT archived_chunk_count, post_count FROM threads_meta WHERE id = ?"
                ).bind(threadId).first<{ archived_chunk_count: number; post_count: number }>();

                if (!threadInfo) {
                    return new Response(JSON.stringify({ error: 'Thread not found' }), { status: 404, headers: corsHeaders });
                }

                let allPosts = [];
                if (threadInfo.archived_chunk_count > 0) {
                    for (let i = 0; i < threadInfo.archived_chunk_count; i++) {
                        const r2Key = `thread/${threadId}/${i}.json`;
                        const r2Object = await env.MY_R2_BUCKET2.get(r2Key);
                        if (r2Object) allPosts.push(...await r2Object.json<any[]>());
                    }
                }
                if (threadInfo.post_count > allPosts.length) {
                    const { results } = await env.MY_D1_DATABASE2.prepare(
                        "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC"
                    ).bind(threadId).all();
                    if (results) allPosts.push(...results);
                }
                allPosts.sort((a, b) => a.post_number - b.post_number);
                return new Response(JSON.stringify(allPosts), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
            }
        }
        
        if (method === 'POST') {
            const genreMatch = path.match(/^\/admin\/threads\/(\d+)\/genre$/);
            if (genreMatch) {
                const threadId = parseInt(genreMatch[1], 10);
                const { genre } = await request.json<{ genre: string }>();
                if (!genre || !/^[a-z0-9_-]+$/.test(genre)) {
                    return new Response(JSON.stringify({ error: 'Invalid genre format' }), { status: 400, headers: corsHeaders });
                }
                await env.MY_D1_DATABASE2.prepare(
                    "UPDATE threads_meta SET genre = ? WHERE id = ?"
                ).bind(genre, threadId).run();
                return new Response(JSON.stringify({ success: true, message: `スレッドID:${threadId}のジャンルを"${genre}"に変更しました。` }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
            }
        }

        return new Response(JSON.stringify({ error: 'Admin API endpoint not found' }), { status: 404, headers: corsHeaders });

    } catch (e: any) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
    }
}