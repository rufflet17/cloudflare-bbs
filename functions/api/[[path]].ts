// functions/api/[[path]].ts

// 1. 型定義
// -----------------------------------------------------------------------------
interface Env {
  MY_D1_DATABASE2: D1Database;
  MY_R2_BUCKET2: R2Bucket;
}

interface Thread {
  id: number;
  title: string;
  post_count: number;
  last_updated: string;
  archived_chunk_count: number;
}
interface Post {
  id: number;
  thread_id: number;
  post_number: number;
  author: string;
  body: string;
  created_at: string;
}

const CHUNK_SIZE = 50;
const R2_CACHE_TTL = 60 * 60 * 24 * 365; // R2由来のデータキャッシュ: 1年
const D1_CACHE_TTL = 60 * 60 * 24 * 7;   // D1由来のデータキャッシュ: 7日間
const THREAD_LIST_CACHE_TTL = 30;       // スレッド一覧のキャッシュ時間: 30秒

// 2. メインハンドラ (リクエストのルーティング)
// -----------------------------------------------------------------------------
export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env } = context;
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  try {
    if (method === 'GET' && path === '/api/threads') {
      return await getThreads(context);
    }
    if (method === 'POST' && path === '/api/threads') {
      return await createThread(context);
    }

    const infoMatch = path.match(/^\/api\/threads\/(\d+)\/info$/);
    if (method === 'GET' && infoMatch) {
      const threadId = parseInt(infoMatch[1], 10);
      return await getThreadInfo(context, threadId);
    }
    
    const postsMatch = path.match(/^\/api\/threads\/(\d+)\/posts$/);
    if (method === 'GET' && postsMatch) {
      const threadId = parseInt(postsMatch[1], 10);
      return await getPostsForThread(context, threadId);
    }
    if (method === 'POST' && postsMatch) {
      const threadId = parseInt(postsMatch[1], 10);
      return await createPost(context, threadId);
    }
    
    return new Response(JSON.stringify({ error: 'API endpoint not found' }), { 
        status: 404, headers: { "Content-Type": "application/json" } 
    });

  } catch (e: any) {
    console.error("API Error:", e);
    const errorResponse = { error: e.message || "An internal server error occurred." };
    return new Response(JSON.stringify(errorResponse), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
};

// 3. APIロジック (個別の関数)
// -----------------------------------------------------------------------------

async function getThreads(context: EventContext<Env, any, any>): Promise<Response> {
  const { request, env, waitUntil } = context;
  const cache = caches.default;
  const cacheKey = new Request(request.url, request);

  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) return cachedResponse;

  const { results } = await env.MY_D1_DATABASE2.prepare(
    "SELECT id, title, post_count, last_updated FROM threads_meta ORDER BY last_updated DESC LIMIT 50"
  ).all<Thread>();
  
  const response = new Response(JSON.stringify(results ?? []), {
    headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${THREAD_LIST_CACHE_TTL}` }
  });

  waitUntil(cache.put(cacheKey, response.clone()));
  return response;
}

async function getThreadInfo(context: EventContext<Env, any, any>, threadId: number): Promise<Response> {
    const { env } = context;
    const threadInfo = await env.MY_D1_DATABASE2.prepare(
        "SELECT id, title, post_count FROM threads_meta WHERE id = ?"
    ).bind(threadId).first<Thread>();

    if (!threadInfo) {
      return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify(threadInfo), { headers: { "Content-Type": "application/json" } });
}

async function createThread(context: EventContext<Env, any, any>): Promise<Response> {
  const { request, env, waitUntil } = context;
  const { title, author, body } = await request.json<{ title: string; author: string; body: string }>();
  if (!title || !body) return new Response(JSON.stringify({ error: "Title and body are required." }), { status: 400, headers: { "Content-Type": "application/json" } });

  const threadResult = await env.MY_D1_DATABASE2.prepare("INSERT INTO threads (title) VALUES (?) RETURNING id").bind(title).first<{ id: number }>();
  const newThreadId = threadResult?.id;
  if (!newThreadId) throw new Error("Failed to create a new thread.");
  
  await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, 1, ?, ?)")
      .bind(newThreadId, author || '名無しさん', body),
    env.MY_D1_DATABASE2.prepare("INSERT INTO threads_meta (id, title, post_count) VALUES (?, ?, 1)")
      .bind(newThreadId, title)
  ]);
  
  const cache = caches.default;
  const threadsCacheUrl = new URL(request.url);
  threadsCacheUrl.pathname = '/api/threads';
  waitUntil(cache.delete(new Request(threadsCacheUrl.toString(), request)));

  return new Response(JSON.stringify({ id: newThreadId }), { status: 201, headers: { "Content-Type": "application/json" } });
}

async function getPostsForThread(context: EventContext<Env, any, any>, threadId: number): Promise<Response> {
  const { request, env, waitUntil } = context;
  const url = new URL(request.url);
  const chunkIndex = parseInt(url.searchParams.get('chunk') || '0', 10);

  const cache = caches.default;
  const cacheKey = new Request(request.url, request);
  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) return cachedResponse;

  const threadInfo = await env.MY_D1_DATABASE2.prepare(
      "SELECT archived_chunk_count FROM threads_meta WHERE id = ?"
  ).bind(threadId).first<{ archived_chunk_count: number }>();
  if (!threadInfo) return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404 });
  
  let posts: Post[] | null = null;
  let cacheTtl = D1_CACHE_TTL;

  if (chunkIndex < threadInfo.archived_chunk_count) {
    // R2から取得
    const r2Key = `thread/${threadId}/${chunkIndex}.json`;
    const r2Object = await env.MY_R2_BUCKET2.get(r2Key);
    if (r2Object) {
        posts = await r2Object.json<Post[]>();
        cacheTtl = R2_CACHE_TTL;
    }
  } else {
    // D1から取得
    const offset = chunkIndex * CHUNK_SIZE;
    const { results } = await env.MY_D1_DATABASE2.prepare(
        "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC LIMIT ? OFFSET ?"
    ).bind(threadId, CHUNK_SIZE, offset).all<Post>();
    posts = results;
  }

  const response = new Response(JSON.stringify(posts ?? []), {
    headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${cacheTtl}` }
  });
  
  if (posts && posts.length > 0) {
    waitUntil(cache.put(cacheKey, response.clone()));
  }
  return response;
}

async function createPost(context: EventContext<Env, any, any>, threadId: number): Promise<Response> {
  const { request, env, waitUntil } = context;
  const { author, body } = await request.json<{ author: string; body: string }>();
  if (!body) return new Response(JSON.stringify({ error: "Body is required." }), { status: 400 });

  // 1. レス数を増やし、新しいレス番号を取得
  const threadInfo = await env.MY_D1_DATABASE2.prepare(
    "UPDATE threads_meta SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ? RETURNING post_count, archived_chunk_count"
  ).bind(threadId).first<{ post_count: number, archived_chunk_count: number }>();
  if (!threadInfo) throw new Error("Failed to update thread counters.");
  
  const newPostCount = threadInfo.post_count;

  // 2. 新しい投稿をD1に挿入
  await env.MY_D1_DATABASE2.prepare(
    "INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, ?, ?, ?)"
  ).bind(threadId, newPostCount, author || '名無しさん', body).run();

  // 3. アーカイブ処理の判定と実行
  const d1PostCount = newPostCount - (threadInfo.archived_chunk_count * CHUNK_SIZE);
  if (d1PostCount > CHUNK_SIZE) {
    waitUntil(archiveChunk(context, threadId, threadInfo.archived_chunk_count));
  }

  // 4. 最新チャンクのキャッシュを手動で生成
  const latestChunkIndex = Math.floor((newPostCount - 1) / CHUNK_SIZE);
  waitUntil(updateLatestChunkCache(context, threadId, latestChunkIndex));
  
  // 5. スレッド一覧のキャッシュを削除
  const url = new URL(request.url);
  const threadsCacheUrl = `${url.protocol}//${url.host}/api/threads`;
  waitUntil(caches.default.delete(new Request(threadsCacheUrl)));
  
  // 6. フロントエンドへ最新情報を返す
  const offset = latestChunkIndex * CHUNK_SIZE;
  const { results: latestChunkPosts } = await env.MY_D1_DATABASE2.prepare(
    "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC LIMIT ? OFFSET ?"
  ).bind(threadId, CHUNK_SIZE, offset).all<Post>();

  return new Response(JSON.stringify({ new_post_count: newPostCount, latest_chunk_posts: latestChunkPosts }), { 
      status: 201, headers: { "Content-Type": "application/json" } 
  });
}

async function archiveChunk(context: EventContext<Env, any, any>, threadId: number, chunkToArchive: number) {
    const { env } = context;
    console.log(`Archiving chunk ${chunkToArchive} for thread ${threadId}`);
    try {
        const offset = chunkToArchive * CHUNK_SIZE;
        const { results, success } = await env.MY_D1_DATABASE2.prepare(
            "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC LIMIT ? OFFSET ?"
        ).bind(threadId, CHUNK_SIZE, offset).all<Post>();

        if (!success || !results || results.length === 0) {
            console.error(`No posts found to archive for chunk ${chunkToArchive}`);
            return;
        }

        const r2Key = `thread/${threadId}/${chunkToArchive}.json`;
        await env.MY_R2_BUCKET2.put(r2Key, JSON.stringify(results));
        
        const postNumbersToDelete = results.map(p => p.post_number);
        const placeholders = postNumbersToDelete.map(() => '?').join(',');
        await env.MY_D1_DATABASE2.prepare(
            `DELETE FROM posts WHERE thread_id = ? AND post_number IN (${placeholders})`
        ).bind(threadId, ...postNumbersToDelete).run();

        await env.MY_D1_DATABASE2.prepare(
            "UPDATE threads_meta SET archived_chunk_count = archived_chunk_count + 1 WHERE id = ?"
        ).bind(threadId).run();

        const url = new URL(context.request.url);
        const cacheUrl = `${url.protocol}//${url.host}/api/threads/${threadId}/posts?chunk=${chunkToArchive}`;
        await caches.default.delete(new Request(cacheUrl));
        console.log(`Archived and purged cache for chunk ${chunkToArchive}`);

    } catch (e: any) {
        console.error(`Failed to archive chunk ${chunkToArchive} for thread ${threadId}:`, e.message);
    }
}

async function updateLatestChunkCache(context: EventContext<Env, any, any>, threadId: number, latestChunkIndex: number) {
    const { env, request } = context;
    const offset = latestChunkIndex * CHUNK_SIZE;
    const { results } = await env.MY_D1_DATABASE2.prepare(
        "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC LIMIT ? OFFSET ?"
    ).bind(threadId, CHUNK_SIZE, offset).all<Post>();
    
    if (!results) return;

    const url = new URL(request.url);
    const cacheUrl = `${url.protocol}//${url.host}/api/threads/${threadId}/posts?chunk=${latestChunkIndex}`;
    const cacheKey = new Request(cacheUrl, request);
    
    const cacheResponse = new Response(JSON.stringify(results), {
        headers: {
            "Content-Type": "application/json",
            "Cache-Control": `public, max-age=${D1_CACHE_TTL}`
        }
    });
    await caches.default.put(cacheKey, cacheResponse);
    console.log(`Updated cache for latest chunk ${latestChunkIndex}`);
}