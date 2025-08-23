// 1. 型定義
// -----------------------------------------------------------------------------
interface Env {
  MY_D1_DATABASE2: D1Database;
  MY_R2_BUCKET2: R2Bucket;
}

interface Thread {
  id: string;
  title: string;
  genre: string;
  post_count: number;
  last_updated: string;
  archived_chunk_count: number;
}
interface Post {
  id: number;
  thread_id: string;
  post_number: number;
  author: string;
  body: string;
  created_at: string;
}

const CHUNK_SIZE = 50;
const R2_CACHE_TTL = 60 * 60 * 24 * 365; // R2由来のデータキャッシュ: 1年
const D1_CACHE_TTL = 60 * 60 * 24 * 7;   // D1由来のデータキャッシュ: 7日間
const THREAD_LIST_CACHE_TTL = 2;       // スレッド一覧のキャッシュ時間: 2秒

/**
 * URLの末尾のスラッシュを削除して正規化する
 * @param url - URL文字列またはURLオブジェクト
 * @returns 正規化されたURLオブジェクト
 */
function normalizeUrl(url: string | URL): URL {
  const urlObj = new URL(url);
  // パス名の末尾がスラッシュで、かつルートパス('/')でない場合にスラッシュを削除
  if (urlObj.pathname.length > 1 && urlObj.pathname.endsWith('/')) {
    urlObj.pathname = urlObj.pathname.slice(0, -1);
  }
  return urlObj;
}

// 2. メインハンドラ (リクエストのルーティング)
// -----------------------------------------------------------------------------
export const onRequest: PagesFunction<Env> = async (context) => {
  const { request } = context;
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  try {
    // GETリクエストのルーティング
    if (method === 'GET') {
      if (path.startsWith('/api/genres')) { // startsWithで末尾スラッシュに対応
        return await getGenres(context);
      }
      if (path.startsWith('/api/threads')) { // startsWithで末尾スラッシュに対応
        const infoMatch = path.match(/^\/api\/threads\/([^/]+)\/info/);
        if (infoMatch) {
            const threadId = infoMatch[1];
            return await getThreadInfo(context, threadId);
        }
        const postsMatch = path.match(/^\/api\/threads\/([^/]+)\/posts/);
        if (postsMatch) {
            const threadId = postsMatch[1];
            return await getPostsForThread(context, threadId);
        }
        // 他にマッチしない場合はスレッド一覧
        return await getThreads(context);
      }
    }

    // POSTリクエストのルーティング
    if (method === 'POST') {
      const body = await request.json<any>();

      if (path.startsWith('/api/threads')) { // startsWithで末尾スラッシュに対応
          const postsMatch = path.match(/^\/api\/threads\/([^/]+)\/posts/);
          if (postsMatch) {
            const threadId = postsMatch[1];
            return await createPost(context, threadId, body);
          }
          // 他にマッチしない場合はスレッド作成
          return await createThread(context, body);
      }
    }
    
    return new Response(JSON.stringify({ error: 'API endpoint not found' }), { 
        status: 404, headers: { "Content-Type": "application/json" } 
    });

  } catch (e: any) {
    if (e instanceof TypeError && e.message.includes('disturbed')) {
        console.error("API Error: Attempted to read request body twice.", e);
        return new Response(JSON.stringify({ error: "Internal server error: Could not process request body."}), { status: 500 });
    }
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

async function getGenres(context: EventContext<Env, any, any>): Promise<Response> {
    const { env } = context;
    const { results } = await env.MY_D1_DATABASE2.prepare(
        "SELECT DISTINCT genre FROM threads_meta WHERE genre IS NOT NULL"
    ).all<{ genre: string }>();
    return new Response(JSON.stringify(results ?? []), {
        headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${THREAD_LIST_CACHE_TTL}` }
    });
}

async function getThreads(context: EventContext<Env, any, any>): Promise<Response> {
  const { request, env, waitUntil } = context;
  const normalizedUrl = normalizeUrl(request.url);
  const genre = normalizedUrl.searchParams.get('genre');

  const cache = caches.default;
  const cacheKey = new Request(normalizedUrl.toString(), request);

  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) return cachedResponse;

  let query;
  if (genre) {
    query = env.MY_D1_DATABASE2.prepare(
        "SELECT id, title, post_count, last_updated FROM threads_meta WHERE genre = ? ORDER BY last_updated DESC LIMIT 50"
    ).bind(genre);
  } else {
    query = env.MY_D1_DATABASE2.prepare(
        "SELECT id, title, post_count, last_updated FROM threads_meta ORDER BY last_updated DESC LIMIT 50"
    );
  }
  const { results } = await query.all<Thread>();
  
  const response = new Response(JSON.stringify(results ?? []), {
    headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${THREAD_LIST_CACHE_TTL}` }
  });

  waitUntil(cache.put(cacheKey, response.clone()));
  return response;
}

async function getThreadInfo(context: EventContext<Env, any, any>, threadId: string): Promise<Response> {
    const { env } = context;
    const threadInfo = await env.MY_D1_DATABASE2.prepare(
        "SELECT id, title, genre, post_count FROM threads_meta WHERE id = ?"
    ).bind(threadId).first<Thread>();

    if (!threadInfo) {
      return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify(threadInfo), { headers: { "Content-Type": "application/json" } });
}

async function createThread(context: EventContext<Env, any, any>, body: { genre: string; title: string; author: string; body: string }): Promise<Response> {
  const { request, env, waitUntil } = context;
  const { genre, title, author, body: postBody } = body;
  if (!genre || !title || !postBody) return new Response(JSON.stringify({ error: "Genre, title and body are required." }), { status: 400 });

  const newThreadId = crypto.randomUUID();

  // 関連テーブルで使うため、まずthreadsテーブルに挿入（スキーマでidがTEXT PRIMARY KEYになっている必要があります）
  await env.MY_D1_DATABASE2.prepare("INSERT INTO threads (id, title) VALUES (?, ?)").bind(newThreadId, title).run();
  
  await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, 1, ?, ?)")
      .bind(newThreadId, author || '名無しさん', postBody),
    env.MY_D1_DATABASE2.prepare("INSERT INTO threads_meta (id, title, genre, post_count) VALUES (?, ?, ?, 1)")
      .bind(newThreadId, title, genre)
  ]);
  
  const cache = caches.default;
  const url = new URL(request.url);
  
  const genreThreadsCacheUrl = normalizeUrl(`${url.protocol}//${url.host}/api/threads?genre=${genre}`);
  const allThreadsCacheUrl = normalizeUrl(`${url.protocol}//${url.host}/api/threads`);
  
  waitUntil(Promise.all([
      cache.delete(new Request(genreThreadsCacheUrl.toString())),
      cache.delete(new Request(allThreadsCacheUrl.toString()))
  ]));

  return new Response(JSON.stringify({ id: newThreadId }), { status: 201, headers: { "Content-Type": "application/json" } });
}

async function getPostsForThread(context: EventContext<Env, any, any>, threadId: string): Promise<Response> {
  const { request, env, waitUntil } = context;
  const normalizedUrl = normalizeUrl(request.url);
  const chunkIndex = parseInt(normalizedUrl.searchParams.get('chunk') || '0', 10);

  const cache = caches.default;
  const cacheKey = new Request(normalizedUrl.toString(), request);
  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) return cachedResponse;

  const threadInfo = await env.MY_D1_DATABASE2.prepare(
      "SELECT archived_chunk_count FROM threads_meta WHERE id = ?"
  ).bind(threadId).first<{ archived_chunk_count: number }>();
  if (!threadInfo) return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404 });
  
  let posts: Post[] | null = null;
  let cacheTtl = D1_CACHE_TTL;

  if (chunkIndex < threadInfo.archived_chunk_count) {
    const r2Key = `thread/${threadId}/${chunkIndex}.json`;
    const r2Object = await env.MY_R2_BUCKET2.get(r2Key);
    if (r2Object) {
        posts = await r2Object.json<Post[]>();
        cacheTtl = R2_CACHE_TTL;
    }
  } else {
    const d1Offset = (chunkIndex - threadInfo.archived_chunk_count) * CHUNK_SIZE;
    const { results } = await env.MY_D1_DATABASE2.prepare(
        "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC LIMIT ? OFFSET ?"
    ).bind(threadId, CHUNK_SIZE, d1Offset).all<Post>();
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

async function createPost(context: EventContext<Env, any, any>, threadId: string, body: { author: string; body: string }): Promise<Response> {
  const { request, env, waitUntil } = context;
  const { author, body: postBody } = body;
  if (!postBody) return new Response(JSON.stringify({ error: "Body is required." }), { status: 400 });

  const threadInfo = await env.MY_D1_DATABASE2.prepare(
    "UPDATE threads_meta SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ? RETURNING post_count, archived_chunk_count, genre"
  ).bind(threadId).first<{ post_count: number, archived_chunk_count: number, genre: string }>();
  if (!threadInfo) throw new Error("Failed to update thread counters.");
  
  const newPostCount = threadInfo.post_count;

  await env.MY_D1_DATABASE2.prepare(
    "INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, ?, ?, ?)"
  ).bind(threadId, newPostCount, author || '名無しさん', postBody).run();

  const d1PostCount = newPostCount - (threadInfo.archived_chunk_count * CHUNK_SIZE);
  if (d1PostCount > CHUNK_SIZE) {
    waitUntil(archiveChunk(context, threadId, threadInfo.archived_chunk_count));
  }

  const latestChunkIndex = Math.floor((newPostCount - 1) / CHUNK_SIZE);
  waitUntil(invalidateChunkCache(context, threadId, latestChunkIndex));
  
  const url = new URL(request.url);
  const genreThreadsCacheUrl = normalizeUrl(`${url.protocol}//${url.host}/api/threads?genre=${threadInfo.genre}`);
  const allThreadsCacheUrl = normalizeUrl(`${url.protocol}//${url.host}/api/threads`);
   waitUntil(Promise.all([
      caches.default.delete(new Request(genreThreadsCacheUrl.toString())),
      caches.default.delete(new Request(allThreadsCacheUrl.toString()))
  ]));
  
  const d1Offset = (latestChunkIndex - threadInfo.archived_chunk_count) * CHUNK_SIZE;
  const { results: latestChunkPosts } = await env.MY_D1_DATABASE2.prepare(
    "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC LIMIT ? OFFSET ?"
  ).bind(threadId, CHUNK_SIZE, d1Offset).all<Post>();

  return new Response(JSON.stringify({ new_post_count: newPostCount, latest_chunk_posts: latestChunkPosts }), { 
      status: 201, headers: { "Content-Type": "application/json" } 
  });
}

async function archiveChunk(context: EventContext<Env, any, any>, threadId: string, chunkToArchive: number) {
    const { env, request } = context;
    console.log(`Archiving chunk ${chunkToArchive} for thread ${threadId}`);
    try {
        const threadInfo = await env.MY_D1_DATABASE2.prepare(
            "SELECT archived_chunk_count FROM threads_meta WHERE id = ?"
        ).bind(threadId).first<{ archived_chunk_count: number }>();
        if (!threadInfo) return;
        
        const d1Offset = (chunkToArchive - threadInfo.archived_chunk_count) * CHUNK_SIZE;

        const { results, success } = await env.MY_D1_DATABASE2.prepare(
            "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC LIMIT ? OFFSET ?"
        ).bind(threadId, CHUNK_SIZE, d1Offset).all<Post>();

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

        const url = new URL(request.url);
        const cacheUrl = normalizeUrl(`${url.protocol}//${url.host}/api/threads/${threadId}/posts?chunk=${chunkToArchive}`);
        await caches.default.delete(new Request(cacheUrl.toString()));
        console.log(`Archived and purged cache for chunk ${chunkToArchive}`);

    } catch (e: any) {
        console.error(`Failed to archive chunk ${chunkToArchive} for thread ${threadId}:`, e.message);
    }
}

async function invalidateChunkCache(context: EventContext<Env, any, any>, threadId: string, chunkIndex: number) {
    const { request } = context;
    
    const url = new URL(request.url);
    const cacheUrl = normalizeUrl(`${url.protocol}//${url.host}/api/threads/${threadId}/posts?chunk=${chunkIndex}`);
    const cacheKey = new Request(cacheUrl.toString());
    
    await caches.default.delete(cacheKey);
    console.log(`Invalidated cache for chunk ${chunkIndex} of thread ${threadId}`);
}