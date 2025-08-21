// functions/api/[[path]].ts

// 1. 型定義
// -----------------------------------------------------------------------------
interface Env {
  MY_D1_DATABASE2: D1Database;
}

interface Thread {
  id: number;
  title: string;
  post_count: number;
  last_updated: string;
}
interface Post {
  id: number;
  thread_id: number;
  post_number: number;
  author: string;
  body: string;
  created_at: string;
}

const POSTS_CACHE_TTL = 60 * 60 * 24 * 7; // 個別スレッドの投稿一覧キャッシュ: 7日間
const THREAD_LIST_CACHE_TTL = 30; // スレッド一覧のキャッシュ時間: 30秒

// 2. メインハンドラ (リクエストのルーティング)
// -----------------------------------------------------------------------------
export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env, waitUntil } = context;
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  try {
    if (method === 'GET' && path === '/api/threads') {
      return await getThreads(request, env, waitUntil);
    }
    if (method === 'POST' && path === '/api/threads') {
      return await createThread(request, env, waitUntil);
    }

    const infoMatch = path.match(/^\/api\/threads\/(\d+)\/info$/);
    if (method === 'GET' && infoMatch) {
      const threadId = parseInt(infoMatch[1], 10);
      return await getThreadInfo(env, threadId);
    }
    
    const postsMatch = path.match(/^\/api\/threads\/(\d+)\/posts$/);
    if (method === 'GET' && postsMatch) {
      const threadId = parseInt(postsMatch[1], 10);
      return await getPostsForThread(request, env, waitUntil, threadId);
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

async function getThreads(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void): Promise<Response> {
  const cache = caches.default;
  const cacheKey = new Request(request.url, request);

  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    console.log("Cache hit for thread list");
    return cachedResponse;
  }
  console.log("Cache miss for thread list");

  const { results } = await env.MY_D1_DATABASE2.prepare(
    "SELECT id, title, post_count, last_updated FROM threads_meta ORDER BY last_updated DESC LIMIT 50"
  ).all<Thread>();
  
  const response = new Response(JSON.stringify(results ?? []), {
    headers: { "Content-Type": "application/json" }
  });

  response.headers.set("Cache-Control", `public, max-age=${THREAD_LIST_CACHE_TTL}`);
  waitUntil(cache.put(cacheKey, response.clone()));
  
  return response;
}

async function getThreadInfo(env: Env, threadId: number): Promise<Response> {
    const threadInfo = await env.MY_D1_DATABASE2.prepare("SELECT id, title, post_count FROM threads WHERE id = ?").bind(threadId).first<Thread>();
    if (!threadInfo) {
      return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify(threadInfo), { headers: { "Content-Type": "application/json" } });
}

async function createThread(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void): Promise<Response> {
  const { title, author, body } = await request.json<{ title: string; author: string; body: string }>();
  if (!title || !body) return new Response(JSON.stringify({ error: "Title and body are required." }), { status: 400, headers: { "Content-Type": "application/json" } });

  const threadResult = await env.MY_D1_DATABASE2.prepare("INSERT INTO threads (title) VALUES (?) RETURNING id").bind(title).first<{ id: number }>();
  const newThreadId = threadResult?.id;
  if (!newThreadId) throw new Error("Failed to create a new thread.");
  
  await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, 1, ?, ?)")
      .bind(newThreadId, author || '名無しさん', body),
    env.MY_D1_DATABASE2.prepare("INSERT INTO threads_meta (id, title) VALUES (?, ?)")
      .bind(newThreadId, title)
  ]);
  
  const cache = caches.default;
  const threadsCacheUrl = new URL(request.url);
  threadsCacheUrl.pathname = '/api/threads';
  threadsCacheUrl.search = '';
  waitUntil(cache.delete(threadsCacheUrl.toString()));
  console.log(`Cache purged for thread list: ${threadsCacheUrl.toString()}`);

  return new Response(JSON.stringify({ id: newThreadId }), { status: 201, headers: { "Content-Type": "application/json" } });
}

async function getPostsForThread(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void, threadId: number): Promise<Response> {
  const cache = caches.default;
  const cacheKey = new Request(request.url, request);

  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    console.log(`Cache hit for posts in thread ${threadId}`);
    return cachedResponse;
  }
  console.log(`Cache miss for posts in thread ${threadId}`);

  const { results: allPosts } = await env.MY_D1_DATABASE2.prepare(
    "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC"
  ).bind(threadId).all<Post>();
  
  const response = new Response(JSON.stringify(allPosts ?? []), {
    headers: { "Content-Type": "application/json" }
  });
  
  response.headers.set("Cache-Control", `public, max-age=${POSTS_CACHE_TTL}`);
  waitUntil(cache.put(cacheKey, response.clone()));

  return response;
}

// =================================================================
// ▼▼▼ ここからが変更された関数です ▼▼▼
// =================================================================
async function createPost(context: EventContext<Env, any, any>, threadId: number): Promise<Response> {
  const { request, env, waitUntil } = context;
  
  const { author, body } = await request.json<{ author: string; body: string }>();
  if (!body) return new Response(JSON.stringify({ error: "Body is required." }), { status: 400, headers: { "Content-Type": "application/json" } });

  // 1. スレッドのメタ情報を更新し、新しい投稿番号を取得
  const batchResults = await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("UPDATE threads SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ? RETURNING post_count").bind(threadId),
    env.MY_D1_DATABASE2.prepare("UPDATE threads_meta SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ?").bind(threadId),
  ]);

  const newPostCount = batchResults[0].results[0]?.post_count as number;
  if (!newPostCount) throw new Error("Failed to update thread counters. Thread might not exist.");

  // 2. 新しい投稿をDBに挿入
  await env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, ?, ?, ?)")
    .bind(threadId, newPostCount, author || '名無しさん', body)
    .run();

  // 3. 関連キャッシュを削除
  const cache = caches.default;
  
  const postsCacheUrl = new URL(request.url);
  postsCacheUrl.pathname = `/api/threads/${threadId}/posts`;
  postsCacheUrl.search = '';
  waitUntil(cache.delete(postsCacheUrl.toString()));
  console.log(`Cache purged for posts: ${postsCacheUrl.toString()}`);

  const threadsCacheUrl = new URL(request.url);
  threadsCacheUrl.pathname = '/api/threads';
  threadsCacheUrl.search = '';
  waitUntil(cache.delete(threadsCacheUrl.toString()));
  console.log(`Cache purged for thread list: ${threadsCacheUrl.toString()}`);
  
  // 4. 更新後の最新の投稿リスト全体をDBから再取得
  const { results: allPosts } = await env.MY_D1_DATABASE2.prepare(
    "SELECT * FROM posts WHERE thread_id = ? ORDER BY post_number ASC"
  ).bind(threadId).all<Post>();

  // 5. 最新の投稿リストをフロントエンドに返却
  return new Response(JSON.stringify(allPosts ?? []), { 
      status: 201, 
      headers: { "Content-Type": "application/json" } 
  });
}
// =================================================================
// ▲▲▲ ここまでが変更された関数です ▲▲▲
// =================================================================