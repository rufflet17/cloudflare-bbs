// functions/api/[[path]].ts

// 1. 型定義
// -----------------------------------------------------------------------------
interface Env {
  MY_D1_DATABASE2: D1Database;
  RATE_LIMITER: RateLimiter;
  // KVの型定義は不要
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

const POSTS_CACHE_TTL = 60 * 60 * 24 * 7; // 7日間

// 2. メインハンドラ (リクエストのルーティング)
// -----------------------------------------------------------------------------
export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env, waitUntil } = context;
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  try {
    if (method === 'GET' && path === '/api/threads') {
      return await getThreads(env);
    }
    if (method === 'POST' && path === '/api/threads') {
      return await createThread(request, env);
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

async function getThreads(env: Env): Promise<Response> {
  const { results } = await env.MY_D1_DATABASE2.prepare(
    "SELECT id, title, post_count, last_updated FROM threads ORDER BY last_updated DESC LIMIT 50"
  ).all<Thread>();
  return new Response(JSON.stringify(results ?? []), { headers: { "Content-Type": "application/json" } });
}

async function getThreadInfo(env: Env, threadId: number): Promise<Response> {
    const threadInfo = await env.MY_D1_DATABASE2.prepare("SELECT id, title, post_count FROM threads WHERE id = ?").bind(threadId).first<Thread>();
    if (!threadInfo) {
      return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404 });
    }
    return new Response(JSON.stringify(threadInfo), { headers: { "Content-Type": "application/json" } });
}

async function createThread(request: Request, env: Env): Promise<Response> {
  const { title, author, body } = await request.json<{ title: string; author: string; body: string }>();
  if (!title || !body) return new Response(JSON.stringify({ error: "Title and body are required." }), { status: 400 });

  const threadResult = await env.MY_D1_DATABASE2.prepare("INSERT INTO threads (title) VALUES (?) RETURNING id").bind(title).first<{ id: number }>();
  if (!threadResult?.id) throw new Error("Failed to create a new thread.");
  
  await env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, 1, ?, ?)")
    .bind(threadResult.id, author || '名無しさん', body).run();
  
  return new Response(JSON.stringify({ id: threadResult.id }), { status: 201 });
}

async function getPostsForThread(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void, threadId: number): Promise<Response> {
  const cache = caches.default;
  const cacheKey = new Request(request.url, request); // Use a Request object as the cache key

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

async function createPost(context: EventContext<Env, any, any>, threadId: number): Promise<Response> {
  const { request, env, waitUntil } = context;

  const ip = request.headers.get("cf-connecting-ip") || "unknown";
  const { success } = await env.RATE_LIMITER.limit({ key: ip });
  if (!success) {
    return new Response(JSON.stringify({ error: "書き込みが速すぎます。1秒に1回までです。" }), { status: 429 });
  }

  const { author, body } = await request.json<{ author: string; body: string }>();
  if (!body) return new Response(JSON.stringify({ error: "Body is required." }), { status: 400 });

  const [ updateResult, postResult ] = await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("UPDATE threads SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ?").bind(threadId),
    env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) SELECT ?, post_count + 1, ?, ? FROM threads WHERE id = ? RETURNING *").bind(threadId, author || '名無しさん', body, threadId)
  ]);
  const newPost = postResult.results[0] as Post;
  if (!newPost) throw new Error("Failed to create post.");

  const cache = caches.default;
  const getRequestUrl = new URL(request.url);
  getRequestUrl.pathname = `/api/threads/${threadId}/posts`;
  getRequestUrl.search = '';
  
  waitUntil(cache.delete(getRequestUrl.toString()));
  console.log(`Cache purged for ${getRequestUrl.toString()}`);
  
  return new Response(JSON.stringify({ success: true, post: newPost }), { status: 201 });
}