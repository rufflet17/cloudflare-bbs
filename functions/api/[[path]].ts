// functions/api/[[path]].ts

// 1. 型定義
// -----------------------------------------------------------------------------
interface Env {
  MY_D1_DATABASE2: D1Database;
  MY_KV_NAMESPACE2: KVNamespace;
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
    const threadMatch = path.match(/^\/api\/threads\/(\d+)$/);
    if (method === 'GET' && threadMatch) {
      const threadId = parseInt(threadMatch[1], 10);
      return await getThreadById(request, env, waitUntil, threadId);
    }
    const postMatch = path.match(/^\/api\/threads\/(\d+)\/posts$/);
    if (method === 'POST' && postMatch) {
      const threadId = parseInt(postMatch[1], 10);
      return await createPost(request, env, waitUntil, threadId);
    }
    return new Response('API endpoint not found', { status: 404 });

  } catch (e: any) {
    console.error("API Error:", e);
    const errorResponse = {
      error: e.message || "An internal server error occurred.",
      cause: e.cause?.message,
    };
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
  
  return new Response(JSON.stringify(results ?? []), {
    headers: { "Content-Type": "application/json" }
  });
}

async function createThread(request: Request, env: Env): Promise<Response> {
  const { title, author, body } = await request.json<{ title: string; author: string; body: string }>();

  if (!title || !body) {
    return new Response(JSON.stringify({ error: "Title and body are required." }), { status: 400 });
  }

  const threadResult = await env.MY_D1_DATABASE2
    .prepare("INSERT INTO threads (title) VALUES (?) RETURNING id")
    .bind(title)
    .first<{ id: number }>();

  if (!threadResult || typeof threadResult.id !== 'number') {
    throw new Error("Failed to create a new thread and retrieve its ID.");
  }
  const newThreadId = threadResult.id;

  await env.MY_D1_DATABASE2
    .prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, 1, ?, ?)")
    .bind(newThreadId, author || '名無しさん', body)
    .run();
  
  return new Response(JSON.stringify({ id: newThreadId }), {
    status: 201,
    headers: { "Content-Type": "application/json" }
  });
}

/**
 * 特定のスレッドと投稿一覧を取得する (キャッシュキーを修正)
 */
async function getThreadById(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void, threadId: number): Promise<Response> {
  const cache = caches.default;
  
  // ★★★ 修正点 1: キャッシュキーをリクエストURLの文字列にする ★★★
  const cacheKey = request.url;
  
  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    console.log(`Cache hit for thread ${threadId}`);
    return cachedResponse;
  }
  console.log(`Cache miss for thread ${threadId}`);

  const threadInfo = await env.MY_D1_DATABASE2.prepare("SELECT * FROM threads WHERE id = ?").bind(threadId).first<Thread>();
  if (!threadInfo) {
    return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404 });
  }

  let snapshotPosts: Post[] = [];
  let lastSnapshotNumber = 0;
  for (let i = Math.floor(threadInfo.post_count / 100) * 100; i > 0; i -= 100) {
    const kvKey = `thread-${threadId}-snapshot-${i}`;
    const snapshotJson = await env.MY_KV_NAMESPACE2.get(kvKey);
    if (snapshotJson) {
      snapshotPosts = JSON.parse(snapshotJson);
      lastSnapshotNumber = i;
      break;
    }
  }

  const { results: newPosts } = await env.MY_D1_DATABASE2.prepare(
    "SELECT * FROM posts WHERE thread_id = ? AND post_number > ? ORDER BY post_number ASC"
  ).bind(threadId, lastSnapshotNumber).all<Post>();
  
  const allPosts = [...snapshotPosts, ...newPosts];
  const responsePayload = { thread: threadInfo, posts: allPosts };
  const response = new Response(JSON.stringify(responsePayload), {
    headers: { "Content-Type": "application/json" }
  });
  
  response.headers.set("Cache-Control", "public, max-age=60");
  
  // ★★★ 修正点 2: 保存時も同じURL文字列をキーにする ★★★
  waitUntil(cache.put(cacheKey, response.clone()));

  return response;
}

/**
 * スレッドに新しい投稿を追加する (キャッシュキーを修正)
 */
async function createPost(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void, threadId: number): Promise<Response> {
  const { author, body } = await request.json<{ author: string; body: string }>();
  if (!body) {
    return new Response(JSON.stringify({ error: "Body is required." }), { status: 400 });
  }

  const [updateResult] = await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("UPDATE threads SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ? RETURNING post_count").bind(threadId)
  ]);
  const newPostCount = updateResult.results[0].post_count as number;

  await env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, ?, ?, ?)")
    .bind(threadId, newPostCount, author || '名無しさん', body)
    .run();
  
  if (newPostCount > 0 && newPostCount % 100 === 0) {
    waitUntil(createAndStoreSnapshot(env, threadId, newPostCount));
  }
  
  // ★★★ 修正点 3: キャッシュ削除時に、GETリクエストと同一のURL文字列を生成してキーにする ★★★
  const cache = caches.default;
  const getRequestUrl = new URL(request.url);
  getRequestUrl.pathname = `/api/threads/${threadId}`;
  waitUntil(cache.delete(getRequestUrl.toString()));
  
  return new Response(JSON.stringify({ success: true, postCount: newPostCount }), { status: 201 });
}


// 4. ヘルパー関数
// -----------------------------------------------------------------------------
async function createAndStoreSnapshot(env: Env, threadId: number, postCount: number): Promise<void> {
  try {
    const { results } = await env.MY_D1_DATABASE2.prepare(
      "SELECT * FROM posts WHERE thread_id = ? AND post_number <= ? ORDER BY post_number ASC"
    ).bind(threadId, postCount).all<Post>();
    
    if (results && results.length > 0) {
      const kvKey = `thread-${threadId}-snapshot-${postCount}`;
      await env.MY_KV_NAMESPACE2.put(kvKey, JSON.stringify(results), { expirationTtl: 86400 * 7 });
      console.log(`Successfully created snapshot for thread ${threadId} at post ${postCount}`);
    }
  } catch (e) {
    console.error(`Failed to create snapshot for thread ${threadId}:`, e);
  }
}