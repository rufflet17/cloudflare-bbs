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

const CACHE_TTL = 60 * 60 * 24 * 7; // 7 days in seconds

// 2. メインハンドラ (リクエストのルーティング)
// -----------------------------------------------------------------------------
export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env, waitUntil } = context;
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  try {
    // スレッド一覧
    if (method === 'GET' && path === '/api/threads') {
      return await getThreads(env);
    }
    // スレッド作成
    if (method === 'POST' && path === '/api/threads') {
      return await createThread(request, env, waitUntil);
    }

    // --- 個別スレッド関連のエンドポイント ---

    // スレッド情報取得
    const infoMatch = path.match(/^\/api\/threads\/(\d+)\/info$/);
    if (method === 'GET' && infoMatch) {
      const threadId = parseInt(infoMatch[1], 10);
      return await getThreadInfo(env, threadId);
    }
    
    // キャッシュ済み投稿取得
    const getPostsMatch = path.match(/^\/api\/threads\/(\d+)\/posts$/);
    if (method === 'GET' && getPostsMatch) {
      const threadId = parseInt(getPostsMatch[1], 10);
      return await getCachedPosts(env, threadId);
    }
    
    // 投稿作成
    const createPostMatch = path.match(/^\/api\/threads\/(\d+)\/posts$/);
    if (method === 'POST' && createPostMatch) {
      const threadId = parseInt(createPostMatch[1], 10);
      return await createPost(request, env, waitUntil, threadId);
    }
    
    // 投稿の再キャッシュ要求
    const recacheMatch = path.match(/^\/api\/threads\/(\d+)\/recache$/);
    if (method === 'POST' && recacheMatch) {
        const threadId = parseInt(recacheMatch[1], 10);
        return await recachePosts(request, env, threadId);
    }
    
    return new Response(JSON.stringify({ error: 'API endpoint not found' }), { 
        status: 404, headers: { "Content-Type": "application/json" } 
    });

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

async function createThread(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void): Promise<Response> {
  const { title, author, body } = await request.json<{ title: string; author: string; body: string }>();

  if (!title || !body) {
    return new Response(JSON.stringify({ error: "Title and body are required." }), { status: 400 });
  }

  // 1. スレッドをD1に作成
  const threadResult = await env.MY_D1_DATABASE2
    .prepare("INSERT INTO threads (title) VALUES (?) RETURNING id")
    .bind(title)
    .first<{ id: number }>();

  if (!threadResult || typeof threadResult.id !== 'number') {
    throw new Error("Failed to create a new thread and retrieve its ID.");
  }
  const newThreadId = threadResult.id;

  // 2. 最初の投稿をD1に作成し、そのデータを取得
  const firstPost = await env.MY_D1_DATABASE2
    .prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, 1, ?, ?) RETURNING *")
    .bind(newThreadId, author || '名無しさん', body)
    .first<Post>();

  // 3. 最初の投稿をKVにキャッシュ
  if(firstPost) {
    const kvKey = `thread:${newThreadId}:post:${firstPost.post_number}`;
    waitUntil(env.MY_KV_NAMESPACE2.put(kvKey, JSON.stringify(firstPost), { expirationTtl: CACHE_TTL }));
  }
  
  return new Response(JSON.stringify({ id: newThreadId }), {
    status: 201,
    headers: { "Content-Type": "application/json" }
  });
}

async function getThreadInfo(env: Env, threadId: number): Promise<Response> {
    const threadInfo = await env.MY_D1_DATABASE2.prepare("SELECT id, title, post_count FROM threads WHERE id = ?").bind(threadId).first<Thread>();
    if (!threadInfo) {
      return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404 });
    }
    return new Response(JSON.stringify(threadInfo), {
      headers: { "Content-Type": "application/json" }
    });
}

async function getCachedPosts(env: Env, threadId: number): Promise<Response> {
    const list = await env.MY_KV_NAMESPACE2.list({ prefix: `thread:${threadId}:post:` });
    if (list.keys.length === 0) {
        return new Response(JSON.stringify([]), { headers: { "Content-Type": "application/json" }});
    }

    const promises = list.keys.map(key => env.MY_KV_NAMESPACE2.get(key.name));
    const values = await Promise.all(promises);
    
    const posts = values.filter(v => v !== null).map(v => JSON.parse(v!));

    return new Response(JSON.stringify(posts), { headers: { "Content-Type": "application/json" }});
}


async function createPost(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void, threadId: number): Promise<Response> {
  const { author, body } = await request.json<{ author: string; body: string }>();
  if (!body) {
    return new Response(JSON.stringify({ error: "Body is required." }), { status: 400 });
  }

  // 1. トランザクションでD1を更新・挿入し、新しい投稿データを取得
  const [ updateResult, postResult ] = await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("UPDATE threads SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ? RETURNING post_count").bind(threadId),
    env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) SELECT ?, post_count, ?, ? FROM threads WHERE id = ? RETURNING *").bind(threadId, author || '名無しさん', body, threadId)
  ]);
  
  const newPost = postResult.results[0] as Post;
  
  if (!newPost) {
      throw new Error("Failed to create post.");
  }

  // 2. 新しい投稿をKVにキャッシュ
  const kvKey = `thread:${threadId}:post:${newPost.post_number}`;
  waitUntil(env.MY_KV_NAMESPACE2.put(kvKey, JSON.stringify(newPost), { expirationTtl: CACHE_TTL }));
  
  return new Response(JSON.stringify({ success: true, post: newPost }), { status: 201 });
}

async function recachePosts(request: Request, env: Env, threadId: number): Promise<Response> {
    const { numbers } = await request.json<{ numbers: number[] }>();
    if (!Array.isArray(numbers) || numbers.length === 0) {
        return new Response(JSON.stringify({ error: "Post numbers must be a non-empty array." }), { status: 400 });
    }
    
    // D1はIN句のプレースホルダーを1つしか受け付けないため、文字列を組み立てる必要がある
    const placeholders = numbers.map(() => '?').join(',');
    const query = `SELECT * FROM posts WHERE thread_id = ? AND post_number IN (${placeholders})`;
    
    const { results: postsToCache } = await env.MY_D1_DATABASE2
      .prepare(query)
      .bind(threadId, ...numbers)
      .all<Post>();
      
    if (postsToCache && postsToCache.length > 0) {
        const promises = postsToCache.map(post => {
            const kvKey = `thread:${threadId}:post:${post.post_number}`;
            return env.MY_KV_NAMESPACE2.put(kvKey, JSON.stringify(post), { expirationTtl: CACHE_TTL });
        });
        await Promise.all(promises);
    }
    
    return new Response(JSON.stringify({ success: true, recachedCount: postsToCache?.length ?? 0 }));
}