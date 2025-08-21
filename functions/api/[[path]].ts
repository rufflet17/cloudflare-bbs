// functions/api/[[path]].ts

// 1. 型定義と定数
// -----------------------------------------------------------------------------
interface Env {
  MY_D1_DATABASE2: D1Database;
  MY_R2_BUCKET2: R2Bucket; // R2バインディング名を指定
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

const CHUNK_SIZE = 50; // 1チャンクあたりのレス数
const CACHE_TTL = 60 * 60 * 24 * 7; // キャッシュ期間: 7日間
const THREAD_LIST_CACHE_TTL = 30;

// ヘルパー関数: 投稿番号からチャンクインデックスを計算
const getChunkIndex = (postNumber: number) => Math.floor((postNumber - 1) / CHUNK_SIZE);
// ヘルパー関数: R2のオブジェクトキーを生成
const getR2Key = (threadId: number, chunkIndex: number) => `posts/${threadId}/chunk-${chunkIndex}.json`;

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
    
    // チャンク単位で投稿を取得する新しいエンドポイント
    const postsChunkMatch = path.match(/^\/api\/threads\/(\d+)\/posts\/(\d+)$/);
    if (method === 'GET' && postsChunkMatch) {
      const threadId = parseInt(postsChunkMatch[1], 10);
      const chunkIndex = parseInt(postsChunkMatch[2], 10);
      return await getPostsChunk(request, env, waitUntil, threadId, chunkIndex);
    }
    
    // 投稿作成エンドポイント
    const postsMatch = path.match(/^\/api\/threads\/(\d+)\/posts$/);
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

async function getPostsChunk(request: Request, env: Env, waitUntil: (promise: Promise<any>) => void, threadId: number, chunkIndex: number): Promise<Response> {
  const cache = caches.default;
  const cacheKey = new Request(request.url, request);

  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    console.log(`Cache hit for thread ${threadId}, chunk ${chunkIndex}`);
    return cachedResponse;
  }
  console.log(`Cache miss for thread ${threadId}, chunk ${chunkIndex}`);

  const threadInfo = await env.MY_D1_DATABASE2.prepare("SELECT post_count FROM threads WHERE id = ?").bind(threadId).first<{ post_count: number }>();
  if (!threadInfo) return new Response(JSON.stringify({ error: "Thread not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
  
  const currentChunkIndex = getChunkIndex(threadInfo.post_count > 0 ? threadInfo.post_count : 1);
  let posts: Post[] | null = null;
  
  if (chunkIndex === currentChunkIndex) {
    console.log(`Fetching chunk ${chunkIndex} from D1 for thread ${threadId}`);
    const { results } = await env.MY_D1_DATABASE2.prepare(
      "SELECT * FROM posts WHERE thread_id = ? AND post_number > ? ORDER BY post_number ASC"
    ).bind(threadId, chunkIndex * CHUNK_SIZE).all<Post>();
    posts = results;
  } else if (chunkIndex < currentChunkIndex) {
    console.log(`Fetching chunk ${chunkIndex} from R2 for thread ${threadId}`);
    const r2Key = getR2Key(threadId, chunkIndex);
    const object = await env.MY_R2_BUCKET2.get(r2Key); // R2 binding name
    if (object) {
      posts = await object.json<Post[]>();
    }
  }

  if (!posts || posts.length === 0) {
    return new Response(JSON.stringify({ error: "Posts not found for this chunk" }), { status: 404, headers: { "Content-Type": "application/json" } });
  }

  const response = new Response(JSON.stringify(posts), {
    headers: { 
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${CACHE_TTL}`
    }
  });
  waitUntil(cache.put(cacheKey, response.clone()));
  
  return response;
}

async function createPost(context: EventContext<Env, any, any>, threadId: number): Promise<Response> {
  const { request, env, waitUntil } = context;
  const { author, body } = await request.json<{ author: string; body: string }>();
  if (!body) return new Response(JSON.stringify({ error: "Body is required." }), { status: 400, headers: { "Content-Type": "application/json" } });

  const batchResults = await env.MY_D1_DATABASE2.batch([
    env.MY_D1_DATABASE2.prepare("UPDATE threads SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ? RETURNING post_count").bind(threadId),
    env.MY_D1_DATABASE2.prepare("UPDATE threads_meta SET post_count = post_count + 1, last_updated = CURRENT_TIMESTAMP WHERE id = ?").bind(threadId),
  ]);

  const newPostCountResult = batchResults[0].results[0] as { post_count: number };
  if (!newPostCountResult) throw new Error("Failed to update thread counters. Thread might not exist.");
  const newPostCount = newPostCountResult.post_count;

  const postResult = await env.MY_D1_DATABASE2.prepare("INSERT INTO posts (thread_id, post_number, author, body) VALUES (?, ?, ?, ?) RETURNING *")
    .bind(threadId, newPostCount, author || '名無しさん', body)
    .first<Post>();

  if (!postResult) throw new Error("Failed to create post.");

  waitUntil(handlePostCreationTasks(env, request, threadId, newPostCount));
  
  return new Response(JSON.stringify({ success: true, post: postResult }), { status: 201, headers: { "Content-Type": "application/json" } });
}

async function handlePostCreationTasks(env: Env, request: Request, threadId: number, newPostCount: number): Promise<void> {
    const cache = caches.default;

    const requestUrl = new URL(request.url);
    const origin = `${requestUrl.protocol}//${requestUrl.host}`;

    const threadsCacheUrl = new URL('/api/threads', origin);
    await cache.delete(threadsCacheUrl.toString());
    console.log(`Cache purged for thread list`);

    const currentChunkIndex = getChunkIndex(newPostCount);
    const { results: currentChunkPosts } = await env.MY_D1_DATABASE2.prepare(
        "SELECT * FROM posts WHERE thread_id = ? AND post_number > ? ORDER BY post_number ASC"
    ).bind(threadId, currentChunkIndex * CHUNK_SIZE).all<Post>();
    
    if (currentChunkPosts && currentChunkPosts.length > 0) {
        const chunkCacheUrl = new URL(`/api/threads/${threadId}/posts/${currentChunkIndex}`, origin);
        const chunkResponse = new Response(JSON.stringify(currentChunkPosts), {
            headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${CACHE_TTL}` }
        });
        await cache.put(chunkCacheUrl.toString(), chunkResponse);
        console.log(`Cache updated for thread ${threadId}, chunk ${currentChunkIndex}`);
    }

    if (newPostCount > 1 && (newPostCount - 1) % CHUNK_SIZE === 0) {
        const chunkToArchiveIndex = getChunkIndex(newPostCount - 1);
        console.log(`Archiving chunk ${chunkToArchiveIndex} for thread ${threadId}...`);
        await archiveAndCleanChunk(env, threadId, chunkToArchiveIndex);
    }
}

async function archiveAndCleanChunk(env: Env, threadId: number, chunkIndex: number): Promise<void> {
  const startPostNumber = chunkIndex * CHUNK_SIZE + 1;
  const endPostNumber = startPostNumber + CHUNK_SIZE - 1;

  const { results: postsToArchive } = await env.MY_D1_DATABASE2.prepare(
    "SELECT * FROM posts WHERE thread_id = ? AND post_number BETWEEN ? AND ? ORDER BY post_number ASC"
  ).bind(threadId, startPostNumber, endPostNumber).all<Post>();

  if (!postsToArchive || postsToArchive.length === 0) {
    console.log(`No posts to archive for thread ${threadId}, chunk ${chunkIndex}`);
    return;
  }

  const r2Key = getR2Key(threadId, chunkIndex);
  await env.MY_R2_BUCKET2.put(r2Key, JSON.stringify(postsToArchive), { // R2 binding name
      httpMetadata: { contentType: 'application/json' },
  });
  console.log(`Successfully archived ${postsToArchive.length} posts to R2 at key: ${r2Key}`);

  await env.MY_D1_DATABASE2.prepare(
    "DELETE FROM posts WHERE thread_id = ? AND post_number BETWEEN ? AND ?"
  ).bind(threadId, startPostNumber, endPostNumber).run();
  console.log(`Cleaned up archived posts from D1 for thread ${threadId}, chunk ${chunkIndex}`);
}