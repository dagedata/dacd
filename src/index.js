export default {
  async fetch(request, env, ctx) {
    return new Response("Hello World from Cloudflare!", {
      headers: { "content-type": "text/plain" },
    });
  },
};
