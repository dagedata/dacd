export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    if (url.pathname === "/api") {
      return await handleApi(request, env);
    } else {
      return new Response("Not Found", { status: 404 });
    }
  },
};

// ---------- MAIN HANDLER ----------
async function handleApi(request, env) {
  // Auth check
  const auth = request.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ")) {
    return nack("unknown", "UNAUTHORIZED", "Missing or invalid Authorization header");
  }

  const token = auth.split(" ")[1];
  if (token !== env.DA_WRITETOKEN) {
    return nack("unknown", "INVALID_TOKEN", "Token authentication failed");
  }

  // Parse JSON
  let body;
  try {
    body = await request.json();
  } catch {
    return nack("unknown", "INVALID_JSON", "Malformed JSON body");
  }

  const requestId = body.request_id || "unknown";
  const action = (body.action || "").toLowerCase();

  if (!body.payload || typeof body.payload !== "object") {
    return nack(requestId, "INVALID_FIELD", "Missing or invalid payload object");
  }

  try {
    const result = await handleApiRequest(action, body.payload, env);
    return ack(requestId, result);
  } catch (err) {
    await errDelegate(`handleApiRequest failed: ${err.message}`);
    return nack(requestId, "DB_ERROR", err.message);
  }
}

// ---------- DB ACTIONS ----------
async function handleApiRequest(action, payload, env) {
  const tableName = "your_table_name";
  const db = env.DB;

  switch (action) {
    case "post": {
      const keys = Object.keys(payload);
      const placeholders = keys.map(() => "?").join(",");
      const sql = `INSERT INTO ${tableName} (${keys.join(",")}) VALUES (${placeholders})`;
      const values = keys.map(k => payload[k]);
      const result = await db.prepare(sql).bind(...values).run();
      return { insertedId: result.lastInsertRowid };
    }

    case "put": {
      if (!payload.id) throw new Error("Missing 'id' for update");
      const { id, ...fields } = payload;
      const keys = Object.keys(fields);
      if (keys.length === 0) throw new Error("No fields to update");
      const setClause = keys.map(k => `${k} = ?`).join(", ");
      const sql = `UPDATE ${tableName} SET ${setClause}, v2 = CURRENT_TIMESTAMP WHERE id = ?`;
      const values = [...keys.map(k => fields[k]), id];
      const result = await db.prepare(sql).bind(...values).run();
      return { changes: result.changes };
    }

    case "get": {
      let sql = `SELECT * FROM ${tableName}`;
      const keys = Object.keys(payload);
      let values = [];
      if (keys.length > 0) {
        const where = keys.map(k => `${k} = ?`).join(" AND ");
        sql += ` WHERE ${where}`;
        values = keys.map(k => payload[k]);
      }
      const stmt = db.prepare(sql).bind(...values);
      const rows = await stmt.all();
      return { data: rows.results };
    }

    case "delete": {
      const keys = Object.keys(payload);
      if (keys.length === 0) throw new Error("Need at least one condition to delete");
      const where = keys.map(k => `${k} = ?`).join(" AND ");
      const sql = `DELETE FROM ${tableName} WHERE ${where}`;
      const values = keys.map(k => payload[k]);
      const result = await db.prepare(sql).bind(...values).run();
      return { deleted: result.changes };
    }

    default:
      throw new Error("Unknown action");
  }
}

// ---------- HELPERS ----------
function ack(requestId, payload = {}) {
  return jsonResponse({ type: "ack", request_id: requestId, payload });
}

function nack(requestId, code, message) {
  return jsonResponse(
    { type: "nack", request_id: requestId, payload: { status: "error", code, message } },
    400
  );
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}

// ---------- LOGGING ----------
async function errDelegate(msg) {
  console.error(msg);
  await postLogToGateway("11", 11, `❌ *Error*\n${msg}`);
}

async function postLogToGateway(request_id, level, message) {
  const url = "https://demo2.dglog.workers.dev/api";
  const body = {
    version: "v1",
    request_id,
    service: "log",
    action: "append",
    payload: {
      service: "dademo",
      instance: "test-1",
      level,
      message
    }
  };

  fetch(url, {
    method: "POST",
    headers: {
      "Authorization": "Bearer aaa",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  }).catch(err => console.error("Error posting log:", err));
}
