

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

// ---------- DB ACTIONS ----------
async function handleApiRequest(action, payload, env) {
  const db = env.DB;

  // Filter out the table_name from payload
  const keys = Object.keys(payload).filter(key => key !== "table_name");
  const tableName = payload.table_name || G_tableName;
  
  switch (action) {
    case "post": {
      const placeholders = keys.map(() => "?").join(",");
      const sql = `INSERT INTO ${tableName} (${keys.join(",")}) VALUES (${placeholders})`;
      const values = keys.map(k => payload[k]);
      const result = await db.prepare(sql).bind(...values).run();
      return { insertedId: result.lastInsertRowid };
    }

    case "put": {
      if (!payload.id) throw new Error("Missing 'id' for update");
      const { id, ...fields } = payload;
      if (keys.length === 0) throw new Error("No fields to update");
      const setClause = keys.map(k => `${k} = ?`).join(", ");
      const sql = `UPDATE ${tableName} SET ${setClause}, v2 = CURRENT_TIMESTAMP WHERE id = ?`;
      const values = [...keys.map(k => fields[k]), id];
      const result = await db.prepare(sql).bind(...values).run();
      return { changes: result.changes };
    }

    case "get": {
      let sql = `SELECT * FROM ${tableName}`;
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


// ---------- DB ACTIONS ----------
async function handleApiRequest(action, payload, env) {
  const db = env.DB;
  if (payload.table_name && payload.table_name !== "") {
    G_tableName = payload.table_name
  }

  const keys = Object.keys(payload).filter(key => key !== "table_name");
  const invalidKeys = keys.filter(key => !allowedColumns.includes(key));
  if (invalidKeys.length > 0) {
    return nack(payload.request_id, "INVALID_COLUMNS", `Invalid columns: ${invalidKeys.join(", ")}`);
  }

  switch (action) {
    case "post": {
      const placeholders = keys.map(() => "?").join(",");
      const sql = `INSERT INTO ${G_tableName} (${keys.join(",")}) VALUES (${placeholders})`;
      const values = keys.map(k => payload[k]);
      const result = await db.prepare(sql).bind(...values).run();
      return { insertedId: result.lastInsertRowid };
    }


    case "put": {
      if (!payload.id) throw new Error("Missing 'id' for update");
      const { id, ...fields } = payload;
      if (keys.length === 0) throw new Error("No fields to update");
      const setClause = keys.map(k => `${k} = ?`).join(", ");
      const sql = `UPDATE ${G_tableName} SET ${setClause}, v2 = CURRENT_TIMESTAMP WHERE id = ?`;
      const values = [...keys.map(k => fields[k]), id];
      const result = await db.prepare(sql).bind(...values).run();
      return { changes: result.changes };
    }

    case "get": {
      let sql = `SELECT * FROM ${G_tableName}`;
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
      if (keys.length === 0) throw new Error("Need at least one condition to delete");
      const where = keys.map(k => `${k} = ?`).join(" AND ");
      const sql = `DELETE FROM ${G_tableName} WHERE ${where}`;
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
  const url = C_LogServiceUrl;
  const body = {
    version: "v1",
    request_id,
    service: "log",
    action: "append",
    payload: {
      service: C_ServiceID,
      instance: C_InstanceID,
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

let G_tableName = "test1";
const allowedColumns = [
  "c1", "c2", "c3", "i1", "i2", "i3", "d1", "d2", "d3", "t1", "t2", "t3", "v1", "v2", "v3"
];
const C_LogServiceUrl = "https://demo2.dglog.workers.dev/api";
const C_ServiceID = "dacds";   // dage common database service;
const C_InstanceID = "dev1";    //