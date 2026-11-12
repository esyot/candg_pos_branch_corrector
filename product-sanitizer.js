require("dotenv").config();
const mysql = require("mysql2/promise");

/* ================================================================== */
/* CLI flags                                                          */
/* ================================================================== */
const ARGV = process.argv.slice(2);
const APPLY = ARGV.includes("--apply");
const SETUP = ARGV.includes("--setup");
const CHECK_ONLY = ARGV.includes("--check");
const INCLUDE_UNSAFE = ARGV.includes("--include-unsafe");
const SKIP_CLUSTER = ARGV.includes("--skip-cluster-check");
const FORCE_ANY_DB = ARGV.includes("--force-any-db");
const ONLY = getFlagValue("--only");

function getFlagValue(name) {
  const i = ARGV.indexOf(name);
  return i >= 0 ? ARGV[i + 1] : null;
}

/* ================================================================== */
/* Sanitization — matches runner.js exactly                           */
/* ================================================================== */
function sanitizeProductCode(raw) {
  if (raw == null) return null;
  let s = String(raw);
  s = s.replace(/\r/g, "");
  s = s.replace(/\n/g, "");
  s = s.replace(/\t/g, "");
  s = s.replace(/\\r/g, "");
  s = s.replace(/\\n/g, "");
  s = s.replace(/^MUG\s*/i, "");
  return s.trim().toUpperCase();
}

function isCorruptCode(raw) {
  if (raw == null || raw === "") return false;
  const s = String(raw);
  return /\r|\n|\t/.test(s) || /\\r|\\n/.test(s) || /^MUG\s*/i.test(s);
}

function normalizeParticular(raw) {
  if (raw == null) return "";
  return String(raw)
    .toUpperCase()
    .replace(/[\s()\-.,#]/g, "");
}

function looksLikeUuid(id) {
  return (
    typeof id === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)
  );
}

/* ================================================================== */
/* Target-only connection (DB_TARGET_*)                               */
/* ================================================================== */
const EXPECTED_DB = "candg_pos_db_main";

function buildTargetPool() {
  const host = process.env.DB_TARGET_HOST;
  const port = Number(process.env.DB_TARGET_PORT) || 3306;
  const user = process.env.DB_TARGET_USERNAME;
  const pass = process.env.DB_TARGET_PASSWORD || "";
  const db = process.env.DB_TARGET_DATABASE;

  if (!host || !user || !db) {
    console.error(
      "[Error] Missing DB_TARGET_HOST / DB_TARGET_USERNAME / DB_TARGET_DATABASE in .env",
    );
    process.exit(1);
  }

  if (db !== EXPECTED_DB && !FORCE_ANY_DB) {
    console.error(
      `[Safety] Refusing to run: DB_TARGET_DATABASE is "${db}", expected "${EXPECTED_DB}".`,
    );
    console.error(`         If this is intentional, pass --force-any-db.`);
    process.exit(1);
  }

  return mysql.createPool({
    host,
    port,
    user,
    password: pass,
    database: db,
    waitForConnections: true,
    connectionLimit: 5,
    timezone: "Z",
    dateStrings: true,
  });
}

const pool = buildTargetPool();

/* ================================================================== */
/* Child tables (product.record_id FKs)                               */
/* ================================================================== */
const CHILD_TABLES = [
  { table: "damaged_stocks_on_hand", column: "product" },
  { table: "in_out_damaged_stocks_details", column: "product" },
  { table: "in_out_stocks_details", column: "product" },
  { table: "layaway_item", column: "product" },
  { table: "pricing_scheme", column: "product" },
  { table: "product_image", column: "product" },
  { table: "purchase_order_details", column: "product" },
  { table: "sales_order_details", column: "product" },
  { table: "stocks_on_hand", column: "product" },
  { table: "stock_cost_details", column: "product" },
  { table: "stock_request_items", column: "product" },
  { table: "store_expenses_item", column: "product" },
];

/* Tables with UNIQUE keys on product-related columns */
const UNIQUE_ON_PRODUCT = {
  stocks_on_hand: ["store"],
  pricing_scheme: ["store", "price", "uom", "is_default"],
};

/* ================================================================== */
/* Cluster preflight                                                  */
/* ================================================================== */
async function checkCluster(conn) {
  const [rows] = await conn.query(
    `SELECT TABLE_NAME FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN ('sym_change_log','sym_checkpoint')`,
  );
  const present = new Set(rows.map((r) => r.TABLE_NAME));
  return {
    isCluster: present.has("sym_change_log") && present.has("sym_checkpoint"),
    hasLog: present.has("sym_change_log"),
    hasCp: present.has("sym_checkpoint"),
  };
}

async function checkTriggers(conn, tables) {
  const problems = [];
  const details = {};
  for (const t of tables) {
    const [rows] = await conn.query(
      `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
       WHERE TRIGGER_SCHEMA = DATABASE()
         AND TRIGGER_NAME IN (?, ?, ?)`,
      [`sym_trig_${t}_ins`, `sym_trig_${t}_upd`, `sym_trig_${t}_del`],
    );
    const found = rows.map((r) => r.TRIGGER_NAME);
    const missing = [
      `sym_trig_${t}_ins`,
      `sym_trig_${t}_upd`,
      `sym_trig_${t}_del`,
    ].filter((x) => !found.includes(x));
    details[t] = found;
    if (missing.length) problems.push(`${t}: missing ${missing.join(", ")}`);
  }
  return { problems, details };
}

async function checkSessionClean(conn) {
  const [rows] = await conn.query("SELECT @sym_is_syncing AS v");
  const v = rows[0]?.v;
  return v == null || v === 0;
}

async function preflight({ requireCluster }) {
  const conn = await pool.getConnection();
  try {
    const cluster = await checkCluster(conn);
    if (!cluster.isCluster) {
      const msg =
        `Target DB "${process.env.DB_TARGET_DATABASE}" is NOT a cluster node ` +
        `(sym_change_log ${cluster.hasLog ? "present" : "MISSING"}, ` +
        `sym_checkpoint ${cluster.hasCp ? "present" : "MISSING"}).`;
      if (requireCluster) {
        console.error("❌ " + msg);
        console.error("   Refusing to run without --skip-cluster-check.");
        process.exitCode = 1;
        return { ok: false, reason: msg };
      }
      console.log("ℹ️  " + msg + "  (standalone mode)");
      return { ok: true, cluster: false };
    }

    console.log("✅ Target cluster node confirmed");

    const tablesToCheck = ["product", "pricing_scheme", "stocks_on_hand"];
    const trig = await checkTriggers(conn, tablesToCheck);
    if (trig.problems.length) {
      console.error("❌ Missing sync triggers:");
      for (const p of trig.problems) console.error("   " + p);
      console.error('   Run "node triggers.js" on this target first.');
      process.exitCode = 1;
      return { ok: false, reason: "missing triggers" };
    }
    console.log(`✅ All sync triggers present (${tablesToCheck.join(", ")})`);

    const clean = await checkSessionClean(conn);
    if (!clean) {
      console.error(
        "❌ @sym_is_syncing is set — triggers suppressed. Clear with: SET @sym_is_syncing = NULL;",
      );
      process.exitCode = 1;
      return { ok: false, reason: "@sym_is_syncing set" };
    }
    console.log("✅ @sym_is_syncing is NULL (triggers will fire)");

    return { ok: true, cluster: true };
  } finally {
    conn.release();
  }
}

/* ================================================================== */
/* Audit table                                                        */
/* ================================================================== */
async function setupAudit() {
  console.log(
    `🔧 Creating product_merge_log on ${process.env.DB_TARGET_DATABASE}...`,
  );
  await pool.query(`
    CREATE TABLE IF NOT EXISTS product_merge_log (
      id             BIGINT AUTO_INCREMENT PRIMARY KEY,
      survivor_id    VARCHAR(64)  NOT NULL,
      loser_id       VARCHAR(64)  NOT NULL,
      normalized_key VARCHAR(128) NOT NULL,
      snapshot_json  JSON         NOT NULL,
      merged_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
      merged_by      VARCHAR(100) NOT NULL DEFAULT 'sanitizer',
      KEY idx_survivor (survivor_id),
      KEY idx_loser    (loser_id),
      KEY idx_mergedat (merged_at)
    ) ENGINE=InnoDB
  `);
  console.log("✅ product_merge_log ready.");
}

async function auditLog(conn, survivorId, loserId, normalizedKey, snapshot) {
  await conn.query(
    `INSERT INTO product_merge_log
       (survivor_id, loser_id, normalized_key, snapshot_json, merged_by)
     VALUES (?, ?, ?, ?, CURRENT_USER())`,
    [survivorId, loserId, normalizedKey, JSON.stringify(snapshot)],
  );
}

/* ================================================================== */
/* Scan                                                               */
/* ================================================================== */
async function scan() {
  const [rows] = await pool.query(`
    SELECT record_id, product_code, particular, category, classification,
           product_type, reorder_point, status, date_added
    FROM product
    WHERE product_code IS NOT NULL AND TRIM(product_code) <> ''
    ORDER BY product_code
  `);

  const byClean = new Map();
  let corruptCount = 0;
  for (const r of rows) {
    const clean = sanitizeProductCode(r.product_code);
    if (!clean) continue;
    if (isCorruptCode(r.product_code)) corruptCount++;
    if (!byClean.has(clean)) byClean.set(clean, []);
    byClean.get(clean).push(r);
  }

  const groups = [];
  for (const [clean, list] of byClean) {
    const hasCorrupt = list.some((r) => isCorruptCode(r.product_code));
    if (!hasCorrupt) continue;
    const particulars = new Set(
      list.map((r) => normalizeParticular(r.particular)),
    );
    const safe = particulars.size === 1;
    groups.push({
      normalizedCode: clean,
      hasCorrupt,
      safe,
      reason: safe ? null : "different particulars — manual review",
      rows: list,
    });
  }
  groups.sort(
    (a, b) =>
      b.rows.length - a.rows.length ||
      a.normalizedCode.localeCompare(b.normalizedCode),
  );
  return { groups, totalRows: rows.length, corruptCount };
}

function pickSurvivor(rows) {
  const uuids = rows.filter((r) => looksLikeUuid(r.record_id));
  const candidates = uuids.length ? uuids : rows;
  return candidates.reduce((best, r) => {
    if (!best) return r;
    const a = new Date(r.date_added ?? 0).getTime();
    const b = new Date(best.date_added ?? 0).getTime();
    return a < b ? r : best;
  }, null);
}

/* ================================================================== */
/* Repoint a single child table                                       */
/* ================================================================== */
async function repointTable(
  conn,
  table,
  column,
  uniqueOn,
  survivorId,
  loserIds,
) {
  const ph = loserIds.map(() => "?").join(",");

  if (!uniqueOn || uniqueOn.length === 0) {
    const [r] = await conn.query(
      `UPDATE \`${table}\` SET \`${column}\` = ? WHERE \`${column}\` IN (${ph})`,
      [survivorId, ...loserIds],
    );
    return r.affectedRows;
  }

  const cols = ["record_id", ...uniqueOn];
  let loserRows;
  try {
    const [rows] = await conn.query(
      `SELECT ${cols.join(", ")}, qty FROM \`${table}\` WHERE \`${column}\` IN (${ph})`,
      loserIds,
    );
    loserRows = rows;
  } catch {
    const [rows] = await conn.query(
      `SELECT ${cols.join(", ")} FROM \`${table}\` WHERE \`${column}\` IN (${ph})`,
      loserIds,
    );
    loserRows = rows;
  }

  let repointed = 0;
  for (const row of loserRows) {
    const whereParts = uniqueOn.map((c) => `${c} = ?`).join(" AND ");
    const whereVals = uniqueOn.map((c) => row[c]);
    const [ex] = await conn.query(
      `SELECT record_id FROM \`${table}\`
       WHERE \`${column}\` = ? AND ${whereParts}`,
      [survivorId, ...whereVals],
    );

    if (ex.length) {
      if (table === "stocks_on_hand" && "qty" in row) {
        await conn.query(
          `UPDATE stocks_on_hand SET qty = qty + ?, updated_at = NOW()
           WHERE record_id = ?`,
          [row.qty, ex[0].record_id],
        );
      }
      await conn.query(`DELETE FROM \`${table}\` WHERE record_id = ?`, [
        row.record_id,
      ]);
    } else {
      await conn.query(
        `UPDATE \`${table}\` SET \`${column}\` = ? WHERE record_id = ?`,
        [survivorId, row.record_id],
      );
    }
    repointed++;
  }
  return repointed;
}

/* ================================================================== */
/* Merge one group                                                    */
/* ================================================================== */
async function mergeGroup(group, { dryRun }) {
  const survivor = pickSurvivor(group.rows);
  const losers = group.rows.filter((r) => r.record_id !== survivor.record_id);

  if (losers.length === 0) return { ok: false, error: "no losers to merge" };

  const sCode = sanitizeProductCode(survivor.product_code);
  const sPart = normalizeParticular(survivor.particular);
  for (const l of losers) {
    if (sanitizeProductCode(l.product_code) !== sCode) {
      return { ok: false, error: `code mismatch: ${l.product_code}` };
    }
    if (normalizeParticular(l.particular) !== sPart) {
      return {
        ok: false,
        error: `particular mismatch: ${l.particular} vs ${survivor.particular}`,
      };
    }
  }

  const loserIds = losers.map((l) => l.record_id);
  if (new Set(loserIds).size !== loserIds.length) {
    return { ok: false, error: "duplicate record_id in losers" };
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const [surv] = await conn.query(
      "SELECT * FROM product WHERE record_id = ? FOR UPDATE",
      [survivor.record_id],
    );
    if (!surv.length) throw new Error("survivor not found");

    const ph = loserIds.map(() => "?").join(",");
    const [locked] = await conn.query(
      `SELECT * FROM product WHERE record_id IN (${ph}) FOR UPDATE`,
      loserIds,
    );
    if (locked.length !== loserIds.length) {
      throw new Error(
        `expected ${loserIds.length} losers, found ${locked.length}`,
      );
    }

    if (!dryRun) {
      for (const l of locked) {
        await auditLog(conn, survivor.record_id, l.record_id, sCode, l);
      }
    }

    const repointed = {};
    for (const { table, column } of CHILD_TABLES) {
      const n = await repointTable(
        conn,
        table,
        column,
        UNIQUE_ON_PRODUCT[table] || null,
        survivor.record_id,
        loserIds,
      );
      if (n > 0) repointed[table] = n;
    }

    if (survivor.product_code !== sCode) {
      await conn.query(
        "UPDATE product SET product_code = ?, updated_at = NOW() WHERE record_id = ?",
        [sCode, survivor.record_id],
      );
    }

    await conn.query(
      `DELETE FROM product WHERE record_id IN (${ph})`,
      loserIds,
    );

    if (dryRun) {
      await conn.rollback();
      return {
        ok: true,
        dryRun: true,
        survivorId: survivor.record_id,
        loserIds,
        repointed,
      };
    }
    await conn.commit();
    return {
      ok: true,
      dryRun: false,
      survivorId: survivor.record_id,
      loserIds,
      repointed,
    };
  } catch (e) {
    await conn.rollback();
    return { ok: false, error: e.message };
  } finally {
    conn.release();
  }
}

/* ================================================================== */
/* Rename a corrupt orphan (no clean twin)                            */
/* ================================================================== */
async function renameOrphan(row, { dryRun }) {
  const clean = sanitizeProductCode(row.product_code);
  if (!clean) throw new Error(`empty clean code for ${row.record_id}`);
  if (clean === row.product_code) return { ok: false, error: "already clean" };

  const [conflict] = await pool.query(
    "SELECT record_id FROM product WHERE product_code = ? AND record_id <> ?",
    [clean, row.record_id],
  );
  if (conflict.length) {
    throw new Error(
      `cannot rename ${row.record_id} → "${clean}": already taken by ${conflict.map((c) => c.record_id).join(", ")}`,
    );
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    await conn.query(
      "SELECT record_id FROM product WHERE record_id = ? FOR UPDATE",
      [row.record_id],
    );

    if (!dryRun) {
      await auditLog(conn, row.record_id, row.record_id, clean, {
        type: "rename",
        from: row.product_code,
        to: clean,
      });
      await conn.query(
        "UPDATE product SET product_code = ?, updated_at = NOW() WHERE record_id = ?",
        [clean, row.record_id],
      );
    }

    if (dryRun) {
      await conn.rollback();
      return {
        ok: true,
        dryRun: true,
        recordId: row.record_id,
        from: row.product_code,
        to: clean,
      };
    }
    await conn.commit();
    return {
      ok: true,
      dryRun: false,
      recordId: row.record_id,
      from: row.product_code,
      to: clean,
    };
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */
async function main() {
  const dbLabel = `${process.env.DB_TARGET_HOST}:${process.env.DB_TARGET_PORT}/${process.env.DB_TARGET_DATABASE}`;
  console.log(`Target DB: ${dbLabel}`);
  console.log(
    APPLY
      ? "⚠️  APPLY MODE — database WILL be modified"
      : "🔍 DRY RUN — no changes will be made",
  );
  if (ONLY) console.log(`🎯 Filtering to code: ${ONLY}`);
  console.log("");

  if (SETUP) {
    await setupAudit();
    return;
  }

  const pre = await preflight({ requireCluster: !SKIP_CLUSTER });
  if (!pre.ok) {
    console.error("\nAborted by preflight.");
    return;
  }
  if (CHECK_ONLY) {
    console.log("\n✅ Preflight OK (--check only).");
    return;
  }

  const { groups, totalRows, corruptCount } = await scan();
  console.log(
    `\n📊 Scanned ${totalRows} product row(s) in ${process.env.DB_TARGET_DATABASE}.`,
  );
  console.log(`   Corrupt codes (MUG/CR/literal-\\r): ${corruptCount}`);
  console.log(`   Groups needing action: ${groups.length}`);

  if (groups.length === 0) {
    console.log("\n✅ Nothing to do — no corrupt codes with duplicates found.");
    return;
  }

  let renamed = 0,
    merged = 0,
    skipped = 0;

  for (const g of groups) {
    if (ONLY && g.normalizedCode !== ONLY.toUpperCase()) continue;

    console.log(`\n📦 Group "${g.normalizedCode}" — ${g.rows.length} row(s)`);
    for (const r of g.rows) {
      const tag = isCorruptCode(r.product_code) ? "⚠ corrupt" : "  clean  ";
      console.log(
        `   ${tag}  ${r.record_id}  ${JSON.stringify(r.product_code)}  (${JSON.stringify(r.particular)})`,
      );
    }

    if (g.rows.length === 1) {
      try {
        const res = await renameOrphan(g.rows[0], { dryRun: !APPLY });
        if (res.ok) {
          console.log(
            `   ✏️  ${res.dryRun ? "WOULD RENAME" : "RENAMED"}: ${JSON.stringify(res.from)} → ${JSON.stringify(res.to)}`,
          );
          renamed++;
        } else {
          console.log(`   ⏭️  ${res.error}`);
          skipped++;
        }
      } catch (e) {
        console.log(`   ❌ FAILED to rename: ${e.message}`);
        skipped++;
      }
      continue;
    }

    if (!g.safe && !INCLUDE_UNSAFE) {
      console.log(`   ⏭️  SKIPPED — ${g.reason}`);
      skipped++;
      continue;
    }
    if (!g.safe && INCLUDE_UNSAFE) {
      console.log(`   ⚠️  forced via --include-unsafe (${g.reason})`);
    }

    const res = await mergeGroup(g, { dryRun: !APPLY });
    if (res.ok) {
      const total = Object.values(res.repointed).reduce((a, b) => a + b, 0);
      console.log(
        `   ✅ ${res.dryRun ? "WOULD MERGE" : "MERGED"} into survivor ${res.survivorId}  (children repointed: ${total})`,
      );
      for (const [t, n] of Object.entries(res.repointed)) {
        console.log(`       ${t}: ${n}`);
      }
      merged++;
    } else {
      console.log(`   ❌ FAILED: ${res.error}`);
      skipped++;
    }
  }

  console.log(
    `\nDone. ${APPLY ? "renamed" : "would-rename"}=${renamed}  ` +
      `${APPLY ? "merged" : "would-merge"}=${merged}  skipped=${skipped}`,
  );
  if (!APPLY) console.log("\nRun again with --apply to execute.");
}

main()
  .catch((e) => {
    console.error("FATAL:", e.message);
    if (process.env.DEBUG) console.error(e.stack);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
