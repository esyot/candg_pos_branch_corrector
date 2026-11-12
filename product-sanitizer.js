require("dotenv").config({ override: true });
const mysql = require("mysql2/promise");

/* ================================================================== */
/* CLI flags                                                          */
/* ================================================================== */
const ARGV = process.argv.slice(2);
const APPLY = ARGV.includes("--apply");
const SETUP = ARGV.includes("--setup");
const CHECK_ONLY = ARGV.includes("--check");
const VERIFY_ONLY = ARGV.includes("--verify-only");
const INCLUDE_UNSAFE = ARGV.includes("--include-unsafe");
const SKIP_CLUSTER = ARGV.includes("--skip-cluster-check");
const ONLY = getFlagValue("--only");

function getFlagValue(name) {
  const i = ARGV.indexOf(name);
  return i >= 0 ? ARGV[i + 1] : null;
}

/* ================================================================== */
/* Sanitization                                                       */
/* ================================================================== */
function sanitizeProductCode(raw) {
  if (raw == null) return null;
  return String(raw)
    .replace(/\r/g, "")
    .replace(/\n/g, "")
    .replace(/\t/g, "")
    .replace(/\\r/g, "")
    .replace(/\\n/g, "")
    .replace(/^MUG\s*/i, "")
    .trim()
    .toUpperCase();
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
    .replace(/\s*\((?:HTC|MW|WC|H|M)\)\s*$/gi, "")
    .replace(/[\s()\-.,#]/g, "");
}

function looksLikeUuid(id) {
  return (
    typeof id === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)
  );
}

/* ================================================================== */
/* Connection                                                         */
/* ================================================================== */
const HOST = process.env.DB_TARGET_HOST;
const PORT = Number(process.env.DB_TARGET_PORT) || 3306;
const USER = process.env.DB_TARGET_USERNAME;
const PASS = process.env.DB_TARGET_PASSWORD || "";
const DB = process.env.DB_TARGET_DATABASE;

if (!HOST || !USER || !DB) {
  console.error("[Error] Missing DB_TARGET_* in .env");
  process.exit(1);
}

console.log(`[Init] ${USER}@${HOST}:${PORT}/${DB}\n`);

const pool = mysql.createPool({
  host: HOST,
  port: PORT,
  user: USER,
  password: PASS,
  database: DB,
  waitForConnections: true,
  connectionLimit: 5,
  timezone: "Z",
  dateStrings: true,
});

/* ================================================================== */
/* FK definitions                                                     */
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

const UNIQUE_ON_PRODUCT = {
  stocks_on_hand: ["store"],
  pricing_scheme: ["store", "price", "uom", "is_default"],
};

// Verified against candg_pos_db:
//   pricing_scheme ← sales_order_details.price_scheme (the only grandchild FK)
const GRANDCHILD_FKS = {
  pricing_scheme: [{ table: "sales_order_details", column: "price_scheme" }],
  stocks_on_hand: [],
};

/* ================================================================== */
/* Preflight                                                          */
/* ================================================================== */
async function checkCluster(conn) {
  const [rows] = await conn.query(
    `SELECT TABLE_NAME FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN ('sym_change_log','sym_checkpoint')`,
  );
  const present = new Set(rows.map((r) => r.TABLE_NAME));
  return present.has("sym_change_log") && present.has("sym_checkpoint");
}

async function checkTriggers(conn, tables) {
  const problems = [];
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
    if (missing.length) problems.push(`${t}: missing ${missing.join(", ")}`);
  }
  return problems;
}

async function checkSessionClean(conn) {
  const [rows] = await conn.query("SELECT @sym_is_syncing AS v");
  const v = rows[0]?.v;
  return v == null || v === 0;
}

async function preflight() {
  const conn = await pool.getConnection();
  try {
    if (!(await checkCluster(conn))) {
      const msg = `DB "${DB}" is not a cluster node (sym_change_log / sym_checkpoint missing).`;
      if (!SKIP_CLUSTER) {
        console.error("❌ " + msg);
        console.error("   Pass --skip-cluster-check to run anyway.");
        return { ok: false, reason: msg };
      }
      console.log("ℹ️  " + msg + " (standalone mode)");
      return { ok: true, cluster: false };
    }
    console.log(`✅ Cluster node confirmed on "${DB}"`);

    const tablesToCheck = ["product", "pricing_scheme", "stocks_on_hand"];
    const problems = await checkTriggers(conn, tablesToCheck);
    if (problems.length) {
      console.error("❌ Missing sync triggers:");
      for (const p of problems) console.error("   " + p);
      return { ok: false, reason: "missing triggers" };
    }
    console.log(`✅ Sync triggers present (${tablesToCheck.join(", ")})`);

    if (!(await checkSessionClean(conn))) {
      console.error("❌ @sym_is_syncing is set — triggers suppressed.");
      return { ok: false, reason: "@sym_is_syncing set" };
    }
    console.log("✅ @sym_is_syncing is NULL");

    return { ok: true, cluster: true };
  } finally {
    conn.release();
  }
}

/* ================================================================== */
/* Audit table                                                        */
/* ================================================================== */
async function setupAudit() {
  console.log(`🔧 Creating product_merge_log on ${DB}...`);
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

async function auditLog(conn, survivorId, loserId, key, snapshot) {
  await conn.query(
    `INSERT INTO product_merge_log
       (survivor_id, loser_id, normalized_key, snapshot_json, merged_by)
     VALUES (?, ?, ?, ?, CURRENT_USER())`,
    [survivorId, loserId, key, JSON.stringify(snapshot)],
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
  `);

  const byClean = new Map();
  let corruptCount = 0;

  for (const r of rows) {
    if (isCorruptCode(r.product_code)) corruptCount++;
    const clean = sanitizeProductCode(r.product_code);
    if (!clean) continue;
    if (!byClean.has(clean)) byClean.set(clean, []);
    byClean.get(clean).push(r);
  }

  const groups = [];
  for (const [clean, list] of byClean) {
    if (!list.some((r) => isCorruptCode(r.product_code))) continue;
    const particulars = new Set(
      list.map((r) => normalizeParticular(r.particular)),
    );
    groups.push({
      normalizedCode: clean,
      safe: particulars.size === 1,
      reason:
        particulars.size === 1 ? null : "different particulars — manual review",
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
/* Repoint one child table                                            */
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

  if (!uniqueOn || !uniqueOn.length) {
    const [r] = await conn.query(
      `UPDATE \`${table}\` SET \`${column}\` = ? WHERE \`${column}\` IN (${ph})`,
      [survivorId, ...loserIds],
    );
    return r.affectedRows;
  }

  // Fetch loser rows for unique-key collision checking
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

  const grandkids = GRANDCHILD_FKS[table] || [];
  let repointed = 0;

  for (const row of loserRows) {
    const whereParts = uniqueOn.map((c) => `${c} = ?`).join(" AND ");
    const whereVals = uniqueOn.map((c) => row[c]);
    const [ex] = await conn.query(
      `SELECT record_id FROM \`${table}\` WHERE \`${column}\` = ? AND ${whereParts}`,
      [survivorId, ...whereVals],
    );

    if (ex.length) {
      const survivorRowId = ex[0].record_id;
      const loserRowId = row.record_id;

      // Sum qty for stocks_on_hand
      if (table === "stocks_on_hand" && "qty" in row) {
        await conn.query(
          `UPDATE stocks_on_hand SET qty = qty + ?, updated_at = NOW()
           WHERE record_id = ?`,
          [row.qty, survivorRowId],
        );
      }

      // Cascade to grandchildren BEFORE deleting
      for (const gc of grandkids) {
        try {
          await conn.query(
            `UPDATE \`${gc.table}\` SET \`${gc.column}\` = ?
             WHERE \`${gc.column}\` = ?`,
            [survivorRowId, loserRowId],
          );
        } catch (e) {
          if (
            e.code === "ER_NO_SUCH_TABLE" ||
            e.code === "ER_BAD_FIELD_ERROR"
          ) {
            console.warn(
              `   ⚠️  Skipped grandchild ${gc.table}.${gc.column}: ${e.code}`,
            );
          } else {
            throw e;
          }
        }
      }

      await conn.query(`DELETE FROM \`${table}\` WHERE record_id = ?`, [
        loserRowId,
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

  if (!losers.length) return { ok: false, error: "no losers" };

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
/* Rename orphan                                                      */
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
      `cannot rename ${row.record_id} → "${clean}": taken by ${conflict.map((c) => c.record_id).join(", ")}`,
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
/* Verify orphan integrity                                            */
/* ================================================================== */
async function verifyOrphans() {
  console.log("\n🔎 FK integrity check...\n");
  const problems = [];

  console.log("   Direct children (product.record_id):");
  for (const { table, column } of CHILD_TABLES) {
    const [rows] = await pool.query(
      `SELECT COUNT(*) AS n
       FROM \`${table}\` c
       LEFT JOIN product p ON p.record_id = c.\`${column}\`
       WHERE c.\`${column}\` IS NOT NULL AND p.record_id IS NULL`,
    );
    const n = Number(rows[0].n);
    if (n === 0) {
      console.log(`   ✅ ${table.padEnd(35)} 0 orphans`);
    } else {
      console.log(`   ❌ ${table.padEnd(35)} ${n} ORPHAN ROW(S)`);
      problems.push({ table, count: n });
    }
  }

  console.log("\n   Grandchildren:");
  for (const [parent, kids] of Object.entries(GRANDCHILD_FKS)) {
    for (const { table, column } of kids) {
      try {
        const [rows] = await pool.query(
          `SELECT COUNT(*) AS n
           FROM \`${table}\` c
           LEFT JOIN \`${parent}\` p ON p.record_id = c.\`${column}\`
           WHERE c.\`${column}\` IS NOT NULL AND p.record_id IS NULL`,
        );
        const n = Number(rows[0].n);
        if (n === 0) {
          console.log(`   ✅ ${(table + "." + column).padEnd(45)} 0 orphans`);
        } else {
          console.log(
            `   ❌ ${(table + "." + column).padEnd(45)} ${n} ORPHAN ROW(S)`,
          );
          problems.push({ table: `${table}.${column}`, count: n });
        }
      } catch (e) {
        if (e.code === "ER_NO_SUCH_TABLE" || e.code === "ER_BAD_FIELD_ERROR") {
          console.log(
            `   ⏭️  ${(table + "." + column).padEnd(45)} skipped (${e.code})`,
          );
        } else throw e;
      }
    }
  }

  console.log("");
  if (!problems.length) {
    console.log(`✅ All FK checks passed on ${DB}.\n`);
    return 0;
  }
  const total = problems.reduce((a, b) => a + b.count, 0);
  console.log(
    `❌ ${problems.length} source(s) have orphans — total ${total} row(s).\n`,
  );
  return problems.length;
}

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */
async function main() {
  console.log(
    APPLY ? "⚠️  APPLY MODE — database WILL be modified" : "🔍 DRY RUN",
  );
  if (ONLY) console.log(`🎯 Only group: ${ONLY}`);
  console.log("");

  if (SETUP) {
    await setupAudit();
    return;
  }

  const pre = await preflight();
  if (!pre.ok) {
    console.error("\nAborted by preflight.");
    process.exitCode = 1;
    return;
  }
  if (CHECK_ONLY) {
    console.log("\n✅ Preflight OK.");
    return;
  }

  if (VERIFY_ONLY) {
    const n = await verifyOrphans();
    process.exitCode = n === 0 ? 0 : 1;
    return;
  }

  const { groups, totalRows, corruptCount } = await scan();
  console.log(`\n📊 Scanned ${totalRows} product row(s) in ${DB}.`);
  console.log(`   Corrupt codes: ${corruptCount}`);
  console.log(`   Groups needing action: ${groups.length}`);

  if (!groups.length) {
    console.log("\n✅ Nothing to do.");
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
        console.log(`   ❌ FAILED rename: ${e.message}`);
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
        `   ✅ ${res.dryRun ? "WOULD MERGE" : "MERGED"} into ${res.survivorId}  (repointed: ${total})`,
      );
      for (const [t, n] of Object.entries(res.repointed))
        console.log(`       ${t}: ${n}`);
      merged++;
    } else {
      console.log(`   ❌ FAILED: ${res.error}`);
      skipped++;
    }
  }

  console.log(
    `\nDone. ${APPLY ? "renamed" : "would-rename"}=${renamed}  ${APPLY ? "merged" : "would-merge"}=${merged}  skipped=${skipped}`,
  );

  if (!APPLY) {
    console.log("\nRun with --apply to execute.");
    return;
  }

  const orphanCount = await verifyOrphans();
  if (orphanCount > 0) {
    console.error("⚠️  Post-merge verification failed — orphans detected.");
    process.exitCode = 1;
  } else {
    console.log("✅ Post-merge verification passed.");
  }
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
