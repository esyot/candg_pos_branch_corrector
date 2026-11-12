require("dotenv").config();
const mysql = require("mysql2/promise");

const norm = (n) => (n == null ? null : String(n).trim().toLowerCase());

// Known renames: legacy name -> target name. Keys must be normalized (lowercase, trimmed).
const ALIASES = {
  product_type: {
    "pots / pans": "pot & pans",
    decoration: "decorations",
    topperware: "toppersware",
    "gel pen": "gelpen",
  },
  product_category: {},
  classification: {
    deskfan: "desk fan",
    standfan: "stand fan",
    wallfan: "wall fan",
    flowers: "flower",
  },
  price: {},
  unit_of_measure: {},
};

const SYNC_TABLES = ["product", "pricing_scheme", "stocks_on_hand"];

/**
 * Check whether the target is a replica cluster node (has sym_change_log
 * and sym_checkpoint). If not, migration inserts will not propagate.
 */
async function checkClusterNode(target) {
  const [rows] = await target.execute(
    `SELECT TABLE_NAME FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN ('sym_change_log', 'sym_checkpoint')`,
  );
  const present = new Set(rows.map((r) => r.TABLE_NAME));
  return {
    isClusterNode:
      present.has("sym_change_log") && present.has("sym_checkpoint"),
    hasLog: present.has("sym_change_log"),
    hasCheckpoint: present.has("sym_checkpoint"),
  };
}

/**
 * Verify that all 3 sync triggers exist on each of SYNC_TABLES on the
 * target database. Missing triggers = inserts won't be logged = drift.
 */
async function checkTriggers(target) {
  const problems = [];
  const details = {};

  for (const table of SYNC_TABLES) {
    const [rows] = await target.execute(
      `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
       WHERE TRIGGER_SCHEMA = DATABASE()
         AND TRIGGER_NAME IN (?, ?, ?)`,
      [
        `sym_trig_${table}_ins`,
        `sym_trig_${table}_upd`,
        `sym_trig_${table}_del`,
      ],
    );
    const found = rows.map((r) => r.TRIGGER_NAME);
    details[table] = found;

    if (found.length !== 3) {
      const expected = [
        `sym_trig_${table}_ins`,
        `sym_trig_${table}_upd`,
        `sym_trig_${table}_del`,
      ];
      const missing = expected.filter((t) => !found.includes(t));
      problems.push(`${table}: missing ${missing.join(", ")}`);
    }
  }

  return { problems, details };
}

/**
 * Check how many products the source store has sales history for.
 * If 0, the migration will run but insert nothing.
 */
async function checkProductCount(legacy, store) {
  const [rows] = await legacy.execute(
    `SELECT COUNT(DISTINCT p.product_code) AS c
     FROM sales_order so
     JOIN sales_order_details sod ON so.record_id = sod.sales_order
     JOIN product p ON sod.product = p.record_id
     WHERE so.store = ?`,
    [store],
  );
  return Number(rows[0]?.c || 0);
}

/**
 * Check that @sym_is_syncing is not set on the target session.
 * If set, triggers will be suppressed.
 */
async function checkSessionClean(target) {
  const [rows] = await target.execute("SELECT @sym_is_syncing AS val");
  const val = rows[0]?.val;
  if (val !== null && val !== 0) {
    console.warn(
      `[WARN] @sym_is_syncing is set to ${JSON.stringify(val)} on target session. ` +
        `Migration inserts will be suppressed. Clear with "SET @sym_is_syncing = NULL".`,
    );
    return false;
  }
  return true;
}

async function main() {
  const legacy = await mysql.createConnection({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT) || 3306,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
  });
  const target = await mysql.createConnection({
    host: process.env.DB_TARGET_HOST,
    port: Number(process.env.DB_TARGET_PORT) || 3306,
    user: process.env.DB_TARGET_USERNAME,
    password: process.env.DB_TARGET_PASSWORD,
    database: process.env.DB_TARGET_DATABASE,
  });

  const store = process.env.SOURCE_STORE_RECORD_ID;

  /* ================================================================ */
  /* SECTION 1: Pre-migration readiness checks                        */
  /* ================================================================ */

  console.log("=".repeat(64));
  console.log("  PRE-MIGRATION READINESS AUDIT");
  console.log("=".repeat(64));
  console.log("");

  const readiness = {
    clusterOk: false,
    triggersOk: false,
    sessionClean: false,
    hasProducts: false,
  };

  // --- Cluster node check ---
  console.log("▸ Target cluster status");
  const cluster = await checkClusterNode(target);
  if (cluster.isClusterNode) {
    console.log("  ✅ Target has sym_change_log and sym_checkpoint");
    readiness.clusterOk = true;
  } else {
    console.error("  ❌ Target is NOT a replica cluster node.");
    console.error(
      `     sym_change_log: ${cluster.hasLog ? "present" : "MISSING"}`,
    );
    console.error(
      `     sym_checkpoint: ${cluster.hasCheckpoint ? "present" : "MISSING"}`,
    );
    console.error(
      "     → Inserts will NOT propagate to hub or other branches.",
    );
    console.error(
      "     → Run the replica engine on this target first, or point DB_TARGET_HOST",
    );
    console.error("       at the hub / a branch that's part of the cluster.");
  }

  // --- Trigger check ---
  if (cluster.isClusterNode) {
    console.log("");
    console.log("▸ Target trigger presence");
    const trig = await checkTriggers(target);
    if (trig.problems.length === 0) {
      console.log(
        `  ✅ All ${SYNC_TABLES.length} sync tables have 3 triggers each`,
      );
      for (const table of SYNC_TABLES) {
        console.log(`     ${table}: ${trig.details[table].length}/3`);
      }
      readiness.triggersOk = true;
    } else {
      console.error("  ❌ Missing triggers:");
      for (const p of trig.problems) console.error("     " + p);
      console.error("     → Inserts into these tables will NOT be logged.");
      console.error('     → Run "node triggers.js" on this target to install.');
    }
  } else {
    console.log("");
    console.log("▸ Target trigger presence");
    console.log("  ⏭️  Skipped (target is not a cluster node)");
  }

  // --- Session check ---
  if (cluster.isClusterNode) {
    console.log("");
    console.log("▸ Target session state");
    const clean = await checkSessionClean(target);
    if (clean) {
      console.log("  ✅ @sym_is_syncing is not set (triggers will fire)");
      readiness.sessionClean = true;
    }
  }

  // --- Product count ---
  console.log("");
  console.log("▸ Source store content");
  const productCount = await checkProductCount(legacy, store);
  if (productCount > 0) {
    console.log(
      `  ✅ Store ${store} has ${productCount} product(s) with sales history`,
    );
    readiness.hasProducts = true;
  } else {
    console.error(`  ❌ Store ${store} has 0 products with sales history`);
    console.error(
      "     → Migration would insert nothing. Check SOURCE_STORE_RECORD_ID.",
    );
  }

  /* ================================================================ */
  /* SECTION 2: Lookup table audit                                     */
  /* ================================================================ */

  console.log("");
  console.log("=".repeat(64));
  console.log("  LOOKUP TABLE AUDIT");
  console.log("=".repeat(64));

  const lookups = [
    {
      label: "product_type",
      targetTable: "product_type",
      sql: `SELECT DISTINCT pt.name AS name
            FROM product p
            LEFT JOIN product_type pt ON p.product_type = pt.record_id
            WHERE p.product_code IN (
              SELECT p_sub.product_code FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              JOIN product p_sub ON sod_sub.product = p_sub.record_id
              WHERE so_sub.store = ?
            )`,
    },
    {
      label: "product_category",
      targetTable: "product_category",
      sql: `SELECT DISTINCT pc.name AS name
            FROM product p
            LEFT JOIN product_category pc ON p.category = pc.record_id
            WHERE p.product_code IN (
              SELECT p_sub.product_code FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              JOIN product p_sub ON sod_sub.product = p_sub.record_id
              WHERE so_sub.store = ?
            )`,
    },
    {
      label: "classification",
      targetTable: "classification",
      sql: `SELECT DISTINCT c.name AS name
            FROM product p
            LEFT JOIN classification c ON p.classification = c.record_id
            WHERE p.product_code IN (
              SELECT p_sub.product_code FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              JOIN product p_sub ON sod_sub.product = p_sub.record_id
              WHERE so_sub.store = ?
            )`,
    },
    {
      label: "price",
      targetTable: "price",
      sql: `SELECT DISTINCT pr.name AS name
            FROM pricing_scheme ps
            LEFT JOIN price pr ON ps.price = pr.record_id
            WHERE ps.store = ? AND ps.product IN (
              SELECT sod_sub.product FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              WHERE so_sub.store = ?
            )`,
      params: [store, store],
    },
    {
      label: "unit_of_measure",
      targetTable: "unit_of_measure",
      sql: `SELECT DISTINCT uom.name AS name
            FROM pricing_scheme ps
            LEFT JOIN unit_of_measure uom ON ps.uom = uom.record_id
            WHERE ps.store = ? AND ps.product IN (
              SELECT sod_sub.product FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              WHERE so_sub.store = ?
            )`,
      params: [store, store],
    },
  ];

  // Track totals for final readiness summary
  const totals = {
    aliasTotal: 0,
    inactiveTotal: 0,
    missingTotal: 0,
  };

  for (const { label, targetTable, sql, params } of lookups) {
    const [legacyRows] = await legacy.execute(sql, params ?? [store]);

    const [activeRows] = await target.execute(
      `SELECT name FROM ${targetTable} WHERE status = 1`,
    );
    const [allRows] = await target.execute(
      `SELECT name, status FROM ${targetTable}`,
    );

    const activeSet = new Set(
      activeRows.map((r) => norm(r.name)).filter(Boolean),
    );
    const inactiveOnlySet = new Set();
    for (const r of allRows) {
      const k = norm(r.name);
      if (!k) continue;
      if (!activeSet.has(k)) inactiveOnlySet.add(k);
    }

    const missingEntirely = [];
    const inactiveOnly = [];
    const alias = [];

    const seen = new Set();
    for (const row of legacyRows) {
      const raw = row.name;
      const key = norm(raw);
      if (key == null || seen.has(key)) continue;
      seen.add(key);

      if (activeSet.has(key)) continue;

      const aliasTo = ALIASES[label]?.[key];
      if (aliasTo && activeSet.has(norm(aliasTo))) {
        alias.push({ from: raw, to: aliasTo });
        continue;
      }

      if (inactiveOnlySet.has(key)) inactiveOnly.push(raw);
      else missingEntirely.push(raw);
    }

    totals.aliasTotal += alias.length;
    totals.inactiveTotal += inactiveOnly.length;
    totals.missingTotal += missingEntirely.length;

    console.log(`\n=== ${label} (target table: ${targetTable}) ===`);
    console.log(`  legacy distinct: ${seen.size}`);
    console.log(
      `  active exact matches: ${
        seen.size - alias.length - inactiveOnly.length - missingEntirely.length
      }`,
    );
    console.log(`  alias-mapped: ${alias.length}`);
    for (const a of alias)
      console.log(`    ~ ${JSON.stringify(a.from)} -> ${JSON.stringify(a.to)}`);
    console.log(
      `  only-inactive in target (flip status=1 or alias): ${inactiveOnly.length}`,
    );
    for (const u of inactiveOnly.sort())
      console.log(`    - ${JSON.stringify(u)}`);
    console.log(`  missing entirely (INSERT): ${missingEntirely.length}`);
    for (const u of missingEntirely.sort())
      console.log(`    - ${JSON.stringify(u)}`);
  }

  /* ================================================================ */
  /* SECTION 3: Final readiness summary                                */
  /* ================================================================ */

  console.log("");
  console.log("=".repeat(64));
  console.log("  FINAL READINESS SUMMARY");
  console.log("=".repeat(64));
  console.log("");

  // Cluster
  console.log(
    `  ${readiness.clusterOk ? "✅" : "❌"} Target is a cluster node`,
  );

  // Triggers
  if (cluster.isClusterNode) {
    console.log(
      `  ${readiness.triggersOk ? "✅" : "❌"} All sync triggers present`,
    );
    console.log(
      `  ${readiness.sessionClean ? "✅" : "❌"} @sym_is_syncing clean`,
    );
  } else {
    console.log("  ⏭️  Triggers: skipped (not a cluster node)");
  }

  // Store content
  console.log(
    `  ${readiness.hasProducts ? "✅" : "❌"} Store has products to migrate`,
  );

  // Lookups
  console.log(
    `  ${totals.missingTotal === 0 ? "✅" : "❌"} Lookups missing entirely: ${totals.missingTotal}`,
  );
  console.log(
    `  ${totals.inactiveTotal === 0 ? "✅" : "❌"} Lookups inactive-only: ${totals.inactiveTotal}`,
  );
  console.log(
    `  ${totals.aliasTotal >= 0 ? "ℹ️ " : " "} Lookups alias-mapped: ${totals.aliasTotal}`,
  );

  console.log("");

  /* ================================================================ */
  /* SECTION 4: Verdict + exit code                                    */
  /* ================================================================ */

  const blockers = [];
  const warnings = [];

  if (!readiness.clusterOk) {
    blockers.push("Target is not a cluster node — inserts won't propagate.");
  }
  if (cluster.isClusterNode && !readiness.triggersOk) {
    blockers.push("Missing sync triggers — inserts won't be logged.");
  }
  if (cluster.isClusterNode && !readiness.sessionClean) {
    blockers.push("@sym_is_syncing is set — inserts will be suppressed.");
  }
  if (!readiness.hasProducts) {
    blockers.push("Store has no products — migration will insert nothing.");
  }
  if (totals.missingTotal > 0) {
    blockers.push(
      `${totals.missingTotal} lookup row(s) missing entirely — migration will fail or fall back.`,
    );
  }
  if (totals.inactiveTotal > 0) {
    blockers.push(
      `${totals.inactiveTotal} lookup row(s) exist but inactive — migration won't resolve them.`,
    );
  }
  if (totals.aliasTotal > 0) {
    warnings.push(
      `${totals.aliasTotal} lookup row(s) will resolve via ALIASES — verify the mappings are correct.`,
    );
  }

  if (blockers.length > 0) {
    console.log("🚫 MIGRATION NOT READY");
    console.log("");
    console.log("  Blockers:");
    for (const b of blockers) console.log(`    ✗ ${b}`);
    if (warnings.length > 0) {
      console.log("");
      console.log("  Warnings:");
      for (const w of warnings) console.log(`    ⚠ ${w}`);
    }
    console.log("");
    console.log("  Fix blockers above before running the migration.");
    console.log("");

    await legacy.end();
    await target.end();
    process.exit(1);
  }

  if (warnings.length > 0) {
    console.log("⚠️  MIGRATION READY (with warnings)");
    console.log("");
    for (const w of warnings) console.log(`    ⚠ ${w}`);
    console.log("");
    console.log(
      "  Consider verifying the alias mappings before running the migration.",
    );
    console.log("");

    await legacy.end();
    await target.end();
    process.exit(0);
  }

  console.log("✅ MIGRATION READY");
  console.log("");
  console.log("  All checks passed. Safe to run:");
  console.log("    node runner.js --dry-run");
  console.log("    node runner.js");
  console.log("");

  await legacy.end();
  await target.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
