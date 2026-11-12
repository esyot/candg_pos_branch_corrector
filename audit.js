/**
 * main.js — Pre-migration readiness audit.
 *
 * Verifies target is safe to write to, that lookups resolve, and that the
 * source product_code values don't contain hidden corruption that runner.js
 * will sanitize (and, if they do, warns about any hard collisions that would
 * lose a distinct product).
 *
 * Exit codes:
 *   0  — ready (possibly with warnings)
 *   1  — blocked (fix blockers before running runner.js)
 */

require("dotenv").config();
const mysql = require("mysql2/promise");
const { diffLookup, norm } = require("./diff-lookup");

/* ================================================================== */
/* Product-code sanitization — mirrors runner.js                      */
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

/* ================================================================== */
/* Aliases (lookup renames)                                           */
/* ================================================================== */
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

/* ================================================================== */
/* Preflight helpers                                                  */
/* ================================================================== */
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

/* ================================================================== */
/* Product-code corruption audit                                      */
/* ================================================================== */
async function auditProductCodes(legacy, store) {
  const [rows] = await legacy.execute(
    `SELECT DISTINCT p.product_code, p.particular
     FROM sales_order so
     JOIN sales_order_details sod ON so.record_id = sod.sales_order
     JOIN product p ON sod.product = p.record_id
     WHERE so.store = ?`,
    [store],
  );

  const corrupt = [];
  const cleanByCode = new Map();

  for (const r of rows) {
    const wasCorrupt = isCorruptCode(r.product_code);
    const clean = sanitizeProductCode(r.product_code);
    if (!clean) continue;

    if (wasCorrupt) {
      corrupt.push({
        raw: r.product_code,
        clean,
        particular: r.particular,
      });
    }

    if (!cleanByCode.has(clean)) cleanByCode.set(clean, []);
    cleanByCode.get(clean).push({
      raw: r.product_code,
      particular: r.particular,
      wasCorrupt,
    });
  }

  const softCollisions = [];
  const hardCollisions = [];
  for (const [clean, list] of cleanByCode) {
    const distinctRaw = new Set(list.map((x) => x.raw));
    if (distinctRaw.size <= 1) continue;

    const particulars = new Set(
      list.map((x) =>
        String(x.particular || "")
          .trim()
          .toLowerCase(),
      ),
    );
    const bucket = particulars.size > 1 ? hardCollisions : softCollisions;
    bucket.push({
      clean,
      forms: [...distinctRaw],
      particulars: [...new Set(list.map((x) => x.particular))],
    });
  }

  return {
    totalRaw: rows.length,
    uniqueCleanCodes: cleanByCode.size,
    corrupt,
    softCollisions,
    hardCollisions,
  };
}

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */
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

  try {
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
        console.error(
          '     → Run "node triggers.js" on this target to install.',
        );
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
    /* SECTION 1.5: Product-code corruption audit                       */
    /* ================================================================ */

    console.log("");
    console.log("=".repeat(64));
    console.log("  PRODUCT-CODE CORRUPTION AUDIT (source)");
    console.log("=".repeat(64));
    console.log("");

    const codeAudit = await auditProductCodes(legacy, store);

    console.log(`▸ Legacy product_code values for store ${store}`);
    console.log(`  total distinct raw : ${codeAudit.totalRaw}`);
    console.log(`  unique clean codes : ${codeAudit.uniqueCleanCodes}`);
    console.log(`  corrupt raw forms  : ${codeAudit.corrupt.length}`);

    if (codeAudit.corrupt.length > 0) {
      console.log("");
      console.log(
        "  ⚠ Corruption found (runner.js will sanitize before insert):",
      );
      for (const c of codeAudit.corrupt.slice(0, 25)) {
        console.log(
          `    "${c.raw}"  →  "${c.clean}"` +
            (c.particular ? `  (${JSON.stringify(c.particular)})` : ""),
        );
      }
      if (codeAudit.corrupt.length > 25) {
        console.log(`    … and ${codeAudit.corrupt.length - 25} more.`);
      }
    } else {
      console.log("  ✅ No corrupt product_code values found");
    }

    if (codeAudit.softCollisions.length > 0) {
      console.log("");
      console.log(
        `  ℹ️  ${codeAudit.softCollisions.length} soft collision(s) — same particular, different raw form (OK, runner keeps first):`,
      );
      for (const c of codeAudit.softCollisions) {
        console.log(`    → "${c.clean}" from ${c.forms.length} raw forms`);
      }
    }

    if (codeAudit.hardCollisions.length > 0) {
      console.log("");
      console.log(
        `  ❌ ${codeAudit.hardCollisions.length} HARD collision(s) — different particulars collapsing to same code:`,
      );
      for (const c of codeAudit.hardCollisions) {
        console.log(`    Code "${c.clean}":`);
        for (const f of c.forms) console.log(`      raw: "${f}"`);
        for (const p of c.particulars)
          console.log(`      particular: ${JSON.stringify(p)}`);
      }
      console.log(
        `    ⚠ Runner keeps the first-seen particular. Verify before migration.`,
      );
    }

    /* ================================================================ */
    /* SECTION 2: Lookup table audit (via diff-lookup.js)               */
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

    const lookupResults = [];
    for (const { label, targetTable, sql, params } of lookups) {
      const res = await diffLookup(legacy, target, {
        label,
        targetTable,
        sql,
        params: params ?? [store],
        aliases: ALIASES[label],
      });
      lookupResults.push(res);
    }

    const lookupTotals = lookupResults.reduce(
      (acc, r) => {
        acc.aliasTotal += r.counts.ALIAS || 0;
        acc.inactiveTotal += r.softMisses;
        acc.missingTotal += r.hardMisses;
        acc.ambiguousTotal += r.ambiguous;
        return acc;
      },
      { aliasTotal: 0, inactiveTotal: 0, missingTotal: 0, ambiguousTotal: 0 },
    );

    /* ================================================================ */
    /* SECTION 3: Final readiness summary                               */
    /* ================================================================ */

    console.log("");
    console.log("=".repeat(64));
    console.log("  FINAL READINESS SUMMARY");
    console.log("=".repeat(64));
    console.log("");

    console.log(
      `  ${readiness.clusterOk ? "✅" : "❌"} Target is a cluster node`,
    );

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

    console.log(
      `  ${readiness.hasProducts ? "✅" : "❌"} Store has products to migrate`,
    );

    if (codeAudit.corrupt.length === 0) {
      console.log("  ✅ No corrupt product_code values in source");
    } else {
      console.log(
        `  ℹ️  ${codeAudit.corrupt.length} corrupt product_code(s) — runner will sanitize`,
      );
    }
    if (codeAudit.hardCollisions.length > 0) {
      console.log(
        `  ❌ ${codeAudit.hardCollisions.length} hard code collision(s) — manual review`,
      );
    }
    if (codeAudit.softCollisions.length > 0) {
      console.log(
        `  ✅ ${codeAudit.softCollisions.length} soft code collision(s) — auto-resolvable`,
      );
    }

    console.log(
      `  ${lookupTotals.missingTotal === 0 ? "✅" : "❌"} Lookups missing entirely: ${lookupTotals.missingTotal}`,
    );
    console.log(
      `  ${lookupTotals.inactiveTotal === 0 ? "✅" : "❌"} Lookups inactive-only: ${lookupTotals.inactiveTotal}`,
    );
    console.log(`  ℹ️  Lookups alias-mapped: ${lookupTotals.aliasTotal}`);
    if (lookupTotals.ambiguousTotal > 0) {
      console.log(
        `  ⚠️  Lookups ambiguous (multi-active): ${lookupTotals.ambiguousTotal}`,
      );
    }

    console.log("");

    /* ================================================================ */
    /* SECTION 4: Verdict + exit code                                   */
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
    if (lookupTotals.missingTotal > 0) {
      blockers.push(
        `${lookupTotals.missingTotal} lookup row(s) missing entirely — migration will fail or fall back.`,
      );
    }
    if (lookupTotals.inactiveTotal > 0) {
      blockers.push(
        `${lookupTotals.inactiveTotal} lookup row(s) exist but inactive — migration won't resolve them.`,
      );
    }
    if (codeAudit.hardCollisions.length > 0) {
      blockers.push(
        `${codeAudit.hardCollisions.length} hard product_code collision(s) — two distinct products would collapse into one.`,
      );
    }
    if (lookupTotals.aliasTotal > 0) {
      warnings.push(
        `${lookupTotals.aliasTotal} lookup row(s) will resolve via ALIASES — verify the mappings are correct.`,
      );
    }
    if (lookupTotals.ambiguousTotal > 0) {
      warnings.push(
        `${lookupTotals.ambiguousTotal} lookup name(s) have multiple active target rows — resolver picks first.`,
      );
    }
    if (codeAudit.corrupt.length > 0) {
      warnings.push(
        `${codeAudit.corrupt.length} corrupt product_code(s) will be sanitized by runner.js before insert.`,
      );
    }
    if (codeAudit.softCollisions.length > 0) {
      warnings.push(
        `${codeAudit.softCollisions.length} soft code collision(s) — runner keeps first-seen particular per code.`,
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

      process.exitCode = 1;
      return;
    }

    if (warnings.length > 0) {
      console.log("⚠️  MIGRATION READY (with warnings)");
      console.log("");
      for (const w of warnings) console.log(`    ⚠ ${w}`);
      console.log("");
      console.log(
        "  Consider reviewing the warnings above before running the migration.",
      );
      console.log("");
      return;
    }

    console.log("✅ MIGRATION READY");
    console.log("");
    console.log("  All checks passed. Safe to run:");
    console.log("    node runner.js --dry-run");
    console.log("    node runner.js");
    console.log("");
  } finally {
    await legacy.end();
    await target.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
