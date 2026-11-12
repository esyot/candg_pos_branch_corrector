require("dotenv").config();
const mysql = require("mysql2/promise");
const crypto = require("crypto");
const { execSync } = require("child_process");

const BATCH_SIZE = 500;

/* ------------------------------------------------------------------ */
/* Pre-flight / post-flight helpers                                   */
/* ------------------------------------------------------------------ */

const SYNC_TABLES = ["product", "pricing_scheme", "stocks_on_hand"];

/**
 * Verify that every sync table on the target has all 3 triggers
 * (INSERT, UPDATE, DELETE). If any are missing, inserts from this
 * migration will NOT be logged and will NOT propagate to other nodes.
 *
 * Throws if the target is not safe to write to.
 */
async function verifyTargetTriggers(targetDb) {
  console.log("[Preflight] Checking sync triggers on target...");
  const problems = [];

  for (const table of SYNC_TABLES) {
    const [rows] = await targetDb.execute(
      `SELECT TRIGGER_NAME, EVENT_MANIPULATION
       FROM information_schema.TRIGGERS
       WHERE TRIGGER_SCHEMA = DATABASE()
         AND TRIGGER_NAME IN (?, ?, ?)`,
      [
        `sym_trig_${table}_ins`,
        `sym_trig_${table}_upd`,
        `sym_trig_${table}_del`,
      ],
    );

    const found = new Set(rows.map((r) => r.TRIGGER_NAME));
    const expected = [
      `sym_trig_${table}_ins`,
      `sym_trig_${table}_upd`,
      `sym_trig_${table}_del`,
    ];
    const missing = expected.filter((t) => !found.has(t));

    if (missing.length === 0) {
      console.log(`  ✅ ${table}: 3/3 triggers`);
    } else {
      console.error(`  ❌ ${table}: missing ${missing.join(", ")}`);
      problems.push(`${table}: missing ${missing.join(", ")}`);
    }
  }

  if (problems.length > 0) {
    throw new Error(
      `\n[Preflight FAILED] Target is missing sync triggers.\n` +
        `If we proceed, inserted rows would NOT be logged into sym_change_log\n` +
        `and would NOT propagate to hub or other branches.\n\n` +
        `Problems:\n  ${problems.join("\n  ")}\n\n` +
        `Fix: run "node triggers.js" on this target to install triggers,\n` +
        `or set SETUP_TRIGGERS_ON_STARTUP=true and restart the engine once.`,
    );
  }

  // Also verify sym_change_log and sym_checkpoint tables exist
  const [syncTables] = await targetDb.execute(
    `SELECT TABLE_NAME FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN ('sym_change_log', 'sym_checkpoint')`,
  );
  const presentTables = new Set(syncTables.map((r) => r.TABLE_NAME));
  if (!presentTables.has("sym_change_log")) {
    throw new Error(
      "[Preflight FAILED] sym_change_log table does not exist on target. " +
        "Run the replica engine once to provision the target.",
    );
  }

  console.log(
    "[Preflight] sym_change_log and sym_checkpoint tables present ✅",
  );
}

/**
 * Verify that @sym_is_syncing is not set on this session.
 * If it's set to TRUE (e.g., leaked from a previous operation), triggers
 * will be suppressed and inserts won't be logged.
 */
async function verifySessionIsClean(targetDb) {
  const [rows] = await targetDb.execute("SELECT @sym_is_syncing AS val");
  const val = rows[0]?.val;
  if (val !== null && val !== 0) {
    console.warn(
      `[Preflight] @sym_is_syncing is set to ${JSON.stringify(val)}. ` +
        `Clearing it so triggers fire.`,
    );
    await targetDb.execute("SET @sym_is_syncing = NULL");
  }
  console.log("[Preflight] Session @sym_is_syncing is clean ✅");
}

/**
 * After the migration commits, verify that sym_change_log actually
 * received INSERT events for the migrated tables. If any table has 0
 * events, the triggers didn't fire and the data will not propagate.
 */
async function verifySyncLogPopulated(targetDb, startTime) {
  console.log("");
  console.log("[Postflight] Checking sym_change_log for new INSERT events...");

  let allPopulated = true;
  for (const table of SYNC_TABLES) {
    const [rows] = await targetDb.execute(
      `SELECT COUNT(*) AS c FROM sym_change_log
       WHERE table_name = ?
         AND operation = 'INSERT'
         AND created_at >= ?`,
      [table, startTime],
    );
    const count = Number(rows[0]?.c || 0);
    if (count === 0) {
      console.error(
        `  ⚠️  ${table}: 0 INSERT events in log since migration started!`,
      );
      console.error(
        `      → Triggers did NOT fire. Data will NOT sync to other nodes.`,
      );
      allPopulated = false;
    } else {
      console.log(`  ✅ ${table}: ${count} INSERT event(s) in log`);
    }
  }

  if (!allPopulated) {
    throw new Error(
      "\n[Postflight FAILED] Some tables have no INSERT events in sym_change_log.\n" +
        "The data was inserted into the target tables but NOT logged.\n" +
        "Consequence: the sync engine cannot propagate these rows to hub or other branches.\n\n" +
        "Likely causes:\n" +
        "  - Triggers were dropped between preflight and now (unlikely)\n" +
        "  - @sym_is_syncing was set during the inserts (should not happen now)\n" +
        "  - Trigger definition is wrong (check SHOW CREATE TRIGGER)\n\n" +
        "Fix: run 'node triggers.js' on this target, then manually re-run the migration.",
    );
  }

  console.log("[Postflight] All tables have INSERT events in log ✅");
}

/**
 * Wait for the sync engine to propagate, then run node-copy --verify
 * to confirm all nodes are in parity.
 *
 * Optional — only runs if VERIFY_AFTER_MIGRATION=true is set in .env.
 */
async function waitAndVerifyParity() {
  if (process.env.VERIFY_AFTER_MIGRATION !== "true") {
    console.log("");
    console.log(
      "[Postflight] Skipping parity check (set VERIFY_AFTER_MIGRATION=true to enable)",
    );
    return;
  }

  const waitMs = Number(process.env.VERIFY_WAIT_MS) || 15000;
  console.log("");
  console.log(
    `[Postflight] Waiting ${waitMs / 1000}s for sync engine to propagate...`,
  );
  await new Promise((r) => setTimeout(r, waitMs));

  console.log("[Postflight] Running node-copy.js --verify...");
  try {
    const output = execSync(
      `node node-copy.js --verify --tables=${SYNC_TABLES.join(",")}`,
      { cwd: __dirname, encoding: "utf8", stdio: "pipe" },
    );
    const warns = output.split("\n").filter((l) => l.includes("[WARN]"));
    if (warns.length > 0) {
      console.error("[Postflight] ⚠️  Drift detected after migration:");
      warns.forEach((w) => console.error("  " + w));
      console.error(
        "[Postflight] The engine may still be processing. " +
          "Wait another 30s and run 'node node-copy.js --verify --tables=" +
          SYNC_TABLES.join(",") +
          "' manually.",
      );
    } else {
      console.log("[Postflight] ✅ All nodes in parity after migration");
    }
  } catch (err) {
    console.error(
      `[Postflight] Parity check failed: ${err.message}. ` +
        `Run 'node node-copy.js --verify --tables=${SYNC_TABLES.join(",")}' manually.`,
    );
  }
}

/* ------------------------------------------------------------------ */
/* Main migration                                                     */
/* ------------------------------------------------------------------ */

async function runMigration() {
  const isDryRun = process.argv.includes("--dry-run");
  const allowFallback = process.argv.includes("--allow-fallback");
  const sourceStoreId = process.env.SOURCE_STORE_RECORD_ID;
  const targetStoreUuid = process.env.TARGET_STORE_RECORD_ID;

  if (!sourceStoreId || !targetStoreUuid) {
    console.error(
      "[Error] Please provide both SOURCE_STORE_RECORD_ID and TARGET_STORE_RECORD_ID in your .env file.",
    );
    process.exit(1);
  }

  if (isDryRun) {
    console.log("--------------------------------------------------");
    console.log(
      "⚠️  TRANSACTION DRY RUN MODE - Will execute fully then ROLLBACK",
    );
    console.log("--------------------------------------------------");
  }

  const legacyPort = Number(process.env.DB_PORT) || 3306;
  const targetPort = Number(process.env.DB_TARGET_PORT) || 3306;

  if (!Number.isInteger(legacyPort) || !Number.isInteger(targetPort)) {
    console.error(
      "[Error] Invalid DB_PORT / DB_TARGET_PORT in .env. Ports must be integers.",
    );
    process.exit(1);
  }

  const legacyDb = await mysql.createConnection({
    host: process.env.DB_HOST,
    port: legacyPort,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
  });

  const targetDb = await mysql.createConnection({
    host: process.env.DB_TARGET_HOST,
    port: targetPort,
    user: process.env.DB_TARGET_USERNAME,
    password: process.env.DB_TARGET_PASSWORD,
    database: process.env.DB_TARGET_DATABASE,
  });

  try {
    console.log(
      `[Migration] Starting transaction-driven sync for Legacy Store ID: ${sourceStoreId} -> Target Store UUID: ${targetStoreUuid}`,
    );
    console.log(
      `[Migration] Legacy DB: ${process.env.DB_HOST}:${legacyPort}/${process.env.DB_DATABASE}`,
    );
    console.log(
      `[Migration] Target DB: ${process.env.DB_TARGET_HOST}:${targetPort}/${process.env.DB_TARGET_DATABASE}`,
    );

    // ─── NEW: pre-flight trigger & session checks ───
    await verifyTargetTriggers(targetDb);
    await verifySessionIsClean(targetDb);

    // Record when the migration starts so postflight can filter log entries
    const migrationStartTime = new Date();

    // Start database transaction on target DB
    await targetDb.beginTransaction();

    // ----------------------------------------------------
    // HELPERS: Batch + lookup utilities
    // ----------------------------------------------------
    function chunk(array, size) {
      const chunks = [];
      for (let i = 0; i < array.length; i += size) {
        chunks.push(array.slice(i, i + size));
      }
      return chunks;
    }

    function normalizeName(name) {
      if (name == null) return null;
      return String(name).trim().toLowerCase();
    }

    const lookupAliases = {
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

    async function loadNameMap(tableName) {
      const [rows] = await targetDb.execute(
        `SELECT record_id, name FROM ${tableName} WHERE status = 1 ORDER BY record_id`,
      );
      const map = new Map();
      for (const row of rows) {
        if (row.name == null) continue;
        const key = normalizeName(row.name);
        if (!map.has(key)) map.set(key, row.record_id);
      }
      return map;
    }

    function resolveUuid(map, tableName, name, context) {
      if (name != null) {
        const key = normalizeName(name);
        let id = map.get(key);
        if (id) return id;

        const aliasTarget = lookupAliases[tableName]?.[key];
        if (aliasTarget) {
          id = map.get(normalizeName(aliasTarget));
          if (id) return id;
        }
      }

      if (!allowFallback) {
        throw new Error(
          `[Migration] '${name ?? "(no name)"}' not found in target table '${tableName}' (${context}). ` +
            "Aborting to avoid corrupt links. Create the missing target record first, " +
            "or re-run with --allow-fallback to use the first row instead.",
        );
      }

      console.warn(
        `[Migration] WARN: '${name ?? "(no name)"}' not found in '${tableName}' (${context}) - falling back to first row.`,
      );
      if (map.size === 0) {
        throw new Error(
          `[Migration] Target table '${tableName}' is empty - cannot resolve '${context}'.`,
        );
      }
      return map.values().next().value;
    }

    // ----------------------------------------------------
    // STEP 1: FETCH ACTIVE PRODUCTS VIA SALES TRANSACTIONS
    // ----------------------------------------------------
    const [soldProducts] = await legacyDb.execute(
      `
      SELECT DISTINCT 
        p.product_code, 
        p.particular, 
        pc.name as category_name, 
        c.name as classification_name, 
        pt.name as product_type_name, 
        p.reorder_point, 
        p.status as product_status
      FROM sales_order so
      JOIN sales_order_details sod ON so.record_id = sod.sales_order
      JOIN product p ON sod.product = p.record_id
      LEFT JOIN product_category pc ON p.category = pc.record_id
      LEFT JOIN classification c ON p.classification = c.record_id
      LEFT JOIN product_type pt ON p.product_type = pt.record_id
      WHERE so.store = ?
    `,
      [sourceStoreId],
    );

    console.log(
      `[Migration] Found ${soldProducts.length} unique products with sales history for this store. Provisioning catalog...`,
    );

    const productIdByCode = new Map();
    if (soldProducts.length > 0) {
      const placeholders = soldProducts.map(() => "?").join(",");
      const [existingProducts] = await targetDb.execute(
        `SELECT record_id, product_code FROM product WHERE product_code IN (${placeholders})`,
        soldProducts.map((p) => p.product_code),
      );
      for (const p of existingProducts) {
        productIdByCode.set(p.product_code, p.record_id);
      }
    }
    const catMap = await loadNameMap("product_category");
    const classMap = await loadNameMap("classification");
    const typeMap = await loadNameMap("product_type");

    const productsToInsert = soldProducts
      .filter((item) => !productIdByCode.has(item.product_code))
      .map((item) => {
        const context = `product ${item.product_code}`;
        return {
          record_id: crypto.randomUUID(),
          product_code: item.product_code,
          particular: item.particular || "MIGRATED PRODUCT",
          category: resolveUuid(
            catMap,
            "product_category",
            item.category_name,
            context,
          ),
          classification: resolveUuid(
            classMap,
            "classification",
            item.classification_name,
            context,
          ),
          product_type: resolveUuid(
            typeMap,
            "product_type",
            item.product_type_name,
            context,
          ),
          reorder_point: item.reorder_point || 0,
          status: item.product_status ?? 1,
        };
      });

    let productsCreated = 0;
    for (const batch of chunk(productsToInsert, BATCH_SIZE)) {
      if (batch.length === 0) continue;
      const placeholders = batch
        .map(() => "(?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW(), NOW())")
        .join(",");
      const values = batch.flatMap((p) => [
        p.record_id,
        p.product_code,
        p.particular,
        p.category,
        p.classification,
        p.product_type,
        p.reorder_point,
        p.status,
      ]);
      await targetDb.execute(
        `
        INSERT INTO product (
          record_id, product_code, particular, category, classification, product_type, reorder_point, status, date_added, created_at, updated_at
        ) VALUES ${placeholders}
      `,
        values,
      );
      for (const p of batch) {
        productIdByCode.set(p.product_code, p.record_id);
      }
      productsCreated += batch.length;
    }
    console.log(
      `[Migration] Catalog provisioning completed. (${productsCreated} new products processed in batches of ${BATCH_SIZE})`,
    );

    // ----------------------------------------------------
    // STEP 2: SYNC PRICING SCHEMES
    // ----------------------------------------------------
    const [legacyPricing] = await legacyDb.execute(
      `
      SELECT 
        p.product_code, 
        ps.amount, 
        ps.is_default, 
        ps.status as pricing_status,
        pr.name as price_name,
        uom.name as uom_name
      FROM pricing_scheme ps
      JOIN product p ON ps.product = p.record_id
      LEFT JOIN price pr ON ps.price = pr.record_id
      LEFT JOIN unit_of_measure uom ON ps.uom = uom.record_id
      WHERE ps.store = ? AND p.product_code IN (
        SELECT DISTINCT p_sub.product_code 
        FROM sales_order so_sub
        JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
        JOIN product p_sub ON sod_sub.product = p_sub.record_id
        WHERE so_sub.store = ?
      )
    `,
      [sourceStoreId, sourceStoreId],
    );

    console.log(
      `[Migration] Processing ${legacyPricing.length} pricing records for active store products...`,
    );

    const priceMap = await loadNameMap("price");
    const uomMap = await loadNameMap("unit_of_measure");

    const pricingRows = [];
    for (const item of legacyPricing) {
      const productUuid = productIdByCode.get(item.product_code);
      if (!productUuid) continue;

      pricingRows.push({
        record_id: crypto.randomUUID(),
        product: productUuid,
        store: targetStoreUuid,
        amount: item.amount,
        price: resolveUuid(
          priceMap,
          "price",
          item.price_name,
          `pricing for ${item.product_code}`,
        ),
        uom: resolveUuid(
          uomMap,
          "unit_of_measure",
          item.uom_name,
          `pricing for ${item.product_code}`,
        ),
        is_default: item.is_default,
        status: item.pricing_status ?? 1,
      });
    }

    let pricingSynced = 0;
    for (const batch of chunk(pricingRows, BATCH_SIZE)) {
      if (batch.length === 0) continue;
      const placeholders = batch
        .map(() => "(?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())")
        .join(",");
      const values = batch.flatMap((r) => [
        r.record_id,
        r.product,
        r.store,
        r.amount,
        r.price,
        r.uom,
        r.is_default,
        r.status,
      ]);
      await targetDb.execute(
        `
        INSERT INTO pricing_scheme (
          record_id, product, store, amount, price, uom, is_default, status, created_at, updated_at
        ) VALUES ${placeholders}
        ON DUPLICATE KEY UPDATE 
          amount = VALUES(amount),
          price = VALUES(price),
          uom = VALUES(uom),
          is_default = VALUES(is_default),
          status = VALUES(status),
          updated_at = NOW()
      `,
        values,
      );
      pricingSynced += batch.length;
    }
    console.log(
      `[Migration] Pricing sync completed. (${pricingSynced} records upserted in batches)`,
    );

    // ----------------------------------------------------
    // STEP 3: SYNC STOCKS ON HAND
    // ----------------------------------------------------
    const [legacyStocks] = await legacyDb.execute(
      `
      SELECT 
        p.product_code, 
        s.qty, 
        s.status as stock_status
      FROM stocks_on_hand s
      JOIN product p ON s.product = p.record_id
      WHERE s.store = ? AND p.product_code IN (
        SELECT DISTINCT p_sub.product_code 
        FROM sales_order so_sub
        JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
        JOIN product p_sub ON sod_sub.product = p_sub.record_id
        WHERE so_sub.store = ?
      )
    `,
      [sourceStoreId, sourceStoreId],
    );

    console.log(
      `[Migration] Processing ${legacyStocks.length} stock records for active store products...`,
    );

    const stockRows = [];
    for (const stock of legacyStocks) {
      const productUuid = productIdByCode.get(stock.product_code);
      if (!productUuid) continue;

      stockRows.push({
        record_id: crypto.randomUUID(),
        product: productUuid,
        store: targetStoreUuid,
        qty: stock.qty,
        status: stock.stock_status ?? 1,
      });
    }

    let stocksSynced = 0;
    for (const batch of chunk(stockRows, BATCH_SIZE)) {
      if (batch.length === 0) continue;
      const placeholders = batch
        .map(() => "(?, ?, ?, ?, ?, NOW(), NOW(), NOW())")
        .join(",");
      const values = batch.flatMap((r) => [
        r.record_id,
        r.product,
        r.store,
        r.qty,
        r.status,
      ]);
      await targetDb.execute(
        `
        INSERT INTO stocks_on_hand (
          record_id, product, store, qty, status, last_updated, created_at, updated_at
        ) VALUES ${placeholders}
        ON DUPLICATE KEY UPDATE 
          qty = VALUES(qty),
          status = VALUES(status),
          last_updated = NOW(),
          updated_at = NOW()
      `,
        values,
      );
      stocksSynced += batch.length;
    }
    console.log(
      `[Migration] Stocks sync completed successfully. (${stocksSynced} records upserted in batches)`,
    );

    // ----------------------------------------------------
    // COMMIT or ROLLBACK
    // ----------------------------------------------------
    if (isDryRun) {
      console.log("--------------------------------------------------");
      console.log("🔄 Rolling back transaction (--dry-run detected)...");
      await targetDb.rollback();
      console.log(
        "✨ DRY RUN FINISHED: All changes successfully rolled back. Database is clean.",
      );
      console.log("--------------------------------------------------");
      return; // skip postflight for dry-run
    }

    console.log("💾 Committing transaction to database...");
    await targetDb.commit();
    console.log("✨ LIVE MIGRATION COMMITTED SUCCESSFULLY.");

    // ─── NEW: postflight — verify log populated, then optional parity ───
    await verifySyncLogPopulated(targetDb, migrationStartTime);
    await waitAndVerifyParity();
  } catch (error) {
    console.error("\n[Error] Migration failed. Details below:");
    console.error(error);
    try {
      await targetDb.rollback();
      console.error("[Migration] Transaction successfully rolled back.");
    } catch (rollbackError) {
      console.error("[Error] Rollback also failed:", rollbackError);
    }
    throw error;
  } finally {
    await legacyDb.end();
    await targetDb.end();
  }
}

runMigration().catch((error) => {
  console.error("\n[FATAL ERROR TRACE]:", error);
  process.exit(1);
});
