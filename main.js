require("dotenv").config();
const mysql = require("mysql2/promise");
const crypto = require("crypto");

const BATCH_SIZE = 500;

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

    // Normalize lookup keys so trailing spaces / casing drift don't break matches.
    function normalizeName(name) {
      if (name == null) return null;
      return String(name).trim().toLowerCase();
    }

    // Legacy lookup name -> target lookup name, for cases where the concept is
    // the same but the target uses a different word/spacing/plural.
    // Keys are normalized (trim + lowercase) to match normalizeName().
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

    // Every lookup table in this schema has a status column (see SHOW COLUMNS).
    // We only want to link freshly migrated products to ACTIVE rows, otherwise
    // ORDER BY record_id may pick an inactive duplicate as the winner.
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
    // STEP 2: SYNC PRICING SCHEMES FOR THESE ACTIVE PRODUCTS
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
    // STEP 3: SYNC STOCKS ON HAND FOR THESE ACTIVE PRODUCTS
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

    if (isDryRun) {
      console.log("--------------------------------------------------");
      console.log("🔄 Rolling back transaction (--dry-run detected)...");
      await targetDb.rollback();
      console.log(
        "✨ DRY RUN FINISHED: All changes successfully rolled back. Database is clean.",
      );
      console.log("--------------------------------------------------");
    } else {
      console.log("💾 Committing transaction to database...");
      await targetDb.commit();
      console.log("✨ LIVE MIGRATION COMMITTED SUCCESSFULLY.");
    }
  } catch (error) {
    console.error("\n[Error] Migration failed. Details below:");
    console.error(error); // Prints full stack trace and MySQL error details
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
