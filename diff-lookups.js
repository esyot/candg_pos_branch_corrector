require("dotenv").config();
const mysql = require("mysql2/promise");

const norm = (n) => (n == null ? null : String(n).trim().toLowerCase());

// Known renames: legacy name -> target name (normalized keys handled by norm()).
// Fill in / adjust after reviewing diff output.
const ALIASES = {
  product_type: {
    "pots / pans": "pot & pans",
    topperware: "toppersware",
    decoration: "decorations",
    "gel pen": "gelpen",
    steel: "steel", // exact already, kept for clarity
    "sunny ware": "sunnyware",
    sunnyware: "sunnyware",
  },
};

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

  const lookups = [
    {
      label: "product_type",
      targetTable: "product_type",
      sql: `SELECT DISTINCT pt.name AS name
            FROM product p
            LEFT JOIN product_type pt ON p.product_type = pt.record_id
            WHERE p.product_code IN (
              SELECT p_sub.product_code
              FROM sales_order so_sub
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
              SELECT p_sub.product_code
              FROM sales_order so_sub
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
              SELECT p_sub.product_code
              FROM sales_order so_sub
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
            WHERE ps.store = ?
              AND ps.product IN (
                SELECT sod_sub.product
                FROM sales_order so_sub
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
            WHERE ps.store = ?
              AND ps.product IN (
                SELECT sod_sub.product
                FROM sales_order so_sub
                JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
                WHERE so_sub.store = ?
              )`,
      params: [store, store],
    },
  ];

  for (const { label, targetTable, sql, params } of lookups) {
    const [legacyRows] = await legacy.execute(sql, params ?? [store]);
    const [targetRows] = await target.execute(
      `SELECT name FROM ${targetTable}`,
    );

    const targetSet = new Set(
      targetRows.map((r) => norm(r.name)).filter(Boolean),
    );

    const insert = [];
    const alias = [];
    const unresolved = [];

    const seen = new Set();
    for (const row of legacyRows) {
      const raw = row.name;
      const key = norm(raw);
      if (key == null || seen.has(key)) continue;
      seen.add(key);

      if (targetSet.has(key)) continue; // exact (normalized) match

      const aliasTo = ALIASES[label]?.[key];
      if (aliasTo && targetSet.has(norm(aliasTo))) {
        alias.push({ from: raw, to: aliasTo });
      } else {
        unresolved.push(raw);
      }
    }

    console.log(`\n=== ${label} (target table: ${targetTable}) ===`);
    console.log(`  legacy distinct: ${seen.size}`);
    console.log(
      `  exact (normalized) matches: ${seen.size - alias.length - unresolved.length}`,
    );
    console.log(`  alias-mapped: ${alias.length}`);
    for (const a of alias)
      console.log(`    ~ ${JSON.stringify(a.from)} -> ${JSON.stringify(a.to)}`);
    console.log(`  missing (need insert or alias): ${unresolved.length}`);
    for (const u of unresolved.sort())
      console.log(`    - ${JSON.stringify(u)}`);

    if (unresolved.length) {
      const valuesSql = unresolved
        .map((u) => `(UUID(), '${u.replace(/'/g, "''")}')`)
        .join(",\n  ");
      console.log(
        `\n  -- INSERT into ${targetTable} (TARGET DB, port ${process.env.DB_TARGET_PORT}):`,
      );
      console.log(
        `  INSERT INTO ${targetTable} (record_id, name) VALUES\n  ${valuesSql};\n`,
      );
    }
  }

  await legacy.end();
  await target.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
