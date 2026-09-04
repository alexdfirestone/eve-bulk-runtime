import { readFile } from "node:fs/promises";
import { neon } from "@neondatabase/serverless";
import dotenv from "dotenv";

dotenv.config({ path: process.env.MIGRATION_ENV === "production" ? ".env.production" : ".env.local" });
dotenv.config();

const url = process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required to run migrations");

const sql = neon(url);
const migration = await readFile(new URL("../migrations/001_initial.sql", import.meta.url), "utf8");
for (const statement of migration.split(/;\s*(?:\n|$)/).map((value) => value.trim()).filter(Boolean)) {
  await sql.query(statement);
}
console.log("Applied migrations/001_initial.sql");
