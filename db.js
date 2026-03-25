/**
 * db.js — MySQL connection pool singleton
 * Uses mysql2/promise for async/await support.
 * All environment variables are loaded by index.js via dotenv.
 */

import mysql from 'mysql2/promise';

/**
 * Creates and returns a mysql2 connection pool.
 * Called once at startup from index.js.
 *
 * @returns {mysql.Pool}
 */
export function createPool() {
  return mysql.createPool({
    host:               process.env.DB_HOST,
    port:               parseInt(process.env.DB_PORT ?? '3306', 10),
    user:               process.env.DB_USER,
    password:           process.env.DB_PASS,
    database:           process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit:    10,
    queueLimit:         0,
    // Return JS Date objects for DATETIME/DATE columns
    dateStrings:        false,
    // Automatically reconnect dropped connections
    enableKeepAlive:    true,
    keepAliveInitialDelay: 10000,
  });
}
