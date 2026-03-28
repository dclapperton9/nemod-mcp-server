/**
 * tools/getDriverInfo.js
 *
 * Returns detailed stats for a specific driver by name.
 * Uses a prepared statement (parameterised query) to prevent SQL injection.
 *
 * Customise the SELECT below to match your actual schema.
 * ─────────────────────────────────────────────────────────────────────────
 * The query uses a LIKE search so partial names work (e.g. "Johnson" matches
 * "Jimmie Johnson").  Adjust the columns to match your `drivers` (or
 * equivalent) table.
 */

import { z } from 'zod';

// Input schema
const GetDriverInfoSchema = z.object({
  driver_name: z
    .string()
    .min(1, 'driver_name must not be empty')
    .describe(
      'Full or partial name of the driver to look up. ' +
      'Example: "Kyle Larson" or just "Larson".'
    ),
});

/**
 * Registers the get_driver_info tool on the MCP server.
 *
 * @param {import('@modelcontextprotocol/sdk/server/mcp.js').McpServer} server
 * @param {import('mysql2/promise').Pool} pool
 */
export function registerGetDriverInfo(server, pool) {
  server.tool(
    'get_driver_info',
    'Look up a dirt track racing driver by name and return their statistics ' +
    '(wins, starts, podiums, points, etc.) from the nemodfacts database. ' +
    'Partial name matches are supported.',
    GetDriverInfoSchema.shape,
    async ({ driver_name }) => {
      // ── Prepared statement — safe from SQL injection ───────────────────
      //
      // TODO: Update this query to match your exact table and column names.
      //
      // Common patterns for racing databases:
      //   - drivers table: driver_id, first_name, last_name, car_number, team
      //   - results / race_results table: wins, starts, top5s, top10s, dnf, points
      //
      // Example multi-table query:
      //
      //   SELECT
      //     d.driver_id,
      //     CONCAT(d.first_name, ' ', d.last_name) AS driver_name,
      //     d.car_number,
      //     d.team,
      //     COUNT(r.race_id)                        AS total_starts,
      //     SUM(r.finish_position = 1)              AS wins,
      //     SUM(r.finish_position <= 5)             AS top5s,
      //     SUM(r.finish_position <= 10)            AS top10s,
      //     SUM(r.points_earned)                    AS total_points
      //   FROM drivers d
      //   LEFT JOIN race_results r ON d.driver_id = r.driver_id
      //   WHERE CONCAT(d.first_name, ' ', d.last_name) LIKE ?
      //   GROUP BY d.driver_id
      //   ORDER BY wins DESC
      //   LIMIT 10
      //
      // The ? placeholder is filled by mysql2 using a prepared statement,
      // so user-supplied strings can NEVER break out of the value context.

      const searchParam = `%${driver_name}%`;

      // Real schema query against racestat_0010
      const sql = `
        SELECT
          d.IDdriver,
          d.DriverName,
          d.FirstName,
          d.LastName,
          d.NickName,
          d.CarNumber,
          d.Hometown,
          d.dob,
          d.Active,
          COUNT(r.IDresult)                          AS total_starts,
          SUM(r.FinishPos = 1)                       AS wins,
          SUM(r.FinishPos > 0 AND r.FinishPos <= 5)  AS top5s,
          SUM(r.FinishPos > 0 AND r.FinishPos <= 10) AS top10s,
          MIN(r.IDseason)                            AS first_season,
          MAX(r.IDseason)                            AS last_season
        FROM Drivers d
        LEFT JOIN results r ON d.IDdriver = r.IDdriver
        WHERE d.DriverName LIKE ?
           OR CONCAT(d.FirstName, ' ', d.LastName) LIKE ?
        GROUP BY d.IDdriver
        ORDER BY wins DESC
        LIMIT 10
      `;

      let conn;
      try {
        conn = await pool.getConnection();
        // Second argument is the parameters array → prepared statement
        const [rows] = await conn.execute(sql, [searchParam, searchParam]);

        if (!Array.isArray(rows) || rows.length === 0) {
          return {
            content: [{
              type: 'text',
              text: `No drivers found matching "${driver_name}". ` +
                    'Try a different partial name or check the spelling.',
            }],
          };
        }

        // Format nicely for the LLM
        const summary = rows.map((row, i) => {
          const fields = Object.entries(row)
            .map(([k, v]) => `  ${k}: ${v ?? 'N/A'}`)
            .join('\n');
          return `Driver #${i + 1}:\n${fields}`;
        }).join('\n\n');

        return {
          content: [{
            type: 'text',
            text: `Found ${rows.length} driver(s) matching "${driver_name}":\n\n${summary}`,
          }],
        };
      } catch (err) {
        return {
          content: [{
            type: 'text',
            text: `Database error while looking up driver "${driver_name}": ${err.message}`,
          }],
          isError: true,
        };
      } finally {
        conn?.release();
      }
    }
  );
}
