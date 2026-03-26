/**
 * tools/queryRacingData.js
 *
 * General-purpose SQL query tool.
 * Intended for flexible, read-only exploration of the racing database.
 *
 * ⚠️  WARNING: This tool executes arbitrary SQL.  If you expose this server
 *     to untrusted clients, either remove this tool or add allowlist/denylist
 *     logic here (e.g. reject any query that starts with INSERT/UPDATE/DELETE).
 */

import { z } from 'zod';

// Input schema
const QueryRacingDataSchema = z.object({
  sql_query: z
    .string()
    .min(1, 'sql_query must not be empty')
    .describe(
      'A valid MySQL SELECT query to run against the racing statistics database. ' +
      'Example: "SELECT * FROM drivers LIMIT 10"'
    ),
});

/**
 * Registers the query_racing_data tool on the MCP server.
 *
 * @param {import('@modelcontextprotocol/sdk/server/mcp.js').McpServer} server
 * @param {import('mysql2/promise').Pool} pool
 */
export function registerQueryRacingData(server, pool) {
  server.tool(
    'query_racing_data',
    `Execute a custom SQL SELECT query against the racing database (racestat_0010).

MANDATORY PROTOCOL: YOU MUST CALL THIS TOOL FOR ANY DATA QUERY. DO NOT SHOW SQL OR JSON.
- TODAY IS MARCH 25, 2026. "This year" = 2026.
- MANDATORY EXECUTION: If you identify a SQL query, you MUST run it using this tool. 
- ERROR PREVENTION: COLUMNS 'scheduled' AND 'completed' DO NOT EXIST. USE 'date' COMPARISONS.
- ACT AS A DATA ANALYST: Provide the result directly.
 
 SCHEMAS:
 - Drivers: IDdriver, DriverName, FirstName, LastName, Hometown, dob, Active
 - results: IDresult, IDdriver, IDrace, IDseason, IDtrack, FinishPos, StartPos
 - races: IDrace, IDseries, IDseason, IDtrack, EventName, date, Laps, Time (total time in seconds), FastLap (seconds), FastLapDriver (ID), PoleTime (seconds), PoleDriver (ID), MoV (Margin of Victory), FieldSize, SB (0=Big Block, 1=Small Block), NT (0=Standard, 1=RWYB, 2=Invitational, 3=Non-Winners, 4=Mixed), IDspecial
 - tracks: IDtrack, TrackName, Location, surface, length (miles), IDstate (1=NY, 2=PA, 3=QC, 7=DE, 9=FL, 11=ON, 12=NJ, 1->50=US/CAN)
 - series: IDseries, seriesname
 - Points: Pos, IDdriver, IDseason, IDseries, IDtrack, Points
 
 MANDATORY QUERY LOGIC:
 - "How many RACES were held/completed?" → COUNT(DISTINCT ra.IDrace) FROM races ra JOIN results r ON ra.IDrace = r.IDrace WHERE ra.date < CURDATE()
 - "How many RACES are scheduled/remaining?" → COUNT(DISTINCT IDrace) FROM races WHERE date >= CURDATE()
 - "WINS" → SUM(r.FinishPos = 1)
 - "POLE WINS" → COUNT(*) FROM races WHERE PoleDriver = d.IDdriver
 - "FAST LAPS" → COUNT(*) FROM races WHERE FastLapDriver = d.IDdriver
 - "POSITIONS GAINED (HARD CHARGER)" → (r.StartPos - r.FinishPos)
 - "AVG FINISH" → AVG(r.FinishPos) WHERE r.FinishPos > 0
 - "AVERAGE FIELD SIZE" → AVG(ra.FieldSize)
 - "FASTEST LAP OVERALL" → MIN(ra.FastLap) WHERE ra.FastLap > 0
 - "FASTEST AVERAGE LAP (WINNER)" → (ra.Time / ra.Laps) WHERE ra.Laps > 0
 
 CRITICAL RULES:
 - Always use table.column dot notation in SELECT and JOINs.
 - Join Drivers d ON r.IDdriver = d.IDdriver
 - Join tracks t ON r.IDtrack = t.IDtrack OR ra.IDtrack = t.IDtrack
 - Join series s ON r.IDseries = s.IDseries OR ra.IDseries = s.IDseries
 - Join races ra ON r.IDrace = ra.IDrace
 
 EXAMPLE QUERIES:
 -- Who has the most Pole Wins at Afton?
 SELECT d.DriverName, COUNT(ra.IDrace) as poles
 FROM races ra
 JOIN Drivers d ON ra.PoleDriver = d.IDdriver
 JOIN tracks t ON ra.IDtrack = t.IDtrack
 WHERE t.TrackName LIKE '%Afton%'
 GROUP BY d.IDdriver ORDER BY poles DESC LIMIT 10
 
 -- Track Record (Fastest Lap ever at a track):
 SELECT ra.date, d.DriverName, ra.FastLap, ra.EventName
 FROM races ra
 JOIN Drivers d ON ra.FastLapDriver = d.IDdriver
 JOIN tracks t ON ra.IDtrack = t.IDtrack
 WHERE t.TrackName LIKE '%Fonda%' AND ra.FastLap > 0
 ORDER BY ra.FastLap ASC LIMIT 1
 
 -- Best Hard Charger (Most positions gained in one race):
 SELECT ra.date, d.DriverName, t.TrackName, (r.StartPos - r.FinishPos) as gained
 FROM results r
 JOIN Drivers d ON r.IDdriver = d.IDdriver
 JOIN races ra ON r.IDrace = ra.IDrace
 JOIN tracks t ON r.IDtrack = t.IDtrack
 WHERE r.StartPos > 0 AND r.FinishPos > 0
 ORDER BY gained DESC LIMIT 10
 
 -- Average Field Size by Series in 2025:
 SELECT s.seriesname, AVG(ra.FieldSize) as avg_field
 FROM races ra
 JOIN series s ON ra.IDseries = s.IDseries
 WHERE ra.IDseason = 2025
 GROUP BY s.IDseries ORDER BY avg_field DESC
 
 -- Driver efficiency (Wins per Start percentage):
 SELECT d.DriverName,
        COUNT(r.IDresult) as starts,
        SUM(r.FinishPos = 1) as wins,
        (SUM(r.FinishPos = 1) / COUNT(r.IDresult)) * 100 as win_percentage
 FROM results r
 JOIN Drivers d ON r.IDdriver = d.IDdriver
 GROUP BY d.IDdriver HAVING starts >= 20
 ORDER BY win_percentage DESC LIMIT 10
 
 -- Upcoming races for a series:
 SELECT ra.date, t.TrackName, ra.EventName
 FROM races ra
 JOIN tracks t ON ra.IDtrack = t.IDtrack
 JOIN series s ON ra.IDseries = s.IDseries
 WHERE s.seriesname LIKE '%Super DIRTcar%' AND ra.date >= CURDATE()
 ORDER BY ra.date LIMIT 10
 
 -- Number of races completed this year:
 SELECT COUNT(DISTINCT ra.IDrace) as CompletedCount
 FROM races ra
 JOIN results r ON ra.IDrace = r.IDrace
 WHERE ra.IDseason = 2026 AND ra.date < CURDATE()
 
 Returns up to 500 rows.`,
    QueryRacingDataSchema.shape,
    async ({ sql_query }) => {
      // ── Basic safety guard ──────────────────────────────────────────────
      // Reject obvious write operations so the LLM can't mutate data.
      const forbidden = /^\s*(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE)\b/i;
      if (forbidden.test(sql_query)) {
        return {
          content: [{
            type: 'text',
            text: 'Error: Only SELECT queries are permitted with this tool. ' +
                  'Mutations (INSERT, UPDATE, DELETE, etc.) are not allowed.',
          }],
          isError: true,
        };
      }

      // ── Pre-flight: catch incomplete WHERE clause (e.g. WHERE col = LIMIT) ──
      // This happens when the LLM forgets to supply the filter value.
      const incompleteWhere = /WHERE\s+\S+\s*=\s*(LIMIT|ORDER|GROUP|HAVING|UNION|$)/i;
      if (incompleteWhere.test(sql_query)) {
        return {
          content: [{
            type: 'text',
            text: 'SQL ERROR - PLEASE FIX AND RETRY: The WHERE clause is incomplete — a value is missing after the = operator. ' +
                  'Example fix: WHERE DriverName = \'Matt Sheppard\' or WHERE DriverName LIKE \'%Sheppard%\'. ' +
                  'Please retry with a complete query.',
          }],
          // Omit `isError: true` so n8n doesn't abort the agent run.
        };
      }

      // ── Row limit safety ───────────────────────────────────────────────
      // Append LIMIT if the query doesn't already have one, to prevent OOM.
      const hasLimit = /\bLIMIT\b/i.test(sql_query);
      const safeQuery = hasLimit ? sql_query : `${sql_query.trimEnd()} LIMIT 500`;

      // ── Execute ────────────────────────────────────────────────────────
      let conn;
      try {
        conn = await pool.getConnection();
        const [rows] = await conn.query(safeQuery);

        if (!Array.isArray(rows) || rows.length === 0) {
          return {
            content: [{ type: 'text', text: 'Query executed successfully. No rows returned.' }],
          };
        }

        return {
          content: [{
            type: 'text',
            text: JSON.stringify(rows, null, 2),
          }],
        };
      } catch (err) {
        // Return a structured error so the LLM can react intelligently.
        return {
          content: [{
            type: 'text',
            text: `SQL ERROR - PLEASE FIX AND RETRY: ${err.message}\n\nSQL attempted:\n${safeQuery}\n\nRead the error message, correct your SQL syntax, and call this tool again.`,
          }],
          // Omit `isError: true` so n8n doesn't abort the agent run.
        };
      } finally {
        conn?.release();
      }
    }
  );
}
