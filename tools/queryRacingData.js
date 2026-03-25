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
    `Execute a custom SQL SELECT query against the nemodfacts racing statistics database (racestat_0010).

TOOL MANDATE: This tool (query_racing_data) is the ONLY way to get racing data, INCLUDING results, rankings, schedules, and upcoming events. There is NO other tool for schedules. ALL racing questions must call this tool with a SQL query.

STRICT RESPONSE PROTOCOL:
- FINAL OUTPUT REQUIREMENT: You MUST call this tool and return the data. 
- PROTOCOL VIOLATION: Providing ONLY text, SQL queries, or JSON snippets without calling this tool is a VIOLATION.
- ANSWER ONLY with the requested data formatted clearly.
- NEVER explain the SQL query or your internal reasoning in the final response.
- NEVER provide SQL snippets or mention "sql_query" in the final output unless specifically asked for the query code.
- NO NARRATION: Do not say "The tool returned..." or "I should report this...".
- If data is empty, say "No information found in the database."

KEY TABLE SCHEMAS:
- Drivers: IDdriver, DriverName, FirstName, LastName, NickName, CarNumber, Hometown, dob (DATE, NOT 'DOB'), Active
- results: IDresult, IDdriver, IDrace, IDseries, IDseason (INTEGER year e.g. 2024 — NOT a date, do NOT use YEAR() on it), IDtrack, FinishPos, StartPos, CarNum
- races: IDrace, IDseries, IDseason, IDtrack, EventName, date, FieldSize, Laps, MoV
- tracks: IDtrack, TrackName, Location, IDstate, surface, length, type
- series: IDseries, seriesname
- Points: IDpoints, IDdriver, IDseries, IDtrack, Pos, Points, IDseason, SB
- DriverStats: IDdriver, ELO, CELO

STATE ID MAPPING (Resolve IDstate to Name):
1: NY, 2: PA, 3: QC, 4: ON, 5: NJ, 6: OH, 7: DE, 8: NC, 9: FL, 10: VT, 11: GA, 12: MD, 13: MI, 14: WV, 15: VA, 16: NH, 17: LA, 18: SC, 19: TX, 20: TN, 21: AR, 22: CT

STRICT GROUNDING MANDATE: 
1. ONLY report data that is explicitly returned in the JSON output from this tool. 
2. If the tool returns NO data, say "No information found in the database."
3. NEVER use your internal training knowledge to "fill in" names of tracks, series, or dates that are not in the database results.
4. If a session is missing a TrackName or seriesname, say "Unknown" rather than making one up.

COUNTING RULES (critical):
- "How many RACES were held?" → COUNT(DISTINCT ra.IDrace) from the races table  ← NOT COUNT(r.IDresult)
- "How many DRIVERS entered?" → COUNT(DISTINCT r.IDdriver) from results
- "How many RESULTS/ENTRIES?" → COUNT(r.IDresult)
- Never use COUNT(r.IDresult) to answer a question about number of race events.

CRITICAL RULES:
- Driver name = DriverName (NOT driver_name, full_name, or name)
- TrackName is ONLY in the tracks table — always JOIN tracks ON results.IDtrack = tracks.IDtrack
- Always use table.column dot notation inside COUNT(), e.g. COUNT(r.IDresult)
- WINS = SUM(r.FinishPos = 1)  ← use SUM not COUNT
- STARTS = COUNT(r.IDresult)   ← total races entered
- TOP 5s = SUM(r.FinishPos <= 5)
- TOP 10s = SUM(r.FinishPos <= 10)
- NEVER use COUNT() for wins — COUNT counts rows regardless of finish position
- If you reference d.DriverName, you MUST JOIN drivers d ON r.IDdriver = d.IDdriver
- If you reference t.TrackName, you MUST JOIN tracks t ON r.IDtrack = t.IDtrack
- If you reference s.seriesname, you MUST JOIN series s ON r.IDseries = s.IDseries
- If you reference ra.date, you MUST JOIN races ra ON r.IDrace = ra.IDrace

-- Best drivers at a specific track (by WINS):
SELECT d.DriverName,
       COUNT(r.IDresult)    AS starts,
       SUM(r.FinishPos = 1) AS wins
FROM results r
JOIN Drivers d ON r.IDdriver = d.IDdriver
JOIN tracks  t ON r.IDtrack  = t.IDtrack
WHERE t.TrackName LIKE '%Afton%'
GROUP BY d.IDdriver ORDER BY wins DESC LIMIT 10

-- Best drivers at a specific track (by TOP 10s):
SELECT d.DriverName,
       COUNT(r.IDresult)       AS starts,
       SUM(r.FinishPos <= 10)  AS top10s,
       SUM(r.FinishPos <= 5)   AS top5s,
       SUM(r.FinishPos = 1)    AS wins
FROM results r
JOIN Drivers d ON r.IDdriver = d.IDdriver
JOIN tracks  t ON r.IDtrack  = t.IDtrack
WHERE t.TrackName LIKE '%Afton%'
GROUP BY d.IDdriver ORDER BY top10s DESC LIMIT 10

IMPORTANT: ORDER BY must match what the user asked for — wins→wins, top5s→top5s, top10s→top10s, most starts→starts.

-- !! RANKING RULE: When asked "where does X rank", DO NOT add WHERE d.DriverName = X !!
-- Get ALL drivers sorted, then the AI finds the driver in the list.
-- Use RANK() window function to find exact position:

-- Rank of a specific driver at a track (e.g. wins ranking):
SELECT DriverName, wins, starts, rank_pos FROM (
  SELECT d.DriverName,
         COUNT(r.IDresult)    AS starts,
         SUM(r.FinishPos = 1) AS wins,
         RANK() OVER (ORDER BY SUM(r.FinishPos = 1) DESC) AS rank_pos
  FROM results r
  JOIN Drivers d ON r.IDdriver = d.IDdriver
  JOIN tracks  t ON r.IDtrack  = t.IDtrack
  WHERE t.TrackName LIKE '%Outlaw Speedway%'
  GROUP BY d.IDdriver
) ranked
WHERE DriverName LIKE '%Paine%'

EXAMPLE QUERIES:
-- Driver career stats (wins vs starts):
SELECT d.DriverName,
       COUNT(r.IDresult)      AS starts,
       SUM(r.FinishPos = 1)   AS wins,
       SUM(r.FinishPos <= 5)  AS top5s,
       SUM(r.FinishPos <= 10) AS top10s
FROM Drivers d LEFT JOIN results r ON d.IDdriver = r.IDdriver
WHERE d.DriverName LIKE '%Sheppard%' GROUP BY d.IDdriver

-- Driver age (use TIMESTAMPDIFF, NOT YEAR(dob)):
SELECT d.DriverName, d.dob,
       TIMESTAMPDIFF(YEAR, d.dob, CURDATE()) AS current_age
FROM Drivers d
WHERE d.DriverName LIKE '%Clapperton%'

-- Season filter (IDseason is an INT, use = directly):
-- CORRECT:   WHERE r.IDseason = 2024
-- WRONG:     WHERE YEAR(r.IDseason) = 2024  ← IDseason is already a year integer

-- Points Standings / Championship Winner (Ranked by Pos or Points):
SELECT p.Pos, d.DriverName, p.Points, s.seriesname, t.TrackName
FROM Points p
JOIN Drivers d ON p.IDdriver = d.IDdriver
LEFT JOIN series s ON p.IDseries = s.IDseries
LEFT JOIN tracks t ON p.IDtrack = t.IDtrack
WHERE p.IDseason = 2023 AND (s.seriesname LIKE '%Super DIRTcar%' OR t.TrackName LIKE '%Fonda%')
ORDER BY p.Pos ASC, p.Points DESC LIMIT 10

-- What series has a driver won the most in (GROUP BY series):
SELECT s.seriesname,
       COUNT(r.IDresult)    AS starts,
       SUM(r.FinishPos = 1) AS wins
FROM results r
JOIN Drivers d ON r.IDdriver = d.IDdriver
JOIN series  s ON r.IDseries = s.IDseries
WHERE d.DriverName = 'Brett Hearn'
GROUP BY s.IDseries ORDER BY wins DESC LIMIT 10

-- Which driver has most wins at any single track (global, no WHERE filter):
SELECT d.DriverName, t.TrackName,
       SUM(r.FinishPos = 1) AS wins
FROM results r
JOIN Drivers d ON r.IDdriver = d.IDdriver
JOIN tracks  t ON r.IDtrack  = t.IDtrack
GROUP BY d.IDdriver, t.IDtrack
ORDER BY wins DESC LIMIT 10

-- Upcoming races in a series (scheduled events):
SELECT ra.date, ra.EventName, t.TrackName
FROM races ra
JOIN tracks t ON ra.IDtrack = t.IDtrack
JOIN series s ON ra.IDseries = s.IDseries
WHERE s.seriesname LIKE '%Super DIRTcar%' AND ra.date >= CURDATE()
ORDER BY ra.date ASC LIMIT 10

-- How many RACE EVENTS were held (use DISTINCT IDrace, NOT COUNT of results rows):
SELECT COUNT(DISTINCT ra.IDrace) AS race_count
FROM races ra
JOIN tracks t ON ra.IDtrack = t.IDtrack
WHERE t.TrackName LIKE '%Outlaw%' AND ra.IDseason = 2024

-- Top drivers in a season range (e.g. 2020-2024):
WHERE r.IDseason BETWEEN 2020 AND 2024
GROUP BY d.IDdriver ORDER BY wins DESC LIMIT 10

-- Compare wins at multiple tracks for one driver:
SELECT t.TrackName,
       SUM(r.FinishPos = 1) AS wins
FROM results r
JOIN Drivers d ON r.IDdriver = d.IDdriver
JOIN tracks  t ON r.IDtrack  = t.IDtrack
WHERE d.IDdriver = (SELECT IDdriver FROM Drivers WHERE DriverName LIKE '%Matt Sheppard%' LIMIT 1)
  AND (t.TrackName LIKE '%Afton%' OR t.TrackName LIKE '%Outlaw%')
GROUP BY t.IDtrack ORDER BY wins DESC

-- Driver's best tracks (wins not starts):
SELECT t.TrackName,
       COUNT(r.IDresult)    AS starts,
       SUM(r.FinishPos = 1) AS wins
FROM Drivers d
JOIN results r ON d.IDdriver = r.IDdriver
JOIN tracks t ON r.IDtrack = t.IDtrack
WHERE d.DriverName = 'Matt Sheppard'
GROUP BY t.IDtrack ORDER BY wins DESC LIMIT 10

-- Race results for a season:
SELECT d.DriverName, r.FinishPos, t.TrackName, ra.date
FROM results r
JOIN Drivers d ON r.IDdriver = d.IDdriver
JOIN tracks t ON r.IDtrack = t.IDtrack
JOIN races ra ON r.IDrace = ra.IDrace
WHERE r.IDseason = 2025 ORDER BY ra.date

-- Last win (most recent victory) — date is in the races table, NOT results:
SELECT ra.date, t.TrackName, s.seriesname, r.CarNum,
       r.FinishPos, 'Win' AS result_type
FROM results r
JOIN Drivers d  ON r.IDdriver  = d.IDdriver
JOIN races   ra ON r.IDrace    = ra.IDrace
JOIN tracks  t  ON r.IDtrack   = t.IDtrack
JOIN series  s  ON r.IDseries  = s.IDseries
WHERE d.DriverName = 'Dennis Clapperton' AND r.FinishPos = 1
ORDER BY ra.date DESC LIMIT 1

-- First win (oldest/earliest victory) — same as last win but ORDER BY date ASC:
SELECT ra.date, t.TrackName, s.seriesname, r.CarNum,
       r.FinishPos, 'First Win' AS result_type
FROM results r
JOIN Drivers d  ON r.IDdriver  = d.IDdriver
JOIN races   ra ON r.IDrace    = ra.IDrace
JOIN tracks  t  ON r.IDtrack   = t.IDtrack
JOIN series  s  ON r.IDseries  = s.IDseries
WHERE d.DriverName LIKE '%Sheppard%' AND r.FinishPos = 1
ORDER BY ra.date ASC LIMIT 1

-- Recent wins list:
SELECT ra.date, t.TrackName, s.seriesname,
       r.FinishPos, 'Win' AS result_type
FROM results r
JOIN Drivers d  ON r.IDdriver  = d.IDdriver
JOIN races   ra ON r.IDrace    = ra.IDrace
JOIN tracks  t  ON r.IDtrack   = t.IDtrack
JOIN series  s  ON r.IDseries  = s.IDseries
WHERE d.DriverName LIKE '%Clapperton%' AND r.FinishPos = 1
ORDER BY ra.date DESC LIMIT 10

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
