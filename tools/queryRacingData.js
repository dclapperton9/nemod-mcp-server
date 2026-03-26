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
- ANTI-REFUSAL: NEVER say "I can't provide real-time data" or "I don't have access to the database." You DO have access via this tool.
- NO TEMPLATES: Never provide SQL templates (e.g., "[IDdriver]") to the user. Always find the IDs herself by querying.
- NO EXPLANATIONS: Do not explain HOW to run the query. RUN IT and provide the answer.
- ACT AS A DATA ANALYST: Provide the result directly.
 
 SCHEMAS:
 - Drivers: IDdriver, DriverName, FirstName, LastName, Hometown, dob, Active
 - results: IDresult, IDdriver, IDrace, IDseason, IDtrack, FinishPos, StartPos
 - races: IDrace, IDseries, IDseason, IDtrack, EventName, date, Laps, Time (total time in seconds), FastLap (Fastest Race Lap seconds), FastLapDriver (ID), PoleTime (Fastest Qualifying Time seconds), PoleDriver (Fastest Qualifying Driver ID), MoV (Margin of Victory), FieldSize, SB (0=Big Block, 1=Small Block), NT (0=Standard, 1=RWYB, 2=Invitational, 3=Non-Winners, 4=Mixed), IDspecial
 - tracks: IDtrack, TrackName, Location, surface, length (miles), IDstate (1=NY, 2=PA, 3=QC, 7=DE, 9=FL, 11=ON, 12=NJ, 1->50=US/CAN)
 - series: IDseries, seriesname
 - Points: Pos, IDdriver, IDseason, IDseries, IDtrack, Points
 - LapsLed: IDLapsLed, IDrace, IDdriver, Start, End, Total
 
 MANDATORY QUERY LOGIC:
 - "How many RACES were held/completed?" → COUNT(DISTINCT ra.IDrace) FROM races ra JOIN results r ON ra.IDrace = r.IDrace WHERE ra.date < CURDATE()
 - "How many RACES are scheduled/remaining?" → COUNT(DISTINCT IDrace) FROM races WHERE date >= CURDATE()
 - "WINS" → SUM(r.FinishPos = 1)
 - "FASTEST QUALIFYING LAP WINS" → COUNT(*) FROM races WHERE PoleDriver = d.IDdriver
 - "FASTEST RACE LAPS" → COUNT(*) FROM races WHERE FastLapDriver = d.IDdriver
 - "POSITIONS GAINED (HARD CHARGER)" → (r.StartPos - r.FinishPos)
 - "AVG FINISH" → AVG(r.FinishPos) WHERE r.FinishPos > 0
 - "AVERAGE FIELD SIZE" → AVG(ra.FieldSize)
 - "FASTEST RACE LAP OVERALL" → MIN(ra.FastLap) WHERE ra.FastLap > 0
 - "FASTEST AVERAGE LAP (WINNER)" → (ra.Time / ra.Laps) WHERE ra.Laps > 0
 - "LAPS LED" → SUM(ll.Total) FROM LapsLed ll
 
 STREAK CALCULATIONS (Gaps & Islands):
  - LONGEST STREAK: Use a CTE with \`ROW_NUMBER() - ROW_NUMBER(PARTITION BY is_hit)\`.
  - CURRENT STREAK: Use \`SUM(CASE WHEN NOT is_hit THEN 1 ELSE 0 END) OVER (ORDER BY date DESC)\` and count where this sum is 0.
 
 CRITICAL RULES:
 - Always use table.column dot notation in SELECT and JOINs.
 - Join Drivers d ON r.IDdriver = d.IDdriver
 - Join tracks t ON r.IDtrack = t.IDtrack OR ra.IDtrack = t.IDtrack
 - Join series s ON r.IDseries = s.IDseries OR ra.IDseries = s.IDseries
 - Join races ra ON r.IDrace = ra.IDrace
 - Join LapsLed ll ON r.IDrace = ll.IDrace AND r.IDdriver = ll.IDdriver
 
 EXAMPLE QUERIES:
 -- Longest Winning Streak ever (Global):
 WITH HitGroups AS (
   SELECT IDdriver, date, (FinishPos = 1) as is_hit,
          ROW_NUMBER() OVER (PARTITION BY IDdriver ORDER BY date, EventName) - 
          ROW_NUMBER() OVER (PARTITION BY IDdriver, (FinishPos = 1) ORDER BY date, EventName) as grp
   FROM results r JOIN races ra ON r.IDrace = ra.IDrace
 )
 SELECT d.DriverName, COUNT(*) as streak_length, MIN(date) as start_date, MAX(date) as end_date
 FROM HitGroups h JOIN Drivers d ON h.IDdriver = d.IDdriver
 WHERE is_hit = 1 GROUP BY h.IDdriver, h.grp ORDER BY streak_length DESC LIMIT 10
 
 -- Stewart Friesen's Current Top 10 Streak:
 SELECT COUNT(*) as current_streak
 FROM (
     SELECT r.FinishPos, 
            SUM(CASE WHEN r.FinishPos > 10 OR r.FinishPos = 0 THEN 1 ELSE 0 END) 
            OVER (ORDER BY ra.date DESC, ra.EventName DESC) as broken
     FROM results r
     JOIN races ra ON r.IDrace = ra.IDrace
     JOIN Drivers d ON r.IDdriver = d.IDdriver
     WHERE d.DriverName LIKE '%Friesen%'
 ) t
 WHERE broken = 0
 
 -- Who has the most Fastest Qualifying Lap wins at Afton?
 SELECT d.DriverName, COUNT(ra.IDrace) as poles
 FROM races ra
 JOIN Drivers d ON ra.PoleDriver = d.IDdriver
 JOIN tracks t ON ra.IDtrack = t.IDtrack
 WHERE t.TrackName LIKE '%Afton%'
 GROUP BY d.IDdriver ORDER BY poles DESC LIMIT 10
 
 -- Track Record (Fastest Race Lap ever at a track):
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
 
 -- Driver efficiency (Wins per Start percentage):
 SELECT d.DriverName,
        COUNT(r.IDresult) as starts,
        SUM(r.FinishPos = 1) as wins,
        (SUM(r.FinishPos = 1) / COUNT(r.IDresult)) * 100 as win_percentage
 FROM results r
 JOIN Drivers d ON r.IDdriver = d.IDdriver
 GROUP BY d.IDdriver HAVING starts >= 20
 ORDER BY win_percentage DESC LIMIT 10
 
 -- Who has the longest winning streak at Fonda, and what was their fastest race lap during that streak?
WITH FondaRaces AS (
  SELECT r.IDdriver, ra.date, ra.EventName, ra.FastLap, (r.FinishPos = 1) as is_hit,
         ROW_NUMBER() OVER (PARTITION BY r.IDdriver ORDER BY ra.date, ra.EventName) - 
         ROW_NUMBER() OVER (PARTITION BY r.IDdriver, (r.FinishPos = 1) ORDER BY ra.date, ra.EventName) as grp
  FROM results r JOIN races ra ON r.IDrace = ra.IDrace JOIN tracks t ON ra.IDtrack = t.IDtrack
  WHERE t.TrackName LIKE '%Fonda%'
),
Streaks AS (
  SELECT IDdriver, COUNT(*) as streak_len, MIN(date) as start_date, MAX(date) as end_date, MIN(NULLIF(FastLap, 0)) as best_lap
  FROM FondaRaces WHERE is_hit = 1 GROUP BY IDdriver, grp
)
SELECT d.DriverName, s.streak_len, s.start_date, s.end_date, s.best_lap
FROM Streaks s JOIN Drivers d ON s.IDdriver = d.IDdriver
ORDER BY streak_len DESC LIMIT 1

-- Number of races completed this year:
 SELECT COUNT(DISTINCT ra.IDrace) as CompletedCount
 FROM races ra
 JOIN results r ON ra.IDrace = r.IDrace
 WHERE ra.IDseason = 2026 AND ra.date < CURDATE()
 
 -- Who led the most laps in the 2025 season?
 SELECT d.DriverName, SUM(ll.Total) as total_laps_led
 FROM LapsLed ll
 JOIN Drivers d ON ll.IDdriver = d.IDdriver
 JOIN races ra ON ll.IDrace = ra.IDrace
 WHERE ra.IDseason = 2025
 GROUP BY ll.IDdriver ORDER BY total_laps_led DESC LIMIT 10
 
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
