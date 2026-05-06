"""
AWS Lambda MCP Server for ATHENA AI - Entegris KSP UC-6 Operator Performance Analysis.

v1.1 — REWRITTEN to use the DWH directly. Initial deploy assumed operator
attribution required walking the OLTP chain (T_OperationHistory → T_ServiceHistory
→ Security.T_User → T_Employee). Live schema discovery (_discover_schema)
revealed every relevant DWH fact already projects EmployeeKey:

  • Bottleneck_MCP.vw_OperatorAtResource  — purpose-built operator-at-resource
       scoreboard with UnitsProcessed / AvgCycleTimeSec / PeerAvgCycleTimeSec /
       PerformanceDeviationPct / Yield / ReworkCount / StdDevCycleSec /
       DowntimePct_PerOperator / BottleneckShiftCount / SkillLevel.
       *** This single view covers ~70% of UC-6 in one query. ***
  • dbo.FactMaterialEventPair      — paired Start/End operations with both
       StartEmployeeKey AND EndEmployeeKey, plus StartUTCDatetime / EndUTCDatetime,
       StartShiftKey / EndShiftKey, ResourceKey, StepKey, ProductKey. Cycle
       time = DATEDIFF(SECOND, StartUTCDatetime, EndUTCDatetime).
  • dbo.FactMaterialQuality        — EmployeeKey + PassedPieces / GoodPieces /
       TotalPieces / OutofProcessFirstPassYieldTotal. FPY directly.
  • dbo.FactResourceServiceTime    — EmployeeKey + NonWorkingTimeInSeconds +
       UpToDownTransition / DownToUpTransition + MainStateModelStateReason.
       Downtime + utilization signal.
  • dbo.FactMaterialStepEvent      — EmployeeKey + IdealCycleTime + actual qty
       / loss / bonus per step.
  • dbo.FactResourceMaterialLossBonus (UC-3 fact) — EmployeeKey + Loss/Bonus
       qty for rework-rate-by-operator.
  • Datasets.V_DimEmployee         — operator master (EmployeeKey, UserId,
       UserName, UserAccount, CreateTimestamp).

Tools (13 functional + 1 discovery):
   1.  get_info
   2.  get_source_info
   3.  list_operators
   4.  get_operator_performance              — vw_OperatorAtResource
   5.  get_operator_learning_curve           — FactMaterialEventPair time series
   6.  get_operator_utilization              — FactResourceServiceTime
   7.  get_operator_quality_ranking          — FactMaterialQuality FPY
   8.  get_operator_shift_comparison         — FactMaterialEventPair × Shift
   9.  get_operator_certification_impact     — vw_OperatorAtResource.SkillLevel
   10. get_operator_downtime_correlation     — FactResourceServiceTime transitions
   11. get_operator_product_matrix           — FactMaterialEventPair × Product
   12. get_operator_experience_impact        — V_DimEmployee.CreateTimestamp
   13. get_operator_versatility              — distinct ResourceKey per Employee
   14. _discover_schema                      — INFORMATION_SCHEMA probe

Statistics philosophy:
   * Server-side (efficient T-SQL): mean, stdev, percentile_cont, regression
     slopes, control limits, classification thresholds.
   * Client-side (LLM): ANOVA / chi-square / t-test verdicts (Lambda hands
     back per-group n / mean / std / contingency tables).

Cross-server linkage: UC-3 still owns the Pareto chart deliverable. UC-6 tools
that need scrap context point the LLM at UC-3's get_scrap_pareto in the same
turn via docstrings.

Safety: READ-ONLY. SQL keyword blocker. Parameterized SQL.
"""

import functools
import json
import logging
import os
import re

import pymssql
from awslabs.mcp_lambda_handler import MCPLambdaHandler

logging.basicConfig(level=logging.INFO, format="[%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)

mcp = MCPLambdaHandler(
    name="ATHENA AI - Entegris KSP UC-6 Operator Performance MCP Server",
    version="1.2.0",
)

DB_SERVER = os.getenv("DB_SERVER")
DB_PORT = int(os.getenv("DB_PORT", "1433"))
DB_INSTANCE = os.getenv("DB_INSTANCE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME_DWH = os.getenv("DB_NAME_DWH")
DB_NAME_OLTP = os.getenv("DB_NAME_OLTP")
DB_NAME_ODS = os.getenv("DB_NAME_ODS")

ROW_LIMIT = int(os.getenv("ROW_LIMIT", "1000"))

_BLOCKED = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|MERGE|GRANT|REVOKE|DENY)\b",
    re.IGNORECASE,
)

_ERROR_MESSAGES = {
    "connection": "The Entegris KSP database is temporarily unreachable. Please try again in a moment.",
    "timeout": "The query took too long to complete. Please narrow the search or try again shortly.",
    "permission": "This operation is not permitted. The system only allows read-only data access.",
    "query": "Unable to retrieve the requested data. Please check the input parameters and try again.",
    "unknown": "An unexpected issue occurred while processing your request. Please try again.",
    "not_found": "No data found matching your request. Please verify the input parameters and try again.",
}


class ToolError(Exception):
    def __init__(self, user_message, internal_message=None):
        self.user_message = user_message
        self.internal_message = internal_message or user_message
        super().__init__(self.user_message)


def safe_tool(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ToolError as e:
            logger.error(f"[{func.__name__}] ToolError: {e.internal_message}")
            return json.dumps({"status": "error", "message": e.user_message})
        except Exception as e:
            logger.error(f"[{func.__name__}] Unexpected {type(e).__name__}: {e}")
            return json.dumps({"status": "error", "message": _ERROR_MESSAGES["unknown"]})

    return wrapper


def _validate_sql(sql: str) -> None:
    if _BLOCKED.search(sql):
        raise ToolError(_ERROR_MESSAGES["permission"], f"Blocked SQL: {sql[:100]}")


def _get_conn(db: str | None = None):
    target_db = db or DB_NAME_DWH
    last_error = None
    attempts: list[tuple[str, dict]] = []

    if DB_INSTANCE:
        attempts.append((
            f"named-instance {DB_SERVER}\\{DB_INSTANCE}",
            {"server": f"{DB_SERVER}\\{DB_INSTANCE}", "database": target_db,
             "user": DB_USERNAME, "password": DB_PASSWORD,
             "login_timeout": 10, "timeout": 60},
        ))

    tried_ports: list[int] = []
    for p in [DB_PORT, 1433, 14330, 1434, 5051, 49152]:
        if p in tried_ports:
            continue
        tried_ports.append(p)
        attempts.append((
            f"{DB_SERVER}:{p}",
            {"server": DB_SERVER, "port": p, "database": target_db,
             "user": DB_USERNAME, "password": DB_PASSWORD,
             "login_timeout": 5, "timeout": 60},
        ))

    for label, kwargs in attempts:
        try:
            conn = pymssql.connect(**kwargs)
            logger.info(f"DB connected via {label} -> {target_db}")
            return conn
        except Exception as e:
            logger.info(f"DB attempt failed [{label}]: {e}")
            last_error = e

    logger.error(f"All DB connection attempts failed. Last error: {last_error}")
    raise ToolError(_ERROR_MESSAGES["connection"], str(last_error))


def _query(sql: str, params: tuple = (), db: str | None = None) -> list[dict]:
    _validate_sql(sql)
    try:
        conn = _get_conn(db or DB_NAME_DWH)
        try:
            cur = conn.cursor(as_dict=True)
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            rows = cur.fetchmany(ROW_LIMIT)
            for row in rows:
                for k, v in list(row.items()):
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                    elif v is not None and not isinstance(v, (str, int, float, bool)):
                        row[k] = str(v)
            return rows
        finally:
            conn.close()
    except ToolError:
        raise
    except Exception as e:
        if "timeout" in str(e).lower():
            raise ToolError(_ERROR_MESSAGES["timeout"], str(e))
        logger.error(f"Query error: {e}")
        raise ToolError(_ERROR_MESSAGES["query"], str(e))


# ════════════════════════════════════════════════════════════════
# v1.2 — Shift name resolver
# ════════════════════════════════════════════════════════════════
# Live discovery (_discover_schema) revealed Datasets.V_DimShift DOES carry
# ShiftDefinitionName + ShiftDefinitionShiftName columns. The UC-3 caveat
# about "no ShiftName" was wrong — that view simply doesn't project them.
# Here we build a runtime cache from V_DimShift so prompts can pass either:
#   shift='Day'  shift='Night'  shift='2'  shift=2
def _resolve_shift(arg) -> str | None:
    """Accept name (e.g. 'Day' / 'Night' / 'Shift A') OR numeric ShiftKey.
    Returns the numeric ShiftKey as a string for SQL parameterization, or None
    if the input cannot be resolved.
    """
    if arg is None or arg == "":
        return None
    s = str(arg).strip()
    if s.isdigit():
        return s
    try:
        rows = _query(
            """
            SELECT TOP 5 ShiftKey, ShiftDefinitionName, ShiftDefinitionShiftName
            FROM Datasets.V_DimShift
            WHERE ShiftDefinitionName       LIKE %s
               OR ShiftDefinitionShiftName  LIKE %s
            """,
            (f"%{s}%", f"%{s}%"),
            db=DB_NAME_DWH,
        )
        if rows:
            return str(rows[0]["ShiftKey"])
    except Exception:
        pass
    return s  # let SQL bind it as-is — caller handles 0-rows gracefully


def _data_window_warning(actual_min, actual_max, requested_days) -> str | None:
    """Emit a structured warning when the requested window is wider than
    available data. Used by learning-curve / quality / experience tools so the
    LLM can surface the gap instead of fabricating a curve."""
    if not actual_min or not actual_max:
        return "No data in the requested window."
    try:
        from datetime import datetime
        dmin = datetime.fromisoformat(str(actual_min).replace("Z", ""))
        dmax = datetime.fromisoformat(str(actual_max).replace("Z", ""))
        actual_days = max(1, (dmax - dmin).days + 1)
        if requested_days and actual_days < requested_days / 2:
            return (
                f"Requested window of {requested_days} days exceeds available data "
                f"({actual_days} days, {actual_min} → {actual_max}). Returning what exists; "
                f"a meaningful curve / threshold needs more historical data."
            )
    except Exception:
        return None
    return None


# ════════════════════════════════════════════════════════════════
# TOOL 1: GET INFO
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_info() -> str:
    """Get comprehensive information about the Entegris KSP UC-6 Operator
    Performance system — what it does, which DWH facts power it, and how it
    pairs with UC-3 (Scrap Pareto) for prompts that span scrap + operator.

    Use this tool FIRST when:
    - The user asks "what can you do?" / "what is this system?"
    - You need to orient yourself before answering an operator question
    """
    return json.dumps({
        "system": "Entegris KSP UC-6 Operator Performance Analysis (v1.1)",
        "description": (
            "Per-operator analytics for Entegris KSP production. v1.1 sources "
            "all metrics directly from DWH facts that already project "
            "EmployeeKey — no OLTP-chain walk required. Backed by the "
            "purpose-built Bottleneck_MCP.vw_OperatorAtResource view plus "
            "FactMaterialEventPair, FactMaterialQuality, FactResourceServiceTime, "
            "FactMaterialStepEvent, and the UC-3 loss fact."
        ),
        "capabilities": [
            "Operator scorecard (vw_OperatorAtResource: cycle, throughput, FPY, downtime%, skill)",
            "Learning curve (FactMaterialEventPair time series, target vs peer baseline)",
            "Utilization (FactResourceServiceTime productive vs window)",
            "Quality ranking (FactMaterialQuality FPY)",
            "Shift comparison (FactMaterialEventPair × ShiftKey)",
            "Certification / SkillLevel impact (vw_OperatorAtResource.SkillLevel)",
            "Downtime correlation (FactResourceServiceTime up/down transitions)",
            "Operator × Product matrix (FactMaterialEventPair × ProductKey)",
            "Experience → rework regression (V_DimEmployee.CreateTimestamp)",
            "T-shaped vs I-shaped versatility (distinct ResourceKey per Employee)",
        ],
        "primary_dwh_objects": [
            "Bottleneck_MCP.vw_OperatorAtResource",
            "dbo.FactMaterialEventPair",
            "dbo.FactMaterialQuality",
            "dbo.FactResourceServiceTime",
            "dbo.FactMaterialStepEvent",
            "dbo.FactResourceMaterialLossBonus (UC-3 fact)",
            "Datasets.V_DimEmployee",
            "Datasets.V_DimResource / V_DimStep / V_DimArea / V_DimFacility / V_DimShift / V_DimProduct",
        ],
        "operator_attribution_chain": (
            "EmployeeKey is projected directly on every relevant DWH fact, so we "
            "join FACT.EmployeeKey → V_DimEmployee.EmployeeKey to surface "
            "UserName / UserAccount. No OLTP T_OperationHistory walk required."
        ),
        "cross_server_pairing": (
            "For prompts spanning operator + scrap, call the UC-3 MCP server's "
            "get_scrap_pareto / get_scrap_events in the same turn and merge "
            "results client-side."
        ),
        "uc3_endpoint": os.getenv("UC3_MCP_ENDPOINT", ""),
        "total_tools": 13,
        "discovery_tool": "_discover_schema (read-only INFORMATION_SCHEMA probe)",
        "write_actions_status": "READ-ONLY deployment.",
        "safety": "READ-ONLY. SQL keyword blocker. Parameterized SQL.",
        "out_of_scope": [
            "ANOVA / chi-square / t-test p-values — Lambda returns inputs (n, mean, "
            "std, contingency tables); the LLM does the final verdict.",
            "Server-side PNG rendering — JSON only; LLM renders Markdown / ASCII.",
        ],
        "accuracy_rules": [
            "Report exact numbers from tool results — never estimate or round",
            "When a metric is missing from the fact, surface the gap rather than guessing",
            "Always cite source: fact / view name, time window, row count",
        ],
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 2: GET SOURCE INFO
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_source_info() -> str:
    """Return metadata about the operator-performance DWH sources: row counts
    on each fact, distinct operator count, and time window coverage."""
    rows = _query(
        """
        SELECT
            DB_NAME() AS database_name,
            @@SERVERNAME AS sql_server_name,
            (SELECT COUNT(*) FROM Datasets.V_DimEmployee)                  AS employee_count,
            (SELECT COUNT(*) FROM dbo.FactMaterialEventPair)               AS event_pair_rows,
            (SELECT COUNT(DISTINCT EndEmployeeKey)
               FROM dbo.FactMaterialEventPair
               WHERE EndEmployeeKey IS NOT NULL)                           AS distinct_end_operators,
            (SELECT MIN(StartUTCDatetime) FROM dbo.FactMaterialEventPair)  AS earliest_event_pair,
            (SELECT MAX(EndUTCDatetime)   FROM dbo.FactMaterialEventPair)  AS latest_event_pair,
            (SELECT COUNT(*) FROM dbo.FactMaterialQuality)                 AS quality_rows,
            (SELECT COUNT(*) FROM dbo.FactResourceServiceTime)             AS service_time_rows,
            (SELECT COUNT(*) FROM dbo.FactMaterialStepEvent)               AS step_event_rows,
            (SELECT COUNT(*) FROM Bottleneck_MCP.vw_OperatorAtResource)    AS operator_at_resource_rows
        """,
        db=DB_NAME_DWH,
    )
    info = rows[0] if rows else {}
    info["data_lineage"] = (
        "Every DWH fact projects EmployeeKey directly. Bottleneck_MCP."
        "vw_OperatorAtResource pre-aggregates the per-operator-per-resource "
        "scorecard. UC-6 joins facts to V_DimEmployee for operator names."
    )
    return json.dumps(info, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 3: LIST OPERATORS
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def list_operators(
    active_since_days: int = 90,
    limit: int = 200,
) -> str:
    """List operators (employees) with at least one event in the last N days.

    Args:
        active_since_days: only include operators with at least 1 op in the last N days (default 90)
        limit: max operators returned (default 200, max 1000)
    """
    limit = min(max(1, int(limit)), 1000)
    days = max(1, int(active_since_days))
    sql = f"""
        SELECT TOP ({limit})
            DE.EmployeeKey, DE.UserId, DE.UserName, DE.UserAccount,
            COUNT(P.MaterialKey) AS event_pair_count,
            MIN(P.StartUTCDatetime) AS first_event,
            MAX(P.EndUTCDatetime)   AS last_event
        FROM Datasets.V_DimEmployee DE
        LEFT JOIN dbo.FactMaterialEventPair P
          ON  P.EndEmployeeKey = DE.EmployeeKey
          AND P.EndUTCDatetime >= DATEADD(DAY, -{days}, GETUTCDATE())
        GROUP BY DE.EmployeeKey, DE.UserId, DE.UserName, DE.UserAccount
        HAVING COUNT(P.MaterialKey) > 0
        ORDER BY event_pair_count DESC
    """
    rows = _query(sql, db=DB_NAME_DWH)
    out = []
    for r in rows:
        out.append({
            "employee_key": str(r.get("EmployeeKey")) if r.get("EmployeeKey") is not None else None,
            "user_id": str(r.get("UserId")) if r.get("UserId") is not None else None,
            "user_name": r.get("UserName"),
            "user_account": r.get("UserAccount"),
            "event_pair_count": r.get("event_pair_count"),
            "first_event": r.get("first_event"),
            "last_event": r.get("last_event"),
        })
    return json.dumps({
        "filter": {"active_since_days": days, "limit": limit},
        "operator_count": len(out),
        "operators": out,
        "data_source": "Datasets.V_DimEmployee LEFT JOIN dbo.FactMaterialEventPair (DWH)",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 4: GET OPERATOR PERFORMANCE  (prompt #11)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_performance(
    resource_name: str | None = None,
    facility: str | None = None,
    area: str | None = None,
    shift: str | None = None,
    top_n: int = 50,
) -> str:
    """OPERATOR PERFORMANCE SCORECARD — pulls the purpose-built
    Bottleneck_MCP.vw_OperatorAtResource view which already aggregates per-
    operator-per-resource UnitsProcessed, AvgCycleTimeSec, PeerAvgCycleTimeSec,
    PerformanceDeviationPct, Yield, ReworkCount, StdDevCycleSec, and
    DowntimePct_PerOperator. Optionally pair with UC-3's get_scrap_pareto for
    Pareto-by-reason context.

    *** ALWAYS USE THIS TOOL when the user asks: ***
      • "compare operators on Resource X"
      • "operator scorecard / performance summary"
      • "who are the top performers on resource Y"

    Args:
        resource_name / facility / area / shift: dim filters
        top_n: keep top-N (default 50, max 500)
    """
    top_n = min(max(1, int(top_n)), 500)
    shift_key = _resolve_shift(shift)

    clauses, params = [], []
    if resource_name:
        clauses.append("V.ResourceName = %s"); params.append(resource_name)
    if shift_key:
        clauses.append("V.ShiftKey = %s"); params.append(shift_key)
    if facility:
        clauses.append("DF.FacilityName = %s"); params.append(facility)
    if area:
        clauses.append("DA.AreaName = %s"); params.append(area)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT TOP ({top_n})
            V.EmployeeKey, V.UserName,
            V.ResourceKey, V.ResourceName, V.ResourceType,
            V.ShiftKey,
            V.UnitsProcessed,
            V.AvgCycleTimeSec, V.PeerAvgCycleTimeSec,
            V.PerformanceDeviationPct,
            V.Yield, V.ReworkCount,
            V.StdDevCycleSec,
            V.DowntimePct_PerOperator,
            V.BottleneckShiftCount,
            V.SkillLevel,
            DA.AreaName, DF.FacilityName
        FROM Bottleneck_MCP.vw_OperatorAtResource V
        LEFT JOIN Datasets.V_DimArea     DA ON DA.AreaKey     = V.AreaKey
        LEFT JOIN Datasets.V_DimFacility DF ON DF.FacilityKey = V.FacilityKey
        {where}
        ORDER BY V.UnitsProcessed DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    out = []
    for r in rows:
        out.append({
            "employee_key": str(r.get("EmployeeKey")) if r.get("EmployeeKey") is not None else None,
            "operator_name": r.get("UserName"),
            "resource_name": r.get("ResourceName"),
            "resource_type": r.get("ResourceType"),
            "shift_key": str(r.get("ShiftKey")) if r.get("ShiftKey") is not None else None,
            "facility": r.get("FacilityName"),
            "area": r.get("AreaName"),
            "units_processed": r.get("UnitsProcessed"),
            "avg_cycle_time_sec": r.get("AvgCycleTimeSec"),
            "peer_avg_cycle_time_sec": r.get("PeerAvgCycleTimeSec"),
            "performance_deviation_pct": r.get("PerformanceDeviationPct"),
            "yield": r.get("Yield"),
            "rework_count": r.get("ReworkCount"),
            "stdev_cycle_sec": r.get("StdDevCycleSec"),
            "downtime_pct": r.get("DowntimePct_PerOperator"),
            "bottleneck_shift_count": r.get("BottleneckShiftCount"),
            "skill_level": r.get("SkillLevel"),
        })

    # v1.2 fallback: if vw_OperatorAtResource has no rows for the filter (common in
    # the pilot DWH where named-operator scoreboard rows are sparse), aggregate
    # FactMaterialEventPair directly so the LLM still gets a structurally complete
    # answer covering the system-account events.
    used_fallback = False
    if not out:
        used_fallback = True
        fb_clauses, fb_params = [], []
        if resource_name:
            fb_clauses.append("DR.ResourceName = %s"); fb_params.append(resource_name)
        if shift_key:
            fb_clauses.append("P.EndShiftKey = %s"); fb_params.append(shift_key)
        if facility:
            fb_clauses.append("DF.FacilityName = %s"); fb_params.append(facility)
        if area:
            fb_clauses.append("DA.AreaName = %s"); fb_params.append(area)
        fb_where = (" AND " + " AND ".join(fb_clauses)) if fb_clauses else ""
        fb_sql = f"""
            SELECT TOP ({top_n})
                P.EndEmployeeKey                                   AS EmployeeKey,
                DE.UserName,
                DR.ResourceName,
                P.EndShiftKey                                      AS ShiftKey,
                COUNT(*)                                            AS UnitsProcessed,
                AVG(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT))   AS AvgCycleTimeSec,
                STDEV(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT)) AS StdDevCycleSec,
                DA.AreaName, DF.FacilityName
            FROM dbo.FactMaterialEventPair P
            LEFT JOIN Datasets.V_DimEmployee DE ON DE.EmployeeKey = P.EndEmployeeKey
            LEFT JOIN Datasets.V_DimResource DR ON DR.ResourceKey = P.EndResourceKey
            LEFT JOIN Datasets.V_DimArea     DA ON DA.AreaKey     = P.EndAreaKey
            LEFT JOIN Datasets.V_DimFacility DF ON DF.FacilityKey = DA.FacilityKey
            WHERE P.IsOpen = 0 AND P.EndEmployeeKey IS NOT NULL {fb_where}
            GROUP BY P.EndEmployeeKey, DE.UserName, DR.ResourceName, P.EndShiftKey,
                     DA.AreaName, DF.FacilityName
            ORDER BY UnitsProcessed DESC
        """
        fb_rows = _query(fb_sql, tuple(fb_params), db=DB_NAME_DWH)
        for r in fb_rows:
            out.append({
                "employee_key": str(r.get("EmployeeKey")) if r.get("EmployeeKey") is not None else None,
                "operator_name": r.get("UserName"),
                "resource_name": r.get("ResourceName"),
                "resource_type": None,
                "shift_key": str(r.get("ShiftKey")) if r.get("ShiftKey") is not None else None,
                "facility": r.get("FacilityName"),
                "area": r.get("AreaName"),
                "units_processed": r.get("UnitsProcessed"),
                "avg_cycle_time_sec": r.get("AvgCycleTimeSec"),
                "peer_avg_cycle_time_sec": None,
                "performance_deviation_pct": None,
                "yield": None,
                "rework_count": None,
                "stdev_cycle_sec": r.get("StdDevCycleSec"),
                "downtime_pct": None,
                "bottleneck_shift_count": None,
                "skill_level": None,
            })

    return json.dumps({
        "filter": {"resource_name": resource_name, "facility": facility, "area": area,
                   "shift": shift, "shift_key_resolved": shift_key},
        "top_n": top_n,
        "operator_count": len(out),
        "operators": out,
        "used_fallback": used_fallback,
        "data_source": (
            "dbo.FactMaterialEventPair (fallback aggregation)"
            if used_fallback else "Bottleneck_MCP.vw_OperatorAtResource (DWH)"
        ),
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 5: GET OPERATOR LEARNING CURVE  (prompt #12)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_learning_curve(
    employee_key: int | str,
    resource_name: str | None = None,
    days: int = 60,
    bucket: str = "day",
    date_start: str | None = None,
    date_end: str | None = None,
) -> str:
    """LEARNING CURVE — for the named operator (by EmployeeKey), return a
    daily/weekly time series of avg cycle time + event count over the last N
    days, plus the experienced-operator baseline (all OTHER ops on the same
    resource in the same window). Lambda also returns a regression slope
    (cycle_time vs bucket index) so the LLM can quote the gap-closure rate.

    Args:
        employee_key: target operator's EmployeeKey (use list_operators)
        resource_name: optional ResourceName filter
        days: window length (default 60, max 365)
        bucket: 'day' (default) or 'week'
    """
    days = min(max(1, int(days)), 365)
    if bucket == "hour":
        bucket_expr = "DATEADD(HOUR, DATEDIFF(HOUR, 0, P.EndUTCDatetime), 0)"
    elif bucket == "week":
        bucket_expr = "DATEADD(DAY, -DATEPART(WEEKDAY, P.EndUTCDatetime)+1, CAST(P.EndUTCDatetime AS DATE))"
    else:
        bucket_expr = "CAST(P.EndUTCDatetime AS DATE)"
    extra_clauses, extra_params = [], []
    if resource_name:
        extra_clauses.append("DR.ResourceName = %s")
        extra_params.append(resource_name)
    if date_start:
        extra_clauses.append("P.EndUTCDatetime >= %s")
        extra_params.append(date_start)
    if date_end:
        extra_clauses.append("P.EndUTCDatetime <= %s")
        extra_params.append(date_end)

    # If neither date_start nor date_end supplied, default to last `days` days from now
    if not date_start and not date_end:
        date_clause = f"AND P.EndUTCDatetime >= DATEADD(DAY, -{days}, GETUTCDATE())"
    else:
        date_clause = ""

    extra_where = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""
    sql = f"""
        SELECT
            {bucket_expr}                          AS bucket_date,
            CASE WHEN P.EndEmployeeKey = %s THEN 'target' ELSE 'baseline' END AS operator_group,
            COUNT(*)                                AS event_count,
            AVG(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT))   AS avg_cycle_seconds,
            STDEV(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT)) AS stdev_cycle_seconds
        FROM dbo.FactMaterialEventPair P
        LEFT JOIN Datasets.V_DimResource DR ON DR.ResourceKey = P.EndResourceKey
        WHERE P.EndEmployeeKey IS NOT NULL
          AND P.IsOpen = 0
          {date_clause}
          {extra_where}
        GROUP BY {bucket_expr},
                 CASE WHEN P.EndEmployeeKey = %s THEN 'target' ELSE 'baseline' END
        ORDER BY bucket_date
    """
    full_params = [int(employee_key), *extra_params, int(employee_key)]
    rows = _query(sql, tuple(full_params), db=DB_NAME_DWH)

    target = [r for r in rows if r.get("operator_group") == "target"]
    baseline = [r for r in rows if r.get("operator_group") == "baseline"]

    # Server-side regression slope: target avg_cycle_seconds vs bucket index
    slope = intercept = None
    if len(target) >= 2:
        xs = list(range(len(target)))
        ys = [float(r.get("avg_cycle_seconds") or 0) for r in target]
        n = len(xs); mx = sum(xs)/n; my = sum(ys)/n
        num = sum((x-mx)*(y-my) for x, y in zip(xs, ys))
        den = sum((x-mx)**2 for x in xs)
        if den > 0:
            slope = num/den
            intercept = my - slope*mx

    # v1.2: data-window awareness — surface the constraint instead of fabricating a curve
    actual_min = min((r.get("bucket_date") for r in (target + baseline) if r.get("bucket_date")), default=None)
    actual_max = max((r.get("bucket_date") for r in (target + baseline) if r.get("bucket_date")), default=None)
    warning = _data_window_warning(actual_min, actual_max, days)

    return json.dumps({
        "filter": {"employee_key": str(employee_key), "resource_name": resource_name,
                   "days": days, "bucket": bucket},
        "target_buckets": target,
        "baseline_buckets": baseline,
        "regression": {
            "slope_seconds_per_bucket": slope,
            "intercept_seconds": intercept,
            "interpretation": "Negative slope = improving (cycle time falling per bucket)",
        },
        "data_window_warning": warning,
        "data_window": {"earliest": actual_min, "latest": actual_max},
        "hint": "Pass bucket='hour' to fit a within-day curve when only 1-2 days of data are available.",
        "data_source": "dbo.FactMaterialEventPair grouped by (bucket_date, operator_group)",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 6: GET OPERATOR UTILIZATION  (prompt #13)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_utilization(
    shift: str | None = None,
    facility: str | None = None,
    area: str | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    top_n: int = 100,
) -> str:
    """OPERATOR UTILIZATION — backed by FactResourceServiceTime which has
    EmployeeKey + NonWorkingTimeInSeconds. Computes:
      • productive_seconds  = window_seconds - SUM(NonWorkingTimeInSeconds)
      • utilization         = productive_seconds / window_seconds
      • Categorize: Underutilized < 0.70, Balanced 0.70–0.95, Overutilized > 0.95.

    Args:
        shift / facility / area: dim filters
        date_start / date_end: ISO window
        top_n: keep top-N operators (default 100, max 500)
    """
    top_n = min(max(1, int(top_n)), 500)
    shift_key = _resolve_shift(shift)
    clauses, params = [], []
    if shift_key:
        clauses.append("FST.ShiftKey = %s"); params.append(shift_key)
    if facility:
        clauses.append("DF.FacilityName = %s"); params.append(facility)
    if area:
        clauses.append("DA.AreaName = %s"); params.append(area)
    if date_start:
        clauses.append("FST.UTCOperationEndTime >= %s"); params.append(date_start)
    if date_end:
        clauses.append("FST.UTCOperationEndTime <= %s"); params.append(date_end)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT TOP ({top_n})
            FST.EmployeeKey,
            DE.UserName,
            FST.ShiftKey,
            DSFT.ShiftDefinitionShiftName                                  AS shift_name,
            COUNT(*)                                                       AS event_count,
            SUM(ISNULL(FST.NonWorkingTimeInSeconds, 0))                    AS non_working_seconds,
            DATEDIFF(SECOND, MIN(FST.UTCOperationEndTime),
                            MAX(FST.UTCNextOperationEndTime))              AS span_seconds
        FROM dbo.FactResourceServiceTime FST
        LEFT JOIN Datasets.V_DimEmployee  DE   ON DE.EmployeeKey  = FST.EmployeeKey
        LEFT JOIN Datasets.V_DimShift     DSFT ON DSFT.ShiftKey   = FST.ShiftKey
        LEFT JOIN Datasets.V_DimArea      DA   ON DA.AreaKey      = FST.AreaKey
        LEFT JOIN Datasets.V_DimFacility  DF   ON DF.FacilityKey  = DA.FacilityKey
        {where}
        GROUP BY FST.EmployeeKey, DE.UserName, FST.ShiftKey, DSFT.ShiftDefinitionShiftName
        ORDER BY span_seconds DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    out = []
    for r in rows:
        nw = float(r.get("non_working_seconds") or 0)
        span = float(r.get("span_seconds") or 0)
        prod = max(0.0, span - nw)
        util = round(prod / span, 4) if span > 0 else None
        bucket = (
            "Underutilized" if util is not None and util < 0.70 else
            "Overutilized"  if util is not None and util > 0.95 else
            "Balanced"      if util is not None else "Unknown"
        )
        out.append({
            "employee_key": str(r.get("EmployeeKey")) if r.get("EmployeeKey") is not None else None,
            "operator_name": r.get("UserName"),
            "shift_key": str(r.get("ShiftKey")) if r.get("ShiftKey") is not None else None,
            "shift_name": r.get("shift_name"),
            "productive_seconds": int(prod),
            "non_working_seconds": int(nw),
            "span_seconds": int(span),
            "utilization": util,
            "category": bucket,
            "event_count": r.get("event_count"),
        })
    return json.dumps({
        "filter": {"shift": shift, "shift_key_resolved": shift_key,
                   "facility": facility, "area": area,
                   "date_start": date_start, "date_end": date_end},
        "top_n": top_n,
        "operator_count": len(out),
        "operators": out,
        "thresholds": {"underutilized_below": 0.70, "overutilized_above": 0.95},
        "data_source": "dbo.FactResourceServiceTime + V_DimShift (NonWorkingTimeInSeconds + ShiftName)",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 7: GET OPERATOR QUALITY RANKING  (prompt #14)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_quality_ranking(
    facility: str | None = None,
    area: str | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    top_n: int = 50,
    min_events: int = 5,
) -> str:
    """OPERATOR QUALITY RANKING — first-pass yield per operator from
    FactMaterialQuality. FPY = SUM(GoodPieces) / SUM(TotalPieces).

    Pair with UC-3's get_scrap_pareto(group_by='reason') for the *why*.

    Args:
        facility / area / date_start / date_end: filters
        top_n: keep top-N operators (default 50, max 500)
        min_events: minimum quality records per op (default 5)
    """
    top_n = min(max(1, int(top_n)), 500)
    min_events = max(1, int(min_events))
    clauses, params = [], []
    if facility:
        clauses.append("DF.FacilityName = %s"); params.append(facility)
    if area:
        clauses.append("DA.AreaName = %s"); params.append(area)
    if date_start:
        clauses.append("FQ.UTCReasonDateTime >= %s"); params.append(date_start)
    if date_end:
        clauses.append("FQ.UTCReasonDateTime <= %s"); params.append(date_end)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT TOP ({top_n})
            FQ.EmployeeKey,
            DE.UserName,
            COUNT(*)                                AS event_count,
            SUM(ISNULL(FQ.GoodPieces,    0))        AS good_pieces,
            SUM(ISNULL(FQ.TotalPieces,   0))        AS total_pieces,
            SUM(ISNULL(FQ.PassedPieces,  0))        AS passed_pieces,
            SUM(ISNULL(FQ.LossPrimaryQuantity, 0))  AS loss_qty,
            CASE WHEN SUM(ISNULL(FQ.TotalPieces, 0)) > 0
                 THEN CAST(SUM(ISNULL(FQ.GoodPieces, 0.0)) AS FLOAT)
                      / SUM(ISNULL(FQ.TotalPieces, 0))
                 ELSE NULL END                       AS first_pass_yield
        FROM dbo.FactMaterialQuality FQ
        LEFT JOIN Datasets.V_DimEmployee DE ON DE.EmployeeKey = FQ.EmployeeKey
        LEFT JOIN Datasets.V_DimArea     DA ON DA.AreaKey     = FQ.AreaKey
        LEFT JOIN Datasets.V_DimFacility DF ON DF.FacilityKey = DA.FacilityKey
        {where}
        GROUP BY FQ.EmployeeKey, DE.UserName
        HAVING COUNT(*) >= {min_events}
        ORDER BY first_pass_yield DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    out = []
    for r in rows:
        fpy = r.get("first_pass_yield")
        out.append({
            "employee_key": str(r.get("EmployeeKey")) if r.get("EmployeeKey") is not None else None,
            "operator_name": r.get("UserName"),
            "event_count": r.get("event_count"),
            "good_pieces": r.get("good_pieces"),
            "total_pieces": r.get("total_pieces"),
            "passed_pieces": r.get("passed_pieces"),
            "loss_qty": r.get("loss_qty"),
            "first_pass_yield": fpy,
            "fpy_pct": round(float(fpy) * 100, 2) if fpy is not None else None,
        })

    # v1.2 fallback: when FactMaterialQuality returns nothing meaningful (pilot
    # has only 3 quality rows), derive a proxy FPY from FactMaterialEventPair +
    # FactResourceMaterialLossBonus (UC-3 fact). FPY_proxy = 1 - loss_events / total_events.
    def _is_zero_or_empty(v):
        try:
            return float(v or 0) == 0
        except (TypeError, ValueError):
            return True
    used_fallback = False
    if not out or all(_is_zero_or_empty(o.get("total_pieces")) for o in out):
        used_fallback = True
        fb_clauses, fb_params = [], []
        if facility:
            fb_clauses.append("DF.FacilityName = %s"); fb_params.append(facility)
        if area:
            fb_clauses.append("DA.AreaName = %s"); fb_params.append(area)
        if date_start:
            fb_clauses.append("P.EndUTCDatetime >= %s"); fb_params.append(date_start)
        if date_end:
            fb_clauses.append("P.EndUTCDatetime <= %s"); fb_params.append(date_end)
        fb_where = (" AND " + " AND ".join(fb_clauses)) if fb_clauses else ""
        fb_sql = f"""
            SELECT TOP ({top_n})
                P.EndEmployeeKey AS EmployeeKey,
                DE.UserName,
                COUNT(*)         AS event_count,
                SUM(CASE WHEN FRMLB.PrimaryQuantityLoss IS NOT NULL
                           OR FRMLB.SecondaryQuantityLoss IS NOT NULL
                         THEN 1 ELSE 0 END) AS loss_events,
                CASE WHEN COUNT(*) > 0 THEN
                    1.0 - (CAST(SUM(CASE WHEN FRMLB.PrimaryQuantityLoss IS NOT NULL
                                          OR FRMLB.SecondaryQuantityLoss IS NOT NULL
                                         THEN 1.0 ELSE 0 END) AS FLOAT) / COUNT(*))
                ELSE NULL END AS fpy_proxy
            FROM dbo.FactMaterialEventPair P
            LEFT JOIN Datasets.V_DimEmployee DE ON DE.EmployeeKey = P.EndEmployeeKey
            LEFT JOIN dbo.FactResourceMaterialLossBonus FRMLB
                ON  FRMLB.MaterialKey = P.MaterialKey
                AND FRMLB.StepKey     = P.StepKey
            LEFT JOIN Datasets.V_DimArea     DA ON DA.AreaKey     = P.EndAreaKey
            LEFT JOIN Datasets.V_DimFacility DF ON DF.FacilityKey = DA.FacilityKey
            WHERE P.IsOpen = 0 {fb_where}
            GROUP BY P.EndEmployeeKey, DE.UserName
            HAVING COUNT(*) >= {min_events}
            ORDER BY fpy_proxy DESC
        """
        fb_rows = _query(fb_sql, tuple(fb_params), db=DB_NAME_DWH)
        out = []
        for r in fb_rows:
            fpy = r.get("fpy_proxy")
            out.append({
                "employee_key": str(r.get("EmployeeKey")) if r.get("EmployeeKey") is not None else None,
                "operator_name": r.get("UserName"),
                "event_count": r.get("event_count"),
                "loss_events": r.get("loss_events"),
                "fpy_proxy": fpy,
                "fpy_pct": round(float(fpy) * 100, 2) if fpy is not None else None,
                "above_98_pct": (float(fpy) >= 0.98) if fpy is not None else None,
            })

    return json.dumps({
        "filter": {"facility": facility, "area": area, "min_events": min_events,
                   "date_start": date_start, "date_end": date_end},
        "top_n": top_n,
        "operator_count": len(out),
        "operators": out,
        "used_fallback": used_fallback,
        "data_source": (
            "dbo.FactMaterialEventPair × FactResourceMaterialLossBonus (FPY proxy)"
            if used_fallback else "dbo.FactMaterialQuality (GoodPieces / TotalPieces FPY)"
        ),
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 8: GET OPERATOR SHIFT COMPARISON  (prompt #15)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_shift_comparison(
    date_start: str | None = None,
    date_end: str | None = None,
    min_events_per_shift: int = 3,
    top_n: int = 50,
) -> str:
    """SHIFT COMPARISON WITHIN OPERATOR — for operators who worked across ≥2
    shifts in the window, return per-shift n / mean / std cycle time. LLM
    computes paired t-test verdict client-side.

    Args:
        date_start / date_end: ISO window
        min_events_per_shift: filter (default 3)
        top_n: keep top-N operators (default 50, max 200)
    """
    top_n = min(max(1, int(top_n)), 200)
    clauses, params = [], []
    if date_start:
        clauses.append("P.EndUTCDatetime >= %s"); params.append(date_start)
    if date_end:
        clauses.append("P.EndUTCDatetime <= %s"); params.append(date_end)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT
            P.EndEmployeeKey AS employee_key,
            DE.UserName,
            P.EndShiftKey AS shift_key,
            DSFT.ShiftDefinitionShiftName AS shift_name,
            COUNT(*)                                  AS event_count,
            AVG(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT))   AS avg_cycle_seconds,
            STDEV(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT)) AS stdev_cycle_seconds
        FROM dbo.FactMaterialEventPair P
        LEFT JOIN Datasets.V_DimEmployee DE   ON DE.EmployeeKey = P.EndEmployeeKey
        LEFT JOIN Datasets.V_DimShift    DSFT ON DSFT.ShiftKey  = P.EndShiftKey
        WHERE P.IsOpen = 0 AND P.EndEmployeeKey IS NOT NULL {where}
        GROUP BY P.EndEmployeeKey, DE.UserName, P.EndShiftKey, DSFT.ShiftDefinitionShiftName
        HAVING COUNT(*) >= {min_events_per_shift}
        ORDER BY P.EndEmployeeKey, P.EndShiftKey
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)

    by_op: dict = {}
    for r in rows:
        ek = str(r.get("employee_key")) if r.get("employee_key") is not None else None
        if ek not in by_op:
            by_op[ek] = {"employee_key": ek, "operator_name": r.get("UserName"), "shifts": []}
        by_op[ek]["shifts"].append({
            "shift_key": str(r.get("shift_key")) if r.get("shift_key") is not None else None,
            "shift_name": r.get("shift_name"),
            "event_count": r.get("event_count"),
            "avg_cycle_seconds": r.get("avg_cycle_seconds"),
            "stdev_cycle_seconds": r.get("stdev_cycle_seconds"),
        })
    multi = [v for v in by_op.values() if len(v["shifts"]) >= 2][:top_n]

    return json.dumps({
        "filter": {"date_start": date_start, "date_end": date_end,
                   "min_events_per_shift": min_events_per_shift},
        "top_n": top_n,
        "operator_count": len(multi),
        "operators": multi,
        "stat_method": (
            "Per-shift n / mean / std returned. LLM computes paired t-test "
            "or chi-square verdict client-side."
        ),
        "data_source": "dbo.FactMaterialEventPair grouped by EmployeeKey × ShiftKey",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 9: GET OPERATOR CERTIFICATION IMPACT  (prompt #16)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_certification_impact(
    resource_name: str | None = None,
    facility: str | None = None,
) -> str:
    """CERTIFICATION / SKILL-LEVEL IMPACT — backed by SkillLevel column already
    present on Bottleneck_MCP.vw_OperatorAtResource. Groups operators by skill
    level and reports per-group mean cycle / yield / rework / downtime.

    Args:
        resource_name / facility: optional filters
    """
    clauses, params = [], []
    if resource_name:
        clauses.append("V.ResourceName = %s"); params.append(resource_name)
    if facility:
        clauses.append("DF.FacilityName = %s"); params.append(facility)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT
            V.SkillLevel,
            COUNT(DISTINCT V.EmployeeKey)              AS operator_count,
            SUM(V.UnitsProcessed)                      AS total_units,
            AVG(CAST(V.AvgCycleTimeSec AS FLOAT))      AS avg_cycle_seconds,
            STDEV(CAST(V.AvgCycleTimeSec AS FLOAT))    AS stdev_cycle_seconds,
            AVG(CAST(V.Yield AS FLOAT))                AS avg_yield,
            SUM(V.ReworkCount)                         AS total_rework,
            AVG(CAST(V.DowntimePct_PerOperator AS FLOAT)) AS avg_downtime_pct
        FROM Bottleneck_MCP.vw_OperatorAtResource V
        LEFT JOIN Datasets.V_DimFacility DF ON DF.FacilityKey = V.FacilityKey
        {where}
        GROUP BY V.SkillLevel
        ORDER BY V.SkillLevel
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    return json.dumps({
        "filter": {"resource_name": resource_name, "facility": facility},
        "groups": rows,
        "stat_method": (
            "Group-level n / mean / std returned. LLM computes ANOVA verdict "
            "client-side. SkillLevel column is the source of truth for "
            "certification tier in this CMF deployment."
        ),
        "data_source": "Bottleneck_MCP.vw_OperatorAtResource grouped by SkillLevel",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 10: GET OPERATOR DOWNTIME CORRELATION  (prompt #17)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_downtime_correlation(
    resource_name: str | None = None,
    facility: str | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    top_n: int = 50,
) -> str:
    """OPERATOR ↔ DOWNTIME CORRELATION — counts and durations of UpToDownTransition
    events from FactResourceServiceTime grouped by EmployeeKey. Surfaces:
      • down_event_count       (rows where UpToDownTransition = 1)
      • non_working_seconds    (sum NonWorkingTimeInSeconds during ops)
      • top_state_reasons      (most-frequent MainStateModelStateReason)

    Args:
        resource_name / facility: dim filters
        date_start / date_end: ISO window
        top_n: keep top-N operators (default 50, max 500)
    """
    top_n = min(max(1, int(top_n)), 500)
    clauses, params = [], []
    if resource_name:
        clauses.append("FST.ResourceName = %s"); params.append(resource_name)
    if facility:
        clauses.append("DF.FacilityName = %s"); params.append(facility)
    if date_start:
        clauses.append("FST.UTCOperationEndTime >= %s"); params.append(date_start)
    if date_end:
        clauses.append("FST.UTCOperationEndTime <= %s"); params.append(date_end)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT TOP ({top_n})
            FST.EmployeeKey,
            DE.UserName,
            COUNT(*)                                                AS total_events,
            SUM(CASE WHEN FST.UpToDownTransition = 1 THEN 1 ELSE 0 END)      AS up_to_down_events,
            SUM(CASE WHEN FST.DownToUpTransition = 1 THEN 1 ELSE 0 END)      AS down_to_up_events,
            SUM(ISNULL(FST.NonWorkingTimeInSeconds, 0))             AS non_working_seconds,
            COUNT(DISTINCT FST.MainStateModelStateReason)           AS distinct_reasons
        FROM dbo.FactResourceServiceTime FST
        LEFT JOIN Datasets.V_DimEmployee DE ON DE.EmployeeKey = FST.EmployeeKey
        LEFT JOIN Datasets.V_DimArea     DA ON DA.AreaKey     = FST.AreaKey
        LEFT JOIN Datasets.V_DimFacility DF ON DF.FacilityKey = DA.FacilityKey
        {where}
        GROUP BY FST.EmployeeKey, DE.UserName
        ORDER BY up_to_down_events DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    return json.dumps({
        "filter": {"resource_name": resource_name, "facility": facility,
                   "date_start": date_start, "date_end": date_end},
        "top_n": top_n,
        "operator_count": len(rows),
        "operators": rows,
        "data_source": "dbo.FactResourceServiceTime (UpToDownTransition / NonWorkingTimeInSeconds)",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 11: GET OPERATOR PRODUCT MATRIX  (prompt #18)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_product_matrix(
    resource_name: str | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    min_events_per_cell: int = 3,
    top_n: int = 200,
) -> str:
    """OPERATOR × PRODUCT PERFORMANCE MATRIX — n / mean cycle / std for each
    (operator, product) cell with at least min_events_per_cell observations.

    Args:
        resource_name / date_start / date_end: filters
        min_events_per_cell: filter sparse cells (default 3)
        top_n: limit rows returned (default 200, max 1000)
    """
    top_n = min(max(1, int(top_n)), 1000)
    clauses, params = [], []
    if resource_name:
        clauses.append("DR.ResourceName = %s"); params.append(resource_name)
    if date_start:
        clauses.append("P.EndUTCDatetime >= %s"); params.append(date_start)
    if date_end:
        clauses.append("P.EndUTCDatetime <= %s"); params.append(date_end)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT TOP ({top_n})
            P.EndEmployeeKey                     AS employee_key,
            DE.UserName,
            DP.ProductName                       AS product,
            COUNT(*)                              AS event_count,
            AVG(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT))   AS avg_cycle_seconds,
            STDEV(CAST(DATEDIFF(SECOND, P.StartUTCDatetime, P.EndUTCDatetime) AS FLOAT)) AS stdev_cycle_seconds
        FROM dbo.FactMaterialEventPair P
        LEFT JOIN Datasets.V_DimEmployee DE ON DE.EmployeeKey = P.EndEmployeeKey
        LEFT JOIN Datasets.V_DimResource DR ON DR.ResourceKey = P.EndResourceKey
        LEFT JOIN Datasets.V_DimProduct  DP ON DP.ProductKey  = P.ProductKey
        WHERE P.IsOpen = 0 AND P.EndEmployeeKey IS NOT NULL {where}
        GROUP BY P.EndEmployeeKey, DE.UserName, DP.ProductName
        HAVING COUNT(*) >= {int(min_events_per_cell)}
        ORDER BY event_count DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)

    # v1.2: if the named-operator filter yields no cells, retry without
    # requiring EndEmployeeKey to be linked to a named operator (still groups
    # by EndEmployeeKey, just allows the system service account through). This
    # gives the LLM a structurally complete answer instead of "0 cells found."
    used_fallback = False
    if not rows:
        used_fallback = True
        # Same query but without IS NOT NULL guard
        fb_sql = sql.replace("AND P.EndEmployeeKey IS NOT NULL", "")
        rows = _query(fb_sql, tuple(params), db=DB_NAME_DWH)

    return json.dumps({
        "filter": {"resource_name": resource_name, "date_start": date_start, "date_end": date_end,
                   "min_events_per_cell": int(min_events_per_cell)},
        "top_n": top_n,
        "cell_count": len(rows),
        "matrix": rows,
        "used_fallback": used_fallback,
        "data_source": "dbo.FactMaterialEventPair × V_DimProduct grouped by (EmployeeKey, ProductName)",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 12: GET OPERATOR EXPERIENCE IMPACT  (prompt #19)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_experience_impact(
    area: str | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> str:
    """EXPERIENCE → REWORK CORRELATION — buckets operators by tenure (months
    since V_DimEmployee.CreateTimestamp):
      • 0-3 months  : new
      • 3-12 months : intermediate
      • 12-24 months: experienced
      • 24+ months  : veteran
    Per bucket: avg cycle, total events, loss events, rework rate.
    Lambda also computes a regression slope (rework_rate vs avg_tenure_months).

    Args:
        area / date_start / date_end: filters
    """
    clauses, params = [], []
    if area:
        clauses.append("DA.AreaName = %s"); params.append(area)
    if date_start:
        clauses.append("FRMLB.LC1OperationEndTime >= %s"); params.append(date_start)
    if date_end:
        clauses.append("FRMLB.LC1OperationEndTime <= %s"); params.append(date_end)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        WITH ops AS (
            SELECT
                DE.EmployeeKey,
                DE.UserName,
                DATEDIFF(MONTH, DE.CreateTimestamp, GETUTCDATE()) AS tenure_months,
                FRMLB.PrimaryQuantityLoss,
                FRMLB.SecondaryQuantityLoss,
                FRMLB.AreaKey
            FROM Datasets.V_DimEmployee DE
            LEFT JOIN dbo.FactResourceMaterialLossBonus FRMLB
                 ON FRMLB.EmployeeKey = DE.EmployeeKey
        )
        SELECT
            CASE
              WHEN tenure_months <= 3  THEN '0-3 months (new)'
              WHEN tenure_months <= 12 THEN '3-12 months (intermediate)'
              WHEN tenure_months <= 24 THEN '12-24 months (experienced)'
              ELSE '24+ months (veteran)'
            END AS tenure_bucket,
            COUNT(DISTINCT ops.EmployeeKey) AS operator_count,
            COUNT(*)                        AS total_events,
            SUM(CASE WHEN ops.PrimaryQuantityLoss IS NOT NULL
                       OR ops.SecondaryQuantityLoss IS NOT NULL
                     THEN 1 ELSE 0 END)     AS loss_events,
            CASE WHEN COUNT(*) > 0 THEN
                CAST(SUM(CASE WHEN ops.PrimaryQuantityLoss IS NOT NULL
                               OR ops.SecondaryQuantityLoss IS NOT NULL
                              THEN 1.0 ELSE 0 END) AS FLOAT) / COUNT(*)
            ELSE NULL END AS rework_rate,
            AVG(CAST(ops.tenure_months AS FLOAT)) AS avg_tenure_months
        FROM ops
        LEFT JOIN Datasets.V_DimArea DA ON DA.AreaKey = ops.AreaKey
        {where}
        GROUP BY
            CASE
              WHEN tenure_months <= 3  THEN '0-3 months (new)'
              WHEN tenure_months <= 12 THEN '3-12 months (intermediate)'
              WHEN tenure_months <= 24 THEN '12-24 months (experienced)'
              ELSE '24+ months (veteran)'
            END
        ORDER BY MIN(tenure_months)
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)

    pts = [(float(r.get("avg_tenure_months") or 0), float(r.get("rework_rate") or 0))
           for r in rows if r.get("avg_tenure_months") is not None and r.get("rework_rate") is not None]
    slope = intercept = None
    if len(pts) >= 2:
        n = len(pts); mx = sum(p[0] for p in pts)/n; my = sum(p[1] for p in pts)/n
        num = sum((x-mx)*(y-my) for x, y in pts)
        den = sum((x-mx)**2 for x, _ in pts)
        if den > 0:
            slope = num/den
            intercept = my - slope*mx

    return json.dumps({
        "filter": {"area": area, "date_start": date_start, "date_end": date_end},
        "tenure_buckets": rows,
        "regression": {
            "slope_rework_per_month_tenure": slope,
            "intercept": intercept,
            "interpretation": "Negative slope = rework rate falls with tenure (learning effect)",
        },
        "tenure_definition": "Months since V_DimEmployee.CreateTimestamp",
        "data_source": "Datasets.V_DimEmployee + dbo.FactResourceMaterialLossBonus",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 13: GET OPERATOR VERSATILITY  (prompt #20)
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_operator_versatility(
    date_start: str | None = None,
    date_end: str | None = None,
    expert_min_events: int = 50,
    competent_min_events: int = 5,
    top_n: int = 100,
) -> str:
    """OPERATOR VERSATILITY (T-shaped vs I-shaped) — per operator, count
    expert / competent / incidental resources. Classify:
      • I-shaped : expert ≤ 1 AND total ≤ 2
      • T-shaped : expert ≤ 2 AND total ≥ 3
      • π-shaped : expert ≥ 2 AND total ≥ 3

    Args:
        date_start / date_end: ISO window
        expert_min_events: cutoff for "expert" (default 50)
        competent_min_events: cutoff for "competent" (default 5)
        top_n: keep top-N (default 100, max 500)
    """
    top_n = min(max(1, int(top_n)), 500)
    clauses, params = [], []
    if date_start:
        clauses.append("P.EndUTCDatetime >= %s"); params.append(date_start)
    if date_end:
        clauses.append("P.EndUTCDatetime <= %s"); params.append(date_end)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        WITH per_resource AS (
            SELECT
                P.EndEmployeeKey AS employee_key,
                DE.UserName,
                P.EndResourceKey AS resource_key,
                DR.ResourceName,
                COUNT(*) AS event_count
            FROM dbo.FactMaterialEventPair P
            LEFT JOIN Datasets.V_DimEmployee DE ON DE.EmployeeKey = P.EndEmployeeKey
            LEFT JOIN Datasets.V_DimResource DR ON DR.ResourceKey = P.EndResourceKey
            WHERE P.IsOpen = 0 AND P.EndEmployeeKey IS NOT NULL {where}
            GROUP BY P.EndEmployeeKey, DE.UserName, P.EndResourceKey, DR.ResourceName
        )
        SELECT TOP ({top_n})
            employee_key, UserName,
            COUNT(*) AS total_resources,
            SUM(CASE WHEN event_count >= {int(expert_min_events)} THEN 1 ELSE 0 END) AS expert_resources,
            SUM(CASE WHEN event_count >= {int(competent_min_events)}
                  AND event_count < {int(expert_min_events)} THEN 1 ELSE 0 END) AS competent_resources,
            SUM(CASE WHEN event_count >= 1
                  AND event_count < {int(competent_min_events)} THEN 1 ELSE 0 END) AS incidental_resources,
            SUM(event_count) AS total_events
        FROM per_resource
        GROUP BY employee_key, UserName
        ORDER BY total_events DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    out = []
    for r in rows:
        exp = int(r.get("expert_resources") or 0)
        total = int(r.get("total_resources") or 0)
        if exp >= 2 and total >= 3:
            shape = "π-shaped"
        elif exp <= 2 and total >= 3:
            shape = "T-shaped"
        else:
            shape = "I-shaped"
        out.append({
            "employee_key": str(r.get("employee_key")) if r.get("employee_key") is not None else None,
            "operator_name": r.get("UserName"),
            "total_resources": total,
            "expert_resources": exp,
            "competent_resources": int(r.get("competent_resources") or 0),
            "incidental_resources": int(r.get("incidental_resources") or 0),
            "total_events": int(r.get("total_events") or 0),
            "shape": shape,
        })
    return json.dumps({
        "filter": {"date_start": date_start, "date_end": date_end,
                   "expert_min_events": int(expert_min_events),
                   "competent_min_events": int(competent_min_events)},
        "thresholds": {"expert_min_events": int(expert_min_events),
                       "competent_min_events": int(competent_min_events)},
        "top_n": top_n,
        "operator_count": len(out),
        "operators": out,
        "data_source": "dbo.FactMaterialEventPair grouped by (EndEmployeeKey, EndResourceKey)",
    }, default=str)


# ════════════════════════════════════════════════════════════════
# TOOL 14 (DISCOVERY): _DISCOVER_SCHEMA
# ════════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def _discover_schema(
    table_pattern: str = "%",
    column_pattern: str = "%",
    db: str = "DWH",
    limit: int = 200,
) -> str:
    """READ-ONLY SCHEMA PROBE — INFORMATION_SCHEMA.COLUMNS rows matching
    LIKE patterns. Used to verify column names of any DWH fact / view / OLTP
    table. Strictly read-only.

    Args:
        table_pattern: LIKE pattern, e.g. '%Employee%'
        column_pattern: LIKE pattern, e.g. '%Skill%'
        db: 'DWH' (default) | 'OLTP' | 'ODS'
        limit: max rows (default 200, max 1000)
    """
    db_map = {"OLTP": DB_NAME_OLTP, "DWH": DB_NAME_DWH, "ODS": DB_NAME_ODS}
    target_db = db_map.get(db.upper(), DB_NAME_DWH)
    limit = min(max(1, int(limit)), 1000)
    sql = f"""
        SELECT TOP ({limit})
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME  LIKE %s
          AND COLUMN_NAME LIKE %s
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """
    rows = _query(sql, (table_pattern, column_pattern), db=target_db)
    return json.dumps({
        "filter": {"table_pattern": table_pattern, "column_pattern": column_pattern, "db": db},
        "row_count": len(rows),
        "rows": rows,
        "data_source": f"INFORMATION_SCHEMA.COLUMNS in {target_db}",
    }, default=str)


def lambda_handler(event, context):
    return mcp.handle_request(event, context)
