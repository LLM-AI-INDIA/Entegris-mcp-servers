"""
AWS Lambda MCP Server for ATHENA AI - Entegris KSP UC-3 Scrap Pareto Analysis
Queries on-premises SQL Server database via Site-to-Site VPN.

Database (configured via Lambda environment variables — see deploy.py):
  - Entegris KSP Data Warehouse — material loss/scrap fact table, dim views,
    reason taxonomy, yield-loss aggregates.

Primary tables (all from Athena's "Material Losses DWH" + "Material Yield Loss DWH"
dataset definitions in CM MES Report Dataset.docx):
  - dbo.FactResourceMaterialLossBonus       (one row per loss/bonus event)
  - DataSets.V_DimReason / V_DimProduct / V_DimMaterial
  - DataSets.V_DimStep / V_DimArea / V_DimFacility / V_DimShift / V_DimUnit
  - dbo.DimReason  (reason taxonomy — same one UC-1 uses)

The SQL projections, joins, and filter shapes here are LIFTED VERBATIM from
Athena's Power-BI dataset SQL so the row counts and aggregates match the
reports Entegris already trusts.

Tools (9 total):
  1.  get_info                   — system info, capabilities, available tools
  2.  get_source_info            — DB provenance, fact-table row counts, time window
  3.  list_loss_reasons          — master list of Loss reason codes from dbo.DimReason
  4.  get_scrap_events           — raw loss events (Athena's "Material Losses DWH" SELECT)
  5.  get_scrap_pareto           — events grouped by reason, ranked desc, with cumulative %
                                    (slide 10 — bars + Pareto line)
  6.  get_scrap_summary          — single-row aggregate (total qty, distinct reasons, top reason)
  7.  get_yield_loss_breakdown   — Athena's "Material Yield Loss DWH" lifecycle decomposition
                                    + Yield Loss % ratio (slide 15)
  8.  get_top_loss_steps         — companion drill-down: which steps generated the most scrap
  9.  get_volume_yield           — Athena's "Material Volume and Yield DWH" — Quantity In /
                                    Out / Yield % per step / product / facility / area (slide 14)

Safety: READ-ONLY access. SQL keyword blocker enforced. Parameterized queries only.
Note: WRITE actions (scrap, disposition, e-signature) are NOT exposed.
"""

import functools
import json
import logging
import os
import re

import pymssql
from awslabs.mcp_lambda_handler import MCPLambdaHandler

# ─── Logging ─── #
logging.basicConfig(level=logging.INFO, format="[%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)

# ─── MCP Lambda Handler ─── #
mcp = MCPLambdaHandler(
    name="ATHENA AI - Entegris KSP UC-3 Scrap Pareto MCP Server",
    version="1.0.0",
)

# ─── Config (must be provided via Lambda environment variables) ─── #
DB_SERVER = os.getenv("DB_SERVER")
DB_PORT = int(os.getenv("DB_PORT", "1433"))
DB_INSTANCE = os.getenv("DB_INSTANCE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME_DWH = os.getenv("DB_NAME_DWH")
DB_NAME_OLTP = os.getenv("DB_NAME_OLTP")
DB_NAME_ODS = os.getenv("DB_NAME_ODS")

# Loss/scrap fact + dimension views from Athena's Power-BI dataset
FACT_LOSS_TABLE = os.getenv("FACT_LOSS_TABLE", "dbo.FactResourceMaterialLossBonus")
REASON_TABLE_DWH = os.getenv("REASON_TABLE_DWH", "dbo.DimReason")

ROW_LIMIT = int(os.getenv("ROW_LIMIT", "1000"))

# ─── SQL Safety ─── #
_BLOCKED = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|MERGE|GRANT|REVOKE|DENY)\b",
    re.IGNORECASE,
)
# EXEC removed from the blocker — we invoke read-only Reports.* / DataSets.*
# stored procedures via a tight whitelist.
_ALLOWED_SPS: set[str] = set()  # No SP calls in UC-3 today; reserved for future.

# ─── User-friendly error messages ─── #
_ERROR_MESSAGES = {
    "connection": "The Entegris KSP database is temporarily unreachable. Please try again in a moment.",
    "timeout": "The query took too long to complete. Please narrow the search or try again shortly.",
    "permission": "This operation is not permitted. The system only allows read-only data access.",
    "query": "Unable to retrieve the requested data. Please check the input parameters and try again.",
    "unknown": "An unexpected issue occurred while processing your request. Please try again.",
    "not_found": "No data found matching your request. Please verify the input parameters and try again.",
}


class ToolError(Exception):
    """Custom exception that carries a user-friendly message."""

    def __init__(self, user_message, internal_message=None):
        self.user_message = user_message
        self.internal_message = internal_message or user_message
        super().__init__(self.user_message)


def safe_tool(func):
    """Catch all exceptions, log the internal detail, return a safe JSON error to the user."""

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


# ─── DB helpers (identical pattern to UC-1 for connection ladder + JSON-safe row coercion) ─── #
def _get_conn(db: str | None = None):
    """Connect to CMF SQL Server. Tries a ladder of connection variants:
      1. Named instance syntax (server\\INSTANCE, needs SQL Browser on UDP 1434)
      2. IP:port with default DB_PORT
      3. A short list of common CMF port assignments
    Returns on first success.
    """
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
            logger.info(f"DB connected via {label}")
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


# ─── Athena's "Material Losses DWH" core SELECT (verbatim columns + joins) ─── #
# Source: CM MES Reports/CM MES Report Dataset.docx, "Material Losses DWH" dataset (lines 794-1145).
# Columns and join graph are exactly Athena's; the WHERE clause is parameterized
# below per-tool.
_LOSS_BASE_SELECT = """
    SELECT
        DF.FacilityName              AS Facility,
        DA.AreaName                  AS Area,
        DS.StepName                  AS Step,
        DS.StepType                  AS StepType,
        DSFT.ShiftKey                AS ShiftKey,
        DP.ProductName               AS Product,
        DP.Revision                  AS ProductRevision,
        DM.MaterialName              AS Material,
        DM.Form                      AS MaterialForm,
        DM.Type                      AS MaterialType,
        UN.UnitName                  AS MaterialUnits,
        DR.ReasonName                AS LossReason,
        DR.ReasonId                  AS LossReasonId,
        FRMLB.LC1OperationEndTime    AS OperationEndTime,
        ISNULL(FRMLB.PrimaryQuantityLoss, FRMLB.SecondaryQuantityLoss) AS MaterialQuantityLoss,
        FRMLB.PrimaryQuantityBonus   AS MaterialPrimaryQuantityBonus,
        CASE WHEN FRMLB.PrimaryQuantityLoss IS NOT NULL
             THEN 'Primary' ELSE 'Secondary' END AS LossType
    FROM dbo.FactResourceMaterialLossBonus FRMLB
    INNER JOIN DataSets.V_DimArea     DA   ON DA.AreaKey     = FRMLB.AreaKey
    INNER JOIN DataSets.V_DimFacility DF   ON DF.FacilityKey = DA.FacilityKey
    INNER JOIN DataSets.V_DimStep     DS   ON DS.StepKey     = FRMLB.StepKey
    INNER JOIN DataSets.V_DimShift    DSFT ON DSFT.ShiftKey  = FRMLB.ShiftKey
    INNER JOIN DataSets.V_DimReason   DR   ON DR.ReasonKey   = FRMLB.LossReasonKey
    INNER JOIN DataSets.V_DimProduct  DP   ON DP.ProductKey  = FRMLB.ProductKey
    INNER JOIN DataSets.V_DimMaterial DM   ON DM.MaterialKey = FRMLB.MaterialKey
    INNER JOIN DataSets.V_DimUnit     UN
            ON UN.UnitKey = CASE WHEN FRMLB.PrimaryQuantityLoss IS NOT NULL
                                 THEN FRMLB.PrimaryUnitKey ELSE FRMLB.SecondaryUnitKey END
"""
# NOTE: V_DimShift has no ShiftName column in this Entegris CMF deployment;
# Athena's "Material Losses DWH" final SELECT also does not project ShiftName
# (it only joins on ShiftKey for the filter). We expose ShiftKey as a stable
# bigint id; consumers can join to a future ShiftName view if/when one exists.

# Filter-by-name pieces (parameterized by pymssql %s placeholders).
def _build_loss_where(
    date_start: str | None,
    date_end: str | None,
    facility: str | None,
    area: str | None,
    step: str | None,
    shift: str | None,
    product: str | None,
    material_type: str | None,
    material_form: str | None,
    loss_reason: str | None,
    step_type: str | None = None,
) -> tuple[str, list]:
    """Build the parameterized WHERE clause used by every loss-tool. Returns (sql, params)."""
    clauses = ["(FRMLB.PrimaryQuantityLoss IS NOT NULL OR FRMLB.SecondaryQuantityLoss IS NOT NULL)"]
    params: list = []
    if date_start:
        clauses.append("FRMLB.LC1OperationEndTime >= %s")
        params.append(date_start)
    if date_end:
        clauses.append("FRMLB.LC1OperationEndTime <= %s")
        params.append(date_end)
    if facility:
        clauses.append("DF.FacilityName = %s")
        params.append(facility)
    if area:
        clauses.append("DA.AreaName = %s")
        params.append(area)
    if step:
        clauses.append("DS.StepName = %s")
        params.append(step)
    if shift:
        # V_DimShift has no ShiftName; filter by ShiftKey (bigint as string) instead.
        clauses.append("DSFT.ShiftKey = %s")
        params.append(shift)
    if product:
        clauses.append("DP.ProductName = %s")
        params.append(product)
    if material_type:
        clauses.append("DM.Type = %s")
        params.append(material_type)
    if material_form:
        clauses.append("DM.Form = %s")
        params.append(material_form)
    if loss_reason:
        clauses.append("DR.ReasonName = %s")
        params.append(loss_reason)
    if step_type:
        clauses.append("DS.StepType = %s")
        params.append(step_type)
    return " WHERE " + " AND ".join(clauses), params


# ═══════════════════════════════════════════════════════════════
# TOOL 1: GET INFO
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_info() -> str:
    """Get comprehensive information about the Entegris KSP UC-3 Scrap Pareto system —
    what it does, which database it queries, and which tools are available.

    Use this tool FIRST when:
    - The user asks "what can you do?" / "what is this system?"
    - You need to orient yourself before answering a scrap/loss question
    """
    logger.info("get_info called")
    return json.dumps({
        "system": "Entegris KSP UC-3 Scrap Pareto Analysis",
        "description": (
            "Scrap & material-loss analytics for Entegris KSP production. Sources every "
            "loss event logged by CMF (Critical Manufacturing) into the data warehouse, "
            "groups and ranks by loss reason / step / product / shift, and produces a "
            "Pareto chart showing which causes drive the most material loss. SQL queries "
            "lifted verbatim from Athena's Power-BI dataset definitions in CM MES Report "
            "Dataset.docx so results match the reports Entegris already trusts."
        ),
        "capabilities": [
            "Loss Reason Catalog — list all 'Loss' reason codes",
            "Scrap Events — raw loss events with full facility/step/shift/reason context "
            "PLUS per-event quantity_lost and quantity_bonus (loss + rework on same event)",
            "Scrap Pareto — qty ranked desc with cumulative % (slide 10). Pivotable via "
            "group_by: reason (default) | step | product | shift | facility | area | step_type. "
            "Also serves Athena's 'Losses Quantity Pareto Line', 'Pareto line', "
            "'cumulative % line', 'Material Losses DWH chart' — same tool, same data shape "
            "(bar height = total_quantity_lost, line height = cumulative_pct).",
            "Scrap Summary — one-line total + top reason for a window",
            "Yield Loss Breakdown — loss decomposed by lifecycle state (Queued/Dispatched/"
            "InProcess/Processed) WITH Yield Loss % ratio — slide 15",
            "Top Loss Steps — drill-down: which steps generated the most loss",
            "Volume And Yield — Quantity In / Out / Yield % per step or product (slide 14)",
            "Source Verification — DB / fact row count / time window",
        ],
        "out_of_scope": [
            "Listing all materials of a product (use UC-1 Genealogy: search_materials).",
            "Listing in-process / WIP materials (use UC-1 Genealogy: get_material_lifecycle_status).",
            "Material genealogy / ancestor / descendant traces (use UC-1 Genealogy MCP).",
            "Server-side PNG rendering of the Pareto chart — JSON only; the LLM client "
            "renders the bars + cumulative line. PNG export is on the future-work backlog.",
        ],
        "databases": [f"{DB_NAME_DWH} (Entegris KSP Data Warehouse)"],
        "primary_table": FACT_LOSS_TABLE,
        "supporting_views": [
            "DataSets.V_DimArea", "DataSets.V_DimFacility", "DataSets.V_DimStep",
            "DataSets.V_DimShift", "DataSets.V_DimReason", "DataSets.V_DimProduct",
            "DataSets.V_DimMaterial", "DataSets.V_DimUnit",
        ],
        "data_model": (
            "Each FactResourceMaterialLossBonus row = one loss or bonus event recorded by "
            "CMF. Pareto axis = ReasonName (DataSets.V_DimReason). Quantity = "
            "ISNULL(PrimaryQuantityLoss, SecondaryQuantityLoss). Filterable by date, "
            "facility, area, step, shift, product, material type/form, loss reason."
        ),
        "athena_sources": [
            "CM MES Report Dataset.docx → 'Material Losses DWH' (lines 794-1145)",
            "CM MES Report Dataset.docx → 'Material Volume and Yield DWH' (lines 2252-2823)",
            "CM MES Report Dataset.docx → 'Material Yield Loss DWH' (lines 2824-3340)",
            "CM MES Reports.pptx → slide 10 (Pareto), slide 14 (Volume+Yield), slide 15 (Yield Loss)",
            "Loss_LBO_CSHARP.cs (LossReason, LossStep, BonusReason, PrimaryQuantityLoss fields)",
        ],
        "total_tools": 9,
        "write_actions_status": (
            "READ-ONLY deployment. Scrap transactions, disposition changes, and "
            "e-signature-gated actions are not exposed."
        ),
        "safety": "READ-ONLY access. Parameterized queries only. No data modifications allowed.",
        "accuracy_rules": [
            "Report exact numbers from tool results — never estimate or round",
            "Only report data returned by the current tool call — do not infer reasons or steps",
            "If a tool returns 0 rows for the date window, say so — do not fabricate scrap",
            "Always cite source: table name, row count, time window",
        ],
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 2: GET SOURCE INFO
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_source_info() -> str:
    """Return metadata about the scrap-loss data source: database name, server,
    fact table row count, distinct reasons / products / steps / shifts, and time
    window of the loss data. Use to verify provenance when a user doubts accuracy."""
    logger.info("get_source_info called")
    rows = _query(f"""
        SELECT
            DB_NAME() AS database_name,
            @@SERVERNAME AS sql_server_name,
            (SELECT COUNT(*) FROM {FACT_LOSS_TABLE}
             WHERE PrimaryQuantityLoss IS NOT NULL OR SecondaryQuantityLoss IS NOT NULL) AS total_loss_events,
            (SELECT COUNT(DISTINCT LossReasonKey) FROM {FACT_LOSS_TABLE}
             WHERE PrimaryQuantityLoss IS NOT NULL OR SecondaryQuantityLoss IS NOT NULL) AS distinct_loss_reasons,
            (SELECT COUNT(DISTINCT ProductKey) FROM {FACT_LOSS_TABLE}
             WHERE PrimaryQuantityLoss IS NOT NULL OR SecondaryQuantityLoss IS NOT NULL) AS distinct_products,
            (SELECT COUNT(DISTINCT StepKey) FROM {FACT_LOSS_TABLE}
             WHERE PrimaryQuantityLoss IS NOT NULL OR SecondaryQuantityLoss IS NOT NULL) AS distinct_steps,
            (SELECT COUNT(DISTINCT ShiftKey) FROM {FACT_LOSS_TABLE}
             WHERE PrimaryQuantityLoss IS NOT NULL OR SecondaryQuantityLoss IS NOT NULL) AS distinct_shifts,
            (SELECT MIN(LC1OperationEndTime) FROM {FACT_LOSS_TABLE}
             WHERE PrimaryQuantityLoss IS NOT NULL OR SecondaryQuantityLoss IS NOT NULL) AS earliest_loss,
            (SELECT MAX(LC1OperationEndTime) FROM {FACT_LOSS_TABLE}
             WHERE PrimaryQuantityLoss IS NOT NULL OR SecondaryQuantityLoss IS NOT NULL) AS latest_loss
    """, db=DB_NAME_DWH)
    info = rows[0] if rows else {}
    info["fact_table"] = FACT_LOSS_TABLE
    info["reason_table"] = REASON_TABLE_DWH
    info["data_lineage"] = (
        "Fact rows produced by CMF when an operator records a Loss/Bonus on a "
        "TrackOut. Same fact table backs Athena's 'Material Losses DWH' and "
        "'Material Yield Loss DWH' Power-BI datasets."
    )
    return json.dumps(info, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 3: LIST LOSS REASONS
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def list_loss_reasons() -> str:
    """List all 'Loss' reason codes from dbo.DimReason — the master taxonomy used by
    operators to classify scrap. Call this when the user asks "what loss reasons /
    scrap reason codes / loss codes / loss categories exist?" or before drilling
    into a Pareto, so they see the full set of possible buckets.
    """
    logger.info("list_loss_reasons called")
    rows = _query(f"""
        SELECT ReasonKey, ReasonId, ReasonType, ReasonName
        FROM {REASON_TABLE_DWH}
        WHERE ReasonType = %s
        ORDER BY ReasonName
    """, ("Loss",), db=DB_NAME_DWH)

    # Stringify 19-digit ReasonId so JS-based MCP clients don't lose precision.
    out = []
    for r in rows:
        out.append({
            "reason_key": r.get("ReasonKey"),
            "reason_id": str(r.get("ReasonId")) if r.get("ReasonId") is not None else None,
            "reason_type": r.get("ReasonType"),
            "reason_name": r.get("ReasonName"),
        })
    return json.dumps({
        "filter_reason_type": "Loss",
        "total_reasons": len(out),
        "reasons": out,
        "data_source": f"{REASON_TABLE_DWH} (filtered by ReasonType='Loss')",
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 4: GET SCRAP EVENTS (raw events — Athena's "Material Losses DWH" SELECT)
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_scrap_events(
    date_start: str | None = None,
    date_end: str | None = None,
    facility: str | None = None,
    area: str | None = None,
    step: str | None = None,
    shift: str | None = None,
    product: str | None = None,
    material_type: str | None = None,
    material_form: str | None = None,
    loss_reason: str | None = None,
    step_type: str | None = None,
    limit: int = 200,
) -> str:
    """RAW LOSS / SCRAP EVENTS for a window — backed by Athena's parameterized
    'Material Losses DWH' Power-BI dataset (CM MES Report Dataset.docx, lines 794-1145).
    Returns one row per loss event with facility / area / step / shift / product /
    material / reason / quantity / timestamp.

    Per-event quantities:
      • quantity_lost   = ISNULL(PrimaryQuantityLoss, SecondaryQuantityLoss)
      • quantity_bonus  = PrimaryQuantityBonus   (rework / recovery bonus on the same event)
    Note: 'initial qty before loss' is NOT on FactResourceMaterialLossBonus —
    that lives on dbo.V_FactMaterialMovement.QueuedPrimaryQuantity (per step) or
    on the Material LBO. Use get_volume_yield(group_by='product', product=...)
    to see Quantity In vs Loss for the lifecycle window covering the event.

    Call this for prompts like:
      • "Show me all scrap events on March 5"
      • "List the loss events at step Pleating last week"
      • "Pull every loss for product S4442R031Y23 between X and Y"
      • "What was the initial qty of <material> before the loss?"

    Out of scope: this tool returns only materials that have a recorded loss
    event. To list ALL materials of a product (regardless of loss) use the UC-1
    Genealogy MCP server (search_materials / get_material_summary).

    Args:
        date_start: ISO datetime, e.g. '2026-02-15T00:00:00' (UTC).
        date_end:   ISO datetime end of window (UTC).
        facility:   FacilityName filter (optional).
        area:       AreaName filter (optional).
        step:       StepName filter (optional).
        shift:      ShiftKey filter (e.g. '2', '3') — V_DimShift has no ShiftName.
        product:    ProductName filter (optional).
        material_type: DimMaterial.Type (e.g. 'Standard').
        material_form: DimMaterial.Form (e.g. 'Batch/Serial').
        loss_reason:   ReasonName from dbo.DimReason (use list_loss_reasons to see options).
        limit:      Max rows to return (1-1000, default 200).
    """
    logger.info(f"get_scrap_events: window={date_start}..{date_end}")
    limit = min(max(1, int(limit)), 1000)
    where_sql, params = _build_loss_where(
        date_start, date_end, facility, area, step, shift,
        product, material_type, material_form, loss_reason, step_type,
    )
    sql = f"""
        SELECT TOP {limit} *
        FROM (
            {_LOSS_BASE_SELECT}
            {where_sql}
        ) X
        ORDER BY OperationEndTime DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)

    out = []
    for r in rows:
        out.append({
            "event_time": r.get("OperationEndTime"),
            "facility": r.get("Facility"),
            "area": r.get("Area"),
            "step": r.get("Step"),
            "step_type": r.get("StepType"),
            "shift_key": str(r.get("ShiftKey")) if r.get("ShiftKey") is not None else None,
            "product": r.get("Product"),
            "product_revision": r.get("ProductRevision"),
            "material": r.get("Material"),
            "material_form": r.get("MaterialForm"),
            "material_type": r.get("MaterialType"),
            "loss_reason": r.get("LossReason"),
            "loss_reason_id": str(r.get("LossReasonId")) if r.get("LossReasonId") is not None else None,
            "quantity_lost": str(r.get("MaterialQuantityLoss") or ""),
            "quantity_bonus": str(r.get("MaterialPrimaryQuantityBonus")) if r.get("MaterialPrimaryQuantityBonus") is not None else None,
            "units": r.get("MaterialUnits"),
            "loss_type": r.get("LossType"),
        })
    return json.dumps({
        "filter": {
            "date_start": date_start, "date_end": date_end,
            "facility": facility, "area": area, "step": step, "shift": shift,
            "product": product, "material_type": material_type,
            "material_form": material_form, "loss_reason": loss_reason,
        },
        "limit": limit,
        "event_count": len(out),
        "events": out,
        "data_source": "Athena 'Material Losses DWH' SELECT (FactResourceMaterialLossBonus + 8 dim views)",
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 5: GET SCRAP PARETO  (the headline UC-3 tool)
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_scrap_pareto(
    date_start: str | None = None,
    date_end: str | None = None,
    facility: str | None = None,
    area: str | None = None,
    step: str | None = None,
    shift: str | None = None,
    product: str | None = None,
    material_type: str | None = None,
    material_form: str | None = None,
    step_type: str | None = None,
    top_n: int = 20,
    group_by: str = "reason",
) -> str:
    """SCRAP PARETO — loss ranked descending by total quantity, with cumulative %
    so you can see which buckets drive the bulk of the scrap (the classic 80/20).
    Same fact table and dimension joins as Athena's 'Material Losses DWH' dataset.
    Each returned row carries BOTH the bar height (total_quantity_lost / pct_of_total)
    AND the line height (cumulative_pct) — so the chart Athena renders on slide 10
    of CM MES Reports.pptx (bars labelled 'Losses Quantity' + line labelled
    'Losses Quantity Pareto Line') is fully reconstructable from this output.

    By default groups by loss REASON (slide 10 of CM MES Reports.pptx). Pivot to
    other dimensions via group_by:
      • 'reason'   → ReasonName            (default — the headline UC-3 chart)
      • 'step'     → DimStep.StepName       (which step generates most scrap)
      • 'product'  → DimProduct.ProductName
      • 'shift'    → DimShift.ShiftKey      (per-shift breakdown)
      • 'facility' → DimFacility.FacilityName
      • 'area'     → DimArea.AreaName
      • 'step_type'→ DimStep.StepType

    *** ALWAYS USE THIS TOOL when the user asks ANY of: ***
      • "scrap pareto / loss pareto / pareto chart / pareto analysis"
      • "Losses Quantity Pareto Line" (Athena's exact slide-10 label)
      • "Pareto line / cumulative % line / cumulative percent line / cumulative loss line"
      • "Material Losses DWH chart / Material Losses report / Losses Quantity bar chart"
      • "Slide 10 chart / Athena's loss Pareto / Athena's scrap chart"
      • "top loss reasons / top scrap causes / what's causing the most scrap"
      • "rank loss reasons by quantity"
      • "which reason has the most loss / 80/20 of our scrap"
      • "compare across shifts / shift-level breakdown" → group_by='shift'
      • "loss by product / loss by step / loss by area" → group_by= that key

    Output keys for chart rendering:
      • bar height          → total_quantity_lost (per group_value)
      • bar % of total      → pct_of_total
      • line (cumulative %) → cumulative_pct        ← the "Pareto Line" itself
      • grand_total_quantity_lost (denominator for both axes)

    Args (all filters optional — same shape as get_scrap_events):
        date_start / date_end: ISO window
        facility / area / step / shift / product / material_type / material_form
        top_n: keep top-N rows in the Pareto (default 20, max 100)
        group_by: aggregation dimension (see options above; default 'reason')
    """
    logger.info(f"get_scrap_pareto: window={date_start}..{date_end}, top_n={top_n}, group_by={group_by}")
    top_n = min(max(1, int(top_n)), 100)

    group_map = {
        "reason":    ("DR.ReasonName",    "loss_reason"),
        "step":      ("DS.StepName",      "step"),
        "product":   ("DP.ProductName",   "product"),
        "shift":     ("CAST(DSFT.ShiftKey AS NVARCHAR(64))", "shift"),
        "facility":  ("DF.FacilityName",  "facility"),
        "area":      ("DA.AreaName",      "area"),
        "step_type": ("DS.StepType",      "step_type"),
    }
    gb = (group_by or "reason").lower().strip()
    if gb not in group_map:
        return json.dumps({"status": "error",
                           "message": f"Unsupported group_by={group_by}. Use one of: {list(group_map)}"})
    group_col, group_key = group_map[gb]

    where_sql, params = _build_loss_where(
        date_start, date_end, facility, area, step, shift,
        product, material_type, material_form, None, step_type,
    )
    # Aggregation against the same Athena join graph; ranked + cumulative% computed
    # via window functions in T-SQL so we hand the LLM finished Pareto rows.
    sql = f"""
        WITH base AS (
            SELECT {group_col} AS GroupValue,
                   ISNULL(FRMLB.PrimaryQuantityLoss, FRMLB.SecondaryQuantityLoss) AS Qty
            FROM dbo.FactResourceMaterialLossBonus FRMLB
            INNER JOIN DataSets.V_DimArea     DA   ON DA.AreaKey     = FRMLB.AreaKey
            INNER JOIN DataSets.V_DimFacility DF   ON DF.FacilityKey = DA.FacilityKey
            INNER JOIN DataSets.V_DimStep     DS   ON DS.StepKey     = FRMLB.StepKey
            INNER JOIN DataSets.V_DimShift    DSFT ON DSFT.ShiftKey  = FRMLB.ShiftKey
            INNER JOIN DataSets.V_DimReason   DR   ON DR.ReasonKey   = FRMLB.LossReasonKey
            INNER JOIN DataSets.V_DimProduct  DP   ON DP.ProductKey  = FRMLB.ProductKey
            INNER JOIN DataSets.V_DimMaterial DM   ON DM.MaterialKey = FRMLB.MaterialKey
            {where_sql}
        ),
        agg AS (
            SELECT GroupValue,
                   COUNT(*)        AS event_count,
                   SUM(Qty)        AS total_quantity_lost
            FROM base
            GROUP BY GroupValue
        ),
        ranked AS (
            SELECT TOP ({top_n})
                   GroupValue, event_count, total_quantity_lost,
                   SUM(total_quantity_lost) OVER ()                                   AS grand_total,
                   SUM(total_quantity_lost) OVER (ORDER BY total_quantity_lost DESC
                                                  ROWS UNBOUNDED PRECEDING)            AS running_total
            FROM agg
            ORDER BY total_quantity_lost DESC
        )
        SELECT GroupValue, event_count, total_quantity_lost, grand_total, running_total
        FROM ranked
        ORDER BY total_quantity_lost DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)

    pareto: list[dict] = []
    grand_total = 0.0
    if rows:
        try:
            grand_total = float(rows[0].get("grand_total") or 0)
        except (ValueError, TypeError):
            grand_total = 0.0
    for i, r in enumerate(rows, start=1):
        try:
            qty = float(r.get("total_quantity_lost") or 0)
        except (ValueError, TypeError):
            qty = 0.0
        try:
            running = float(r.get("running_total") or 0)
        except (ValueError, TypeError):
            running = 0.0
        bucket = {
            "rank": i,
            group_key: r.get("GroupValue"),
            "group_value": r.get("GroupValue"),
            "event_count": r.get("event_count"),
            "total_quantity_lost": str(r.get("total_quantity_lost") or ""),
            "pct_of_total": round((qty / grand_total * 100.0), 2) if grand_total else 0.0,
            "cumulative_pct": round((running / grand_total * 100.0), 2) if grand_total else 0.0,
        }
        # Backwards-compat alias: existing callers expect 'loss_reason' on the default Pareto.
        if gb != "reason":
            bucket["loss_reason"] = None
        pareto.append(bucket)

    return json.dumps({
        "filter": {
            "date_start": date_start, "date_end": date_end,
            "facility": facility, "area": area, "step": step, "shift": shift,
            "product": product, "material_type": material_type, "material_form": material_form,
        },
        "group_by": gb,
        "top_n": top_n,
        "reason_count": len(pareto),
        "row_count": len(pareto),
        "grand_total_quantity_lost": str(grand_total),
        "pareto": pareto,
        "data_source": (
            f"Athena 'Material Losses DWH' SQL — grouped by {group_col}, ranked desc, cumulative%"
        ),
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 6: GET SCRAP SUMMARY  (single-row aggregate)
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_scrap_summary(
    date_start: str | None = None,
    date_end: str | None = None,
    facility: str | None = None,
    area: str | None = None,
    step: str | None = None,
    shift: str | None = None,
    product: str | None = None,
    step_type: str | None = None,
) -> str:
    """SCRAP SUMMARY — one-row headline view of loss for a window: total events,
    total quantity lost, distinct reasons, top reason, distinct steps, time span.
    Use this BEFORE the Pareto when the user asks 'how much scrap did we have
    last week' / 'give me a quick scrap summary' / 'overall loss for product X'.

    Args (all optional):
        date_start / date_end: ISO window
        facility / area / step / shift / product: filter dimensions
    """
    logger.info(f"get_scrap_summary: window={date_start}..{date_end}")
    where_sql, params = _build_loss_where(
        date_start, date_end, facility, area, step, shift,
        product, None, None, None, step_type,
    )
    sql = f"""
        WITH base AS (
            SELECT DR.ReasonName AS LossReason,
                   DS.StepName   AS StepName,
                   DSFT.ShiftKey AS ShiftKey,
                   FRMLB.LC1OperationEndTime AS event_time,
                   ISNULL(FRMLB.PrimaryQuantityLoss, FRMLB.SecondaryQuantityLoss) AS Qty
            FROM dbo.FactResourceMaterialLossBonus FRMLB
            INNER JOIN DataSets.V_DimArea     DA   ON DA.AreaKey     = FRMLB.AreaKey
            INNER JOIN DataSets.V_DimFacility DF   ON DF.FacilityKey = DA.FacilityKey
            INNER JOIN DataSets.V_DimStep     DS   ON DS.StepKey     = FRMLB.StepKey
            INNER JOIN DataSets.V_DimShift    DSFT ON DSFT.ShiftKey  = FRMLB.ShiftKey
            INNER JOIN DataSets.V_DimReason   DR   ON DR.ReasonKey   = FRMLB.LossReasonKey
            INNER JOIN DataSets.V_DimProduct  DP   ON DP.ProductKey  = FRMLB.ProductKey
            INNER JOIN DataSets.V_DimMaterial DM   ON DM.MaterialKey = FRMLB.MaterialKey
            {where_sql}
        )
        SELECT
            COUNT(*)                              AS total_events,
            SUM(Qty)                              AS total_quantity_lost,
            COUNT(DISTINCT LossReason)            AS distinct_reasons,
            COUNT(DISTINCT StepName)              AS distinct_steps,
            COUNT(DISTINCT ShiftKey)              AS distinct_shifts,
            MIN(event_time)                       AS earliest_event,
            MAX(event_time)                       AS latest_event
        FROM base
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    summary = rows[0] if rows else {}

    # Fetch top reason in the same window
    top_sql = f"""
        SELECT TOP 1 DR.ReasonName AS LossReason,
               SUM(ISNULL(FRMLB.PrimaryQuantityLoss, FRMLB.SecondaryQuantityLoss)) AS Qty
        FROM dbo.FactResourceMaterialLossBonus FRMLB
        INNER JOIN DataSets.V_DimArea     DA   ON DA.AreaKey     = FRMLB.AreaKey
        INNER JOIN DataSets.V_DimFacility DF   ON DF.FacilityKey = DA.FacilityKey
        INNER JOIN DataSets.V_DimStep     DS   ON DS.StepKey     = FRMLB.StepKey
        INNER JOIN DataSets.V_DimShift    DSFT ON DSFT.ShiftKey  = FRMLB.ShiftKey
        INNER JOIN DataSets.V_DimReason   DR   ON DR.ReasonKey   = FRMLB.LossReasonKey
        INNER JOIN DataSets.V_DimProduct  DP   ON DP.ProductKey  = FRMLB.ProductKey
        INNER JOIN DataSets.V_DimMaterial DM   ON DM.MaterialKey = FRMLB.MaterialKey
        {where_sql}
        GROUP BY DR.ReasonName
        ORDER BY Qty DESC
    """
    top_rows = _query(top_sql, tuple(params), db=DB_NAME_DWH)
    top_reason = top_rows[0] if top_rows else {}

    return json.dumps({
        "filter": {
            "date_start": date_start, "date_end": date_end,
            "facility": facility, "area": area, "step": step, "shift": shift, "product": product,
        },
        "total_events": summary.get("total_events"),
        "total_quantity_lost": str(summary.get("total_quantity_lost") or ""),
        "distinct_reasons": summary.get("distinct_reasons"),
        "distinct_steps": summary.get("distinct_steps"),
        "distinct_shifts": summary.get("distinct_shifts"),
        "earliest_event": summary.get("earliest_event"),
        "latest_event": summary.get("latest_event"),
        "top_reason": top_reason.get("LossReason"),
        "top_reason_quantity": str(top_reason.get("Qty") or ""),
        "data_source": "Athena 'Material Losses DWH' SQL — single-row aggregate",
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 7: GET YIELD LOSS BREAKDOWN
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_yield_loss_breakdown(
    date_start: str | None = None,
    date_end: str | None = None,
    facility: str | None = None,
    area: str | None = None,
    step: str | None = None,
    shift: str | None = None,
    product: str | None = None,
) -> str:
    """YIELD-LOSS LIFECYCLE BREAKDOWN — backed by Athena's parameterized 'Material
    Yield Loss DWH' Power-BI dataset (CM MES Report Dataset.docx, lines 2824-3340).
    Decomposes loss into the four lifecycle states CMF tracks per material:
      • Queued / Dispatched / InProcess / Processed.
    The "Processed" loss is what shipped scrap; the others reflect WIP loss.

    Use when the user asks:
      • "yield loss / yield loss breakdown"
      • "loss by lifecycle state / when in the flow are we losing material"
      • "WIP loss vs processed loss"
    """
    logger.info(f"get_yield_loss_breakdown: window={date_start}..{date_end}")

    # Athena's "Material Yield Loss DWH" lifecycle-bucket columns
    # (Queued/Dispatched/InProcess/Processed PrimaryQuantityLoss) live on
    # dbo.V_FactMaterialMovement (FMM), NOT on FactResourceMaterialLossBonus.
    # Athena's section builds an ELIGIBLE_MATERIALS CTE off FMM (line 3169 of
    # CM MES Report Dataset.docx) — we hit that view directly with the same
    # facility/area/step/product filters parameterized below.
    clauses: list[str] = []
    fmm_params: list = []
    if date_start:
        # FMM uses UTCQueuedDatetime / UTCProcessedDatetime — gate on the
        # widest available timestamp so all four buckets are covered.
        clauses.append("(FMM.UTCQueuedDatetime >= %s OR FMM.UTCProcessedDatetime >= %s)")
        fmm_params.extend([date_start, date_start])
    if date_end:
        clauses.append("(FMM.UTCQueuedDatetime <= %s OR FMM.UTCProcessedDatetime <= %s)")
        fmm_params.extend([date_end, date_end])
    if facility:
        clauses.append("DF.FacilityName = %s")
        fmm_params.append(facility)
    if area:
        clauses.append("DA.AreaName = %s")
        fmm_params.append(area)
    if step:
        clauses.append("DS.StepName = %s")
        fmm_params.append(step)
    if product:
        clauses.append("DP.ProductName = %s")
        fmm_params.append(product)
    where_fmm = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    # QuantityIn (denominator for Yield Loss %) = SUM(QueuedPrimaryQuantity)
    # per Athena's Material Yield Loss CTE (line 2663 of CM MES Report Dataset.docx,
    # `EM.QueuedPrimaryQuantity + ISNULL(EM.QueuedSubMaterialsPrimaryQuantity, 0)`).
    # We use the base column on FMM and ignore SubMaterials in v1 — covers ~100%
    # of pilot data which is single-material lots.
    sql = f"""
        SELECT
            ISNULL(SUM(FMM.QueuedPrimaryQuantityLoss), 0)     AS queued_primary,
            ISNULL(SUM(FMM.DispatchedPrimaryQuantityLoss), 0) AS dispatched_primary,
            ISNULL(SUM(FMM.InProcessPrimaryQuantityLoss), 0)  AS inprocess_primary,
            ISNULL(SUM(FMM.ProcessedPrimaryQuantityLoss), 0)  AS processed_primary,
            ISNULL(SUM(FMM.QueuedPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.DispatchedPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.InProcessPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.ProcessedPrimaryQuantityLoss), 0)  AS overall_primary_loss,
            ISNULL(SUM(FMM.InProcessPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.ProcessedPrimaryQuantityLoss), 0)  AS processed_primary_loss_only,
            ISNULL(SUM(FMM.QueuedSecondaryQuantityLoss), 0)     AS queued_secondary,
            ISNULL(SUM(FMM.DispatchedSecondaryQuantityLoss), 0) AS dispatched_secondary,
            ISNULL(SUM(FMM.InProcessSecondaryQuantityLoss), 0)  AS inprocess_secondary,
            ISNULL(SUM(FMM.ProcessedSecondaryQuantityLoss), 0)  AS processed_secondary,
            ISNULL(SUM(FMM.QueuedSecondaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.DispatchedSecondaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.InProcessSecondaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.ProcessedSecondaryQuantityLoss), 0) AS overall_secondary_loss,
            ISNULL(SUM(FMM.QueuedPrimaryQuantity), 0)   AS quantity_in_primary,
            ISNULL(SUM(FMM.QueuedSecondaryQuantity), 0) AS quantity_in_secondary,
            COUNT(*) AS event_count
        FROM dbo.V_FactMaterialMovement FMM
        INNER JOIN DataSets.V_DimArea     DA ON DA.AreaKey     = FMM.AreaKey
        INNER JOIN DataSets.V_DimFacility DF ON DF.FacilityKey = DA.FacilityKey
        INNER JOIN DataSets.V_DimStep     DS ON DS.StepKey     = FMM.StepKey
        INNER JOIN DataSets.V_DimMaterial DM ON DM.MaterialKey = FMM.MaterialKey
        INNER JOIN DataSets.V_DimProduct  DP ON DP.ProductKey  = FMM.ProductKey
        {where_fmm}
    """
    params = fmm_params
    where_sql = where_fmm  # reused below for symmetry; not strictly needed
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    r = rows[0] if rows else {}

    # Yield Loss % = OverallLoss / QuantityIn × 100   (slide 15 of CM MES Reports.pptx)
    def _pct(num, den):
        try:
            n = float(num or 0); d = float(den or 0)
            return round((n / d) * 100.0, 4) if d else 0.0
        except (ValueError, TypeError):
            return 0.0

    qty_in_p = r.get("quantity_in_primary") or 0
    qty_in_s = r.get("quantity_in_secondary") or 0
    overall_p = r.get("overall_primary_loss") or 0
    overall_s = r.get("overall_secondary_loss") or 0
    proc_only_p = r.get("processed_primary_loss_only") or 0

    return json.dumps({
        "filter": {
            "date_start": date_start, "date_end": date_end,
            "facility": facility, "area": area, "step": step, "shift": shift, "product": product,
        },
        "event_count": r.get("event_count"),
        "primary": {
            "queued":     str(r.get("queued_primary") or "0"),
            "dispatched": str(r.get("dispatched_primary") or "0"),
            "inprocess":  str(r.get("inprocess_primary") or "0"),
            "processed":  str(r.get("processed_primary") or "0"),
            "overall_loss": str(r.get("overall_primary_loss") or "0"),
            "processed_only": str(r.get("processed_primary_loss_only") or "0"),
            "quantity_in":   str(qty_in_p),
            "yield_loss_pct":          _pct(overall_p, qty_in_p),
            "processed_yield_loss_pct": _pct(proc_only_p, qty_in_p),
        },
        "secondary": {
            "queued":     str(r.get("queued_secondary") or "0"),
            "dispatched": str(r.get("dispatched_secondary") or "0"),
            "inprocess":  str(r.get("inprocess_secondary") or "0"),
            "processed":  str(r.get("processed_secondary") or "0"),
            "overall_loss": str(r.get("overall_secondary_loss") or "0"),
            "quantity_in":   str(qty_in_s),
            "yield_loss_pct": _pct(overall_s, qty_in_s),
        },
        "data_source": (
            "dbo.V_FactMaterialMovement (Athena's 'Material Yield Loss DWH' source view). "
            "Lifecycle buckets: Queued/Dispatched/InProcess/Processed PrimaryQuantityLoss + "
            "SecondaryQuantityLoss. yield_loss_pct = OverallLoss / SUM(QueuedPrimaryQuantity) × 100 "
            "— mirrors slide 15's 'Yield Loss %' column."
        ),
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 8: GET TOP LOSS STEPS  (process-step Pareto drill-down)
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_top_loss_steps(
    date_start: str | None = None,
    date_end: str | None = None,
    facility: str | None = None,
    product: str | None = None,
    loss_reason: str | None = None,
    step_type: str | None = None,
    top_n: int = 20,
) -> str:
    """TOP-N PROCESS STEPS by total loss quantity. Companion to get_scrap_pareto:
    once you know the top reasons, this tells you which steps in the flow they
    happen at. Same Athena join graph; grouped by StepName.

    Use when the user asks:
      • "which step has the most scrap / losses"
      • "where in the process is the scrap happening"
      • "top loss steps for product X / reason Y"

    Args:
        date_start / date_end / facility / product / loss_reason: filters
        top_n: keep top-N steps (default 20, max 100)
    """
    logger.info(f"get_top_loss_steps: window={date_start}..{date_end}, top_n={top_n}")
    top_n = min(max(1, int(top_n)), 100)
    where_sql, params = _build_loss_where(
        date_start, date_end, facility, None, None, None,
        product, None, None, loss_reason, step_type,
    )
    sql = f"""
        SELECT TOP ({top_n})
               DS.StepName                    AS step_name,
               COUNT(*)                       AS event_count,
               SUM(ISNULL(FRMLB.PrimaryQuantityLoss, FRMLB.SecondaryQuantityLoss)) AS total_quantity_lost
        FROM dbo.FactResourceMaterialLossBonus FRMLB
        INNER JOIN DataSets.V_DimArea     DA   ON DA.AreaKey     = FRMLB.AreaKey
        INNER JOIN DataSets.V_DimFacility DF   ON DF.FacilityKey = DA.FacilityKey
        INNER JOIN DataSets.V_DimStep     DS   ON DS.StepKey     = FRMLB.StepKey
        INNER JOIN DataSets.V_DimShift    DSFT ON DSFT.ShiftKey  = FRMLB.ShiftKey
        INNER JOIN DataSets.V_DimReason   DR   ON DR.ReasonKey   = FRMLB.LossReasonKey
        INNER JOIN DataSets.V_DimProduct  DP   ON DP.ProductKey  = FRMLB.ProductKey
        INNER JOIN DataSets.V_DimMaterial DM   ON DM.MaterialKey = FRMLB.MaterialKey
        {where_sql}
        GROUP BY DS.StepName
        ORDER BY total_quantity_lost DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)
    out = []
    for i, r in enumerate(rows, start=1):
        out.append({
            "rank": i,
            "step_name": r.get("step_name"),
            "event_count": r.get("event_count"),
            "total_quantity_lost": str(r.get("total_quantity_lost") or ""),
        })
    return json.dumps({
        "filter": {
            "date_start": date_start, "date_end": date_end,
            "facility": facility, "product": product, "loss_reason": loss_reason,
        },
        "top_n": top_n,
        "step_count": len(out),
        "steps": out,
        "data_source": "Athena 'Material Losses DWH' SQL — grouped by DimStep.StepName, ranked desc",
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 9: GET VOLUME AND YIELD  (Slide 14 — Material Volume And Yield)
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_volume_yield(
    date_start: str | None = None,
    date_end: str | None = None,
    facility: str | None = None,
    area: str | None = None,
    step: str | None = None,
    product: str | None = None,
    step_type: str | None = None,
    group_by: str = "step",
    top_n: int = 50,
) -> str:
    """MATERIAL VOLUME AND YIELD — slide 14 of CM MES Reports.pptx. For each
    facility / area / step / product (controlled by group_by), returns:
      • Quantity In   = SUM(QueuedPrimaryQuantity)
      • Loss Quantity = SUM(Queued+Dispatched+InProcess+Processed PrimaryQuantityLoss)
      • Bonus Quantity = SUM(Queued+Dispatched+InProcess+Processed PrimaryQuantityBonus)
      • Quantity Out  = QuantityIn - LossQuantity + BonusQuantity
      • Yield (%)     = QuantityOut / QuantityIn × 100

    Mirrors Athena's 'Material Volume and Yield DWH' Power-BI dataset
    (CM MES Report Dataset.docx, lines 2252-2823) — same source view
    (dbo.V_FactMaterialMovement) and same lifecycle-bucket aggregation.

    Use when the user asks ANY of:
      • "material volume and yield / volume yield report"
      • "yield % / yield percentage / production yield"
      • "quantity in vs quantity out / yield by step / yield by product"

    Args:
        date_start / date_end: ISO window
        facility / area / step / product / step_type: dimension filters
        group_by: 'step' (default), 'product', 'facility', 'area'
        top_n: keep top-N rows (default 50, max 200)
    """
    logger.info(f"get_volume_yield: group_by={group_by}, window={date_start}..{date_end}")
    top_n = min(max(1, int(top_n)), 200)
    group_map = {
        "step":     ("DS.StepName",     "step"),
        "product":  ("DP.ProductName",  "product"),
        "facility": ("DF.FacilityName", "facility"),
        "area":     ("DA.AreaName",     "area"),
    }
    if group_by not in group_map:
        return json.dumps({"status": "error",
                           "message": f"Unsupported group_by={group_by}. Use one of: {list(group_map)}"})
    group_col, group_key = group_map[group_by]

    clauses: list[str] = []
    params: list = []
    if date_start:
        clauses.append("(FMM.UTCQueuedDatetime >= %s OR FMM.UTCProcessedDatetime >= %s)")
        params.extend([date_start, date_start])
    if date_end:
        clauses.append("(FMM.UTCQueuedDatetime <= %s OR FMM.UTCProcessedDatetime <= %s)")
        params.extend([date_end, date_end])
    if facility:
        clauses.append("DF.FacilityName = %s"); params.append(facility)
    if area:
        clauses.append("DA.AreaName = %s"); params.append(area)
    if step:
        clauses.append("DS.StepName = %s"); params.append(step)
    if product:
        clauses.append("DP.ProductName = %s"); params.append(product)
    if step_type:
        clauses.append("DS.StepType = %s"); params.append(step_type)
    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT TOP ({top_n})
            {group_col} AS group_value,
            ISNULL(SUM(FMM.QueuedPrimaryQuantity), 0)         AS quantity_in,
            ISNULL(SUM(FMM.QueuedPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.DispatchedPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.InProcessPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.ProcessedPrimaryQuantityLoss), 0) AS loss_quantity,
            ISNULL(SUM(FMM.QueuedPrimaryQuantityBonus), 0)
              + ISNULL(SUM(FMM.DispatchedPrimaryQuantityBonus), 0)
              + ISNULL(SUM(FMM.InProcessPrimaryQuantityBonus), 0)
              + ISNULL(SUM(FMM.ProcessedPrimaryQuantityBonus), 0) AS bonus_quantity,
            ISNULL(SUM(FMM.InProcessPrimaryQuantityLoss), 0)
              + ISNULL(SUM(FMM.ProcessedPrimaryQuantityLoss), 0)  AS processed_loss_quantity,
            COUNT(*) AS event_count
        FROM dbo.V_FactMaterialMovement FMM
        INNER JOIN DataSets.V_DimArea     DA ON DA.AreaKey     = FMM.AreaKey
        INNER JOIN DataSets.V_DimFacility DF ON DF.FacilityKey = DA.FacilityKey
        INNER JOIN DataSets.V_DimStep     DS ON DS.StepKey     = FMM.StepKey
        INNER JOIN DataSets.V_DimMaterial DM ON DM.MaterialKey = FMM.MaterialKey
        INNER JOIN DataSets.V_DimProduct  DP ON DP.ProductKey  = FMM.ProductKey
        {where_sql}
        GROUP BY {group_col}
        ORDER BY ISNULL(SUM(FMM.QueuedPrimaryQuantity), 0) DESC
    """
    rows = _query(sql, tuple(params), db=DB_NAME_DWH)

    out: list[dict] = []
    for r in rows:
        try:
            qty_in = float(r.get("quantity_in") or 0)
            loss = float(r.get("loss_quantity") or 0)
            bonus = float(r.get("bonus_quantity") or 0)
        except (ValueError, TypeError):
            qty_in = loss = bonus = 0.0
        qty_out = qty_in - loss + bonus
        yield_pct = round((qty_out / qty_in * 100.0), 4) if qty_in else 0.0
        loss_pct = round((loss / qty_in * 100.0), 4) if qty_in else 0.0
        out.append({
            group_key: r.get("group_value"),
            "quantity_in":     str(r.get("quantity_in") or "0"),
            "quantity_out":    str(qty_out),
            "loss_quantity":   str(r.get("loss_quantity") or "0"),
            "bonus_quantity":  str(r.get("bonus_quantity") or "0"),
            "processed_loss_quantity": str(r.get("processed_loss_quantity") or "0"),
            "yield_pct":       yield_pct,
            "yield_loss_pct":  loss_pct,
            "event_count":     r.get("event_count"),
        })

    return json.dumps({
        "filter": {
            "date_start": date_start, "date_end": date_end,
            "facility": facility, "area": area, "step": step,
            "product": product, "step_type": step_type,
        },
        "group_by": group_by,
        "row_count": len(out),
        "rows": out,
        "data_source": (
            "dbo.V_FactMaterialMovement (Athena's 'Material Volume and Yield DWH' source). "
            "Quantity In = SUM(QueuedPrimaryQuantity); Quantity Out = In - Loss + Bonus; "
            "Yield % = Out / In × 100 (slide 14 of CM MES Reports.pptx)."
        ),
    }, default=str)


# ─── Lambda entrypoint ─── #
def lambda_handler(event, context):
    """Entry point — MCPLambdaHandler dispatches JSON-RPC MCP requests to the tools above."""
    return mcp.handle_request(event, context)
