"""
AWS Lambda MCP Server for ATHENA AI - Entegris KSP UC-1 Material Genealogy
Queries on-premises SQL Server database via Site-to-Site VPN

Database:
  - EntegrisKSPUpgradeDWH (CMF SQL Server, address+credentials supplied via env vars):
      Entegris KSP Data Warehouse — material genealogy, assemblies, merges,
      BOM references, process steps, products.

Primary table:
  - Staging.T_MaterialGenealogy — one row per Assemble or Merge operation.
    Columns:  AscMaterialName (component consumed)
             DescMaterialName (product built)
             AscStep / DescStep, AscProduct / DescProduct, DescBOM,
             AscPQ / AscAssembleQ / DescPQ, OperationEndTime, Operation.

Tools (13 total):
  1.  get_info                     — system info, capabilities, available tools
  2.  get_source_info              — data provenance: DB, server, row counts, time window
  3.  search_materials             — find materials by substring (ascendant or descendant)
  4.  get_material_summary         — usage stats: component count, product count, product code
  5.  get_material_ancestors       — materials consumed/assembled to build this material
  6.  get_material_descendants     — materials built FROM this material
  7.  get_material_genealogy       — full tree (ancestors + descendants) recursive walk
  8.  get_batch_overview           — batch-level aggregate stats for a product/batch pattern
  9.  list_hold_reasons            — master list of Hold/Loss/Defect/Rework reason codes
  10. get_material_hold_status     — current hold flag, hold count, disposition for a material
  11. get_material_hold_history    — historical hold events (timeline of holds + releases)
  12. get_material_lifecycle_status — current lifecycle state per problem statement
                                       (Processed / Consumed / Terminated / In-Process / On-Hold)
  13. get_material_edhr            — Electronic Device History Report — full chronological
                                       event log (TrackIn/Out, MoveToNext, Assemble, Hold, Terminate)

Safety: READ-ONLY access only. SQL keyword blocker enforced.
Note: WRITE actions (place hold, release, change disposition, scrap) are NOT
exposed — those require Athena's Hold & Release service + e-signature workflow.
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
    name="ATHENA AI - Entegris KSP UC-1 Genealogy MCP Server",
    version="1.0.0",
)

# ─── Config (passed via Lambda env vars — see .env.example at repo root) ─── #
DB_SERVER = os.environ["DB_SERVER"]
DB_PORT = int(os.getenv("DB_PORT", "1433"))
DB_INSTANCE = os.getenv("DB_INSTANCE", "ONLINE")  # CMF SQL named instance
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME_DWH = os.getenv("DB_NAME_DWH", "EntegrisKSPUpgradeDWH")
DB_NAME_OLTP = os.getenv("DB_NAME_OLTP", "EntegrisKSPUpgrade")

# Genealogy now sourced from the ODS view used by Athena's
# Reports.P_GetMaterialGenealogy_RP_MultiParam_ODS — same 240 rows but with
# 12 OLTP-only enrichments (ResourceName, ProductDescription, AscPQAfter,
# BOMRevision/Id, Units, etc.).
DB_NAME_ODS = os.getenv("DB_NAME_ODS", "EntegrisKSPUpgradeODS")
GENEALOGY_TABLE = os.getenv("GENEALOGY_TABLE", "CoreDataModel.V_MaterialGenealogy")

# Hold / Reason tables remain on the DWH
REASON_TABLE_DWH = os.getenv("REASON_TABLE_DWH", "dbo.DimReason")
HOLD_HISTORY_TABLE_DWH = os.getenv("HOLD_HISTORY_TABLE_DWH", "Staging.T_MaterialHoldReasonHistory")
FACT_MATERIAL_TABLE = os.getenv("FACT_MATERIAL_TABLE", "dbo.FactMaterial")
DIM_MATERIAL_TABLE = os.getenv("DIM_MATERIAL_TABLE", "dbo.DimMaterial")

ROW_LIMIT = int(os.getenv("ROW_LIMIT", "1000"))

# ─── SQL Safety ─── #
_BLOCKED = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|MERGE|GRANT|REVOKE|DENY)\b",
    re.IGNORECASE,
)
# EXEC removed from blocker — we now invoke Athena's read-only Reports.* SPs.
# A whitelist below restricts which SPs we'll actually call.
_ALLOWED_SPS = {
    "Reports.P_GetMaterialGenealogy_RP_MultiParam_ODS",
    "Reports.P_GetMaterialHistory_RP_MultiParam_ODS",   # backs lifecycle_status + edhr
    "Reports.P_GetResourceHistory_RP_ODS",
    "Reports.P_GetResourceMaintenanceHistory_RP_ODS",
    "Reports.P_GetProtocolInstanceHistory_RP_ODS",
}

# ─── User-friendly error messages ─── #
_ERROR_MESSAGES = {
    "connection": "The Entegris KSP database is temporarily unreachable. Please try again in a moment.",
    "timeout": "The query took too long to complete. Please narrow the search or try again shortly.",
    "permission": "This operation is not permitted. The system only allows read-only data access.",
    "query": "Unable to retrieve the requested data. Please check the input parameters and try again.",
    "unknown": "An unexpected issue occurred while processing your request. Please try again.",
    "not_found": "No data found matching your request. Please verify the material name and try again.",
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


def _validate_sp(sp_name: str) -> None:
    if sp_name not in _ALLOWED_SPS:
        raise ToolError(_ERROR_MESSAGES["permission"], f"Blocked SP: {sp_name}")


def _exec_sp(sp_name: str, params: dict, db: str | None = None) -> list[dict]:
    """Execute a whitelisted Reports.* stored procedure with named params.
    The SP itself is read-only; it returns rows we forward to the caller.
    """
    _validate_sp(sp_name)
    target_db = db or DB_NAME_ODS
    try:
        conn = _get_conn(target_db)
        try:
            cur = conn.cursor(as_dict=True)
            placeholder = ", ".join(f"@{k}=%s" for k in params.keys())
            cur.execute(f"EXEC {sp_name} {placeholder}", tuple(params.values()))
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
        logger.error(f"SP error ({sp_name}): {e}")
        raise ToolError(_ERROR_MESSAGES["query"], str(e))


def _sql_literal(value: str) -> str:
    """Escape a string value for safe inline use (fallback; prefer parameterized)."""
    return "'" + str(value).replace("'", "''") + "'"


# ─── DB helpers ─── #
def _get_conn(db: str | None = None):
    """Connect to CMF SQL Server. Tries a ladder of connection variants:
      1. Named instance syntax (server\\INSTANCE, needs SQL Browser on UDP 1434)
      2. IP:port with default DB_PORT
      3. A short list of common CMF port assignments
    Returns on first success. Logs the winning variant for debugging.

    Default DB switched to ODS (was DWH) so genealogy tools land on
    CoreDataModel.V_MaterialGenealogy by default. Hold/Reason queries
    explicitly pass DB_NAME_DWH where needed.
    """
    target_db = db or DB_NAME_ODS
    last_error = None

    attempts: list[tuple[str, dict]] = []

    if DB_INSTANCE:
        attempts.append((
            f"named-instance {DB_SERVER}\\{DB_INSTANCE}",
            {"server": f"{DB_SERVER}\\{DB_INSTANCE}", "database": target_db,
             "user": DB_USERNAME, "password": DB_PASSWORD,
             "login_timeout": 10, "timeout": 60},
        ))

    # Explicit port attempts — try the env-var first, then common CMF ports
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
        conn = _get_conn(db or DB_NAME_ODS)
        try:
            cur = conn.cursor(as_dict=True)
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            rows = cur.fetchmany(ROW_LIMIT)
            # Convert datetime / Decimal values to strings for JSON
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


# ─── Projection used by genealogy tools ─── #
# Now sourced from CoreDataModel.V_MaterialGenealogy on the ODS — the same
# view used by Athena's Reports.P_GetMaterialGenealogy_RP_MultiParam_ODS.
_GENEALOGY_COLS = (
    "GenealogyId, Operation, OperationEndTime, "
    "AscMaterialId, AscMaterialName, AscStep, "
    "AscProduct, AscProductRevision, AscProductDescription, "
    "AscBOM, AscBOMRevision, "
    "AscPQ, AscPQAfter, AscAssembleQ, AscPrimaryUnits, "
    "AscResourceName, "
    "DescMaterialId, DescMaterialName, DescStep, "
    "DescProduct, DescProductRevision, DescProductDescription, "
    "DescBOM, DescBOMRevision, DescBOMId, "
    "DescPQ, DescAssembleQ, DescPrimaryUnits, "
    "DescResourceName, "
    "IsExplicitAssemble"
)

_GENEALOGY_FROM = f" FROM {GENEALOGY_TABLE} "


def _edge_to_dict(row: dict, depth: int, direction: str) -> dict:
    return {
        "genealogy_id": row.get("GenealogyId"),
        "operation": row.get("Operation", ""),
        "operation_end_time": row.get("OperationEndTime", ""),
        "ascendant": {
            "id": row.get("AscMaterialId"),
            "name": row.get("AscMaterialName", ""),
            "product": row.get("AscProduct", ""),
            "product_revision": row.get("AscProductRevision") or "",
            "product_description": row.get("AscProductDescription") or "",
            "step": row.get("AscStep", ""),
            "bom_revision": row.get("AscBOMRevision") or "",
            "primary_units": row.get("AscPrimaryUnits") or "",
            "primary_quantity_after": str(row.get("AscPQAfter") or ""),
            "resource_name": row.get("AscResourceName") or "",
        },
        "descendant": {
            "id": row.get("DescMaterialId"),
            "name": row.get("DescMaterialName", ""),
            "product": row.get("DescProduct", ""),
            "product_revision": row.get("DescProductRevision") or "",
            "product_description": row.get("DescProductDescription") or "",
            "step": row.get("DescStep", ""),
            "bom": row.get("DescBOM"),
            "bom_revision": row.get("DescBOMRevision") or "",
            "bom_id": row.get("DescBOMId") or 0,
            "primary_units": row.get("DescPrimaryUnits") or "",
            "resource_name": row.get("DescResourceName") or "",
        },
        "quantity_consumed": row.get("AscAssembleQ") or row.get("AscPQ"),
        "is_explicit_assemble": row.get("IsExplicitAssemble"),
        "depth": depth,
        "direction": direction,
    }


def _walk(start: str, direction: str, max_depth: int = 5) -> list[dict]:
    """Genealogy walk via Athena's parameterized SP.

    Calls Reports.P_GetMaterialGenealogy_RP_MultiParam_ODS directly with
    @GenealogyMode = 0 (Ascendant) or 1 (Descendant) and @Depth set by caller.
    The SP itself handles the recursion server-side.
    """
    mode = 0 if direction == "ancestor" else 1
    rows = _exec_sp(
        "Reports.P_GetMaterialGenealogy_RP_MultiParam_ODS",
        {
            "Material": start,
            "MatId": 0,                  # 0 lets SP resolve from name
            "Facilities": "",            # no facility filter
            "NFacTotalElems": 0,
            "Steps": "",                 # no step filter
            "NStepTotalElems": 0,
            "DateStart": None,           # SP defaults to GETDATE()-200
            "DateEnd": None,             # SP defaults to GETDATE()
            "Depth": int(max_depth),
            "GenealogyMode": mode,
            "UserAccount": None,         # security context skipped (DB user is dbo)
        },
        db=DB_NAME_ODS,
    )

    edges: list[dict] = []
    for row in rows:
        depth_value = row.get("Level") or 1
        edges.append(_edge_to_dict(row, int(depth_value), direction))
    return edges


# ═══════════════════════════════════════════════════════════════
# TOOL 1: GET INFO
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_info() -> str:
    """Get comprehensive information about the Entegris KSP UC-1 Genealogy system — what
    it does, which database it queries, what questions it can answer, and what tools
    are available.

    Use this tool FIRST when:
    - The user asks "what can you do?" or "what is this system?"
    - You need to determine which genealogy tool to use
    - You need to orient yourself before answering a question
    """
    logger.info("get_info called")
    return json.dumps({
        "system": "Entegris KSP UC-1 Material Genealogy",
        "description": (
            "Semiconductor filter material genealogy system for Entegris KSP production. "
            "Traces assembly and merge operations logged in the CMF (Critical Manufacturing) "
            "data warehouse. Given a material/lot ID, returns ancestors (components consumed) "
            "and descendants (products built from it) for defect tracing, BOM verification, "
            "hold/disposition decision support, and regulatory traceability."
        ),
        "capabilities": [
            "Material Search — find a material by name substring",
            "Summary — quick usage stats for any material",
            "Ancestors — recursively trace what was consumed to build a given lot",
            "Descendants — recursively trace what was built from a given lot",
            "Full Genealogy — both directions in one call",
            "Batch Overview — aggregate stats across all lots in a batch or product",
            "Source Verification — prove provenance with live DB + row counts",
            "Hold Reason Catalog — list all Hold/Loss/Defect/Rework reason codes",
            "Material Hold Status — check if a material is currently on hold",
            "Hold History — timeline of past hold events for a material",
            "Lifecycle Status — current state per problem statement (Processed/Consumed/Terminated/In-Process)",
            "EDHR — Electronic Device History Report (full chronological event log)",
        ],
        "databases": [f"{DB_NAME_DWH} (Entegris KSP Data Warehouse)"],
        "primary_table": GENEALOGY_TABLE,
        "supporting_tables": [
            REASON_TABLE_DWH, HOLD_HISTORY_TABLE_DWH, FACT_MATERIAL_TABLE, DIM_MATERIAL_TABLE,
        ],
        "data_model": (
            "Each genealogy row = one Assemble or Merge operation. "
            "AscMaterial (consumed) → DescMaterial (built). "
            "Material naming convention: product|batch|batch lot_number "
            "(e.g., 'S4442R031Y23|K9B381421|K9B381421 6'). "
            "Hold state lives on FactMaterial.IsOnHold; reason codes in DimReason."
        ),
        "total_tools": 13,
        "write_actions_status": (
            "READ-ONLY deployment. Placing a material on hold, changing disposition, "
            "or scrapping requires Athena's Hold & Release service with e-signature "
            "workflow — not exposed by this server."
        ),
        "safety": "READ-ONLY access. No data modifications allowed.",
        "accuracy_rules": [
            "Report exact numbers from tool results — never estimate or round",
            "Only report data returned by the current tool call — do not infer from names",
            "If a tool returns 0 rows, say so explicitly — do not fabricate genealogy",
            "Always cite source: table name, row count, query time",
        ],
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 2: GET SOURCE INFO
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_source_info() -> str:
    """Return metadata about the genealogy data source: database name, SQL server name,
    total row count, distinct counts, operation types, and time window of data.
    Use to verify provenance when a user doubts accuracy, or to establish scope before analysis.
    """
    logger.info("get_source_info called")
    rows = _query(f"""
        SELECT
          DB_NAME() AS database_name,
          @@SERVERNAME AS sql_server_name,
          (SELECT COUNT(*) FROM {GENEALOGY_TABLE}) AS total_genealogy_rows,
          (SELECT COUNT(DISTINCT AscMaterialName) FROM {GENEALOGY_TABLE}) AS distinct_ascendants,
          (SELECT COUNT(DISTINCT DescMaterialName) FROM {GENEALOGY_TABLE}) AS distinct_descendants,
          (SELECT COUNT(DISTINCT AscProduct) FROM {GENEALOGY_TABLE}) AS distinct_asc_products,
          (SELECT COUNT(DISTINCT DescProduct) FROM {GENEALOGY_TABLE}) AS distinct_desc_products,
          (SELECT COUNT(DISTINCT Operation) FROM {GENEALOGY_TABLE}) AS operation_types,
          (SELECT MIN(OperationEndTime) FROM {GENEALOGY_TABLE}) AS earliest_event,
          (SELECT MAX(OperationEndTime) FROM {GENEALOGY_TABLE}) AS latest_event
    """, db=DB_NAME_ODS)
    info = rows[0] if rows else {}
    info["table"] = GENEALOGY_TABLE
    return json.dumps(info, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 3: SEARCH MATERIALS
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def search_materials(pattern: str, limit: int = 30) -> str:
    """Find materials whose name contains the given substring (case-insensitive).

    Returns materials that appear as either an ascendant (consumed) or a descendant (built).
    Always returns `total_match_count` (true total in DB) AND `match_count` (rows in
    payload). If `truncated` is true, re-call with limit=500 to get the full set.

    Args:
        pattern: Substring to search for (e.g., 'S4442', 'K9B381421', 'BOM_Material')
        limit: Max results to return (default 30, max 500)
    """
    logger.info(f"search_materials: pattern={pattern!r}, limit={limit}")
    if not pattern:
        return json.dumps({"status": "error", "message": "pattern is required"})

    limit = min(max(1, int(limit)), 500)
    rows = _query(f"""
        SELECT DISTINCT Name, Product, Role FROM (
            SELECT AscMaterialName AS Name, AscProduct AS Product, 'ascendant' AS Role
            FROM {GENEALOGY_TABLE} WHERE AscMaterialName LIKE %s
            UNION
            SELECT DescMaterialName AS Name, DescProduct AS Product, 'descendant' AS Role
            FROM {GENEALOGY_TABLE} WHERE DescMaterialName LIKE %s
        ) x
        ORDER BY Name
    """, (f"%{pattern}%", f"%{pattern}%"))

    total = len(rows)
    rows = rows[:limit]
    return json.dumps({
        "pattern": pattern,
        "limit": limit,
        "match_count": len(rows),
        "total_match_count": total,
        "truncated": total > len(rows),
        "matches": rows,
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 4: GET MATERIAL SUMMARY
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_material_summary(material_name: str) -> str:
    """Lightweight usage counts for a material: times used as component, times built as
    product, product code in each role. ONLY for "is this used as a component or built
    as a product" questions.

    DO NOT use this tool to answer:
      • "current state / status / lifecycle / what step / what resource"  → use get_material_lifecycle_status
      • "device history / EDHR / eDHR / DHR / event log / audit trail"   → use get_material_edhr

    Args:
        material_name: Exact material name (e.g., 'S4442R031Y23|K9B381421|K9B381421 6')
    """
    logger.info(f"get_material_summary: {material_name!r}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})

    rows = _query(f"""
        SELECT
          (SELECT COUNT(*) FROM {GENEALOGY_TABLE} WHERE AscMaterialName = %s)  AS used_as_component_count,
          (SELECT COUNT(*) FROM {GENEALOGY_TABLE} WHERE DescMaterialName = %s) AS built_as_product_count,
          (SELECT TOP 1 AscProduct  FROM {GENEALOGY_TABLE} WHERE AscMaterialName  = %s) AS product_as_ascendant,
          (SELECT TOP 1 DescProduct FROM {GENEALOGY_TABLE} WHERE DescMaterialName = %s) AS product_as_descendant
    """, (material_name, material_name, material_name, material_name), db=DB_NAME_ODS)

    summary = rows[0] if rows else {}
    return json.dumps({
        "material_name": material_name,
        "summary": summary,
        "_routing_hint": (
            "This tool only returns assembly counts. For CURRENT lifecycle state "
            "(what step/resource/status), call get_material_lifecycle_status. For "
            "the chronological event log / EDHR / device history, call get_material_edhr."
        ),
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 5: GET MATERIAL ANCESTORS
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_material_ancestors(material_name: str, recursive: bool = True, max_depth: int = 5) -> str:
    """Return materials consumed/assembled to build the given material.
    Walks the genealogy graph upward (toward raw materials).

    Args:
        material_name: Exact material name (e.g., 'S4442R031Y23|K9B381421|K9B381421 6')
        recursive: If True, walk up multiple levels. If False, direct parents only.
        max_depth: Recursion limit (default 5; only used when recursive=True)
    """
    logger.info(f"get_material_ancestors: {material_name!r} recursive={recursive} depth={max_depth}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})

    if recursive:
        edges = _walk(material_name, direction="ancestor", max_depth=int(max_depth))
    else:
        rows = _query(
            f"SELECT {_GENEALOGY_COLS} {_GENEALOGY_FROM} WHERE DescMaterialName = %s",
            (material_name,),
        )
        edges = [_edge_to_dict(r, 1, "ancestor") for r in rows]

    unique = sorted({e["ascendant"]["name"] for e in edges if e["ascendant"]["name"]})
    return json.dumps({
        "material_name": material_name,
        "recursive": recursive,
        "max_depth": max_depth,
        "ancestor_edge_count": len(edges),
        "unique_ancestors": unique,
        "ancestors": edges,
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 6: GET MATERIAL DESCENDANTS
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_material_descendants(material_name: str, recursive: bool = True, max_depth: int = 5) -> str:
    """Return materials built FROM the given material.
    Walks the genealogy graph downward (toward finished products).

    Args:
        material_name: Exact material name (e.g., 'BOM_Material')
        recursive: If True, walk down multiple levels. If False, direct children only.
        max_depth: Recursion limit (default 5; only used when recursive=True)
    """
    logger.info(f"get_material_descendants: {material_name!r} recursive={recursive} depth={max_depth}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})

    if recursive:
        edges = _walk(material_name, direction="descendant", max_depth=int(max_depth))
    else:
        rows = _query(
            f"SELECT {_GENEALOGY_COLS} {_GENEALOGY_FROM} WHERE AscMaterialName = %s",
            (material_name,),
        )
        edges = [_edge_to_dict(r, 1, "descendant") for r in rows]

    unique = sorted({e["descendant"]["name"] for e in edges if e["descendant"]["name"]})
    return json.dumps({
        "material_name": material_name,
        "recursive": recursive,
        "max_depth": max_depth,
        "descendant_edge_count": len(edges),
        "unique_descendants": unique,
        "descendants": edges,
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 7: GET MATERIAL GENEALOGY (FULL TREE)
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_material_genealogy(
    material_name: str,
    include_ancestors: bool = True,
    include_descendants: bool = True,
    max_depth: int = 5,
) -> str:
    """Return the BOM-Assembly genealogy tree (ancestors + descendants) for a given
    material. ONLY returns Assemble/Merge events from T_MaterialGenealogy — typically
    2–5 edges per lot. Use ONLY for "trace the lot / what was it built from / what
    was built from it / what's the BOM lineage".

    DO NOT use this tool to answer:
      • "current state / status / lifecycle / what step / what resource"  → use get_material_lifecycle_status
      • "device history / EDHR / eDHR / DHR / event log / audit trail / all events" → use get_material_edhr
        (Genealogy contains 2–5 events; an EDHR contains 50–200+ events including
         TrackIn/TrackOut/Dispatch/MoveToNextStep — DO NOT fake an EDHR from this tool.)

    Args:
        material_name: Exact material name
        include_ancestors: Walk upward to raw materials (default True)
        include_descendants: Walk downward to finished products (default True)
        max_depth: Recursion limit (default 5)
    """
    logger.info(f"get_material_genealogy: {material_name!r}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})

    ancestors = _walk(material_name, "ancestor", int(max_depth)) if include_ancestors else []
    descendants = _walk(material_name, "descendant", int(max_depth)) if include_descendants else []

    unique_asc = sorted({e["ascendant"]["name"] for e in ancestors})
    unique_desc = sorted({e["descendant"]["name"] for e in descendants})
    max_d = max([e["depth"] for e in ancestors + descendants] or [0])

    return json.dumps({
        "target_material": material_name,
        "ancestor_edge_count": len(ancestors),
        "descendant_edge_count": len(descendants),
        "unique_ancestors": unique_asc,
        "unique_descendants": unique_desc,
        "max_depth_reached": max_d,
        "max_depth_limit": max_depth,
        "ancestors": ancestors,
        "descendants": descendants,
        "_routing_hint": (
            "This tool returned BOM-assembly edges only. For CURRENT lifecycle state "
            "(step / resource / processed-or-not), call get_material_lifecycle_status. "
            "For the FULL chronological event log (EDHR / device history with "
            "TrackIn/TrackOut/Dispatch events), call get_material_edhr — DO NOT "
            "synthesize an EDHR from these genealogy edges."
        ),
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 8: GET BATCH OVERVIEW
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_batch_overview(pattern: str) -> str:
    """For a batch or product identifier substring, return aggregate stats:
    number of lots, assembly events, unique components, process steps,
    time window, BOM reference, and primary product.

    Use this for batch-scope questions (e.g., 'tell me about K9B381421').

    Args:
        pattern: Batch / product substring (e.g., 'K9B381421' or 'S4442')
    """
    logger.info(f"get_batch_overview: {pattern!r}")
    if not pattern:
        return json.dumps({"status": "error", "message": "pattern is required"})

    p = f"%{pattern}%"
    rows = _query(f"""
        SELECT
          (SELECT COUNT(DISTINCT DescMaterialName) FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s) AS lots_in_batch,
          (SELECT COUNT(*) FROM {GENEALOGY_TABLE} WHERE DescMaterialName LIKE %s) AS assembly_events,
          (SELECT COUNT(DISTINCT AscMaterialName) FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s) AS unique_components,
          (SELECT COUNT(DISTINCT AscProduct) FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s) AS component_products,
          (SELECT COUNT(DISTINCT DescStep) FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s) AS process_steps,
          (SELECT MIN(OperationEndTime) FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s) AS batch_started,
          (SELECT MAX(OperationEndTime) FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s) AS batch_ended,
          (SELECT TOP 1 DescBOM FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s AND DescBOM IS NOT NULL) AS bom_reference,
          (SELECT TOP 1 DescProduct FROM {GENEALOGY_TABLE}
           WHERE DescMaterialName LIKE %s) AS primary_product
    """, tuple([p] * 9), db=DB_NAME_ODS)

    info = rows[0] if rows else {}
    info["pattern"] = pattern
    return json.dumps(info, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 9: LIST HOLD REASONS
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def list_hold_reasons(reason_type: str = "") -> str:
    """List master reason codes from DimReason. Use when the user asks 'what hold
    reasons are available?', 'show me the disposition codes', 'list defect codes', etc.

    Args:
        reason_type: Optional filter. One of 'Hold', 'Loss', 'Defect', 'Rework',
                     or '' (empty, the default) to get all types.
    """
    logger.info(f"list_hold_reasons: reason_type={reason_type!r}")

    if reason_type:
        sql = f"""
            SELECT ReasonKey, ReasonId, ReasonType, ReasonName
            FROM {REASON_TABLE_DWH}
            WHERE ReasonType = %s
            ORDER BY ReasonType, ReasonName
        """
        rows = _query(sql, (reason_type,), db=DB_NAME_DWH)
    else:
        sql = f"""
            SELECT ReasonKey, ReasonId, ReasonType, ReasonName
            FROM {REASON_TABLE_DWH}
            ORDER BY ReasonType, ReasonName
        """
        rows = _query(sql, db=DB_NAME_DWH)

    # Group by type for easier LLM consumption
    by_type: dict = {}
    for r in rows:
        t = r.get("ReasonType") or "Unknown"
        by_type.setdefault(t, []).append({
            "reason_key": r.get("ReasonKey"),
            "reason_id": r.get("ReasonId"),
            "reason_name": r.get("ReasonName"),
        })

    return json.dumps({
        "filter_reason_type": reason_type or "(all)",
        "total_reasons": len(rows),
        "types": sorted(by_type.keys()),
        "reasons_by_type": by_type,
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 10: GET MATERIAL HOLD STATUS
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_material_hold_status(material_name: str) -> str:
    """Check current hold status of a material — IsOnHold flag, HoldCount, product info.

    Use when the user asks "is this lot on hold?", "what's the hold status of X?",
    or needs to decide whether a downstream action is possible.

    Args:
        material_name: Exact material name (e.g., 'S4442R031Y23|K9B381421|K9B381421 6')
    """
    logger.info(f"get_material_hold_status: {material_name!r}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})

    # Use only columns verified to exist on FactMaterial + DimMaterial
    sql = f"""
        SELECT TOP 1
          m.MaterialName,
          m.MaterialId,
          m.Type,
          m.Form,
          m.OrderNumber AS OrderNumber_Dim,
          m.ProductionOrderKey AS ProductionOrderKey_Dim,
          fm.IsOnHold,
          fm.ParentMaterialKey
        FROM {FACT_MATERIAL_TABLE} fm
        JOIN {DIM_MATERIAL_TABLE} m ON fm.MaterialKey = m.MaterialKey
        WHERE m.MaterialName = %s
    """
    rows = _query(sql, (material_name,), db=DB_NAME_DWH)

    if not rows:
        # Try DimMaterial only (material may not have any FactMaterial row yet)
        rows_dim = _query(f"""
            SELECT TOP 1 MaterialName, MaterialId, Type, Form, OrderNumber, ProductionOrderKey
            FROM {DIM_MATERIAL_TABLE} WHERE MaterialName = %s
        """, (material_name,), db=DB_NAME_DWH)
        if not rows_dim:
            return json.dumps({
                "material_name": material_name,
                "found": False,
                "message": "Material not found in DimMaterial.",
            })
        d = rows_dim[0]
        # Stringify 19-digit IDs to survive JS Number precision in MCP clients.
        return json.dumps({
            "material_name": material_name,
            "found": True,
            "is_on_hold": False,
            "note": "Exists in DimMaterial but no FactMaterial row yet — IsOnHold not tracked.",
            "material_id": str(d.get("MaterialId")) if d.get("MaterialId") is not None else None,
            "type": d.get("Type"),
            "form": d.get("Form"),
            "order_number": d.get("OrderNumber"),
            "production_order_key": d.get("ProductionOrderKey"),
        }, default=str)

    row = rows[0]
    is_on_hold = bool(row.get("IsOnHold"))
    # Stringify 19-digit IDs to survive JS Number precision in MCP clients.
    return json.dumps({
        "material_name": material_name,
        "found": True,
        "is_on_hold": is_on_hold,
        "material_id": str(row.get("MaterialId")) if row.get("MaterialId") is not None else None,
        "type": row.get("Type"),
        "form": row.get("Form"),
        "order_number": row.get("OrderNumber_Dim"),
        "production_order_key": row.get("ProductionOrderKey_Dim"),
        "parent_material_key": row.get("ParentMaterialKey"),
        "interpretation": (
            "This material is currently ON HOLD."
            if is_on_hold else "This material is NOT on hold."
        ),
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 11: GET MATERIAL HOLD HISTORY
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_material_hold_history(material_name: str) -> str:
    """Return the historical hold/release timeline for a material from
    T_MaterialHoldReasonHistory. Useful for 'show me when this lot was held and why'.

    Args:
        material_name: Exact material name
    """
    logger.info(f"get_material_hold_history: {material_name!r}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})

    # The Staging history table joins MaterialId → DimMaterial.MaterialId (not Key)
    sql = f"""
        SELECT TOP 200
          h.*
        FROM {HOLD_HISTORY_TABLE_DWH} h
        WHERE h.MaterialId IN (
            SELECT MaterialId FROM {DIM_MATERIAL_TABLE} WHERE MaterialName = %s
        )
    """
    try:
        rows = _query(sql, (material_name,), db=DB_NAME_DWH)
    except ToolError:
        # Schema may differ; return what we know
        rows = []

    # Also get the total count in the table so the user knows if this is populated at all
    total_history = _query(f"SELECT COUNT(*) AS n FROM {HOLD_HISTORY_TABLE_DWH}", db=DB_NAME_DWH)
    total_rows = total_history[0].get("n", 0) if total_history else 0

    return json.dumps({
        "material_name": material_name,
        "history_events_for_material": len(rows),
        "total_hold_history_rows_in_table": total_rows,
        "events": rows,
        "note": (
            "No hold events recorded anywhere yet in this pilot deployment."
            if total_rows == 0
            else f"Found {len(rows)} events for this material out of {total_rows} total."
        ),
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 12: GET MATERIAL LIFECYCLE STATUS
# ═══════════════════════════════════════════════════════════════
# Process-state code → human label, sourced from CMF documentation conventions.
_PROCESS_STATE_LABELS = {
    0: "InProcess",
    1: "Queued",
    2: "Dispatchable",
    3: "Inactive",
    4: "Processed",
    5: "Terminated",
    6: "Suspended",
}
_SYSTEM_STATE_LABELS = {
    0: "Active",
    1: "Closed",
    2: "Terminated",
    3: "Suspended",
    4: "Cancelled",
}


@mcp.tool()
@safe_tool
def get_material_lifecycle_status(material_name: str) -> str:
    """CURRENT LIFECYCLE STATE of a material — read live from CoreDataModel.T_Material.
    Returns one of: Processed / Consumed / Terminated / In Process / Queued / On Hold,
    plus current step, last resource, hold count, flow path, and timestamps.

    *** ALWAYS USE THIS TOOL — DO NOT DERIVE STATE FROM GENEALOGY OR SUMMARY. ***
    Genealogy only shows assembly history; it cannot tell you the lot's PRESENT
    status, the step it's currently on, or the resource that last touched it.

    Call this whenever the user asks ANY of:
      • "current status / current state / current lifecycle"
      • "is it processed / consumed / terminated / on hold / complete / in process"
      • "what step is it on / what step is the lot at"
      • "what resource last touched it / which machine has it now"
      • "is this lot active / closed / suspended"
      • "where is this lot in the flow"
      • Any present-tense state question about a single lot.

    Args:
        material_name: Exact material name (e.g., 'S4442R031Y23|K9B381421|K9B381421 6')
    """
    logger.info(f"get_material_lifecycle_status: {material_name!r}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})

    # NOTE: Athena's Reports.P_GetMaterialHistory_RP_MultiParam_ODS uses an
    # internal filter (#ChangeStepOperations) that excludes TrackIn / Assemble /
    # Merge — events Entegris's pilot data is dominated by — so it returns 0 rows
    # for these materials. We instead read the live state straight from
    # CoreDataModel.T_Material (CMF's authoritative current-state row per
    # material). This is the same row the SP itself uses for its security check
    # in its FROM clause, so the data lineage is identical.
    rows = _query(f"""
        SELECT TOP 1
            M.MaterialId,
            M.Name,
            M.LastProcessState,
            M.SystemState,
            M.UniversalState,
            M.HoldCount,
            M.IsDispatchable,
            M.IsProductionComplete,
            M.LastProcessStepResourceId,
            M.LastProcessedResourceId,
            M.TrackInDate,
            M.TrackInPrimaryQuantity,
            M.ModifiedOn,
            M.ModifiedBy,
            M.CreatedOn,
            M.Type,
            M.Form,
            M.FlowPath,
            M.ReworkCount,
            M.DateEnteredStep,
            M.StepId,
            M.FacilityId,
            M.ProductId,
            R.Name AS LastResourceName,
            S.Name AS LastStepName
        FROM CoreDataModel.T_Material M
        LEFT JOIN CoreDataModel.T_Resource R ON R.ResourceId = M.LastProcessStepResourceId
        LEFT JOIN CoreDataModel.T_Step     S ON S.StepId     = M.StepId
        WHERE M.Name = %s
    """, (material_name,), db=DB_NAME_ODS)

    if not rows:
        return json.dumps({
            "material_name": material_name,
            "found": False,
            "message": "Material not found in CoreDataModel.T_Material.",
        })

    latest = rows[0]

    last_proc = latest.get("LastProcessState")
    last_sys = latest.get("SystemState")
    hold_count = latest.get("HoldCount") or 0

    is_on_hold = bool(hold_count and hold_count > 0)
    is_terminated = bool(last_sys == 2 or latest.get("OperationName") == "Terminate")
    is_processed = bool(last_proc == 4)

    # Derive a single human label per the problem statement's vocabulary
    if is_terminated:
        label = "Terminated"
    elif is_on_hold:
        label = "On Hold"
    elif is_processed:
        label = "Processed"
    elif last_proc == 0:
        label = "In Process"
    elif last_proc == 1:
        label = "Queued"
    elif last_proc == 5:
        label = "Terminated"
    else:
        label = _PROCESS_STATE_LABELS.get(last_proc, f"State_{last_proc}")

    # CMF entity IDs are 19-digit longs (>2^53). JS-based MCP clients lose
    # precision when these arrive as JSON numbers, so emit all bigint IDs
    # as strings.
    def _id(v):
        return str(v) if v is not None else None

    return json.dumps({
        "material_name": material_name,
        "material_id": _id(latest.get("MaterialId")),
        "found": True,
        "current_status": label,
        "is_on_hold": is_on_hold,
        "is_terminated": is_terminated,
        "is_processed": is_processed,
        "hold_count": hold_count,
        "last_process_state_code": last_proc,
        "last_process_state_label": _PROCESS_STATE_LABELS.get(last_proc, "Unknown"),
        "system_state_code": last_sys,
        "system_state_label": _SYSTEM_STATE_LABELS.get(last_sys, "Unknown"),
        "last_event_time": latest.get("ModifiedOn"),
        "last_modified_by": latest.get("ModifiedBy"),
        "created_on": latest.get("CreatedOn"),
        "track_in_date": latest.get("TrackInDate"),
        "track_in_primary_quantity": str(latest.get("TrackInPrimaryQuantity") or ""),
        "last_step_entered": latest.get("DateEnteredStep"),
        "last_step_id": _id(latest.get("StepId")),
        "last_step_name": latest.get("LastStepName"),
        "last_resource_id": _id(latest.get("LastProcessStepResourceId")),
        "last_resource_name": latest.get("LastResourceName"),
        "is_dispatchable": latest.get("IsDispatchable"),
        "is_production_complete": latest.get("IsProductionComplete"),
        "type": latest.get("Type"),
        "form": latest.get("Form"),
        "flow_path": latest.get("FlowPath"),
        "rework_count": latest.get("ReworkCount"),
        "data_source": "CoreDataModel.T_Material (live OLTP state row)",
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# TOOL 13: GET MATERIAL EDHR (Electronic Device History Report)
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_material_edhr(material_name: str, limit: int = 100) -> str:
    """ELECTRONIC DEVICE HISTORY REPORT (EDHR / eDHR / DHR) — full chronological
    event log for a material from CoreDataModel.T_MaterialHistory ⨝ T_OperationHistory.
    Returns every TrackIn, TrackOut, Dispatch, Assemble, Merge, MoveToNextStep,
    Save, AbortProcess, ChangeQuantity, SetDispatchableFlag, Terminate, etc. with
    timestamp, step, resource, operator, quantity, and process/system-state codes.

    *** ALWAYS USE THIS TOOL FOR ANY HISTORY-LOG REQUEST. DO NOT ASSEMBLE AN EDHR
        FROM GENEALOGY + HOLD TOOLS — those only show 2–3 assembly events, while
        a real EDHR contains 10s–100s of state-change events. ***

    Call this whenever the user asks ANY of:
      • "EDHR / eDHR / DHR"
      • "device history / electronic device history report"
      • "manufacturing history / production history / process history"
      • "event log / audit trail / activity log"
      • "show all events / chronological trace / every state change"
      • "pull the device history / pull the history for lot X"

    Args:
        material_name: Exact material name
        limit:        Max events to return (default 100, max 500). Use 10 if the
                      user explicitly says "limit 10" or "top 10 events".
    """
    logger.info(f"get_material_edhr: {material_name!r} limit={limit}")
    if not material_name:
        return json.dumps({"status": "error", "message": "material_name is required"})
    limit = min(max(1, int(limit)), 500)

    # SP needs explicit dates + TimeZone — defaults of NULL produce empty result.
    # Use a wide window that covers all observed pilot data through future runs.
    # NOTE: We don't use Reports.P_GetMaterialHistory_RP_MultiParam_ODS here
    # because its #ChangeStepOperations filter excludes the operation types
    # this pilot's materials actually have (TrackIn, Assemble, Merge, etc.).
    # We read directly from T_OperationHistory ⨝ T_MaterialHistory ⨝ T_Material
    # — the same source tables the SP itself reads — but without the narrow
    # operation-name filter, so we return the full chronological event log
    # an EDHR is meant to provide.
    rows = _query(f"""
        SELECT TOP {limit}
            OH.OperationEndTime    AS event_time,
            OH.OperationStartTime  AS event_start,
            OH.OperationName       AS operation,
            CAST(NULL AS NVARCHAR(256)) AS service,
            MH.MaterialId          AS material_id,
            MH.StepId              AS step_id,
            ST.Name                AS step_name,
            MH.LastProcessedResourceId AS resource_id,
            R.Name                 AS resource_name,
            MH.ModifiedBy          AS user_account,
            MH.PrimaryQuantity     AS primary_quantity,
            MH.SecondaryQuantity   AS secondary_quantity,
            MH.PrimaryUnits        AS primary_units,
            MH.LastProcessState    AS process_state_code,
            MH.SystemState         AS system_state_code,
            MH.HoldCount           AS hold_count_at_event,
            MH.IsDispatchable      AS is_dispatchable,
            MH.ReworkCount         AS rework_count,
            MH.FlowPath            AS flow_path,
            OH.OperationHistorySeq AS operation_history_seq,
            OH.ServiceHistoryId    AS service_history_id
        FROM CoreDataModel.T_Material M
        JOIN CoreDataModel.T_MaterialHistory MH
                ON MH.MaterialId = M.MaterialId
        JOIN dbo.T_OperationHistory OH
                ON OH.EntityId = MH.MaterialId
               AND OH.ServiceHistoryId = MH.ServiceHistoryId
               AND OH.OperationHistorySeq = MH.OperationHistorySeq
        -- Service join removed (T_OperationHistory does not expose ServiceId
        -- in this CMF version; ServiceName is left null in the response).
        LEFT JOIN CoreDataModel.T_Step ST ON ST.StepId = MH.StepId
        LEFT JOIN CoreDataModel.T_Resource R
                ON R.ResourceId = MH.LastProcessedResourceId
        WHERE M.Name = %s
        ORDER BY OH.OperationEndTime ASC, OH.OperationHistorySeq ASC
    """, (material_name,), db=DB_NAME_ODS)

    # CMF entity IDs are 19-digit longs (>2^53). Stringify so JS-based MCP
    # clients don't lose precision on the trailing digits.
    def _id(v):
        return str(v) if v is not None else None

    events = []
    for r in rows:
        events.append({
            "timestamp": r.get("event_time"),
            "event_start": r.get("event_start"),
            "operation": r.get("operation"),
            "service": r.get("service"),
            "step_id": _id(r.get("step_id")),
            "step_name": r.get("step_name"),
            "resource_id": _id(r.get("resource_id")),
            "resource_name": r.get("resource_name"),
            "user": r.get("user_account"),
            "primary_quantity": str(r.get("primary_quantity") or ""),
            "secondary_quantity": str(r.get("secondary_quantity") or ""),
            "primary_units": r.get("primary_units"),
            "process_state_code": r.get("process_state_code"),
            "process_state_label": _PROCESS_STATE_LABELS.get(r.get("process_state_code"), "Unknown"),
            "system_state_code": r.get("system_state_code"),
            "hold_count_at_event": r.get("hold_count_at_event"),
            "is_dispatchable": r.get("is_dispatchable"),
            "rework_count": r.get("rework_count"),
            "flow_path": r.get("flow_path"),
            "operation_history_seq": r.get("operation_history_seq"),
            "service_history_id": _id(r.get("service_history_id")),
        })

    # Operation tally for quick summary
    tally: dict = {}
    for e in events:
        op = e["operation"] or "Unknown"
        tally[op] = tally.get(op, 0) + 1

    return json.dumps({
        "material_name": material_name,
        "event_count": len(events),
        "limit": limit,
        "operation_breakdown": tally,
        "earliest_event": events[0]["timestamp"] if events else None,
        "latest_event": events[-1]["timestamp"] if events else None,
        "events": events,
        "data_source": "CoreDataModel.T_MaterialHistory ⨝ dbo.T_OperationHistory (live OLTP)",
    }, default=str)


# ═══════════════════════════════════════════════════════════════
# ALIAS TOOLS — direct, prompt-matching names for Claude tool routing.
# These are thin wrappers over get_material_lifecycle_status / get_material_edhr
# whose names match user vocabulary verbatim, so the LLM picks them up
# even when it would otherwise default to summary/genealogy.
# ═══════════════════════════════════════════════════════════════
@mcp.tool()
@safe_tool
def get_current_state(material_name: str) -> str:
    """CURRENT STATE / CURRENT STATUS / CURRENT STEP of a material — alias for
    get_material_lifecycle_status. Call this for any prompt phrased as "current
    state", "current status", "current step", "what step is it on", "what resource
    last touched it", "is it processed/consumed/terminated/on hold/complete".

    Args:
        material_name: Exact material name
    """
    return get_material_lifecycle_status(material_name)


@mcp.tool()
@safe_tool
def get_device_history(material_name: str, limit: int = 100) -> str:
    """DEVICE HISTORY / DEVICE HISTORY REPORT / DHR / EDHR / eDHR — alias for
    get_material_edhr. Call this for any prompt mentioning "device history",
    "device history report", "DHR", "EDHR", "eDHR", "manufacturing history",
    "production history", "process history", "pull the history", "show the history".
    Returns the full chronological event log (TrackIn, TrackOut, Dispatch, Assemble,
    Merge, MoveToNextStep, etc.) — NOT just assembly edges.

    Args:
        material_name: Exact material name
        limit: Max events to return (default 100, max 500)
    """
    return get_material_edhr(material_name, limit)


@mcp.tool()
@safe_tool
def get_event_log(material_name: str, limit: int = 100) -> str:
    """EVENT LOG / AUDIT TRAIL / ACTIVITY LOG — alias for get_material_edhr.
    Call this for any prompt phrased as "event log", "audit trail", "activity log",
    "all events", "every state change", "chronological events".

    Args:
        material_name: Exact material name
        limit: Max events to return (default 100, max 500)
    """
    return get_material_edhr(material_name, limit)


@mcp.tool()
@safe_tool
def get_dhr(material_name: str, limit: int = 100) -> str:
    """DHR (Device History Record) — alias for get_material_edhr. Use whenever
    the user says "DHR" or "device history record".

    Args:
        material_name: Exact material name
        limit: Max events to return (default 100, max 500)
    """
    return get_material_edhr(material_name, limit)


@mcp.tool()
@safe_tool
def get_edhr(material_name: str, limit: int = 100) -> str:
    """EDHR / eDHR (Electronic Device History Report) — alias for get_material_edhr.
    Use whenever the user says "EDHR", "eDHR", or "electronic device history report".

    Args:
        material_name: Exact material name
        limit: Max events to return (default 100, max 500)
    """
    return get_material_edhr(material_name, limit)


# ─── Lambda entrypoint ─── #
def lambda_handler(event, context):
    """Entry point — MCPLambdaHandler dispatches JSON-RPC MCP requests to the tools above."""
    return mcp.handle_request(event, context)
