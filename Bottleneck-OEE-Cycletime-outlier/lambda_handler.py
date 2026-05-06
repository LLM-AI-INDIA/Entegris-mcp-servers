"""
AWS Lambda MCP Server for Athena AI - Entegris Bottleneck_MCP Schema
Queries on-premises SQL Server via Site-to-Site VPN

Schema:   Bottleneck_MCP (37 objects: 35 views + 2 stored procedures)
Database: EntegrisKSPUpgradeDWH (connection params from DB_SERVER / DB_PORT env vars)

Tools (consolidated v3.8 — 9 mcp tools):
  1. get_topic_guide          - Route a question to the correct views + example SQL
  2. query                    - Consolidated SELECT (scope='curated' for Bottleneck_MCP.vw_*; scope='raw' for any schema)
  3. query_view               - [DEPRECATED v3.8 — use query(scope='curated')]
  4. get_view_details         - Get column names, types, and sample data for a view
  5. run_stored_procedure     - Execute whitelisted SPs (multi-resultset)
  6. list_parameter_values    - Get valid filter values for categorical columns
  7. run_query                - [DEPRECATED v3.8 — use query(scope='raw')]
  8. get_instructions         - Returns the playbook
  9. get_response_format      - Element checklist + layout guidance per prompt

Topics (31 total):
  1. Current Bottleneck Detection      7. Historical Trend
  2. Root Cause Analysis               8. Comparative Analysis
  3. Cycle Time                        9. Drill-down / What's Stuck
  4. Queue & WIP                       10. Rework Impact
  5. Throughput & Target               11. Operator at Bottleneck
  6. Downtime & Utilization            12. Predictive / What-If
"""

import json
import logging
import os
import pymssql
from awslabs.mcp_lambda_handler import MCPLambdaHandler

# ----------------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# MCP LAMBDA HANDLER
# ----------------------------------------------------------------------------
mcp = MCPLambdaHandler(
    name="Athena AI - Entegris Bottleneck_MCP",
    version="1.0.0"
)

# ----------------------------------------------------------------------------
# SQL SERVER CONFIG
# ----------------------------------------------------------------------------
DB_SERVER   = os.environ["DB_SERVER"]
DB_PORT     = int(os.environ["DB_PORT"])
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME     = os.environ["DB_NAME"]
VIEW_SCHEMA = "Bottleneck_MCP"

ROW_LIMIT_DEFAULT = 200
ROW_LIMIT_MAX = 1000


def get_db_connection(database: str = DB_NAME):
    conn = pymssql.connect(
        server=DB_SERVER,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=database,
        login_timeout=15,
        timeout=60,
    )
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 30000")
    cur.close()
    return conn


# ----------------------------------------------------------------------------
# SHARED HELPERS
# ----------------------------------------------------------------------------
def _coerce_value(value):
    """Stringify datetimes and other non-primitive values for JSON safety."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value is not None and not isinstance(value, (str, int, float, bool)):
        return str(value)
    return value


def _execute_read_query(sql: str, params: tuple = (), limit: int = ROW_LIMIT_DEFAULT) -> dict:
    """Execute a single-resultset read-only query and return columns + rows."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        if cursor.description is None:
            cursor.close()
            conn.close()
            return {"columns": [], "rows": [], "row_count": 0, "truncated": False}

        columns = [col[0] for col in cursor.description]
        rows = []
        truncated = False
        for i, row in enumerate(cursor):
            if i >= limit:
                truncated = True
                break
            rows.append([_coerce_value(v) for v in row])

        cursor.close()
        conn.close()
        return {"columns": columns, "rows": rows, "row_count": len(rows), "truncated": truncated}
    except pymssql.Error as e:
        logger.error(f"Database error: {e}")
        return {"error": str(e), "columns": [], "rows": [], "row_count": 0}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"error": str(e), "columns": [], "rows": [], "row_count": 0}


def _execute_multi_resultset(sql: str, params: tuple = (), limit: int = ROW_LIMIT_MAX) -> dict:
    """Execute a stored procedure that may return multiple result sets."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        result_sets = []
        more = True
        while more:
            if cursor.description is None:
                more = cursor.nextset()
                continue
            columns = [col[0] for col in cursor.description]
            rows = []
            truncated = False
            for i, row in enumerate(cursor):
                if i >= limit:
                    truncated = True
                    break
                rows.append([_coerce_value(v) for v in row])
            result_sets.append({
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated,
            })
            more = cursor.nextset()

        cursor.close()
        conn.close()
        return {
            "result_sets": result_sets,
            "set_count": len(result_sets),
            "total_rows": sum(rs["row_count"] for rs in result_sets),
        }
    except pymssql.Error as e:
        logger.error(f"Database error (multi-resultset): {e}")
        return {"error": str(e), "result_sets": [], "set_count": 0}
    except Exception as e:
        logger.error(f"Unexpected error (multi-resultset): {e}")
        return {"error": str(e), "result_sets": [], "set_count": 0}


# ============================================================================
# DISAMBIGUATION RULES - returned by get_topic_guide to prevent LLM confusion
# ============================================================================
DISAMBIGUATION_RULES = {
    "bottleneck_vs_slowdown": {
        "rule": "'Bottleneck' and 'Slowdown' are NOT the same concept",
        "definitions": {
            "Bottleneck": "The slowest step in the production flow - the resource that limits overall throughput. Detected via lowest ET + highest queue across the line.",
            "Slowdown": "A resource running slower than its OWN baseline (i.e. CycleTime > IdealCycleTime), regardless of whether it is the line constraint.",
        },
        "use": {
            "for_bottleneck": "vw_BottleneckRanked, vw_Summary_ShiftBottleneck, vw_BottleneckAlert",
            "for_slowdown": "vw_ResourceCycleTime_Main (compare AvgCycleTime vs IdealCycleTime), vw_CycleTimeTrend",
        },
    },
    "queue_vs_wip": {
        "rule": "'Queue' and 'WIP' are different scopes at a resource",
        "definitions": {
            "Queue": "Material WAITING in front of a resource (not yet in process). Measured by QueuedLag and queued count.",
            "WIP": "ALL material at a resource - both queued AND in-process. Broader than queue.",
        },
        "use": {
            "for_queue": "vw_ResourceQueue_Main, vw_QueueAgeDistribution, vw_WaitingLots_Details",
            "for_wip": "vw_ResourceWIP_Main, vw_MaterialsAtResource",
        },
    },
    "cycle_time_vs_ECT": {
        "rule": "'Cycle Time' and 'ECT' (Effective Cycle Time) are different - ECT is the bottleneck-detection metric",
        "definitions": {
            "CycleTime": "Total Processing Time / Total Units Processed. Pure throughput rate when the machine is up.",
            "ECT": "CycleTime x (1 + DownTimePct). Inflates cycle time by downtime - this is what bottleneck detection compares.",
            "ET": "Units Produced / ECT. The Effective Throughput. Lowest ET = most likely bottleneck.",
        },
        "rule_of_thumb": "Use ECT for bottleneck detection (vw_Summary_ShiftBottleneck.ECT). Use CycleTime for raw cycle time questions (vw_ResourceCycleTime_Main).",
    },
    "current_state_values": {
        "rule": "Material 'CurrentState' values - know what each one means before filtering",
        "values": {
            "Queued": "Waiting in front of a resource - has not started processing",
            "Dispatched": "Assigned to a resource but not yet picked up",
            "InProcess": "Actively being processed at the resource",
            "Processed": "Completed processing at the resource - waiting for transit",
            "InTransit": "Moving between resources",
            "Consumable": "Consumable material (BOM input)",
            "StepOutput": "Output of a process step",
        },
        "note": "For queue-length questions, filter CurrentState IN ('Queued','Dispatched'). For WIP questions, include 'InProcess' as well.",
    },
    "semi_e10_states": {
        "rule": "SEMI E10 equipment states - 6 standard categories. Use these for utilization analysis",
        "values": {
            "Productive": "Equipment is processing material - good time",
            "Standby": "Equipment is up but idle (no material to run) - utilization loss",
            "Engineering": "Equipment is up but reserved for engineering tests",
            "Scheduled Down": "Planned downtime (PM, calibration)",
            "Unscheduled Down": "Unplanned downtime (failure, breakdown) - this is the painful one",
            "Nonscheduled": "Outside the scheduled production window",
        },
        "use": "vw_SEMIE10StateDistribution shows hours and pct per state per resource",
    },
    "shift_summary_vs_comparison": {
        "rule": "'Shift Summary' and 'Shift Comparison' are different views",
        "definitions": {
            "Summary": "Snapshot of the CURRENT shift - per-resource metrics for one shift in one place",
            "Comparison": "Shift-VS-shift deltas - showing how Day vs Night, or Mon vs Tue, differ",
        },
        "use": {
            "for_current_shift": "vw_Summary_ShiftBottleneck",
            "for_shift_vs_shift": "vw_ShiftComparison",
            "for_area_vs_area": "vw_AreaComparison",
        },
    },
    "ranked_vs_alert": {
        "rule": "'Bottleneck Ranked' and 'Bottleneck Alert' answer different questions",
        "definitions": {
            "vw_BottleneckRanked": "ALL resources for a shift, sorted by BottleneckScore (lowest ET + highest queue first). Use for 'top N bottlenecks'.",
            "vw_BottleneckAlert": "ONLY resources that breach a threshold (queue > N OR ET < X% of ideal). Use for 'show me the alerts'.",
        },
        "rule_of_thumb": "If the user asks 'who is the bottleneck' use Ranked. If they ask 'is anything wrong / are there alerts' use Alert.",
    },
    "root_cause_dimensions": {
        "rule": "Root cause is split into 3 ORTHOGONAL dimensions - Equipment, Material, Operator",
        "definitions": {
            "Equipment": "Downtime, MTBF, MTTR, state transitions. View: vw_RootCause_Equipment.",
            "Material": "Holds, quality losses, rework counts. View: vw_RootCause_Material.",
            "Operator": "Operator cycle time vs peer average at the resource. View: vw_RootCause_Operator.",
        },
        "combined": "vw_RootCause_Combined returns one row per resource indicating which of the three dominates - start there.",
    },
    "facility_filter_required": {
        "rule": "Every data-collection view requires FacilityKey - never query without it",
        "filter_views": ["vw_Facilities (list of valid FacilityKey)", "vw_Areas", "vw_Resources", "vw_Shifts", "vw_Products"],
        "example": "Always include WHERE FacilityKey = <int> in any data-collection view query",
    },
}


# ============================================================================
# v3.7.0 — A2 EXTRA_ROUTINGS — keyword-direct view/SP shortcuts that bypass the
# 12-topic registry. Used when a question's answer is "always X view" — e.g.
# "correlation" -> vw_StatisticalCorrelations + vw_OEE_ComponentCorrelation.
# Checked AFTER numeric topic lookup but BEFORE generic fuzzy match.
# ============================================================================
EXTRA_ROUTINGS = {
    "statistical_significance": {
        "keywords": ["significant", "significance", "p-value", "p value", "t-test", "anova",
                     "chi-square", "chi square", "fisher", "confidence interval"],
        "primary_views": ["vw_CategoricalSignificanceTests", "vw_ShiftANOVA"],
        "must_include_in_answer": [
            "Cite the test name (chi-square / Fisher / ANOVA F).",
            "Print N, statistic, p-value.",
            "If IsLowSampleConfidence=1, OPEN with: 'Sample size is below the validity threshold — treat as directional only.'",
        ],
    },
    "correlation_relationship": {
        "keywords": ["correlation", "correlate", "trade-off", "trade off", "tradeoff",
                     "relationship between", "scatter", "regression"],
        "primary_views": [
            "vw_StatisticalCorrelations",
            "vw_WIPWaitRelationship",
            "vw_VolumeQualityTradeoff",
            "vw_OEE_ComponentCorrelation",
        ],
        "must_include_in_answer": [
            "Pearson r per pair, sample N, and a directional verdict.",
            "Disclose 'undefined (zero variance)' rather than omitting when r is NaN.",
        ],
    },
    "whatif_capacity": {
        "keywords": ["what if", "what-if", "if we add", "counterfactual",
                     "theoretical max", "investment", "roi", "cost-benefit", "cost benefit"],
        "primary_views": [
            "vw_WhatIf_AddResource",
            "vw_AreaWhatIf_ThroughputLift",
            "vw_AreaInvestmentROI",
            "vw_TheoreticalCapacity",
        ],
        "must_include_in_answer": [
            "Current vs projected metric, delta, and one-line investment line.",
            "When IsEstimatedDefault=1: OPEN with the Finance disclosure.",
        ],
    },
    "lot_journey_lead_time": {
        "keywords": ["lot journey", "transit", "lead time", "step-by-step",
                     "step by step", "end-to-end flow", "end to end flow"],
        "primary_views": ["vw_MaterialFlowJourney_Detail", "vw_LotJourney"],
        "must_include_in_answer": [
            "Sequential step list with QueueSec, ProcessSec, TransitSec.",
            "Total lead time and the longest-wait step explicitly named.",
        ],
    },
    "time_slice_intra_shift": {
        "keywords": ["time slice", "2-hour", "2 hour", "hourly bottleneck",
                     "intra-shift", "intra shift", "throughout the shift",
                     "hour of shift", "by hour", "hourly bottleneck pattern"],
        "primary_sps": ["sp_BottleneckAnalysis"],
        "primary_views": ["vw_BottleneckByHourOfShift"],
        "param_hint": "@IncludeTimeSlice=1, @SliceIntervalMinutes=120 for 2-hour slices. vw_BottleneckByHourOfShift is the pre-computed/declarative alternative when the user wants the heatmap shape directly.",
    },
}


def _match_extra_routing(text: str):
    """v3.7.0 A2 — return matching EXTRA_ROUTINGS entry or None."""
    t = (text or "").lower()
    for name, cfg in EXTRA_ROUTINGS.items():
        for kw in cfg["keywords"]:
            if kw in t:
                return name, cfg
    return None, None


# ============================================================================
# TOPIC REGISTRY - 12 topics x view routing
# ============================================================================
TOPIC_REGISTRY = {
    1: {
        "name": "Current Bottleneck Detection",
        "keywords": ["bottleneck", "current bottleneck", "constraint", "where is the bottleneck", "slowest resource", "limiting resource", "ranked", "alert", "time slice events", "event-by-event timeline", "what happened at minute"],
        "description": "Identify the resource limiting throughput RIGHT NOW (or in a specified shift). Returns ranked resources by BottleneckScore (lowest ET + highest queue) and threshold-based alerts.",
        "summary_view": {
            "name": "vw_BottleneckRanked",
            "columns": ["FacilityKey", "AreaKey", "ShiftKey", "ResourceKey", "ResourceName", "ECT", "ET", "QueueLength", "BottleneckScore", "IsBottleneck", "Rank", "RankTiebreak", "AvgQueueLength_WhenBottlenecked"],
            "column_notes": "AvgQueueLength_WhenBottlenecked (added v3.2.0) is a window AVG over (ResourceKey, FacilityKey) of QueueLength on shifts where IsBottleneck=1. Use it to answer 'when this resource bottlenecks, how big does the queue typically get?'.",
        },
        "detail_views": [
            {"name": "vw_Summary_ShiftBottleneck", "columns": ["FacilityKey", "AreaKey", "ShiftKey", "ShiftName", "ResourceKey", "ResourceName", "CycleTime", "Throughput", "UtilizationPct", "DownTimePct", "ECT", "ET", "QueueLength", "WIPCount", "ReworkInflatedQuantity", "IsBottleneck"], "use_when": "Per-resource per-shift metrics with pre-computed ECT, ET, IsBottleneck flag"},
            {"name": "vw_BottleneckAlert", "columns": ["FacilityKey", "AreaKey", "ShiftKey", "ResourceKey", "ResourceName", "AlertType", "AlertValue", "Threshold", "TriggeredAt"], "use_when": "Threshold-based alerts (queue > N OR ET < X% of ideal)"},
            {"name": "vw_TimeSliceEvents", "use_when": "v3.7.1 NEW wiring — raw event-by-event timeline that underlies sp_BottleneckCascade. Use for 'time slice events', 'event-by-event timeline', 'what happened at minute X' style follow-ups when the user wants the underlying events rather than the SP roll-up."},
        ],
        "stored_procedure": {"name": "sp_BottleneckAnalysis", "params": {"FacilityKey": "required BIGINT", "DateFrom": "required DATETIME", "DateTo": "required DATETIME", "AreaKey": "optional BIGINT", "ShiftKey": "optional BIGINT", "TopN": "optional INT, default 20", "IncludeTimeSlice": "optional BIT, default 0", "SliceIntervalMinutes": "optional INT, default 30"}, "use_when": "Single-call full bottleneck report - returns 3 result sets (ShiftSummary, ResourceDetail, TimeSliceHistory). TimeSliceHistory (v3.2.0) now includes ET_InSlice, QueueLength_AtSliceEnd, UnitsProduced_InSlice, TransitionTrigger ('Same' / 'NewBottleneck' / 'NoData')."},
        "parameter_views": ["vw_Facilities", "vw_Areas", "vw_Shifts"],
        "example_questions": [
            "Where is the bottleneck right now in facility 4266?",
            "Which resource is the constraint in Shift 1234?",
            "Show me the top 5 bottleneck resources for today.",
            "Are there any bottleneck alerts right now?",
            "Which resource has the lowest Effective Throughput in the current shift?",
        ],
        "example_queries": [
            "SELECT TOP 10 ResourceName, BottleneckScore, RankTiebreak, AvgQueueLength_WhenBottlenecked FROM Bottleneck_MCP.vw_BottleneckRanked WHERE FacilityKey = 2 AND ShiftKey = 3 ORDER BY RankTiebreak",
            "SELECT ResourceName, CycleTime, ECT, ET, QueueLength, IsBottleneck FROM Bottleneck_MCP.vw_Summary_ShiftBottleneck WHERE FacilityKey = 2 AND ShiftKey = (SELECT MAX(ShiftKey) FROM Bottleneck_MCP.vw_Shifts WHERE FacilityKey = 2) ORDER BY ET ASC",
            "SELECT * FROM Bottleneck_MCP.vw_BottleneckAlert WHERE FacilityKey = 2 AND TriggeredAt >= '2026-02-01' ORDER BY TriggeredAt DESC",
        ],
    },
    2: {
        "name": "Root Cause Analysis",
        "keywords": ["root cause", "why bottleneck", "why is", "equipment cause", "material cause", "operator cause", "dominant cause", "blame"],
        "description": "Explain WHY a resource is the bottleneck. Splits cause into 3 orthogonal dimensions: Equipment (downtime/MTBF/MTTR), Material (holds/quality/rework), Operator (cycle time vs peer).",
        "summary_view": {
            "name": "vw_RootCause_Combined",
            "columns": ["FacilityKey", "ResourceKey", "ShiftKey", "EquipmentScore", "MaterialScore", "OperatorScore", "DominantCause", "Confidence"],
            "column_notes": "DominantCause is a string label ('Equipment' / 'Material' / 'Operator') derived from the max of the three *Score columns; Confidence (0.0-1.0) reflects how cleanly one dimension dominates. NOTE: this view does NOT carry ResourceName — join on ResourceKey to vw_Resources if a name is required. There is no DominantDimension or RootCauseSummary column.",
            "use_when": "Top-level root-cause split for a (Facility, Shift, Resource). Use for 'is the bottleneck caused by equipment / material / operator?' style questions. Drill into the per-dimension detail views for component-level metrics (MTBF, hold count, operator cycle time, etc.).",
        },
        "detail_views": [
            {"name": "vw_RootCause_Equipment", "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "ResourceType", "ShiftKey", "DowntimePct", "UnscheduledDownHours", "MTBF_Hours", "MTTR_Hours", "UpToDownTransitionCount", "DominantDowntimeReason"], "use_when": "Equipment-side root cause — downtime %, MTBF_Hours, MTTR_Hours, up→down transition count, and the dominant downtime reason text. Use for 'which resource has the worst MTBF / MTTR' or 'what is the top downtime reason on the bottleneck resource'. Column names are MTBF_Hours / MTTR_Hours (with underscore) — NOT MTBFHours/MTTRHours."},
            {"name": "vw_RootCause_Material", "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "ResourceType", "ShiftKey", "HoldCount", "TotalQuantityLoss", "TotalQuantityBonus", "ReworkCount", "OutOfStepCount", "MaterialsInReworkCount"], "use_when": "Material-side root cause — hold count, quantity loss/bonus, rework count, out-of-step count, and how many distinct materials are in rework. There is no QualityLossUnits or MaterialIssuesScore column — use TotalQuantityLoss for quantity-loss questions."},
            {"name": "vw_RootCause_Operator", "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "ResourceType", "ShiftKey", "OperatorCount", "AvgCycleTimeSec", "PeerAvgCycleTimeSec", "CycleTimeDeviationPct"], "use_when": "Operator-side root cause aggregated at the (Resource, Shift) level — operator headcount, avg operator cycle time vs peer avg, and the % deviation. NOTE: this view does NOT carry OperatorKey / OperatorName / per-operator rows — use vw_OperatorAtResource (Topic 11) for per-operator detail."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources", "vw_Shifts"],
        "example_questions": [
            "Why is Resource X the bottleneck this shift?",
            "Is the bottleneck caused by equipment, material, or operator?",
            "Which resource has the worst MTBF?",
            "Show me equipment downtime vs material holds for the current bottleneck.",
            "How does operator cycle time at this resource compare to the peer average?",
        ],
        "example_queries": [
            "SELECT ResourceKey, DominantCause, Confidence, EquipmentScore, MaterialScore, OperatorScore FROM Bottleneck_MCP.vw_RootCause_Combined WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY (EquipmentScore + MaterialScore + OperatorScore) DESC",
            "SELECT ResourceName, DowntimePct, MTBF_Hours, MTTR_Hours, UpToDownTransitionCount, DominantDowntimeReason FROM Bottleneck_MCP.vw_RootCause_Equipment WHERE FacilityKey = 2 AND ResourceKey = 17 AND ShiftKey BETWEEN 1200 AND 1240 ORDER BY ShiftKey",
            "SELECT ResourceName, OperatorCount, AvgCycleTimeSec, PeerAvgCycleTimeSec, CycleTimeDeviationPct FROM Bottleneck_MCP.vw_RootCause_Operator WHERE FacilityKey = 2 AND ShiftKey = 1234 AND CycleTimeDeviationPct > 20 ORDER BY CycleTimeDeviationPct DESC",
        ],
    },
    3: {
        "name": "Cycle Time",
        "keywords": ["cycle time", "actual vs ideal", "slower than usual", "wait time", "active time", "setup time", "how long does", "step duration"],
        "description": "Analyze actual vs ideal cycle time per resource and per material. Includes per-material breakdown into Wait/Active/Setup/Movement components.",
        "summary_view": {
            "name": "vw_ResourceCycleTime_Main",
            "columns": ["FacilityKey", "AreaKey", "ShiftKey", "ResourceKey", "ResourceName", "AvgCycleTime", "IdealCycleTime", "MinCycleTime", "MaxCycleTime", "DeltaVsIdealPct"],
        },
        "detail_views": [
            {"name": "vw_CycleTimeBreakdown", "columns": ["FacilityKey", "ShiftKey", "ResourceKey", "MaterialKey", "MaterialName", "WaitTime", "ActiveTime", "SetupTime", "MovementTime", "TotalTime"], "use_when": "Per-material breakdown into Wait/Active/Setup/Movement components"},
            {"name": "vw_CycleTimeTrend", "columns": ["FacilityKey", "ResourceKey", "ResourceName", "Day", "AvgCycleTime", "IdealCycleTime", "ShiftCount"], "use_when": "Daily cycle-time trend per resource (multi-day view)"},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "What is the average cycle time on Resource X today?",
            "Is this step slower than its ideal cycle time?",
            "Show me the Wait/Active/Setup/Movement breakdown for the bottleneck resource.",
            "How has cycle time on Resource X trended over the last 7 days?",
            "Which resources have the largest delta between actual and ideal cycle time?",
        ],
        "example_queries": [
            "SELECT TOP 20 ResourceName, AvgCycleTime, IdealCycleTime, DeltaVsIdealPct FROM Bottleneck_MCP.vw_ResourceCycleTime_Main WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY DeltaVsIdealPct DESC",
            "SELECT MaterialName, WaitTime, ActiveTime, SetupTime, MovementTime FROM Bottleneck_MCP.vw_CycleTimeBreakdown WHERE FacilityKey = 2 AND ResourceKey = 17 AND ShiftKey = 1234 ORDER BY TotalTime DESC",
            "SELECT Day, AvgCycleTime, IdealCycleTime FROM Bottleneck_MCP.vw_CycleTimeTrend WHERE FacilityKey = 2 AND ResourceKey = 17 AND Day BETWEEN '2026-02-01' AND '2026-02-28' ORDER BY Day",
        ],
    },
    4: {
        "name": "Queue & WIP",
        "keywords": ["queue", "wip", "waiting", "how many lots", "queue depth", "queue length", "oldest queued", "stuck", "queue age", "wait time vs wip", "wip-wait relationship", "wip-bottleneck correlation", "wip vs bottleneck"],
        "description": "Track material waiting in front of a resource (Queue) and material at a resource overall (WIP). Includes queue-age distribution and per-lot waiting detail.",
        "summary_view": {
            "name": "vw_ResourceQueue_Main",
            "columns": ["FacilityKey", "AreaKey", "AreaName", "ShiftKey", "ResourceKey", "ResourceName", "QueueCount", "WaitingLots", "WaitingUnits", "AvgQueuedLag", "MaxQueuedLag", "AvgQueuedLag_Hours", "MaxQueuedLag_Hours"],
            "column_notes": "WaitingLots = count of queued lots. WaitingUnits = sum of queued PrimaryQuantity (use this when user asks 'how many units / pieces / wafers are waiting'). AvgQueuedLag is in seconds; AvgQueuedLag_Hours is the human-friendly form (already / 3600). Same for MaxQueuedLag.",
        },
        "detail_views": [
            {"name": "vw_ResourceWIP_Main", "columns": ["FacilityKey", "AreaKey", "AreaName", "ShiftKey", "ResourceKey", "ResourceName", "StepKey", "StepName", "WIPCount", "OnHoldCount", "InReworkCount"], "use_when": "WIP per step per resource - includes both queued AND in-process material; resolved ResourceName/AreaName (no '-' masking)"},
            {"name": "vw_ResourceWIP_Snapshot", "columns": ["FacilityKey", "AreaKey", "AreaName", "ResourceKey", "ResourceName", "StepKey", "StepName", "WIPCount", "OnHoldCount", "InReworkCount", "OldestQueuedHours", "CurrentSEMI_E10State"], "use_when": "Point-in-time WIP snapshot. CurrentSEMI_E10State surfaces the latest-shift SEMI E10 state (Productive / Standby / etc.) so 'list resources with WIP > N' answers can include current equipment state without a second query."},
            {"name": "vw_QueueAgeDistribution", "columns": ["FacilityKey", "AreaKey", "ShiftKey", "ResourceKey", "AgeBucket", "LotCount"], "column_notes": "LONG-FORM shape: one row per (Resource, Shift, AgeBucket) — NOT wide-form. AgeBucket is a string label ('0-1h' / '1-4h' / '4-24h' / '>24h'); LotCount is the count of queued lots in that bucket. There is no ResourceName column — join on ResourceKey to vw_Resources if a name is required. To produce wide-form output, use SQL PIVOT, e.g.: SELECT FacilityKey, ShiftKey, ResourceKey, [0-1h] AS Bucket_0_1h, [1-4h] AS Bucket_1_4h, [4-24h] AS Bucket_4_24h, [>24h] AS Bucket_Over_24h FROM (SELECT FacilityKey, ShiftKey, ResourceKey, AgeBucket, LotCount FROM Bottleneck_MCP.vw_QueueAgeDistribution) src PIVOT (SUM(LotCount) FOR AgeBucket IN ([0-1h],[1-4h],[4-24h],[>24h])) p.", "use_when": "Queue distributed into 0-1h / 1-4h / 4-24h / >24h age buckets in long-form. Use long-form SELECTs for chartable tabular output, or PIVOT (see column_notes) when callers expect wide-form Bucket_* columns. NOTE: a finer-grained, already-wide variant vw_QueueAgeDistribution_Custom exists for short-wait analytics (Lt1h / 1-2h / 2-4h / Gt4h)."},
            {"name": "vw_QueueAgeDistribution_Custom", "columns": ["FacilityKey", "AreaKey", "ShiftKey", "ResourceKey", "ResourceName", "Bucket_Lt1h", "Bucket_1_2h", "Bucket_2_4h", "Bucket_Gt4h", "TotalQueued"], "use_when": "Finer-grained queue-age distribution (Lt1h / 1-2h / 2-4h / Gt4h). Anchored on vw_DataAvailability.LatestEvent. Use when the user asks about short-wait detail or the 1-2h / 2-4h buckets specifically."},
            {"name": "vw_WaitingLots_Details", "columns": ["FacilityKey", "ShiftKey", "ResourceKey", "ResourceName", "MaterialKey", "MaterialName", "LotName", "QueuedAt", "AgeHours", "Priority"], "use_when": "Per-lot waiting detail with age and priority"},
            {"name": "vw_WIPWaitRelationship", "use_when": "v3.7 NEW — relationship between WIP and wait time. Also routed via EXTRA_ROUTINGS.correlation_relationship. Use for 'wait time vs wip', 'wip-wait relationship'."},
            {"name": "vw_WIPBottleneckCorrelation", "use_when": "v3.7 NEW — correlation between WIP and bottleneck activity. Also referenced from Topic 16 (Statistical Correlations). Use for 'wip-bottleneck correlation', 'wip vs bottleneck'."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "How many lots are waiting at Resource X?",
            "What is the oldest lot in queue right now?",
            "Show me the queue age distribution for the bottleneck resource.",
            "How much WIP is at each resource in Area 5?",
            "Which lots have been waiting more than 24 hours?",
            "Show the WIP vs wait-time relationship.",
            "Is WIP correlated with bottleneck frequency?",
        ],
        "example_queries": [
            "SELECT ResourceName, AreaName, QueueCount, WaitingLots, WaitingUnits, AvgQueuedLag_Hours, MaxQueuedLag_Hours FROM Bottleneck_MCP.vw_ResourceQueue_Main WHERE FacilityKey = 2 AND ShiftKey = 3 ORDER BY WaitingUnits DESC",
            "SELECT TOP 20 ResourceName, WIPCount, CurrentSEMI_E10State FROM Bottleneck_MCP.vw_ResourceWIP_Snapshot WHERE FacilityKey = 2 AND WIPCount > 15 ORDER BY WIPCount DESC",
            "SELECT ResourceKey, AgeBucket, LotCount FROM Bottleneck_MCP.vw_QueueAgeDistribution WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY ResourceKey, AgeBucket",
            "SELECT FacilityKey, ShiftKey, ResourceKey, ISNULL([0-1h],0) AS Bucket_0_1h, ISNULL([1-4h],0) AS Bucket_1_4h, ISNULL([4-24h],0) AS Bucket_4_24h, ISNULL([>24h],0) AS Bucket_Over_24h FROM (SELECT FacilityKey, ShiftKey, ResourceKey, AgeBucket, LotCount FROM Bottleneck_MCP.vw_QueueAgeDistribution WHERE FacilityKey = 2 AND ShiftKey = 1234) src PIVOT (SUM(LotCount) FOR AgeBucket IN ([0-1h],[1-4h],[4-24h],[>24h])) p ORDER BY Bucket_Over_24h DESC",
            "SELECT ResourceName, Bucket_Lt1h, Bucket_1_2h, Bucket_2_4h, Bucket_Gt4h, TotalQueued FROM Bottleneck_MCP.vw_QueueAgeDistribution_Custom WHERE FacilityKey = 2 ORDER BY Bucket_Gt4h DESC",
            "SELECT TOP 20 LotName, MaterialName, ResourceName, AgeHours, Priority FROM Bottleneck_MCP.vw_WaitingLots_Details WHERE FacilityKey = 2 AND AgeHours > 24 ORDER BY AgeHours DESC",
        ],
    },
    5: {
        "name": "Throughput & Target",
        "keywords": [
            "throughput", "pph", "actual vs target", "pieces per hour", "target",
            "will we make", "lost production", "behind plan",
            # v3.6.4 STEP 5 additions
            "mtd", "month-to-date", "month to date", "mtd forecast",
            "end-of-month forecast", "eom forecast", "forecast units",
            "monthly target", "monthly plan", "achievement percent",
            "will we hit the monthly target", "month-end gap", "gap to target",
            "risk level", "trajectory",
            "flow velocity", "velocity heatmap", "material flow heatmap",
        ],
        "description": "Track actual production rate (PPH) vs target, projection to end-of-shift, and units lost to bottleneck downtime/queue.",
        "summary_view": {
            "name": "vw_ResourceThroughput_Main",
            "columns": ["FacilityKey", "AreaKey", "ShiftKey", "ResourceKey", "ResourceName", "ActualUnits", "ActualPPH", "ShiftHours"],
        },
        "detail_views": [
            {"name": "vw_ShiftTarget", "columns": ["FacilityKey", "ShiftKey", "TargetQty", "TargetUnits", "ActualSoFar", "RemainingSec", "ProjectedToEndOfShift", "AchievementStatus", "Shortfall_Units", "Shortfall_Pct"], "use_when": "Target vs actual with projection to end of shift; Shortfall_Units/Pct are pre-computed (no need to subtract by hand)"},
            {"name": "vw_LostProduction", "columns": ["FacilityKey", "ShiftKey", "ResourceKey", "ResourceName", "UnitsLostToDowntime", "UnitsLostToQueue", "TotalUnitsLost", "LossCategory", "LostUnitsByCategory"], "use_when": "Units lost to bottleneck downtime and queue accumulation. v3.6.0 adds LossCategory ('Downtime' / 'Queue' / 'Quality' / 'Other') and LostUnitsByCategory so the LLM can attribute losses to a category without recomputing."},
            {"name": "vw_LossPareto", "columns": ["FacilityKey", "AreaKey", "AreaName", "LossCategory", "LostUnits", "PctOfTotal", "CumulativePct", "RankN"], "use_when": "Pareto roll-up of loss categories per area (3 rows). Use for '80/20 of losses' / 'which loss category is biggest' / 'show me the Pareto'. Rows are pre-ranked; surface RankN, PctOfTotal, CumulativePct directly."},
            {"name": "vw_FlowVelocity", "columns": ["FacilityKey", "AreaKey", "AreaName", "ShiftKey", "Day", "UnitsIn", "UnitsOut", "AvgPPH", "TargetPPH"], "use_when": "Area-level flow velocity per shift. UnitsIn/UnitsOut are area-aggregated material counts; AvgPPH is the realised area parts-per-hour. TargetPPH depends on FactTargets (currently empty -> NULL)."},
            {"name": "vw_FacilityTarget_MTD_Forecast",
             "columns": ["FacilityKey","FacilityName","MonthStart","DaysElapsed","DaysRemaining","ActualUnitsMTD","TargetUnitsMTD","AchievementPct","RequiredDailyRateRemaining","ForecastEOMUnits","GapToTargetEOM","RiskLevel","IsEstimatedDefault"],
             "use_when": "v3.6.4 STEP 5: per (Facility, MonthStart) MTD actuals + linear-trajectory EOM forecast + RiskLevel ('High' / 'Medium' / 'Low' / 'Unknown'). IsEstimatedDefault = 1 means TargetUnitsMTD/EOM was derived from the trailing 30-day average daily production (FactTargets is currently empty); the LLM should disclose 'target is an estimate, not a recorded plan'. Use for 'will we hit the monthly target?', 'MTD vs target', 'end-of-month forecast', 'how many units behind for the month?' style prompts."},
            {"name": "vw_FlowVelocityHeatmap", "use_when": "v3.7 NEW — flow / material-velocity heatmap. Use for 'flow velocity', 'velocity heatmap', 'material flow heatmap' style prompts. Placed here (Topic 5) because vw_FlowVelocity already lives in this topic; no dedicated 'Flow / Material Velocity' topic exists in the registry."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Areas", "vw_Shifts"],
        "example_questions": [
            "What is the actual PPH on Resource X this shift?",
            "Will we meet the target today?",
            "How far behind plan are we right now?",
            "How many units have we lost to bottleneck downtime this shift?",
            "Which resource has the highest throughput?",
            # v3.6.4 STEP 5 additions
            "Are we on track to hit this month's target?",
            "What is the MTD vs target for facility 2?",
            "Forecast the end-of-month units for facility 2 at the current run rate.",
            "How many units do we still need per day to make the monthly target?",
        ],
        "example_queries": [
            "SELECT ResourceName, ActualUnits, ActualPPH FROM Bottleneck_MCP.vw_ResourceThroughput_Main WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY ActualPPH DESC",
            "SELECT ShiftKey, TargetUnits, ActualSoFar, Shortfall_Units, Shortfall_Pct, AchievementStatus FROM Bottleneck_MCP.vw_ShiftTarget WHERE FacilityKey = 2 AND ShiftKey = 3",
            "SELECT TOP 10 ResourceName, UnitsLostToDowntime, UnitsLostToQueue, TotalUnitsLost FROM Bottleneck_MCP.vw_LostProduction WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY TotalUnitsLost DESC",
            "SELECT AreaName, Day, ShiftKey, UnitsIn, UnitsOut, AvgPPH FROM Bottleneck_MCP.vw_FlowVelocity WHERE FacilityKey = 2 ORDER BY Day DESC, ShiftKey",
        ],
    },
    6: {
        "name": "Downtime & Utilization",
        "keywords": ["downtime", "utilization", "uptime", "mtbf", "mttr", "down time", "semi e10", "state distribution", "downtime reason", "area downtime", "area-level downtime", "downtime by area", "downtime pareto", "top downtime reasons by area", "reason pareto"],
        "description": "Track equipment up/down time, SEMI E10 state distribution, and dominant downtime reasons per resource. EffectiveDowntimePct (= DownTime + Standby) is the headline figure when DownTimePct = 0 — IsStandbyFallback flags whether the value came from real Down events (0) or a Standby idle proxy (1). Both are pre-computed in the data layer.",
        "summary_view": {
            "name": "vw_ResourceDowntime_Main",
            "columns": ["FacilityKey", "AreaKey", "AreaName", "ShiftKey", "ResourceKey", "ResourceName", "UpTimeHours", "DownTimeHours", "DownTimePct", "UpTimePct", "IdleTimeHours", "MTBFHours", "MTTRHours", "StandbyTimePct", "EffectiveDowntimePct", "IsStandbyFallback"],
        },
        "detail_views": [
            {"name": "vw_SEMIE10StateDistribution", "columns": ["FacilityKey", "ShiftKey", "ResourceKey", "ResourceName", "ProductiveHours", "ProductivePct", "StandbyHours", "StandbyPct", "EngineeringHours", "ScheduledDownHours", "UnscheduledDownHours", "NonscheduledHours", "SubState", "StandbyCauseCategory"], "use_when": "Hours and pct per SEMI E10 state (Productive, Standby, Engineering, Scheduled Down, Unscheduled Down, Nonscheduled). v3.4.0: SubState (finer-grained state classification) + StandbyCauseCategory (why-was-it-idle classifier; 'Unclassified' when reason text doesn't match any known category)."},
            {"name": "vw_DowntimeReasons", "columns": ["FacilityKey", "ShiftKey", "ResourceKey", "ResourceName", "ReasonCode", "ReasonName", "EventCount", "TotalHours"], "use_when": "Reason codes with frequency and total hours per resource"},
            {"name": "vw_AtRiskResources", "columns": ["ResourceKey", "ResourceName", "UtilizationPct", "QueueGrowthSlope", "RiskScore"], "use_when": "Resources nearing bottleneck status. RiskScore = (UtilizationPct/100)*0.6 + (QueueGrowthSlope>0?0.4:0). Use for 'who is at risk', 'pre-emptive intervention'. UtilizationPct here is the proxy form (NULL IdealQuantity caveat)."},
            {"name": "vw_DowntimeComparison_DayNight", "columns": ["FacilityKey", "AreaKey", "AreaName", "DayShiftDowntimePct", "NightShiftDowntimePct", "DayShiftEffectiveDowntimePct", "NightShiftEffectiveDowntimePct", "DayShiftMTBF", "NightShiftMTBF", "DayShiftMTTR", "NightShiftMTTR", "DowntimeDelta_Pct"], "use_when": "Per-area Day-vs-Night downtime comparison. DowntimeDelta_Pct = NightEffective - DayEffective. MTBF/MTTR currently NULL across rows (P1 plant-side ask). Use for 'is downtime worse on day or night shift in area X' questions."},
            {"name": "vw_AreaDowntime", "use_when": "v3.7 NEW — area-level downtime aggregation. Use for 'area downtime' / 'area-level downtime' / 'downtime by area' style prompts."},
            {"name": "vw_DowntimeReasonByArea_Pareto", "use_when": "v3.7 NEW — Pareto of downtime reasons per area. Use for 'downtime pareto', 'top downtime reasons by area', 'reason pareto' style prompts. Also referenced from Topic 25 (Loss Pareto)."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "How long was Resource X down this shift?",
            "What was the dominant downtime reason on the bottleneck?",
            "Show me the SEMI E10 state distribution for Area 5.",
            "Which resource has the worst MTBF?",
            "What percentage of time is each resource productive?",
            "Show me area-level downtime for facility 2.",
            "Pareto the top downtime reasons by area.",
        ],
        "example_queries": [
            "SELECT TOP 5 ResourceName, DownTimePct, EffectiveDowntimePct, IsStandbyFallback FROM Bottleneck_MCP.vw_ResourceDowntime_Main WHERE FacilityKey = 2 ORDER BY EffectiveDowntimePct DESC",
            "SELECT ResourceName, ProductivePct, StandbyHours, UnscheduledDownHours FROM Bottleneck_MCP.vw_SEMIE10StateDistribution WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY UnscheduledDownHours DESC",
            "SELECT ResourceName, ReasonName, EventCount, TotalHours FROM Bottleneck_MCP.vw_DowntimeReasons WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY TotalHours DESC",
            "SELECT TOP 10 ResourceName, UtilizationPct, QueueGrowthSlope, RiskScore FROM Bottleneck_MCP.vw_AtRiskResources ORDER BY RiskScore DESC",
            "SELECT AreaName, DayShiftEffectiveDowntimePct, NightShiftEffectiveDowntimePct, DowntimeDelta_Pct FROM Bottleneck_MCP.vw_DowntimeComparison_DayNight WHERE FacilityKey = 2 ORDER BY ABS(DowntimeDelta_Pct) DESC",
        ],
    },
    7: {
        "name": "Historical Trend",
        "keywords": ["history", "trend", "evolved", "this week", "this month", "most frequent bottleneck", "frequency", "over time", "hour of shift", "by hour", "hourly bottleneck pattern", "throughput delta", "24-hour throughput", "emerging equipment issue"],
        "description": "Track how the bottleneck has moved over time. One row per shift indicating the identified bottleneck, plus frequency rank and cycle-time trend.",
        "summary_view": {
            "name": "vw_BottleneckHistory",
            "columns": ["ShiftKey", "Day", "StartDateTime", "EndDateTime", "FacilityKey", "FacilityName", "AreaKey", "AreaName", "BottleneckResourceKey", "BottleneckResourceName", "BottleneckScore", "ET", "QueueLength", "UnitsProduced", "Throughput", "DowntimePct", "UtilizationPct"],
        },
        "detail_views": [
            {"name": "vw_BottleneckFrequency", "columns": ["FacilityKey", "AreaKey", "AreaName", "ResourceKey", "ResourceName", "TimesBottleneck", "TotalShifts", "FrequencyPct", "FirstDay", "LastDay", "BottleneckHours", "LostHours", "LostUnits"], "use_when": "Resources ranked by how often they were the shift bottleneck inside the last 30 days (anchored on MAX(FactShift.Day), not GETDATE()). BottleneckHours = total hours as bottleneck. LostHours/LostUnits depend on FactTargets (currently empty -> NULL). FirstDay/LastDay give the bottleneck date span. 0 rows means no bottleneck activity in the 30-day window."},
            {"name": "vw_PersistentBottleneck", "columns": ["FacilityKey", "ResourceKey", "ResourceName", "RunLength", "FirstShift", "LastShift", "FirstDay", "LastDay"], "use_when": "Resources that were the bottleneck for >= 2 consecutive shifts (RunLength is the streak length). Use for 'persistent bottleneck' / 'X shifts in a row' / 'who keeps showing up as bottleneck'."},
            {"name": "vw_BottleneckPrediction_NextShift", "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "Probability", "BasisFactors"], "use_when": "Per-resource heuristic probability of being the next-shift bottleneck (0.0-1.0). Probability = 0.6*(recent_bottleneck_count/5) + 0.4*(positive_queue_growth?1:0). BasisFactors describes the components contributing to the score."},
            {"name": "vw_CycleTimeTrend", "columns": ["FacilityKey", "ResourceKey", "ResourceName", "Day", "AvgCycleTime", "IdealCycleTime", "ShiftCount"], "use_when": "Daily cycle-time trend per resource"},
            {"name": "vw_StatisticalCorrelations", "columns": ["CorrelationName", "ResourceKey", "ResourceName", "PearsonR", "PValue", "SampleSize", "LinearVsThreshold", "KneePoint"], "use_when": "Pearson correlations + linearity flag for three facility-wide pairs (WIP_vs_BottleneckFreq, CycleVar_vs_BottleneckScore, Queue_vs_Wait), plus per-resource breakdown. ResourceKey IS NULL = facility-wide row. Use for 'WIP vs bottleneck frequency', 'cycle variance vs bottleneck severity', 'queue vs wait' questions."},
            {"name": "vw_BottleneckByHourOfShift", "use_when": "v3.7.1 NEW wiring — intra-shift bottleneck heatmap by hour-of-shift. Pre-computed/declarative form of the per-hour pattern that sp_BottleneckAnalysis emits. Use for 'hour of shift', 'by hour', 'hourly bottleneck pattern', 'shift-start vs shift-end bottleneck location' style prompts. Also routed via EXTRA_ROUTINGS.time_slice_intra_shift."},
            {"name": "vw_ThroughputDelta_24h", "use_when": "v3.7.1 NEW wiring — 24-hour rolling throughput delta. Flag emerging equipment issues (>=20% drop in last 24h). Use for 'show me resources where Effective Throughput dropped by more than 20% in the last 24 hours' style prompts."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources", "vw_Shifts"],
        "example_questions": [
            "How did the bottleneck evolve over the last 7 days?",
            "Which resource has been the bottleneck most often this month?",
            "Show me the daily bottleneck history for Facility 4266.",
            "Which resources have been the bottleneck for 2+ shifts in a row?",
            "Predict next shift's bottleneck.",
            "What is the cycle time trend for Resource X over February?",
            "Which resources show up as bottleneck more than 20% of the time?",
        ],
        "example_queries": [
            "SELECT Day, AreaName, BottleneckResourceName, BottleneckScore, ET, QueueLength, Throughput, DowntimePct, UtilizationPct FROM Bottleneck_MCP.vw_BottleneckHistory WHERE FacilityKey = 2 AND Day BETWEEN '2026-02-01' AND '2026-02-28' ORDER BY Day, StartDateTime",
            "SELECT TOP 10 AreaName, ResourceName, TimesBottleneck, TotalShifts, FrequencyPct, BottleneckHours, FirstDay, LastDay FROM Bottleneck_MCP.vw_BottleneckFrequency WHERE FacilityKey = 2 ORDER BY TimesBottleneck DESC",
            "SELECT ResourceName, RunLength, FirstShift, LastShift, FirstDay, LastDay FROM Bottleneck_MCP.vw_PersistentBottleneck WHERE FacilityKey = 2 ORDER BY RunLength DESC",
            "SELECT TOP 10 ResourceName, Probability, BasisFactors FROM Bottleneck_MCP.vw_BottleneckPrediction_NextShift WHERE FacilityKey = 2 ORDER BY Probability DESC",
            "SELECT Day, AvgCycleTime, IdealCycleTime FROM Bottleneck_MCP.vw_CycleTimeTrend WHERE FacilityKey = 2 AND ResourceKey = 17 AND Day BETWEEN '2026-02-01' AND '2026-02-28' ORDER BY Day",
            "SELECT CorrelationName, PearsonR, PValue, SampleSize, LinearVsThreshold, KneePoint FROM Bottleneck_MCP.vw_StatisticalCorrelations WHERE ResourceKey IS NULL ORDER BY CorrelationName",
        ],
    },
    8: {
        "name": "Comparative Analysis",
        "keywords": ["comparison", "shift a vs shift b", "today vs yesterday", "compare", "delta", "area vs area", "side by side"],
        "description": "Compare metrics shift-vs-shift and area-vs-area. Returns deltas for cycle time, throughput, downtime, queue.",
        "summary_view": {
            "name": "vw_ShiftComparison",
            "columns": ["FacilityKey", "AreaKey", "ShiftKey_A", "ShiftName_A", "ShiftKey_B", "ShiftName_B", "ResourceKey", "ResourceName", "CycleTimeDelta", "ThroughputDelta", "DownTimePctDelta", "QueueDelta"],
        },
        "detail_views": [
            {"name": "vw_AreaComparison", "columns": ["FacilityKey", "AreaKey", "AreaName", "ShiftKey", "BottleneckResourceName", "AvgCycleTime", "TotalQueue", "TotalDownTimePct", "OverallET"], "use_when": "Area-vs-area summary - one row per area per shift"},
            {"name": "vw_FacilityComparison", "columns": ["MetricName", "Facility1_Key", "Facility1_Name", "Facility1_Value", "Facility2_Key", "Facility2_Name", "Facility2_Value", "Delta"], "use_when": "Side-by-side facility KPI comparison (AvgCycleTime / AvgThroughput / AvgDowntimePct / AvgUtilizationPct). One row per metric. When only one facility has data, Facility2_* are NULL."},
            {"name": "vw_ResourcePeerBenchmark", "columns": ["ResourceKey", "ResourceName", "PeerGroupKey", "PeerGroupName", "PeerAvgET", "PeerMedianET", "PeerAvgUtilization", "PeerAvgUtil", "ResourceETvsPeerPct", "PerformanceRank"], "use_when": "Each resource's ET / Util compared against its area peers (PeerGroupKey=AreaKey, proxy until FlowKey/ProductionLine exist). ResourceETvsPeerPct < 0 means slower than peers. PeerMedianET (added v3.2.0) is the peer-group median ET; PerformanceRank (added v3.2.0) ranks resources within the peer group with 1 = best (highest ET)."},
            {"name": "vw_ShiftHandoverImpact", "columns": ["FacilityKey", "AreaKey", "AreaName", "ShiftBoundaryDateTime", "LastHourDayET", "FirstHourNightET", "DeltaPct"], "use_when": "Day-to-Night handover impact at area level. DeltaPct < 0 = throughput drops at the handover."},
            {"name": "vw_DowntimeComparison_DayNight", "columns": ["FacilityKey", "AreaKey", "AreaName", "DayShiftDowntimePct", "NightShiftDowntimePct", "DayShiftEffectiveDowntimePct", "NightShiftEffectiveDowntimePct", "DayShiftMTBF", "NightShiftMTBF", "DayShiftMTTR", "NightShiftMTTR", "DowntimeDelta_Pct"], "use_when": "Per-area Day-vs-Night downtime side-by-side. Use for 'compare Day vs Night downtime in area X' questions. MTBF/MTTR currently NULL (P1 plant-side ask)."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Areas", "vw_Shifts"],
        "example_questions": [
            "How does Day shift compare to Night shift on Resource X?",
            "Compare today's bottleneck to yesterday's.",
            "Which area has the worst overall throughput?",
            "Compare facility 4266 vs facility 'Production' overall.",
            "Does throughput drop at the Day-to-Night handover?",
            "How does SD1100 compare to its area peers?",
            "Show me cycle time delta between this shift and the previous one.",
        ],
        "example_queries": [
            "SELECT ResourceName, CycleTimeDelta, ThroughputDelta, QueueDelta FROM Bottleneck_MCP.vw_ShiftComparison WHERE FacilityKey = 2 AND ShiftKey_A = 1234 AND ShiftKey_B = 1235 ORDER BY ABS(ThroughputDelta) DESC",
            "SELECT AreaName, BottleneckResourceName, AvgCycleTime, TotalQueue, OverallET FROM Bottleneck_MCP.vw_AreaComparison WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY OverallET ASC",
            "SELECT MetricName, Facility1_Name, Facility1_Value, Facility2_Name, Facility2_Value, Delta FROM Bottleneck_MCP.vw_FacilityComparison ORDER BY MetricName",
            "SELECT TOP 10 ResourceName, PeerGroupName, PeerAvgET, PeerMedianET, ResourceETvsPeerPct, PerformanceRank FROM Bottleneck_MCP.vw_ResourcePeerBenchmark ORDER BY PerformanceRank",
            "SELECT TOP 10 AreaName, ShiftBoundaryDateTime, LastHourDayET, FirstHourNightET, DeltaPct FROM Bottleneck_MCP.vw_ShiftHandoverImpact WHERE FacilityKey = 2 ORDER BY DeltaPct ASC",
        ],
    },
    9: {
        "name": "Drill-down / What's Stuck",
        "keywords": ["what is stuck", "list materials", "materials at", "drill down", "lots at resource", "what is at", "which lots"],
        "description": "List materials currently at a given resource, and lots waiting in queue with age and priority. Use this to answer 'what is stuck on Resource X?'.",
        "summary_view": {
            "name": "vw_MaterialsAtResource",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "MaterialKey", "MaterialName", "LotName", "CurrentState", "ProductKey", "ProductName", "ArrivedAt", "AgeHours"],
        },
        "detail_views": [
            {"name": "vw_WaitingLots_Details", "columns": ["FacilityKey", "ShiftKey", "ResourceKey", "ResourceName", "MaterialKey", "MaterialName", "LotName", "QueuedAt", "AgeHours", "Priority"], "use_when": "Per-lot waiting detail (queued only) with age and priority"},
            {"name": "vw_LotJourney", "columns": ["LotName", "MaterialKey", "MaterialName", "StepSeq", "StepKey", "StepName", "ResourceKey", "ResourceName", "TrackInTime", "TrackOutTime", "QueueSec", "ProcessSec"], "use_when": "End-to-end step trace for a single lot. One row per (Lot, Step), ordered by StepSeq. Use this for 'lot history', 'show me the journey of lot K9B381421', 'where has this lot been'. Filter on LotName / MaterialName."},
            {"name": "vw_MaterialFlowJourney_Detail", "columns": ["LotName", "MaterialKey", "MaterialName", "StepSeq", "StepKey", "StepName", "ResourceKey", "ResourceName", "TrackInTime", "TrackOutTime", "QueueSec", "ProcessSec", "ProcessingTimeSec", "TransitTimeSec", "TotalLeadTimeSec", "HeatmapBucket"], "use_when": "Per-(Lot,Step) detail layered on top of vw_LotJourney with explicit ProcessingTimeSec / TransitTimeSec / TotalLeadTimeSec, plus HeatmapBucket ('low' / 'medium' / 'high' via NTILE on TotalLeadTimeSec) for visual heatmaps and slow-step detection."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "What materials are at Resource X right now?",
            "List the lots stuck at the bottleneck resource.",
            "Which lots are queued at Resource X and how old are they?",
            "What is the highest priority lot waiting at Resource Y?",
            "Show me the journey of lot K9B381421.",
            "Trace the path of MaterialName 'S4442R031Y23'.",
        ],
        "example_queries": [
            "SELECT LotName, MaterialName, ProductName, CurrentState, AgeHours FROM Bottleneck_MCP.vw_MaterialsAtResource WHERE FacilityKey = 2 AND ResourceKey = 17 ORDER BY AgeHours DESC",
            "SELECT TOP 20 LotName, MaterialName, AgeHours, Priority FROM Bottleneck_MCP.vw_WaitingLots_Details WHERE FacilityKey = 2 AND ResourceKey = 17 ORDER BY Priority ASC, AgeHours DESC",
            "SELECT ResourceName, COUNT(*) AS LotCount FROM Bottleneck_MCP.vw_MaterialsAtResource WHERE FacilityKey = 2 AND CurrentState IN ('Queued','InProcess') GROUP BY ResourceName ORDER BY LotCount DESC",
            "SELECT StepSeq, StepName, ResourceName, TrackInTime, QueueSec, ProcessSec FROM Bottleneck_MCP.vw_LotJourney WHERE LotName LIKE '%K9B381421%' ORDER BY StepSeq",
            "SELECT TOP 10 LotName, StepSeq, StepName, ResourceName, ProcessingTimeSec, TransitTimeSec, TotalLeadTimeSec, HeatmapBucket FROM Bottleneck_MCP.vw_MaterialFlowJourney_Detail ORDER BY TotalLeadTimeSec DESC",
        ],
    },
    10: {
        "name": "Rework Impact",
        "keywords": [
            "rework", "rework count", "rework reason", "reworked lots",
            "rework impact", "redo",
            # v3.6.4 STEP 5 additions (defect Pareto)
            "defect", "defects", "defect type", "defect category",
            "defect pareto", "top defects", "top defect types",
            "top 3 defects", "defect contribution", "defect rank",
            "quality issues", "scrap categories", "defect mix",
        ],
        "description": "Track rework count and reasons per resource. Rework load is also rolled into vw_Summary_ShiftBottleneck.ReworkInflatedQuantity for combined primary+rework ECT/ET.",
        "summary_view": {
            "name": "vw_ReworkImpact_Main",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "ShiftKey", "MaterialKey", "MaterialName", "ReworkCount", "TotalProcessed", "ReworkPercentOfVolume", "ReworkReason", "TopReworkReason", "ReworkCycleTimeImpactSec", "ReworkInflatedQty", "TotalOutputQty", "PctThroughputConsumedByRework", "BaselineET", "ActualET", "ETReductionPct", "InputUnits", "ScrapUnits", "ScrapPct", "TopDefectCategory"],
            "column_notes": "One row per (Resource, Shift, Material, ReworkReason). ReworkReason names the dbo.DimReason for the rework event ('-' when no reason logged). MaterialName narrows rework to a specific lot/material. PctThroughputConsumedByRework (added v3.2.0) = ReworkInflatedQty / TotalOutputQty * 100. ETReductionPct (added v3.2.0) = (BaselineET - ActualET) / BaselineET * 100; NULL where source data is missing (P4 plant-side ask). v3.3.0 emphasis: when answering rework-impact questions ALWAYS surface (a) ReworkInflatedQuantity (per-shift) — pulled from vw_Summary_ShiftBottleneck.ReworkInflatedQuantity for the resource+shift, (b) TopReworkReason from this view, and (c) Yield % per resource via vw_ResourceThroughput_Main.Yield (compare against ReworkPercentOfVolume). v3.4.0 additions: InputUnits, ScrapUnits, ScrapPct (= ScrapUnits / InputUnits * 100), and TopDefectCategory (mapped via Bottleneck_MCP.DimDefectType seed). When TopDefectCategory='Uncategorized' (no naming-pattern match), disclose that the defect taxonomy is not yet mapped at the material level.",
        },
        "detail_views": [
            {"name": "DimDefectType",
             "columns": ["DefectTypeKey", "DefectCategory", "DefectName", "Description"],
             "use_when": "Read-only seed dim mapping defect names to defect category. 6 default rows (Mechanical, Electrical, Cosmetic, Material, Process, Uncategorized). Lookup target for vw_ReworkImpact_Main.TopDefectCategory."},
            {"name": "vw_QualityDefectPareto",
             "columns": ["FacilityKey","AreaKey","DefectTypeKey","DefectTypeName","DefectCount","ContributionPct","CumulativePct","RankInArea","IsTop3","IsLowSampleConfidence"],
             "use_when": "v3.6.4 STEP 5: per (Facility, Area, DefectType) Pareto roll-up of defect counts with ContributionPct, CumulativePct (running 80/20 view), RankInArea, IsTop3 flag. Source-substitution note: dbo.FactScrap does not exist in the current schema; defect counts are sourced from vw_ReworkImpact_Main.ScrapUnits joined to DimDefectType via TopDefectCategory. IsLowSampleConfidence = 1 when the entire area is 'Uncategorized' (taxonomy not yet mapped at the material level) — disclose that. Use for 'top 3 defect types in area X', 'defect Pareto', 'what's driving 80% of defects' style prompts."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "How is rework affecting the bottleneck?",
            "Which resource has the most rework events this shift?",
            "What are the top rework reasons on Resource X?",
            "What % of total work on Resource X is rework?",
            "Which material has the most rework on the bottleneck resource?",
            # v3.6.4 STEP 5 additions (defect Pareto)
            "What are the top 3 defect types in area 5?",
            "Show me the defect Pareto for facility 2.",
            "Which defect categories drive 80% of defects in area X?",
        ],
        "example_queries": [
            "SELECT TOP 10 ResourceName, MaterialName, ReworkCount, TotalProcessed, ReworkPercentOfVolume, PctThroughputConsumedByRework, ETReductionPct, ReworkReason FROM Bottleneck_MCP.vw_ReworkImpact_Main WHERE FacilityKey = 2 AND ShiftKey = 1234 ORDER BY ReworkPercentOfVolume DESC",
            "SELECT ResourceName, ReworkReason, SUM(ReworkCount) AS Total FROM Bottleneck_MCP.vw_ReworkImpact_Main WHERE FacilityKey = 2 AND ReworkCount > 0 GROUP BY ResourceName, ReworkReason ORDER BY Total DESC",
            "SELECT ResourceName, ReworkInflatedQuantity, IsBottleneck FROM Bottleneck_MCP.vw_Summary_ShiftBottleneck WHERE FacilityKey = 2 AND ShiftKey = 1234 AND ReworkInflatedQuantity > 0 ORDER BY ReworkInflatedQuantity DESC",
        ],
    },
    11: {
        "name": "Operator at Bottleneck",
        "keywords": ["operator", "who ran", "which operator", "operator cycle time", "employee", "who worked", "batch size", "batching strategy", "large batches", "small batches"],
        "description": "Identify operators who worked on the bottleneck resource and their per-operator cycle times. Use sp_ResourceDetailLookup for fuzzy resource search.",
        "summary_view": {
            "name": "vw_OperatorAtResource",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "ResourceType", "ShiftKey", "EmployeeKey", "UserName", "UnitsProcessed", "AvgCycleTimeSec", "PeerAvgCycleTimeSec", "PerformanceDeviationPct", "Yield", "ReworkCount", "ET", "DowntimePct_PerOperator", "StdDevCycleSec", "BottleneckShiftCount", "SkillLevel"],
            "column_notes": "Yield = ParcelOut/ParcelIn (NULL when in/out qty=0). ReworkCount = sum of rework events for the operator at the resource. ET = UnitsProcessed*3600/SUM(CycleSeconds), units/hour, mirrors vw_BottleneckRanked.ET. v3.3.0 additions: DowntimePct_PerOperator (per-operator share of downtime while assigned), StdDevCycleSec (consistency), BottleneckShiftCount (shifts where this operator was on the bottleneck), SkillLevel (literal 'Unrated' until DimEmployee gets a skill column — disclose the source caveat).",
        },
        "detail_views": [
            {"name": "vw_ResourcePeerBenchmark", "columns": ["ResourceKey", "ResourceName", "PeerGroupName", "PeerAvgET", "PeerMedianET", "ResourceETvsPeerPct", "PerformanceRank"], "use_when": "Cross-link from operator analysis to resource-vs-peer benchmark. PeerMedianET / PerformanceRank (1=best) added v3.2.0 — useful for 'is the operator slow because the resource itself underperforms peers?' questions."},
            {"name": "vw_BatchSizeProfile", "use_when": "v3.7.1 NEW wiring — batch-size profile per resource. Use for 'batch size', 'batching strategy', 'large batches vs small batches' style prompts (e.g. 'compare bottleneck occurrence for resources processing large batches >50 units vs small batches <20 units'). Placed under Topic 11 because batching is most often raised in the operator/process-discipline context."},
        ],
        "stored_procedure": {"name": "sp_ResourceDetailLookup", "params": {"SearchTerm": "required NVARCHAR(256) - fuzzy resource-name match", "FacilityKey": "optional BIGINT"}, "use_when": "Fuzzy resource-name search returning current state and queue length"},
        "parameter_views": ["vw_Facilities", "vw_Resources", "vw_Shifts"],
        "example_questions": [
            "Which operators ran the bottleneck resource this shift?",
            "Who had the slowest cycle time on Resource X?",
            "Which operator on the bottleneck resource had the worst yield?",
            "Who has the highest rework count this shift?",
            "Compare per-operator ET on the bottleneck resource.",
        ],
        "example_queries": [
            "SELECT TOP 20 ResourceName, UserName, UnitsProcessed, AvgCycleTimeSec, Yield, ReworkCount, ET FROM Bottleneck_MCP.vw_OperatorAtResource WHERE FacilityKey = 2 AND ShiftKey = 2 ORDER BY ET ASC",
            "SELECT UserName, COUNT(DISTINCT ResourceKey) AS ResourcesWorked, AVG(Yield) AS AvgYield FROM Bottleneck_MCP.vw_OperatorAtResource WHERE FacilityKey = 2 AND ShiftKey = 2 GROUP BY UserName ORDER BY AvgYield ASC",
            "SELECT ResourceName, UserName, AvgCycleTimeSec, PeerAvgCycleTimeSec, PerformanceDeviationPct, Yield, ReworkCount FROM Bottleneck_MCP.vw_OperatorAtResource WHERE FacilityKey = 2 AND ResourceKey = 49 AND ShiftKey = 2 ORDER BY PerformanceDeviationPct DESC",
        ],
    },
    12: {
        "name": "Predictive / What-If",
        "keywords": ["predictive", "what if", "spare capacity", "alternate resource", "queue overflow", "queue growth", "parallel resource", "when will", "cascade", "ripple", "upstream backup", "downstream starvation"],
        "description": "Predictive views: which alternate qualified resources have spare capacity to absorb load, and how fast queues are growing. HoursToOverflow / MaxCapacity are pre-computed columns on vw_QueueGrowthRate (no manual division needed); IsCapacityHeuristic = 1 means MaxCapacity was a fallback (2 * QueueAtHour). For cascade/ripple impact, call sp_BottleneckCascade for a single-call 3-result-set bundle (Bottleneck | UpstreamBackup | DownstreamStarvation).",
        "summary_view": {
            "name": "vw_ParallelResourceCapacity",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "ResourceType", "Model", "CurrentUtilizationPct", "SpareCapacityPct", "CurrentQueueLength", "CouldAbsorbUnits", "LoadBalanceScore", "UnderutilizedSpareUnits", "IsCapacityHeuristic"],
            "column_notes": "LoadBalanceScore = stdev(QueueCount)/avg(QueueCount) over the (Area, ResourceType) peer group; lower = better balance, NULL when group has only one resource. UnderutilizedSpareUnits = unused WIP slots across non-bottleneck peers (can be negative when WIP exceeds heuristic capacity). IsCapacityHeuristic=1 means MaxCapacity fell back to 2*QueueLength.",
        },
        "detail_views": [
            {"name": "vw_QueueGrowthRate", "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "HourBucket", "QueueAtHour", "QueueAtEnd", "MaxCapacity", "QueueDelta", "GrowthLotsPerHour", "GrowthRate", "HoursToOverflow", "IsCapacityHeuristic"], "use_when": "Hourly queue-length delta per resource; HoursToOverflow / MaxCapacity / IsCapacityHeuristic are pre-computed (do not divide manually)."},
            {"name": "vw_WhatIf_AddResource", "columns": ["BottleneckResourceKey", "BottleneckResourceName", "ProjectedQueueReductionPct", "ProjectedETGainPct", "BasisFactors"], "use_when": "What-if heuristic: how would adding a parallel resource affect a bottleneck's queue and ET. ProjectedQueueReductionPct=50 (two resources halve the queue); ProjectedETGainPct uses CurrentET vs CurrentET/2. BasisFactors documents the assumption."},
            {"name": "vw_BottleneckByProduct", "columns": ["FacilityKey", "ProductKey", "ProductName", "ProductionVolumeBucket", "BottleneckResourceKey", "BottleneckResourceName", "AvgQueue", "ET", "ShiftKey", "Day", "ShiftName", "UnitsProduced"], "use_when": "Bottleneck attributed to its dominant product. ProductionVolumeBucket = High/Medium/Low (NTILE quartile of total UnitsOut per Product). v3.6.1 enriched with ShiftKey/Day/ShiftName/UnitsProduced so the LLM can answer per-shift product-bottleneck questions without an extra join. Use for 'which product is the bottleneck on?' or 'high-volume products that hit bottlenecks' or 'product bottleneck on Day vs Night shift'."},
        ],
        "stored_procedure": {"name": "sp_BottleneckCascade", "params": {"FacilityKey": "required BIGINT", "ShiftKey": "required BIGINT", "DateFrom": "optional DATETIME (defaults to last 7 shifts)", "DateTo": "optional DATETIME"}, "use_when": "Single-call cascade impact: returns 4 result sets — Bottleneck | UpstreamBackup | DownstreamStarvation | CascadeTimeline."},
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "Which parallel resources can absorb load from the bottleneck?",
            "When will the queue at Resource X overflow?",
            "Show me alternate qualified resources with spare capacity.",
            "How fast is the queue growing on Resource X?",
            "What if we added one more LM1100 — how would queue and ET change?",
            "Which product is the bottleneck most often on?",
            "Show the cascade / ripple impact of the current bottleneck (upstream backup + downstream starvation + timeline).",
        ],
        "example_queries": [
            "SELECT TOP 10 ResourceName, CurrentUtilizationPct, SpareCapacityPct, CurrentQueueLength, LoadBalanceScore, UnderutilizedSpareUnits, IsCapacityHeuristic FROM Bottleneck_MCP.vw_ParallelResourceCapacity WHERE FacilityKey = 2 ORDER BY SpareCapacityPct DESC",
            "SELECT ResourceName, QueueAtEnd, MaxCapacity, GrowthLotsPerHour, HoursToOverflow, IsCapacityHeuristic FROM Bottleneck_MCP.vw_QueueGrowthRate WHERE FacilityKey = 2 AND GrowthLotsPerHour > 0 ORDER BY HoursToOverflow ASC",
            "SELECT BottleneckResourceName, ProjectedQueueReductionPct, ProjectedETGainPct, BasisFactors FROM Bottleneck_MCP.vw_WhatIf_AddResource ORDER BY ProjectedETGainPct DESC",
            "SELECT ProductName, ProductionVolumeBucket, BottleneckResourceName, AvgQueue, ET FROM Bottleneck_MCP.vw_BottleneckByProduct WHERE FacilityKey = 2 ORDER BY AvgQueue DESC",
            "SELECT ProductName, ShiftName, Day, ShiftKey, BottleneckResourceName, UnitsProduced, AvgQueue, ET FROM Bottleneck_MCP.vw_BottleneckByProduct WHERE FacilityKey = 2 AND ShiftName = 'Day' ORDER BY Day DESC, AvgQueue DESC",
            "EXEC Bottleneck_MCP.sp_BottleneckCascade @FacilityKey = 2, @ShiftKey = 3",
        ],
    },

    # ========================================================================
    # UC2 — Topic 13: Equipment OEE Report
    # Spec: Docs/UC4_Bottleneck_Analysis/UC2_UC5_UC7_Merge_Proposal.md §3
    # ========================================================================
    13: {
        "name": "Equipment OEE Report",
        "keywords": [
            "oee", "overall equipment effectiveness", "availability", "performance", "quality",
            "availability x performance x quality", "semi e10 oee", "iso 22400", "equipment oee",
            "machine oee", "oee chart", "oee for the day", "oee for the week", "oee per shift",
            "oee for resource", "oee report", "efficient rate", "track efficiency", "effectiveness",
        ],
        "description": (
            "Compute SEMI-E10 OEE = Availability x Performance x Quality, exposed as a single "
            "percentage plus the three components individually. Three grains: per-shift "
            "(vw_ResourceOEE_Composite), per-day (vw_ResourceOEE_Daily), per-ISO-week "
            "(vw_ResourceOEE_Weekly). Each row exposes IdealSource ('Engineered' / 'HistoricalMean' / 'Mixed') "
            "so the LLM can flag when Performance is computed against a historical baseline instead of an engineering target."
        ),
        "summary_view": {
            "name": "vw_ResourceOEE_Composite",
            "columns": [
                "FacilityKey", "FacilityName", "AreaKey", "AreaName",
                "ResourceKey", "ResourceName", "ResourceType", "Model", "ShiftKey",
                "Availability_Pct", "Performance_Pct", "Quality_Pct", "OEE_Pct",
                "IdealSource", "UpTimeHours", "DownTimeHours",
                "ActualUnits", "IdealUnits", "QtyIn", "QtyOut",
            ],
        },
        "detail_views": [
            {"name": "vw_ResourceOEE_Daily",
             "columns": ["FacilityKey","AreaKey","ResourceKey","ResourceName","Day",
                          "Availability_Pct","Performance_Pct","Quality_Pct","OEE_Pct",
                          "IdealSource","UpTimeHours","DownTimeHours",
                          "ActualUnits","IdealUnits","QtyIn","QtyOut"],
             "use_when": "Per-resource per-day OEE roll-up; use for 'OEE for the day' / daily trend questions."},
            {"name": "vw_ResourceOEE_Weekly",
             "columns": ["FacilityKey","AreaKey","ResourceKey","ResourceName","WeekStartDate",
                          "Availability_Pct","Performance_Pct","Quality_Pct","OEE_Pct",
                          "IdealSource","UpTimeHours","DownTimeHours",
                          "ActualUnits","IdealUnits","QtyIn","QtyOut"],
             "use_when": "Per-resource per-ISO-week OEE roll-up (Monday-anchored); use for 'OEE this week / last week'."},
            {"name": "vw_ResourceOEE_Hourly",
             "columns": ["FacilityKey","AreaKey","ResourceKey","ResourceName","Day","ShiftKey",
                          "HourOfShift","HourBucketStart","HourBucketEnd",
                          "Availability_Pct","Performance_Pct","Quality_Pct","OEE_Pct",
                          "UnitsProduced_InHour","ProductiveSec_InHour","StandbySec_InHour","DownSec_InHour",
                          "IsEstimatedPerformance"],
             "use_when": "hour-of-shift granular OEE breakdown — required for prompts like 'how did OEE evolve through the shift'. 6,147 rows. IsEstimatedPerformance=1 flags hours where the Performance component is back-filled from a historical baseline."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Areas", "vw_Resources", "vw_Shifts"],
        "example_questions": [
            "What is the OEE of OV1110 last week?",
            "Show OEE for all resources in facility 4266 today.",
            "Which resource has the worst OEE this week?",
            "Break down OEE for Resource X into Availability, Performance, Quality.",
            "What was the Performance % of LM1100 yesterday?",
            "Which OEE component is dragging us down on the bottleneck resource?",
            "Show OEE trend for OV1110 over the last 7 days.",
        ],
        "example_queries": [
            "SELECT TOP 10 ResourceName, Availability_Pct, Performance_Pct, Quality_Pct, OEE_Pct, IdealSource FROM Bottleneck_MCP.vw_ResourceOEE_Composite WHERE FacilityKey = 2 ORDER BY OEE_Pct ASC",
            "SELECT ResourceName, Day, OEE_Pct, Availability_Pct, Performance_Pct, Quality_Pct FROM Bottleneck_MCP.vw_ResourceOEE_Daily WHERE FacilityKey = 2 AND ResourceName = 'OV1110' ORDER BY Day DESC",
            "SELECT ResourceName, WeekStartDate, OEE_Pct, IdealSource FROM Bottleneck_MCP.vw_ResourceOEE_Weekly WHERE FacilityKey = 2 ORDER BY WeekStartDate DESC, OEE_Pct DESC",
        ],
        "related_topics": [6, 1, 5],  # Downtime, Bottleneck, Throughput
    },

    # ========================================================================
    # UC5 — Topic 14: Cycle Time Outliers
    # Spec: Docs/UC4_Bottleneck_Analysis/UC2_UC5_UC7_Merge_Proposal.md §4
    # ========================================================================
    14: {
        "name": "Cycle Time Outliers",
        "keywords": [
            "outlier", "outliers", "unusual", "abnormal cycle time",
            "slower than usual", "longer than usual", "took longer",
            "irregular", "irregularities", "> 2 sigma", "two sigma", "sigma", "deviation",
            "histogram", "distribution", "p95 cycle time", "median cycle time",
            "cycle time outlier", "outlier lots", "by part number",
            "statistical", "std dev", "standard deviation",
            # v3.6.4 STEP 5 additions
            "drift", "drifting", "creeping up", "micro-outlier", "micro outliers",
            "110-120%", "110 to 120 percent", "110%-120% band", "early warning",
            "maintenance impact", "after maintenance", "before maintenance",
            "after pm", "before pm", "post maintenance", "pre maintenance",
            "did maintenance help", "cycle time after pm",
        ],
        "description": (
            "Identify lots whose cycle time is statistically unusual versus the (Product x Step x Resource) "
            "population. Returns descriptive statistics (mean, median, std dev, P25/P50/P75/P95), per-lot "
            "outlier flag at >|2 sigma|, and 20-bucket histogram data. ActualCycleSec = "
            "DATEDIFF(SECOND, UTCTrackInDateTime, UTCTrackOutDateTime)."
        ),
        "summary_view": {
            "name": "vw_CycleTime_Statistics",
            "columns": ["FacilityKey","FacilityName","AreaKey","AreaName",
                         "ResourceKey","ResourceName","ResourceType","Model",
                         "ProductKey","ProductName","StepKey","StepName",
                         "LotCount","MeanCycleSec","StdDevCycleSec",
                         "MinCycleSec","MaxCycleSec",
                         "MedianCycleSec","P25CycleSec","P50CycleSec","P75CycleSec","P95CycleSec"],
        },
        "detail_views": [
            {"name": "vw_CycleTime_Outliers",
             "columns": ["MaterialKey","MaterialName","LotKey","LotName","ProductName","StepName","ResourceName","ShiftKey",
                          "ActualCycleSec","MeanCycleSec","StdDevCycleSec","IdealCycleSec","DeviationPct",
                          "Sigma","IsOutlier","OperatorKey","OperatorName",
                          "LC1TrackInDateTime","LC1TrackOutDateTime","BatchID"],
             "column_notes": "BatchID parsed from MaterialName middle pipe-segment (no canonical batch column upstream).",
             "use_when": "Per-lot detail with deviation in sigma units. v3.6.0 enriched with LotKey/LotName, IdealCycleSec, DeviationPct (% over ideal), and OperatorKey/OperatorName so the LLM can name the lot and operator without a join. v3.6.1 adds BatchID (parsed from MaterialName middle pipe-segment — no canonical batch column upstream) so the LLM can group outliers by batch. Filter IsOutlier = 1 for unusual lots only."},
            {"name": "vw_CycleTime_Distribution",
             "columns": ["ProductName","StepName","ResourceName","BucketIndex",
                          "BucketLowerBound","BucketUpperBound","LotCountInBucket","GroupLotCount"],
             "use_when": "Histogram bucket data (20 buckets per group) for distribution chart rendering."},
            {"name": "vw_OutlierLotDetail",
             "columns": ["LotKey","LotName","ResourceKey","ResourceName","ShiftKey","ActualCycleSec","MeanCycleSec","Sigma","IsOutlier","OperatorKey","OperatorName","RootCauseCategory"],
             "use_when": "Per-lot outlier deep-dive (143 rows). Same grain as vw_CycleTime_Outliers but adds RootCauseCategory ('Equipment' / 'Material' / 'Operator' / 'Unclassified') so the LLM can attribute each outlier lot. Use for 'show me the worst outlier lots and why they were slow' style prompts."},
            {"name": "vw_CycleTime_MicroOutliers",
             "columns": ["LotKey","LotName","ResourceKey","ResourceName","ActualCycleSec","IdealCycleSec","DeviationPct","ShiftKey","OperatorName","BatchID"],
             "use_when": "v3.6.4 STEP 5: drift / micro-outlier feed. Lots whose ActualCycleSec is in the 110%-120% band of IdealCycleSec (DeviationPct BETWEEN 10 AND 20). NOT severe outliers — vw_CycleTime_Outliers handles |Sigma|>2. Use for 'is cycle time drifting?' / 'which lots are creeping above ideal?' / 'show me the 110-120% band' / 'micro-outliers' style prompts."},
            {"name": "vw_CycleTime_MaintenanceImpact",
             "columns": ["ResourceKey","ResourceName","MaintenanceDate","CycleSecBefore","CycleSecAfter","DeltaPct","OutlierCountBefore","OutlierCountAfter"],
             "use_when": "v3.6.4 STEP 5: cycle-time delta in the 24h windows before vs after each scheduled-maintenance event per resource. DeltaPct = (After-Before)/Before*100. Use for 'did maintenance help?' / 'cycle time after PM' / 'maintenance impact on cycle time' / 'before vs after maintenance' style prompts. Returns 0 rows when vw_ScheduledMaintenance is empty (current data window)."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources", "vw_Products"],
        "example_questions": [
            "Are there any cycle-time outliers for product CWAT02MLT-Y this week?",
            "Which lots took longer than usual on Resource SE1100?",
            "Show me the mean, median, and std dev of cycle time on the Laser Marking step.",
            "List the top 10 outlier lots by sigma for facility 4266.",
            "What is the P95 cycle time on the Seaming step for product S4442R031Y23?",
            "Give me the histogram distribution of cycle times for LM1100 on product S4442R031Y23.",
            "Which (Product, Step, Resource) group has the largest std dev in cycle time?",
            # v3.6.4 STEP 5 additions
            "Show me cycle-time drift — lots in the 110%-120% band of ideal.",
            "Which lots are micro-outliers (early warning, not yet at 2 sigma)?",
            "Did the recent maintenance help cycle time on Resource X?",
            "What is the cycle-time delta before vs after the last PM event?",
        ],
        "example_queries": [
            "SELECT TOP 20 ProductName, StepName, ResourceName, LotCount, MeanCycleSec, MedianCycleSec, StdDevCycleSec, P95CycleSec FROM Bottleneck_MCP.vw_CycleTime_Statistics WHERE FacilityKey = 2 ORDER BY StdDevCycleSec DESC",
            "SELECT TOP 20 MaterialName, ProductName, StepName, ResourceName, ActualCycleSec, MeanCycleSec, Sigma FROM Bottleneck_MCP.vw_CycleTime_Outliers WHERE FacilityKey = 2 AND IsOutlier = 1 ORDER BY ABS(Sigma) DESC",
            "SELECT BucketIndex, BucketLowerBound, BucketUpperBound, LotCountInBucket FROM Bottleneck_MCP.vw_CycleTime_Distribution WHERE FacilityKey = 2 AND ProductName = 'S4442R031Y23' AND ResourceName = 'LM1100' ORDER BY BucketIndex",
        ],
        "related_topics": [3, 7, 1],  # Cycle Time, Trend, Bottleneck
    },

    # ========================================================================
    # UC7 — Topic 15: Facility Performance / Executive View
    # Spec: Docs/UC4_Bottleneck_Analysis/UC2_UC5_UC7_Merge_Proposal.md §5
    # ========================================================================
    15: {
        "name": "Facility Performance / Executive View",
        "keywords": [
            "facility performance", "executive summary", "site performance",
            "overall performance", "facility kpi", "facility dashboard",
            "daily performance", "weekly performance", "facility report",
            "executive report", "site health", "plant health",
            "facility trend", "facility anomalies", "facility roll-up",
            "summary across the facility", "how is the facility doing",
            "executive narrative", "one paragraph summary",
        ],
        "description": (
            "Executive-style facility roll-up: per (FacilityKey, Day) headline KPIs (Total Lots Produced, OEE%, "
            "Scrap %, Avg Queue, Bottleneck of the Day, EOD WIP) plus a per-day anomaly rollup driven by "
            "vw_BottleneckAlert. Designed to feed an LLM-composed one-paragraph performance narrative. "
            "Use sp_FacilityExecutiveSummary for a single-call multi-resultset executive bundle."
        ),
        "summary_view": {
            "name": "vw_FacilityPerformance_Daily",
            "columns": ["FacilityKey","FacilityName","Day",
                         "TotalLotsProduced","OEE_Pct","ScrapRate_Pct","AvgQueueLength",
                         "BottleneckResourceKey","BottleneckResourceName","BottleneckScore",
                         "WIP_Snapshot_EOD","FirstPassYield_Pct","ReworkRate_Pct"],
            "column_notes": "v3.6.0 additions: FirstPassYield_Pct (lots that passed without rework / total lots) and ReworkRate_Pct (lots reworked / total lots). Surface both whenever the user asks about facility quality or yield.",
        },
        "detail_views": [
            {"name": "vw_FacilityKPI_Anomalies",
             "columns": ["FacilityKey","Day","AnomalyCount","AnomalyTypes",
                          "MostSevereResourceKey","MostSevereResourceName","MostSevereAlertReason"],
             "use_when": "Per-day anomaly count + alert types + most-severe resource for the facility (rollup of vw_BottleneckAlert)."},
            {"name": "vw_BottleneckAlert",
             "columns": ["FacilityKey","Day","ShiftKey","ResourceKey","ResourceName",
                          "AlertReasons","IsBottleneck","BottleneckScore",
                          "DowntimePct","UtilizationPct","QueueLength"],
             "use_when": "Drill-down: see actual shift-level alert rows behind a facility-day anomaly count."},
            {"name": "vw_AreaOEE",
             "columns": ["FacilityKey","AreaKey","AreaName","Day","Avg_Availability_Pct","Avg_Performance_Pct","Avg_Quality_Pct","Avg_OEE_Pct","ResourceCount","ProductiveHours","IsEstimatedPerformance"],
             "use_when": "Area-grain daily OEE roll-up (8 rows). Use for 'OEE for the Cleaning area today' / area-by-area OEE comparison. IsEstimatedPerformance=1 flags days where the Performance component was back-filled from a historical baseline."},
            {"name": "vw_AreaCapacityAnalysis",
             "columns": ["FacilityKey","AreaKey","AreaName","ShiftKey","Day","ResourceCount","IdealPPH","ActualPPH","CapacityUtilizationPct"],
             "use_when": "Area capacity utilisation: IdealPPH × ResourceCount vs ActualPPH per area+shift+day (25 rows). Use for 'how much spare capacity does the Cleaning area have' style prompts."},
            {"name": "vw_ShiftKPI_Summary",
             "columns": ["FacilityKey","ShiftName","Days","Avg_OEE_Pct","Avg_Throughput_PPH","Avg_ScrapRate_Pct","Avg_DowntimePct","Avg_QueueLength","Avg_FirstPassYield_Pct"],
             "use_when": "Day vs Night shift KPI roll-up (10 rows). v3.6.1 adds Avg_FirstPassYield_Pct (sourced from vw_FacilityPerformance_Daily.FirstPassYield_Pct) so quality is in the same view as OEE/Throughput/Scrap. Use for 'is Day shift outperforming Night shift across the facility' or 'Day vs Night first-pass yield' questions."},
            {"name": "vw_VolumeQualityTradeoff",
             "columns": ["FacilityKey","Day","DailyVolume","DailyFPY","PearsonR_running","RegressionSlope","RegressionIntercept","SampleSize","IsHighSampleConfidence"],
             "use_when": "Daily volume-vs-FPY trade-off (25 rows) with running Pearson R, regression slope/intercept, and IsHighSampleConfidence flag. Use for 'are we sacrificing quality when we ramp volume' style executive prompts."},
        ],
        "stored_procedure": {
            "name": "sp_FacilityExecutiveSummary",
            "params": {
                "FacilityKey": "BIGINT — required",
                "DateFrom":    "DATETIME — required (window <= 90 days)",
                "DateTo":      "DATETIME — required (window <= 90 days)",
            },
            "use_when": "Single-call executive summary returning 4 result sets: HeadlineKPIs | DailyTrend | TopBottlenecks | TopAnomalies.",
        },
        "parameter_views": ["vw_Facilities", "vw_Shifts"],
        "example_questions": [
            "Give me a one-paragraph performance summary for facility 4266 for last week.",
            "How did facility 4266 perform between Feb 13 and Feb 17, 2026?",
            "Which sites are running below 70% OEE this month?",
            "What was the bottleneck of the day for facility 4266 yesterday?",
            "Show the daily KPI trend for facility 4266 over the last 30 days.",
            "Which days had the most anomalies at facility 4266?",
            "List the top 5 resources that were the bottleneck most often last week.",
        ],
        "example_queries": [
            "SELECT TOP 30 * FROM Bottleneck_MCP.vw_FacilityPerformance_Daily WHERE FacilityKey = 2 ORDER BY Day DESC",
            "SELECT TOP 30 * FROM Bottleneck_MCP.vw_FacilityKPI_Anomalies WHERE FacilityKey = 2 ORDER BY AnomalyCount DESC",
            "EXEC Bottleneck_MCP.sp_FacilityExecutiveSummary @FacilityKey=2, @DateFrom='2026-02-10', @DateTo='2026-02-20'",
        ],
        "related_topics": [1, 6, 13],  # Bottleneck, Downtime, OEE
    },

    # ========================================================================
    # v3.2.0 — Topic 16: Statistical Correlations
    # Spec Trace: v3_1_groupA / A4_vw_StatisticalCorrelations.sql
    # ========================================================================
    16: {
        "name": "Statistical Correlations",
        "keywords": [
            "correlation", "correlated", "pearson", "p-value", "regression", "relationship between",
            "wip vs bottleneck", "cycle variance vs bottleneck", "queue vs wait",
            "is x correlated with y", "linear relationship", "knee point", "threshold",
            "wip-bottleneck correlation",
        ],
        "description": (
            "Pre-computed Pearson correlations for the three canonical operational pairs at Entegris: "
            "WIP_vs_BottleneckFreq, CycleVar_vs_BottleneckScore, Queue_vs_Wait. Returns "
            "PearsonR, PValue, SampleSize, LinearVsThreshold ('linear' / 'threshold'), and KneePoint "
            "(WIP / variance / queue value where the relationship inflects). One facility-wide row per "
            "correlation pair (ResourceKey IS NULL) plus per-resource breakdowns."
        ),
        "summary_view": {
            "name": "vw_StatisticalCorrelations",
            "columns": ["CorrelationName", "ResourceKey", "ResourceName", "PearsonR", "PValue", "SampleSize", "LinearVsThreshold", "KneePoint", "SignificantAt95Pct", "TTest_PValue", "SignificantAt99Pct"],
            "column_notes": "ResourceKey IS NULL means facility-wide correlation. PearsonR is the Pearson coefficient (-1..1). PValue is two-tailed. LinearVsThreshold flags whether the relationship is closer to linear or to a step/threshold pattern. KneePoint is the inflection value when LinearVsThreshold='threshold'. SignificantAt95Pct (BIT, added v3.3.0) is 1 when |PearsonR| > 2/sqrt(SampleSize) at 95% — only meaningful when SampleSize >= 30 (facility-wide WIP_vs_BottleneckFreq=1; CycleVar n=27 and Queue_vs_Wait n=35 currently 0). v3.4.0 additions: TTest_PValue (formal t-test p-value derived from PearsonR + SampleSize) and SignificantAt99Pct (BIT — 1 when TTest_PValue < 0.01 AND SampleSize >= 30). Live state: WIP_vs_BottleneckFreq TTest_PValue=0.0 / SignificantAt99Pct=1; Queue_vs_Wait n=35 TTest_PValue=0.69; CycleVar n=27 TTest_PValue=NULL by design (sample too small).",
        },
        "detail_views": [
            {"name": "vw_WIPBottleneckCorrelation", "use_when": "v3.7 NEW — pre-computed WIP vs bottleneck activity correlation. Also referenced from Topic 4 (Queue & WIP). Use for 'wip-bottleneck correlation', 'wip vs bottleneck'."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "Is there a statistical correlation between WIP and bottleneck frequency?",
            "Does cycle-time variance correlate with bottleneck severity?",
            "What's the Pearson r between queue depth and wait time?",
            "Is the WIP-vs-bottleneck relationship linear or threshold-driven?",
            "What WIP level (knee point) is where bottlenecks start spiking?",
        ],
        "example_queries": [
            "SELECT CorrelationName, PearsonR, PValue, SampleSize, LinearVsThreshold, KneePoint FROM Bottleneck_MCP.vw_StatisticalCorrelations WHERE ResourceKey IS NULL ORDER BY CorrelationName",
            "SELECT TOP 10 ResourceName, CorrelationName, PearsonR, PValue, SampleSize FROM Bottleneck_MCP.vw_StatisticalCorrelations WHERE ResourceKey IS NOT NULL AND CorrelationName = 'WIP_vs_BottleneckFreq' ORDER BY ABS(PearsonR) DESC",
        ],
        "related_topics": [4, 7, 14],  # Queue/WIP, Trend, Outliers
    },

    # ========================================================================
    # v3.3.0 — Topic 17: Operational Events (timeline / state changes)
    # Spec Trace: v3_3_groupX3 / vw_OperationalEventsLog
    # ========================================================================
    17: {
        "name": "Operational Events",
        "keywords": [
            "operational events", "event timeline", "event log", "what happened around",
            "state change", "state changes", "shift handover", "bottleneck transition",
            "events around time", "what events occurred",
        ],
        "description": (
            "Unified per-event timeline UNION'd over FactResourceServiceTime (state changes), "
            "FactShift (handovers), and vw_BottleneckHistory (transitions). 4,475 rows in the "
            "loaded data window. Use for 'what happened around time T?' / 'show events for "
            "resource X over the last day' / event-type breakdown questions."
        ),
        "summary_view": {
            "name": "vw_OperationalEventsLog",
            "columns": ["EventTime", "EventType", "EventDetail", "FacilityKey", "AreaKey", "ResourceKey", "ResourceName"],
            "column_notes": "EventType IN ('StateChange','ShiftHandover','BottleneckTransition'). EventDetail is the human-readable event description (state name, shift name, transition reason).",
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "What events happened on resource OV1110 between Feb 14 and Feb 16?",
            "Show me a timeline of state changes for facility 4266 yesterday.",
            "List all bottleneck transitions in the last week.",
            "Were there any shift handovers when the bottleneck moved?",
            "Give me a chronological event log for area 5 today.",
        ],
        "example_queries": [
            "SELECT TOP 50 EventTime, EventType, EventDetail, ResourceName FROM Bottleneck_MCP.vw_OperationalEventsLog WHERE FacilityKey = 2 AND EventTime BETWEEN '2026-02-14' AND '2026-02-17' ORDER BY EventTime",
            "SELECT EventType, COUNT(*) AS EventCount FROM Bottleneck_MCP.vw_OperationalEventsLog WHERE FacilityKey = 2 GROUP BY EventType ORDER BY EventCount DESC",
            "SELECT TOP 20 EventTime, EventType, EventDetail FROM Bottleneck_MCP.vw_OperationalEventsLog WHERE ResourceKey = 17 ORDER BY EventTime DESC",
        ],
        "related_topics": [1, 7, 6],  # Bottleneck, Trend, Downtime
    },

    # ========================================================================
    # v3.3.0 — Topic 18: Dispatching Rules
    # Spec Trace: v3_3_groupX3 / vw_DispatchingRulesEvaluation
    # ========================================================================
    18: {
        "name": "Dispatching Rules",
        "keywords": [
            "dispatching", "dispatch rule", "dispatching policy", "FIFO", "priority dispatch",
            "release order", "fairness", "out of order", "priority leakage",
            "why is one lot dispatched before another",
        ],
        "description": (
            "Per-resource inferred dispatching policy ('FIFO' / 'Priority' / 'Mixed' / 'NoData') "
            "based on the order lots are released vs their queue arrival time and priority. 175 rows "
            "(11 FIFO / 3 Mixed / 1 Priority / 160 NoData where SampleLots < 30). PriorityLeakagePct "
            "= % of out-of-order dispatches; AvgWaitDeviation is a fairness proxy."
        ),
        "summary_view": {
            "name": "vw_DispatchingRulesEvaluation",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "DispatchingPolicy", "PriorityLeakagePct", "AvgWaitDeviation", "SampleLots"],
            "column_notes": "DispatchingPolicy='NoData' when SampleLots < 30 — report this fact explicitly rather than guessing the policy.",
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "What dispatching policy is OV1110 running under?",
            "Which resources are dispatching out-of-order most often?",
            "Show me the priority leakage % across facility 4266.",
            "Are any resources dispatching by priority instead of FIFO?",
            "Why is one lot being dispatched before another at SD1100?",
        ],
        "example_queries": [
            "SELECT TOP 20 ResourceName, DispatchingPolicy, PriorityLeakagePct, SampleLots FROM Bottleneck_MCP.vw_DispatchingRulesEvaluation WHERE FacilityKey = 2 AND DispatchingPolicy <> 'NoData' ORDER BY SampleLots DESC",
            "SELECT DispatchingPolicy, COUNT(*) AS ResourceCount FROM Bottleneck_MCP.vw_DispatchingRulesEvaluation WHERE FacilityKey = 2 GROUP BY DispatchingPolicy ORDER BY ResourceCount DESC",
            "SELECT ResourceName, PriorityLeakagePct, AvgWaitDeviation FROM Bottleneck_MCP.vw_DispatchingRulesEvaluation WHERE ResourceKey = 17",
        ],
        "related_topics": [4, 9, 12],  # Queue/WIP, Drill-down, Predictive
    },

    # ========================================================================
    # v3.3.0 — Topic 19: Cost & Investment
    # Spec Trace: v3_3_groupX3 / vw_ResourceCostModel + vw_MaterialPriceCatalog
    # ========================================================================
    19: {
        "name": "Cost & Investment",
        "keywords": [
            "cost", "hourly cost", "operating cost", "downtime cost", "investment",
            "ROI", "payback", "how much did this cost", "unit value", "material price",
            "revenue", "what is this material worth",
        ],
        "description": (
            "Resource cost model (HourlyOperatingCost / DownEventEstimatedCost / "
            "InvestmentEstimate) and material price catalog (UnitValue). BOTH views are "
            "backed by EMPTY seed tables (Bottleneck_MCP.ResourceCostModel_Manual and "
            "MaterialPrice_Manual) — defaults are applied via COALESCE. IsEstimatedDefault=1 "
            "flags every default row. **You MUST disclose IsEstimatedDefault=1 in every "
            "cost / revenue answer until Finance populates the seed tables.**"
        ),
        "summary_view": {
            "name": "vw_ResourceCostModel",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "HourlyOperatingCost", "DownEventEstimatedCost", "InvestmentEstimate", "Currency", "IsEstimatedDefault"],
            "column_notes": "Defaults: HourlyOperatingCost=50.00, DownEventEstimatedCost=5000.00, InvestmentEstimate=250000.00. Currency default 'USD'. IsEstimatedDefault=1 across all 175 rows until ResourceCostModel_Manual is populated.",
        },
        "detail_views": [
            {"name": "vw_MaterialPriceCatalog",
             "columns": ["MaterialKey", "MaterialName", "UnitValue", "Currency", "IsEstimatedDefault"],
             "use_when": "Per-material unit value lookup. 101 rows. Default UnitValue=100.00 USD with IsEstimatedDefault=1 until MaterialPrice_Manual is populated."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "What is the hourly operating cost of OV1110?",
            "How much did the downtime on the bottleneck cost us yesterday?",
            "What is the investment estimate for SD1100?",
            "How much is the material S4442R031Y23 worth per unit?",
            "What's our ROI if we add a parallel LM1100?",
        ],
        "example_queries": [
            "SELECT TOP 10 ResourceName, HourlyOperatingCost, DownEventEstimatedCost, InvestmentEstimate, Currency, IsEstimatedDefault FROM Bottleneck_MCP.vw_ResourceCostModel WHERE FacilityKey = 2 ORDER BY ResourceName",
            "SELECT TOP 10 MaterialName, UnitValue, Currency, IsEstimatedDefault FROM Bottleneck_MCP.vw_MaterialPriceCatalog ORDER BY UnitValue DESC",
            "SELECT ResourceName, HourlyOperatingCost, IsEstimatedDefault FROM Bottleneck_MCP.vw_ResourceCostModel WHERE ResourceKey = 17",
        ],
        "related_topics": [6, 12, 5],  # Downtime (down event cost), Predictive (ROI), Throughput (revenue impact)
    },

    # ========================================================================
    # v3.3.0 — Topic 20: Resource Lifecycle
    # Spec Trace: v3_3_groupX3 / vw_ResourceLifecycle
    # ========================================================================
    20: {
        "name": "Resource Lifecycle",
        "keywords": [
            "lifecycle", "months in service", "how old is", "when was this resource installed",
            "shifts observed", "first service timestamp", "last service timestamp",
            "total productive hours", "equipment age",
        ],
        "description": (
            "Per-resource lifecycle metrics anchored on vw_DataAvailability.LatestEvent: "
            "MonthsInService (from DimResource.CreateTimestamp), ShiftsObserved, First/Last "
            "service timestamps, TotalProductiveHours over the data window. 175 rows. "
            "MonthsInService=0 across the board because DimResource.CreateTimestamp is recent "
            "vs the loaded window — disclose this honestly rather than implying brand-new equipment."
        ),
        "summary_view": {
            "name": "vw_ResourceLifecycle",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName", "MonthsInService", "ShiftsObserved", "FirstServiceTimestamp", "LastServiceTimestamp", "TotalProductiveHours"],
            "column_notes": "MonthsInService is computed from DimResource.CreateTimestamp anchored on vw_DataAvailability.LatestEvent. When 0, state 'within the loaded data window' rather than 'brand new'.",
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "How old is OV1110?",
            "When was SD1100 first put in service?",
            "How many shifts has LM1100 been observed running?",
            "What's the total productive hours on the bottleneck resource?",
            "Show me lifecycle for all resources in facility 4266.",
        ],
        "example_queries": [
            "SELECT TOP 10 ResourceName, MonthsInService, ShiftsObserved, TotalProductiveHours FROM Bottleneck_MCP.vw_ResourceLifecycle WHERE FacilityKey = 2 ORDER BY ShiftsObserved DESC",
            "SELECT ResourceName, FirstServiceTimestamp, LastServiceTimestamp FROM Bottleneck_MCP.vw_ResourceLifecycle WHERE ResourceKey = 17",
            "SELECT ResourceName, MonthsInService, TotalProductiveHours FROM Bottleneck_MCP.vw_ResourceLifecycle WHERE FacilityKey = 2 ORDER BY TotalProductiveHours DESC",
        ],
        "related_topics": [6, 1, 13],  # Downtime, Bottleneck, OEE
    },

    # ========================================================================
    # v3.4.0 — Topic 21: Production Line Mapping
    # Spec Trace: v3_4_groupB / B5_vw_ProductionLine_Mapping.sql
    # ========================================================================
    21: {
        "name": "Production Line Mapping",
        "keywords": [
            "production line", "line mapping", "PL-101", "which line", "line id",
            "production line id", "how is line X performing", "resources on line",
        ],
        "description": (
            "Resolve `[A-Z]{2}-\\d{3}` style production-line IDs (e.g. PL-101) to the underlying "
            "ResourceKey. Heuristic mapping from ResourceName naming convention (first 2 chars of "
            "ResourceName); IsHeuristic flag surfaced for disclosure. 175 rows. Companion to "
            "sp_ResolveResource which now returns MatchType='ProductionLine' for these tokens."
        ),
        "summary_view": {
            "name": "vw_ProductionLine_Mapping",
            "columns": ["FacilityKey", "AreaKey", "ResourceKey", "ResourceName",
                         "ProductionLineCode", "ProductionLineName", "ProductionLineId", "IsHeuristic"],
            "column_notes": "ProductionLineCode is the 2-char prefix of ResourceName (e.g. 'PL'). ProductionLineId is the canonical 'PL-101' style token. IsHeuristic=1 across all rows until DimResource gets a real ProductionLine column (P7 plant-side ask) — disclose this every time the answer cites a Production Line ID.",
        },
        "detail_views": [],
        "stored_procedure": {
            "name": "sp_ResolveResource",
            "params": {"Token": "required NVARCHAR(64) - the user-supplied token (resource name, alias, or production line ID)"},
            "use_when": "Resolve a token to ResourceKey. Returns MatchType IN ('Exact','Alias','ProductionLine','Fuzzy','NotFound'). v3.4.0 adds 'ProductionLine' MatchType — 'PL-101' resolves to ResourceKey=7 (PL1100).",
        },
        "parameter_views": ["vw_Facilities", "vw_Resources"],
        "example_questions": [
            "Which resource is line PL-101?",
            "How is line PL-101 performing today?",
            "What resources are on production line OV?",
            "Resolve 'PL-101' for me.",
        ],
        "example_queries": [
            "SELECT TOP 20 ProductionLineId, ProductionLineCode, ResourceName, IsHeuristic FROM Bottleneck_MCP.vw_ProductionLine_Mapping WHERE FacilityKey = 2 ORDER BY ProductionLineId",
            "SELECT ResourceKey, ResourceName, ProductionLineId FROM Bottleneck_MCP.vw_ProductionLine_Mapping WHERE ProductionLineId = 'PL-101'",
            "EXEC Bottleneck_MCP.sp_ResolveResource @Token=N'PL-101'",
        ],
        "related_topics": [1, 6, 13],  # Bottleneck, Downtime, OEE
    },

    # ========================================================================
    # v3.4.1 — Topic 22: Shift Staffing
    # Spec Trace: v3_4_1 / vw_ShiftStaffing_Daily.sql
    # ========================================================================
    22: {
        "name": "Shift Staffing",
        "keywords": ["staffing", "operators on shift", "headcount", "coverage", "operators per resource"],
        "description": "Per-shift staffing aggregates: unique operators, resources staffed, events per operator, coverage ratio (operators / resources). Lifts staffing-correlation prompts when answering 'is OEE drop linked to under-staffing'.",
        "summary_view": {
            "name": "vw_ShiftStaffing_Daily",
            "columns": ["FacilityKey", "AreaKey", "AreaName", "ShiftKey", "ShiftDay", "ShiftName", "UniqueOperators", "ResourcesStaffed", "TotalEvents", "AvgEventsPerOperator", "CoverageRatio"],
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Areas", "vw_Shifts"],
        "example_questions": [
            "How was the day shift staffed in the Cleaning area on 2026-02-13?",
            "Which shifts had fewer operators than usual?",
            "Was OEE down because of under-staffing?",
        ],
        "example_queries": [
            "SELECT AreaName, ShiftName, UniqueOperators, ResourcesStaffed, CoverageRatio FROM Bottleneck_MCP.vw_ShiftStaffing_Daily WHERE FacilityKey = 2 AND ShiftDay = '2026-02-13' ORDER BY AreaName, ShiftName",
        ],
    },

    # ========================================================================
    # v3.6.0 — Topic 23: Real-time WIP Alert
    # ========================================================================
    23: {
        "name": "Real-time WIP Alert",
        "keywords": [
            "in-process", "in process", "currently running", "lots running long",
            "wip alert", "late lots", "lots over ideal", "real-time alert", "live alert",
            "still in process", "stuck on resource right now", "exceeding ideal cycle",
        ],
        "description": (
            "Surface lots that are currently in-process (TrackIn but not TrackOut) and are exceeding their "
            "ideal cycle time. Use for 'which lots are running long right now?' or 'real-time WIP alert' "
            "prompts. vw_InProcessLotAlert is 0 rows by design when nothing is currently late — that is a "
            "valid answer. sp_RealtimeOutlierAlert is the recommended single-call wrapper that returns the "
            "alerts sorted by Severity with a RecommendedAction column the LLM can surface verbatim."
        ),
        "summary_view": {
            "name": "vw_InProcessLotAlert",
            "columns": ["FacilityKey","AreaKey","ResourceKey","ResourceName","LotKey","LotName",
                         "ElapsedSec","IdealCycleSec","PctOverIdeal","CurrentSEMI_E10State",
                         "OperatorKey","OperatorName","EstRemainingSec","Severity"],
            "column_notes": "0 rows by design — only flags lots currently exceeding ideal. PctOverIdeal = (ElapsedSec - IdealCycleSec) / IdealCycleSec * 100. Severity is bucketed ('Low' / 'Medium' / 'High'). EstRemainingSec is a heuristic estimate.",
        },
        "detail_views": [],
        "stored_procedure": {
            "name": "sp_RealtimeOutlierAlert",
            "params": {
                "FacilityKey":   {"type": "int", "required": True,  "doc": "BIGINT — required"},
                "AreaKey":       {"type": "int", "required": False, "doc": "BIGINT — optional (NULL allowed)"},
                "ThresholdPct":  {"type": "int", "required": False, "doc": "INT — % over ideal threshold; default 100"},
            },
            "use_when": "Single-call real-time alert: returns sorted in-process alerts (by Severity DESC) plus a RecommendedAction column. 0 rows means nothing currently late — that's a valid answer.",
        },
        "parameter_views": ["vw_Facilities", "vw_Areas", "vw_Resources"],
        "example_questions": [
            "Which lots are running long right now in facility 4266?",
            "Show me the real-time WIP alert for the Cleaning area.",
            "Are any lots currently exceeding their ideal cycle time?",
            "Give me the high-severity in-process alerts.",
        ],
        "example_queries": [
            "SELECT TOP 20 LotName, ResourceName, ElapsedSec, IdealCycleSec, PctOverIdeal, Severity FROM Bottleneck_MCP.vw_InProcessLotAlert WHERE FacilityKey = 2 ORDER BY PctOverIdeal DESC",
            "EXEC Bottleneck_MCP.sp_RealtimeOutlierAlert @FacilityKey=2, @AreaKey=NULL, @ThresholdPct=100",
        ],
        "related_topics": [14, 4, 9],  # Cycle Time Outliers, Queue/WIP, Drill-down
    },

    # ========================================================================
    # v3.6.0 — Topic 24: Area Capacity & What-If
    # ========================================================================
    24: {
        "name": "Area Capacity & What-If",
        "keywords": [
            "area capacity", "capacity utilisation", "capacity utilization", "area pph",
            "spare area capacity", "what if we add", "throughput lift", "area what-if",
            "if we de-bottlenecked", "area-level capacity", "ideal pph",
        ],
        "description": (
            "Area-grain capacity analysis: IdealPPH × ResourceCount vs ActualPPH per area+shift+day, "
            "plus a what-if heuristic projecting throughput lift if the area's bottleneck resource were "
            "removed. Use for 'how much spare capacity does the Cleaning area have' or "
            "'what's the throughput lift if we de-bottleneck Drying' prompts."
        ),
        "summary_view": {
            "name": "vw_AreaCapacityAnalysis",
            "columns": ["FacilityKey","AreaKey","AreaName","ShiftKey","Day","ResourceCount","IdealPPH","ActualPPH","CapacityUtilizationPct"],
            "column_notes": "25 rows. CapacityUtilizationPct = ActualPPH / (IdealPPH × ResourceCount) * 100. Surface ResourceCount so the user knows the denominator.",
        },
        "detail_views": [
            {"name": "vw_AreaWhatIf_ThroughputLift",
             "columns": ["FacilityKey","AreaKey","AreaName","BottleneckResourceName","Current_AreaPPH","Projected_AreaPPH_Delta","Projected_AreaPPH_DeltaPct","Heuristic_AssumptionNote"],
             "use_when": "What-if heuristic (4 rows): how much would area PPH improve if BottleneckResourceName were de-bottlenecked. Heuristic_AssumptionNote documents the assumption — surface it verbatim so the user understands the projection is a heuristic, not a forecast."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Areas", "vw_Shifts"],
        "example_questions": [
            "How much spare capacity does the Cleaning area have right now?",
            "Show me area capacity utilisation for facility 4266 today.",
            "What's the projected throughput lift if we de-bottleneck the Drying area?",
            "Which area has the lowest capacity utilisation?",
        ],
        "example_queries": [
            "SELECT AreaName, ShiftKey, IdealPPH, ActualPPH, CapacityUtilizationPct FROM Bottleneck_MCP.vw_AreaCapacityAnalysis WHERE FacilityKey = 2 ORDER BY CapacityUtilizationPct ASC",
            "SELECT AreaName, BottleneckResourceName, Current_AreaPPH, Projected_AreaPPH_Delta, Projected_AreaPPH_DeltaPct, Heuristic_AssumptionNote FROM Bottleneck_MCP.vw_AreaWhatIf_ThroughputLift ORDER BY Projected_AreaPPH_DeltaPct DESC",
        ],
        "related_topics": [5, 1, 12],  # Throughput, Bottleneck, Predictive
    },

    # ========================================================================
    # v3.6.0 — Topic 25: Loss Pareto & Opportunity (CONTAINS IsEstimatedDefault VIEW)
    # ========================================================================
    25: {
        "name": "Loss Pareto & Opportunity",
        "keywords": [
            "loss pareto", "loss categories", "which loss biggest", "80/20 of losses",
            "pareto of losses", "loss opportunity", "50% reduction projection",
            "loss difficulty", "roi score", "if we cut losses",
            "downtime pareto", "top downtime reasons by area", "reason pareto",
        ],
        "description": (
            "Pareto roll-up of loss categories per area (vw_LossPareto, clean) plus a projection of how much "
            "could be recovered if the dominant loss category were halved (vw_LossOpportunity, FLAGGED). "
            "**vw_LossOpportunity carries IsEstimatedDefault=1 — when 1, the answer MUST OPEN with the disclosure "
            "'These figures use estimated defaults — Finance has not loaded actual values yet.' This is non-negotiable.**"
        ),
        "summary_view": {
            "name": "vw_LossPareto",
            "columns": ["FacilityKey","AreaKey","AreaName","LossCategory","LostUnits","PctOfTotal","CumulativePct","RankN"],
            "column_notes": "3 rows. Pre-ranked by RankN. Use directly for Pareto charts and 80/20 narratives.",
        },
        "detail_views": [
            {"name": "vw_LossOpportunity",
             "columns": ["FacilityKey","AreaKey","AreaName","Current_LostUnits","Projected_Gain_Units","Projected_Gain_Value","Difficulty","ROIScore","IsEstimatedDefault"],
             "use_when": "Loss-recovery opportunity (1 row). Projected_Gain_Value is the financial projection — uses ESTIMATED DEFAULT pricing because Finance has not loaded actuals. **IsEstimatedDefault=1 means the LLM MUST OPEN the answer with: 'These figures use estimated defaults — Finance has not loaded actual values yet.'** Difficulty and ROIScore are heuristic ranks."},
            {"name": "vw_DowntimeReasonByArea_Pareto", "use_when": "v3.7 NEW — Pareto of downtime reasons per area (OEE Loss Pareto angle). Also referenced from Topic 6 (Downtime & Utilization). Use for 'downtime pareto', 'top downtime reasons by area', 'reason pareto'."},
        ],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Areas"],
        "example_questions": [
            "Show me the Pareto of losses by category.",
            "Which loss category accounts for the most lost units?",
            "What's the projected gain if we halve the biggest loss category?",
            "Give me the ROI score and Difficulty for the top loss-reduction opportunity.",
        ],
        "example_queries": [
            "SELECT AreaName, LossCategory, LostUnits, PctOfTotal, CumulativePct, RankN FROM Bottleneck_MCP.vw_LossPareto ORDER BY RankN",
            "SELECT AreaName, Current_LostUnits, Projected_Gain_Units, Projected_Gain_Value, Difficulty, ROIScore, IsEstimatedDefault FROM Bottleneck_MCP.vw_LossOpportunity",
        ],
        "related_topics": [5, 6, 19],  # Throughput, Downtime, Cost
    },

    # ========================================================================
    # v3.6.0 — Topic 26: Investment ROI (IsEstimatedDefault)
    # ========================================================================
    26: {
        "name": "Investment ROI",
        "keywords": [
            "investment roi", "area roi", "roi months", "payback months",
            "investment per area", "area investment", "capital investment area",
        ],
        "description": (
            "Per-area investment ROI (months to payback) computed from estimated investment vs estimated "
            "loss value. **vw_AreaInvestmentROI carries IsEstimatedDefault=1 on every row — when 1, the answer "
            "MUST OPEN with the disclosure 'These figures use estimated defaults — Finance has not loaded "
            "actual values yet.' This is non-negotiable.**"
        ),
        "summary_view": {
            "name": "vw_AreaInvestmentROI",
            "columns": ["FacilityKey","AreaKey","AreaName","TotalLostUnits","EstimatedLossValue","EstimatedInvestment","ROI_Months","IsEstimatedDefault"],
            "column_notes": "6 rows. EstimatedLossValue uses default $/unit pricing; EstimatedInvestment uses default capital figures. **IsEstimatedDefault=1 across all rows until Finance loads actuals — the LLM MUST OPEN every ROI answer with the IsEstimatedDefault disclosure.**",
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities", "vw_Areas"],
        "example_questions": [
            "What's the investment ROI per area?",
            "Which area has the shortest payback in months?",
            "Show me ROI months for the Cleaning area.",
        ],
        "example_queries": [
            "SELECT AreaName, TotalLostUnits, EstimatedLossValue, EstimatedInvestment, ROI_Months, IsEstimatedDefault FROM Bottleneck_MCP.vw_AreaInvestmentROI ORDER BY ROI_Months ASC",
        ],
        "related_topics": [19, 25, 5],  # Cost & Investment, Loss Pareto, Throughput
    },

    # ========================================================================
    # v3.6.0 — Topic 27: Financial Impact Summary (IsEstimatedDefault)
    # ========================================================================
    27: {
        "name": "Financial Impact Summary",
        "keywords": [
            "financial impact", "revenue", "loss value", "improvement potential",
            "executive financials", "facility revenue", "estimated revenue",
            "money lost", "financial summary",
        ],
        "description": (
            "Daily facility-level financial roll-up: estimated revenue, estimated loss value, and estimated "
            "improvement potential. **vw_FinancialImpactSummary carries IsEstimatedDefault=1 on every row — "
            "when 1, the answer MUST OPEN with the disclosure 'These figures use estimated defaults — Finance "
            "has not loaded actual values yet.' This is non-negotiable.**"
        ),
        "summary_view": {
            "name": "vw_FinancialImpactSummary",
            "columns": ["FacilityKey","FacilityName","Day","Revenue_Estimated","LossValue_Estimated","ImprovementPotential_Estimated","IsEstimatedDefault"],
            "column_notes": "10 rows. All three financial columns use default $/unit pricing. **IsEstimatedDefault=1 on every row — LLM MUST OPEN every financial-impact answer with the IsEstimatedDefault disclosure.**",
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": ["vw_Facilities"],
        "example_questions": [
            "What's the financial impact of the bottleneck this week?",
            "Show me estimated revenue and loss value for facility 4266.",
            "How much improvement potential do we have?",
        ],
        "example_queries": [
            "SELECT Day, Revenue_Estimated, LossValue_Estimated, ImprovementPotential_Estimated, IsEstimatedDefault FROM Bottleneck_MCP.vw_FinancialImpactSummary WHERE FacilityKey = 2 ORDER BY Day DESC",
        ],
        "related_topics": [19, 25, 26],  # Cost, Loss Pareto, Investment ROI
    },

    # ========================================================================
    # v3.6.0 — Topic 28: Strategic Priority Plan (SP-only, IsEstimatedDefault)
    # ========================================================================
    28: {
        "name": "Strategic Priority Plan",
        "keywords": [
            "strategic priorities", "top 3 priorities", "strategic plan", "priority plan",
            "priority recommendations", "what should we do first", "strategic roadmap",
            "headcount required", "investment required", "expected outcome",
        ],
        "description": (
            "Top-3 strategic priorities for a facility over a configurable period (default 30 days). "
            "Returns RankN, PriorityName, Justification, RequiredHeadcount_Estimated, "
            "RequiredInvestment_Estimated_USD, ExpectedOutcome, Timeline_Months, IsEstimatedDefault. "
            "**sp_StrategicPriorityPlan returns IsEstimatedDefault=1 — when 1, the answer MUST OPEN with the "
            "disclosure 'These figures use estimated defaults — Finance has not loaded actual values yet.' "
            "This is non-negotiable.** SP-only topic — there is no companion view."
        ),
        "summary_view": {
            "name": "sp_StrategicPriorityPlan",
            "columns": ["RankN","PriorityName","Justification","RequiredHeadcount_Estimated","RequiredInvestment_Estimated_USD","ExpectedOutcome","Timeline_Months","IsEstimatedDefault"],
            "column_notes": "SP-only — call via run_stored_procedure. RequiredHeadcount_Estimated and RequiredInvestment_Estimated_USD use default values. **IsEstimatedDefault=1 means the LLM MUST OPEN the answer with the IsEstimatedDefault disclosure.**",
        },
        "detail_views": [],
        "stored_procedure": {
            "name": "sp_StrategicPriorityPlan",
            "params": {
                "FacilityKey":   {"type": "int", "required": True,  "doc": "BIGINT — required"},
                "PeriodDays":    {"type": "int", "required": False, "doc": "INT — analysis window in days; default 30"},
            },
            "use_when": "Single-call top-3 strategic priorities for a facility. **Output carries IsEstimatedDefault=1 — open the answer with the IsEstimatedDefault disclosure.**",
        },
        "parameter_views": ["vw_Facilities"],
        "example_questions": [
            "What are the top 3 strategic priorities for facility 4266?",
            "Show me the strategic plan for the next quarter.",
            "What headcount and investment do we need to fix the top priority?",
            "Give me the executive priority recommendations.",
        ],
        "example_queries": [
            "EXEC Bottleneck_MCP.sp_StrategicPriorityPlan @FacilityKey=2, @PeriodDays=30",
        ],
        "related_topics": [27, 26, 25],  # Financial Impact, Investment ROI, Loss Opportunity
    },

    # ========================================================================
    # v3.6.1 — Topic 29: Categorical Significance Tests
    # ========================================================================
    29: {
        "name": "Categorical Significance Tests",
        "keywords": [
            "chi-square", "chi square", "fisher exact", "fisher's exact",
            "contingency table", "categorical significance", "categorical test",
            "is the difference between resources significant",
            "is the difference between shifts significant",
            "is the difference between batches significant",
            "resource vs outlier", "shift vs outlier", "batch vs outlier",
            "p-value categorical",
        ],
        "description": (
            "Pre-computed chi-square + Fisher exact for Resource/Shift/Batch contingency tables vs Outlier "
            "rate. Today most rows carry `IsLowSampleConfidence=1` because the loaded data window is 6 days; "
            "the LLM MUST disclose this when reporting any p-value (the contingency tables are too small for "
            "reliable inference)."
        ),
        "summary_view": {
            "name": "vw_CategoricalSignificanceTests",
            "columns": [
                "TestName", "RowCategory",
                "RowOutlierCount", "RowTotalCount", "OtherOutlierCount", "OtherTotalCount",
                "ChiSquareStat", "ChiSquare_PValue", "FisherExact_PValue",
                "SignificantAt95Pct", "IsLowSampleConfidence",
            ],
            "column_notes": (
                "30 rows. TestName ∈ ('Resource_vs_Outlier','Shift_vs_Outlier','Batch_vs_Outlier'). "
                "20/30 rows carry IsLowSampleConfidence=1 today (6-day window — expected count < 5 in at "
                "least one cell). When IsLowSampleConfidence=1 the LLM MUST OPEN with the chi-square "
                "validity-threshold disclosure."
            ),
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": [],
        "example_questions": [
            "Are the outlier rates between resources statistically different?",
            "Is the difference in outlier rate between Day and Night shifts significant?",
            "Run a chi-square test for batch vs outlier.",
            "Show me the Fisher exact p-value for Resource_vs_Outlier.",
            "Which categorical tests are statistically significant?",
        ],
        "example_queries": [
            "SELECT TestName, RowCategory, RowOutlierCount, RowTotalCount, OtherOutlierCount, OtherTotalCount, ChiSquareStat, ChiSquare_PValue, FisherExact_PValue, SignificantAt95Pct, IsLowSampleConfidence FROM Bottleneck_MCP.vw_CategoricalSignificanceTests WHERE TestName = 'Resource_vs_Outlier' ORDER BY ChiSquare_PValue",
            "SELECT * FROM Bottleneck_MCP.vw_CategoricalSignificanceTests WHERE SignificantAt95Pct = 1 AND IsLowSampleConfidence = 0",
        ],
        "related_topics": [14, 16, 15],  # Cycle Time Outliers, Statistical Correlations, Facility Performance
    },

    # ========================================================================
    # v3.6.1 — Topic 30: Lot Economic Impact (FLAGGED IsEstimatedDefault)
    # ========================================================================
    30: {
        "name": "Lot Economic Impact",
        "keywords": [
            "lot economic impact", "scrap value", "completion value",
            "economic risk score", "economic risk per lot",
            "which lot is most expensive to lose",
            "value at risk per lot", "lot value", "estimated scrap value",
        ],
        "description": (
            "Per-lot scrap and completion economic value, EconomicRiskScore. Today every row carries "
            "`IsEstimatedDefault=1` because Finance has not loaded actual unit values into "
            "`MaterialPrice_Manual`. The LLM MUST OPEN every answer with the Finance disclosure line."
        ),
        "summary_view": {
            "name": "vw_LotEconomicImpact",
            "columns": [
                "LotName", "MaterialKey", "ResourceKey", "ResourceName",
                "CurrentState", "LotQuantity", "UnitValue",
                "EstimatedScrapValue", "EstimatedCompletionValue", "EconomicRiskScore",
                "IsEstimatedDefault",
            ],
            "column_notes": (
                "1,713 rows. Every row carries IsEstimatedDefault=1 today (default $100/unit until Finance "
                "loads MaterialPrice_Manual). **The LLM MUST OPEN every answer with the disclosure 'These "
                "figures use estimated defaults — Finance has not loaded actual values yet.' This is "
                "non-negotiable.**"
            ),
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": ["vw_Resources"],
        "example_questions": [
            "Which lot is the most expensive to lose right now?",
            "Show me the top 10 lots by EconomicRiskScore.",
            "What is the estimated scrap value of the lots currently queued?",
            "Which resource carries the highest economic risk in WIP?",
            "What's the economic exposure if we scrap everything currently at LM1100?",
        ],
        "example_queries": [
            "SELECT TOP 20 LotName, ResourceName, CurrentState, LotQuantity, EstimatedScrapValue, EstimatedCompletionValue, EconomicRiskScore, IsEstimatedDefault FROM Bottleneck_MCP.vw_LotEconomicImpact ORDER BY EconomicRiskScore DESC",
            "SELECT ResourceName, COUNT(*) AS LotCount, SUM(EstimatedScrapValue) AS TotalScrapAtRisk, MAX(IsEstimatedDefault) AS IsEstimatedDefault FROM Bottleneck_MCP.vw_LotEconomicImpact GROUP BY ResourceName ORDER BY TotalScrapAtRisk DESC",
        ],
        "related_topics": [27, 26, 25],  # Financial Impact, Investment ROI, Loss Opportunity
    },

    # ========================================================================
    # v3.6.1 — Topic 31: Shift ANOVA
    # ========================================================================
    31: {
        "name": "Shift ANOVA",
        "keywords": [
            "shift anova", "anova", "f-test", "f test", "f statistic",
            "day vs night significance", "day vs night anova",
            "are the day and night shifts significantly different",
            "one-way anova", "shift comparison anova",
        ],
        "description": (
            "One-way ANOVA across Day vs Night for OEE_Pct, Throughput_PPH, ScrapRate_Pct. Today every row "
            "carries `IsLowSampleConfidence=1` (max N=22 per group on 6-day window, < 30 threshold). "
            "F-statistics are computed but the LLM MUST OPEN every ANOVA answer with: 'F-test results below "
            "are computed but not statistically meaningful — the loaded window has fewer than 30 samples per "
            "shift group. Treat as directional only.'"
        ),
        "summary_view": {
            "name": "vw_ShiftANOVA",
            "columns": [
                "MetricName",
                "DayMean", "NightMean",
                "DayVariance", "NightVariance",
                "DayN", "NightN",
                "FStatistic", "ANOVA_PValue", "SignificantAt95Pct",
                "IsLowSampleConfidence",
            ],
            "column_notes": (
                "3 rows (one per metric: OEE_Pct, Throughput_PPH, ScrapRate_Pct). All 3 rows have "
                "IsLowSampleConfidence=1 today (max N=22 per shift group, below the 30-sample threshold). "
                "**LLM MUST OPEN with the directional-only disclosure when IsLowSampleConfidence=1.**"
            ),
        },
        "detail_views": [],
        "stored_procedure": None,
        "parameter_views": [],
        "example_questions": [
            "Are Day and Night shifts significantly different on OEE?",
            "Run an ANOVA across Day vs Night for throughput.",
            "Is the scrap rate gap between Day and Night statistically significant?",
            "Show me the F-statistic for Day vs Night across all KPIs.",
        ],
        "example_queries": [
            "SELECT MetricName, DayMean, NightMean, DayN, NightN, FStatistic, ANOVA_PValue, SignificantAt95Pct, IsLowSampleConfidence FROM Bottleneck_MCP.vw_ShiftANOVA",
            "SELECT MetricName, DayMean, NightMean, FStatistic, ANOVA_PValue FROM Bottleneck_MCP.vw_ShiftANOVA WHERE SignificantAt95Pct = 1",
        ],
        "related_topics": [15, 8, 16],  # Facility Performance, Comparative Analysis, Statistical Correlations
    },

}


# ============================================================================
# WHITELISTED STORED PROCEDURES
# ============================================================================
WHITELISTED_SPS = {
    "sp_BottleneckAnalysis": {
        "params": {
            "FacilityKey":           {"type": "int",  "required": True,  "doc": "BIGINT - required"},
            "DateFrom":              {"type": "str",  "required": True,  "doc": "DATETIME - required (e.g. '2026-02-01')"},
            "DateTo":                {"type": "str",  "required": True,  "doc": "DATETIME - required (e.g. '2026-02-28')"},
            "AreaKey":               {"type": "int",  "required": False, "doc": "BIGINT - optional"},
            "ShiftKey":              {"type": "int",  "required": False, "doc": "BIGINT - optional"},
            "TopN":                  {"type": "int",  "required": False, "doc": "INT - optional, default 20"},
            "IncludeTimeSlice":      {"type": "int",  "required": False, "doc": "BIT - optional, default 0"},
            "SliceIntervalMinutes":  {"type": "int",  "required": False, "doc": "INT - optional, default 30"},
        },
        "returns": (
            "Multi-resultset: (1) ShiftSummary | (2) ResourceDetail | "
            "(3) TimeSliceHistory — adds UnitsProduced_InSlice, UnitsLost_InSlice "
            "(NULL when FactTargets is empty), and TransitionTrigger ('Same' / "
            "'NewBottleneck' / 'NoData') so the LLM can describe how the bottleneck "
            "shifted over time slices."
        ),
        "use_when": "Single-call full bottleneck report for a date range",
    },
    "sp_ResourceDetailLookup": {
        "params": {
            "SearchTerm":            {"type": "str",  "required": True,  "doc": "NVARCHAR(256) - required, fuzzy match"},
            "FacilityKey":           {"type": "int",  "required": False, "doc": "BIGINT - optional"},
        },
        "returns": "Single resultset: resource detail rows with current state + queue length",
        "use_when": "Fuzzy resource-name search",
    },
    # ------------------------------------------------------------------------
    # UC7 — Topic 15 — Facility Performance / Executive View
    # Spec Trace: Docs/UC4_Bottleneck_Analysis/UC2_UC5_UC7_Merge_Proposal.md §5
    # ------------------------------------------------------------------------
    "sp_FacilityExecutiveSummary": {
        "params": {
            "FacilityKey":  {"type": "int", "required": True,  "doc": "BIGINT - required"},
            "DateFrom":     {"type": "str", "required": True,  "doc": "DATETIME - required (e.g. '2026-02-10'); window must be <= 90 days"},
            "DateTo":       {"type": "str", "required": True,  "doc": "DATETIME - required (e.g. '2026-02-20'); window must be <= 90 days"},
        },
        "returns": (
            "Multi-resultset: "
            "(1) HeadlineKPIs (TotalLotsProduced, AvgOEE_Pct, AvgScrapRate_Pct, BottleneckShifts, AnomalyCount) | "
            "(2) DailyTrend (Day, OEE_Pct, ScrapRate_Pct, AvgQueueLength, BottleneckResourceName, BottleneckScore, WIP_Snapshot_EOD) | "
            "(3) TopBottlenecks (ResourceName, Days_As_Bottleneck, Shifts_As_Bottleneck, AvgBottleneckScore) | "
            "(4) TopAnomalies (Day, AnomalyCount, AnomalyTypes, MostSevereResourceName, MostSevereAlertReason)"
        ),
        "use_when": "Single-call executive facility summary for a date window (<= 90 days). Designed for the LLM to compose a one-paragraph narrative.",
    },
    # ------------------------------------------------------------------------
    # v2 refactor — single-call cascade impact bundle
    # ------------------------------------------------------------------------
    "sp_BottleneckCascade": {
        "params": {
            "FacilityKey":  {"type": "int", "required": True,  "doc": "BIGINT - required"},
            "ShiftKey":     {"type": "int", "required": True,  "doc": "BIGINT - required"},
            "DateFrom":     {"type": "str", "required": False, "doc": "DATETIME - optional (defaults to last 7 shifts for CascadeTimeline)"},
            "DateTo":       {"type": "str", "required": False, "doc": "DATETIME - optional"},
        },
        "returns": (
            "4 result sets: (1) Bottleneck | (2) UpstreamBackup (with new "
            "FacilityImpactUnits column) | (3) DownstreamStarvation (with new "
            "FacilityImpactUnits column) | (4) CascadeTimeline — one row per "
            "(Shift, Resource, Role) showing how Bottleneck/Upstream/Downstream "
            "pressure evolved across shifts."
        ),
        "use_when": "Single-call cascade impact: identifies the bottleneck for a shift, plus upstream resources backing up, downstream resources starved, and a per-shift timeline of cascade roles. Replaces the 4-step manual chain.",
    },
    # ------------------------------------------------------------------------
    # v2 refactor — token-to-resource resolver (call BEFORE per-resource queries)
    # ------------------------------------------------------------------------
    "sp_ResolveResource": {
        "params": {
            "Token":        {"type": "str", "required": True,  "doc": "NVARCHAR(256) - required, e.g. 'SD1100'"},
            "FacilityKey":  {"type": "int", "required": False, "doc": "BIGINT - optional (NULL allowed)"},
        },
        "returns": "1 row: ResourceKey, ResourceName, AreaKey, AreaName, FacilityKey, IsTerminated, MatchType (Exact/Alias/Fuzzy/NotFound)",
        "use_when": "Resolve a user-supplied token to a resource. Use this BEFORE running any per-resource query when the user typed a code like 'SD1100'.",
    },
    # ------------------------------------------------------------------------
    # v3.6.0 — Topic 28 — Strategic Priority Plan (IsEstimatedDefault flagged)
    # ------------------------------------------------------------------------
    "sp_StrategicPriorityPlan": {
        "params": {
            "FacilityKey":  {"type": "int", "required": True,  "doc": "BIGINT — required"},
            "PeriodDays":   {"type": "int", "required": False, "doc": "INT — analysis window in days; default 30"},
        },
        "returns": (
            "Single resultset: top-3 priorities — RankN, PriorityName, Justification, "
            "RequiredHeadcount_Estimated, RequiredInvestment_Estimated_USD, ExpectedOutcome, "
            "Timeline_Months, IsEstimatedDefault. When IsEstimatedDefault=1 the LLM MUST OPEN the "
            "answer with the IsEstimatedDefault disclosure (non-negotiable)."
        ),
        "use_when": "Top-3 strategic priorities for a facility. **Output carries IsEstimatedDefault=1 until Finance loads actuals — every answer MUST OPEN with the IsEstimatedDefault disclosure.**",
    },
}


# ============================================================================
# TOOL 0: GET INSTRUCTIONS — call this FIRST for every new user question.
# Returns the playbook, decision tree, and recommended tool sequence.
# ============================================================================
INSTRUCTIONS = {
    "server_name": "Athena AI — Entegris Bottleneck MCP",
    "purpose": (
        "Answer manufacturing-floor questions about the Entegris KSP MES "
        "(Bottleneck_MCP schema in EntegrisKSPUpgradeDWH). The LLM is the "
        "translation + reasoning layer; all formulas live in DB views."
    ),
    "core_principles": [
        "0. **GROUND EVERY ANSWER IN LIVE DATA.** If you have not retrieved data from the MCP tools in the current turn, you have NO basis to give numbers. Do NOT invent figures (OEE %, queue counts, dates, resource names, sigma values) from training data — Entegris's data lives only in the live DB. If tools fail or you cannot retrieve data, say so plainly to the user; do not improvise a plausible-sounding answer.",
        "1. Confirm column names BEFORE writing SQL. Use get_view_details(view_names=[...]) — pass multiple names in one call when joining or comparing views.",
        "2. The DB has the formulas. ECT, ET, BottleneckScore, Yield, Utilization%, OEE %, Sigma, IsOutlier are pre-computed in views — do not recompute in SQL or in your head.",
        "3. Filter from the user's words. Translate phrases like 'OV1110 last week' into a SQL WHERE clause.",
        "4. Use Bottleneck_MCP.vw_* views (curated for business users). Avoid dbo.Fact*/dbo.Dim* unless raw data is explicitly requested.",
        "5. Keep result sets bounded. Default limit 200, max 1000. Add TOP N + ORDER BY.",
        "6. **TALK LIKE A BUSINESS USER.** The audience is supervisors, plant managers, executives — NOT developers. Do NOT show SQL, tool names, view names, ResourceKey numbers, or audit trails in the final answer. Translate everything into manufacturing language: 'OV1110 ran at 78% OEE last week — Performance was the laggard at 62%' rather than 'vw_ResourceOEE_Daily.OEE_Pct = 78.00'.",
        "7. PICK the view yourself. get_topic_guide() returns rich descriptions of every topic. Read them and decide which fits — do not blindly trust keyword scoring.",
        "8. CLARIFY when ambiguous. If the user's question matches 2+ topics with similar specificity, or a required filter (Facility, Date, Resource) is missing, ask a short clarifying question instead of guessing.",
        "9. **ALWAYS RETURN A FINAL ANSWER BLOCK.** If your reasoning chain is long, produce a partial-but-structured answer at the end with the elements you DO have. Never cut off mid-investigation. Use a 'Status: partial' header and list which Expected elements remain unanswered if the data isn't ready yet. The user must always see a usable answer, never a truncated investigation.",
        "10. **DATA-AGNOSTIC RETRIEVAL.** Before any time-bounded question, call vw_DataAvailability and OPEN the answer by disclosing the actual data window. Never refuse based on assumed data presence — query the views, report exactly what comes back. Zero is valid. NULL is valid. For event-presence metrics (MTBF, MTTR, downtime %, target achievement) that return no rows or all zeros, REPORT '0 events / 0% / undefined' with one-line context — never 'data missing' or 'cannot answer'. The user's question is your contract; the DB's current state is your answer; bridge the two with disclosure, never with refusal. See `data_agnostic_retrieval` block for the exact wording template.",
        "11. **(reserved)**",
        "12. **ENUMERATE EVERY EXPECTED ELEMENT.** When the user's prompt asks for multiple items (e.g. 'show A, B, C, D' or '(1)…(2)…(3)…'), structure the answer as a numbered markdown list with ONE SECTION PER ITEM — do NOT collapse two items into one paragraph, do NOT skip an item because the answer is sparse. Even data-ceiling items get a section: '## 5. Downtime Reasons\\nNo Down events recorded in window — reason codes unavailable.' The user MUST scan the answer and see every requested element addressed individually.",
        "13. **FINAL ANSWER IS THE BUDGET PRIORITY.** Tool-call narration, SQL planning, and intermediate reasoning must NEVER push the final answer out of the response. If your reasoning is running long: STOP narrating, write the final answer block now. NEVER end with HTML/dashboard scaffolding alone. NEVER end with tool-trace narration alone. Reserve at least 100 words per Expected element for the final answer block.  ALSO: if must_include_in_answer demands more items than fit in the response budget, prioritize EVERY user-Expected element first; treat must_include as advisory beyond the user's literal list. Truncation of strategic-summary content is a contract violation.",
        "15. **MUST_INCLUDE LISTS ARE ROUTING HINTS, NOT EXHAUSTIVE CHECKLISTS.** When a decision_tree entry's must_include_in_answer has 5+ items AND the user's Expected Output already lists 5+ atomic elements: prioritize the user's literal list over the must_include enumeration. Use must_include as a 'don't skip these specific views/columns' guide, not as a section template. The LLM enumerates Expected elements naturally — must_include exists only to mandate non-obvious view invocations (chi-square, IsEstimatedDefault disclosure, etc.).",
    ],

    "forbidden_behaviours": [
        "❌ Inventing numbers (OEE %, ResourceKey, sigma, queue counts, dates, throughput, MTBF, MTTR) without a tool call this turn.",
        "❌ Inventing resource names, material IDs, shift IDs, or product names that you did not get from a tool result.",
        "❌ Quoting percentages or aggregates from training data — Entegris's data lives in the live DB, not in your weights.",
        "❌ Writing SQL from memory of column names — always confirm columns with get_view_details first.",
        "❌ Returning a 'plausible-looking' answer when tools failed — say tools failed instead.",
        "❌ Showing SQL, tool names, view names, ResourceKey numbers, or audit trails in the final business answer (those belong in your internal reasoning only, never in the user-facing reply).",
        "❌ Padding answers with manufacturing 'common knowledge' (typical OEE ranges, etc.) and presenting it as Entegris-specific.",
        "❌ Returning a response that is only HTML/dashboard scaffolding or only tool-call narration with no actual data answer at the end.",
        "❌ Truncating mid-investigation — the final answer block must always be reached.",
        "❌ Collapsing multi-element prompts: if user asks for A, B, C, D — your answer needs A, B, C, D as distinct sections.",
        "❌ Spending more words on disclosing data limits than on delivering available data. The disclosure is one line; the delivery is the rest of the answer.",
        "❌ Skipping invocation of a view that was specifically built for the user's question topic. If a decision_tree entry's must_include_in_answer cites a view by name, that view MUST be queried and its key columns MUST appear in the final answer.",
    ],

    "data_recovery_rules": [
        "For ranking questions, use RankTiebreak from vw_BottleneckRanked as the unique sort key when scores tie.",
        "Downtime fallback: when dbo.FactResourceServiceTime has no Down events (P1 plant-side ask), DownTimePct=0 and is misleading. The canonical fallback is vw_ResourceDowntime_Main.EffectiveDowntimePct (= DownTime + Standby) plus IsStandbyFallback (1 = Standby idle proxy, 0 = real downtime). Surface BOTH columns in answers about downtime/utilization.",
        "Bottleneck Frequency window: vw_BottleneckFrequency anchors on MAX(FactShift.Day), not GETDATE(). If the user asks about a window outside [EarliestDay, LatestDay] from vw_DataAvailability, openly state that the answer reflects the freshest 30 days of loaded data.",
        "Targets fallback: dbo.FactTargets is empty (P4). vw_BottleneckFrequency.LostHours/LostUnits, vw_FlowVelocity.TargetPPH, sp_BottleneckAnalysis.TimeSliceHistory.UnitsLost_InSlice all return NULL. Do not invent target numbers - say 'target not currently recorded' and report the actual / proxy figure instead.",
        "Queue-growth anchor mismatch: vw_QueueGrowthRate.HourBucket is anchored on GETDATE() while loaded data is Feb 12-17, 2026. As a result, GrowthLotsPerHour for many resources is 0 / no-overlap, which propagates into vw_BottleneckPrediction_NextShift (growth term = 0) and vw_AtRiskResources (RiskScore underweighted). Treat predictive scores as lower bounds until the source view is re-anchored on vw_DataAvailability.LatestDay.",
        "LotJourney filter convention: vw_LotJourney's LotName comes from DimMaterial.MaterialName (Entegris uses material name as the lot key). Use LIKE '%<token>%' rather than equality - operators commonly truncate or reformat the lot ID.",
    ],

    "data_agnostic_retrieval": (
        "DATA-AGNOSTIC RETRIEVAL — ALWAYS APPLY (this rule overrides any default 'I don't have data' instinct):\n"
        "\n"
        "1. Before answering ANY time-bounded question ('today', 'yesterday', 'last week', 'this month', etc.), "
        "FIRST call: SELECT EarliestDay, LatestDay, DaysCovered, LatestEvent FROM Bottleneck_MCP.vw_DataAvailability. "
        "Open the answer with one short sentence disclosing the actual window the data covers — e.g. "
        "'Data window: 2026-02-12 to 2026-02-17 (6 days). Treating \"yesterday\" as 2026-02-17.'\n"
        "\n"
        "2. NEVER refuse based on assumptions about data presence. Query the views, report exactly what comes back. "
        "Zero is a valid value. NULL is a valid value. Empty result set is a valid result. Disclose them as facts, not as failures.\n"
        "\n"
        "3. For metrics that depend on event presence (MTBF, MTTR, downtime %, target achievement, lost units): "
        "if the source returns no rows or all zeros, REPORT THE ACTUAL FIGURE with one-line context. "
        "Examples:\n"
        "  • '0 Down events recorded in 2,549 logged shift-state segments — MTBF undefined for this period.'\n"
        "  • 'EffectiveDowntimePct = 100% (Standby fallback; IsStandbyFallback=1 because no Down state events have been logged).'\n"
        "  • 'Target shortfall: not currently computable — FactTargets has 0 rows in the loaded window.'\n"
        "Do NOT say 'data missing' or 'cannot answer' as a terminal sentence. Always report the observed value first, then disclose the caveat.\n"
        "\n"
        "4. The user's question is your contract. The DB's current state is your answer. Bridge the two with disclosure, "
        "never with refusal. The user MUST always see a number, a state, or a labelled NULL — never a 'cannot answer'.\n"
        "\n"
        "5. **DISCLOSE BRIEFLY, DELIVER FULLY.** When data is partial (6-day window vs 30-day request, FactTargets empty vs target question, "
        "no Down events vs MTBF question): the data-window disclosure is ONE LINE at the top. The rest of the answer must surface every "
        "available metric in detail — shift-over-shift instead of week-over-week, IdealPPH × ShiftHours instead of FactTargets, "
        "EffectiveDowntimePct instead of DownTimePct. Maximum proxy delivery, minimum disclosure padding. If the user asked for 7 elements "
        "and 4 are data-ceiling, the disclosed elements still get full sections — they don't get collapsed to a single 'data unavailable' "
        "paragraph.\n"
        "\n"
        "This rule makes the MCP fully agnostic to whether it's pointed at a sandbox, a slim demo dataset, or full prod. "
        "Same code, same prompts — only the disclosed numbers differ."
    ),

    "server_routing_priority": (
        "FabOrchestrator has 3 MCP servers active (Bottleneck, OpcSemi, Tickets). "
        "For ANY question about a resource code like 'SD1100', 'OV1110', 'AI1100', 'LM1200' "
        "(uppercase letters + digits, no spaces) — use the Bottleneck MCP. "
        "DO NOT route these to OpcSemi (which is for containers/lots/products) or Tickets (which is for MES support tickets). "
        "Trigger words for Bottleneck: cycle time, queue length, downtime, MTBF, MTTR, utilization, "
        "throughput, WIP at a resource, current state, SEMI E10, OEE."
    ),

    "resource_name_resolution": (
        "For tokens like 'SD1100' (uppercase letters + digits), call sp_ResolveResource @Token=N'<token>' first. "
        "The returned MatchType ('Exact'/'Alias'/'Fuzzy'/'NotFound') tells you whether to proceed, ask, or refuse."
    ),

    "if_you_cannot_call_tools": [
        "Tell the user plainly: 'I'm not able to reach the data right now — please retry in a moment.' Do not improvise; if the user gave too little detail (facility / date / resource), ask one short clarifying question instead.",
    ],

    "business_language_style": {
        "audience": "Shift supervisors, plant managers, process engineers, operations executives.",
        "tone": "Plain English. Business-friendly. No SQL, no view names, no tool names, no ResourceKey integers, no schema references.",
        "utilization_proxy_caveat": (
            "UtilizationPct is computed as a PROXY (Productive seconds / total scheduled seconds) "
            "because dbo.FactTargets.IdealQuantity is currently empty (P4 plant-side ask). When the "
            "user asks 'utilization', do not call it 'OEE' or claim it factors target output. Use phrases "
            "like 'time-based utilization' or 'productive-time share' and note the caveat ONLY when the "
            "user pushes for a target-vs-actual reading."
        ),
        "do": [
            "'OV1110 ran at 78% OEE last week.'",
            "'18 lots are queued at the bottleneck — the longest has been waiting 4 hours.'",
            "'Cycle time at LM1100 is 23% above its weekly average.'",
            "'Performance is the laggard — Availability and Quality are healthy.'",
            "For partial-data prompts, lead with proxy delivery — example for 'WoW past month' when DB has 6 days: "
            "'## Data window: Feb 12–17 (6 days loaded). Comparing first half (Feb 12–14) vs second half (Feb 15–17) as the closest WoW proxy:\\n\\n"
            "## 1. Bottleneck Occurrence Trend\\n<concrete numbers>\\n## 2. Avg ET Trend\\n<concrete numbers>\\n…' — "
            "every Expected element gets a section with available proxy data. Do NOT shrink to a single disclosure paragraph.",
        ],
        "do_not": [
            "'vw_ResourceOEE_Daily.OEE_Pct returned 78.00 for ResourceKey = 41.'",
            "'Tools used: get_view_details, query_view ...'",
            "'I called mcp_get_instructions() then mcp_query_view(...).'",
            "'The Bottleneck_MCP schema contains a view named vw_BottleneckRanked.'",
        ],
        "if_data_is_missing": (
            "Say so in plain language: 'OEE for OV1110 cannot be computed for that period — "
            "Performance and Quality data weren't recorded.' Do not say 'NULL' or expose the column name."
        ),
        "ranking_output_rules": [
            "Top-N answers MUST be presented as a numbered/markdown table with: Rank, Resource Name, Primary Metric value, and a Tiebreaker column when ties exist.",
            "If all rows are tied on the primary metric, explicitly call out 'all tied at X%' in the lead sentence and add a tiebreaker column (uptime hours, throughput, WIP).",
            "Never return a top-N where the user can't tell rank #1 from rank #5.",
        ],
    },
    "clarification_policy": {
        "when_to_ask_user": [
            "Ambiguous topic: question matches 2+ topics with similar specificity (e.g., 'rework affecting bottleneck' could be Topic 10 OR Topic 1).",
            "Missing required filter: question implies a facility / date range / resource but does not name one ('show me the bottleneck' without facility).",
            "Privacy-sensitive: question targets a specific operator by name (Topic 11 — confirm authorisation level).",
            "Time scope unclear: 'recent' / 'lately' / 'now' — ask whether this means today, this shift, this week.",
            "Filter values out of range: user asks about a date older than data exists (DB only has Feb 2026 onward).",
            "User wants 'everything' — confirm scope (everything could mean 100k rows; cap at 1000).",
        ],
        "how_to_ask": [
            "Be brief: one or two short questions, never a wall of text.",
            "Offer choices when possible: 'Did you mean A (rework volume) or B (root-cause attribution)?'",
            "Show what defaults you'd use if the user prefers: 'If you'd like, I can default to facility 4266, last 24h.'",
            "Never block on trivial defaults — if a sensible default exists (TopN=20, limit=200), apply it and tell the user.",
        ],
        "examples": [
            {
                "user_question": "Show me the bottleneck.",
                "ambiguity": "No facility specified. KSP has 3 facilities.",
                "good_clarification": "Which facility — 4266 (the main KSP line, 175 resources) or one of the others? If unsure, I'll default to 4266 for the current shift.",
            },
        ],
    },
    "decision_tree": [
        {
            "if_user_asks": "WHERE the bottleneck is / which resource is slowest / current constraint",
            "use_topic": "Current Bottleneck Detection",
            "primary_view": "vw_BottleneckRanked",
            "key_columns": ["ResourceName", "ECT", "ET", "QueueLength", "BottleneckScore", "IsBottleneck"],
            "filter_pattern": (
                "WHERE FacilityKey = X ORDER BY BottleneckScore. "
                "If the user names a Production Line ID like 'PL-101' or 'OV-105' (regex `[A-Z]{2}-\\d{3}`): "
                "EXEC sp_ResolveResource @Token=N'<token>' — the SP now returns MatchType='ProductionLine' "
                "and resolves to the underlying ResourceKey. Don't search ProductDef or any product table for these tokens."
            ),
            "must_include_in_answer": [
                "Resource name (never ResourceKey)",
                "Effective Throughput (ET) value with units (units/hour)",
                "Queue Length (current count of lots waiting)",
                "Downtime % for the active shift",
                "Utilization percentage for the active shift (UtilizationPct column)",
                "Bottleneck Score (for ranking context)",
            ],
        },
        {
            "if_user_asks": "WHY a resource is the bottleneck / equipment vs material vs operator",
            "use_topic": "Root Cause Analysis",
            "primary_view": "vw_RootCause_Combined",
            "key_columns": ["EquipmentScore", "MaterialScore", "OperatorScore", "DominantCause"],
            "filter_pattern": "WHERE FacilityKey = X AND ResourceKey = Y",
            "must_include_in_answer": [
                "Operator skill: SkillLevel column on vw_OperatorAtResource (literal 'Unrated' until DimEmployee gets a skill column — disclose the source caveat)",
                "State sub-category and standby cause: surface SubState + StandbyCauseCategory from vw_SEMIE10StateDistribution. Disclose 'Unclassified' explicitly when the source reason text doesn't match any known category.",
            ],
        },
        {
            "if_user_asks": "lots that are stuck / waiting / oldest queued",
            "use_topic": "Drill-down / What's Stuck",
            "primary_view": "vw_WaitingLots_Details",
            "key_columns": ["MaterialName", "ResourceName", "HoursWaiting", "AgeBucket"],
            "filter_pattern": "WHERE HoursWaiting > N ORDER BY HoursWaiting DESC",
        },
        {
            "if_user_asks": "cycle time / actual vs ideal / how long",
            "use_topic": "Cycle Time",
            "primary_view": "vw_ResourceCycleTime_Main",
            "key_columns": ["AvgCycleTimeSec", "EndToEndCycleTimeSec", "IdealCycleTime"],
            "note": "Use EndToEndCycleTimeSec (Entegris-canonical) for 'real' wait+process time; AvgCycleTimeSec for active processing only.",
            "filter_pattern": (
                "STEP 1 — EXEC sp_ResolveResource @Token=N'<USER_TOKEN>'. "
                "STEP 2 — once you have ResourceKey, query vw_ResourceCycleTime_Main "
                "with WHERE ResourceKey = @ResourceKey AND ShiftKey = (latest from CurrentShift rule)."
            ),
            "must_include_in_answer": [
                "Resource name",
                "Average cycle time (with units — seconds or minutes)",
                "Ideal cycle time (target)",
                "Delta vs ideal as % (positive = slower than spec)",
            ],
            "do_not": [
                "Do NOT search Container, MaterialAtResource, ProductDef, or any lot/product table — bare tokens like 'SD1100' are RESOURCE codes, not products.",
                "Do NOT respond 'I cannot find SD1100' before calling sp_ResolveResource; an Exact match almost always exists.",
            ],
        },
        {
            "if_user_asks": "queue depth / how many waiting / WIP",
            "use_topic": "Queue & WIP",
            "primary_view": "vw_ResourceQueue_Main",
            "detail_views": ["vw_ResourceWIP_Snapshot (point-in-time)", "vw_ResourceWIP_Main (per-shift)"],
            "key_columns": ["QueueCount", "AvgQueuedLag", "MaxQueuedLag"],
            "filter_pattern": (
                "STEP 1 — EXEC sp_ResolveResource @Token=N'<USER_TOKEN>'. "
                "STEP 2 — once you have ResourceKey, query vw_ResourceQueue_Main / vw_ResourceWIP_Snapshot "
                "with WHERE ResourceKey = @ResourceKey. ResourceName / AreaName come back clean (no '-' masking)."
            ),
            "must_include_in_answer": [
                "Resource name",
                "Queue count (number of LOTS waiting) — from QueueCount column",
                "Total UNITS waiting — from WaitingUnits column. NEVER omit this even if 0.",
                "Average wait time in hours — use AvgQueuedLag_Hours (already converted from seconds)",
                "Oldest queued lot — material name and age in hours",
            ],
        },
        {
            "if_user_asks": "throughput / PPH / output / yield / loss",
            "use_topic": "Throughput & Target",
            "primary_view": "vw_ResourceThroughput_Main",
            "key_columns": ["ActualPPH", "IdealPPH", "AchievementPct", "Yield", "QuantityIn", "QuantityOut", "LossQuantityAbsolute"],
            "filter_pattern": (
                "STEP 1 (only if the user named a SPECIFIC resource like 'throughput for SD1100') — "
                "EXEC sp_ResolveResource @Token=N'<USER_TOKEN>' to confirm ResourceKey. "
                "Skip STEP 1 when the question is a ranking ('top 5 by throughput') or area-wide aggregate. "
                "STEP 2 — query vw_ResourceThroughput_Main with WHERE FacilityKey = @FacilityKey "
                "(plus AND ResourceKey = @ResourceKey when STEP 1 ran)."
            ),
            "must_include_in_answer": [
                "Resource name",
                "Actual PPH (parts per hour) with units",
                "Ideal/target PPH",
                "Achievement % (actual vs target)",
                "Yield % when output and input quantities are both available",
            ],
            "do_not": [
                "Do NOT search Container, MaterialAtResource, ProductDef, or any lot/product table — bare tokens like 'SD1100' are RESOURCE codes.",
            ],
        },
        {
            "if_user_asks": "current status / current state / what state is X in / is X up or down / SEMI E10 state of a SPECIFIC resource",
            "use_topic": "Downtime & Utilization",
            "primary_view": "vw_SEMIE10StateDistribution",
            "key_columns": ["ResourceName", "SEMI_E10_State", "StateHours", "ShiftKey", "Day"],
            "filter_pattern": (
                "STEP 1 — EXEC sp_ResolveResource @Token=N'<USER_TOKEN>'. "
                "STEP 2 — query vw_SEMIE10StateDistribution WHERE ResourceKey = @ResourceKey AND "
                "ShiftKey = (latest from CurrentShift rule) ORDER BY StateHours DESC; the state with the "
                "most hours in the latest shift is the 'current' state."
            ),
            "must_include_in_answer": [
                "Resource name",
                "Current SEMI E10 state (Productive / Standby / Engineering / Scheduled Down / Unscheduled Down)",
                "Hours in that state for the active shift",
                "Note if only Productive + Standby are recorded (downtime states not logged)",
            ],
            "do_not": [
                "Do NOT search Container, MaterialAtResource, or any lot/product table — the question is about resource state, not material location.",
                "Do NOT respond 'I cannot find SD1100' before checking list_parameter_values('resource'); these are resource codes.",
            ],
        },
        {
            "if_user_asks": "downtime / OEE / availability / MTBF / MTTR / SEMI E10 states",
            "use_topic": "Downtime & Utilization",
            "primary_view": "vw_ResourceDowntime_Main",
            "detail_views": ["vw_SEMIE10StateDistribution", "vw_DowntimeReasons"],
            "key_columns": ["UpTimePct", "DownTimePct", "MTBF_Hours", "MTTR_Hours"],
            "filter_pattern": (
                "STEP 1 — EXEC sp_ResolveResource @Token=N'<USER_TOKEN>'. "
                "STEP 2 — query vw_ResourceDowntime_Main WHERE ResourceKey = @ResourceKey."
            ),
            "must_include_in_answer": [
                "Resource name",
                "EffectiveDowntimePct (the headline figure when DownTimePct = 0)",
                "IsStandbyFallback flag — 1 means the value came from a Standby idle proxy; 0 means real downtime",
                "MTBF hours (when available)",
                "MTTR hours (when available)",
                "Top downtime reason if any",
            ],
            "downtime_fallback_note": (
                "When DownTimePct = 0 across all resources (SEMI E10 logging incomplete) AND the user asked for multiple elements "
                "(e.g. 'longest downtime, percentage, and reason'), ADDRESS EACH ELEMENT INDIVIDUALLY — do not collapse them into a single "
                "'no data exists' sentence. Use this exact format:\n"
                "  • Resource: [Standby-fallback resource name with most idle hours, ranked by EffectiveDowntimePct DESC]\n"
                "  • Downtime percentage: 0% (no Down events; using EffectiveDowntimePct = X% as Standby proxy, IsStandbyFallback = 1)\n"
                "  • Reason: not recorded (no rows in vw_DowntimeReasons; possible reasons from DimReason catalog: ...).\n"
                "Each line must explicitly address the asked element so the user sees nothing was missed."
            ),
        },
        {
            "if_user_asks": "history / trend / over the last week / how often / list bottlenecks for [period]",
            "use_topic": "Historical Trend",
            "primary_view": "vw_BottleneckHistory",
            "drill_view": "vw_BottleneckFrequency",
            "filter_pattern": (
                "For 'list bottleneck resources' or 'how often was X the bottleneck': prefer "
                "vw_BottleneckFrequency (returns one row per resource with TimesBottleneck + FrequencyPct + FirstDay + LastDay). "
                "For shift-by-shift trace: use vw_BottleneckHistory. "
                "vw_BottleneckFrequency has AreaKey + AreaName columns natively — area-filtered queries like "
                "'bottlenecks in the Assembly area last week' should add WHERE AreaName = '<Area>' directly to vw_BottleneckFrequency "
                "and surface the same TimesBottleneck + FrequencyPct columns per resource within that area. "
                "Anchor 'last week' / 'last month' on MAX(Day) from vw_DataAvailability — never the wall clock."
            ),
            "must_include_in_answer": [
                "List of resources that were bottlenecks in the date range",
                "Per-resource occurrence count (TimesBottleneck column from vw_BottleneckFrequency)",
                "Per-resource frequency percentage (FrequencyPct column)",
                "Date range covered, anchored to MAX(Day) if user said 'last week' / 'last month'",
            ],
        },
        {
            "if_user_asks": "compare two shifts / today vs yesterday / area vs area / which area is more severe",
            "use_topic": "Comparative",
            "primary_view": "vw_ShiftComparison",
            "drill_views": ["vw_AreaComparison", "vw_FacilityComparison"],
            "must_include_in_answer": [
                "Both areas/shifts named with concrete labels (Cleaning vs Assembly, Day vs Night, etc.)",
                "Bottleneck frequency (count or pct) for each",
                "Average queue length for each",
                "Average Effective Throughput (ET) for each — required for severity comparison",
                "One-line verdict: which is more severe and why",
                "Operator-side breakdown for each shift — pull from vw_OperatorAtResource (Yield, ReworkCount, ET per operator). Required for 'Compare Day vs Night for area X' questions.",
                "Statistical significance: cite vw_StatisticalCorrelations.SignificantAt95Pct (BIT — 1 means |r| > 2/√n at 95%) when SampleSize ≥ 30; else state 'sample too small for significance test'",
            ],
        },
        {
            "if_user_asks": "bottlenecks for [product] / per-product bottleneck breakdown / which resource is the bottleneck for product X",
            "use_topic": "Bottleneck for Product",
            "primary_view": "vw_BottleneckByProduct",
            "key_columns": ["ProductKey", "ProductName", "ProductionVolumeBucket", "BottleneckResourceKey", "BottleneckResourceName", "AvgQueue", "ET"],
            "filter_pattern": "WHERE FacilityKey = X AND ProductName = '<USER_PRODUCT>' (or ProductKey = Y)",
            "must_include_in_answer": [
                "List of bottleneck resources for the product",
                "Most recent shift identified",
                "Production quantity (UnitsProduced or PrimaryQuantity for the product)",
                "Queue metrics — AvgQueue, queue length, avg/max wait",
                "ProductionVolumeBucket label (High/Medium/Low) for context",
            ],
        },
        {
            "if_user_asks": "production line / which line / line PL-101 / how is line X performing",
            "use_topic": "Production Line Mapping",
            "primary_view": "vw_ProductionLine_Mapping",
            "key_columns": ["ResourceKey", "ResourceName", "ProductionLineCode", "ProductionLineName", "ProductionLineId", "IsHeuristic"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally ProductionLineCode = 'PL' or ProductionLineId = 'PL-101'). The mapping is heuristic (first 2 chars of ResourceName); IsHeuristic=1 flag must be surfaced.",
            "must_include_in_answer": [
                "ProductionLineId (e.g. 'PL-101')",
                "Resolved resource name(s)",
                "IsHeuristic=1 disclosure: 'Production Line ID is inferred from ResourceName naming convention; refine when DimResource gets a real ProductionLine column (P7).'",
            ],
        },
        {
            "if_user_asks": "staffing / operators on shift / headcount / coverage / was the shift under-staffed",
            "use_topic": "Shift Staffing",
            "primary_view": "vw_ShiftStaffing_Daily",
            "key_columns": ["AreaName", "ShiftDay", "ShiftName", "UniqueOperators", "ResourcesStaffed", "AvgEventsPerOperator", "CoverageRatio"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally AreaKey = Y or ShiftDay BETWEEN @from AND @to). Coverage ratio < 1 means fewer operators than resources staffed.",
            "must_include_in_answer": [
                "Shift identification (ShiftDay + ShiftName + AreaName)",
                "UniqueOperators count",
                "ResourcesStaffed count",
                "CoverageRatio with one-line interpretation (e.g. '0.6 means 6 operators across 10 resources — 4 resources unmanned')",
                "AvgEventsPerOperator as a workload proxy",
            ],
        },
        {
            "if_user_asks": "rework count / how is rework affecting",
            "use_topic": "Rework Impact",
            "primary_view": "vw_ReworkImpact_Main",
            "must_include_in_answer": [
                "ReworkInflatedQuantity (column on vw_ReworkImpact_Main)",
                "Yield % per resource (compare to vw_ResourceThroughput_Main.Yield)",
                "ReworkCount and TopReworkReason from vw_ReworkImpact_Main",
                "Scrap and defect breakdown: ScrapUnits + ScrapPct + TopDefectCategory columns from vw_ReworkImpact_Main. If TopDefectCategory='Uncategorized' (no naming-pattern match), disclose that the defect taxonomy is not yet mapped at the material level.",
            ],
        },
        {
            "if_user_asks": "operator at the bottleneck / per-operator cycle time",
            "use_topic": "Operator at Bottleneck",
            "primary_view": "vw_OperatorAtResource",
            "note": "Privacy-sensitive — only respond at aggregate or supervisor level.",
            "must_include_in_answer": [
                "DowntimePct_PerOperator (newly added column on vw_OperatorAtResource)",
                "StdDevCycleSec (newly added column — measure of consistency per operator)",
                "BottleneckShiftCount (newly added column — count of shifts where this operator was on the bottleneck)",
                "For statistical comparison ('is operator X significantly slower than peers'): cite vw_StatisticalCorrelations.SignificantAt99Pct + TTest_PValue when SampleSize >= 30.",
            ],
        },
        {
            "if_user_asks": "alternate resources / spare capacity / queue growing",
            "use_topic": "Predictive / What-If",
            "primary_view": "vw_ParallelResourceCapacity",
            "detail_views": ["vw_QueueGrowthRate"],
        },
        {
            "if_user_asks": "full report / shift summary + detail + time-slice / executive view",
            "use_tool": "run_stored_procedure",
            "procedure": "sp_BottleneckAnalysis",
            "params_required": ["FacilityKey", "DateFrom", "DateTo"],
            "params_optional": ["AreaKey", "ShiftKey", "TopN", "IncludeTimeSlice", "SliceIntervalMinutes"],
            "returns": "3 result sets: ShiftSummary | ResourceDetail | TimeSliceHistory (TimeSliceHistory now includes UnitsProduced_InSlice, UnitsLost_InSlice, TransitionTrigger)",
        },
        {
            "if_user_asks": "X consecutive shifts / persistent bottleneck / N times this week / repeated occurrences / why X keeps becoming bottleneck",
            "use_topic": "Historical Trend",
            "primary_view": "vw_PersistentBottleneck",
            "drill_view": "vw_BottleneckFrequency",
            "key_columns": ["ResourceKey", "ResourceName", "RunLength", "FirstShift", "LastShift"],
            "filter_pattern": "Use vw_PersistentBottleneck (already runs gaps-and-islands) for consecutive-shift questions; for total occurrence counts use vw_BottleneckFrequency (TimesBottleneck + FrequencyPct + FirstDay/LastDay).",
            "must_include_in_answer": [
                "List of resources that were bottlenecks in the date range",
                "Per-resource occurrence count (TimesBottleneck from vw_BottleneckFrequency)",
                "Per-resource frequency percentage (FrequencyPct)",
                "RunLength + FirstShift/LastShift when the user asks 'how many shifts in a row'",
                "Date range covered, anchored to MAX(Day) if user said 'last week' / 'last month'",
                "Material-release timing: chain vw_ArrivalPattern + vw_MaterialFlowJourney_Detail.upstream_release_window — surface BurstIndex and InterArrivalCV",
                "Dispatching policy: vw_DispatchingRulesEvaluation.DispatchingPolicy + PriorityLeakagePct",
            ],
        },
        {
            "if_user_asks": "predict / forecast next shift bottleneck / which resource will become bottleneck / who is at risk next shift",
            "use_topic": "Predictive",
            "primary_view": "vw_BottleneckPrediction_NextShift",
            "drill_views": ["vw_AtRiskResources", "vw_QueueGrowthRate", "vw_ScheduledMaintenance"],
            "key_columns": ["ResourceKey", "ResourceName", "Probability", "BasisFactors"],
            "filter_pattern": (
                "Chain three queries: (1) vw_BottleneckPrediction_NextShift for the probability score; "
                "(2) vw_AtRiskResources for utilization + queue-growth risk score; "
                "(3) vw_QueueGrowthRate for projected queue size at end of next shift. "
                "Anchor 'next shift' on vw_LatestShift.LatestShiftKey + 1 (or just the latest available)."
            ),
            "must_include_in_answer": [
                "Resource(s) at risk with probability score (from vw_BottleneckPrediction_NextShift)",
                "Current ET trend direction (from vw_ThroughputDelta_24h)",
                "Queue growth rate per hour (GrowthLotsPerHour from vw_QueueGrowthRate)",
                "Projected queue size at end of next shift",
                "Scheduled maintenance flag (vw_ScheduledMaintenance — currently 0 rows; report 'no scheduled maintenance recorded' explicitly)",
                "Recommended preventive actions in plain English",
            ],
        },
        {
            "if_user_asks": "production shortfall / lost units / units we could have made / target vs actual gap / target tracking / month-to-date forecast / will we make plan",
            "use_topic": "Throughput & Target",
            "primary_view": "vw_ShiftTarget",
            "drill_views": ["vw_LostProduction", "vw_FacilityPerformance_Daily", "vw_AreaWhatIf_ThroughputLift"],
            "math_hint": "Shortfall_units = (TargetUnits − ActualSoFar). Lost-production = (IdealPPH − ActualPPH) × ShiftHours summed across shifts. Show both number and as % of target.",
            "must_include_in_answer": [
                "Monthly target vs actual to-date — when FactTargets is empty, disclose: 'Production targets not currently loaded; gap analysis cannot be computed.'",
                "Days remaining in the period — date math: DATEDIFF(DAY, MAX(Day), EOMonth)",
                "Actual daily production rate — vw_FacilityPerformance_Daily.TotalLotsProduced ÷ days elapsed",
                "Linear forecast to period-end — actual_to_date + (actual_daily_rate × days_remaining)",
                "Scenario analysis: invoke vw_AreaWhatIf_ThroughputLift for '+5% OEE / weekend shift / eliminate top bottleneck'",
                "Risk assessment (high/medium/low) based on shortfall vs days remaining",
                "Recommendations to close the gap",
            ],
        },
        {
            "if_user_asks": "cascade / ripple / impact on downstream / starvation / upstream backup",
            "use_tool": "run_stored_procedure",
            "procedure": "sp_BottleneckCascade",
            "params_required": ["FacilityKey", "ShiftKey"],
            "params_optional": ["DateFrom", "DateTo"],
            "returns": "4 result sets: Bottleneck | UpstreamBackup (with FacilityImpactUnits) | DownstreamStarvation (with FacilityImpactUnits) | CascadeTimeline",
            "fallback_chain": "Backup only if the SP is unavailable: (1) vw_BottleneckRanked → (2) vw_ResourceQueue_Main upstream + vw_ResourceWIP_Snapshot downstream → (3) compose 'Bottleneck / Upstream backing up / Downstream starved' sections.",
            "must_include_in_answer": [
                "Time-to-recovery: derive from CascadeTimeline.first_recovery_slice minus disruption_start_slice (4th resultset of sp_BottleneckCascade)",
                "Secondary bottleneck identification — surface 'Position'='Secondary' rows from CascadeTimeline",
            ],
        },
        {
            "if_user_asks": "WoW / week-over-week / MoM / month-over-month / weekday vs weekend / improving or worsening",
            "use_topic": "Comparative Analysis",
            "pattern": "Run two queries on vw_BottleneckHistory or vw_ResourceDowntime_Main with disjoint date ranges; compute deltas in your reasoning; output side-by-side table with absolute values + % delta + arrow direction (↑/↓).",
        },
        # D1 — bottleneck migration / how the bottleneck changed across days
        {
            "if_user_asks": "bottleneck migration / how the bottleneck changed / which resource is the bottleneck most often",
            "use_topic": "Historical Trend",
            "primary_view": "vw_BottleneckHistory",
            "drill_views": ["vw_PersistentBottleneck", "vw_BottleneckFrequency"],
            "key_columns": ["Day", "ShiftKey", "BottleneckResourceName", "RunLength", "TimesBottleneck", "BottleneckHours"],
            "filter_pattern": "WHERE FacilityKey = X AND Day BETWEEN '...' AND '...' ORDER BY Day, ShiftKey. Then drill vw_PersistentBottleneck for streaks (RunLength) and vw_BottleneckFrequency for total occurrences.",
            "must_include_in_answer": ["Resource that migrated in/out of bottleneck role", "When (Day/Shift)", "Streak length (RunLength) for any resource that persisted >= 2 shifts"],
        },
        # D2 — comprehensive bottleneck RCA
        {
            "if_user_asks": "comprehensive RCA / multi-cause / why is X the bottleneck (full picture)",
            "use_topic": "Root Cause Analysis",
            "primary_view": "vw_BottleneckRanked",
            "chain": [
                "1. vw_BottleneckRanked → identify the bottleneck.",
                "2. vw_RootCause_Combined → which dimension dominates (Equipment / Material / Operator).",
                "3. vw_OperatorAtResource → operator-side detail (Yield, ReworkCount, ET).",
                "4. vw_ReworkImpact_Main → material-side detail (ReworkReason, MaterialName).",
            ],
            "must_include_in_answer": [
                "Bottleneck resource + score",
                "Dominant root-cause dimension",
                "If operator-driven: worst-Yield / worst-ET operator",
                "If material-driven: top ReworkReason and MaterialName",
            ],
        },
        # D3 — lot history / journey / single lot trace
        {
            "if_user_asks": "lot history / lot journey / where has lot X been / step trace for a lot",
            "use_topic": "Drill-down / What's Stuck",
            "primary_view": "vw_LotJourney",
            "key_columns": ["LotName", "StepSeq", "StepName", "ResourceName", "TrackInTime", "QueueSec", "ProcessSec"],
            "filter_pattern": "WHERE LotName LIKE '%<token>%' (or MaterialName LIKE) ORDER BY StepSeq",
            "must_include_in_answer": [
                "Each step in order with resource name",
                "Time at each resource",
                "Total queue + process time per step",
                "TransitTimeSec column from vw_MaterialFlowJourney_Detail (already pre-computed; do NOT recompute via LEAD)",
            ],
        },
        # D4 — shift handover / Day-to-Night transition
        {
            "if_user_asks": "shift handover / Day-to-Night transition / does throughput dip at handover",
            "use_topic": "Comparative Analysis",
            "primary_view": "vw_ShiftHandoverImpact",
            "key_columns": ["AreaName", "ShiftBoundaryDateTime", "LastHourDayET", "FirstHourNightET", "DeltaPct"],
            "filter_pattern": "WHERE FacilityKey = X ORDER BY DeltaPct ASC (most negative = biggest drop at handover)",
            "must_include_in_answer": ["Area name", "Day-shift ET vs Night-shift ET", "DeltaPct (negative = drop at handover)"],
        },
        # D5 — predict next shift bottleneck / forecast
        {
            "if_user_asks": "predict next shift / forecast bottleneck / who is at risk next shift",
            "use_topic": "Historical Trend",
            "primary_view": "vw_BottleneckPrediction_NextShift",
            "key_columns": ["ResourceName", "Probability", "BasisFactors"],
            "filter_pattern": "WHERE FacilityKey = X ORDER BY Probability DESC",
            "must_include_in_answer": ["Top 3-5 resources by Probability", "BasisFactors (heuristic mix)", "Note: Probability is heuristic, not statistical"],
        },
        # D6 — variance vs bottleneck correlation
        {
            "if_user_asks": "variance vs bottleneck / does cycle-time variability cause bottlenecking / cycle time variance / stability / control chart / drift detection",
            "use_topic": "Cycle Time Outliers",
            "primary_view": "vw_CycleTime_Statistics",
            "drill_views": ["vw_BottleneckFrequency", "vw_CycleTimeTrend", "vw_CycleTime_Outliers"],
            "pattern": "Join vw_CycleTime_Statistics (StdDevCycleSec) with vw_BottleneckFrequency (TimesBottleneck) on ResourceKey; resources with high stddev AND high TimesBottleneck are the variability-driven bottlenecks.",
            "must_include_in_answer": [
                "Daily cycle time variance trend — table or markdown chart of StdDevCycleSec by date from vw_CycleTime_Statistics + vw_CycleTimeTrend",
                "Control chart bounds: Mean ±2σ and ±3σ explicitly listed in the answer",
                "Out-of-control points: list of dates/lots where Sigma > 3 from vw_CycleTime_Outliers",
                "Coefficient of variation (CV) = StdDevCycleSec / MeanCycleSec — show value and trend direction",
                "Stability assessment: stable / drifting / unstable verdict",
                "If user asks for Cp/Cpk: data-ceiling response — disclose 'Spec limits (USL/LSL) not loaded; process capability indices not computable for this period'",
                "Recommendations for variation reduction",
            ],
        },
        # D7 — WIP vs wait relationship / threshold detection
        {
            "if_user_asks": "WIP vs wait / WIP threshold / when does WIP become a problem / recommended WIP cap",
            "use_topic": "Queue & WIP",
            "primary_view": "vw_WIPWaitRelationship",
            "drill_views": ["vw_WIPBottleneckCorrelation"],
            "key_columns": ["AreaName", "AvgWIP", "AvgWaitHours", "KneePointWIP", "RecommendedWIPCap"],
            "filter_pattern": "WHERE KneePointWIP IS NOT NULL ORDER BY KneePointWIP DESC. Drill vw_WIPBottleneckCorrelation for per-resource Pearson R between WIP and IsBottleneck.",
            "must_include_in_answer": ["Area name", "KneePointWIP (where wait time more than doubles)", "RecommendedWIPCap", "If null: state that no clear knee was detected in current data"],
        },
        # ====================================================================
        # v3.3.0 — Group X4 — 5 NEW decision_tree entries pointing at X3 views
        # ====================================================================
        # X4-1 — Dispatching Rules Evaluation
        {
            "if_user_asks": "dispatching policy / why is one lot dispatched before another / FIFO vs priority / fairness of release order",
            "use_topic": "Predictive / Operational",
            "primary_view": "vw_DispatchingRulesEvaluation",
            "key_columns": ["ResourceName", "DispatchingPolicy", "PriorityLeakagePct", "AvgWaitDeviation", "SampleLots"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally ResourceKey = Y). NoData when SampleLots < 30.",
            "must_include_in_answer": [
                "Resource name",
                "Inferred dispatching policy ('FIFO' / 'Priority' / 'Mixed' / 'NoData')",
                "PriorityLeakagePct (% of out-of-order dispatches)",
                "AvgWaitDeviation (proxy for fairness)",
                "If DispatchingPolicy='NoData' (sample < 30 lots), report it explicitly",
            ],
        },
        # X4-2 — Operational Events Log
        {
            "if_user_asks": "operational events / what happened around time T / event timeline / what state changes occurred",
            "use_topic": "Operational Events",
            "primary_view": "vw_OperationalEventsLog",
            "key_columns": ["EventTime", "EventType", "EventDetail", "ResourceName"],
            "filter_pattern": "WHERE EventTime BETWEEN @from AND @to (and optionally ResourceKey = Y or EventType IN ('StateChange','ShiftHandover','BottleneckTransition'))",
            "must_include_in_answer": [
                "Chronological list of events ordered by EventTime",
                "EventType + EventDetail per row",
                "Resource name (never ResourceKey)",
                "Total event count and breakdown by EventType",
            ],
        },
        # X4-3 — Resource Cost Model
        {
            "if_user_asks": "cost / hourly operating cost / downtime cost / ROI / investment / how much did this cost",
            "use_topic": "Cost & Investment",
            "primary_view": "vw_ResourceCostModel",
            "key_columns": ["HourlyOperatingCost", "DownEventEstimatedCost", "InvestmentEstimate", "IsEstimatedDefault"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally ResourceKey = Y).",
            "must_include_in_answer": [
                "Resource name",
                "Cost figure(s) requested (hourly operating, down-event, investment)",
                "Currency",
                "**MUST disclose IsEstimatedDefault=1** — say 'estimated default; not validated by Finance' explicitly. Never present default cost as authoritative.",
            ],
            "do_not": [
                "Do NOT round / re-format the default values to look more 'real'.",
                "Do NOT compute revenue, ROI, or payback without surfacing the IsEstimatedDefault flag in the same answer.",
            ],
        },
        # X4-4 — Material Price Catalog
        {
            "if_user_asks": "unit value / material price / revenue / how much is this material worth",
            "use_topic": "Material Pricing",
            "primary_view": "vw_MaterialPriceCatalog",
            "key_columns": ["MaterialName", "UnitValue", "Currency", "IsEstimatedDefault"],
            "filter_pattern": "WHERE MaterialKey = X (or MaterialName LIKE '%token%').",
            "must_include_in_answer": [
                "Material name",
                "Unit value + currency",
                "**MUST disclose IsEstimatedDefault=1** — 'estimated $100/unit default; refine via MaterialPrice_Manual table when Finance provides real values'.",
            ],
            "do_not": [
                "Do NOT compute total revenue without surfacing the IsEstimatedDefault flag.",
            ],
        },
        # X4-5 — Resource Lifecycle
        {
            "if_user_asks": "how old is the equipment / lifecycle / months in service / when was this resource installed",
            "use_topic": "Resource Lifecycle",
            "primary_view": "vw_ResourceLifecycle",
            "key_columns": ["ResourceName", "MonthsInService", "ShiftsObserved", "FirstServiceTimestamp", "LastServiceTimestamp", "TotalProductiveHours"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally ResourceKey = Y).",
            "must_include_in_answer": [
                "Resource name",
                "MonthsInService (computed from DimResource.CreateTimestamp anchored on vw_DataAvailability.LatestEvent)",
                "First and last observed service timestamps",
                "TotalProductiveHours over the data window",
                "If MonthsInService = 0 (CreateTimestamp recent vs window), explicitly note 'within the loaded data window' instead of implying brand-new equipment",
            ],
        },
        # ====================================================================
        # v3.6.0 — UC5+UC7 enrichment — 8 NEW decision_tree entries
        # ====================================================================
        # v3.6.0-1 — Outlier Lot Detail (per-lot deep-dive with RootCauseCategory)
        {
            "if_user_asks": "lot detail / outlier drill-down / RootCauseCategory / per-lot deep dive / which outlier lots and why / cycle time outliers / variance / outlier detection",
            "use_topic": "Cycle Time Outliers",
            "primary_view": "vw_OutlierLotDetail",
            "drill_views": ["vw_CycleTime_Outliers", "vw_CategoricalSignificanceTests"],
            "key_columns": ["LotName", "ResourceName", "Sigma", "IsOutlier", "OperatorName", "RootCauseCategory", "ActualCycleSec", "IdealCycleSec", "DeviationPct"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally ResourceKey = Y) ORDER BY ABS(Sigma) DESC. Filter IsOutlier = 1 for the unusual lots only.",
            "must_include_in_answer": [
                "Outlier lots: LotName, ActualCycleSec, IdealCycleSec, DeviationPct, Sigma — from vw_CycleTime_Outliers (extended in v3.6.0)",
                "Statistical threshold actually used (e.g. 'Sigma > 2', 'IsOutlier=1')",
                "Mean cycle time vs outlier cycle times comparison",
                "Root cause categorization — from vw_OutlierLotDetail.RootCauseCategory",
                "When ranking resources/shifts/operators by outlier rate: invoke vw_CategoricalSignificanceTests for chi-square/Fisher significance. If IsLowSampleConfidence=1, OPEN with the disclosure: 'Sample size is below the chi-square validity threshold — treat as directional only.'",
                "Intervention threshold filter (e.g. '>10% outlier rate' flag) — explicit list of resources meeting the threshold",
                "Cumulative production impact — SUM(ActualCycleSec - IdealCycleSec) over the outlier window expressed as units lost or hours lost",
                "Recommendations actionable to a shift supervisor",
            ],
        },
        # v3.6.0-2 — Real-time WIP Alert (in-process lots exceeding ideal)
        {
            "if_user_asks": "in-process lot / lots running long / WIP alert / late lots / lots over ideal / real-time alert / currently exceeding ideal cycle",
            "use_topic": "Real-time WIP Alert",
            "primary_view": "vw_InProcessLotAlert",
            "drill_view": "sp_RealtimeOutlierAlert",
            "key_columns": ["LotName", "ResourceName", "ElapsedSec", "IdealCycleSec", "PctOverIdeal", "Severity", "OperatorName", "EstRemainingSec"],
            "filter_pattern": (
                "Prefer the SP wrapper: EXEC Bottleneck_MCP.sp_RealtimeOutlierAlert @FacilityKey=X, @AreaKey=NULL, @ThresholdPct=100. "
                "0 rows means nothing currently late — that is a valid answer; report it as 'no in-process lots are currently exceeding ideal.'"
            ),
            "must_include_in_answer": [
                "Lot name + Resource name",
                "ElapsedSec vs IdealCycleSec + PctOverIdeal",
                "Severity ('Low' / 'Medium' / 'High')",
                "RecommendedAction column when calling sp_RealtimeOutlierAlert — surface verbatim",
                "If 0 rows: state 'no in-process lots are currently exceeding ideal' — do not say 'data missing'",
            ],
        },
        # v3.6.0-3 — Area OEE
        {
            "if_user_asks": "area OEE / area-grain availability / area-grain performance / area-grain quality / OEE for the Cleaning area / area OEE comparison / OEE breakdown / facility OEE / benchmark / 85% OEE",
            "use_topic": "Facility Performance / Executive View",
            "primary_view": "vw_AreaOEE",
            "drill_views": ["vw_FacilityPerformance_Daily", "vw_AreaWhatIf_ThroughputLift"],
            "key_columns": ["AreaName", "Day", "Avg_Availability_Pct", "Avg_Performance_Pct", "Avg_Quality_Pct", "Avg_OEE_Pct", "ResourceCount", "ProductiveHours", "IsEstimatedPerformance"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally AreaKey = Y) ORDER BY Day DESC, Avg_OEE_Pct DESC.",
            "must_include_in_answer": [
                "Overall facility OEE % from vw_FacilityPerformance_Daily.OEE_Pct",
                "OEE per area from vw_AreaOEE — markdown table",
                "Weakest area identification (MIN OEE)",
                "A/P/Q component breakdown for each area",
                "Detailed root cause for the weakest area's lowest component",
                "Industry benchmark comparison (85% reference)",
                "Gap to 85% — explicit math: '85 - <current OEE>' = X percentage points",
                "Projected production increase if all areas reached 85% OEE — invoke vw_AreaWhatIf_ThroughputLift to compute",
                "Improvement priorities ranked (Availability vs Performance vs Quality)",
                "IsEstimatedPerformance flag — if 1, disclose 'Performance back-filled from a historical baseline'",
            ],
        },
        # v3.6.0-4 — Area Capacity & What-If
        {
            "if_user_asks": "area capacity / capacity utilisation / capacity utilization / area PPH / spare area capacity / what-if de-bottleneck / projected throughput lift / compare areas / which area is the constraint",
            "use_topic": "Area Capacity & What-If",
            "primary_view": "vw_AreaCapacityAnalysis",
            "drill_views": ["vw_AreaWhatIf_ThroughputLift", "vw_AreaOEE", "vw_AreaInvestmentROI", "vw_BottleneckRanked", "vw_ResourceWIP_Snapshot"],
            "key_columns": ["AreaName", "ShiftKey", "Day", "ResourceCount", "IdealPPH", "ActualPPH", "CapacityUtilizationPct"],
            "filter_pattern": "WHERE FacilityKey = X ORDER BY CapacityUtilizationPct ASC. Drill vw_AreaWhatIf_ThroughputLift for the de-bottleneck projection.",
            "must_include_in_answer": [
                "Per-area metrics from vw_AreaOEE: OEE %, A%/P%/Q% breakdown",
                "Per-area capacity from vw_AreaCapacityAnalysis: IdealPPH_AreaTotal, ActualPPH_AreaTotal, CapacityUtilizationPct",
                "Constraint area identification — area with lowest CapacityUtilizationPct",
                "Bottleneck resource(s) inside the constraint area from vw_BottleneckRanked filtered by AreaKey",
                "Scenario projection from vw_AreaWhatIf_ThroughputLift: '+10% throughput at constraint → projected facility delta'",
                "Investment ROI ranking from vw_AreaInvestmentROI — when used, OPEN with the disclosure: 'These figures use estimated defaults — Finance has not loaded actual values yet.'",
                "Flow balance: where WIP is piling up upstream of the constraint (vw_ResourceWIP_Snapshot)",
                "Recommendations for flow balancing",
            ],
            "do_not_omit": [
                "vw_AreaCapacityAnalysis must be invoked when user asks 'which area' or 'capacity'",
                "vw_AreaWhatIf_ThroughputLift must be invoked when user asks 'what if' or '+N% scenario'",
                "vw_AreaInvestmentROI must be invoked when user asks 'investment' or 'ROI' or 'priority'",
            ],
        },
        # v3.6.0-5 — Loss Pareto (clean — no IsEstimatedDefault)
        {
            "if_user_asks": "loss Pareto / loss categories / which loss biggest / 80/20 of losses / Pareto chart of losses",
            "use_topic": "Loss Pareto & Opportunity",
            "primary_view": "vw_LossPareto",
            "key_columns": ["AreaName", "LossCategory", "LostUnits", "PctOfTotal", "CumulativePct", "RankN"],
            "filter_pattern": "ORDER BY RankN. Pre-ranked — surface RankN, PctOfTotal, CumulativePct directly.",
            "must_include_in_answer": [
                "Area name",
                "LossCategory + LostUnits per category",
                "PctOfTotal + CumulativePct (for the 80/20 narrative)",
                "RankN for explicit ordering",
            ],
        },
        # v3.6.0-6 — Loss Opportunity (FLAGGED IsEstimatedDefault)
        {
            "if_user_asks": "loss opportunity / 50% reduction projection / Difficulty / ROI score / projected gain from cutting losses",
            "use_topic": "Loss Pareto & Opportunity",
            "primary_view": "vw_LossOpportunity",
            "key_columns": ["AreaName", "Current_LostUnits", "Projected_Gain_Units", "Projected_Gain_Value", "Difficulty", "ROIScore", "IsEstimatedDefault"],
            "filter_pattern": "WHERE FacilityKey = X (and optionally AreaKey = Y).",
            "must_include_in_answer": [
                "Area name",
                "Current_LostUnits + Projected_Gain_Units + Projected_Gain_Value",
                "Difficulty + ROIScore (heuristic)",
                "IsEstimatedDefault flag — when 1, the answer MUST OPEN with the disclosure 'These figures use estimated defaults — Finance has not loaded actual values yet.' This is non-negotiable and the LLM is explicitly forbidden from hiding this disclosure.",
            ],
        },
        # v3.6.0-7 — Investment ROI (FLAGGED IsEstimatedDefault)
        {
            "if_user_asks": "investment ROI per area / area ROI months / payback months per area / which area pays back fastest",
            "use_topic": "Investment ROI",
            "primary_view": "vw_AreaInvestmentROI",
            "key_columns": ["AreaName", "TotalLostUnits", "EstimatedLossValue", "EstimatedInvestment", "ROI_Months", "IsEstimatedDefault"],
            "filter_pattern": "WHERE FacilityKey = X ORDER BY ROI_Months ASC.",
            "must_include_in_answer": [
                "Area name",
                "TotalLostUnits + EstimatedLossValue + EstimatedInvestment",
                "ROI_Months (lower = faster payback)",
                "IsEstimatedDefault flag — when 1, the answer MUST OPEN with the disclosure 'These figures use estimated defaults — Finance has not loaded actual values yet.' This is non-negotiable and the LLM is explicitly forbidden from hiding this disclosure.",
            ],
        },
        # v3.6.0-8 — Financial Impact + Strategic Priority Plan (FLAGGED IsEstimatedDefault)
        {
            "if_user_asks": "financial impact / revenue / loss value / improvement potential / strategic priorities / top 3 priorities / strategic plan / executive priority recommendations",
            "use_topic": "Financial Impact Summary",
            "primary_view": "vw_FinancialImpactSummary",
            "drill_sp": "sp_StrategicPriorityPlan",
            "key_columns": ["FacilityName", "Day", "Revenue_Estimated", "LossValue_Estimated", "ImprovementPotential_Estimated", "IsEstimatedDefault"],
            "filter_pattern": (
                "vw_FinancialImpactSummary: WHERE FacilityKey = X ORDER BY Day DESC. "
                "For the strategic plan companion: EXEC Bottleneck_MCP.sp_StrategicPriorityPlan @FacilityKey=X, @PeriodDays=30 — "
                "returns RankN, PriorityName, Justification, RequiredHeadcount_Estimated, RequiredInvestment_Estimated_USD, "
                "ExpectedOutcome, Timeline_Months, IsEstimatedDefault."
            ),
            "must_include_in_answer": [
                "Facility name + Day",
                "Revenue_Estimated + LossValue_Estimated + ImprovementPotential_Estimated",
                "When sp_StrategicPriorityPlan is invoked: surface RankN, PriorityName, Justification, RequiredHeadcount_Estimated, RequiredInvestment_Estimated_USD, ExpectedOutcome, Timeline_Months",
                "IsEstimatedDefault flag — when 1, the answer MUST OPEN with the disclosure 'These figures use estimated defaults — Finance has not loaded actual values yet.' This is non-negotiable and the LLM is explicitly forbidden from hiding this disclosure.",
            ],
        },
        # ====================================================================
        # v3.6.1 — 3 NEW decision_tree entries
        # ====================================================================
        # v3.6.1-1 — Categorical Significance Tests
        {
            "if_user_asks": "chi-square / Fisher exact / contingency table / categorical significance / is the difference between resources/shifts/batches significant",
            "use_topic": "Categorical Significance Tests",
            "primary_view": "vw_CategoricalSignificanceTests",
            "key_columns": ["TestName", "RowCategory", "RowOutlierCount", "RowTotalCount", "OtherOutlierCount", "OtherTotalCount", "ChiSquareStat", "ChiSquare_PValue", "FisherExact_PValue", "SignificantAt95Pct", "IsLowSampleConfidence"],
            "filter_pattern": "WHERE TestName IN ('Resource_vs_Outlier','Shift_vs_Outlier','Batch_vs_Outlier') ORDER BY ChiSquare_PValue. Today 20/30 rows carry IsLowSampleConfidence=1 because the loaded window is 6 days.",
            "must_include_in_answer": [
                "Test name (Resource_vs_Outlier / Shift_vs_Outlier / Batch_vs_Outlier)",
                "Row category + 2x2 contingency cells (RowOutlierCount, RowTotalCount, OtherOutlierCount, OtherTotalCount)",
                "ChiSquare_PValue OR FisherExact_PValue (whichever is non-NULL)",
                "IsLowSampleConfidence flag — when 1, the answer MUST OPEN with: 'Sample size is below the chi-square validity threshold (expected count < 5 in at least one cell). The reported p-value is computed for completeness but should not be used for inference.' This is non-negotiable.",
            ],
        },
        # v3.6.1-2 — Lot Economic Impact (FLAGGED IsEstimatedDefault)
        {
            "if_user_asks": "lot economic impact / scrap value / which lot is most expensive to lose / economic risk per lot",
            "use_topic": "Lot Economic Impact",
            "primary_view": "vw_LotEconomicImpact",
            "key_columns": ["LotName", "ResourceName", "CurrentState", "LotQuantity", "UnitValue", "EstimatedScrapValue", "EstimatedCompletionValue", "EconomicRiskScore", "IsEstimatedDefault"],
            "filter_pattern": "ORDER BY EconomicRiskScore DESC (or filter by ResourceName / CurrentState). All 1,713 rows carry IsEstimatedDefault=1 today because Finance has not loaded MaterialPrice_Manual.",
            "must_include_in_answer": [
                "LotName, ResourceName, CurrentState",
                "LotQuantity, EstimatedScrapValue, EstimatedCompletionValue, EconomicRiskScore",
                "IsEstimatedDefault flag — when 1, the answer MUST OPEN with the disclosure 'These figures use estimated defaults — Finance has not loaded actual values yet.' This is non-negotiable and the LLM is explicitly forbidden from hiding this disclosure.",
            ],
        },
        # v3.6.1-3 — Shift ANOVA
        {
            "if_user_asks": "shift ANOVA / Day vs Night significance / are the Day and Night shifts significantly different / F-test across shifts",
            "use_topic": "Shift ANOVA",
            "primary_view": "vw_ShiftANOVA",
            "key_columns": ["MetricName", "DayMean", "NightMean", "DayN", "NightN", "FStatistic", "ANOVA_PValue", "SignificantAt95Pct", "IsLowSampleConfidence"],
            "filter_pattern": "Just SELECT * FROM Bottleneck_MCP.vw_ShiftANOVA — only 3 rows (one per metric: OEE_Pct, Throughput_PPH, ScrapRate_Pct). All 3 rows carry IsLowSampleConfidence=1 today (max N=22 per shift group on 6-day window).",
            "must_include_in_answer": [
                "MetricName + DayMean / NightMean + DayN / NightN",
                "FStatistic + ANOVA_PValue + SignificantAt95Pct",
                "IsLowSampleConfidence flag — when 1, the answer MUST OPEN with: 'F-test results below are computed but not statistically meaningful — the loaded window has fewer than 30 samples per shift group. Treat as directional only.' This is non-negotiable.",
            ],
        },
    ],
    "recommended_tool_sequence": [
        "Step 1 — get_instructions()                 — you are here. Sets the rules + business-language style guide.",
        "Step 2 — get_topic_guide()                  — call WITHOUT args first. Returns rich descriptions for ALL 31 topics; YOU pick the best match by reading 'when_to_use' / 'when_NOT_to_use'.",
        "Step 2a — If 2+ topics fit equally, OR a required filter is missing — ASK THE USER. Do not guess.",
        "Step 2b — get_response_format(prompt) — call AFTER picking the topic and BEFORE writing SQL. Returns the element checklist + layout hint + disclosure rules. Use the checklist to verify every Expected element is in your final answer; use the layout hint to structure sections.",
        "Step 3 — get_view_details(view_names=[...]) — pass ONE or MORE view/SP names; returns columns, sample rows. Use this BEFORE every query to avoid hallucinated column names.",
        "Step 4 — query(sql, scope='curated') or run_stored_procedure(...)  — execute against the live DB. NON-NEGOTIABLE if your answer contains any factual numbers about Entegris. (For raw fact/dim access, use query(scope='raw').)",
        "Step 5 — (optional)                         — follow-up tool calls for drill-down (root cause, history, compare).",
        "Step 6 — Translate INTO BUSINESS LANGUAGE   — turn rows into a plain-English answer for a supervisor/manager/exec. NO SQL, NO tool names, NO view names, NO ResourceKey integers, NO audit trail in the user-facing reply. If data is missing, say so plainly without exposing 'NULL' or column names.",
    ],
    "common_filters_to_extract_from_user_text": {
        "FacilityKey": "Facility name like '4266' → look up via list_parameter_values('facility')",
        "AreaKey":     "Area name like 'Cleaning' → look up via list_parameter_values('area')",
        "ShiftKey":    "Day/Night → look up via list_parameter_values('shift'); 'today' → resolve via CurrentShift / DateFrom rules below",
        "DateFrom/DateTo": (
            "SELECT EarliestDay, LatestDay, DaysCovered, LatestEvent FROM Bottleneck_MCP.vw_DataAvailability "
            "once per session. Anchor every relative date phrase ('yesterday', 'last week') against LatestDay. "
            "If the user's relative date falls outside [EarliestDay, LatestDay], openly tell them and use "
            "LatestDay as the most recent reference point."
        ),
        "CurrentShift": (
            "SELECT LatestShiftKey, LatestShiftName, LatestStartDateTime FROM Bottleneck_MCP.vw_LatestShift "
            "WHERE FacilityKey = @FacilityKey. Use the returned ShiftKey in your follow-up query."
        ),
        "TopN":        "User says 'top 5' → TopN=5; default 20",
        "ResourceKey": "Resource name like 'OV1110' → look up via lookup_resource_detail('OV1110')",
    },
    "do_not_do": [
        "Don't compute ECT/ET/Yield/MTBF/MTTR yourself — they're in the views. Just SELECT them.",
        "Don't run DROP / DELETE / INSERT / UPDATE / EXEC as raw SQL — query(scope='curated'/'raw') will reject these.",
        "Don't query dbo.Fact* or dbo.Dim* tables directly in scope='curated'; use Bottleneck_MCP.vw_* — query(scope='curated') enforces this. Switch to scope='raw' only when you genuinely need fact/dim raw access.",
        "Don't return more than 200 rows by default. If the user asks for 'all', say so but cap at 1000.",
        "Don't mix UTC and LC1 columns in one filter. Use LC1 (local) for shift attribution if available.",
    ],
    "disambiguation_quick_reference": {
        "Bottleneck vs Slowdown": "Bottleneck = slowest in flow; Slowdown = slower than own baseline.",
        "Queue vs WIP": "Queue = waiting before resource (CurrentState='Queued'); WIP = all material at resource.",
        "Cycle time (actual) vs ECT": "ECT = CycleTime × (1 + Downtime%/100). Use ECT for bottleneck detection.",
        "AvgCycleTimeSec vs EndToEndCycleTimeSec": "Avg = active processing only; End-to-End = queue+dispatch+process+post-process. Use End-to-End to match Entegris reports.",
        "vw_ResourceWIP_Main vs vw_ResourceWIP_Snapshot": "Main = aggregated per shift; Snapshot = point-in-time current state.",
        "vw_BottleneckRanked vs vw_BottleneckAlert": "Ranked = sorted by score; Alert = filtered to threshold breaches only.",
    },
    "complex_answer_structure": {
        "rule": "If the question has multiple parts ('show X AND explain Y AND identify Z'), structure the answer as numbered sections matching the parts of the question. One section per sub-question. No raw row dump.",
        "format": [
            "**1. Bottleneck identification:** Resource X was the bottleneck on N of M shifts (averaged ET = ...).",
            "**2. Trend analysis:** Daily trend went from … to …, peaking on …",
            "**3. Most-frequent bottleneck:** Resource Y appeared … times.",
            "**4. Recommendation:** Investigate … because …",
        ],
        "do_not": [
            "Dump 50 rows of SQL output verbatim.",
            "Skip a sub-question because it needs a second query — run the second query.",
        ],
    },

    "threshold_extraction_patterns": {
        "rule": "Convert natural-language thresholds in the user prompt into explicit SQL filter clauses. The LLM must read numbers + units out of prose.",
        "examples": {
            "queue > 10 lots":              "WHERE QueueLength > 10",
            "wait > 2 hours":               "WHERE AvgQueuedLag > 7200            -- seconds",
            "downtime > 20%":               "WHERE DownTimePct > 20",
            "utilization > 90%":            "WHERE UtilizationPct > 90",
            "MTBF < 50 OR MTTR > 2":        "WHERE (MTBFHours < 50 OR MTTRHours > 2)",
            "cycle time > 1.3 × ideal":     "WHERE AvgCycleTimeSec > 1.3 * IdealCycleTime",
            "ET dropped by 20% in 24h":     "JOIN current-day vs prior-day; compute (curr.ET - prev.ET) / prev.ET; filter <= -0.2",
            "downtime increased ≥10pp WoW": "JOIN week-current vs week-prior on ResourceKey; HAVING (curr.DownTimePct - prev.DownTimePct) >= 10",
            "≥3 occurrences in last month": "GROUP BY ResourceKey HAVING COUNT(*) >= 3",
            "3 consecutive shifts":         "Use LAG/LEAD window functions on (ResourceKey ORDER BY ShiftKey) to count consecutive runs; filter run-length >= 3",
        },
    },

    "multi_query_chaining": {
        "rule": "Some questions cannot be answered by ONE query. Run multiple queries and compose the answer. Each query is still a single tool call; the chaining lives in YOUR reasoning, not in SQL.",
        "patterns": [
            {
                "pattern": "Cascade / ripple impact",
                "single_call": "EXEC Bottleneck_MCP.sp_BottleneckCascade @FacilityKey = X, @ShiftKey = Y. Returns 3 result sets — Bottleneck / UpstreamBackup / DownstreamStarvation.",
            },
            {
                "pattern": "Production shortfall",
                "steps": [
                    "Query 1 — vw_ShiftTarget for current shift.",
                    "Query 2 — vw_BottleneckRanked for current shift.",
                    "Compute: shortfall_units = TargetUnits - ActualSoFar; attribute to bottleneck if its IsBottleneck = 1.",
                ],
            },
            {
                "pattern": "Capacity expansion / what-if",
                "steps": [
                    "Query 1 — current bottleneck performance (Topic 1).",
                    "Query 2 — vw_ParallelResourceCapacity for that resource (Topic 12).",
                    "Compute: projected_throughput_with_added_machine = current_ET × (1 + AlternateETSpare/current_ET).",
                ],
            },
            {
                "pattern": "WoW / MoM trend",
                "steps": [
                    "Query 1 — vw_BottleneckHistory for current period.",
                    "Query 2 — same view for prior period (same length, shifted back).",
                    "Compute deltas in reasoning; present as side-by-side table with arrows.",
                ],
            },
        ],
    },

    "predictive_math_hints": {
        "rule": "When the user asks for projections / forecasts / 'what-if', prefer pre-computed columns; only do arithmetic for the formulae that are not yet in the data layer.",
        "formulae": {
            "Production shortfall (current shift)":
                "Use Shortfall_Units / Shortfall_Pct columns on vw_ShiftTarget directly. Do not subtract by hand.",
            "Projected end-of-shift output":
                "ProjectedToEndOfShift is pre-computed on vw_ShiftTarget. Use it directly.",
            "Time to queue overflow":
                "Use HoursToOverflow on vw_QueueGrowthRate directly. Disclose 'estimate' if IsCapacityHeuristic = 1.",
            "Lost production over a window":
                "lost_units = SUM((IdealPPH - ActualPPH) * ShiftHours) across shifts where IsBottleneck = 1",
            "WoW % change":
                "delta_pct = 100 * (current_value - prior_value) / NULLIF(prior_value, 0)",
            "Operator efficiency (one operator on one resource)":
                "100 * SUM(IdealCycleTime * ParcelOutPrimaryQuantity) / NULLIF(SUM(DATEDIFF(SECOND, UTCTrackInDateTime, UTCTrackOutDateTime)), 0)",
        },
        "do_not": [
            "Show the formula in the user-facing answer — show the result.",
            "Quote 'projected' numbers without sourcing them from a query this turn.",
        ],
    },

    "example_full_workflow": {
        "user_question": "Are there any lots that have been waiting more than 24 hours?",
        "internal_steps": "get_instructions → get_topic_guide (Drill-down / What's Stuck) → get_view_details → query(scope='curated') (don't expose any of these in the answer).",
        "USER_FACING_REPLY_EXAMPLE": "Yes — 20 lots have been waiting more than 24 hours. The longest-stuck lot has been queued for over 12 days at the Drying Oven (OV1110). Most of the affected lots are on the S4442R031Y23 product line. Recommend engineering review for hold-release on the oldest lots, and a maintenance check on OV1110.",
        "rule": "The user only sees the USER_FACING_REPLY — never the internal trace.",
    },
    "version": "3.7.2",
    "schema_in_db": "Bottleneck_MCP (102 views + 7 SPs + 4 seed tables in EntegrisKSPUpgradeDWH)",
    "tools_available": [
        "get_instructions   — call FIRST for any new question",
        "get_topic_guide    — keyword → view routing (31 topics)",
        "get_response_format    — element checklist + layout guidance per prompt (call before final answer)",
        "query              — consolidated SELECT tool (scope='curated' for Bottleneck_MCP.vw_*; scope='raw' for any schema). Prefer this over query_view/run_query going forward.",
        "query_view         — [DEPRECATED v3.8 — use query(scope='curated')] safe SELECT against Bottleneck_MCP.vw_*",
        "get_view_details      — columns + sample rows + related topics for ONE OR MORE views/SPs in a single call",
        "run_stored_procedure — whitelisted SPs only (6 available, includes sp_BottleneckCascade, sp_ResolveResource, sp_StrategicPriorityPlan)",
        "list_parameter_values — filter dropdown values",
        "run_query          — [DEPRECATED v3.8 — use query(scope='raw')] fallback raw SQL (any schema)",
    ],
}


@mcp.tool()
def get_instructions() -> dict:
    """Returns the AI assistant's playbook for using this MCP server.

    CALL THIS FIRST whenever you receive a new user question. It explains:
      - the 6 core principles (discover before query, math in views, etc.)
      - a decision tree mapping question patterns to topics + views
      - the recommended tool-call sequence (get_instructions → topic_guide → get_view_details → query(scope='curated'))
      - common filters to extract from user text (FacilityKey, ShiftKey, etc.)
      - do's and don'ts
      - disambiguation quick-reference
      - a full worked example

    No parameters. Returns the playbook as a structured dict.
    """
    logger.info("Tool 'get_instructions' called")
    return INSTRUCTIONS


# ============================================================================
# TOOL 1: GET TOPIC GUIDE  (LLM-decides mode)
#
# Default behaviour: return ALL 31 topics with rich descriptions so the LLM
# can read them and pick the best fit itself (no opaque keyword scoring).
# Pass a topic name/number/keyword to fetch a single topic in detail.
# ============================================================================
@mcp.tool()
def get_topic_guide(topic: str = "") -> dict:
    """Returns rich descriptions for 31 bottleneck-analysis topics so the LLM can decide which one(s) fit the user's question.

    USAGE MODES:
      - get_topic_guide()             → returns ALL 31 topics with full descriptions (recommended; the LLM reads them and picks)
      - get_topic_guide("3")          → returns topic #3 only (Cycle Time)
      - get_topic_guide("rework")     → returns the single best keyword match (legacy fuzzy mode)
      - get_topic_guide("bottleneck") → same as above

    Each topic entry includes:
      - name, description, when_to_use, when_NOT_to_use
      - summary_view (with column list, what each column means)
      - detail_views (with use_when notes)
      - stored_procedure (if any)
      - example_questions, example_queries (real SQL with FacilityKey=2)
      - related_topics (if user might also mean one of these)

    The LLM should:
      1. Call this tool with no args (or 'all') for a fresh question.
      2. Read all 31 topic blurbs.
      3. Select the best match (or 2 candidates if ambiguous).
      4. If 2 topics tie OR a required filter is missing, ASK THE USER for clarification.

    Args:
        topic: optional. Empty/'all' to list all topics. Number 1–31 to fetch one. Keyword to fuzzy-match one.
    """
    logger.info(f"Tool 'get_topic_guide' called with topic='{topic}'")

    def _enrich(num: int, entry: dict) -> dict:
        """Build a rich entry the LLM can reason about."""
        return {
            "topic_number":      num,
            "name":              entry["name"],
            "description":       entry.get("description", ""),
            "when_to_use":       entry.get("when_to_use", entry.get("description", "")),
            "when_NOT_to_use":   entry.get("when_NOT_to_use", ""),
            "primary_view":      entry.get("summary_view", entry.get("primary_view", "")),
            "detail_views":      entry.get("detail_views", []),
            "stored_procedure":  entry.get("stored_procedure"),
            "example_questions": entry.get("example_questions", []),
            "example_queries":   entry.get("example_queries", []),
            "keywords":          entry.get("keywords", []),
            "related_topics":    entry.get("related_topics", []),
        }

    def _relevant_rules(text: str) -> dict:
        t = (text or "").lower()
        triggers = {
            "bottleneck_vs_slowdown":      ["bottleneck", "slowdown", "constraint", "ranked", "alert"],
            "queue_vs_wip":                ["queue", "wip", "waiting", "stuck", "queue depth"],
            "cycle_time_vs_ECT":           ["cycle time", "ect", "et", "throughput", "actual vs ideal"],
            "current_state_values":        ["state", "queued", "dispatched", "inprocess", "current state"],
            "semi_e10_states":             ["downtime", "utilization", "uptime", "semi e10", "productive", "standby"],
            "shift_summary_vs_comparison": ["shift", "comparison", "compare", "today vs yesterday"],
            "ranked_vs_alert":             ["ranked", "alert", "top n", "threshold"],
            "root_cause_dimensions":       ["root cause", "why", "equipment", "material", "operator"],
            "facility_filter_required":    ["facility", "where is", "filter", "all resources"],
        }
        return {k: DISAMBIGUATION_RULES[k] for k, trigs in triggers.items()
                if any(g in t for g in trigs)}

    topic_str = (topic or "").strip().lower()

    # ── MODE 1: list ALL topics so the LLM can decide ─────────────────────
    if topic_str in ("", "all", "list", "*"):
        all_topics = [_enrich(num, entry) for num, entry in TOPIC_REGISTRY.items()]
        return {
            "mode": "all_topics_for_llm_decision",
            "instruction_to_llm": (
                "READ each topic's name + description + when_to_use + when_NOT_to_use. "
                "Pick the topic whose 'when_to_use' best matches the user's question. "
                "If two topics fit equally (ambiguity), ASK THE USER a brief clarifying question "
                "before running any query — see clarification_policy in get_instructions(). "
                "If you need column-level detail, call get_view_details(<view_name>) before composing SQL."
            ),
            "total_topics": len(all_topics),
            "topics": all_topics,
            "disambiguation_rules": DISAMBIGUATION_RULES,
            "next_step": "Pick a topic → call get_view_details on its primary_view → compose dynamic SELECT → call query(scope='curated').",
        }

    # ── MODE 2: numeric lookup ────────────────────────────────────────────
    try:
        n = int(topic_str)
        if n in TOPIC_REGISTRY:
            entry = TOPIC_REGISTRY[n]
            return {
                "mode": "single_topic_by_number",
                **_enrich(n, entry),
                "disambiguation_rules": _relevant_rules(entry["name"] + " " + " ".join(entry.get("keywords", []))),
            }
    except ValueError:
        pass

    # ── MODE 2.5 (v3.7.0 A2): EXTRA_ROUTINGS shortcut ─────────────────────
    routing_name, routing_cfg = _match_extra_routing(topic_str)
    if routing_cfg is not None:
        return {
            "mode": "extra_routing",
            "routing_name": routing_name,
            "primary_views": routing_cfg.get("primary_views", []),
            "primary_sps": routing_cfg.get("primary_sps", []),
            "param_hint": routing_cfg.get("param_hint", ""),
            "must_include_in_answer": routing_cfg.get("must_include_in_answer", []),
            "tip_to_llm": (
                "This is a direct keyword routing — pull from primary_views/primary_sps "
                "BEFORE falling back to topic registry. Honour the must_include_in_answer "
                "checklist verbatim in your final answer."
            ),
        }

    # ── MODE 3: keyword fuzzy match (legacy) ──────────────────────────────
    best, best_score = None, 0
    for n, entry in TOPIC_REGISTRY.items():
        s = 0
        if topic_str == entry["name"].lower():        s = 100
        elif topic_str in entry["name"].lower():      s = 80
        else:
            for kw in entry.get("keywords", []):
                if kw in topic_str or topic_str in kw:
                    s = max(s, 60)
        if s > best_score:
            best, best_score = (n, entry), s

    if best and best_score >= 60:
        n, entry = best
        return {
            "mode": "single_topic_by_keyword",
            "match_score": best_score,
            **_enrich(n, entry),
            "disambiguation_rules": _relevant_rules(entry["name"] + " " + " ".join(entry.get("keywords", [])) + " " + topic_str),
            "tip_to_llm": (
                "If this match feels off, call get_topic_guide() with no args to see ALL 12 topics "
                "and pick yourself. Or ASK THE USER to clarify their question."
            ),
        }

    return {
        "error": f"No topic match for '{topic}'. Call get_topic_guide() with no args to see all 12 topics.",
        "available_topics": [{"number": n, "name": e["name"]} for n, e in TOPIC_REGISTRY.items()],
    }


# ============================================================================
# TOOL 1b (v3.5.0): RESPONSE FORMAT — element checklist + layout guidance
# ============================================================================
@mcp.tool()
def get_response_format(prompt: str = "", topic: str = "") -> dict:
    """Return response-format guidance for a user prompt.

    Reads from the existing INSTRUCTIONS.decision_tree + data_recovery_rules —
    no duplicated state. CALL THIS BEFORE COMPOSING THE FINAL ANSWER.

    The returned checklist tells you which atomic elements your final answer
    must contain. The layout hint tells you how to structure them. The
    disclosure rules give you boilerplate for any data-ceiling items.

    Args:
        prompt: the user's original question (used for keyword routing)
        topic:  optional explicit topic name (e.g. 'Current Bottleneck Detection')

    Returns:
        {
            "matched_topic": str,
            "matched_keywords": str,
            "required_elements": [str, ...],     # pulled from decision_tree.must_include_in_answer
            "element_count": int,
            "layout_hint": str,                  # 'numbered_sections' | 'table' | 'narrative'
            "checklist_instruction": str,        # actionable guidance based on element_count
            "disclosure_rules": [str, ...],      # data_recovery_rules + ceiling fallbacks
        }
    """
    logger.info(f"Tool 'get_response_format' called for: prompt='{prompt[:60]}...' topic='{topic}'")

    text = (prompt + " " + topic).lower()
    best = None
    for entry in INSTRUCTIONS.get("decision_tree", []):
        keywords = entry.get("if_user_asks", "").lower()
        score = sum(1 for word in keywords.split() if len(word) > 3 and word in text)
        if score and (best is None or score > best[0]):
            best = (score, entry)

    entry = best[1] if best else {}
    elements = entry.get("must_include_in_answer", []) or []

    n = len(elements)
    if n >= 4:
        layout = "numbered_sections"
        instruction = (
            f"User asked for {n} distinct items. Structure your answer as "
            f"'## 1. <Item Name>\\n<concrete value>\\n\\n## 2. <Item Name>\\n…' — "
            f"one markdown section per item below. Even if an item is data-ceiling, "
            f"write its section with the disclosed zero/null value (e.g. "
            f"'MTBF: undefined — 0 Down events in N service intervals'). "
            f"Do NOT collapse, do NOT skip."
        )
    elif n >= 2:
        layout = "table"
        instruction = (
            f"User asked for {n} items. Use a markdown table with one row per item: "
            f"| Metric | Value |. Concrete data + units in the value column. "
            f"Disclose any zero/NULL/empty result as a fact, not a refusal."
        )
    elif n == 1:
        layout = "narrative"
        instruction = (
            "Single-element answer — give the direct value with units, then one "
            "short context paragraph. No padding."
        )
    else:
        layout = "narrative"
        instruction = (
            "No specific element checklist matched. Give a direct concrete answer "
            "to the user's question; disclose any data-ceiling values as facts."
        )

    # v3.7.0 A3 — always-disclose templates (financial, comparison, root-cause)
    disclosure_rules = list(INSTRUCTIONS.get("data_recovery_rules", []))

    FINANCIAL_TOPICS = {"revenue", "cost", "roi", "investment", "loss",
                        "target", "shortfall", "value", "$", "dollar"}
    p_lower = (prompt or "").lower()

    if any(t in p_lower for t in FINANCIAL_TOPICS):
        disclosure_rules.append(
            "FINANCIAL DISCLOSURE TEMPLATE — when FactTargets is empty OR "
            "MaterialPrice_Manual is unloaded, end the relevant section with: "
            "'Revenue/target impact not currently computable — FactTargets has 0 rows "
            "and MaterialPrice_Manual is unloaded; surfacing actual production only.' "
            "Never silently omit the financial line."
        )

    if "compare" in p_lower or " vs " in p_lower or "versus" in p_lower:
        disclosure_rules.append(
            "STATISTICAL TEST REQUIRED — for any comparison answer include a "
            "significance section sourced from vw_CategoricalSignificanceTests or "
            "vw_ShiftANOVA. If N<30, mark IsLowSampleConfidence=1 and disclose."
        )

    if any(k in p_lower for k in ("root cause", "investigate", "why")):
        disclosure_rules.append(
            "COUNTERFACTUAL REQUIRED — close root-cause answers with one line of "
            "'what would change if X were addressed' sourced from vw_WhatIf_AddResource "
            "or vw_AreaWhatIf_ThroughputLift when relevant."
        )

    return {
        "matched_topic": entry.get("use_topic", "Generic"),
        "matched_keywords": entry.get("if_user_asks", ""),
        "required_elements": elements,
        "element_count": n,
        "layout_hint": layout,
        "checklist_instruction": instruction,
        "disclosure_rules": disclosure_rules,
    }


# ============================================================================
# TOOL 2: QUERY (consolidated v3.8) — single tool with curated/raw scopes
# ============================================================================
@mcp.tool()
def query(sql: str, scope: str = "curated", limit: int = 200, database: str = None) -> dict:
    """Execute a read-only T-SQL SELECT query.

    SCOPE MODES:
      - scope='curated' (default): restricted to Bottleneck_MCP.vw_* views.
                                    Safest path. Pre-joined, business-friendly.
      - scope='raw':                allows any schema including dbo.Fact*, dbo.Dim*,
                                    INFORMATION_SCHEMA, sys.*. Use only for exploratory
                                    queries the curated views don't cover.

    For routine bottleneck/OEE/cycle-time/facility questions, leave scope='curated'.
    Switch to scope='raw' only when you genuinely need fact/dim raw access — and
    document why in your reasoning.

    Args:
        sql: T-SQL SELECT query (or WITH ... SELECT).
        scope: 'curated' (Bottleneck_MCP.vw_* only) or 'raw' (any schema).
        limit: Max rows to return (default 200, max 1000).
        database: Override default database. Defaults to DB_NAME.
                  Only used with scope='raw' for cross-DB queries.
    """
    logger.info(f"Tool 'query' called scope={scope}: {sql}")

    if scope not in ("curated", "raw"):
        return {"error": f"Invalid scope '{scope}'. Use 'curated' or 'raw'."}

    sql_stripped = (sql or "").strip()
    sql_upper = sql_stripped.upper()

    # 1) SELECT/WITH only
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return {"error": "Only SELECT (or WITH ... SELECT) queries are permitted."}

    # 2) Block destructive / DDL / DML / EXEC keywords
    blocked = ["DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE", "MERGE", "EXEC", "EXECUTE"]
    padded = f" {sql_upper} "
    for kw in blocked:
        if f" {kw} " in padded or sql_upper.startswith(kw):
            return {"error": f"'{kw}' statements are not allowed. Only SELECT queries are permitted."}

    sql_check = sql.lower()

    # 3) Curated scope guardrails
    if scope == "curated":
        if "bottleneck_mcp.vw_" not in sql_check and "[bottleneck_mcp].[vw_" not in sql_check:
            return {"error": "scope='curated' requires referencing Bottleneck_MCP.vw_* views. Use get_topic_guide to find the right view, or call query(scope='raw') for ad-hoc dbo.* access."}
        if "dbo.fact" in sql_check or "dbo.dim" in sql_check or "[dbo].[fact" in sql_check or "[dbo].[dim" in sql_check:
            return {"error": "Direct access to dbo.Fact*/dbo.Dim* is not allowed in scope='curated'. Use the curated Bottleneck_MCP.vw_* views, or call query(scope='raw') for genuinely exploratory access."}

    # 4) Execute — bound by ROW_LIMIT_MAX cap
    limit = min(max(limit, 1), ROW_LIMIT_MAX)
    db = database or DB_NAME

    if scope == "curated":
        # curated path uses the default DB connection helper
        result = _execute_read_query(sql, limit=limit)
        result["scope"] = "curated"
        result["database"] = db
        return result

    # raw scope: allow database override
    try:
        conn = pymssql.connect(
            server=DB_SERVER,
            port=DB_PORT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=db,
            login_timeout=15,
            timeout=60,
        )
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.description is None:
            cursor.close()
            conn.close()
            return {"columns": [], "rows": [], "row_count": 0, "truncated": False, "scope": "raw", "database": db}

        columns = [col[0] for col in cursor.description]
        rows = []
        truncated = False
        for i, row in enumerate(cursor):
            if i >= limit:
                truncated = True
                break
            rows.append([_coerce_value(v) for v in row])

        cursor.close()
        conn.close()
        return {"columns": columns, "rows": rows, "row_count": len(rows), "truncated": truncated, "scope": "raw", "database": db}
    except pymssql.Error as e:
        logger.error(f"Database error: {e}")
        return {"error": str(e), "scope": "raw", "database": db}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"error": str(e), "scope": "raw", "database": db}


# ============================================================================
# TOOL 2 (legacy): query_view — DEPRECATED v3.8 wrapper around query(scope='curated')
# ============================================================================
@mcp.tool()
def query_view(sql: str, limit: int = 200) -> dict:
    """[DEPRECATED v3.8 — use query(scope='curated')] Restricted to Bottleneck_MCP.vw_*."""
    logger.warning("query_view is deprecated; routing to query(scope='curated')")
    return query(sql=sql, scope="curated", limit=limit)


# ============================================================================
# TOOL 3: GET VIEW DETAILS  (replaces the old get_view_details)
#
# Multi-object schema introspection. Pass a list of view/SP names and get
# columns + sample rows + description + related topics + parameters (for SPs)
# in ONE round trip. Saves multiple tool calls when joining or comparing views.
# ============================================================================
def _normalise_object_name(name: str) -> str:
    """Strip schema prefix and brackets to canonical name."""
    n = (name or "").strip()
    for pfx in ("bottleneck_mcp.", "Bottleneck_MCP.", "[Bottleneck_MCP].", "[bottleneck_mcp]."):
        if n.lower().startswith(pfx.lower()):
            n = n[len(pfx):]
            break
    return n.strip("[]").strip()


def _topics_referencing(object_name: str) -> list:
    """Return list of TOPIC_REGISTRY entries that mention this view/SP."""
    hits = []
    for num, entry in TOPIC_REGISTRY.items():
        sv = entry.get("summary_view")
        if sv and sv.get("name") == object_name:
            hits.append({"topic_number": num, "topic_name": entry["name"], "tier": "summary"})
        for dv in entry.get("detail_views", []) or []:
            if isinstance(dv, dict) and dv.get("name") == object_name:
                hits.append({"topic_number": num, "topic_name": entry["name"], "tier": "detail"})
        sp = entry.get("stored_procedure")
        if sp and sp.get("name") == object_name:
            hits.append({"topic_number": num, "topic_name": entry["name"], "tier": "stored_procedure"})
    return hits


def _list_all_objects() -> list:
    """Returns every view + SP name in the Bottleneck_MCP schema."""
    sql = """
        SELECT TABLE_NAME, 'VIEW' AS object_type
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = 'Bottleneck_MCP'
        UNION ALL
        SELECT ROUTINE_NAME, 'PROCEDURE'
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA = 'Bottleneck_MCP' AND ROUTINE_TYPE = 'PROCEDURE'
        ORDER BY 1
    """
    res = _execute_read_query(sql, limit=200)
    return [{"name": r[0], "type": r[1]} for r in res.get("rows", [])]


def _details_for_one(object_name: str) -> dict:
    """Fetch full details for a single view or SP."""
    vn = _normalise_object_name(object_name)
    if not vn:
        return {"error": "object_name is required"}

    # Determine type — view or procedure?
    type_sql = """
        SELECT 'VIEW' AS t FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = 'Bottleneck_MCP' AND TABLE_NAME = %s
        UNION ALL
        SELECT 'PROCEDURE' FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA = 'Bottleneck_MCP' AND ROUTINE_NAME = %s AND ROUTINE_TYPE = 'PROCEDURE'
    """
    tres = _execute_read_query(type_sql, (vn, vn), limit=2)
    if not tres.get("rows"):
        return {
            "object_name": vn,
            "error": f"'{vn}' not found in Bottleneck_MCP schema.",
            "hint": "Call get_view_details() with no args to list all 72 objects.",
        }
    obj_type = tres["rows"][0][0]

    out = {
        "object_name": vn,
        "schema": "Bottleneck_MCP",
        "object_type": obj_type,
    }

    # ── columns (for views) OR parameters (for SPs) ─────────────────────
    if obj_type == "VIEW":
        col_sql = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Bottleneck_MCP' AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        cols = _execute_read_query(col_sql, (vn,), limit=500)
        out["columns"] = [
            {"name": r[0], "data_type": r[1], "nullable": r[2],
             "max_length": r[3] if r[3] and r[3] > 0 else None}
            for r in cols.get("rows", [])
        ]
        out["column_count"] = len(out["columns"])

        # Sample rows (3)
        try:
            sample = _execute_read_query(
                f"SELECT TOP 3 * FROM [Bottleneck_MCP].[{vn}]",
                limit=3,
            )
            out["sample_rows"] = {
                "columns": sample.get("columns", []),
                "rows":    sample.get("rows", []),
                "error":   sample.get("error"),
            }
        except Exception as e:
            out["sample_rows"] = {"error": str(e)[:200]}

    else:  # PROCEDURE
        param_sql = """
            SELECT PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE,
                   CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.PARAMETERS
            WHERE SPECIFIC_SCHEMA = 'Bottleneck_MCP' AND SPECIFIC_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        prms = _execute_read_query(param_sql, (vn,), limit=200)
        out["parameters"] = [
            {
                "name": (r[0] or "").lstrip("@"),
                "data_type": r[1],
                "mode": r[2],
                "max_length": r[3] if r[3] and r[3] > 0 else None,
                "position": r[4],
            }
            for r in prms.get("rows", [])
        ]
        out["parameter_count"] = len(out["parameters"])

        # Look up SP semantics from WHITELISTED_SPS if present
        if vn in WHITELISTED_SPS:
            out["sp_metadata"] = WHITELISTED_SPS[vn]

    # ── related topics — which TOPIC_REGISTRY entries reference this object
    out["related_topics"] = _topics_referencing(vn)

    # ── enrich with vw_Dictionary description if present
    try:
        desc = _execute_read_query(
            "SELECT Description, SourceTables, RowCountBudget, ExampleQuery "
            "FROM Bottleneck_MCP.vw_Dictionary WHERE ObjectName = %s",
            (vn,), limit=1,
        )
        if desc.get("rows"):
            r = desc["rows"][0]
            out["description"] = r[0]
            out["source_tables"] = r[1]
            out["row_count_budget"] = r[2]
            out["example_query"] = r[3]
    except Exception:
        pass

    return out


@mcp.tool()
def get_view_details(view_names: list = None) -> dict:
    """One-stop schema introspection for views and stored procedures in Bottleneck_MCP.

    Replaces the older `get_view_details`. Accepts a LIST of names and returns the full
    schema picture for EACH in a single round trip. Use this BEFORE composing any SQL
    to avoid hallucinated column names.

    USAGE MODES:
      get_view_details()                                    → returns the catalogue: every view + SP name (37 objects)
      get_view_details(view_names=[])                       → same as above
      get_view_details(view_names=["vw_BottleneckRanked"])  → full details for one object
      get_view_details(view_names=["vw_A", "vw_B"])         → full details for multiple in ONE call (saves round trips for joins / comparisons)

    For each requested object the response includes:
      - object_name, object_type (VIEW | PROCEDURE), schema
      - For VIEWs:   columns (name + data_type + nullable + max_length) + 3 sample rows
      - For SPs:     parameters (name + data_type + mode + position) + sp_metadata
      - description (from vw_Dictionary)
      - source_tables, row_count_budget, example_query (from vw_Dictionary)
      - related_topics (TOPIC_REGISTRY entries that reference this object)

    The LLM should:
      1. Always call this BEFORE query(scope='curated') to verify columns.
      2. When composing a JOIN or comparing two views, pass BOTH names in one call.

    Args:
        view_names: list of view or SP names. Empty/None returns the catalogue.
    """
    logger.info(f"Tool 'get_view_details' called with view_names={view_names}")

    # Normalise input
    if view_names is None:
        view_names = []
    if isinstance(view_names, str):
        # tolerate single string
        view_names = [view_names] if view_names.strip().lower() not in ("", "all") else []
    view_names = [v for v in view_names if v and v.strip().lower() not in ("all",)]

    # ── Catalogue mode ────────────────────────────────────────────────────
    if not view_names:
        objects = _list_all_objects()
        return {
            "mode": "catalogue",
            "instruction_to_llm": (
                "This is the full list of objects in Bottleneck_MCP. To get column-level "
                "detail for one or more, call get_view_details(view_names=[...]). For "
                "JOINs or comparisons, pass ALL involved names in a single call."
            ),
            "object_count": len(objects),
            "objects": objects,
        }

    # ── Detail mode ───────────────────────────────────────────────────────
    results = []
    for vn in view_names:
        results.append(_details_for_one(vn))

    return {
        "mode": "detail",
        "object_count": len(results),
        "objects": results,
    }


# ============================================================================
# TOOL 4: RUN STORED PROCEDURE
# ============================================================================
@mcp.tool()
def run_stored_procedure(procedure: str, params: dict) -> dict:
    """Execute one of 2 whitelisted Bottleneck_MCP stored procedures with typed parameters.

    Available procedures:

    - sp_BottleneckAnalysis: Single-call full bottleneck report. Returns 3 result sets
      (ShiftSummary, ResourceDetail, TimeSliceHistory).
      Required params: FacilityKey (int), DateFrom (str 'YYYY-MM-DD'), DateTo (str 'YYYY-MM-DD').
      Optional: AreaKey (int), ShiftKey (int), TopN (int, default 20),
                IncludeTimeSlice (int 0/1, default 0), SliceIntervalMinutes (int, default 30).

    - sp_ResourceDetailLookup: Fuzzy resource-name search. Returns a single result set.
      Required params: SearchTerm (str).
      Optional: FacilityKey (int).

    Args:
        procedure: One of 'sp_BottleneckAnalysis' or 'sp_ResourceDetailLookup'.
        params: Key-value parameter pairs matching the procedure signature.
    """
    logger.info(f"Tool 'run_stored_procedure' called: {procedure} with {params}")

    if procedure not in WHITELISTED_SPS:
        return {"error": f"Unknown procedure '{procedure}'. Must be one of: {list(WHITELISTED_SPS.keys())}"}

    sp_def = WHITELISTED_SPS[procedure]
    params = params or {}

    # Validate required params
    for pname, pdef in sp_def["params"].items():
        if pdef["required"] and (pname not in params or params[pname] in (None, "")):
            return {"error": f"Missing required parameter '{pname}' ({pdef['doc']}) for {procedure}"}

    # Build EXEC string with bound parameters (pymssql %s placeholders)
    placeholder_parts = []
    bound_values = []
    for pname, pdef in sp_def["params"].items():
        if pname in params and params[pname] is not None:
            val = params[pname]
            if pdef["type"] == "int":
                try:
                    int_val = int(val)
                except (ValueError, TypeError):
                    return {"error": f"Parameter '{pname}' must be an integer (got: {val!r})"}
                placeholder_parts.append(f"@{pname}=%s")
                bound_values.append(int_val)
            else:
                placeholder_parts.append(f"@{pname}=%s")
                bound_values.append(str(val))

    exec_sql = f"EXEC [Bottleneck_MCP].[{procedure}] {', '.join(placeholder_parts)}"
    logger.info(f"Executing SP: {exec_sql} with {len(bound_values)} bound params")

    multi_result = _execute_multi_resultset(exec_sql, params=tuple(bound_values), limit=ROW_LIMIT_MAX)

    if "error" in multi_result and multi_result.get("set_count", 0) == 0:
        return multi_result

    # If exactly one result set, flatten for convenience (mirror AIMES pattern)
    if multi_result.get("set_count") == 1:
        rs = multi_result["result_sets"][0]
        return {
            "procedure": procedure,
            "params_used": params,
            "columns": rs["columns"],
            "rows": rs["rows"],
            "row_count": rs["row_count"],
            "truncated": rs.get("truncated", False),
        }

    # Multiple result sets - return them all with names where known
    set_names = []
    if procedure == "sp_BottleneckAnalysis":
        set_names = ["ShiftSummary", "ResourceDetail", "TimeSliceHistory"]
    elif procedure == "sp_BottleneckCascade":
        set_names = ["Bottleneck", "UpstreamBackup", "DownstreamStarvation", "CascadeTimeline"]
    elif procedure == "sp_FacilityExecutiveSummary":
        set_names = ["HeadlineKPIs", "DailyTrend", "TopBottlenecks", "TopAnomalies"]
    named = []
    for i, rs in enumerate(multi_result.get("result_sets", [])):
        named.append({
            "name": set_names[i] if i < len(set_names) else f"ResultSet_{i+1}",
            "columns": rs["columns"],
            "rows": rs["rows"],
            "row_count": rs["row_count"],
            "truncated": rs.get("truncated", False),
        })

    return {
        "procedure": procedure,
        "params_used": params,
        "result_sets": named,
        "set_count": multi_result.get("set_count", len(named)),
        "total_rows": multi_result.get("total_rows", sum(r["row_count"] for r in named)),
    }


# ============================================================================
# TOOL 5: LIST PARAMETER VALUES
# ============================================================================
PARAMETER_MAP = {
    "facility":         {"view": "vw_Facilities",  "columns": "FacilityKey, FacilityName, FacilityType"},
    "area":             {"view": "vw_Areas",       "columns": "AreaKey, AreaName, FacilityKey, FacilityName"},
    "resource":         {"view": "vw_Resources",   "columns": "ResourceKey, ResourceName, ResourceType, Model, AreaKey"},
    "shift":            {"view": "vw_Shifts",      "columns": "ShiftKey, ShiftName, StartDateTime, EndDateTime, AreaKey"},
    "product":          {"view": "vw_Products",    "columns": "ProductKey, ProductName, ProductGroup"},
    "step":             {"query": "SELECT DISTINCT StepKey, StepName FROM [Bottleneck_MCP].[vw_ResourceWIP_Main] WHERE StepKey IS NOT NULL ORDER BY StepName"},
    "semi_e10_state":   {"static": [
        ["Productive",       "Equipment is processing material - good time"],
        ["Standby",          "Equipment is up but idle (no material) - utilization loss"],
        ["Engineering",      "Equipment is up but reserved for engineering tests"],
        ["Scheduled Down",   "Planned downtime (PM, calibration)"],
        ["Unscheduled Down", "Unplanned downtime (failure, breakdown)"],
        ["Nonscheduled",     "Outside the scheduled production window"],
    ], "columns": ["StateName", "Description"]},
}


@mcp.tool()
def list_parameter_values(parameter_type: str) -> dict:
    """Get valid filter values for categorical columns - use before writing WHERE clauses.

    Available parameter types:
      facility, area, resource, shift, product, step, semi_e10_state

    Args:
        parameter_type: One of facility, area, resource, shift, product, step, semi_e10_state.
    """
    logger.info(f"Tool 'list_parameter_values' called for: {parameter_type}")

    pt = parameter_type.lower().strip()
    if pt not in PARAMETER_MAP:
        return {"error": f"Unknown parameter_type '{parameter_type}'. Must be one of: {list(PARAMETER_MAP.keys())}"}

    pdef = PARAMETER_MAP[pt]

    # Static lookup (e.g., SEMI E10 states)
    if "static" in pdef:
        return {
            "parameter_type": pt,
            "columns": pdef["columns"],
            "values": pdef["static"],
            "count": len(pdef["static"]),
        }

    if "query" in pdef:
        sql = pdef["query"]
    else:
        sql = f"SELECT {pdef['columns']} FROM [Bottleneck_MCP].[{pdef['view']}] ORDER BY 1"

    result = _execute_read_query(sql, limit=ROW_LIMIT_MAX)
    if "error" in result and not result.get("rows"):
        return result

    return {
        "parameter_type": pt,
        "columns": result["columns"],
        "values": result["rows"],
        "count": result["row_count"],
    }


# ============================================================================
# TOOL 6 (legacy): RUN QUERY — DEPRECATED v3.8 wrapper around query(scope='raw')
# ============================================================================
@mcp.tool()
def run_query(sql: str, database: str = "EntegrisKSPUpgradeDWH", limit: int = 200) -> dict:
    """[DEPRECATED v3.8 — use query(scope='raw')] Unrestricted SELECT, any schema."""
    logger.warning("run_query is deprecated; routing to query(scope='raw')")
    return query(sql=sql, scope="raw", limit=limit, database=database)


# ============================================================================
# META: vw_Dictionary catalog reference
# ============================================================================
# The Bottleneck_MCP schema includes a self-describing meta view:
#   Bottleneck_MCP.vw_Dictionary
# It catalogues every object in the schema with description, mandatory parameters,
# and example queries. Consumers can call:
#   query_view("SELECT * FROM Bottleneck_MCP.vw_Dictionary")
# to auto-discover all 37 objects (35 views + 2 SPs).


# ============================================================================
# SCHEMA-DRIFT GUARD (v3.7.2)
# ============================================================================
def _validate_topic_registry_columns():
    """Compare TOPIC_REGISTRY-advertised columns vs live sys.columns. Log warnings on drift.

    Returns a list of drift descriptors (one per drifted view). Empty list means
    every advertised column list matches live DB. Errors are caught and logged at
    WARNING level — this guard NEVER raises, so it cannot break a cold-start.
    """
    drift = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for topic_id, topic in TOPIC_REGISTRY.items():
            blocks = []
            sv = topic.get("summary_view")
            if sv and isinstance(sv, dict):
                blocks.append(sv)
            blocks.extend(topic.get("detail_views", []) or [])
            for view_block in blocks:
                if not view_block or "name" not in view_block or "columns" not in view_block:
                    continue
                vname = view_block["name"]
                cur.execute(f"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('Bottleneck_MCP.{vname}')")
                rows = cur.fetchall()
                if not rows:
                    continue
                live = {r[0] for r in rows}
                advertised = set(view_block["columns"])
                missing = advertised - live
                extra = live - advertised
                if missing or extra:
                    drift.append({
                        "topic": topic_id,
                        "view": vname,
                        "advertised_not_in_db": sorted(missing),
                        "in_db_not_advertised": sorted(extra),
                    })
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"_validate_topic_registry_columns failed: {e}")
        return []
    if drift:
        logger.warning(f"TOPIC_REGISTRY column drift detected: {drift}")
    else:
        logger.info("TOPIC_REGISTRY column lists match live DB")
    return drift


# ============================================================================
# LAMBDA ENTRY POINT
# ============================================================================
_drift_check_done = False


def lambda_handler(event, context):
    """AWS Lambda handler function for MCP requests."""
    global _drift_check_done
    if not _drift_check_done:
        try:
            _validate_topic_registry_columns()
        except Exception as e:
            logger.warning(f"drift check skipped: {e}")
        _drift_check_done = True
    logger.info(f"Received event: {json.dumps(event) if isinstance(event, dict) else event}")
    return mcp.handle_request(event, context)
