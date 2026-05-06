"""
Deploy MCP Server for ATHENA AI - Entegris KSP UC-3 Scrap Pareto Analysis.
Mirrors the UC-1 deploy structure exactly (same VPC / subnet / SG / role) but
creates a SEPARATE Lambda function and SEPARATE API Gateway so UC-1 and UC-3
are independently deployable, monitored, and rolled back.

Entegris KSP UC-3 MCP Server — 9 tools for scrap/loss Pareto analytics.

DB connection details (server, instance, username, password, DB names) are
loaded from a local `.env` file (gitignored) and pushed to the Lambda function
as environment variables at deploy time. See `.env.example` for the variable
list.
Access: READ-ONLY only.
"""

import os
import subprocess
import sys
import tempfile
import shutil
import zipfile
import json
import boto3

# ============================================================================
# CONFIGURATION
# ============================================================================
REGION = "us-west-2"
RUNTIME = "python3.12"
TIMEOUT = 300
MEMORY_SIZE = 512

# Naming — distinct from UC-1
FUNCTION_NAME = "entegris-ksp-uc3-scrap-pareto-mcp-server"
API_NAME = "entegris-ksp-uc3-scrap-pareto-mcp-api"

# Database Config — loaded from .env (gitignored). See .env.example for the
# variable list. We refuse to deploy if any required value is missing so we
# never accidentally push an empty-credential Lambda config.
REQUIRED_ENV_KEYS = [
    "DB_SERVER", "DB_PORT", "DB_INSTANCE", "DB_USERNAME", "DB_PASSWORD",
    "DB_NAME_DWH", "DB_NAME_OLTP", "DB_NAME_ODS",
    "FACT_LOSS_TABLE", "REASON_TABLE_DWH", "ROW_LIMIT",
]


def _load_env_file(env_path: str) -> dict:
    """Tiny .env parser — KEY=VALUE per line, '#' comments, no quotes required."""
    cfg: dict = {}
    if not os.path.exists(env_path):
        return cfg
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            cfg[key.strip()] = val.strip().strip('"').strip("'")
    return cfg


def load_db_config() -> dict:
    """Resolve DB config from (a) actual environment variables, then (b) a local
    .env file alongside this script. Errors out clearly if anything is missing."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_cfg = _load_env_file(os.path.join(script_dir, ".env"))
    cfg = {}
    missing = []
    for k in REQUIRED_ENV_KEYS:
        cfg[k] = os.environ.get(k) or file_cfg.get(k)
        if cfg[k] is None or cfg[k] == "":
            missing.append(k)
    if missing:
        raise SystemExit(
            f"Missing required env values: {missing}. "
            f"Set them in {os.path.join(script_dir, '.env')} or export them."
        )
    return cfg

# Tags
TAGS = [
    {"Key": "Project", "Value": "entegris-ksp-uc3-scrap-pareto"},
    {"Key": "Environment", "Value": "poc"},
    {
        "Key": "Description",
        "Value": "Entegris KSP UC-3 Scrap Pareto MCP Server - 9 tools for material loss analytics via CMF DWH",
    },
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SHARED_INFRA_FILE = os.path.join(SCRIPT_DIR, "..", "shared_infra.json")


# ============================================================================
# STEP 1: LOAD SHARED INFRASTRUCTURE
# ============================================================================
def load_shared_infra(ec2):
    """Load shared infra from shared_infra.json or look up by tags."""
    print("\n[Step 1] Loading shared infrastructure...")

    if os.path.exists(SHARED_INFRA_FILE):
        with open(SHARED_INFRA_FILE) as f:
            infra = json.load(f)
        print(f"  Loaded from {SHARED_INFRA_FILE}")
        print(f"  VPC: {infra['vpc_id']}, Subnet: {infra['subnet_id']}")
        print(f"  SG: {infra['sg_id']}, Role: {infra['role_arn']}")
        return infra

    print("  shared_infra.json not found, looking up by tags...")
    infra = {}

    vpcs = ec2.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": ["feb-orc-shared-vpc"]}])
    if not vpcs["Vpcs"]:
        raise RuntimeError("Shared VPC not found. Run deploy_shared_infra.py first.")
    infra["vpc_id"] = vpcs["Vpcs"][0]["VpcId"]

    subnets = ec2.describe_subnets(
        Filters=[
            {"Name": "tag:Name", "Values": ["feb-orc-shared-subnet"]},
            {"Name": "vpc-id", "Values": [infra["vpc_id"]]},
        ]
    )
    infra["subnet_id"] = subnets["Subnets"][0]["SubnetId"]

    sgs = ec2.describe_security_groups(
        Filters=[
            {"Name": "tag:Name", "Values": ["feb-orc-shared-lambda-sg"]},
            {"Name": "vpc-id", "Values": [infra["vpc_id"]]},
        ]
    )
    infra["sg_id"] = sgs["SecurityGroups"][0]["GroupId"]

    iam = boto3.client("iam", region_name=REGION)
    role = iam.get_role(RoleName="feb-orc-mcp-lambda-role")
    infra["role_arn"] = role["Role"]["Arn"]

    print("  Found shared infra by tags")
    return infra


# ============================================================================
# STEP 2: CREATE DEPLOYMENT PACKAGE + DEPLOY LAMBDA
# ============================================================================
def create_deployment_package():
    """Create Lambda deployment zip with dependencies."""
    print("\n[Step 2a] Creating deployment package...")

    temp_dir = tempfile.mkdtemp()
    package_dir = os.path.join(temp_dir, "package")
    os.makedirs(package_dir)

    try:
        print("  Installing dependencies...")
        subprocess.run(
            [
                sys.executable, "-m", "pip", "install",
                "-t", package_dir,
                "-r", os.path.join(SCRIPT_DIR, "requirements.txt"),
                "--platform", "manylinux2014_x86_64",
                "--implementation", "cp",
                "--python-version", "3.12",
                "--only-binary=:all:",
                "--quiet",
            ],
            check=True,
        )

        print("  Copying lambda handler...")
        shutil.copy(os.path.join(SCRIPT_DIR, "lambda_handler.py"), package_dir)

        zip_path = os.path.join(SCRIPT_DIR, "deployment.zip")
        if os.path.exists(zip_path):
            os.remove(zip_path)

        print(f"  Creating zip: {zip_path}")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, package_dir)
                    zipf.write(file_path, arcname)

        zip_size = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"  Package size: {zip_size:.2f} MB")
        return zip_path

    finally:
        shutil.rmtree(temp_dir)


def deploy_lambda(lambda_client, zip_path, role_arn, subnet_id, sg_id, db_config):
    """Deploy or update Lambda function in VPC."""
    print("\n[Step 2b] Deploying Lambda Function...")

    env_vars = {**db_config}

    with open(zip_path, "rb") as f:
        zip_content = f.read()

    zip_size = len(zip_content) / (1024 * 1024)

    if zip_size > 10:
        s3 = boto3.client("s3", region_name=REGION)
        bucket_name = f"entegris-ksp-uc3-mcp-lambda-{REGION}"
        try:
            s3.head_bucket(Bucket=bucket_name)
        except Exception:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        s3_key = f"deployment/{FUNCTION_NAME}.zip"
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=zip_content)
        code_params = {"S3Bucket": bucket_name, "S3Key": s3_key}
    else:
        code_params = {"ZipFile": zip_content}

    try:
        lambda_client.get_function(FunctionName=FUNCTION_NAME)
        function_exists = True
    except lambda_client.exceptions.ResourceNotFoundException:
        function_exists = False

    description = (
        "Entegris KSP UC-3 Scrap Pareto MCP - 9 tools for loss analytics. "
        "READ-ONLY. DB names + connection injected via env vars. "
        "SQL from Athena's Material Losses DWH + Material Yield Loss DWH datasets."
    )

    if function_exists:
        print(f"  Updating function: {FUNCTION_NAME}")
        lambda_client.update_function_code(FunctionName=FUNCTION_NAME, **code_params)
        waiter = lambda_client.get_waiter("function_updated")
        waiter.wait(FunctionName=FUNCTION_NAME)

        lambda_client.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Handler="lambda_handler.lambda_handler",
            Timeout=TIMEOUT,
            MemorySize=MEMORY_SIZE,
            Environment={"Variables": env_vars},
            VpcConfig={"SubnetIds": [subnet_id], "SecurityGroupIds": [sg_id]},
            Description=description,
        )
    else:
        print(f"  Creating function: {FUNCTION_NAME}")
        lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Role=role_arn,
            Handler="lambda_handler.lambda_handler",
            Code=code_params,
            Timeout=TIMEOUT,
            MemorySize=MEMORY_SIZE,
            Environment={"Variables": env_vars},
            VpcConfig={"SubnetIds": [subnet_id], "SecurityGroupIds": [sg_id]},
            Description=description,
            Tags={t["Key"]: t["Value"] for t in TAGS},
        )
        waiter = lambda_client.get_waiter("function_active")
        waiter.wait(FunctionName=FUNCTION_NAME)

    response = lambda_client.get_function(FunctionName=FUNCTION_NAME)
    function_arn = response["Configuration"]["FunctionArn"]
    print(f"  Lambda ARN: {function_arn}")
    return function_arn


# ============================================================================
# STEP 3: API GATEWAY
# ============================================================================
def create_api_gateway(apigateway, lambda_client, function_arn):
    """Create API Gateway HTTP API for the MCP server."""
    print("\n[Step 3] Setting up API Gateway...")

    apis = apigateway.get_apis()
    existing_api = None
    for api in apis["Items"]:
        if api["Name"] == API_NAME:
            existing_api = api
            break

    if existing_api:
        api_id = existing_api["ApiId"]
        api_endpoint = existing_api["ApiEndpoint"]
        print(f"  Using existing API: {API_NAME} ({api_id})")
    else:
        print(f"  Creating API: {API_NAME}")
        response = apigateway.create_api(
            Name=API_NAME,
            ProtocolType="HTTP",
            Target=function_arn,
            Description="Entegris KSP UC-3 Scrap Pareto MCP API - 9 tools for material loss analytics",
        )
        api_id = response["ApiId"]
        api_endpoint = response["ApiEndpoint"]

        account_id = boto3.client("sts").get_caller_identity()["Account"]
        try:
            lambda_client.add_permission(
                FunctionName=FUNCTION_NAME,
                StatementId="api-gateway-invoke",
                Action="lambda:InvokeFunction",
                Principal="apigateway.amazonaws.com",
                SourceArn=f"arn:aws:execute-api:{REGION}:{account_id}:{api_id}/*",
            )
        except lambda_client.exceptions.ResourceConflictException:
            pass

    routes = apigateway.get_routes(ApiId=api_id)
    existing_routes = {r.get("RouteKey") for r in routes["Items"]}

    integrations = apigateway.get_integrations(ApiId=api_id)
    if integrations["Items"]:
        integration_id = integrations["Items"][0]["IntegrationId"]
        for method in ["POST", "GET", "OPTIONS"]:
            route_key = f"{method} /mcp"
            if route_key not in existing_routes:
                try:
                    apigateway.create_route(
                        ApiId=api_id,
                        RouteKey=route_key,
                        Target=f"integrations/{integration_id}",
                    )
                except Exception:
                    pass

    mcp_url = f"{api_endpoint}/mcp"
    print(f"  MCP Endpoint: {mcp_url}")
    return mcp_url


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("=" * 60)
    print("Deploying Entegris KSP UC-3 Scrap Pareto MCP Server")
    print("  9 tools | DWH | READ-ONLY (parameterized SQL only)")
    print("=" * 60)

    ec2 = boto3.client("ec2", region_name=REGION)
    lambda_client = boto3.client("lambda", region_name=REGION)
    apigateway = boto3.client("apigatewayv2", region_name=REGION)

    db_config = load_db_config()
    infra = load_shared_infra(ec2)

    zip_path = create_deployment_package()
    function_arn = deploy_lambda(
        lambda_client, zip_path,
        infra["role_arn"], infra["subnet_id"], infra["sg_id"],
        db_config,
    )

    mcp_url = create_api_gateway(apigateway, lambda_client, function_arn)

    if os.path.exists(zip_path):
        os.remove(zip_path)

    print("\n" + "=" * 60)
    print("DEPLOYMENT COMPLETE")
    print("=" * 60)
    print(f"\nMCP Endpoint:  {mcp_url}")
    print(f"Lambda:        {FUNCTION_NAME}")
    print(f"API Gateway:   {API_NAME}")
    print(f"Tools:         9")
    print(f"Database:      {db_config.get('DB_NAME_DWH', '(from env)')}")
    print(f"Access:        READ-ONLY")
    print(f"\nTo test:")
    print(f'  curl -X POST "{mcp_url}" -H "Content-Type: application/json" -d \'{{"jsonrpc":"2.0","id":1,"method":"tools/list"}}\'')


if __name__ == "__main__":
    main()
