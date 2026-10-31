terraform {
  required_version = ">= 1.5"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }

  # State distant et protégé (C5) : décommentez avec votre bucket S3 (versioning + chiffrement activés)
  # backend "s3" {
  #   bucket       = "freshroute-tfstate"
  #   key          = "snowflake/terraform.tfstate"
  #   region       = "eu-west-3"
  #   encrypt      = true
  #   use_lockfile = true
  # }
}

provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.admin_user
  role              = "ACCOUNTADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(var.private_key_path)
}

# --- Garde-fou de coût : notifie à 75 %, suspend le warehouse à 100 % ---
resource "snowflake_resource_monitor" "budget" {
  name            = "FRESHROUTE_MONITOR"
  credit_quota    = var.monthly_credit_quota
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"
  notify_triggers = [75]
  suspend_trigger = 100
}

# --- Compute : XS, auto-suspend après 60 s d'inactivité ---
resource "snowflake_warehouse" "wh" {
  name                = "FRESHROUTE_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.budget.name
}

# --- Stockage : base et couches medallion ---
resource "snowflake_database" "db" {
  name = "FRESHROUTE"
}

resource "snowflake_schema" "layers" {
  for_each = toset(["BRONZE", "SILVER", "GOLD"])
  database = snowflake_database.db.name
  name     = each.key
}

# --- Rôles : least privilege ---
resource "snowflake_account_role" "engineer" {
  name    = "FRESHROUTE_ENGINEER"
  comment = "Pipeline: read/write on all layers"
}

resource "snowflake_account_role" "analyst" {
  name    = "FRESHROUTE_ANALYST"
  comment = "Business users: read GOLD only"
}

resource "snowflake_grant_privileges_to_account_role" "engineer_wh" {
  account_role_name = snowflake_account_role.engineer.name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_wh" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "engineer_db" {
  account_role_name = snowflake_account_role.engineer.name
  all_privileges    = true
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "engineer_schemas" {
  for_each          = snowflake_schema.layers
  account_role_name = snowflake_account_role.engineer.name
  all_privileges    = true
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${each.value.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_db" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_gold_usage" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"GOLD\""
  }
  depends_on = [snowflake_schema.layers]
}

resource "snowflake_grant_privileges_to_account_role" "analyst_gold_tables" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.db.name}\".\"GOLD\""
    }
  }
  depends_on = [snowflake_schema.layers]
}

# Le compte de service du pipeline reçoit le rôle engineer
resource "snowflake_grant_account_role" "svc_engineer" {
  role_name = snowflake_account_role.engineer.name
  user_name = var.service_user
}
