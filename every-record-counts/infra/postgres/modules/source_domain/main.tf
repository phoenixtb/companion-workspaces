# One source domain: its schema (owned by the team's role), a NOLOGIN reader role the platform's connectors are granted,
# and read access to every table in the schema, today's and tomorrow's.

terraform {
  required_providers {
    postgresql = {
      source = "cyrilgdn/postgresql"
    }
  }
}

variable "domain" {
  type        = string
  description = "Schema name, lower case: the domain's tables live here."
  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{1,30}$", var.domain))
    error_message = "domain must be a lower-case identifier."
  }
}

variable "owner_team" {
  type        = string
  description = "The team that owns the domain's data and answers its data incidents."
}

resource "postgresql_role" "owner" {
  name  = "${var.domain}_owner" # the team's role: owns the schema and creates its tables
  login = false
}

resource "postgresql_role" "reader" {
  name  = "${var.domain}_reader" # granted to the platform's CDC and extract users; reads only
  login = false
}

resource "postgresql_schema" "this" {
  name  = var.domain
  owner = postgresql_role.owner.name
}

resource "postgresql_grant" "usage" {
  database    = "larkspur"
  schema      = postgresql_schema.this.name
  role        = postgresql_role.reader.name
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "tables" {
  database    = "larkspur"
  schema      = postgresql_schema.this.name
  role        = postgresql_role.reader.name
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_default_privileges" "future_tables" {
  database    = "larkspur"
  schema      = postgresql_schema.this.name
  owner       = postgresql_role.owner.name
  role        = postgresql_role.reader.name
  object_type = "table"
  privileges  = ["SELECT"]
}

output "reader_role" {
  value = "${postgresql_role.reader.name} (owner team: ${var.owner_team})"
}
