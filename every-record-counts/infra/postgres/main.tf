# Part 34: the platform's onboarding kit for a source domain, as code. Each domain whose operational tables feed the
# lakehouse (CDC, Part 16; batch extracts, Part 15) gets the same three things in the lab's PostgreSQL: a schema, a
# reader role for the platform's connectors, and grants that also cover tables created later.
# Credentials come from the environment (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGSSLMODE: `make infra-plan` sets them).

terraform {
  required_version = ">= 1.13.0"
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "1.27.0"
    }
  }
}

provider "postgresql" {
  database        = "larkspur"
  superuser       = true # the lab's admin user; a managed database (RDS, Cloud SQL) needs `false`
  connect_timeout = 5
}

module "loyalty" {
  source     = "./modules/source_domain"
  domain     = "loyalty"
  owner_team = "crm"
}

module "promotions" {
  source     = "./modules/source_domain"
  domain     = "promotions"
  owner_team = "merchandising"
}

output "reader_roles" {
  value = [module.loyalty.reader_role, module.promotions.reader_role]
}
