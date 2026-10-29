# Part 23: who may do what through the lab's Trino, which rows they see and which columns come back masked.
# Trino asks OPA before every operation (trino/access-control.properties); OPA 1.x syntax (`if`, `contains`).
# Data: data.larkspur.users (who is who), data.larkspur.classification (exported from Iceberg table properties by
# `scripts.part23_privacy policy-data`). The pseudonymisation key is not here: it lives in a one-row table only the
# `masker` identity may read (see the identifier mask).
package trino

import rego.v1

user := input.context.identity.user

people := data.larkspur.users

platform if user in people.platform # every Trino user Parts 1-22 run as: unchanged behaviour

privacy if user in people.privacy # the privacy office and its erasure job: unmasked, may delete

developer if user in people.developers # dbt's dev target and CI (Part 18)

analyst := people.analysts[user] # people who read governed data through Trino, masked and filtered

masker if user == "masker" # the identity Trino evaluates identifier masks as: the key table and classified tables

# ---------- allow ----------

default allow := false

allow if platform

allow if privacy

allow if {
	developer
	not writes_outside_dev
}

allow if {
	analyst
	analyst_may
}

allow if {
	masker
	input.action.operation == "ExecuteQuery"
}

allow if {
	masker
	input.action.operation == "AccessCatalog"
	catalog == "governed"
}

allow if { # the key table, and the classified tables whose columns it hashes; nothing else
	masker
	input.action.operation in {"SelectFromColumns", "FilterColumns"}
	t := input.action.resource.table
	t.catalogName == "governed"
	masker_table(t)
}

masker_table(t) if [t.schemaName, t.tableName] == ["customer", "pseudonym_key"]

masker_table(t) if classified(t)

# Statements that change a table, view or schema. A developer may run them only in the dev catalog.
write_ops := {
	"CreateSchema", "DropSchema", "RenameSchema", "SetSchemaAuthorization",
	"CreateTable", "DropTable", "RenameTable", "SetTableProperties", "SetTableComment", "SetTableAuthorization",
	"AddColumn", "DropColumn", "RenameColumn", "AlterColumn", "SetColumnComment",
	"InsertIntoTable", "DeleteFromTable", "TruncateTable", "UpdateTableColumns",
	"CreateView", "RenameView", "DropView", "SetViewComment", "SetViewAuthorization",
	"CreateMaterializedView", "RefreshMaterializedView", "DropMaterializedView", "RenameMaterializedView",
	"SetMaterializedViewProperties", "ExecuteTableProcedure", "ExecuteProcedure", "CreateFunction", "DropFunction",
}

writes_outside_dev if {
	input.action.operation in write_ops
	catalog != "lakehouse_dev"
}

# What an analyst may do: read, in the governed catalog only (and see that the catalog exists).
read_ops := {
	"AccessCatalog", "FilterCatalogs", "ShowSchemas", "FilterSchemas", "ShowTables", "FilterTables",
	"ShowColumns", "FilterColumns", "SelectFromColumns", "ShowCreateTable", "FilterViewQueryOwnedBy",
}

analyst_may if input.action.operation == "ExecuteQuery"

analyst_may if {
	input.action.operation in read_ops
	catalog in {"governed", "system"}
	not unclassified_table
}

# Fail closed: a table nobody has classified yet is not readable by analysts, masked or not.
unclassified_table if {
	t := input.action.resource.table
	t.catalogName == "governed"
	not classified(t)
}

# The catalog an operation touches, whatever the resource's shape.
catalog := input.action.resource.catalog.name

catalog := input.action.resource.schema.catalogName

catalog := input.action.resource.table.catalogName

catalog := input.action.resource.function.catalogName

# ---------- batched filtering (FilterCatalogs, FilterTables, ...): the indexes of the permitted items ----------

batch contains i if {
	input.action.operation != "FilterColumns"
	some i, r in input.action.filterResources
	allow with input.action.resource as r
}

batch contains i if { # one table, many columns: all of them if the table may be read
	input.action.operation == "FilterColumns"
	r := input.action.filterResources[0]
	allow with input.action.resource as r
	some i, _ in r.table.columns
}

# ---------- rows: analysts see only their countries, on tables classified with a row scope ----------

classified(t) := data.larkspur.classification[concat(".", [t.schemaName, t.tableName])]

row_filters contains {"expression": sprintf("%s IN ('%s')", [scope, concat("', '", analyst.countries)])} if {
	analyst
	scope := classified(input.action.resource.table)["row-scope"]
}

# ---------- columns: analysts get masked values for classified columns ----------

batch_masks contains {"index": i, "viewExpression": view} if {
	analyst
	some i, r in input.action.filterResources
	c := r.column
	cls := classified(c).columns[c.columnName]
	view := mask(c, cls)
}

# direct identifiers (name, email, phone, street): gone
mask(c, cls) := {"expression": sprintf("CAST(NULL AS %s)", [c.columnType])} if cls == "direct"

# keys that link a person's rows (customer_id, loyalty_id, anonymous_id): a keyed hash, the same in every table, so
# joins and distinct counts still work. The key is read by a subquery that Trino runs as `masker`: the analyst's
# plan (EXPLAIN) and OPA's decision log show the subquery, never the key, and analysts can't read the key table.
mask(c, cls) := {
	"expression": sprintf(
		"substr(to_hex(hmac_sha256(to_utf8(%s), (SELECT key FROM governed.customer.pseudonym_key))), 1, 16)",
		[quote(c.columnName)],
	),
	"identity": "masker",
} if cls == "identifier"

# quasi-identifiers (postcode, user agent): coarsened to their first three characters, not removed
mask(c, cls) := {"expression": sprintf("substr(%s, 1, 3)", [quote(c.columnName)])} if cls == "quasi"

quote(name) := sprintf("\"%s\"", [name])
