-- The SQL that OpenMeter's Ent schema exporter wrote, unmodified apart from
-- this header and the checkout path in its atlas:pos comments:
--   github.com/openmeterio/openmeter at f04b561f8dd2dafd1e77a02fb4258fab0edbef8f
--   go run -mod=readonly ./tools/migrate/cmd/entschema
-- OpenMeter is licensed under the Apache License 2.0.
-- atlas:pos addons[type=table] openmeter/ent/schema/addon.go:17
-- atlas:pos addon_rate_cards[type=table] openmeter/ent/schema/addon.go:114
-- atlas:pos apps[type=table] openmeter/ent/schema/app.go:16
-- atlas:pos app_custom_invoicings[type=table] openmeter/ent/schema/appcustominvoicing.go:15
-- atlas:pos app_custom_invoicing_customers[type=table] openmeter/ent/schema/appcustominvoicing.go:54
-- atlas:pos app_customers[type=table] openmeter/ent/schema/app.go:53
-- atlas:pos app_stripes[type=table] openmeter/ent/schema/app_stripe.go:16
-- atlas:pos app_stripe_customers[type=table] openmeter/ent/schema/app_stripe.go:72
-- atlas:pos balance_snapshots[type=table] openmeter/ent/schema/balance_snapshot.go:16
-- atlas:pos billing_customer_locks[type=table] openmeter/ent/schema/billing.go:1436
-- atlas:pos billing_customer_overrides[type=table] openmeter/ent/schema/billing.go:198
-- atlas:pos billing_gathering_invoice_lines[type=table] openmeter/ent/schema/billing.go:626
-- atlas:pos billing_invoices[type=table] openmeter/ent/schema/billing.go:1106
-- atlas:pos billing_invoice_lines[type=table] openmeter/ent/schema/billing.go:411
-- atlas:pos billing_invoice_line_usage_discounts[type=table] openmeter/ent/schema/billing.go:918
-- atlas:pos billing_invoice_split_line_groups[type=table] openmeter/ent/schema/billing.go:801
-- atlas:pos billing_invoice_usage_based_line_configs[type=table] openmeter/ent/schema/billing.go:716
-- atlas:pos billing_invoice_validation_issues[type=table] openmeter/ent/schema/billing.go:1345
-- atlas:pos billing_invoice_write_schema_levels[type=table] openmeter/ent/schema/billing.go:1465
-- atlas:pos billing_profiles[type=table] openmeter/ent/schema/billing.go:30
-- atlas:pos billing_sequence_numbers[type=table] openmeter/ent/schema/billing.go:1410
-- atlas:pos billing_standard_invoice_detailed_lines[type=table] openmeter/ent/schema/billing.go:982
-- atlas:pos billing_standard_invoice_detailed_line_amount_discounts[type=table] openmeter/ent/schema/billing.go:1040
-- atlas:pos billing_workflow_configs[type=table] openmeter/ent/schema/billing.go:112
-- atlas:pos charges[type=table] openmeter/ent/schema/charges.go:145
-- atlas:pos charge_credit_purchases[type=table] openmeter/ent/schema/chargescreditpurchase.go:20
-- atlas:pos charge_credit_purchase_cost_bases[type=table] openmeter/ent/schema/chargescreditpurchase.go:204
-- atlas:pos charge_credit_purchase_credit_grants[type=table] openmeter/ent/schema/chargescreditpurchase.go:224
-- atlas:pos charge_credit_purchase_external_payments[type=table] openmeter/ent/schema/chargescreditpurchase.go:272
-- atlas:pos charge_credit_purchase_invoiced_payments[type=table] openmeter/ent/schema/chargescreditpurchase.go:307
-- atlas:pos charge_flat_fees[type=table] openmeter/ent/schema/chargesflatfee.go:25
-- atlas:pos charge_flat_fee_cost_bases[type=table] openmeter/ent/schema/chargesflatfee.go:177
-- atlas:pos charge_flat_fee_overrides[type=table] openmeter/ent/schema/chargesflatfee.go:197
-- atlas:pos charge_flat_fee_runs[type=table] openmeter/ent/schema/chargesflatfee.go:320
-- atlas:pos charge_flat_fee_run_credit_allocations[type=table] openmeter/ent/schema/chargesflatfee.go:547
-- atlas:pos charge_flat_fee_run_detailed_lines[type=table] openmeter/ent/schema/chargesflatfee.go:422
-- atlas:pos charge_flat_fee_run_invoiced_usages[type=table] openmeter/ent/schema/chargesflatfee.go:509
-- atlas:pos charge_flat_fee_run_overage_credit_allocations[type=table] openmeter/ent/schema/chargesflatfee.go:585
-- atlas:pos charge_flat_fee_run_payments[type=table] openmeter/ent/schema/chargesflatfee.go:468
-- atlas:pos charge_usage_based[type=table] openmeter/ent/schema/chargesusagebased.go:25
-- atlas:pos charge_usage_based_cost_bases[type=table] openmeter/ent/schema/chargesusagebased.go:190
-- atlas:pos charge_usage_based_overrides[type=table] openmeter/ent/schema/chargesusagebased.go:210
-- atlas:pos charge_usage_based_run_credit_allocations[type=table] openmeter/ent/schema/chargesusagebased.go:546
-- atlas:pos charge_usage_based_run_detailed_line[type=table] openmeter/ent/schema/chargesusagebased.go:473
-- atlas:pos charge_usage_based_run_invoiced_usages[type=table] openmeter/ent/schema/chargesusagebased.go:614
-- atlas:pos charge_usage_based_run_overage_credit_allocations[type=table] openmeter/ent/schema/chargesusagebased.go:580
-- atlas:pos charge_usage_based_run_payments[type=table] openmeter/ent/schema/chargesusagebased.go:645
-- atlas:pos charge_usage_based_runs[type=table] openmeter/ent/schema/chargesusagebased.go:323
-- atlas:pos credit_realization_lineages[type=table] openmeter/ent/schema/creditrealizationlineage.go:26
-- atlas:pos credit_realization_lineage_segments[type=table] openmeter/ent/schema/creditrealizationlineage.go:111
-- atlas:pos currency_cost_bases[type=table] openmeter/ent/schema/custom_currencies.go:89
-- atlas:pos custom_currencies[type=table] openmeter/ent/schema/custom_currencies.go:16
-- atlas:pos customers[type=table] openmeter/ent/schema/customer.go:17
-- atlas:pos customer_subjects[type=table] openmeter/ent/schema/customer.go:90
-- atlas:pos entitlements[type=table] openmeter/ent/schema/entitlement.go:21
-- atlas:pos event_outboxes[type=table] openmeter/ent/schema/eventoutbox.go:15
-- atlas:pos features[type=table] openmeter/ent/schema/feature.go:17
-- atlas:pos grants[type=table] openmeter/ent/schema/grant.go:15
-- atlas:pos llm_cost_prices[type=table] openmeter/ent/schema/llmcostprice.go:16
-- atlas:pos ledger_accounts[type=table] openmeter/ent/schema/ledger_account.go:17
-- atlas:pos ledger_breakage_records[type=table] openmeter/ent/schema/ledger_breakage_record.go:17
-- atlas:pos ledger_credit_void_records[type=table] openmeter/ent/schema/ledger_credit_void_record.go:14
-- atlas:pos ledger_customer_accounts[type=table] openmeter/ent/schema/ledger_customer_account.go:16
-- atlas:pos ledger_entries[type=table] openmeter/ent/schema/ledger_entry.go:15
-- atlas:pos ledger_sub_accounts[type=table] openmeter/ent/schema/ledger_account.go:50
-- atlas:pos ledger_sub_account_routes[type=table] openmeter/ent/schema/ledger_account.go:100
-- atlas:pos ledger_transactions[type=table] openmeter/ent/schema/ledger_transaction.go:14
-- atlas:pos ledger_transaction_groups[type=table] openmeter/ent/schema/ledger_transaction_group.go:12
-- atlas:pos meters[type=table] openmeter/ent/schema/meter.go:15
-- atlas:pos notification_channels[type=table] openmeter/ent/schema/notification.go:21
-- atlas:pos notification_events[type=table] openmeter/ent/schema/notification.go:118
-- atlas:pos notification_event_delivery_status[type=table] openmeter/ent/schema/notification.go:170
-- atlas:pos notification_rules[type=table] openmeter/ent/schema/notification.go:67
-- atlas:pos organization_default_tax_codes[type=table] openmeter/ent/schema/organizationdefaulttaxcodes.go:15
-- atlas:pos plans[type=table] openmeter/ent/schema/productcatalog.go:17
-- atlas:pos plan_addons[type=table] openmeter/ent/schema/planaddon.go:13
-- atlas:pos plan_phases[type=table] openmeter/ent/schema/productcatalog.go:113
-- atlas:pos plan_rate_cards[type=table] openmeter/ent/schema/productcatalog.go:168
-- atlas:pos subjects[type=table] openmeter/ent/schema/subject.go:14
-- atlas:pos subscriptions[type=table] openmeter/ent/schema/subscription.go:19
-- atlas:pos subscription_addons[type=table] openmeter/ent/schema/subscription_addon.go:15
-- atlas:pos subscription_addon_quantities[type=table] openmeter/ent/schema/subscription_addon.go:68
-- atlas:pos subscription_billing_sync_states[type=table] openmeter/ent/schema/subscriptionbillingsync.go:13
-- atlas:pos subscription_cost_basis_pins[type=table] openmeter/ent/schema/subscription_cost_basis_pin.go:14
-- atlas:pos subscription_items[type=table] openmeter/ent/schema/subscription.go:160
-- atlas:pos subscription_phases[type=table] openmeter/ent/schema/subscription.go:112
-- atlas:pos tax_codes[type=table] openmeter/ent/schema/taxcode.go:18
-- atlas:pos usage_resets[type=table] openmeter/ent/schema/usage_reset.go:14
-- atlas:pos notification_channel_rules[type=table] openmeter/ent/schema/notification.go:21
-- atlas:pos notification_event_delivery_status_events[type=table] openmeter/ent/schema/notification.go:170
-- atlas:pos charges_search_v1s[type=view] openmeter/ent/schema/charges.go:68

-- Create "addons" table
CREATE TABLE "addons" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "version" bigint NOT NULL,
  "currency" character varying NOT NULL,
  "instance_type" character varying NOT NULL DEFAULT 'single',
  "effective_from" timestamptz NULL,
  "effective_to" timestamptz NULL,
  "annotations" jsonb NULL,
  "custom_currency_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "addon_currency_code_length" CHECK (char_length(currency) BETWEEN 3 AND 24),
  CONSTRAINT "addon_currency_reference" CHECK ((char_length(currency) = 3 AND custom_currency_id IS NULL) OR (char_length(currency) > 3 AND custom_currency_id IS NOT NULL))
);
-- Create index "addon_id" to table: "addons"
CREATE UNIQUE INDEX "addon_id" ON "addons" ("id");
-- Create index "addon_namespace" to table: "addons"
CREATE INDEX "addon_namespace" ON "addons" ("namespace");
-- Create index "addon_namespace_id" to table: "addons"
CREATE UNIQUE INDEX "addon_namespace_id" ON "addons" ("namespace", "id");
-- Create index "addon_namespace_key_deleted_at" to table: "addons"
CREATE UNIQUE INDEX "addon_namespace_key_deleted_at" ON "addons" ("namespace", "key", "deleted_at");
-- Create index "addon_namespace_key_version" to table: "addons"
CREATE UNIQUE INDEX "addon_namespace_key_version" ON "addons" ("namespace", "key", "version") WHERE deleted_at IS NULL;
-- Create index "addon_annotations" to table: "addons"
CREATE INDEX "addon_annotations" ON "addons" USING GIN ("annotations");
-- Create index "addon_custom_currency_id" to table: "addons"
CREATE INDEX "addon_custom_currency_id" ON "addons" ("custom_currency_id");
-- Create "addon_rate_cards" table
CREATE TABLE "addon_rate_cards" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "tax_behavior" character varying NULL,
  "type" character varying NOT NULL,
  "feature_key" character varying NULL,
  "annotations" jsonb NULL,
  "entitlement_template" jsonb NULL,
  "tax_config" jsonb NULL,
  "billing_cadence" character varying NULL,
  "price" jsonb NULL,
  "currency" character varying NULL,
  "discounts" jsonb NULL,
  "unit_config" jsonb NULL,
  "addon_id" character(26) NOT NULL,
  "custom_currency_id" character(26) NULL,
  "feature_id" character(26) NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "addon_rate_card_currency_code_length" CHECK (currency IS NULL OR char_length(currency) BETWEEN 3 AND 24),
  CONSTRAINT "addon_rate_card_currency_has_price" CHECK (price IS NOT NULL OR currency IS NULL),
  CONSTRAINT "addon_rate_card_currency_reference" CHECK ((currency IS NULL AND custom_currency_id IS NULL) OR (currency IS NOT NULL AND char_length(currency) = 3 AND custom_currency_id IS NULL) OR (currency IS NOT NULL AND char_length(currency) > 3 AND custom_currency_id IS NOT NULL)),
  CONSTRAINT "addon_rate_card_feature_reference" CHECK ((feature_key IS NULL AND feature_id IS NULL) OR (feature_key IS NOT NULL AND feature_key <> '' AND feature_id IS NOT NULL AND feature_id <> '')),
  CONSTRAINT "addon_rate_card_tax_behavior_consistency" CHECK (tax_behavior IS NOT DISTINCT FROM tax_config ->> 'behavior'),
  CONSTRAINT "addon_rate_card_tax_code_consistency" CHECK ((tax_code_id::text IS NOT DISTINCT FROM tax_config ->> 'tax_code_id') AND (NULLIF(btrim(tax_config -> 'stripe' ->> 'code'), '') IS NULL OR tax_code_id IS NOT NULL))
);
-- Create index "addonratecard_id" to table: "addon_rate_cards"
CREATE UNIQUE INDEX "addonratecard_id" ON "addon_rate_cards" ("id");
-- Create index "addonratecard_namespace" to table: "addon_rate_cards"
CREATE INDEX "addonratecard_namespace" ON "addon_rate_cards" ("namespace");
-- Create index "addonratecard_namespace_id" to table: "addon_rate_cards"
CREATE UNIQUE INDEX "addonratecard_namespace_id" ON "addon_rate_cards" ("namespace", "id");
-- Create index "addonratecard_namespace_key_deleted_at" to table: "addon_rate_cards"
CREATE UNIQUE INDEX "addonratecard_namespace_key_deleted_at" ON "addon_rate_cards" ("namespace", "key", "deleted_at");
-- Create index "addonratecard_tax_code_id" to table: "addon_rate_cards"
CREATE INDEX "addonratecard_tax_code_id" ON "addon_rate_cards" ("tax_code_id");
-- Create index "addonratecard_addon_id_key" to table: "addon_rate_cards"
CREATE UNIQUE INDEX "addonratecard_addon_id_key" ON "addon_rate_cards" ("addon_id", "key") WHERE deleted_at IS NULL;
-- Create index "addonratecard_addon_id_feature_key" to table: "addon_rate_cards"
CREATE UNIQUE INDEX "addonratecard_addon_id_feature_key" ON "addon_rate_cards" ("addon_id", "feature_key") WHERE deleted_at IS NULL;
-- Create index "addonratecard_custom_currency_id" to table: "addon_rate_cards"
CREATE INDEX "addonratecard_custom_currency_id" ON "addon_rate_cards" ("custom_currency_id");
-- Create "apps" table
CREATE TABLE "apps" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "type" character varying NOT NULL,
  "status" character varying NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "app_id" to table: "apps"
CREATE UNIQUE INDEX "app_id" ON "apps" ("id");
-- Create index "app_namespace" to table: "apps"
CREATE INDEX "app_namespace" ON "apps" ("namespace");
-- Create index "app_namespace_id" to table: "apps"
CREATE UNIQUE INDEX "app_namespace_id" ON "apps" ("namespace", "id");
-- Create index "app_namespace_type" to table: "apps"
CREATE INDEX "app_namespace_type" ON "apps" ("namespace", "type");
-- Create "app_custom_invoicings" table
CREATE TABLE "app_custom_invoicings" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "enable_draft_sync_hook" boolean NOT NULL DEFAULT false,
  "enable_issuing_sync_hook" boolean NOT NULL DEFAULT false,
  PRIMARY KEY ("id")
);
-- Create index "appcustominvoicing_id" to table: "app_custom_invoicings"
CREATE UNIQUE INDEX "appcustominvoicing_id" ON "app_custom_invoicings" ("id");
-- Create index "appcustominvoicing_namespace" to table: "app_custom_invoicings"
CREATE INDEX "appcustominvoicing_namespace" ON "app_custom_invoicings" ("namespace");
-- Create index "appcustominvoicing_namespace_id" to table: "app_custom_invoicings"
CREATE UNIQUE INDEX "appcustominvoicing_namespace_id" ON "app_custom_invoicings" ("namespace", "id");
-- Create "app_custom_invoicing_customers" table
CREATE TABLE "app_custom_invoicing_customers" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "metadata" jsonb NULL,
  "app_id" character(26) NOT NULL,
  "customer_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "appcustominvoicingcustomer_namespace" to table: "app_custom_invoicing_customers"
CREATE INDEX "appcustominvoicingcustomer_namespace" ON "app_custom_invoicing_customers" ("namespace");
-- Create index "appcustominvoicingcustomer_namespace_app_id_customer_id" to table: "app_custom_invoicing_customers"
CREATE UNIQUE INDEX "appcustominvoicingcustomer_namespace_app_id_customer_id" ON "app_custom_invoicing_customers" ("namespace", "app_id", "customer_id") WHERE deleted_at IS NULL;
-- Create "app_customers" table
CREATE TABLE "app_customers" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "app_id" character(26) NOT NULL,
  "customer_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "appcustomer_namespace" to table: "app_customers"
CREATE INDEX "appcustomer_namespace" ON "app_customers" ("namespace");
-- Create index "appcustomer_namespace_app_id_customer_id" to table: "app_customers"
CREATE UNIQUE INDEX "appcustomer_namespace_app_id_customer_id" ON "app_customers" ("namespace", "app_id", "customer_id") WHERE deleted_at IS NULL;
-- Create "app_stripes" table
CREATE TABLE "app_stripes" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "stripe_account_id" character varying NOT NULL,
  "stripe_livemode" boolean NOT NULL,
  "api_key" character varying NULL,
  "masked_api_key" character varying NOT NULL,
  "stripe_webhook_id" character varying NOT NULL,
  "webhook_secret" character varying NULL,
  "webhook_schema_version" bigint NOT NULL DEFAULT 1,
  PRIMARY KEY ("id"),
  CONSTRAINT "app_stripe_secret_lifecycle" CHECK ((deleted_at IS NULL AND api_key IS NOT NULL AND webhook_secret IS NOT NULL) OR (deleted_at IS NOT NULL AND api_key IS NULL AND webhook_secret IS NULL))
);
-- Create index "appstripe_id" to table: "app_stripes"
CREATE UNIQUE INDEX "appstripe_id" ON "app_stripes" ("id");
-- Create index "appstripe_namespace" to table: "app_stripes"
CREATE INDEX "appstripe_namespace" ON "app_stripes" ("namespace");
-- Create index "appstripe_namespace_stripe_account_id_stripe_livemode" to table: "app_stripes"
CREATE UNIQUE INDEX "appstripe_namespace_stripe_account_id_stripe_livemode" ON "app_stripes" ("namespace", "stripe_account_id", "stripe_livemode") WHERE deleted_at IS NULL;
-- Create "app_stripe_customers" table
CREATE TABLE "app_stripe_customers" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "stripe_customer_id" character varying NOT NULL,
  "stripe_default_payment_method_id" character varying NULL,
  "app_id" character(26) NOT NULL,
  "customer_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "appstripecustomer_namespace" to table: "app_stripe_customers"
CREATE INDEX "appstripecustomer_namespace" ON "app_stripe_customers" ("namespace");
-- Create index "appstripecustomer_namespace_app_id_customer_id" to table: "app_stripe_customers"
CREATE UNIQUE INDEX "appstripecustomer_namespace_app_id_customer_id" ON "app_stripe_customers" ("namespace", "app_id", "customer_id") WHERE deleted_at IS NULL;
-- Create index "appstripecustomer_app_id_stripe_customer_id" to table: "app_stripe_customers"
CREATE UNIQUE INDEX "appstripecustomer_app_id_stripe_customer_id" ON "app_stripe_customers" ("app_id", "stripe_customer_id") WHERE deleted_at IS NULL;
-- Create "balance_snapshots" table
CREATE TABLE "balance_snapshots" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "grant_balances" jsonb NOT NULL,
  "usage" jsonb NULL,
  "usage_snapshot" jsonb NULL,
  "balance" numeric NOT NULL,
  "overage" numeric NOT NULL,
  "at" timestamptz NOT NULL,
  "unit_config" jsonb NULL,
  "owner_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "balancesnapshot_namespace" to table: "balance_snapshots"
CREATE INDEX "balancesnapshot_namespace" ON "balance_snapshots" ("namespace");
-- Create index "balancesnapshot_namespace_owner_id_at" to table: "balance_snapshots"
CREATE INDEX "balancesnapshot_namespace_owner_id_at" ON "balance_snapshots" ("namespace", "owner_id", "at") WHERE deleted_at IS NULL;
-- Create "billing_customer_locks" table
CREATE TABLE "billing_customer_locks" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "customer_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billingcustomerlock_namespace" to table: "billing_customer_locks"
CREATE INDEX "billingcustomerlock_namespace" ON "billing_customer_locks" ("namespace");
-- Create index "billingcustomerlock_id" to table: "billing_customer_locks"
CREATE UNIQUE INDEX "billingcustomerlock_id" ON "billing_customer_locks" ("id");
-- Create index "billingcustomerlock_namespace_customer_id" to table: "billing_customer_locks"
CREATE UNIQUE INDEX "billingcustomerlock_namespace_customer_id" ON "billing_customer_locks" ("namespace", "customer_id");
-- Create "billing_customer_overrides" table
CREATE TABLE "billing_customer_overrides" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "tax_behavior" character varying NULL,
  "collection_alignment" character varying NULL,
  "anchored_alignment_detail" jsonb NULL,
  "line_collection_period" character varying NULL,
  "invoice_auto_advance" boolean NULL,
  "invoice_draft_period" character varying NULL,
  "invoice_due_after" character varying NULL,
  "invoice_collection_method" character varying NULL,
  "invoice_progressive_billing" boolean NULL,
  "invoice_default_tax_config" jsonb NULL,
  "billing_profile_id" character(26) NULL,
  "customer_id" character(26) NOT NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id")
);
-- Create index "billing_customer_overrides_customer_id_key" to table: "billing_customer_overrides"
CREATE UNIQUE INDEX "billing_customer_overrides_customer_id_key" ON "billing_customer_overrides" ("customer_id");
-- Create index "billingcustomeroverride_id" to table: "billing_customer_overrides"
CREATE UNIQUE INDEX "billingcustomeroverride_id" ON "billing_customer_overrides" ("id");
-- Create index "billingcustomeroverride_namespace" to table: "billing_customer_overrides"
CREATE INDEX "billingcustomeroverride_namespace" ON "billing_customer_overrides" ("namespace");
-- Create index "billingcustomeroverride_tax_code_id" to table: "billing_customer_overrides"
CREATE INDEX "billingcustomeroverride_tax_code_id" ON "billing_customer_overrides" ("tax_code_id");
-- Create index "billingcustomeroverride_namespace_id" to table: "billing_customer_overrides"
CREATE UNIQUE INDEX "billingcustomeroverride_namespace_id" ON "billing_customer_overrides" ("namespace", "id");
-- Create index "billingcustomeroverride_namespace_customer_id" to table: "billing_customer_overrides"
CREATE UNIQUE INDEX "billingcustomeroverride_namespace_customer_id" ON "billing_customer_overrides" ("namespace", "customer_id");
-- Create "billing_gathering_invoice_lines" table
CREATE TABLE "billing_gathering_invoice_lines" (
  "id" character(26) NOT NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "currency" character varying(3) NOT NULL,
  "service_period_start" timestamptz NOT NULL,
  "service_period_end" timestamptz NOT NULL,
  "tax_config" jsonb NULL,
  "price_type" character varying NOT NULL,
  "feature_key" character varying NULL,
  "price" jsonb NOT NULL,
  "unit_config" jsonb NULL,
  "ratecard_discounts" jsonb NULL,
  "child_unique_reference_id" character varying NULL,
  "subscription_billing_period_from" timestamptz NULL,
  "subscription_billing_period_to" timestamptz NULL,
  "tax_behavior" character varying NULL,
  "invoice_at" timestamptz NOT NULL,
  "managed_by" character varying NOT NULL,
  "engine" character varying NOT NULL DEFAULT 'invoicing',
  "invoice_id" character(26) NOT NULL,
  "split_line_group_id" character(26) NULL,
  "charge_id" character(26) NULL,
  "subscription_id" character(26) NULL,
  "subscription_item_id" character(26) NULL,
  "subscription_phase_id" character(26) NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "child_unique_reference_id_not_empty" CHECK (child_unique_reference_id <> '')
);
-- Create index "billinggatheringinvoiceline_annotations" to table: "billing_gathering_invoice_lines"
CREATE INDEX "billinggatheringinvoiceline_annotations" ON "billing_gathering_invoice_lines" USING GIN ("annotations");
-- Create index "billinggatheringinvoiceline_id" to table: "billing_gathering_invoice_lines"
CREATE UNIQUE INDEX "billinggatheringinvoiceline_id" ON "billing_gathering_invoice_lines" ("id");
-- Create index "billinggatheringinvoiceline_namespace" to table: "billing_gathering_invoice_lines"
CREATE INDEX "billinggatheringinvoiceline_namespace" ON "billing_gathering_invoice_lines" ("namespace");
-- Create index "billinggatheringinvoiceline_namespace_id" to table: "billing_gathering_invoice_lines"
CREATE UNIQUE INDEX "billinggatheringinvoiceline_namespace_id" ON "billing_gathering_invoice_lines" ("namespace", "id");
-- Create index "billinggatheringinvoiceline_tax_code_id" to table: "billing_gathering_invoice_lines"
CREATE INDEX "billinggatheringinvoiceline_tax_code_id" ON "billing_gathering_invoice_lines" ("tax_code_id");
-- Create index "billinggatheringinvoiceline_namespace_invoice_id" to table: "billing_gathering_invoice_lines"
CREATE INDEX "billinggatheringinvoiceline_namespace_invoice_id" ON "billing_gathering_invoice_lines" ("namespace", "invoice_id");
-- Create index "billinggatheringinvoiceline_namespace_split_line_group_id" to table: "billing_gathering_invoice_lines"
CREATE INDEX "billinggatheringinvoiceline_namespace_split_line_group_id" ON "billing_gathering_invoice_lines" ("namespace", "split_line_group_id");
-- Create index "billinggatheringinvoiceline_namespace_charge_id" to table: "billing_gathering_invoice_lines"
CREATE INDEX "billinggatheringinvoiceline_namespace_charge_id" ON "billing_gathering_invoice_lines" ("namespace", "charge_id");
-- Create index "billinggatheringline_ns_invoice_child_id" to table: "billing_gathering_invoice_lines"
CREATE UNIQUE INDEX "billinggatheringline_ns_invoice_child_id" ON "billing_gathering_invoice_lines" ("namespace", "invoice_id", "child_unique_reference_id") WHERE child_unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create index "billinggatheringline_ns_subscription_ref" to table: "billing_gathering_invoice_lines"
CREATE INDEX "billinggatheringline_ns_subscription_ref" ON "billing_gathering_invoice_lines" ("namespace", "subscription_id", "subscription_phase_id", "subscription_item_id");
-- Create "billing_invoices" table
CREATE TABLE "billing_invoices" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "supplier_address_country" character varying NULL,
  "supplier_address_postal_code" character varying NULL,
  "supplier_address_state" character varying NULL,
  "supplier_address_city" character varying NULL,
  "supplier_address_line1" character varying NULL,
  "supplier_address_line2" character varying NULL,
  "supplier_address_phone_number" character varying NULL,
  "customer_address_country" character varying NULL,
  "customer_address_postal_code" character varying NULL,
  "customer_address_state" character varying NULL,
  "customer_address_city" character varying NULL,
  "customer_address_line1" character varying NULL,
  "customer_address_line2" character varying NULL,
  "customer_address_phone_number" character varying NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "invoicing_app_external_id" character varying NULL,
  "payment_app_external_id" character varying NULL,
  "tax_app_external_id" character varying NULL,
  "supplier_name" character varying NOT NULL,
  "supplier_tax_code" character varying NULL,
  "customer_key" character varying NULL,
  "customer_name" character varying NOT NULL,
  "customer_usage_attribution" jsonb NULL,
  "number" character varying NOT NULL,
  "type" character varying NOT NULL,
  "description" character varying NULL,
  "voided_at" timestamptz NULL,
  "issued_at" timestamptz NULL,
  "sent_to_customer_at" timestamptz NULL,
  "draft_until" timestamptz NULL,
  "quantity_snapshoted_at" timestamptz NULL,
  "deletion_source" character varying NULL,
  "currency" character varying(3) NOT NULL,
  "due_at" timestamptz NULL,
  "status" character varying NOT NULL,
  "status_details_cache" jsonb NULL,
  "period_start" timestamptz NULL,
  "period_end" timestamptz NULL,
  "collection_at" timestamptz NULL,
  "payment_processing_entered_at" timestamptz NULL,
  "schema_level" bigint NOT NULL DEFAULT 2,
  "tax_app_id" character(26) NOT NULL,
  "invoicing_app_id" character(26) NOT NULL,
  "payment_app_id" character(26) NOT NULL,
  "source_billing_profile_id" character(26) NOT NULL,
  "workflow_config_id" character(26) NOT NULL,
  "customer_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billing_invoices_workflow_config_id_key" to table: "billing_invoices"
CREATE UNIQUE INDEX "billing_invoices_workflow_config_id_key" ON "billing_invoices" ("workflow_config_id");
-- Create index "billinginvoice_id" to table: "billing_invoices"
CREATE UNIQUE INDEX "billinginvoice_id" ON "billing_invoices" ("id");
-- Create index "billinginvoice_namespace" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace" ON "billing_invoices" ("namespace");
-- Create index "billinginvoice_namespace_id" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace_id" ON "billing_invoices" ("namespace", "id");
-- Create index "billinginvoice_namespace_customer_id" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace_customer_id" ON "billing_invoices" ("namespace", "customer_id");
-- Create index "billinginvoice_namespace_status" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace_status" ON "billing_invoices" ("namespace", "status");
-- Create index "billinginvoice_namespace_period_start" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace_period_start" ON "billing_invoices" ("namespace", "period_start");
-- Create index "billinginvoice_namespace_created_at" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace_created_at" ON "billing_invoices" ("namespace", "created_at");
-- Create index "billinginvoice_namespace_updated_at" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace_updated_at" ON "billing_invoices" ("namespace", "updated_at");
-- Create index "billinginvoice_namespace_issued_at" to table: "billing_invoices"
CREATE INDEX "billinginvoice_namespace_issued_at" ON "billing_invoices" ("namespace", "issued_at");
-- Create index "billinginvoice_status_details_cache" to table: "billing_invoices"
CREATE INDEX "billinginvoice_status_details_cache" ON "billing_invoices" USING GIN ("status_details_cache");
-- Create index "billinginvoice_namespace_customer_id_currency" to table: "billing_invoices"
CREATE UNIQUE INDEX "billinginvoice_namespace_customer_id_currency" ON "billing_invoices" ("namespace", "customer_id", "currency") WHERE deleted_at IS NULL and status = 'gathering';
-- Create "billing_invoice_lines" table
CREATE TABLE "billing_invoice_lines" (
  "id" character(26) NOT NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "currency" character varying(3) NOT NULL,
  "tax_config" jsonb NULL,
  "tax_behavior" character varying NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "invoicing_app_external_id" character varying NULL,
  "period_start" timestamptz NOT NULL,
  "period_end" timestamptz NOT NULL,
  "managed_by" character varying NOT NULL,
  "invoice_at" timestamptz NOT NULL,
  "override_collection_period_end" timestamptz NULL,
  "type" character varying NOT NULL,
  "status" character varying NOT NULL,
  "quantity" numeric NULL,
  "ratecard_discounts" jsonb NULL,
  "child_unique_reference_id" character varying NULL,
  "subscription_billing_period_from" timestamptz NULL,
  "subscription_billing_period_to" timestamptz NULL,
  "engine" character varying NOT NULL DEFAULT 'invoicing',
  "credits_applied" jsonb NULL,
  "invoice_id" character(26) NOT NULL,
  "usage_based_line_config_id" character(26) NULL,
  "parent_line_id" character(26) NULL,
  "split_line_group_id" character(26) NULL,
  "charge_id" character(26) NULL,
  "subscription_id" character(26) NULL,
  "subscription_item_id" character(26) NULL,
  "subscription_phase_id" character(26) NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "billing_invoice_lines_billing_invoice_lines_detailed_lines" FOREIGN KEY ("parent_line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE SET NULL,
  CONSTRAINT "billing_invoice_line_tax_behavior_consistency" CHECK (tax_behavior IS NOT DISTINCT FROM tax_config ->> 'behavior'),
  CONSTRAINT "billing_invoice_line_tax_code_consistency" CHECK ((tax_code_id::text IS NOT DISTINCT FROM tax_config ->> 'tax_code_id') AND (NULLIF(btrim(tax_config -> 'stripe' ->> 'code'), '') IS NULL OR tax_code_id IS NOT NULL))
);
-- Create index "billinginvoiceline_annotations" to table: "billing_invoice_lines"
CREATE INDEX "billinginvoiceline_annotations" ON "billing_invoice_lines" USING GIN ("annotations");
-- Create index "billinginvoiceline_id" to table: "billing_invoice_lines"
CREATE UNIQUE INDEX "billinginvoiceline_id" ON "billing_invoice_lines" ("id");
-- Create index "billinginvoiceline_namespace" to table: "billing_invoice_lines"
CREATE INDEX "billinginvoiceline_namespace" ON "billing_invoice_lines" ("namespace");
-- Create index "billinginvoiceline_namespace_id" to table: "billing_invoice_lines"
CREATE UNIQUE INDEX "billinginvoiceline_namespace_id" ON "billing_invoice_lines" ("namespace", "id");
-- Create index "billinginvoiceline_tax_code_id" to table: "billing_invoice_lines"
CREATE INDEX "billinginvoiceline_tax_code_id" ON "billing_invoice_lines" ("tax_code_id");
-- Create index "billing_invoice_lines_usage_config_id_idx" to table: "billing_invoice_lines"
CREATE INDEX "billing_invoice_lines_usage_config_id_idx" ON "billing_invoice_lines" ("usage_based_line_config_id");
-- Create index "billing_invoice_lines_parent_id_idx" to table: "billing_invoice_lines"
CREATE INDEX "billing_invoice_lines_parent_id_idx" ON "billing_invoice_lines" ("parent_line_id");
-- Create index "billinginvoiceline_namespace_invoice_id" to table: "billing_invoice_lines"
CREATE INDEX "billinginvoiceline_namespace_invoice_id" ON "billing_invoice_lines" ("namespace", "invoice_id");
-- Create index "billinginvoiceline_namespace_parent_line_id" to table: "billing_invoice_lines"
CREATE INDEX "billinginvoiceline_namespace_parent_line_id" ON "billing_invoice_lines" ("namespace", "parent_line_id");
-- Create index "billinginvoiceline_namespace_parent_line_id_child_unique_reference_id" to table: "billing_invoice_lines"
CREATE UNIQUE INDEX "billinginvoiceline_namespace_parent_line_id_child_unique_reference_id" ON "billing_invoice_lines" ("namespace", "parent_line_id", "child_unique_reference_id") WHERE child_unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create index "billinginvoiceline_namespace_subscription_id_subscription_phase_id_subscription_item_id" to table: "billing_invoice_lines"
CREATE INDEX "billinginvoiceline_namespace_subscription_id_subscription_phase_id_subscription_item_id" ON "billing_invoice_lines" ("namespace", "subscription_id", "subscription_phase_id", "subscription_item_id");
-- Create "billing_invoice_line_usage_discounts" table
CREATE TABLE "billing_invoice_line_usage_discounts" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "invoicing_app_external_id" character varying NULL,
  "child_unique_reference_id" character varying NULL,
  "description" character varying NULL,
  "reason" character varying NOT NULL,
  "quantity" numeric NOT NULL,
  "pre_line_period_quantity" numeric NULL,
  "reason_details" jsonb NULL,
  "line_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billinginvoicelineusagediscount_id" to table: "billing_invoice_line_usage_discounts"
CREATE UNIQUE INDEX "billinginvoicelineusagediscount_id" ON "billing_invoice_line_usage_discounts" ("id");
-- Create index "billinginvoicelineusagediscount_namespace" to table: "billing_invoice_line_usage_discounts"
CREATE INDEX "billinginvoicelineusagediscount_namespace" ON "billing_invoice_line_usage_discounts" ("namespace");
-- Create index "billing_invoice_line_usage_discounts_line_id_idx" to table: "billing_invoice_line_usage_discounts"
CREATE INDEX "billing_invoice_line_usage_discounts_line_id_idx" ON "billing_invoice_line_usage_discounts" ("line_id");
-- Create index "billinginvoicelineusagediscount_namespace_line_id" to table: "billing_invoice_line_usage_discounts"
CREATE INDEX "billinginvoicelineusagediscount_namespace_line_id" ON "billing_invoice_line_usage_discounts" ("namespace", "line_id");
-- Create index "billinginvoicelineusagediscount_namespace_line_id_child_unique_reference_id" to table: "billing_invoice_line_usage_discounts"
CREATE UNIQUE INDEX "billinginvoicelineusagediscount_namespace_line_id_child_unique_reference_id" ON "billing_invoice_line_usage_discounts" ("namespace", "line_id", "child_unique_reference_id") WHERE child_unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create "billing_invoice_split_line_groups" table
CREATE TABLE "billing_invoice_split_line_groups" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "currency" character varying(3) NOT NULL,
  "service_period_start" timestamptz NOT NULL,
  "service_period_end" timestamptz NOT NULL,
  "unique_reference_id" character varying NULL,
  "ratecard_discounts" jsonb NULL,
  "feature_key" character varying NULL,
  "price" jsonb NOT NULL,
  "subscription_billing_period_from" timestamptz NULL,
  "subscription_billing_period_to" timestamptz NULL,
  "charge_id" character(26) NULL,
  "subscription_id" character(26) NULL,
  "subscription_item_id" character(26) NULL,
  "subscription_phase_id" character(26) NULL,
  PRIMARY KEY ("id")
);
-- Create index "billinginvoicesplitlinegroup_id" to table: "billing_invoice_split_line_groups"
CREATE UNIQUE INDEX "billinginvoicesplitlinegroup_id" ON "billing_invoice_split_line_groups" ("id");
-- Create index "billinginvoicesplitlinegroup_namespace" to table: "billing_invoice_split_line_groups"
CREATE INDEX "billinginvoicesplitlinegroup_namespace" ON "billing_invoice_split_line_groups" ("namespace");
-- Create index "billinginvoicesplitlinegroup_namespace_id" to table: "billing_invoice_split_line_groups"
CREATE UNIQUE INDEX "billinginvoicesplitlinegroup_namespace_id" ON "billing_invoice_split_line_groups" ("namespace", "id");
-- Create index "billinginvoicesplitlinegroup_namespace_unique_reference_id" to table: "billing_invoice_split_line_groups"
CREATE UNIQUE INDEX "billinginvoicesplitlinegroup_namespace_unique_reference_id" ON "billing_invoice_split_line_groups" ("namespace", "unique_reference_id") WHERE unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create "billing_invoice_usage_based_line_configs" table
CREATE TABLE "billing_invoice_usage_based_line_configs" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "price_type" character varying NOT NULL,
  "feature_key" character varying NULL,
  "price" jsonb NOT NULL,
  "pre_line_period_quantity" numeric NULL,
  "metered_pre_line_period_quantity" numeric NULL,
  "metered_quantity" numeric NULL,
  "unit_config" jsonb NULL,
  PRIMARY KEY ("id")
);
-- Create index "billinginvoiceusagebasedlineconfig_namespace" to table: "billing_invoice_usage_based_line_configs"
CREATE INDEX "billinginvoiceusagebasedlineconfig_namespace" ON "billing_invoice_usage_based_line_configs" ("namespace");
-- Create index "billinginvoiceusagebasedlineconfig_id" to table: "billing_invoice_usage_based_line_configs"
CREATE UNIQUE INDEX "billinginvoiceusagebasedlineconfig_id" ON "billing_invoice_usage_based_line_configs" ("id");
-- Create "billing_invoice_validation_issues" table
CREATE TABLE "billing_invoice_validation_issues" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "severity" character varying NOT NULL,
  "code" character varying NULL,
  "message" character varying NOT NULL,
  "path" character varying NULL,
  "component" character varying NOT NULL,
  "attributes" jsonb NULL,
  "dedupe_hash" bytea NOT NULL,
  "invoice_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billinginvoicevalidationissue_id" to table: "billing_invoice_validation_issues"
CREATE UNIQUE INDEX "billinginvoicevalidationissue_id" ON "billing_invoice_validation_issues" ("id");
-- Create index "billinginvoicevalidationissue_namespace" to table: "billing_invoice_validation_issues"
CREATE INDEX "billinginvoicevalidationissue_namespace" ON "billing_invoice_validation_issues" ("namespace");
-- Create index "billinginvoicevalidationissue_namespace_invoice_id_dedupe_hash" to table: "billing_invoice_validation_issues"
CREATE UNIQUE INDEX "billinginvoicevalidationissue_namespace_invoice_id_dedupe_hash" ON "billing_invoice_validation_issues" ("namespace", "invoice_id", "dedupe_hash");
-- Create "billing_invoice_write_schema_levels" table
CREATE TABLE "billing_invoice_write_schema_levels" (
  "id" character varying NOT NULL,
  "schema_level" bigint NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billinginvoicewriteschemalevel_id" to table: "billing_invoice_write_schema_levels"
CREATE UNIQUE INDEX "billinginvoicewriteschemalevel_id" ON "billing_invoice_write_schema_levels" ("id");
-- Create "billing_profiles" table
CREATE TABLE "billing_profiles" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "supplier_address_country" character varying NULL,
  "supplier_address_postal_code" character varying NULL,
  "supplier_address_state" character varying NULL,
  "supplier_address_city" character varying NULL,
  "supplier_address_line1" character varying NULL,
  "supplier_address_line2" character varying NULL,
  "supplier_address_phone_number" character varying NULL,
  "default" boolean NOT NULL DEFAULT false,
  "supplier_name" character varying NOT NULL,
  "supplier_tax_code" character varying NULL,
  "tax_app_id" character(26) NOT NULL,
  "invoicing_app_id" character(26) NOT NULL,
  "payment_app_id" character(26) NOT NULL,
  "workflow_config_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billing_profiles_workflow_config_id_key" to table: "billing_profiles"
CREATE UNIQUE INDEX "billing_profiles_workflow_config_id_key" ON "billing_profiles" ("workflow_config_id");
-- Create index "billingprofile_id" to table: "billing_profiles"
CREATE UNIQUE INDEX "billingprofile_id" ON "billing_profiles" ("id");
-- Create index "billingprofile_namespace" to table: "billing_profiles"
CREATE INDEX "billingprofile_namespace" ON "billing_profiles" ("namespace");
-- Create index "billingprofile_namespace_id" to table: "billing_profiles"
CREATE UNIQUE INDEX "billingprofile_namespace_id" ON "billing_profiles" ("namespace", "id");
-- Create index "billingprofile_namespace_default" to table: "billing_profiles"
CREATE UNIQUE INDEX "billingprofile_namespace_default" ON "billing_profiles" ("namespace", "default") WHERE "default" AND deleted_at IS NULL;
-- Create "billing_sequence_numbers" table
CREATE TABLE "billing_sequence_numbers" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "namespace" character varying NOT NULL,
  "scope" character varying NOT NULL,
  "last" numeric NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billingsequencenumbers_namespace" to table: "billing_sequence_numbers"
CREATE INDEX "billingsequencenumbers_namespace" ON "billing_sequence_numbers" ("namespace");
-- Create index "billingsequencenumbers_namespace_scope" to table: "billing_sequence_numbers"
CREATE UNIQUE INDEX "billingsequencenumbers_namespace_scope" ON "billing_sequence_numbers" ("namespace", "scope");
-- Create "billing_standard_invoice_detailed_lines" table
CREATE TABLE "billing_standard_invoice_detailed_lines" (
  "id" character(26) NOT NULL,
  "currency" character varying(3) NULL,
  "service_period_start" timestamptz NOT NULL,
  "service_period_end" timestamptz NOT NULL,
  "quantity" numeric NOT NULL,
  "invoicing_app_external_id" character varying NULL,
  "child_unique_reference_id" character varying NOT NULL,
  "per_unit_amount" numeric NOT NULL,
  "category" character varying NOT NULL DEFAULT 'regular',
  "payment_term" character varying NOT NULL DEFAULT 'in_advance',
  "index" bigint NULL,
  "credits_applied" jsonb NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "invoice_id" character(26) NOT NULL,
  "parent_line_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "child_unique_reference_id_not_empty" CHECK (child_unique_reference_id <> '')
);
-- Create index "billingstandardinvoicedetailedline_annotations" to table: "billing_standard_invoice_detailed_lines"
CREATE INDEX "billingstandardinvoicedetailedline_annotations" ON "billing_standard_invoice_detailed_lines" USING GIN ("annotations");
-- Create index "billingstandardinvoicedetailedline_id" to table: "billing_standard_invoice_detailed_lines"
CREATE UNIQUE INDEX "billingstandardinvoicedetailedline_id" ON "billing_standard_invoice_detailed_lines" ("id");
-- Create index "billingstandardinvoicedetailedline_namespace" to table: "billing_standard_invoice_detailed_lines"
CREATE INDEX "billingstandardinvoicedetailedline_namespace" ON "billing_standard_invoice_detailed_lines" ("namespace");
-- Create index "billingstandardinvoicedetailedline_namespace_id" to table: "billing_standard_invoice_detailed_lines"
CREATE UNIQUE INDEX "billingstandardinvoicedetailedline_namespace_id" ON "billing_standard_invoice_detailed_lines" ("namespace", "id");
-- Create index "billing_std_invoice_detailed_lines_parent_id_idx" to table: "billing_standard_invoice_detailed_lines"
CREATE INDEX "billing_std_invoice_detailed_lines_parent_id_idx" ON "billing_standard_invoice_detailed_lines" ("parent_line_id");
-- Create index "billingstandardinvoicedetailedline_namespace_invoice_id" to table: "billing_standard_invoice_detailed_lines"
CREATE INDEX "billingstandardinvoicedetailedline_namespace_invoice_id" ON "billing_standard_invoice_detailed_lines" ("namespace", "invoice_id");
-- Create index "billingstandardinvoicedetailedline_namespace_parent_line_id" to table: "billing_standard_invoice_detailed_lines"
CREATE INDEX "billingstandardinvoicedetailedline_namespace_parent_line_id" ON "billing_standard_invoice_detailed_lines" ("namespace", "parent_line_id");
-- Create index "billingstdinvdetailedline_ns_parent_child_id" to table: "billing_standard_invoice_detailed_lines"
CREATE UNIQUE INDEX "billingstdinvdetailedline_ns_parent_child_id" ON "billing_standard_invoice_detailed_lines" ("namespace", "parent_line_id", "child_unique_reference_id") WHERE deleted_at IS NULL;
-- Create "billing_standard_invoice_detailed_line_amount_discounts" table
CREATE TABLE "billing_standard_invoice_detailed_line_amount_discounts" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "invoicing_app_external_id" character varying NULL,
  "child_unique_reference_id" character varying NULL,
  "description" character varying NULL,
  "reason" character varying NOT NULL,
  "amount" numeric NOT NULL,
  "rounding_amount" numeric NULL,
  "source_discount" jsonb NULL,
  "line_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "billingstandardinvoicedetailedlineamountdiscount_id" to table: "billing_standard_invoice_detailed_line_amount_discounts"
CREATE UNIQUE INDEX "billingstandardinvoicedetailedlineamountdiscount_id" ON "billing_standard_invoice_detailed_line_amount_discounts" ("id");
-- Create index "billingstandardinvoicedetailedlineamountdiscount_namespace" to table: "billing_standard_invoice_detailed_line_amount_discounts"
CREATE INDEX "billingstandardinvoicedetailedlineamountdiscount_namespace" ON "billing_standard_invoice_detailed_line_amount_discounts" ("namespace");
-- Create index "billing_std_invoice_detail_amount_discounts_line_id_idx" to table: "billing_standard_invoice_detailed_line_amount_discounts"
CREATE INDEX "billing_std_invoice_detail_amount_discounts_line_id_idx" ON "billing_standard_invoice_detailed_line_amount_discounts" ("line_id");
-- Create index "billingstandardinvoicedetailedlineamountdiscount_namespace_line_id" to table: "billing_standard_invoice_detailed_line_amount_discounts"
CREATE INDEX "billingstandardinvoicedetailedlineamountdiscount_namespace_line_id" ON "billing_standard_invoice_detailed_line_amount_discounts" ("namespace", "line_id");
-- Create index "billingstdinvdetailedlineamntdiscount_ns_parent_child_id" to table: "billing_standard_invoice_detailed_line_amount_discounts"
CREATE UNIQUE INDEX "billingstdinvdetailedlineamntdiscount_ns_parent_child_id" ON "billing_standard_invoice_detailed_line_amount_discounts" ("namespace", "line_id", "child_unique_reference_id") WHERE child_unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create "billing_workflow_configs" table
CREATE TABLE "billing_workflow_configs" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "tax_behavior" character varying NULL,
  "collection_alignment" character varying NOT NULL,
  "anchored_alignment_detail" jsonb NULL,
  "line_collection_period" character varying NOT NULL,
  "invoice_auto_advance" boolean NOT NULL,
  "invoice_draft_period" character varying NOT NULL,
  "invoice_due_after" character varying NOT NULL,
  "invoice_collection_method" character varying NOT NULL,
  "invoice_progressive_billing" boolean NOT NULL,
  "subscription_end_proration_mode" character varying NOT NULL DEFAULT 'bill_actual_period',
  "invoice_default_tax_settings" jsonb NULL,
  "tax_enabled" boolean NOT NULL DEFAULT true,
  "tax_enforced" boolean NOT NULL DEFAULT false,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "billing_workflow_config_tax_behavior_consistency" CHECK (tax_behavior IS NOT DISTINCT FROM invoice_default_tax_settings ->> 'behavior'),
  CONSTRAINT "billing_workflow_config_tax_code_consistency" CHECK ((tax_code_id::text IS NOT DISTINCT FROM invoice_default_tax_settings ->> 'tax_code_id') AND (NULLIF(btrim(invoice_default_tax_settings -> 'stripe' ->> 'code'), '') IS NULL OR tax_code_id IS NOT NULL))
);
-- Create index "billingworkflowconfig_id" to table: "billing_workflow_configs"
CREATE UNIQUE INDEX "billingworkflowconfig_id" ON "billing_workflow_configs" ("id");
-- Create index "billingworkflowconfig_namespace" to table: "billing_workflow_configs"
CREATE INDEX "billingworkflowconfig_namespace" ON "billing_workflow_configs" ("namespace");
-- Create index "billingworkflowconfig_tax_code_id" to table: "billing_workflow_configs"
CREATE INDEX "billingworkflowconfig_tax_code_id" ON "billing_workflow_configs" ("tax_code_id");
-- Create index "billingworkflowconfig_namespace_id" to table: "billing_workflow_configs"
CREATE INDEX "billingworkflowconfig_namespace_id" ON "billing_workflow_configs" ("namespace", "id");
-- Create "charges" table
CREATE TABLE "charges" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "unique_reference_id" character varying NULL,
  "type" character varying NOT NULL,
  "charge_credit_purchase_id" character(26) NULL,
  "charge_flat_fee_id" character(26) NULL,
  "charge_usage_based_id" character(26) NULL,
  PRIMARY KEY ("id")
);
-- Create index "charges_charge_credit_purchase_id_key" to table: "charges"
CREATE UNIQUE INDEX "charges_charge_credit_purchase_id_key" ON "charges" ("charge_credit_purchase_id");
-- Create index "charges_charge_flat_fee_id_key" to table: "charges"
CREATE UNIQUE INDEX "charges_charge_flat_fee_id_key" ON "charges" ("charge_flat_fee_id");
-- Create index "charges_charge_usage_based_id_key" to table: "charges"
CREATE UNIQUE INDEX "charges_charge_usage_based_id_key" ON "charges" ("charge_usage_based_id");
-- Create index "charge_namespace" to table: "charges"
CREATE INDEX "charge_namespace" ON "charges" ("namespace");
-- Create index "charge_id" to table: "charges"
CREATE UNIQUE INDEX "charge_id" ON "charges" ("id");
-- Create index "charge_namespace_unique_reference_id" to table: "charges"
CREATE UNIQUE INDEX "charge_namespace_unique_reference_id" ON "charges" ("namespace", "unique_reference_id") WHERE unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create "charge_credit_purchases" table
CREATE TABLE "charge_credit_purchases" (
  "id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "billing_period_from" timestamptz NOT NULL,
  "billing_period_to" timestamptz NOT NULL,
  "full_service_period_from" timestamptz NOT NULL,
  "full_service_period_to" timestamptz NOT NULL,
  "status" character varying NOT NULL,
  "unique_reference_id" character varying NULL,
  "currency" character varying(3) NULL,
  "managed_by" character varying NOT NULL,
  "subscription_plan" jsonb NULL,
  "advance_after" timestamptz NULL,
  "tax_behavior" character varying NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "validation_issues" jsonb NULL,
  "schema_level" smallint NOT NULL DEFAULT 2,
  "fiat_cost_basis" numeric NULL,
  "settlement_type" character varying NULL,
  "initial_payment_settlement_status" character varying NULL,
  "credit_amount" numeric NOT NULL,
  "effective_at" timestamptz NULL,
  "expires_at" timestamptz NULL,
  "priority" bigint NULL,
  "filters" jsonb NOT NULL,
  "feature_filters" text[] NULL,
  "settlement" jsonb NULL,
  "status_detailed" character varying NOT NULL,
  "key" character varying NULL,
  "voided_at" timestamptz NULL,
  "cost_basis_id" character(26) NULL,
  "custom_currency_id" character(26) NULL,
  "customer_id" character(26) NOT NULL,
  "subscription_id" character(26) NULL,
  "subscription_item_id" character(26) NULL,
  "subscription_phase_id" character(26) NULL,
  "tax_code_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "cost_basis_schema_level_settlement_fields" CHECK (settlement_type IS NOT NULL AND ((settlement_type = 'external' AND initial_payment_settlement_status IS NOT NULL) OR (settlement_type IN ('invoice', 'promotional') AND initial_payment_settlement_status IS NULL))),
  CONSTRAINT "currency_not_empty" CHECK (currency IS NULL OR currency <> ''),
  CONSTRAINT "currency_reference" CHECK ((currency IS NULL) <> (custom_currency_id IS NULL)),
  CONSTRAINT "fiat_cost_basis_positive" CHECK (fiat_cost_basis IS NULL OR (fiat_cost_basis <> 'NaN'::numeric AND fiat_cost_basis > 0)),
  CONSTRAINT "initial_payment_settlement_status" CHECK (initial_payment_settlement_status IS NULL OR initial_payment_settlement_status IN ('created', 'authorized', 'settled')),
  CONSTRAINT "schema_level" CHECK (schema_level = 2),
  CONSTRAINT "settlement_type" CHECK (settlement_type IS NULL OR settlement_type IN ('invoice', 'external', 'promotional'))
);
-- Create index "chargecreditpurchase_namespace_customer_id_unique_reference_id" to table: "charge_credit_purchases"
CREATE UNIQUE INDEX "chargecreditpurchase_namespace_customer_id_unique_reference_id" ON "charge_credit_purchases" ("namespace", "customer_id", "unique_reference_id") WHERE unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create index "chargecreditpurchase_annotations" to table: "charge_credit_purchases"
CREATE INDEX "chargecreditpurchase_annotations" ON "charge_credit_purchases" USING GIN ("annotations");
-- Create index "chargecreditpurchase_id" to table: "charge_credit_purchases"
CREATE UNIQUE INDEX "chargecreditpurchase_id" ON "charge_credit_purchases" ("id");
-- Create index "chargecreditpurchase_namespace" to table: "charge_credit_purchases"
CREATE INDEX "chargecreditpurchase_namespace" ON "charge_credit_purchases" ("namespace");
-- Create index "chargecreditpurchase_namespace_id" to table: "charge_credit_purchases"
CREATE UNIQUE INDEX "chargecreditpurchase_namespace_id" ON "charge_credit_purchases" ("namespace", "id");
-- Create index "chargecreditpurchases_tax_code_id" to table: "charge_credit_purchases"
CREATE INDEX "chargecreditpurchases_tax_code_id" ON "charge_credit_purchases" ("tax_code_id");
-- Create index "chargecreditpurchases_cost_basis_id" to table: "charge_credit_purchases"
CREATE UNIQUE INDEX "chargecreditpurchases_cost_basis_id" ON "charge_credit_purchases" ("cost_basis_id");
-- Create index "chargecreditpurchase_namespace_customer_id_key" to table: "charge_credit_purchases"
CREATE UNIQUE INDEX "chargecreditpurchase_namespace_customer_id_key" ON "charge_credit_purchases" ("namespace", "customer_id", "key") WHERE key IS NOT NULL AND deleted_at IS NULL;
-- Create "charge_credit_purchase_cost_bases" table
CREATE TABLE "charge_credit_purchase_cost_bases" (
  "id" character(26) NOT NULL,
  "mode" character varying NOT NULL,
  "fiat_currency" character varying(3) NOT NULL,
  "manual_rate" numeric NULL,
  "resolved_cost_basis" numeric NULL,
  "resolved_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "currency_cost_basis_id" character(26) NULL,
  "resolved_cost_basis_id" character(26) NULL,
  "currency_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fiat_currency_not_empty" CHECK (fiat_currency <> ''),
  CONSTRAINT "resolved_cost_basis_positive" CHECK (resolved_cost_basis IS NULL OR resolved_cost_basis > 0),
  CONSTRAINT "state" CHECK (
				(
					mode = 'dynamic'
					AND currency_cost_basis_id IS NULL
					AND manual_rate IS NULL
					AND (
						(resolved_cost_basis_id IS NULL AND resolved_cost_basis IS NULL AND resolved_at IS NULL)
						OR (resolved_cost_basis_id IS NOT NULL AND resolved_cost_basis IS NOT NULL AND resolved_at IS NOT NULL)
					)
				)
				OR (
					mode = 'pinned'
					AND currency_cost_basis_id IS NOT NULL
					AND resolved_cost_basis_id IS NOT NULL
					AND resolved_cost_basis_id = currency_cost_basis_id
					AND manual_rate IS NULL
					AND resolved_cost_basis IS NOT NULL
					AND resolved_at IS NOT NULL
				)
				OR (
					mode = 'manual'
					AND currency_cost_basis_id IS NULL
					AND resolved_cost_basis_id IS NULL
					AND manual_rate > 0
					AND resolved_cost_basis IS NOT NULL
					AND resolved_at IS NOT NULL
				)
			)
);
-- Create index "chargecreditpurchasecostbasis_currency_cost_basis_id" to table: "charge_credit_purchase_cost_bases"
CREATE INDEX "chargecreditpurchasecostbasis_currency_cost_basis_id" ON "charge_credit_purchase_cost_bases" ("currency_cost_basis_id");
-- Create index "chargecreditpurchasecostbasis_resolved_cost_basis_id" to table: "charge_credit_purchase_cost_bases"
CREATE INDEX "chargecreditpurchasecostbasis_resolved_cost_basis_id" ON "charge_credit_purchase_cost_bases" ("resolved_cost_basis_id");
-- Create index "chargecreditpurchasecostbasis_currency_id" to table: "charge_credit_purchase_cost_bases"
CREATE INDEX "chargecreditpurchasecostbasis_currency_id" ON "charge_credit_purchase_cost_bases" ("currency_id");
-- Create index "chargecreditpurchasecostbasis_namespace" to table: "charge_credit_purchase_cost_bases"
CREATE INDEX "chargecreditpurchasecostbasis_namespace" ON "charge_credit_purchase_cost_bases" ("namespace");
-- Create index "chargecreditpurchasecostbasis_id" to table: "charge_credit_purchase_cost_bases"
CREATE UNIQUE INDEX "chargecreditpurchasecostbasis_id" ON "charge_credit_purchase_cost_bases" ("id");
-- Create "charge_credit_purchase_credit_grants" table
CREATE TABLE "charge_credit_purchase_credit_grants" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "transaction_group_id" character(26) NOT NULL,
  "granted_at" timestamptz NOT NULL,
  "charge_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_credit_purchase_credit_grants_charge_id_key" to table: "charge_credit_purchase_credit_grants"
CREATE UNIQUE INDEX "charge_credit_purchase_credit_grants_charge_id_key" ON "charge_credit_purchase_credit_grants" ("charge_id");
-- Create index "chargecreditpurchasecreditgrant_namespace" to table: "charge_credit_purchase_credit_grants"
CREATE INDEX "chargecreditpurchasecreditgrant_namespace" ON "charge_credit_purchase_credit_grants" ("namespace");
-- Create index "chargecreditpurchasecreditgrant_id" to table: "charge_credit_purchase_credit_grants"
CREATE UNIQUE INDEX "chargecreditpurchasecreditgrant_id" ON "charge_credit_purchase_credit_grants" ("id");
-- Create index "chargecreditpurchasecreditgrant_namespace_charge_id" to table: "charge_credit_purchase_credit_grants"
CREATE UNIQUE INDEX "chargecreditpurchasecreditgrant_namespace_charge_id" ON "charge_credit_purchase_credit_grants" ("namespace", "charge_id");
-- Create "charge_credit_purchase_external_payments" table
CREATE TABLE "charge_credit_purchase_external_payments" (
  "id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "status" character varying NOT NULL,
  "amount" numeric NOT NULL,
  "authorized_transaction_group_id" character(26) NULL,
  "authorized_at" timestamptz NULL,
  "settled_transaction_group_id" character(26) NULL,
  "settled_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "charge_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_credit_purchase_external_payments_charge_id_key" to table: "charge_credit_purchase_external_payments"
CREATE UNIQUE INDEX "charge_credit_purchase_external_payments_charge_id_key" ON "charge_credit_purchase_external_payments" ("charge_id");
-- Create index "chargecreditpurchaseexternalpayment_namespace" to table: "charge_credit_purchase_external_payments"
CREATE INDEX "chargecreditpurchaseexternalpayment_namespace" ON "charge_credit_purchase_external_payments" ("namespace");
-- Create index "chargecreditpurchaseexternalpayment_id" to table: "charge_credit_purchase_external_payments"
CREATE UNIQUE INDEX "chargecreditpurchaseexternalpayment_id" ON "charge_credit_purchase_external_payments" ("id");
-- Create index "chargecreditpurchaseexternalpayment_annotations" to table: "charge_credit_purchase_external_payments"
CREATE INDEX "chargecreditpurchaseexternalpayment_annotations" ON "charge_credit_purchase_external_payments" USING GIN ("annotations");
-- Create "charge_credit_purchase_invoiced_payments" table
CREATE TABLE "charge_credit_purchase_invoiced_payments" (
  "id" character(26) NOT NULL,
  "invoice_id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "status" character varying NOT NULL,
  "amount" numeric NOT NULL,
  "authorized_transaction_group_id" character(26) NULL,
  "authorized_at" timestamptz NULL,
  "settled_transaction_group_id" character(26) NULL,
  "settled_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "line_id" character(26) NOT NULL,
  "charge_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_credit_purchase_invoiced_payments_line_id_key" to table: "charge_credit_purchase_invoiced_payments"
CREATE UNIQUE INDEX "charge_credit_purchase_invoiced_payments_line_id_key" ON "charge_credit_purchase_invoiced_payments" ("line_id");
-- Create index "charge_credit_purchase_invoiced_payments_charge_id_key" to table: "charge_credit_purchase_invoiced_payments"
CREATE UNIQUE INDEX "charge_credit_purchase_invoiced_payments_charge_id_key" ON "charge_credit_purchase_invoiced_payments" ("charge_id");
-- Create index "chargecreditpurchaseinvoicedpayment_namespace" to table: "charge_credit_purchase_invoiced_payments"
CREATE INDEX "chargecreditpurchaseinvoicedpayment_namespace" ON "charge_credit_purchase_invoiced_payments" ("namespace");
-- Create index "chargecreditpurchaseinvoicedpayment_id" to table: "charge_credit_purchase_invoiced_payments"
CREATE UNIQUE INDEX "chargecreditpurchaseinvoicedpayment_id" ON "charge_credit_purchase_invoiced_payments" ("id");
-- Create index "chargecreditpurchaseinvoicedpayment_annotations" to table: "charge_credit_purchase_invoiced_payments"
CREATE INDEX "chargecreditpurchaseinvoicedpayment_annotations" ON "charge_credit_purchase_invoiced_payments" USING GIN ("annotations");
-- Create index "chargecreditpurchaseinvoicedpayment_namespace_charge_id" to table: "charge_credit_purchase_invoiced_payments"
CREATE UNIQUE INDEX "chargecreditpurchaseinvoicedpayment_namespace_charge_id" ON "charge_credit_purchase_invoiced_payments" ("namespace", "charge_id");
-- Create "charge_flat_fees" table
CREATE TABLE "charge_flat_fees" (
  "id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "billing_period_from" timestamptz NOT NULL,
  "billing_period_to" timestamptz NOT NULL,
  "full_service_period_from" timestamptz NOT NULL,
  "full_service_period_to" timestamptz NOT NULL,
  "status" character varying NOT NULL,
  "unique_reference_id" character varying NULL,
  "currency" character varying(3) NULL,
  "managed_by" character varying NOT NULL,
  "subscription_plan" jsonb NULL,
  "advance_after" timestamptz NULL,
  "tax_behavior" character varying NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "validation_issues" jsonb NULL,
  "payment_term" character varying NOT NULL,
  "invoice_at" timestamptz NOT NULL,
  "settlement_mode" character varying NOT NULL,
  "intent_deleted_at" timestamptz NULL,
  "discounts" jsonb NULL,
  "pro_rating" character varying NOT NULL,
  "feature_key" character varying NULL,
  "amount_before_proration" numeric NOT NULL,
  "amount_after_proration" numeric NOT NULL,
  "status_detailed" character varying NOT NULL,
  "current_realization_run_id" character(26) NULL,
  "cost_basis_id" character(26) NULL,
  "custom_currency_id" character(26) NULL,
  "customer_id" character(26) NOT NULL,
  "feature_id" character(26) NULL,
  "subscription_id" character(26) NULL,
  "subscription_item_id" character(26) NULL,
  "subscription_phase_id" character(26) NULL,
  "tax_code_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "currency_not_empty" CHECK (currency IS NULL OR currency <> ''),
  CONSTRAINT "currency_reference" CHECK ((currency IS NULL) <> (custom_currency_id IS NULL))
);
-- Create index "chargeflatfee_namespace_customer_id_unique_reference_id" to table: "charge_flat_fees"
CREATE UNIQUE INDEX "chargeflatfee_namespace_customer_id_unique_reference_id" ON "charge_flat_fees" ("namespace", "customer_id", "unique_reference_id") WHERE unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create index "chargeflatfee_annotations" to table: "charge_flat_fees"
CREATE INDEX "chargeflatfee_annotations" ON "charge_flat_fees" USING GIN ("annotations");
-- Create index "chargeflatfee_id" to table: "charge_flat_fees"
CREATE UNIQUE INDEX "chargeflatfee_id" ON "charge_flat_fees" ("id");
-- Create index "chargeflatfee_namespace" to table: "charge_flat_fees"
CREATE INDEX "chargeflatfee_namespace" ON "charge_flat_fees" ("namespace");
-- Create index "chargeflatfee_namespace_id" to table: "charge_flat_fees"
CREATE UNIQUE INDEX "chargeflatfee_namespace_id" ON "charge_flat_fees" ("namespace", "id");
-- Create index "chargeflatfees_tax_code_id" to table: "charge_flat_fees"
CREATE INDEX "chargeflatfees_tax_code_id" ON "charge_flat_fees" ("tax_code_id");
-- Create index "chargeflatfees_cost_basis_id" to table: "charge_flat_fees"
CREATE UNIQUE INDEX "chargeflatfees_cost_basis_id" ON "charge_flat_fees" ("cost_basis_id");
-- Create "charge_flat_fee_cost_bases" table
CREATE TABLE "charge_flat_fee_cost_bases" (
  "id" character(26) NOT NULL,
  "mode" character varying NOT NULL,
  "fiat_currency" character varying(3) NOT NULL,
  "manual_rate" numeric NULL,
  "resolved_cost_basis" numeric NULL,
  "resolved_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "currency_cost_basis_id" character(26) NULL,
  "resolved_cost_basis_id" character(26) NULL,
  "currency_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fiat_currency_not_empty" CHECK (fiat_currency <> ''),
  CONSTRAINT "resolved_cost_basis_positive" CHECK (resolved_cost_basis IS NULL OR resolved_cost_basis > 0),
  CONSTRAINT "state" CHECK (
				(
					mode = 'dynamic'
					AND currency_cost_basis_id IS NULL
					AND manual_rate IS NULL
					AND (
						(resolved_cost_basis_id IS NULL AND resolved_cost_basis IS NULL AND resolved_at IS NULL)
						OR (resolved_cost_basis_id IS NOT NULL AND resolved_cost_basis IS NOT NULL AND resolved_at IS NOT NULL)
					)
				)
				OR (
					mode = 'pinned'
					AND currency_cost_basis_id IS NOT NULL
					AND resolved_cost_basis_id IS NOT NULL
					AND resolved_cost_basis_id = currency_cost_basis_id
					AND manual_rate IS NULL
					AND resolved_cost_basis IS NOT NULL
					AND resolved_at IS NOT NULL
				)
				OR (
					mode = 'manual'
					AND currency_cost_basis_id IS NULL
					AND resolved_cost_basis_id IS NULL
					AND manual_rate > 0
					AND resolved_cost_basis IS NOT NULL
					AND resolved_at IS NOT NULL
				)
			)
);
-- Create index "chargeflatfeecostbasis_currency_cost_basis_id" to table: "charge_flat_fee_cost_bases"
CREATE INDEX "chargeflatfeecostbasis_currency_cost_basis_id" ON "charge_flat_fee_cost_bases" ("currency_cost_basis_id");
-- Create index "chargeflatfeecostbasis_resolved_cost_basis_id" to table: "charge_flat_fee_cost_bases"
CREATE INDEX "chargeflatfeecostbasis_resolved_cost_basis_id" ON "charge_flat_fee_cost_bases" ("resolved_cost_basis_id");
-- Create index "chargeflatfeecostbasis_currency_id" to table: "charge_flat_fee_cost_bases"
CREATE INDEX "chargeflatfeecostbasis_currency_id" ON "charge_flat_fee_cost_bases" ("currency_id");
-- Create index "chargeflatfeecostbasis_namespace" to table: "charge_flat_fee_cost_bases"
CREATE INDEX "chargeflatfeecostbasis_namespace" ON "charge_flat_fee_cost_bases" ("namespace");
-- Create index "chargeflatfeecostbasis_id" to table: "charge_flat_fee_cost_bases"
CREATE UNIQUE INDEX "chargeflatfeecostbasis_id" ON "charge_flat_fee_cost_bases" ("id");
-- Create "charge_flat_fee_overrides" table
CREATE TABLE "charge_flat_fee_overrides" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "metadata" jsonb NULL,
  "tax_behavior" character varying NULL,
  "intent_deleted_at" timestamptz NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "full_service_period_from" timestamptz NOT NULL,
  "full_service_period_to" timestamptz NOT NULL,
  "billing_period_from" timestamptz NOT NULL,
  "billing_period_to" timestamptz NOT NULL,
  "invoice_at" timestamptz NOT NULL,
  "feature_key" character varying NULL,
  "payment_term" character varying NOT NULL,
  "pro_rating" jsonb NOT NULL,
  "amount_before_proration" numeric NOT NULL,
  "discounts" jsonb NULL,
  "percentage_discounts" jsonb NULL,
  "charge_id" character(26) NOT NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_flat_fee_overrides_charge_id_key" to table: "charge_flat_fee_overrides"
CREATE UNIQUE INDEX "charge_flat_fee_overrides_charge_id_key" ON "charge_flat_fee_overrides" ("charge_id");
-- Create index "chargeflatfeeoverride_namespace" to table: "charge_flat_fee_overrides"
CREATE INDEX "chargeflatfeeoverride_namespace" ON "charge_flat_fee_overrides" ("namespace");
-- Create index "chargeflatfeeoverride_id" to table: "charge_flat_fee_overrides"
CREATE UNIQUE INDEX "chargeflatfeeoverride_id" ON "charge_flat_fee_overrides" ("id");
-- Create index "chargeflatfeeoverrides_tax_code_id" to table: "charge_flat_fee_overrides"
CREATE INDEX "chargeflatfeeoverrides_tax_code_id" ON "charge_flat_fee_overrides" ("tax_code_id");
-- Create index "chargeflatfeeoverride_namespace_charge_id" to table: "charge_flat_fee_overrides"
CREATE UNIQUE INDEX "chargeflatfeeoverride_namespace_charge_id" ON "charge_flat_fee_overrides" ("namespace", "charge_id");
-- Create "charge_flat_fee_runs" table
CREATE TABLE "charge_flat_fee_runs" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "type" character varying NOT NULL,
  "initial_type" character varying NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "amount_after_proration" numeric NOT NULL,
  "no_fiat_transaction_required" boolean NOT NULL,
  "fiat_overage_credit_allocation_completed" boolean NOT NULL DEFAULT false,
  "immutable" boolean NOT NULL,
  "invoice_id" character(26) NULL,
  "line_id" character(26) NULL,
  "charge_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_flat_fee_runs_line_id_key" to table: "charge_flat_fee_runs"
CREATE UNIQUE INDEX "charge_flat_fee_runs_line_id_key" ON "charge_flat_fee_runs" ("line_id");
-- Create index "chargeflatfeerun_namespace" to table: "charge_flat_fee_runs"
CREATE INDEX "chargeflatfeerun_namespace" ON "charge_flat_fee_runs" ("namespace");
-- Create index "chargeflatfeerun_id" to table: "charge_flat_fee_runs"
CREATE UNIQUE INDEX "chargeflatfeerun_id" ON "charge_flat_fee_runs" ("id");
-- Create index "chargeflatfeerun_namespace_charge_id" to table: "charge_flat_fee_runs"
CREATE INDEX "chargeflatfeerun_namespace_charge_id" ON "charge_flat_fee_runs" ("namespace", "charge_id");
-- Create "charge_flat_fee_run_credit_allocations" table
CREATE TABLE "charge_flat_fee_run_credit_allocations" (
  "id" character(26) NOT NULL,
  "amount" numeric NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "ledger_transaction_group_id" character(26) NOT NULL,
  "sort_hint" bigint NOT NULL,
  "type" character varying NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "line_id" character(26) NULL,
  "run_id" character(26) NOT NULL,
  "corrects_realization_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "charge_flat_fee_run_credit_allocations_charge_flat_fee_run_cred" FOREIGN KEY ("corrects_realization_id") REFERENCES "charge_flat_fee_run_credit_allocations" ("id") ON DELETE SET NULL
);
-- Create index "chargeflatfeeruncreditallocations_namespace" to table: "charge_flat_fee_run_credit_allocations"
CREATE INDEX "chargeflatfeeruncreditallocations_namespace" ON "charge_flat_fee_run_credit_allocations" ("namespace");
-- Create index "chargeflatfeeruncreditallocations_id" to table: "charge_flat_fee_run_credit_allocations"
CREATE UNIQUE INDEX "chargeflatfeeruncreditallocations_id" ON "charge_flat_fee_run_credit_allocations" ("id");
-- Create index "chargeflatfeeruncreditallocations_annotations" to table: "charge_flat_fee_run_credit_allocations"
CREATE INDEX "chargeflatfeeruncreditallocations_annotations" ON "charge_flat_fee_run_credit_allocations" USING GIN ("annotations");
-- Create "charge_flat_fee_run_detailed_lines" table
CREATE TABLE "charge_flat_fee_run_detailed_lines" (
  "id" character(26) NOT NULL,
  "amount_discounts" jsonb NULL,
  "currency" character varying(3) NULL,
  "service_period_start" timestamptz NOT NULL,
  "service_period_end" timestamptz NOT NULL,
  "quantity" numeric NOT NULL,
  "invoicing_app_external_id" character varying NULL,
  "child_unique_reference_id" character varying NOT NULL,
  "per_unit_amount" numeric NOT NULL,
  "category" character varying NOT NULL DEFAULT 'regular',
  "payment_term" character varying NOT NULL DEFAULT 'in_advance',
  "index" bigint NULL,
  "credits_applied" jsonb NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "pricer_reference_id" character varying NOT NULL,
  "run_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "child_unique_reference_id_not_empty" CHECK (child_unique_reference_id <> '')
);
-- Create index "chargeflatfeerundetailedline_annotations" to table: "charge_flat_fee_run_detailed_lines"
CREATE INDEX "chargeflatfeerundetailedline_annotations" ON "charge_flat_fee_run_detailed_lines" USING GIN ("annotations");
-- Create index "chargeflatfeerundetailedline_id" to table: "charge_flat_fee_run_detailed_lines"
CREATE UNIQUE INDEX "chargeflatfeerundetailedline_id" ON "charge_flat_fee_run_detailed_lines" ("id");
-- Create index "chargeflatfeerundetailedline_namespace" to table: "charge_flat_fee_run_detailed_lines"
CREATE INDEX "chargeflatfeerundetailedline_namespace" ON "charge_flat_fee_run_detailed_lines" ("namespace");
-- Create index "chargeflatfeerundetailedline_namespace_id" to table: "charge_flat_fee_run_detailed_lines"
CREATE UNIQUE INDEX "chargeflatfeerundetailedline_namespace_id" ON "charge_flat_fee_run_detailed_lines" ("namespace", "id");
-- Create index "chargeflatfeerundetailedline_namespace_run_id" to table: "charge_flat_fee_run_detailed_lines"
CREATE INDEX "chargeflatfeerundetailedline_namespace_run_id" ON "charge_flat_fee_run_detailed_lines" ("namespace", "run_id");
-- Create index "chargeffdetailedline_ns_run_child_id" to table: "charge_flat_fee_run_detailed_lines"
CREATE UNIQUE INDEX "chargeffdetailedline_ns_run_child_id" ON "charge_flat_fee_run_detailed_lines" ("namespace", "run_id", "child_unique_reference_id") WHERE deleted_at IS NULL;
-- Create "charge_flat_fee_run_invoiced_usages" table
CREATE TABLE "charge_flat_fee_run_invoiced_usages" (
  "id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "ledger_transaction_group_id" character(26) NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "run_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_flat_fee_run_invoiced_usages_run_id_key" to table: "charge_flat_fee_run_invoiced_usages"
CREATE UNIQUE INDEX "charge_flat_fee_run_invoiced_usages_run_id_key" ON "charge_flat_fee_run_invoiced_usages" ("run_id");
-- Create index "chargeflatfeeruninvoicedusage_namespace" to table: "charge_flat_fee_run_invoiced_usages"
CREATE INDEX "chargeflatfeeruninvoicedusage_namespace" ON "charge_flat_fee_run_invoiced_usages" ("namespace");
-- Create index "chargeflatfeeruninvoicedusage_id" to table: "charge_flat_fee_run_invoiced_usages"
CREATE UNIQUE INDEX "chargeflatfeeruninvoicedusage_id" ON "charge_flat_fee_run_invoiced_usages" ("id");
-- Create index "chargeflatfeeruninvoicedusage_annotations" to table: "charge_flat_fee_run_invoiced_usages"
CREATE INDEX "chargeflatfeeruninvoicedusage_annotations" ON "charge_flat_fee_run_invoiced_usages" USING GIN ("annotations");
-- Create index "chargeflatfeeruninvoicedusage_namespace_run_id" to table: "charge_flat_fee_run_invoiced_usages"
CREATE UNIQUE INDEX "chargeflatfeeruninvoicedusage_namespace_run_id" ON "charge_flat_fee_run_invoiced_usages" ("namespace", "run_id");
-- Create "charge_flat_fee_run_overage_credit_allocations" table
CREATE TABLE "charge_flat_fee_run_overage_credit_allocations" (
  "id" character(26) NOT NULL,
  "amount" numeric NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "ledger_transaction_group_id" character(26) NOT NULL,
  "sort_hint" bigint NOT NULL,
  "type" character varying NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "line_id" character(26) NULL,
  "run_id" character(26) NOT NULL,
  "corrects_realization_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "charge_ff_overage_credit_alloc_correction" FOREIGN KEY ("corrects_realization_id") REFERENCES "charge_flat_fee_run_overage_credit_allocations" ("id") ON DELETE SET NULL
);
-- Create index "chargeflatfeerunoveragecreditallocations_namespace" to table: "charge_flat_fee_run_overage_credit_allocations"
CREATE INDEX "chargeflatfeerunoveragecreditallocations_namespace" ON "charge_flat_fee_run_overage_credit_allocations" ("namespace");
-- Create index "chargeflatfeerunoveragecreditallocations_id" to table: "charge_flat_fee_run_overage_credit_allocations"
CREATE UNIQUE INDEX "chargeflatfeerunoveragecreditallocations_id" ON "charge_flat_fee_run_overage_credit_allocations" ("id");
-- Create index "chargeflatfeerunoveragecreditallocations_annotations" to table: "charge_flat_fee_run_overage_credit_allocations"
CREATE INDEX "chargeflatfeerunoveragecreditallocations_annotations" ON "charge_flat_fee_run_overage_credit_allocations" USING GIN ("annotations");
-- Create "charge_flat_fee_run_payments" table
CREATE TABLE "charge_flat_fee_run_payments" (
  "id" character(26) NOT NULL,
  "invoice_id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "status" character varying NOT NULL,
  "amount" numeric NOT NULL,
  "authorized_transaction_group_id" character(26) NULL,
  "authorized_at" timestamptz NULL,
  "settled_transaction_group_id" character(26) NULL,
  "settled_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "line_id" character(26) NOT NULL,
  "run_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_flat_fee_run_payments_line_id_key" to table: "charge_flat_fee_run_payments"
CREATE UNIQUE INDEX "charge_flat_fee_run_payments_line_id_key" ON "charge_flat_fee_run_payments" ("line_id");
-- Create index "charge_flat_fee_run_payments_run_id_key" to table: "charge_flat_fee_run_payments"
CREATE UNIQUE INDEX "charge_flat_fee_run_payments_run_id_key" ON "charge_flat_fee_run_payments" ("run_id");
-- Create index "chargeflatfeerunpayment_namespace" to table: "charge_flat_fee_run_payments"
CREATE INDEX "chargeflatfeerunpayment_namespace" ON "charge_flat_fee_run_payments" ("namespace");
-- Create index "chargeflatfeerunpayment_id" to table: "charge_flat_fee_run_payments"
CREATE UNIQUE INDEX "chargeflatfeerunpayment_id" ON "charge_flat_fee_run_payments" ("id");
-- Create index "chargeflatfeerunpayment_annotations" to table: "charge_flat_fee_run_payments"
CREATE INDEX "chargeflatfeerunpayment_annotations" ON "charge_flat_fee_run_payments" USING GIN ("annotations");
-- Create "charge_usage_based" table
CREATE TABLE "charge_usage_based" (
  "id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "billing_period_from" timestamptz NOT NULL,
  "billing_period_to" timestamptz NOT NULL,
  "full_service_period_from" timestamptz NOT NULL,
  "full_service_period_to" timestamptz NOT NULL,
  "status" character varying NOT NULL,
  "unique_reference_id" character varying NULL,
  "currency" character varying(3) NULL,
  "managed_by" character varying NOT NULL,
  "subscription_plan" jsonb NULL,
  "advance_after" timestamptz NULL,
  "tax_behavior" character varying NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "validation_issues" jsonb NULL,
  "invoice_at" timestamptz NOT NULL,
  "settlement_mode" character varying NOT NULL,
  "intent_deleted_at" timestamptz NULL,
  "discounts" jsonb NULL,
  "feature_key" character varying NOT NULL,
  "rating_engine" character varying NOT NULL,
  "price" jsonb NOT NULL,
  "unit_config" jsonb NULL,
  "status_detailed" character varying NOT NULL,
  "current_realization_run_id" character(26) NULL,
  "cost_basis_id" character(26) NULL,
  "custom_currency_id" character(26) NULL,
  "customer_id" character(26) NOT NULL,
  "feature_id" character(26) NULL,
  "subscription_id" character(26) NULL,
  "subscription_item_id" character(26) NULL,
  "subscription_phase_id" character(26) NULL,
  "tax_code_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "currency_not_empty" CHECK (currency IS NULL OR currency <> ''),
  CONSTRAINT "currency_reference" CHECK ((currency IS NULL) <> (custom_currency_id IS NULL))
);
-- Create index "chargeusagebased_namespace_customer_id_unique_reference_id" to table: "charge_usage_based"
CREATE UNIQUE INDEX "chargeusagebased_namespace_customer_id_unique_reference_id" ON "charge_usage_based" ("namespace", "customer_id", "unique_reference_id") WHERE unique_reference_id IS NOT NULL AND deleted_at IS NULL;
-- Create index "chargeusagebased_annotations" to table: "charge_usage_based"
CREATE INDEX "chargeusagebased_annotations" ON "charge_usage_based" USING GIN ("annotations");
-- Create index "chargeusagebased_id" to table: "charge_usage_based"
CREATE UNIQUE INDEX "chargeusagebased_id" ON "charge_usage_based" ("id");
-- Create index "chargeusagebased_namespace" to table: "charge_usage_based"
CREATE INDEX "chargeusagebased_namespace" ON "charge_usage_based" ("namespace");
-- Create index "chargeusagebased_namespace_id" to table: "charge_usage_based"
CREATE UNIQUE INDEX "chargeusagebased_namespace_id" ON "charge_usage_based" ("namespace", "id");
-- Create index "chargeusagebased_tax_code_id" to table: "charge_usage_based"
CREATE INDEX "chargeusagebased_tax_code_id" ON "charge_usage_based" ("tax_code_id");
-- Create index "chargeusagebased_cost_basis_id" to table: "charge_usage_based"
CREATE UNIQUE INDEX "chargeusagebased_cost_basis_id" ON "charge_usage_based" ("cost_basis_id");
-- Create "charge_usage_based_cost_bases" table
CREATE TABLE "charge_usage_based_cost_bases" (
  "id" character(26) NOT NULL,
  "mode" character varying NOT NULL,
  "fiat_currency" character varying(3) NOT NULL,
  "manual_rate" numeric NULL,
  "resolved_cost_basis" numeric NULL,
  "resolved_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "currency_cost_basis_id" character(26) NULL,
  "resolved_cost_basis_id" character(26) NULL,
  "currency_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fiat_currency_not_empty" CHECK (fiat_currency <> ''),
  CONSTRAINT "resolved_cost_basis_positive" CHECK (resolved_cost_basis IS NULL OR resolved_cost_basis > 0),
  CONSTRAINT "state" CHECK (
				(
					mode = 'dynamic'
					AND currency_cost_basis_id IS NULL
					AND manual_rate IS NULL
					AND (
						(resolved_cost_basis_id IS NULL AND resolved_cost_basis IS NULL AND resolved_at IS NULL)
						OR (resolved_cost_basis_id IS NOT NULL AND resolved_cost_basis IS NOT NULL AND resolved_at IS NOT NULL)
					)
				)
				OR (
					mode = 'pinned'
					AND currency_cost_basis_id IS NOT NULL
					AND resolved_cost_basis_id IS NOT NULL
					AND resolved_cost_basis_id = currency_cost_basis_id
					AND manual_rate IS NULL
					AND resolved_cost_basis IS NOT NULL
					AND resolved_at IS NOT NULL
				)
				OR (
					mode = 'manual'
					AND currency_cost_basis_id IS NULL
					AND resolved_cost_basis_id IS NULL
					AND manual_rate > 0
					AND resolved_cost_basis IS NOT NULL
					AND resolved_at IS NOT NULL
				)
			)
);
-- Create index "chargeusagebasedcostbasis_currency_cost_basis_id" to table: "charge_usage_based_cost_bases"
CREATE INDEX "chargeusagebasedcostbasis_currency_cost_basis_id" ON "charge_usage_based_cost_bases" ("currency_cost_basis_id");
-- Create index "chargeusagebasedcostbasis_resolved_cost_basis_id" to table: "charge_usage_based_cost_bases"
CREATE INDEX "chargeusagebasedcostbasis_resolved_cost_basis_id" ON "charge_usage_based_cost_bases" ("resolved_cost_basis_id");
-- Create index "chargeusagebasedcostbasis_currency_id" to table: "charge_usage_based_cost_bases"
CREATE INDEX "chargeusagebasedcostbasis_currency_id" ON "charge_usage_based_cost_bases" ("currency_id");
-- Create index "chargeusagebasedcostbasis_namespace" to table: "charge_usage_based_cost_bases"
CREATE INDEX "chargeusagebasedcostbasis_namespace" ON "charge_usage_based_cost_bases" ("namespace");
-- Create index "chargeusagebasedcostbasis_id" to table: "charge_usage_based_cost_bases"
CREATE UNIQUE INDEX "chargeusagebasedcostbasis_id" ON "charge_usage_based_cost_bases" ("id");
-- Create "charge_usage_based_overrides" table
CREATE TABLE "charge_usage_based_overrides" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "metadata" jsonb NULL,
  "tax_behavior" character varying NULL,
  "intent_deleted_at" timestamptz NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "full_service_period_from" timestamptz NOT NULL,
  "full_service_period_to" timestamptz NOT NULL,
  "billing_period_from" timestamptz NOT NULL,
  "billing_period_to" timestamptz NOT NULL,
  "invoice_at" timestamptz NOT NULL,
  "feature_key" character varying NULL,
  "price" jsonb NOT NULL,
  "discounts" jsonb NOT NULL,
  "unit_config" jsonb NULL,
  "charge_id" character(26) NOT NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_usage_based_overrides_charge_id_key" to table: "charge_usage_based_overrides"
CREATE UNIQUE INDEX "charge_usage_based_overrides_charge_id_key" ON "charge_usage_based_overrides" ("charge_id");
-- Create index "chargeusagebasedoverride_namespace" to table: "charge_usage_based_overrides"
CREATE INDEX "chargeusagebasedoverride_namespace" ON "charge_usage_based_overrides" ("namespace");
-- Create index "chargeusagebasedoverride_id" to table: "charge_usage_based_overrides"
CREATE UNIQUE INDEX "chargeusagebasedoverride_id" ON "charge_usage_based_overrides" ("id");
-- Create index "chargeusagebasedoverrides_tax_code_id" to table: "charge_usage_based_overrides"
CREATE INDEX "chargeusagebasedoverrides_tax_code_id" ON "charge_usage_based_overrides" ("tax_code_id");
-- Create index "chargeusagebasedoverride_namespace_charge_id" to table: "charge_usage_based_overrides"
CREATE UNIQUE INDEX "chargeusagebasedoverride_namespace_charge_id" ON "charge_usage_based_overrides" ("namespace", "charge_id");
-- Create "charge_usage_based_run_credit_allocations" table
CREATE TABLE "charge_usage_based_run_credit_allocations" (
  "id" character(26) NOT NULL,
  "line_id" character(26) NULL,
  "amount" numeric NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "ledger_transaction_group_id" character(26) NOT NULL,
  "sort_hint" bigint NOT NULL,
  "type" character varying NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "corrects_realization_id" character(26) NULL,
  "run_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "charge_usage_based_run_credit_allocations_charge_usage_based_ru" FOREIGN KEY ("corrects_realization_id") REFERENCES "charge_usage_based_run_credit_allocations" ("id") ON DELETE SET NULL
);
-- Create index "chargeusagebasedruncreditallocations_namespace" to table: "charge_usage_based_run_credit_allocations"
CREATE INDEX "chargeusagebasedruncreditallocations_namespace" ON "charge_usage_based_run_credit_allocations" ("namespace");
-- Create index "chargeusagebasedruncreditallocations_id" to table: "charge_usage_based_run_credit_allocations"
CREATE UNIQUE INDEX "chargeusagebasedruncreditallocations_id" ON "charge_usage_based_run_credit_allocations" ("id");
-- Create index "chargeusagebasedruncreditallocations_annotations" to table: "charge_usage_based_run_credit_allocations"
CREATE INDEX "chargeusagebasedruncreditallocations_annotations" ON "charge_usage_based_run_credit_allocations" USING GIN ("annotations");
-- Create "charge_usage_based_run_detailed_line" table
CREATE TABLE "charge_usage_based_run_detailed_line" (
  "id" character(26) NOT NULL,
  "amount_discounts" jsonb NULL,
  "currency" character varying(3) NULL,
  "service_period_start" timestamptz NOT NULL,
  "service_period_end" timestamptz NOT NULL,
  "quantity" numeric NOT NULL,
  "invoicing_app_external_id" character varying NULL,
  "child_unique_reference_id" character varying NOT NULL,
  "per_unit_amount" numeric NOT NULL,
  "category" character varying NOT NULL DEFAULT 'regular',
  "payment_term" character varying NOT NULL DEFAULT 'in_advance',
  "index" bigint NULL,
  "credits_applied" jsonb NULL,
  "annotations" jsonb NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "pricer_reference_id" character varying NOT NULL,
  "charge_id" character(26) NOT NULL,
  "run_id" character(26) NOT NULL,
  "corrects_run_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "child_unique_reference_id_not_empty" CHECK (child_unique_reference_id <> '')
);
-- Create index "chargeusagebasedrundetailedline_annotations" to table: "charge_usage_based_run_detailed_line"
CREATE INDEX "chargeusagebasedrundetailedline_annotations" ON "charge_usage_based_run_detailed_line" USING GIN ("annotations");
-- Create index "chargeusagebasedrundetailedline_id" to table: "charge_usage_based_run_detailed_line"
CREATE UNIQUE INDEX "chargeusagebasedrundetailedline_id" ON "charge_usage_based_run_detailed_line" ("id");
-- Create index "chargeusagebasedrundetailedline_namespace" to table: "charge_usage_based_run_detailed_line"
CREATE INDEX "chargeusagebasedrundetailedline_namespace" ON "charge_usage_based_run_detailed_line" ("namespace");
-- Create index "chargeusagebasedrundetailedline_namespace_id" to table: "charge_usage_based_run_detailed_line"
CREATE UNIQUE INDEX "chargeusagebasedrundetailedline_namespace_id" ON "charge_usage_based_run_detailed_line" ("namespace", "id");
-- Create index "chargeusagebasedrundetailedline_namespace_charge_id" to table: "charge_usage_based_run_detailed_line"
CREATE INDEX "chargeusagebasedrundetailedline_namespace_charge_id" ON "charge_usage_based_run_detailed_line" ("namespace", "charge_id");
-- Create index "chargeusagebasedrundetailedline_namespace_run_id" to table: "charge_usage_based_run_detailed_line"
CREATE INDEX "chargeusagebasedrundetailedline_namespace_run_id" ON "charge_usage_based_run_detailed_line" ("namespace", "run_id");
-- Create index "chargeubdetailedline_ns_charge_run_child_id" to table: "charge_usage_based_run_detailed_line"
CREATE UNIQUE INDEX "chargeubdetailedline_ns_charge_run_child_id" ON "charge_usage_based_run_detailed_line" ("namespace", "charge_id", "run_id", "child_unique_reference_id") WHERE deleted_at IS NULL;
-- Create "charge_usage_based_run_invoiced_usages" table
CREATE TABLE "charge_usage_based_run_invoiced_usages" (
  "id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "ledger_transaction_group_id" character(26) NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "run_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_usage_based_run_invoiced_usages_run_id_key" to table: "charge_usage_based_run_invoiced_usages"
CREATE UNIQUE INDEX "charge_usage_based_run_invoiced_usages_run_id_key" ON "charge_usage_based_run_invoiced_usages" ("run_id");
-- Create index "chargeusagebasedruninvoicedusage_namespace" to table: "charge_usage_based_run_invoiced_usages"
CREATE INDEX "chargeusagebasedruninvoicedusage_namespace" ON "charge_usage_based_run_invoiced_usages" ("namespace");
-- Create index "chargeusagebasedruninvoicedusage_id" to table: "charge_usage_based_run_invoiced_usages"
CREATE UNIQUE INDEX "chargeusagebasedruninvoicedusage_id" ON "charge_usage_based_run_invoiced_usages" ("id");
-- Create index "chargeusagebasedruninvoicedusage_annotations" to table: "charge_usage_based_run_invoiced_usages"
CREATE INDEX "chargeusagebasedruninvoicedusage_annotations" ON "charge_usage_based_run_invoiced_usages" USING GIN ("annotations");
-- Create "charge_usage_based_run_overage_credit_allocations" table
CREATE TABLE "charge_usage_based_run_overage_credit_allocations" (
  "id" character(26) NOT NULL,
  "line_id" character(26) NULL,
  "amount" numeric NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "ledger_transaction_group_id" character(26) NOT NULL,
  "sort_hint" bigint NOT NULL,
  "type" character varying NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "corrects_realization_id" character(26) NULL,
  "run_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "charge_ub_overage_credit_alloc_correction" FOREIGN KEY ("corrects_realization_id") REFERENCES "charge_usage_based_run_overage_credit_allocations" ("id") ON DELETE SET NULL
);
-- Create index "chargeusagebasedrunoveragecreditallocations_namespace" to table: "charge_usage_based_run_overage_credit_allocations"
CREATE INDEX "chargeusagebasedrunoveragecreditallocations_namespace" ON "charge_usage_based_run_overage_credit_allocations" ("namespace");
-- Create index "chargeusagebasedrunoveragecreditallocations_id" to table: "charge_usage_based_run_overage_credit_allocations"
CREATE UNIQUE INDEX "chargeusagebasedrunoveragecreditallocations_id" ON "charge_usage_based_run_overage_credit_allocations" ("id");
-- Create index "chargeusagebasedrunoveragecreditallocations_annotations" to table: "charge_usage_based_run_overage_credit_allocations"
CREATE INDEX "chargeusagebasedrunoveragecreditallocations_annotations" ON "charge_usage_based_run_overage_credit_allocations" USING GIN ("annotations");
-- Create "charge_usage_based_run_payments" table
CREATE TABLE "charge_usage_based_run_payments" (
  "id" character(26) NOT NULL,
  "line_id" character(26) NOT NULL,
  "invoice_id" character(26) NOT NULL,
  "service_period_from" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "status" character varying NOT NULL,
  "amount" numeric NOT NULL,
  "authorized_transaction_group_id" character(26) NULL,
  "authorized_at" timestamptz NULL,
  "settled_transaction_group_id" character(26) NULL,
  "settled_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "run_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "charge_usage_based_run_payments_run_id_key" to table: "charge_usage_based_run_payments"
CREATE UNIQUE INDEX "charge_usage_based_run_payments_run_id_key" ON "charge_usage_based_run_payments" ("run_id");
-- Create index "chargeusagebasedrunpayment_namespace" to table: "charge_usage_based_run_payments"
CREATE INDEX "chargeusagebasedrunpayment_namespace" ON "charge_usage_based_run_payments" ("namespace");
-- Create index "chargeusagebasedrunpayment_id" to table: "charge_usage_based_run_payments"
CREATE UNIQUE INDEX "chargeusagebasedrunpayment_id" ON "charge_usage_based_run_payments" ("id");
-- Create index "chargeusagebasedrunpayment_annotations" to table: "charge_usage_based_run_payments"
CREATE INDEX "chargeusagebasedrunpayment_annotations" ON "charge_usage_based_run_payments" USING GIN ("annotations");
-- Create "charge_usage_based_runs" table
CREATE TABLE "charge_usage_based_runs" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "amount" numeric NOT NULL,
  "taxes_total" numeric NOT NULL,
  "taxes_inclusive_total" numeric NOT NULL,
  "taxes_exclusive_total" numeric NOT NULL,
  "charges_total" numeric NOT NULL,
  "discounts_total" numeric NOT NULL,
  "credits_total" numeric NOT NULL,
  "total" numeric NOT NULL,
  "type" character varying NOT NULL,
  "initial_type" character varying NOT NULL,
  "stored_at_lt" timestamptz NOT NULL,
  "service_period_to" timestamptz NOT NULL,
  "schema_level" smallint NOT NULL DEFAULT 2,
  "detailed_lines_present" boolean NOT NULL,
  "detailed_lines_include_credit_allocations" boolean NOT NULL DEFAULT false,
  "metered_quantity" numeric NOT NULL,
  "no_fiat_transaction_required" boolean NOT NULL,
  "immutable" boolean NOT NULL DEFAULT false,
  "fiat_overage_credit_allocation_completed" boolean NOT NULL DEFAULT false,
  "invoice_id" character(26) NULL,
  "line_id" character(26) NULL,
  "charge_id" character(26) NOT NULL,
  "prior_run_id" character(26) NULL,
  "feature_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "charge_ub_run_prior_run" FOREIGN KEY ("prior_run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE SET NULL
);
-- Create index "charge_usage_based_runs_line_id_key" to table: "charge_usage_based_runs"
CREATE UNIQUE INDEX "charge_usage_based_runs_line_id_key" ON "charge_usage_based_runs" ("line_id");
-- Create index "chargeusagebasedruns_namespace" to table: "charge_usage_based_runs"
CREATE INDEX "chargeusagebasedruns_namespace" ON "charge_usage_based_runs" ("namespace");
-- Create index "chargeusagebasedruns_id" to table: "charge_usage_based_runs"
CREATE UNIQUE INDEX "chargeusagebasedruns_id" ON "charge_usage_based_runs" ("id");
-- Create index "chargeusagebasedruns_namespace_charge_id" to table: "charge_usage_based_runs"
CREATE INDEX "chargeusagebasedruns_namespace_charge_id" ON "charge_usage_based_runs" ("namespace", "charge_id");
-- Create "credit_realization_lineages" table
CREATE TABLE "credit_realization_lineages" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "root_realization_id" character(26) NOT NULL,
  "customer_id" character(26) NOT NULL,
  "currency" character varying(24) NOT NULL,
  "custom_currency_id" character(26) NULL,
  "origin_kind" character varying NOT NULL,
  "advance_features" text[] NULL,
  "created_at" timestamptz NOT NULL,
  "charge_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "creditrealizationlineage_id" to table: "credit_realization_lineages"
CREATE UNIQUE INDEX "creditrealizationlineage_id" ON "credit_realization_lineages" ("id");
-- Create index "creditrealizationlineage_namespace" to table: "credit_realization_lineages"
CREATE INDEX "creditrealizationlineage_namespace" ON "credit_realization_lineages" ("namespace");
-- Create index "creditrealizationlineage_namespace_root_realization_id" to table: "credit_realization_lineages"
CREATE UNIQUE INDEX "creditrealizationlineage_namespace_root_realization_id" ON "credit_realization_lineages" ("namespace", "root_realization_id");
-- Create index "creditrealizationlineage_namespace_charge_id" to table: "credit_realization_lineages"
CREATE INDEX "creditrealizationlineage_namespace_charge_id" ON "credit_realization_lineages" ("namespace", "charge_id");
-- Create index "creditrealizationlineage_namespace_customer_id" to table: "credit_realization_lineages"
CREATE INDEX "creditrealizationlineage_namespace_customer_id" ON "credit_realization_lineages" ("namespace", "customer_id");
-- Create "credit_realization_lineage_segments" table
CREATE TABLE "credit_realization_lineage_segments" (
  "id" character(26) NOT NULL,
  "amount" numeric NOT NULL,
  "state" character varying NOT NULL,
  "backing_transaction_group_id" character(26) NULL,
  "source_state" character varying NULL,
  "source_backing_transaction_group_id" character(26) NULL,
  "closed_at" timestamptz NULL,
  "created_at" timestamptz NOT NULL,
  "lineage_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "creditrealizationlineagesegment_id" to table: "credit_realization_lineage_segments"
CREATE UNIQUE INDEX "creditrealizationlineagesegment_id" ON "credit_realization_lineage_segments" ("id");
-- Create index "creditrealizationlineagesegment_lineage_id" to table: "credit_realization_lineage_segments"
CREATE INDEX "creditrealizationlineagesegment_lineage_id" ON "credit_realization_lineage_segments" ("lineage_id");
-- Create index "creditrealizationlineagesegment_lineage_id_closed_at" to table: "credit_realization_lineage_segments"
CREATE INDEX "creditrealizationlineagesegment_lineage_id_closed_at" ON "credit_realization_lineage_segments" ("lineage_id", "closed_at");
-- Create "currency_cost_bases" table
CREATE TABLE "currency_cost_bases" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "fiat_code" character varying NOT NULL,
  "rate" numeric NOT NULL,
  "effective_from" timestamptz NOT NULL,
  "effective_to" timestamptz NULL,
  "currency_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "currencycostbasis_namespace" to table: "currency_cost_bases"
CREATE INDEX "currencycostbasis_namespace" ON "currency_cost_bases" ("namespace");
-- Create index "currencycostbasis_id" to table: "currency_cost_bases"
CREATE UNIQUE INDEX "currencycostbasis_id" ON "currency_cost_bases" ("id");
-- Create index "currencycostbasis_namespace_currency_id_fiat_code_effective_from" to table: "currency_cost_bases"
CREATE UNIQUE INDEX "currencycostbasis_namespace_currency_id_fiat_code_effective_from" ON "currency_cost_bases" ("namespace", "currency_id", "fiat_code", "effective_from") WHERE deleted_at IS NULL;
-- Create "custom_currencies" table
CREATE TABLE "custom_currencies" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "code" character varying NOT NULL,
  "name" character varying NOT NULL,
  "symbol" character varying NULL,
  "precision" bigint NOT NULL DEFAULT 2,
  "decimal_mark" character varying NOT NULL DEFAULT '.',
  "thousands_separator" character varying NOT NULL DEFAULT ',',
  PRIMARY KEY ("id")
);
-- Create index "customcurrency_namespace" to table: "custom_currencies"
CREATE INDEX "customcurrency_namespace" ON "custom_currencies" ("namespace");
-- Create index "customcurrency_id" to table: "custom_currencies"
CREATE UNIQUE INDEX "customcurrency_id" ON "custom_currencies" ("id");
-- Create index "customcurrency_namespace_code" to table: "custom_currencies"
CREATE UNIQUE INDEX "customcurrency_namespace_code" ON "custom_currencies" ("namespace", "code") WHERE deleted_at IS NULL;
-- Create index "customcurrency_namespace_id" to table: "custom_currencies"
CREATE UNIQUE INDEX "customcurrency_namespace_id" ON "custom_currencies" ("namespace", "id");
-- Create "customers" table
CREATE TABLE "customers" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "billing_address_country" character varying NULL,
  "billing_address_postal_code" character varying NULL,
  "billing_address_state" character varying NULL,
  "billing_address_city" character varying NULL,
  "billing_address_line1" character varying NULL,
  "billing_address_line2" character varying NULL,
  "billing_address_phone_number" character varying NULL,
  "annotations" jsonb NULL,
  "key" character varying NULL,
  "primary_email" character varying NULL,
  "currency" character varying NULL,
  PRIMARY KEY ("id")
);
-- Create index "customer_id" to table: "customers"
CREATE UNIQUE INDEX "customer_id" ON "customers" ("id");
-- Create index "customer_namespace" to table: "customers"
CREATE INDEX "customer_namespace" ON "customers" ("namespace");
-- Create index "customer_namespace_id" to table: "customers"
CREATE UNIQUE INDEX "customer_namespace_id" ON "customers" ("namespace", "id");
-- Create index "customer_annotations" to table: "customers"
CREATE INDEX "customer_annotations" ON "customers" USING GIN ("annotations");
-- Create index "customer_namespace_key" to table: "customers"
CREATE UNIQUE INDEX "customer_namespace_key" ON "customers" ("namespace", "key") WHERE deleted_at IS NULL;
-- Create index "customer_namespace_key_deleted_at" to table: "customers"
CREATE INDEX "customer_namespace_key_deleted_at" ON "customers" ("namespace", "key", "deleted_at");
-- Create index "customer_name" to table: "customers"
CREATE INDEX "customer_name" ON "customers" ("name");
-- Create index "customer_primary_email" to table: "customers"
CREATE INDEX "customer_primary_email" ON "customers" ("primary_email");
-- Create index "customer_created_at" to table: "customers"
CREATE INDEX "customer_created_at" ON "customers" ("created_at");
-- Create "customer_subjects" table
CREATE TABLE "customer_subjects" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "namespace" character varying NOT NULL,
  "subject_key" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "customer_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "customersubjects_namespace" to table: "customer_subjects"
CREATE INDEX "customersubjects_namespace" ON "customer_subjects" ("namespace");
-- Create index "customersubjects_namespace_customer_id_deleted_at" to table: "customer_subjects"
CREATE INDEX "customersubjects_namespace_customer_id_deleted_at" ON "customer_subjects" ("namespace", "customer_id", "deleted_at");
-- Create index "customersubjects_namespace_subject_key" to table: "customer_subjects"
CREATE UNIQUE INDEX "customersubjects_namespace_subject_key" ON "customer_subjects" ("namespace", "subject_key") WHERE deleted_at IS NULL;
-- Create index "customersubjects_deleted_at" to table: "customer_subjects"
CREATE INDEX "customersubjects_deleted_at" ON "customer_subjects" ("deleted_at");
-- Create index "customersubjects_subject_key" to table: "customer_subjects"
CREATE INDEX "customersubjects_subject_key" ON "customer_subjects" ("subject_key");
-- Create index "customersubjects_customer_id" to table: "customer_subjects"
CREATE INDEX "customersubjects_customer_id" ON "customer_subjects" ("customer_id");
-- Create index "customersubjects_deleted_at_customer_id" to table: "customer_subjects"
CREATE INDEX "customersubjects_deleted_at_customer_id" ON "customer_subjects" ("deleted_at", "customer_id");
-- Create "entitlements" table
CREATE TABLE "entitlements" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "entitlement_type" character varying NOT NULL,
  "active_from" timestamptz NULL,
  "active_to" timestamptz NULL,
  "feature_key" character varying NOT NULL,
  "measure_usage_from" timestamptz NULL,
  "issue_after_reset" double precision NULL,
  "issue_after_reset_priority" smallint NULL,
  "is_soft_limit" boolean NULL,
  "preserve_overage_at_reset" boolean NULL,
  "config" jsonb NULL,
  "unit_config" jsonb NULL,
  "usage_period_interval" character varying NULL,
  "usage_period_anchor" timestamptz NULL,
  "current_usage_period_start" timestamptz NULL,
  "current_usage_period_end" timestamptz NULL,
  "annotations" jsonb NULL,
  "customer_id" character(26) NOT NULL,
  "feature_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "entitlement_id" to table: "entitlements"
CREATE UNIQUE INDEX "entitlement_id" ON "entitlements" ("id");
-- Create index "entitlement_namespace" to table: "entitlements"
CREATE INDEX "entitlement_namespace" ON "entitlements" ("namespace");
-- Create index "entitlement_namespace_id" to table: "entitlements"
CREATE INDEX "entitlement_namespace_id" ON "entitlements" ("namespace", "id");
-- Create index "entitlement_namespace_customer_id" to table: "entitlements"
CREATE INDEX "entitlement_namespace_customer_id" ON "entitlements" ("namespace", "customer_id");
-- Create index "entitlement_namespace_id_customer_id" to table: "entitlements"
CREATE INDEX "entitlement_namespace_id_customer_id" ON "entitlements" ("namespace", "id", "customer_id");
-- Create index "entitlement_namespace_feature_id_id" to table: "entitlements"
CREATE INDEX "entitlement_namespace_feature_id_id" ON "entitlements" ("namespace", "feature_id", "id");
-- Create index "entitlement_namespace_current_usage_period_end" to table: "entitlements"
CREATE INDEX "entitlement_namespace_current_usage_period_end" ON "entitlements" ("namespace", "current_usage_period_end");
-- Create index "entitlement_current_usage_period_end_deleted_at" to table: "entitlements"
CREATE INDEX "entitlement_current_usage_period_end_deleted_at" ON "entitlements" ("current_usage_period_end", "deleted_at");
-- Create index "entitlement_created_at_id" to table: "entitlements"
CREATE UNIQUE INDEX "entitlement_created_at_id" ON "entitlements" ("created_at", "id");
-- Create "event_outboxes" table
CREATE TABLE "event_outboxes" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "created_at" timestamptz NOT NULL,
  "message_id" character varying NOT NULL,
  "transaction_id" character(26) NOT NULL,
  "attempts" bigint NOT NULL DEFAULT 0,
  "topic" character varying NOT NULL,
  "payload" bytea NOT NULL,
  "metadata" jsonb NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "eventoutbox_topic_id" to table: "event_outboxes"
CREATE INDEX "eventoutbox_topic_id" ON "event_outboxes" ("topic", "id");
-- Create index "eventoutbox_topic_transaction_id_id" to table: "event_outboxes"
CREATE INDEX "eventoutbox_topic_transaction_id_id" ON "event_outboxes" ("topic", "transaction_id", "id");
-- Create "features" table
CREATE TABLE "features" (
  "id" character(26) NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "metadata" jsonb NULL,
  "namespace" character varying NOT NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "meter_slug" character varying NULL,
  "meter_group_by_filters" jsonb NULL,
  "advanced_meter_group_by_filters" jsonb NULL,
  "unit_cost_type" character varying NULL,
  "unit_cost_manual_amount" numeric NULL,
  "unit_cost_llm_provider_property" character varying NULL,
  "unit_cost_llm_provider" character varying NULL,
  "unit_cost_llm_model_property" character varying NULL,
  "unit_cost_llm_model" character varying NULL,
  "unit_cost_llm_token_type_property" character varying NULL,
  "unit_cost_llm_token_type" character varying NULL,
  "archived_at" timestamptz NULL,
  "meter_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "unit_cost_llm_model_mutual_exclusive" CHECK (NOT (unit_cost_llm_model_property IS NOT NULL AND unit_cost_llm_model IS NOT NULL)),
  CONSTRAINT "unit_cost_llm_provider_mutual_exclusive" CHECK (NOT (unit_cost_llm_provider_property IS NOT NULL AND unit_cost_llm_provider IS NOT NULL)),
  CONSTRAINT "unit_cost_llm_token_type_mutual_exclusive" CHECK (NOT (unit_cost_llm_token_type_property IS NOT NULL AND unit_cost_llm_token_type IS NOT NULL))
);
-- Create index "feature_id" to table: "features"
CREATE UNIQUE INDEX "feature_id" ON "features" ("id");
-- Create index "feature_namespace_id" to table: "features"
CREATE INDEX "feature_namespace_id" ON "features" ("namespace", "id");
-- Create index "feature_namespace_key" to table: "features"
CREATE UNIQUE INDEX "feature_namespace_key" ON "features" ("namespace", "key") WHERE archived_at IS NULL;
-- Create index "feature_namespace_meter_slug" to table: "features"
CREATE INDEX "feature_namespace_meter_slug" ON "features" ("namespace", "meter_slug") WHERE archived_at IS NULL;
-- Create index "feature_namespace_meter_id" to table: "features"
CREATE INDEX "feature_namespace_meter_id" ON "features" ("namespace", "meter_id") WHERE archived_at IS NULL;
-- Create "grants" table
CREATE TABLE "grants" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "metadata" jsonb NULL,
  "amount" numeric NOT NULL,
  "priority" smallint NOT NULL DEFAULT 0,
  "effective_at" timestamptz NOT NULL,
  "expiration" jsonb NULL,
  "expires_at" timestamptz NULL,
  "voided_at" timestamptz NULL,
  "reset_max_rollover" numeric NOT NULL,
  "reset_min_rollover" numeric NOT NULL,
  "recurrence_period" character varying NULL,
  "recurrence_anchor" timestamptz NULL,
  "owner_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "grant_id" to table: "grants"
CREATE UNIQUE INDEX "grant_id" ON "grants" ("id");
-- Create index "grant_namespace" to table: "grants"
CREATE INDEX "grant_namespace" ON "grants" ("namespace");
-- Create index "grant_annotations" to table: "grants"
CREATE INDEX "grant_annotations" ON "grants" USING GIN ("annotations");
-- Create index "grant_namespace_owner_id" to table: "grants"
CREATE INDEX "grant_namespace_owner_id" ON "grants" ("namespace", "owner_id");
-- Create index "grant_effective_at_expires_at" to table: "grants"
CREATE INDEX "grant_effective_at_expires_at" ON "grants" ("effective_at", "expires_at");
-- Create "llm_cost_prices" table
CREATE TABLE "llm_cost_prices" (
  "id" character(26) NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "namespace" character varying NULL,
  "provider" character varying NOT NULL,
  "model_id" character varying NOT NULL,
  "model_name" character varying NOT NULL DEFAULT '',
  "input_per_token" numeric NOT NULL,
  "output_per_token" numeric NOT NULL,
  "cache_read_per_token" numeric NOT NULL,
  "reasoning_per_token" numeric NOT NULL,
  "cache_write_per_token" numeric NOT NULL,
  "currency" character varying NOT NULL DEFAULT 'USD',
  "source" character varying NOT NULL,
  "source_prices" jsonb NULL,
  "effective_from" timestamptz NOT NULL,
  "effective_to" timestamptz NULL,
  PRIMARY KEY ("id")
);
-- Create index "llmcostprice_id" to table: "llm_cost_prices"
CREATE UNIQUE INDEX "llmcostprice_id" ON "llm_cost_prices" ("id");
-- Create index "llmcostprice_provider_model_id_namespace_effective_from" to table: "llm_cost_prices"
CREATE UNIQUE INDEX "llmcostprice_provider_model_id_namespace_effective_from" ON "llm_cost_prices" ("provider", "model_id", "namespace", "effective_from") WHERE deleted_at IS NULL;
-- Create index "llmcostprice_namespace_provider_model_id" to table: "llm_cost_prices"
CREATE INDEX "llmcostprice_namespace_provider_model_id" ON "llm_cost_prices" ("namespace", "provider", "model_id") WHERE deleted_at IS NULL;
-- Create index "llmcostprice_provider_model_id" to table: "llm_cost_prices"
CREATE INDEX "llmcostprice_provider_model_id" ON "llm_cost_prices" ("provider", "model_id") WHERE deleted_at IS NULL AND namespace IS NULL;
-- Create "ledger_accounts" table
CREATE TABLE "ledger_accounts" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "account_type" character varying NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgeraccount_id" to table: "ledger_accounts"
CREATE UNIQUE INDEX "ledgeraccount_id" ON "ledger_accounts" ("id");
-- Create index "ledgeraccount_namespace" to table: "ledger_accounts"
CREATE INDEX "ledgeraccount_namespace" ON "ledger_accounts" ("namespace");
-- Create index "ledgeraccount_annotations" to table: "ledger_accounts"
CREATE INDEX "ledgeraccount_annotations" ON "ledger_accounts" USING GIN ("annotations");
-- Create index "ledgeraccount_namespace_id" to table: "ledger_accounts"
CREATE UNIQUE INDEX "ledgeraccount_namespace_id" ON "ledger_accounts" ("namespace", "id");
-- Create "ledger_breakage_records" table
CREATE TABLE "ledger_breakage_records" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "kind" character varying NOT NULL,
  "amount" numeric NOT NULL,
  "customer_id" character(26) NOT NULL,
  "currency" character varying NOT NULL,
  "credit_priority" bigint NOT NULL,
  "expires_at" timestamptz NOT NULL,
  "source_kind" character varying NOT NULL,
  "source_charge_id" character(26) NULL,
  "plan_id" character(26) NULL,
  "release_id" character(26) NULL,
  "source_entry_id" character(26) NULL,
  "fbo_sub_account_id" character(26) NOT NULL,
  "breakage_sub_account_id" character(26) NOT NULL,
  "source_transaction_id" character(26) NULL,
  "breakage_transaction_id" character(26) NOT NULL,
  "source_transaction_group_id" character(26) NULL,
  "breakage_transaction_group_id" character(26) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "ledger_breakage_records_ledger_breakage_records_planned_releases" FOREIGN KEY ("plan_id") REFERENCES "ledger_breakage_records" ("id") ON DELETE RESTRICT,
  CONSTRAINT "ledger_breakage_records_ledger_breakage_records_release_reopens" FOREIGN KEY ("release_id") REFERENCES "ledger_breakage_records" ("id") ON DELETE RESTRICT
);
-- Create index "ledgerbreakagerecord_id" to table: "ledger_breakage_records"
CREATE UNIQUE INDEX "ledgerbreakagerecord_id" ON "ledger_breakage_records" ("id");
-- Create index "ledgerbreakagerecord_namespace" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_namespace" ON "ledger_breakage_records" ("namespace");
-- Create index "ledgerbreakagerecord_annotations" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_annotations" ON "ledger_breakage_records" USING GIN ("annotations");
-- Create index "ledgerbreakagerecord_namespace_customer_id_currency_credit_" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_namespace_customer_id_currency_credit_" ON "ledger_breakage_records" ("namespace", "customer_id", "currency", "credit_priority", "expires_at", "id");
-- Create index "ledgerbreakagerecord_namespace_plan_id" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_namespace_plan_id" ON "ledger_breakage_records" ("namespace", "plan_id");
-- Create index "ledgerbreakagerecord_namespace_source_charge_id" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_namespace_source_charge_id" ON "ledger_breakage_records" ("namespace", "source_charge_id");
-- Create index "ledgerbreakagerecord_namespace_source_transaction_group_id" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_namespace_source_transaction_group_id" ON "ledger_breakage_records" ("namespace", "source_transaction_group_id");
-- Create index "ledgerbreakagerecord_namespace_source_entry_id" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_namespace_source_entry_id" ON "ledger_breakage_records" ("namespace", "source_entry_id");
-- Create index "ledgerbreakagerecord_namespace_breakage_transaction_group_id" to table: "ledger_breakage_records"
CREATE INDEX "ledgerbreakagerecord_namespace_breakage_transaction_group_id" ON "ledger_breakage_records" ("namespace", "breakage_transaction_group_id");
-- Create "ledger_credit_void_records" table
CREATE TABLE "ledger_credit_void_records" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "amount" numeric NOT NULL,
  "customer_id" character(26) NOT NULL,
  "currency" character varying NOT NULL,
  "voided_at" timestamptz NOT NULL,
  "source_charge_id" character(26) NOT NULL,
  "void_transaction_group_id" character(26) NOT NULL,
  "void_transaction_id" character(26) NOT NULL,
  "fbo_sub_account_id" character(26) NOT NULL,
  "receivable_sub_account_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgercreditvoidrecord_id" to table: "ledger_credit_void_records"
CREATE UNIQUE INDEX "ledgercreditvoidrecord_id" ON "ledger_credit_void_records" ("id");
-- Create index "ledgercreditvoidrecord_namespace" to table: "ledger_credit_void_records"
CREATE INDEX "ledgercreditvoidrecord_namespace" ON "ledger_credit_void_records" ("namespace");
-- Create index "ledgercreditvoidrecord_annotations" to table: "ledger_credit_void_records"
CREATE INDEX "ledgercreditvoidrecord_annotations" ON "ledger_credit_void_records" USING GIN ("annotations");
-- Create index "ledgercreditvoidrecord_namespace_customer_currency_voided" to table: "ledger_credit_void_records"
CREATE INDEX "ledgercreditvoidrecord_namespace_customer_currency_voided" ON "ledger_credit_void_records" ("namespace", "customer_id", "currency", "voided_at", "id");
-- Create index "ledgercreditvoidrecord_namespace_source_charge_id" to table: "ledger_credit_void_records"
CREATE INDEX "ledgercreditvoidrecord_namespace_source_charge_id" ON "ledger_credit_void_records" ("namespace", "source_charge_id");
-- Create index "ledgercreditvoidrecord_namespace_void_transaction_group_id" to table: "ledger_credit_void_records"
CREATE INDEX "ledgercreditvoidrecord_namespace_void_transaction_group_id" ON "ledger_credit_void_records" ("namespace", "void_transaction_group_id");
-- Create "ledger_customer_accounts" table
CREATE TABLE "ledger_customer_accounts" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "customer_id" character varying NOT NULL,
  "account_type" character varying NOT NULL,
  "account_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgercustomeraccount_id" to table: "ledger_customer_accounts"
CREATE UNIQUE INDEX "ledgercustomeraccount_id" ON "ledger_customer_accounts" ("id");
-- Create index "ledgercustomeraccount_namespace" to table: "ledger_customer_accounts"
CREATE INDEX "ledgercustomeraccount_namespace" ON "ledger_customer_accounts" ("namespace");
-- Create index "ledgercustomeraccount_namespace_id" to table: "ledger_customer_accounts"
CREATE UNIQUE INDEX "ledgercustomeraccount_namespace_id" ON "ledger_customer_accounts" ("namespace", "id");
-- Create index "ledgercustomeraccount_namespace_customer_id_account_type" to table: "ledger_customer_accounts"
CREATE UNIQUE INDEX "ledgercustomeraccount_namespace_customer_id_account_type" ON "ledger_customer_accounts" ("namespace", "customer_id", "account_type");
-- Create "ledger_entries" table
CREATE TABLE "ledger_entries" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "identity_key" character varying NOT NULL DEFAULT '',
  "schema_version" bigint NOT NULL DEFAULT 1,
  "collection_origin_id" character(26) NULL,
  "source_charge_id" character(26) NULL,
  "spend_charge_id" character(26) NULL,
  "amount" numeric NOT NULL,
  "sub_account_id" character(26) NOT NULL,
  "transaction_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgerentry_id" to table: "ledger_entries"
CREATE UNIQUE INDEX "ledgerentry_id" ON "ledger_entries" ("id");
-- Create index "ledgerentry_namespace" to table: "ledger_entries"
CREATE INDEX "ledgerentry_namespace" ON "ledger_entries" ("namespace");
-- Create index "ledgerentry_annotations" to table: "ledger_entries"
CREATE INDEX "ledgerentry_annotations" ON "ledger_entries" USING GIN ("annotations");
-- Create index "ledgerentry_namespace_id" to table: "ledger_entries"
CREATE UNIQUE INDEX "ledgerentry_namespace_id" ON "ledger_entries" ("namespace", "id");
-- Create index "ledgerentry_namespace_transaction_id" to table: "ledger_entries"
CREATE INDEX "ledgerentry_namespace_transaction_id" ON "ledger_entries" ("namespace", "transaction_id");
-- Create index "ledgerentry_namespace_sub_account_id" to table: "ledger_entries"
CREATE INDEX "ledgerentry_namespace_sub_account_id" ON "ledger_entries" ("namespace", "sub_account_id");
-- Create index "ledgerentry_namespace_source_charge_id" to table: "ledger_entries"
CREATE INDEX "ledgerentry_namespace_source_charge_id" ON "ledger_entries" ("namespace", "source_charge_id");
-- Create index "ledgerentry_namespace_spend_charge_id" to table: "ledger_entries"
CREATE INDEX "ledgerentry_namespace_spend_charge_id" ON "ledger_entries" ("namespace", "spend_charge_id");
-- Create index "ledgerentry_namespace_collection_origin_id" to table: "ledger_entries"
CREATE INDEX "ledgerentry_namespace_collection_origin_id" ON "ledger_entries" ("namespace", "collection_origin_id");
-- Create index "ledgerentry_namespace_source_charge_id_spend_charge_id" to table: "ledger_entries"
CREATE INDEX "ledgerentry_namespace_source_charge_id_spend_charge_id" ON "ledger_entries" ("namespace", "source_charge_id", "spend_charge_id");
-- Create index "ledgerentry_transaction_id_sub_account_id_identity_key" to table: "ledger_entries"
CREATE UNIQUE INDEX "ledgerentry_transaction_id_sub_account_id_identity_key" ON "ledger_entries" ("transaction_id", "sub_account_id", "identity_key");
-- Create index "ledgerentry_created_at_id" to table: "ledger_entries"
CREATE INDEX "ledgerentry_created_at_id" ON "ledger_entries" ("created_at", "id") WHERE deleted_at IS NULL;
-- Create "ledger_sub_accounts" table
CREATE TABLE "ledger_sub_accounts" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "account_id" character(26) NOT NULL,
  "route_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgersubaccount_id" to table: "ledger_sub_accounts"
CREATE UNIQUE INDEX "ledgersubaccount_id" ON "ledger_sub_accounts" ("id");
-- Create index "ledgersubaccount_namespace" to table: "ledger_sub_accounts"
CREATE INDEX "ledgersubaccount_namespace" ON "ledger_sub_accounts" ("namespace");
-- Create index "ledgersubaccount_annotations" to table: "ledger_sub_accounts"
CREATE INDEX "ledgersubaccount_annotations" ON "ledger_sub_accounts" USING GIN ("annotations");
-- Create index "ledgersubaccount_namespace_account_id_route_id" to table: "ledger_sub_accounts"
CREATE UNIQUE INDEX "ledgersubaccount_namespace_account_id_route_id" ON "ledger_sub_accounts" ("namespace", "account_id", "route_id");
-- Create "ledger_sub_account_routes" table
CREATE TABLE "ledger_sub_account_routes" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "routing_key_version" character varying NOT NULL,
  "routing_key" character varying NOT NULL,
  "currency" character varying NOT NULL,
  "cost_basis_currency" character varying NULL,
  "tax_code" character varying NULL,
  "tax_behavior" character varying NULL,
  "filters" jsonb NOT NULL,
  "features" text[] NULL,
  "cost_basis" numeric NULL,
  "credit_priority" bigint NULL,
  "transaction_authorization_status" character varying NULL,
  "account_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgersubaccountroute_id" to table: "ledger_sub_account_routes"
CREATE UNIQUE INDEX "ledgersubaccountroute_id" ON "ledger_sub_account_routes" ("id");
-- Create index "ledgersubaccountroute_namespace" to table: "ledger_sub_account_routes"
CREATE INDEX "ledgersubaccountroute_namespace" ON "ledger_sub_account_routes" ("namespace");
-- Create index "ledgersubaccountroute_namespace_account_id_routing_key_version_routing_key" to table: "ledger_sub_account_routes"
CREATE UNIQUE INDEX "ledgersubaccountroute_namespace_account_id_routing_key_version_routing_key" ON "ledger_sub_account_routes" ("namespace", "account_id", "routing_key_version", "routing_key");
-- Create "ledger_transactions" table
CREATE TABLE "ledger_transactions" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "booked_at" timestamptz NOT NULL,
  "group_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgertransaction_id" to table: "ledger_transactions"
CREATE UNIQUE INDEX "ledgertransaction_id" ON "ledger_transactions" ("id");
-- Create index "ledgertransaction_namespace" to table: "ledger_transactions"
CREATE INDEX "ledgertransaction_namespace" ON "ledger_transactions" ("namespace");
-- Create index "ledgertransaction_annotations" to table: "ledger_transactions"
CREATE INDEX "ledgertransaction_annotations" ON "ledger_transactions" USING GIN ("annotations");
-- Create index "ledgertransaction_namespace_id" to table: "ledger_transactions"
CREATE UNIQUE INDEX "ledgertransaction_namespace_id" ON "ledger_transactions" ("namespace", "id");
-- Create index "ledgertransaction_namespace_group_id" to table: "ledger_transactions"
CREATE INDEX "ledgertransaction_namespace_group_id" ON "ledger_transactions" ("namespace", "group_id");
-- Create index "ledgertransaction_namespace_booked_at" to table: "ledger_transactions"
CREATE INDEX "ledgertransaction_namespace_booked_at" ON "ledger_transactions" ("namespace", "booked_at");
-- Create "ledger_transaction_groups" table
CREATE TABLE "ledger_transaction_groups" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  PRIMARY KEY ("id")
);
-- Create index "ledgertransactiongroup_id" to table: "ledger_transaction_groups"
CREATE UNIQUE INDEX "ledgertransactiongroup_id" ON "ledger_transaction_groups" ("id");
-- Create index "ledgertransactiongroup_namespace" to table: "ledger_transaction_groups"
CREATE INDEX "ledgertransactiongroup_namespace" ON "ledger_transaction_groups" ("namespace");
-- Create index "ledgertransactiongroup_annotations" to table: "ledger_transaction_groups"
CREATE INDEX "ledgertransactiongroup_annotations" ON "ledger_transaction_groups" USING GIN ("annotations");
-- Create index "ledgertransactiongroup_namespace_id" to table: "ledger_transaction_groups"
CREATE UNIQUE INDEX "ledgertransactiongroup_namespace_id" ON "ledger_transaction_groups" ("namespace", "id");
-- Create "meters" table
CREATE TABLE "meters" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "annotations" jsonb NULL,
  "event_type" character varying NOT NULL,
  "value_property" character varying NULL,
  "group_by" jsonb NULL,
  "aggregation" character varying NOT NULL,
  "event_from" timestamptz NULL,
  PRIMARY KEY ("id")
);
-- Create index "meter_id" to table: "meters"
CREATE UNIQUE INDEX "meter_id" ON "meters" ("id");
-- Create index "meter_namespace" to table: "meters"
CREATE INDEX "meter_namespace" ON "meters" ("namespace");
-- Create index "meter_namespace_id" to table: "meters"
CREATE UNIQUE INDEX "meter_namespace_id" ON "meters" ("namespace", "id");
-- Create index "meter_namespace_key_deleted_at" to table: "meters"
CREATE UNIQUE INDEX "meter_namespace_key_deleted_at" ON "meters" ("namespace", "key", "deleted_at");
-- Create index "meter_annotations" to table: "meters"
CREATE INDEX "meter_annotations" ON "meters" USING GIN ("annotations");
-- Create index "meter_namespace_key" to table: "meters"
CREATE UNIQUE INDEX "meter_namespace_key" ON "meters" ("namespace", "key") WHERE deleted_at IS NULL;
-- Create index "meter_namespace_event_type" to table: "meters"
CREATE INDEX "meter_namespace_event_type" ON "meters" ("namespace", "event_type");
-- Create "notification_channels" table
CREATE TABLE "notification_channels" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "metadata" jsonb NULL,
  "type" character varying NOT NULL,
  "name" character varying NOT NULL,
  "disabled" boolean NULL DEFAULT false,
  "config" jsonb NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "notificationchannel_id" to table: "notification_channels"
CREATE UNIQUE INDEX "notificationchannel_id" ON "notification_channels" ("id");
-- Create index "notificationchannel_namespace" to table: "notification_channels"
CREATE INDEX "notificationchannel_namespace" ON "notification_channels" ("namespace");
-- Create index "notificationchannel_annotations" to table: "notification_channels"
CREATE INDEX "notificationchannel_annotations" ON "notification_channels" USING GIN ("annotations");
-- Create index "notificationchannel_namespace_id" to table: "notification_channels"
CREATE INDEX "notificationchannel_namespace_id" ON "notification_channels" ("namespace", "id");
-- Create index "notificationchannel_namespace_type" to table: "notification_channels"
CREATE INDEX "notificationchannel_namespace_type" ON "notification_channels" ("namespace", "type");
-- Create "notification_events" table
CREATE TABLE "notification_events" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "type" character varying NOT NULL,
  "payload" jsonb NOT NULL,
  "rule_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "notificationevent_id" to table: "notification_events"
CREATE UNIQUE INDEX "notificationevent_id" ON "notification_events" ("id");
-- Create index "notificationevent_namespace" to table: "notification_events"
CREATE INDEX "notificationevent_namespace" ON "notification_events" ("namespace");
-- Create index "notificationevent_annotations" to table: "notification_events"
CREATE INDEX "notificationevent_annotations" ON "notification_events" USING GIN ("annotations");
-- Create index "notificationevent_namespace_id" to table: "notification_events"
CREATE INDEX "notificationevent_namespace_id" ON "notification_events" ("namespace", "id");
-- Create index "notificationevent_namespace_type" to table: "notification_events"
CREATE INDEX "notificationevent_namespace_type" ON "notification_events" ("namespace", "type");
-- Create "notification_event_delivery_status" table
CREATE TABLE "notification_event_delivery_status" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "event_id" character varying NOT NULL,
  "channel_id" character varying NOT NULL,
  "state" character varying NOT NULL DEFAULT 'PENDING',
  "reason" character varying NULL,
  "next_attempt_at" timestamptz NULL,
  "attempts" jsonb NULL,
  PRIMARY KEY ("id")
);
-- Create index "notificationeventdeliverystatus_id" to table: "notification_event_delivery_status"
CREATE UNIQUE INDEX "notificationeventdeliverystatus_id" ON "notification_event_delivery_status" ("id");
-- Create index "notificationeventdeliverystatus_namespace" to table: "notification_event_delivery_status"
CREATE INDEX "notificationeventdeliverystatus_namespace" ON "notification_event_delivery_status" ("namespace");
-- Create index "notificationeventdeliverystatus_annotations" to table: "notification_event_delivery_status"
CREATE INDEX "notificationeventdeliverystatus_annotations" ON "notification_event_delivery_status" USING GIN ("annotations");
-- Create index "notificationeventdeliverystatus_namespace_id" to table: "notification_event_delivery_status"
CREATE INDEX "notificationeventdeliverystatus_namespace_id" ON "notification_event_delivery_status" ("namespace", "id");
-- Create index "notificationeventdeliverystatus_namespace_event_id_channel_id" to table: "notification_event_delivery_status"
CREATE INDEX "notificationeventdeliverystatus_namespace_event_id_channel_id" ON "notification_event_delivery_status" ("namespace", "event_id", "channel_id");
-- Create index "notificationeventdeliverystatus_namespace_state" to table: "notification_event_delivery_status"
CREATE INDEX "notificationeventdeliverystatus_namespace_state" ON "notification_event_delivery_status" ("namespace", "state");
-- Create index "notificationeventdeliverystatus_namespace_state_next_attempt_at" to table: "notification_event_delivery_status"
CREATE INDEX "notificationeventdeliverystatus_namespace_state_next_attempt_at" ON "notification_event_delivery_status" ("namespace", "state", "next_attempt_at");
-- Create "notification_rules" table
CREATE TABLE "notification_rules" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "annotations" jsonb NULL,
  "metadata" jsonb NULL,
  "type" character varying NOT NULL,
  "name" character varying NOT NULL,
  "disabled" boolean NULL DEFAULT false,
  "config" jsonb NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "notificationrule_id" to table: "notification_rules"
CREATE UNIQUE INDEX "notificationrule_id" ON "notification_rules" ("id");
-- Create index "notificationrule_namespace" to table: "notification_rules"
CREATE INDEX "notificationrule_namespace" ON "notification_rules" ("namespace");
-- Create index "notificationrule_annotations" to table: "notification_rules"
CREATE INDEX "notificationrule_annotations" ON "notification_rules" USING GIN ("annotations");
-- Create index "notificationrule_namespace_id" to table: "notification_rules"
CREATE INDEX "notificationrule_namespace_id" ON "notification_rules" ("namespace", "id");
-- Create index "notificationrule_namespace_type" to table: "notification_rules"
CREATE INDEX "notificationrule_namespace_type" ON "notification_rules" ("namespace", "type");
-- Create "organization_default_tax_codes" table
CREATE TABLE "organization_default_tax_codes" (
  "id" character(26) NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "namespace" character varying NOT NULL,
  "invoicing_tax_code_id" character(26) NOT NULL,
  "credit_grant_tax_code_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "organizationdefaulttaxcodes_id" to table: "organization_default_tax_codes"
CREATE UNIQUE INDEX "organizationdefaulttaxcodes_id" ON "organization_default_tax_codes" ("id");
-- Create index "organizationdefaulttaxcodes_namespace" to table: "organization_default_tax_codes"
CREATE UNIQUE INDEX "organizationdefaulttaxcodes_namespace" ON "organization_default_tax_codes" ("namespace") WHERE deleted_at IS NULL;
-- Create "plans" table
CREATE TABLE "plans" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "version" bigint NOT NULL,
  "currency" character varying NOT NULL,
  "billing_cadence" character varying NOT NULL,
  "pro_rating_config" jsonb NOT NULL,
  "effective_from" timestamptz NULL,
  "effective_to" timestamptz NULL,
  "settlement_mode" character varying NOT NULL DEFAULT 'credit_then_invoice',
  "custom_currency_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "plan_currency_code_length" CHECK (char_length(currency) BETWEEN 3 AND 24),
  CONSTRAINT "plan_currency_reference" CHECK ((char_length(currency) = 3 AND custom_currency_id IS NULL) OR (char_length(currency) > 3 AND custom_currency_id IS NOT NULL))
);
-- Create index "plan_id" to table: "plans"
CREATE UNIQUE INDEX "plan_id" ON "plans" ("id");
-- Create index "plan_namespace" to table: "plans"
CREATE INDEX "plan_namespace" ON "plans" ("namespace");
-- Create index "plan_namespace_id" to table: "plans"
CREATE UNIQUE INDEX "plan_namespace_id" ON "plans" ("namespace", "id");
-- Create index "plan_namespace_key_deleted_at" to table: "plans"
CREATE UNIQUE INDEX "plan_namespace_key_deleted_at" ON "plans" ("namespace", "key", "deleted_at");
-- Create index "plan_namespace_key_version" to table: "plans"
CREATE UNIQUE INDEX "plan_namespace_key_version" ON "plans" ("namespace", "key", "version") WHERE deleted_at IS NULL;
-- Create index "plan_custom_currency_id" to table: "plans"
CREATE INDEX "plan_custom_currency_id" ON "plans" ("custom_currency_id");
-- Create "plan_addons" table
CREATE TABLE "plan_addons" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "from_plan_phase" character varying NOT NULL,
  "max_quantity" bigint NULL,
  "addon_id" character(26) NOT NULL,
  "plan_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "planaddon_id" to table: "plan_addons"
CREATE UNIQUE INDEX "planaddon_id" ON "plan_addons" ("id");
-- Create index "planaddon_namespace" to table: "plan_addons"
CREATE INDEX "planaddon_namespace" ON "plan_addons" ("namespace");
-- Create index "planaddon_annotations" to table: "plan_addons"
CREATE INDEX "planaddon_annotations" ON "plan_addons" USING GIN ("annotations");
-- Create index "planaddon_namespace_plan_id_addon_id" to table: "plan_addons"
CREATE UNIQUE INDEX "planaddon_namespace_plan_id_addon_id" ON "plan_addons" ("namespace", "plan_id", "addon_id") WHERE deleted_at IS NULL;
-- Create "plan_phases" table
CREATE TABLE "plan_phases" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "index" smallint NOT NULL,
  "duration" character varying NULL,
  "plan_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "planphase_id" to table: "plan_phases"
CREATE UNIQUE INDEX "planphase_id" ON "plan_phases" ("id");
-- Create index "planphase_namespace" to table: "plan_phases"
CREATE INDEX "planphase_namespace" ON "plan_phases" ("namespace");
-- Create index "planphase_namespace_id" to table: "plan_phases"
CREATE UNIQUE INDEX "planphase_namespace_id" ON "plan_phases" ("namespace", "id");
-- Create index "planphase_namespace_key_deleted_at" to table: "plan_phases"
CREATE UNIQUE INDEX "planphase_namespace_key_deleted_at" ON "plan_phases" ("namespace", "key", "deleted_at");
-- Create index "planphase_namespace_key" to table: "plan_phases"
CREATE INDEX "planphase_namespace_key" ON "plan_phases" ("namespace", "key");
-- Create index "planphase_plan_id_key" to table: "plan_phases"
CREATE UNIQUE INDEX "planphase_plan_id_key" ON "plan_phases" ("plan_id", "key") WHERE deleted_at IS NULL;
-- Create index "planphase_plan_id_index" to table: "plan_phases"
CREATE UNIQUE INDEX "planphase_plan_id_index" ON "plan_phases" ("plan_id", "index") WHERE deleted_at IS NULL;
-- Create "plan_rate_cards" table
CREATE TABLE "plan_rate_cards" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "tax_behavior" character varying NULL,
  "type" character varying NOT NULL,
  "feature_key" character varying NULL,
  "annotations" jsonb NULL,
  "entitlement_template" jsonb NULL,
  "tax_config" jsonb NULL,
  "billing_cadence" character varying NULL,
  "price" jsonb NULL,
  "currency" character varying NULL,
  "discounts" jsonb NULL,
  "unit_config" jsonb NULL,
  "custom_currency_id" character(26) NULL,
  "feature_id" character(26) NULL,
  "phase_id" character(26) NOT NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "plan_rate_card_currency_code_length" CHECK (currency IS NULL OR char_length(currency) BETWEEN 3 AND 24),
  CONSTRAINT "plan_rate_card_currency_has_price" CHECK (price IS NOT NULL OR currency IS NULL),
  CONSTRAINT "plan_rate_card_currency_reference" CHECK ((currency IS NULL AND custom_currency_id IS NULL) OR (currency IS NOT NULL AND char_length(currency) = 3 AND custom_currency_id IS NULL) OR (currency IS NOT NULL AND char_length(currency) > 3 AND custom_currency_id IS NOT NULL)),
  CONSTRAINT "plan_rate_card_feature_reference" CHECK ((feature_key IS NULL AND feature_id IS NULL) OR (feature_key IS NOT NULL AND feature_key <> '' AND feature_id IS NOT NULL AND feature_id <> '')),
  CONSTRAINT "plan_rate_card_tax_behavior_consistency" CHECK (tax_behavior IS NOT DISTINCT FROM tax_config ->> 'behavior'),
  CONSTRAINT "plan_rate_card_tax_code_consistency" CHECK ((tax_code_id::text IS NOT DISTINCT FROM tax_config ->> 'tax_code_id') AND (NULLIF(btrim(tax_config -> 'stripe' ->> 'code'), '') IS NULL OR tax_code_id IS NOT NULL))
);
-- Create index "planratecard_id" to table: "plan_rate_cards"
CREATE UNIQUE INDEX "planratecard_id" ON "plan_rate_cards" ("id");
-- Create index "planratecard_namespace" to table: "plan_rate_cards"
CREATE INDEX "planratecard_namespace" ON "plan_rate_cards" ("namespace");
-- Create index "planratecard_namespace_id" to table: "plan_rate_cards"
CREATE UNIQUE INDEX "planratecard_namespace_id" ON "plan_rate_cards" ("namespace", "id");
-- Create index "planratecard_namespace_key_deleted_at" to table: "plan_rate_cards"
CREATE UNIQUE INDEX "planratecard_namespace_key_deleted_at" ON "plan_rate_cards" ("namespace", "key", "deleted_at");
-- Create index "planratecard_tax_code_id" to table: "plan_rate_cards"
CREATE INDEX "planratecard_tax_code_id" ON "plan_rate_cards" ("tax_code_id");
-- Create index "planratecard_phase_id_key" to table: "plan_rate_cards"
CREATE UNIQUE INDEX "planratecard_phase_id_key" ON "plan_rate_cards" ("phase_id", "key") WHERE deleted_at IS NULL;
-- Create index "planratecard_phase_id_feature_key" to table: "plan_rate_cards"
CREATE UNIQUE INDEX "planratecard_phase_id_feature_key" ON "plan_rate_cards" ("phase_id", "feature_key") WHERE deleted_at IS NULL;
-- Create index "planratecard_custom_currency_id" to table: "plan_rate_cards"
CREATE INDEX "planratecard_custom_currency_id" ON "plan_rate_cards" ("custom_currency_id");
-- Create "subjects" table
CREATE TABLE "subjects" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "key" character varying NOT NULL,
  "display_name" character varying NULL,
  "stripe_customer_id" character varying NULL,
  "metadata" jsonb NULL,
  PRIMARY KEY ("id")
);
-- Create index "subject_id" to table: "subjects"
CREATE UNIQUE INDEX "subject_id" ON "subjects" ("id");
-- Create index "subject_namespace" to table: "subjects"
CREATE INDEX "subject_namespace" ON "subjects" ("namespace");
-- Create index "subject_namespace_key" to table: "subjects"
CREATE UNIQUE INDEX "subject_namespace_key" ON "subjects" ("namespace", "key") WHERE deleted_at IS NULL;
-- Create index "subject_namespace_key_deleted_at_unique" to table: "subjects"
CREATE UNIQUE INDEX "subject_namespace_key_deleted_at_unique" ON "subjects" ("namespace", "key", "deleted_at") WHERE deleted_at IS NULL;
-- Create index "subject_namespace_key_deleted_at" to table: "subjects"
CREATE INDEX "subject_namespace_key_deleted_at" ON "subjects" ("namespace", "key", "deleted_at");
-- Create index "subject_namespace_id" to table: "subjects"
CREATE UNIQUE INDEX "subject_namespace_id" ON "subjects" ("namespace", "id");
-- Create index "subject_display_name" to table: "subjects"
CREATE INDEX "subject_display_name" ON "subjects" ("display_name");
-- Create index "subject_created_at_id" to table: "subjects"
CREATE INDEX "subject_created_at_id" ON "subjects" ("created_at", "id");
-- Create "subscriptions" table
CREATE TABLE "subscriptions" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "metadata" jsonb NULL,
  "active_from" timestamptz NOT NULL,
  "active_to" timestamptz NULL,
  "name" character varying NOT NULL DEFAULT 'Subscription',
  "description" character varying NULL,
  "currency" character varying NOT NULL,
  "cost_basis_mode" character varying NOT NULL DEFAULT 'dynamic',
  "billing_anchor" timestamptz NOT NULL,
  "billing_cadence" character varying NOT NULL,
  "pro_rating_config" jsonb NOT NULL,
  "settlement_mode" character varying NOT NULL DEFAULT 'credit_then_invoice',
  "customer_id" character(26) NOT NULL,
  "plan_id" character(26) NULL,
  PRIMARY KEY ("id")
);
-- Create index "subscription_id" to table: "subscriptions"
CREATE UNIQUE INDEX "subscription_id" ON "subscriptions" ("id");
-- Create index "subscription_namespace" to table: "subscriptions"
CREATE INDEX "subscription_namespace" ON "subscriptions" ("namespace");
-- Create index "subscription_annotations" to table: "subscriptions"
CREATE INDEX "subscription_annotations" ON "subscriptions" USING GIN ("annotations");
-- Create index "subscription_namespace_id" to table: "subscriptions"
CREATE INDEX "subscription_namespace_id" ON "subscriptions" ("namespace", "id");
-- Create index "subscription_namespace_customer_id" to table: "subscriptions"
CREATE INDEX "subscription_namespace_customer_id" ON "subscriptions" ("namespace", "customer_id");
-- Create "subscription_addons" table
CREATE TABLE "subscription_addons" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "addon_id" character(26) NOT NULL,
  "subscription_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "subscriptionaddon_id" to table: "subscription_addons"
CREATE UNIQUE INDEX "subscriptionaddon_id" ON "subscription_addons" ("id");
-- Create index "subscriptionaddon_namespace" to table: "subscription_addons"
CREATE INDEX "subscriptionaddon_namespace" ON "subscription_addons" ("namespace");
-- Create index "subscriptionaddon_namespace_subscription_id_addon_id" to table: "subscription_addons"
CREATE UNIQUE INDEX "subscriptionaddon_namespace_subscription_id_addon_id" ON "subscription_addons" ("namespace", "subscription_id", "addon_id") WHERE deleted_at IS NULL;
-- Create "subscription_addon_quantities" table
CREATE TABLE "subscription_addon_quantities" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "active_from" timestamptz NOT NULL,
  "quantity" bigint NOT NULL DEFAULT 1,
  "subscription_addon_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "subscriptionaddonquantity_id" to table: "subscription_addon_quantities"
CREATE UNIQUE INDEX "subscriptionaddonquantity_id" ON "subscription_addon_quantities" ("id");
-- Create index "subscriptionaddonquantity_namespace" to table: "subscription_addon_quantities"
CREATE INDEX "subscriptionaddonquantity_namespace" ON "subscription_addon_quantities" ("namespace");
-- Create index "subscriptionaddonquantity_subscription_addon_id" to table: "subscription_addon_quantities"
CREATE INDEX "subscriptionaddonquantity_subscription_addon_id" ON "subscription_addon_quantities" ("subscription_addon_id");
-- Create "subscription_billing_sync_states" table
CREATE TABLE "subscription_billing_sync_states" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "has_billables" boolean NOT NULL,
  "synced_at" timestamptz NOT NULL,
  "next_sync_after" timestamptz NULL,
  "subscription_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "subscription_billing_sync_states_subscription_id_key" to table: "subscription_billing_sync_states"
CREATE UNIQUE INDEX "subscription_billing_sync_states_subscription_id_key" ON "subscription_billing_sync_states" ("subscription_id");
-- Create index "subscriptionbillingsyncstate_id" to table: "subscription_billing_sync_states"
CREATE UNIQUE INDEX "subscriptionbillingsyncstate_id" ON "subscription_billing_sync_states" ("id");
-- Create index "subscriptionbillingsyncstate_namespace" to table: "subscription_billing_sync_states"
CREATE INDEX "subscriptionbillingsyncstate_namespace" ON "subscription_billing_sync_states" ("namespace");
-- Create index "subscriptionbillingsyncstate_namespace_subscription_id" to table: "subscription_billing_sync_states"
CREATE UNIQUE INDEX "subscriptionbillingsyncstate_namespace_subscription_id" ON "subscription_billing_sync_states" ("namespace", "subscription_id");
-- Create "subscription_cost_basis_pins" table
CREATE TABLE "subscription_cost_basis_pins" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "invoice_currency" character varying NOT NULL,
  "cost_basis_id" character(26) NOT NULL,
  "custom_currency_id" character(26) NOT NULL,
  "subscription_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "subscriptioncostbasispin_id" to table: "subscription_cost_basis_pins"
CREATE UNIQUE INDEX "subscriptioncostbasispin_id" ON "subscription_cost_basis_pins" ("id");
-- Create index "subscriptioncostbasispin_namespace" to table: "subscription_cost_basis_pins"
CREATE INDEX "subscriptioncostbasispin_namespace" ON "subscription_cost_basis_pins" ("namespace");
-- Create index "subscriptioncostbasispin_namespace_subscription_id_custom_currency_id_invoice_currency" to table: "subscription_cost_basis_pins"
CREATE UNIQUE INDEX "subscriptioncostbasispin_namespace_subscription_id_custom_currency_id_invoice_currency" ON "subscription_cost_basis_pins" ("namespace", "subscription_id", "custom_currency_id", "invoice_currency");
-- Create index "subscriptioncostbasispin_subscription_id" to table: "subscription_cost_basis_pins"
CREATE INDEX "subscriptioncostbasispin_subscription_id" ON "subscription_cost_basis_pins" ("subscription_id");
-- Create index "subscriptioncostbasispin_custom_currency_id" to table: "subscription_cost_basis_pins"
CREATE INDEX "subscriptioncostbasispin_custom_currency_id" ON "subscription_cost_basis_pins" ("custom_currency_id");
-- Create index "subscriptioncostbasispin_cost_basis_id" to table: "subscription_cost_basis_pins"
CREATE INDEX "subscriptioncostbasispin_cost_basis_id" ON "subscription_cost_basis_pins" ("cost_basis_id");
-- Create "subscription_items" table
CREATE TABLE "subscription_items" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "metadata" jsonb NULL,
  "tax_behavior" character varying NULL,
  "annotations" jsonb NULL,
  "active_from" timestamptz NOT NULL,
  "active_to" timestamptz NULL,
  "key" character varying NOT NULL,
  "restarts_billing_period" boolean NULL,
  "active_from_override_relative_to_phase_start" character varying NULL,
  "active_to_override_relative_to_phase_start" character varying NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "feature_key" character varying NULL,
  "entitlement_template" jsonb NULL,
  "tax_config" jsonb NULL,
  "billing_cadence" character varying NULL,
  "price" jsonb NULL,
  "currency" character varying NULL,
  "discounts" jsonb NULL,
  "unit_config" jsonb NULL,
  "custom_currency_id" character(26) NULL,
  "entitlement_id" character(26) NULL,
  "phase_id" character(26) NOT NULL,
  "tax_code_id" character(26) NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "subscription_item_currency_code_length" CHECK (currency IS NULL OR char_length(currency) BETWEEN 3 AND 24),
  CONSTRAINT "subscription_item_currency_has_price" CHECK (((price IS NULL) AND (currency IS NULL) AND (custom_currency_id IS NULL)) OR ((price IS NOT NULL) AND (currency IS NOT NULL))),
  CONSTRAINT "subscription_item_currency_reference" CHECK ((currency IS NULL AND custom_currency_id IS NULL) OR (currency IS NOT NULL AND char_length(currency) = 3 AND custom_currency_id IS NULL) OR (currency IS NOT NULL AND char_length(currency) > 3 AND custom_currency_id IS NOT NULL)),
  CONSTRAINT "subscription_item_tax_behavior_consistency" CHECK (tax_behavior IS NOT DISTINCT FROM tax_config ->> 'behavior'),
  CONSTRAINT "subscription_item_tax_code_consistency" CHECK ((tax_code_id::text IS NOT DISTINCT FROM tax_config ->> 'tax_code_id') AND (NULLIF(btrim(tax_config -> 'stripe' ->> 'code'), '') IS NULL OR tax_code_id IS NOT NULL))
);
-- Create index "subscriptionitem_id" to table: "subscription_items"
CREATE UNIQUE INDEX "subscriptionitem_id" ON "subscription_items" ("id");
-- Create index "subscriptionitem_namespace" to table: "subscription_items"
CREATE INDEX "subscriptionitem_namespace" ON "subscription_items" ("namespace");
-- Create index "subscriptionitem_tax_code_id" to table: "subscription_items"
CREATE INDEX "subscriptionitem_tax_code_id" ON "subscription_items" ("tax_code_id");
-- Create index "subscriptionitem_namespace_id" to table: "subscription_items"
CREATE INDEX "subscriptionitem_namespace_id" ON "subscription_items" ("namespace", "id");
-- Create index "subscriptionitem_namespace_phase_id_key" to table: "subscription_items"
CREATE INDEX "subscriptionitem_namespace_phase_id_key" ON "subscription_items" ("namespace", "phase_id", "key");
-- Create index "subscriptionitem_custom_currency_id" to table: "subscription_items"
CREATE INDEX "subscriptionitem_custom_currency_id" ON "subscription_items" ("custom_currency_id");
-- Create "subscription_phases" table
CREATE TABLE "subscription_phases" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "metadata" jsonb NULL,
  "key" character varying NOT NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "active_from" timestamptz NOT NULL,
  "sort_hint" smallint NULL,
  "subscription_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "subscriptionphase_id" to table: "subscription_phases"
CREATE UNIQUE INDEX "subscriptionphase_id" ON "subscription_phases" ("id");
-- Create index "subscriptionphase_namespace" to table: "subscription_phases"
CREATE INDEX "subscriptionphase_namespace" ON "subscription_phases" ("namespace");
-- Create index "subscriptionphase_namespace_id" to table: "subscription_phases"
CREATE INDEX "subscriptionphase_namespace_id" ON "subscription_phases" ("namespace", "id");
-- Create index "subscriptionphase_namespace_subscription_id" to table: "subscription_phases"
CREATE INDEX "subscriptionphase_namespace_subscription_id" ON "subscription_phases" ("namespace", "subscription_id");
-- Create index "subscriptionphase_namespace_subscription_id_key" to table: "subscription_phases"
CREATE INDEX "subscriptionphase_namespace_subscription_id_key" ON "subscription_phases" ("namespace", "subscription_id", "key");
-- Create "tax_codes" table
CREATE TABLE "tax_codes" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "name" character varying NOT NULL,
  "description" character varying NULL,
  "key" character varying NOT NULL,
  "annotations" jsonb NULL,
  "app_mappings" jsonb NULL,
  PRIMARY KEY ("id")
);
-- Create index "taxcode_id" to table: "tax_codes"
CREATE UNIQUE INDEX "taxcode_id" ON "tax_codes" ("id");
-- Create index "taxcode_namespace" to table: "tax_codes"
CREATE INDEX "taxcode_namespace" ON "tax_codes" ("namespace");
-- Create index "taxcode_namespace_id" to table: "tax_codes"
CREATE UNIQUE INDEX "taxcode_namespace_id" ON "tax_codes" ("namespace", "id");
-- Create index "taxcode_namespace_key_deleted_at" to table: "tax_codes"
CREATE UNIQUE INDEX "taxcode_namespace_key_deleted_at" ON "tax_codes" ("namespace", "key", "deleted_at");
-- Create index "taxcode_annotations" to table: "tax_codes"
CREATE INDEX "taxcode_annotations" ON "tax_codes" USING GIN ("annotations");
-- Create index "taxcode_namespace_key" to table: "tax_codes"
CREATE UNIQUE INDEX "taxcode_namespace_key" ON "tax_codes" ("namespace", "key") WHERE deleted_at IS NULL;
-- Create "usage_resets" table
CREATE TABLE "usage_resets" (
  "id" character(26) NOT NULL,
  "namespace" character varying NOT NULL,
  "annotations" jsonb NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "deleted_at" timestamptz NULL,
  "reset_time" timestamptz NOT NULL,
  "anchor" timestamptz NOT NULL,
  "usage_period_interval" character varying NOT NULL,
  "entitlement_id" character(26) NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "usagereset_id" to table: "usage_resets"
CREATE UNIQUE INDEX "usagereset_id" ON "usage_resets" ("id");
-- Create index "usagereset_namespace" to table: "usage_resets"
CREATE INDEX "usagereset_namespace" ON "usage_resets" ("namespace");
-- Create index "usagereset_annotations" to table: "usage_resets"
CREATE INDEX "usagereset_annotations" ON "usage_resets" USING GIN ("annotations");
-- Create index "usagereset_namespace_entitlement_id" to table: "usage_resets"
CREATE INDEX "usagereset_namespace_entitlement_id" ON "usage_resets" ("namespace", "entitlement_id");
-- Create index "usagereset_namespace_entitlement_id_reset_time" to table: "usage_resets"
CREATE INDEX "usagereset_namespace_entitlement_id_reset_time" ON "usage_resets" ("namespace", "entitlement_id", "reset_time");
-- Create "notification_channel_rules" table
CREATE TABLE "notification_channel_rules" (
  "notification_channel_id" character(26) NOT NULL,
  "notification_rule_id" character(26) NOT NULL,
  PRIMARY KEY ("notification_channel_id", "notification_rule_id")
);
-- Create "notification_event_delivery_status_events" table
CREATE TABLE "notification_event_delivery_status_events" (
  "notification_event_delivery_status_id" character(26) NOT NULL,
  "notification_event_id" character(26) NOT NULL,
  PRIMARY KEY ("notification_event_delivery_status_id", "notification_event_id")
);
-- Modify "addons" table
ALTER TABLE "addons" ADD
CONSTRAINT "addons_custom_currencies_addons" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT;
-- Modify "addon_rate_cards" table
ALTER TABLE "addon_rate_cards" ADD
CONSTRAINT "addon_rate_cards_addons_ratecards" FOREIGN KEY ("addon_id") REFERENCES "addons" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "addon_rate_cards_custom_currencies_addon_rate_cards" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "addon_rate_cards_features_addon_ratecard" FOREIGN KEY ("feature_id") REFERENCES "features" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "addon_rate_cards_tax_codes_addon_rate_cards" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "app_custom_invoicings" table
ALTER TABLE "app_custom_invoicings" ADD
CONSTRAINT "app_custom_invoicings_apps_app" FOREIGN KEY ("id") REFERENCES "apps" ("id") ON DELETE CASCADE;
-- Modify "app_custom_invoicing_customers" table
ALTER TABLE "app_custom_invoicing_customers" ADD
CONSTRAINT "app_custom_invoicing_customers_app_custom_invoicings_customer_apps" FOREIGN KEY ("app_id") REFERENCES "app_custom_invoicings" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "app_custom_invoicing_customers_customers_customer" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE CASCADE;
-- Modify "app_customers" table
ALTER TABLE "app_customers" ADD
CONSTRAINT "app_customers_apps_customer_apps" FOREIGN KEY ("app_id") REFERENCES "apps" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "app_customers_customers_apps" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE CASCADE;
-- Modify "app_stripes" table
ALTER TABLE "app_stripes" ADD
CONSTRAINT "app_stripes_apps_app" FOREIGN KEY ("id") REFERENCES "apps" ("id") ON DELETE CASCADE;
-- Modify "app_stripe_customers" table
ALTER TABLE "app_stripe_customers" ADD
CONSTRAINT "app_stripe_customers_app_stripes_customer_apps" FOREIGN KEY ("app_id") REFERENCES "app_stripes" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "app_stripe_customers_customers_customer" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE CASCADE;
-- Modify "balance_snapshots" table
ALTER TABLE "balance_snapshots" ADD
CONSTRAINT "balance_snapshots_entitlements_balance_snapshot" FOREIGN KEY ("owner_id") REFERENCES "entitlements" ("id") ON DELETE CASCADE;
-- Modify "billing_customer_overrides" table
ALTER TABLE "billing_customer_overrides" ADD
CONSTRAINT "billing_customer_overrides_billing_profiles_billing_customer_override" FOREIGN KEY ("billing_profile_id") REFERENCES "billing_profiles" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_customer_overrides_customers_billing_customer_override" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "billing_customer_overrides_tax_codes_billing_customer_overrides" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "billing_gathering_invoice_lines" table
ALTER TABLE "billing_gathering_invoice_lines" ADD
CONSTRAINT "billing_gathering_line_invoice_fk" FOREIGN KEY ("invoice_id") REFERENCES "billing_invoices" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "billing_gathering_line_split_group_fk" FOREIGN KEY ("split_line_group_id") REFERENCES "billing_invoice_split_line_groups" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_gathering_line_charge_fk" FOREIGN KEY ("charge_id") REFERENCES "charges" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_gathering_line_subscription_fk" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_gathering_line_subscription_item_fk" FOREIGN KEY ("subscription_item_id") REFERENCES "subscription_items" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_gathering_line_subscription_phase_fk" FOREIGN KEY ("subscription_phase_id") REFERENCES "subscription_phases" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_gathering_line_tax_code_fk" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "billing_invoices" table
ALTER TABLE "billing_invoices" ADD
CONSTRAINT "billing_invoices_apps_billing_invoice_tax_app" FOREIGN KEY ("tax_app_id") REFERENCES "apps" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_invoices_apps_billing_invoice_invoicing_app" FOREIGN KEY ("invoicing_app_id") REFERENCES "apps" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_invoices_apps_billing_invoice_payment_app" FOREIGN KEY ("payment_app_id") REFERENCES "apps" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_invoices_billing_profiles_billing_invoices" FOREIGN KEY ("source_billing_profile_id") REFERENCES "billing_profiles" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_invoices_billing_workflow_configs_billing_invoices" FOREIGN KEY ("workflow_config_id") REFERENCES "billing_workflow_configs" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_invoices_customers_billing_invoice" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE NO ACTION;
-- Modify "billing_invoice_lines" table
ALTER TABLE "billing_invoice_lines" ADD
CONSTRAINT "billing_invoice_lines_billing_invoices_billing_invoice_lines" FOREIGN KEY ("invoice_id") REFERENCES "billing_invoices" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "billing_invoice_lines_billing_invoice_usage_based_line_configs_usage_based_line" FOREIGN KEY ("usage_based_line_config_id") REFERENCES "billing_invoice_usage_based_line_configs" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "billing_invoice_lines_billing_invoice_split_line_groups_billing_invoice_lines" FOREIGN KEY ("split_line_group_id") REFERENCES "billing_invoice_split_line_groups" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_lines_charges_billing_invoice_lines" FOREIGN KEY ("charge_id") REFERENCES "charges" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_lines_subscriptions_billing_lines" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_lines_subscription_items_billing_lines" FOREIGN KEY ("subscription_item_id") REFERENCES "subscription_items" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_lines_subscription_phases_billing_lines" FOREIGN KEY ("subscription_phase_id") REFERENCES "subscription_phases" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_lines_tax_codes_billing_invoice_lines" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "billing_invoice_line_usage_discounts" table
ALTER TABLE "billing_invoice_line_usage_discounts" ADD
CONSTRAINT "billing_invoice_line_usage_discounts_billing_invoice_lines_line_usage_discounts" FOREIGN KEY ("line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE CASCADE;
-- Modify "billing_invoice_split_line_groups" table
ALTER TABLE "billing_invoice_split_line_groups" ADD
CONSTRAINT "billing_invoice_split_line_groups_charges_billing_split_line_groups" FOREIGN KEY ("charge_id") REFERENCES "charges" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_split_line_groups_subscriptions_billing_split_line_groups" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_split_line_groups_subscription_items_billing_split_line_groups" FOREIGN KEY ("subscription_item_id") REFERENCES "subscription_items" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "billing_invoice_split_line_groups_subscription_phases_billing_split_line_groups" FOREIGN KEY ("subscription_phase_id") REFERENCES "subscription_phases" ("id") ON DELETE SET NULL;
-- Modify "billing_invoice_validation_issues" table
ALTER TABLE "billing_invoice_validation_issues" ADD
CONSTRAINT "billing_invoice_validation_issues_billing_invoices_billing_invoice_validation_issues" FOREIGN KEY ("invoice_id") REFERENCES "billing_invoices" ("id") ON DELETE CASCADE;
-- Modify "billing_profiles" table
ALTER TABLE "billing_profiles" ADD
CONSTRAINT "billing_profiles_apps_billing_profile_tax_app" FOREIGN KEY ("tax_app_id") REFERENCES "apps" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_profiles_apps_billing_profile_invoicing_app" FOREIGN KEY ("invoicing_app_id") REFERENCES "apps" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_profiles_apps_billing_profile_payment_app" FOREIGN KEY ("payment_app_id") REFERENCES "apps" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "billing_profiles_billing_workflow_configs_billing_profile" FOREIGN KEY ("workflow_config_id") REFERENCES "billing_workflow_configs" ("id") ON DELETE NO ACTION;
-- Modify "billing_standard_invoice_detailed_lines" table
ALTER TABLE "billing_standard_invoice_detailed_lines" ADD
CONSTRAINT "billing_standard_invoice_detailed_lines_billing_invoices_billing_invoice_detailed_lines" FOREIGN KEY ("invoice_id") REFERENCES "billing_invoices" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "billing_standard_invoice_detailed_lines_billing_invoice_lines_detailed_lines_v2" FOREIGN KEY ("parent_line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE CASCADE;
-- Modify "billing_standard_invoice_detailed_line_amount_discounts" table
ALTER TABLE "billing_standard_invoice_detailed_line_amount_discounts" ADD
CONSTRAINT "billing_standard_invoice_detailed_line_amount_discounts_billing_standard_invoice_detailed_lines_amount_discounts" FOREIGN KEY ("line_id") REFERENCES "billing_standard_invoice_detailed_lines" ("id") ON DELETE CASCADE;
-- Modify "billing_workflow_configs" table
ALTER TABLE "billing_workflow_configs" ADD
CONSTRAINT "billing_workflow_configs_tax_codes_billing_workflow_configs" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "charges" table
ALTER TABLE "charges" ADD
CONSTRAINT "charges_charge_credit_purchases_charge" FOREIGN KEY ("charge_credit_purchase_id") REFERENCES "charge_credit_purchases" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charges_charge_flat_fees_charge" FOREIGN KEY ("charge_flat_fee_id") REFERENCES "charge_flat_fees" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charges_charge_usage_based_charge" FOREIGN KEY ("charge_usage_based_id") REFERENCES "charge_usage_based" ("id") ON DELETE CASCADE;
-- Modify "charge_credit_purchases" table
ALTER TABLE "charge_credit_purchases" ADD
CONSTRAINT "charge_credit_purchase_cost_basis_charge_fk" FOREIGN KEY ("cost_basis_id") REFERENCES "charge_credit_purchase_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_credit_purchases_custom_currencies_charges_credit_purchase" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_credit_purchases_customers_charges_credit_purchase" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "charge_credit_purchases_subscriptions_charges_credit_purchase" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_credit_purchases_subscription_items_charges_credit_purchase" FOREIGN KEY ("subscription_item_id") REFERENCES "subscription_items" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_credit_purchases_subscription_phases_charges_credit_purchase" FOREIGN KEY ("subscription_phase_id") REFERENCES "subscription_phases" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_credit_purchases_tax_codes_charge_credit_purchases" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE NO ACTION;
-- Modify "charge_credit_purchase_cost_bases" table
ALTER TABLE "charge_credit_purchase_cost_bases" ADD
CONSTRAINT "charge_credit_purchase_cost_basis_currency_cost_basis_fk" FOREIGN KEY ("currency_cost_basis_id") REFERENCES "currency_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_credit_purchase_cost_basis_resolved_cost_basis_fk" FOREIGN KEY ("resolved_cost_basis_id") REFERENCES "currency_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_credit_purchase_cost_basis_currency_fk" FOREIGN KEY ("currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT;
-- Modify "charge_credit_purchase_credit_grants" table
ALTER TABLE "charge_credit_purchase_credit_grants" ADD
CONSTRAINT "charge_credit_purchase_credit_grants_charge_credit_purchases_credit_grant" FOREIGN KEY ("charge_id") REFERENCES "charge_credit_purchases" ("id") ON DELETE CASCADE;
-- Modify "charge_credit_purchase_external_payments" table
ALTER TABLE "charge_credit_purchase_external_payments" ADD
CONSTRAINT "charge_credit_purchase_external_payments_charge_credit_purchases_external_payment" FOREIGN KEY ("charge_id") REFERENCES "charge_credit_purchases" ("id") ON DELETE CASCADE;
-- Modify "charge_credit_purchase_invoiced_payments" table
ALTER TABLE "charge_credit_purchase_invoiced_payments" ADD
CONSTRAINT "charge_credit_purchase_invoiced_payments_billing_invoice_lines_charge_credit_purchase_invoiced_payment" FOREIGN KEY ("line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charge_credit_purchase_invoiced_payments_charge_credit_purchases_invoiced_payment" FOREIGN KEY ("charge_id") REFERENCES "charge_credit_purchases" ("id") ON DELETE CASCADE;
-- Modify "charge_flat_fees" table
ALTER TABLE "charge_flat_fees" ADD
CONSTRAINT "charge_flat_fees_charge_flat_fee_runs_current_run" FOREIGN KEY ("current_realization_run_id") REFERENCES "charge_flat_fee_runs" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_flat_fee_cost_basis_charge_fk" FOREIGN KEY ("cost_basis_id") REFERENCES "charge_flat_fee_cost_bases" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charge_flat_fees_custom_currencies_charges_flat_fee" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_flat_fees_customers_charges_flat_fee" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "charge_flat_fees_features_flat_fee_charges" FOREIGN KEY ("feature_id") REFERENCES "features" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_flat_fees_subscriptions_charges_flat_fee" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_flat_fees_subscription_items_charges_flat_fee" FOREIGN KEY ("subscription_item_id") REFERENCES "subscription_items" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_flat_fees_subscription_phases_charges_flat_fee" FOREIGN KEY ("subscription_phase_id") REFERENCES "subscription_phases" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_flat_fees_tax_codes_charge_flat_fees" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE NO ACTION;
-- Modify "charge_flat_fee_cost_bases" table
ALTER TABLE "charge_flat_fee_cost_bases" ADD
CONSTRAINT "charge_flat_fee_cost_basis_currency_cost_basis_fk" FOREIGN KEY ("currency_cost_basis_id") REFERENCES "currency_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_flat_fee_cost_basis_resolved_cost_basis_fk" FOREIGN KEY ("resolved_cost_basis_id") REFERENCES "currency_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_flat_fee_cost_basis_currency_fk" FOREIGN KEY ("currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT;
-- Modify "charge_flat_fee_overrides" table
ALTER TABLE "charge_flat_fee_overrides" ADD
CONSTRAINT "charge_flat_fee_overrides_charge_flat_fees_intent_override" FOREIGN KEY ("charge_id") REFERENCES "charge_flat_fees" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charge_flat_fee_overrides_tax_codes_charge_flat_fee_overrides" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "charge_flat_fee_runs" table
ALTER TABLE "charge_flat_fee_runs" ADD
CONSTRAINT "charge_flat_fee_runs_billing_invoices_charge_flat_fee_runs" FOREIGN KEY ("invoice_id") REFERENCES "billing_invoices" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_flat_fee_runs_billing_invoice_lines_charge_flat_fee_runs" FOREIGN KEY ("line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_flat_fee_runs_charge_flat_fees_runs" FOREIGN KEY ("charge_id") REFERENCES "charge_flat_fees" ("id") ON DELETE CASCADE;
-- Modify "charge_flat_fee_run_credit_allocations" table
ALTER TABLE "charge_flat_fee_run_credit_allocations" ADD
CONSTRAINT "charge_flat_fee_run_credit_allocations_billing_invoice_lines_charge_flat_fee_run_credit_allocations" FOREIGN KEY ("line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_ff_credit_alloc_run" FOREIGN KEY ("run_id") REFERENCES "charge_flat_fee_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_flat_fee_run_detailed_lines" table
ALTER TABLE "charge_flat_fee_run_detailed_lines" ADD
CONSTRAINT "charge_flat_fee_run_detailed_lines_charge_flat_fee_runs_detailed_lines" FOREIGN KEY ("run_id") REFERENCES "charge_flat_fee_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_flat_fee_run_invoiced_usages" table
ALTER TABLE "charge_flat_fee_run_invoiced_usages" ADD
CONSTRAINT "charge_flat_fee_run_invoiced_usages_charge_flat_fee_runs_invoiced_usage" FOREIGN KEY ("run_id") REFERENCES "charge_flat_fee_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_flat_fee_run_overage_credit_allocations" table
ALTER TABLE "charge_flat_fee_run_overage_credit_allocations" ADD
CONSTRAINT "billing_line_charge_ff_overage_credit_alloc" FOREIGN KEY ("line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_ff_overage_credit_alloc_run" FOREIGN KEY ("run_id") REFERENCES "charge_flat_fee_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_flat_fee_run_payments" table
ALTER TABLE "charge_flat_fee_run_payments" ADD
CONSTRAINT "charge_flat_fee_run_payments_billing_invoice_lines_charge_flat_fee_run_payment" FOREIGN KEY ("line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "charge_flat_fee_run_payments_charge_flat_fee_runs_payment" FOREIGN KEY ("run_id") REFERENCES "charge_flat_fee_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_usage_based" table
ALTER TABLE "charge_usage_based" ADD
CONSTRAINT "charge_usage_based_charge_usage_based_runs_current_run" FOREIGN KEY ("current_realization_run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_usage_based_cost_basis_charge_fk" FOREIGN KEY ("cost_basis_id") REFERENCES "charge_usage_based_cost_bases" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charge_usage_based_custom_currencies_charges_usage_based" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_usage_based_customers_charges_usage_based" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "charge_usage_based_features_usage_based_charges" FOREIGN KEY ("feature_id") REFERENCES "features" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_usage_based_subscriptions_charges_usage_based" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_usage_based_subscription_items_charges_usage_based" FOREIGN KEY ("subscription_item_id") REFERENCES "subscription_items" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_usage_based_subscription_phases_charges_usage_based" FOREIGN KEY ("subscription_phase_id") REFERENCES "subscription_phases" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_usage_based_tax_codes_charge_usage_based" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE NO ACTION;
-- Modify "charge_usage_based_cost_bases" table
ALTER TABLE "charge_usage_based_cost_bases" ADD
CONSTRAINT "charge_usage_based_cost_basis_currency_cost_basis_fk" FOREIGN KEY ("currency_cost_basis_id") REFERENCES "currency_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_usage_based_cost_basis_resolved_cost_basis_fk" FOREIGN KEY ("resolved_cost_basis_id") REFERENCES "currency_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "charge_usage_based_cost_basis_currency_fk" FOREIGN KEY ("currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT;
-- Modify "charge_usage_based_overrides" table
ALTER TABLE "charge_usage_based_overrides" ADD
CONSTRAINT "charge_usage_based_overrides_charge_usage_based_intent_override" FOREIGN KEY ("charge_id") REFERENCES "charge_usage_based" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charge_usage_based_overrides_tax_codes_charge_usage_based_overrides" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "charge_usage_based_run_credit_allocations" table
ALTER TABLE "charge_usage_based_run_credit_allocations" ADD
CONSTRAINT "charge_ub_run_credit_alloc_run" FOREIGN KEY ("run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_usage_based_run_detailed_line" table
ALTER TABLE "charge_usage_based_run_detailed_line" ADD
CONSTRAINT "charge_usage_based_run_detailed_line_charge_usage_based_detailed_lines" FOREIGN KEY ("charge_id") REFERENCES "charge_usage_based" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charge_usage_based_run_detailed_line_charge_usage_based_runs_detailed_lines" FOREIGN KEY ("run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "cub_run_corrected_detailed_lines" FOREIGN KEY ("corrects_run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE SET NULL;
-- Modify "charge_usage_based_run_invoiced_usages" table
ALTER TABLE "charge_usage_based_run_invoiced_usages" ADD
CONSTRAINT "charge_usage_based_run_invoiced_usages_charge_usage_based_runs_invoiced_usage" FOREIGN KEY ("run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_usage_based_run_overage_credit_allocations" table
ALTER TABLE "charge_usage_based_run_overage_credit_allocations" ADD
CONSTRAINT "charge_ub_overage_credit_alloc_run" FOREIGN KEY ("run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_usage_based_run_payments" table
ALTER TABLE "charge_usage_based_run_payments" ADD
CONSTRAINT "charge_usage_based_run_payments_charge_usage_based_runs_payment" FOREIGN KEY ("run_id") REFERENCES "charge_usage_based_runs" ("id") ON DELETE CASCADE;
-- Modify "charge_usage_based_runs" table
ALTER TABLE "charge_usage_based_runs" ADD
CONSTRAINT "charge_usage_based_runs_billing_invoices_charge_usage_based_runs" FOREIGN KEY ("invoice_id") REFERENCES "billing_invoices" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_usage_based_runs_billing_invoice_lines_charge_usage_based_run" FOREIGN KEY ("line_id") REFERENCES "billing_invoice_lines" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "charge_usage_based_runs_charge_usage_based_runs" FOREIGN KEY ("charge_id") REFERENCES "charge_usage_based" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "charge_usage_based_runs_features_usage_based_runs" FOREIGN KEY ("feature_id") REFERENCES "features" ("id") ON DELETE NO ACTION;
-- Modify "credit_realization_lineages" table
ALTER TABLE "credit_realization_lineages" ADD
CONSTRAINT "credit_realization_lineages_charges_credit_realization_lineages" FOREIGN KEY ("charge_id") REFERENCES "charges" ("id") ON DELETE NO ACTION;
-- Modify "credit_realization_lineage_segments" table
ALTER TABLE "credit_realization_lineage_segments" ADD
CONSTRAINT "credit_realization_lineage_segments_credit_realization_lineages_segments" FOREIGN KEY ("lineage_id") REFERENCES "credit_realization_lineages" ("id") ON DELETE NO ACTION;
-- Modify "currency_cost_bases" table
ALTER TABLE "currency_cost_bases" ADD
CONSTRAINT "currency_cost_bases_custom_currencies_cost_basis_history" FOREIGN KEY ("currency_id") REFERENCES "custom_currencies" ("id") ON DELETE CASCADE;
-- Modify "customer_subjects" table
ALTER TABLE "customer_subjects" ADD
CONSTRAINT "customer_subjects_customers_subjects" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE CASCADE;
-- Modify "entitlements" table
ALTER TABLE "entitlements" ADD
CONSTRAINT "entitlements_customers_entitlements" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "entitlements_features_entitlement" FOREIGN KEY ("feature_id") REFERENCES "features" ("id") ON DELETE NO ACTION;
-- Modify "features" table
ALTER TABLE "features" ADD
CONSTRAINT "features_meters_feature" FOREIGN KEY ("meter_id") REFERENCES "meters" ("id") ON DELETE SET NULL;
-- Modify "grants" table
ALTER TABLE "grants" ADD
CONSTRAINT "grants_entitlements_grant" FOREIGN KEY ("owner_id") REFERENCES "entitlements" ("id") ON DELETE CASCADE;
-- Modify "ledger_breakage_records" table
ALTER TABLE "ledger_breakage_records" ADD
CONSTRAINT "ledger_breakage_records_ledger_entries_source_breakage_records" FOREIGN KEY ("source_entry_id") REFERENCES "ledger_entries" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "ledger_breakage_records_ledger_sub_accounts_fbo_breakage_records" FOREIGN KEY ("fbo_sub_account_id") REFERENCES "ledger_sub_accounts" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "ledger_breakage_records_ledger_sub_accounts_breakage_records" FOREIGN KEY ("breakage_sub_account_id") REFERENCES "ledger_sub_accounts" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "ledger_breakage_records_ledger_transactions_source_breakage_records" FOREIGN KEY ("source_transaction_id") REFERENCES "ledger_transactions" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "ledger_breakage_records_ledger_transactions_breakage_records" FOREIGN KEY ("breakage_transaction_id") REFERENCES "ledger_transactions" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "ledger_breakage_records_ledger_transaction_groups_source_breakage_records" FOREIGN KEY ("source_transaction_group_id") REFERENCES "ledger_transaction_groups" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "ledger_breakage_records_ledger_transaction_groups_breakage_records" FOREIGN KEY ("breakage_transaction_group_id") REFERENCES "ledger_transaction_groups" ("id") ON DELETE NO ACTION;
-- Modify "ledger_customer_accounts" table
ALTER TABLE "ledger_customer_accounts" ADD
CONSTRAINT "ledger_customer_accounts_ledger_accounts_customer_accounts" FOREIGN KEY ("account_id") REFERENCES "ledger_accounts" ("id") ON DELETE NO ACTION;
-- Modify "ledger_entries" table
ALTER TABLE "ledger_entries" ADD
CONSTRAINT "ledger_entries_ledger_sub_accounts_entries" FOREIGN KEY ("sub_account_id") REFERENCES "ledger_sub_accounts" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "ledger_entries_ledger_transactions_entries" FOREIGN KEY ("transaction_id") REFERENCES "ledger_transactions" ("id") ON DELETE NO ACTION;
-- Modify "ledger_sub_accounts" table
ALTER TABLE "ledger_sub_accounts" ADD
CONSTRAINT "ledger_sub_accounts_ledger_accounts_sub_accounts" FOREIGN KEY ("account_id") REFERENCES "ledger_accounts" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "ledger_sub_accounts_ledger_sub_account_routes_sub_accounts" FOREIGN KEY ("route_id") REFERENCES "ledger_sub_account_routes" ("id") ON DELETE NO ACTION;
-- Modify "ledger_sub_account_routes" table
ALTER TABLE "ledger_sub_account_routes" ADD
CONSTRAINT "ledger_sub_account_routes_ledger_accounts_sub_account_routes" FOREIGN KEY ("account_id") REFERENCES "ledger_accounts" ("id") ON DELETE NO ACTION;
-- Modify "ledger_transactions" table
ALTER TABLE "ledger_transactions" ADD
CONSTRAINT "ledger_transactions_ledger_transaction_groups_transactions" FOREIGN KEY ("group_id") REFERENCES "ledger_transaction_groups" ("id") ON DELETE NO ACTION;
-- Modify "notification_events" table
ALTER TABLE "notification_events" ADD
CONSTRAINT "notification_events_notification_rules_events" FOREIGN KEY ("rule_id") REFERENCES "notification_rules" ("id") ON DELETE NO ACTION;
-- Modify "organization_default_tax_codes" table
ALTER TABLE "organization_default_tax_codes" ADD
CONSTRAINT "org_dtc_invoicing_tax_code_fk" FOREIGN KEY ("invoicing_tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "org_dtc_credit_grant_tax_code_fk" FOREIGN KEY ("credit_grant_tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE NO ACTION;
-- Modify "plans" table
ALTER TABLE "plans" ADD
CONSTRAINT "plans_custom_currencies_plans" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT;
-- Modify "plan_addons" table
ALTER TABLE "plan_addons" ADD
CONSTRAINT "plan_addons_addons_plans" FOREIGN KEY ("addon_id") REFERENCES "addons" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "plan_addons_plans_addons" FOREIGN KEY ("plan_id") REFERENCES "plans" ("id") ON DELETE CASCADE;
-- Modify "plan_phases" table
ALTER TABLE "plan_phases" ADD
CONSTRAINT "plan_phases_plans_phases" FOREIGN KEY ("plan_id") REFERENCES "plans" ("id") ON DELETE CASCADE;
-- Modify "plan_rate_cards" table
ALTER TABLE "plan_rate_cards" ADD
CONSTRAINT "plan_rate_cards_custom_currencies_plan_rate_cards" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "plan_rate_cards_features_ratecard" FOREIGN KEY ("feature_id") REFERENCES "features" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "plan_rate_cards_plan_phases_ratecards" FOREIGN KEY ("phase_id") REFERENCES "plan_phases" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "plan_rate_cards_tax_codes_plan_rate_cards" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "subscriptions" table
ALTER TABLE "subscriptions" ADD
CONSTRAINT "subscriptions_customers_subscription" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") ON DELETE NO ACTION, ADD
CONSTRAINT "subscriptions_plans_subscriptions" FOREIGN KEY ("plan_id") REFERENCES "plans" ("id") ON DELETE SET NULL;
-- Modify "subscription_addons" table
ALTER TABLE "subscription_addons" ADD
CONSTRAINT "subscription_addons_addons_subscription_addons" FOREIGN KEY ("addon_id") REFERENCES "addons" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "subscription_addons_subscriptions_addons" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE CASCADE;
-- Modify "subscription_addon_quantities" table
ALTER TABLE "subscription_addon_quantities" ADD
CONSTRAINT "subscription_addon_quantities_subscription_addons_quantities" FOREIGN KEY ("subscription_addon_id") REFERENCES "subscription_addons" ("id") ON DELETE CASCADE;
-- Modify "subscription_billing_sync_states" table
ALTER TABLE "subscription_billing_sync_states" ADD
CONSTRAINT "subscription_billing_sync_states_subscriptions_billing_sync_state" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE CASCADE;
-- Modify "subscription_cost_basis_pins" table
ALTER TABLE "subscription_cost_basis_pins" ADD
CONSTRAINT "subscription_cost_basis_pins_currency_cost_bases_subscription_pins" FOREIGN KEY ("cost_basis_id") REFERENCES "currency_cost_bases" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "subscription_cost_basis_pins_custom_currencies_subscription_cost_basis_pins" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "subscription_cost_basis_pins_subscriptions_cost_basis_pins" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE CASCADE;
-- Modify "subscription_items" table
ALTER TABLE "subscription_items" ADD
CONSTRAINT "subscription_items_custom_currencies_subscription_items" FOREIGN KEY ("custom_currency_id") REFERENCES "custom_currencies" ("id") ON DELETE RESTRICT, ADD
CONSTRAINT "subscription_items_entitlements_subscription_item" FOREIGN KEY ("entitlement_id") REFERENCES "entitlements" ("id") ON DELETE SET NULL, ADD
CONSTRAINT "subscription_items_subscription_phases_items" FOREIGN KEY ("phase_id") REFERENCES "subscription_phases" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "subscription_items_tax_codes_subscription_items" FOREIGN KEY ("tax_code_id") REFERENCES "tax_codes" ("id") ON DELETE SET NULL;
-- Modify "subscription_phases" table
ALTER TABLE "subscription_phases" ADD
CONSTRAINT "subscription_phases_subscriptions_phases" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions" ("id") ON DELETE CASCADE;
-- Modify "usage_resets" table
ALTER TABLE "usage_resets" ADD
CONSTRAINT "usage_resets_entitlements_usage_reset" FOREIGN KEY ("entitlement_id") REFERENCES "entitlements" ("id") ON DELETE CASCADE;
-- Modify "notification_channel_rules" table
ALTER TABLE "notification_channel_rules" ADD
CONSTRAINT "notification_channel_rules_notification_channel_id" FOREIGN KEY ("notification_channel_id") REFERENCES "notification_channels" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "notification_channel_rules_notification_rule_id" FOREIGN KEY ("notification_rule_id") REFERENCES "notification_rules" ("id") ON DELETE CASCADE;
-- Modify "notification_event_delivery_status_events" table
ALTER TABLE "notification_event_delivery_status_events" ADD
CONSTRAINT "notification_event_delivery_status_events_notification_event_delivery_status_id" FOREIGN KEY ("notification_event_delivery_status_id") REFERENCES "notification_event_delivery_status" ("id") ON DELETE CASCADE, ADD
CONSTRAINT "notification_event_delivery_status_events_notification_event_id" FOREIGN KEY ("notification_event_id") REFERENCES "notification_events" ("id") ON DELETE CASCADE;
-- Add "charges_search_v1s" view
CREATE VIEW "charges_search_v1s" ("type", "base_intent_deleted_at", "feature_id", "feature_key", "customer_id", "service_period_from", "service_period_to", "billing_period_from", "billing_period_to", "full_service_period_from", "full_service_period_to", "status", "unique_reference_id", "currency", "custom_currency_id", "managed_by", "subscription_plan", "subscription_id", "subscription_phase_id", "subscription_item_id", "advance_after", "tax_code_id", "tax_behavior", "annotations", "id", "namespace", "metadata", "created_at", "updated_at", "deleted_at", "name", "description") AS SELECT "id", "namespace", "metadata", "created_at", "updated_at", "deleted_at", "name", "description", "annotations", "customer_id", "service_period_from", "service_period_to", "billing_period_from", "billing_period_to", "full_service_period_from", "full_service_period_to", "status", "unique_reference_id", "currency", "custom_currency_id", "managed_by", "subscription_id", "subscription_phase_id", "subscription_item_id", "advance_after", "tax_code_id", "tax_behavior", NULL::timestamptz AS "base_intent_deleted_at", 'credit_purchase' AS "type", NULL::char(26) AS "feature_id", NULL::varchar AS "feature_key", "subscription_plan" FROM "charge_credit_purchases" UNION ALL SELECT "id", "namespace", "metadata", "created_at", "updated_at", "deleted_at", "name", "description", "annotations", "customer_id", "service_period_from", "service_period_to", "billing_period_from", "billing_period_to", "full_service_period_from", "full_service_period_to", "status", "unique_reference_id", "currency", "custom_currency_id", "managed_by", "subscription_id", "subscription_phase_id", "subscription_item_id", "advance_after", "tax_code_id", "tax_behavior", "intent_deleted_at" AS "base_intent_deleted_at", 'flat_fee' AS "type", "feature_id" AS "feature_id", "feature_key" AS "feature_key", "subscription_plan" FROM "charge_flat_fees" UNION ALL SELECT "id", "namespace", "metadata", "created_at", "updated_at", "deleted_at", "name", "description", "annotations", "customer_id", "service_period_from", "service_period_to", "billing_period_from", "billing_period_to", "full_service_period_from", "full_service_period_to", "status", "unique_reference_id", "currency", "custom_currency_id", "managed_by", "subscription_id", "subscription_phase_id", "subscription_item_id", "advance_after", "tax_code_id", "tax_behavior", "intent_deleted_at" AS "base_intent_deleted_at", 'usage_based' AS "type", "feature_id" AS "feature_id", "feature_key" AS "feature_key", "subscription_plan" FROM "charge_usage_based";

-- Breakage bookkeeping is inserted before the caller posts its ledger group.
-- References must exist when the enclosing transaction commits.
ALTER TABLE "ledger_breakage_records"
    ALTER CONSTRAINT "ledger_breakage_records_ledger_entries_source_breakage_records" DEFERRABLE INITIALLY DEFERRED,
    ALTER CONSTRAINT "ledger_breakage_records_ledger_transaction_groups_breakage_reco" DEFERRABLE INITIALLY DEFERRED,
    ALTER CONSTRAINT "ledger_breakage_records_ledger_transaction_groups_source_breaka" DEFERRABLE INITIALLY DEFERRED,
    ALTER CONSTRAINT "ledger_breakage_records_ledger_transactions_breakage_records" DEFERRABLE INITIALLY DEFERRED,
    ALTER CONSTRAINT "ledger_breakage_records_ledger_transactions_source_breakage_rec" DEFERRABLE INITIALLY DEFERRED;
