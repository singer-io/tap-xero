# Changelog

## 1.0.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 1.0.0
  * Preparing for release

## 0.2.1
  * Fixes a bug removing a reference to the pendulum library which is no longer used

## 0.2.0
  * Updates the tap to use metadata and allow for property selection [#58](https://github.com/singer-io/tap-xero/pull/58)

## 0.1.30
  * Adds `AccountID` to the schema for manual_journal.JournalLines [#57](https://github.com/singer-io/tap-xero/pull/57)

## 0.1.29
  * Adds "TotalDiscount" to repeating_invoices schema [#55](https://github.com/singer-io/tap-xero/pull/55)
  * Fixes [bug](https://github.com/singer-io/tap-xero/issues/53) where linked transaction page was not incrementing [#54](https://github.com/singer-io/tap-xero/pull/54)

## 0.1.28
  * Fixes [bug](https://github.com/singer-io/tap-xero/issues/50) where empty list was being returned during an API call after re-auth [#51](https://github.com/singer-io/tap-xero/pull/51)

## 0.1.27
  * Fixes issue where credentials wouldn't refresh properly during a tap run > 30 min

## 0.1.26
  * Add payments to tap_schema_dependencies in prepayments schema

## 0.1.25
  * Add Payments array to prepayments schema [#47](https://github.com/singer-io/tap-xero/pull/47)

## 0.1.24
  * Add "ID" and "AppliedAmount" fields to prepayments schema [#44](https://github.com/singer-io/tap-xero/pull/44/files)

## 0.1.23
  * Fix for refreshing credentials after short term expiration.

## 0.1.21
  * Fixes formatting of overpayments and prepayments [#37](https://github.com/singer-io/tap-xero/pull/37)

## 0.1.20
  * Fixes PaymentTerm schema issue in contacts stream [#31](https://github.com/singer-io/tap-xero/pull/31)
  * Fixes bug in LinkedTransactions stream where start time was not parsed properly into datetime [#28](https://github.com/singer-io/tap-xero/pull/28)

## 0.1.19
  * Fixes usage of 'since' parameter [#24](https://github.com/singer-io/tap-xero/pull/24)
  * Adds TrackingOptionName and TrackingCategoryName to tracking_categories sub-schema [#25](https://github.com/singer-io/tap-xero/pull/25)
  * Adds IsNonRecoverable field to tax_rates schema [#26](https://github.com/singer-io/tap-xero/pull/26)

## 0.1.18
  * Adding ExpectedPaymentDateString to invoice schema [#21](https://github.com/singer-io/tap-xero/pull/21)

## 0.1.17
  * Fix a bug in the bank_transactions schema.

## 0.1.16
  * Added ExternalLinkProviderName and fixed multipleOf precision for bank_transactions schema [#19](https://github.com/singer-io/tap-xero/pull/19)

## 0.1.15
  * Added PlannedPaymentDateString to invoices schema [#18](https://github.com/singer-io/tap-xero/pull/18)
