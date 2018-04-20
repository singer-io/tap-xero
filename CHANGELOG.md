# Changelog

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
