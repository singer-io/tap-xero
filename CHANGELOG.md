# Changelog

## 2.3.1
  * Add new fields into the schema of credit_notes stream [#117](https://github.com/singer-io/tap-xero/pull/117)

## 2.3.0
  * Updates to run on python 3.9.6 [#113](https://github.com/singer-io/tap-xero/pull/113)

## 2.2.3
  * Adds a workaround for a Xero bug to allow pagination to function properly in the `manual_journals` stream [#104](https://github.com/singer-io/tap-xero/pull/104)

## 2.2.2
  * Changes the endpoint used for check_platform_access from `Contacts` to `Currencies` [#98](https://github.com/singer-io/tap-xero/pull/98)

## 2.2.1
  * Make the TotalDiscount schema on the Quotes stream the same as the schema on the other streams [#93](https://github.com/singer-io/tap-xero/pull/93)

## 2.2.0
  * Additional handling for HTTP 429 responses from Xero [#90](https://github.com/singer-io/tap-xero/pull/90)
  * Adding a feature to allow the syncing of Archived Contacts via `include_archived_contacts` config [#84](https://github.com/singer-io/tap-xero/pull/84)

## 2.1.0
  * Add retry to JSON decode error [#83](https://github.com/singer-io/tap-xero/pull/83)
  * Add backoff mechanism for 429 errors. Validate authorization during discovery [#85](https://github.com/singer-io/tap-xero/pull/85)
  * Add Quotes stream support [#86](https://github.com/singer-io/tap-xero/pull/86)
  * Increase the allowable precision in the CurrencyRate schema from `1e-06` to `1e-10` [#87](https://github.com/singer-io/tap-xero/pull/87)

## 2.0.4
  * In json parsing hook, explicitly check for `date` type values to add 0 hh:mm:ss to so that the parser avoids truncating `datetime` objects [#79](https://github.com/singer-io/tap-xero/pull/79)

## 2.0.3
  * Switching the Payments stream to use the PaginatedStream class as that API endpoint can paginate using the "page" query param [#76](https://github.com/singer-io/tap-xero/pull/76)

## 2.0.2
  * Add handling for date formatting on negative dates [#74](https://github.com/singer-io/tap-xero/pull/74)

## 2.0.1
  * Check for if a value is in datetime format before parsing as a datetime [#70](https://github.com/singer-io/tap-xero/pull/70)

## 2.0.0
  * Switch from OAuth1 to OAuth2. This will no longer work with OAuth1 credentials and any users will need to [migrate their tokens](https://developer.xero.com/documentation/oauth2/migrate) [#66](https://github.com/singer-io/tap-xero/pull/66)

## 1.0.4
  * Add handling for date formatting on JournalDate for when Xero returns the date as '/Date(0+0000)/' [#64](https://github.com/singer-io/tap-xero/pull/64)

## 1.0.3
  * Add handling for date formatting when an invoice is voided [#62](https://github.com/singer-io/tap-xero/pull/62)

## 1.0.2
  * Restrict the version of pyxero being used more explicitly

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
