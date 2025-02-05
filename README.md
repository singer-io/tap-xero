# tap-xero

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Xero's [API](https://developer.xero.com/documentation/)
- Extracts the following resources from Xero
  - [Bank Transactions](https://developer.xero.com/documentation/api/banktransactions)
  - [Contacts](https://developer.xero.com/documentation/api/contacts)
  - [Credit Notes](https://developer.xero.com/documentation/api/credit-notes)
  - [Invoices](https://developer.xero.com/documentation/api/invoices)
  - [Manual Journals](https://developer.xero.com/documentation/api/manual-journals)
  - [Overpayments](https://developer.xero.com/documentation/api/overpayments)
  - [Prepayments](https://developer.xero.com/documentation/api/prepayments)
  - [Purchase Orders](https://developer.xero.com/documentation/api/purchase-orders)
  - [Journals](https://developer.xero.com/documentation/api/journals)
  - [Accounts](https://developer.xero.com/documentation/api/accounts)
  - [Bank Transfers](https://developer.xero.com/documentation/api/bank-transfers)
  - [Employees](https://developer.xero.com/documentation/api/employees)
  - [Expense Claims](https://developer.xero.com/documentation/api/expense-claims)
  - [Items](https://developer.xero.com/documentation/api/items)
  - [Payments](https://developer.xero.com/documentation/api/payments)
  - [Receipts](https://developer.xero.com/documentation/api/receipts)
  - [Users](https://developer.xero.com/documentation/api/users)
  - [Branding Themes](https://developer.xero.com/documentation/api/branding-themes)
  - [Contact Groups](https://developer.xero.com/documentation/api/contactgroups)
  - [Currencies](https://developer.xero.com/documentation/api/currencies)
  - [Organisations](https://developer.xero.com/documentation/api/organisation)
  - [Repeating Invoices](https://developer.xero.com/documentation/api/repeating-invoices)
  - [Tax Rates](https://developer.xero.com/documentation/api/tax-rates)
  - [Tracking Categories](https://developer.xero.com/documentation/api/tracking-categories)
  - [Linked Transactions](https://developer.xero.com/documentation/api/linked-transactions)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Limitations

- Only designed to work with Xero [Partner Applications](https://developer.xero.com/documentation/auth-and-limits/partner-applications), not Private Applications.

## Authentication

This tap requires a client id, client secret, tenant id, and refresh token in order to authenticate (and thus receive an
access token).

See [the Xero documentation](https://developer.xero.com/documentation/guides/oauth2/auth-flow) for more info.

### Refresh token

The tap is able to use the refresh token to refresh the access token. Doing so also generates a new refresh token, which
the tap is able to write back to you.

There are two ways to pass in a refresh token, and two ways to have an updated refresh token written back to you.  

To pass in a refresh token, either use the `refresh_token` arg or the `refresh_token_path` arg.  
If `refresh_token` is not set, the refresh token will be read as the entire contents of the file at
`refresh_token_path`.  
If `refresh_token_path` is set, a new refresh token will override the entire contents of the file **as well as** being
written to the config JSON (which is the default behavior if `refresh_token_path` is not set).

This is important for the use of this tap via Meltano. Since Meltano generates an ephemeral config JSON, writing the
new refresh token to it is not very helpful.

---

Copyright &copy; 2017 Stitch
