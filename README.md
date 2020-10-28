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

## Getting started

### Configuration

The configuration should take the following form:

```json
{
  "start_date": "2020-04-01",
  "client_id": "...",
  "client_secret": "...",
  "tenant_id": "...",
  "refresh_token": "..."
}
```

Choose your preferred start date. You will need to get the other 4 values from the Xero API.

 * Client ID and Secret are provided by the [Xero developer portal](http://developer.xero.com),
   if you create a new OAuth2 app.
 * To get the tenant ID and refresh token, you will need to set up a web app
   to receive the OAuth2 repsonse. A simple script for this is available at
   [sminnee/tap-xero-config](http://github.com/sminnee/tap-xero-config).

### Running without installation

When making improvements to tap-xero, you will likely to want to run the 
script without installing the pacakge. You can do this with the following
command:

```
> python -m tap_xero.__init__ -c yourconfig.json -p catalog.json
```

---

Copyright &copy; 2017 Stitch
