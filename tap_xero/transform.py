def _format_nested_invoice(invoice):
    invoice.pop("Prepayments", None)
    invoice.pop("Payments", None)
    invoice.pop("CreditNotes", None)
    invoice.pop("Overpayments", None)


def _format_allocations(allocations):
    for allocation in allocations:
        invoice = allocation.get("Invoice", {})
        _format_nested_invoice(invoice)


def format_credit_notes(credit_notes):
    for credit_note in credit_notes:
        credit_note.pop("Payments", None)
        _format_allocations(credit_note.get("Allocations", []))


def format_contact_groups(contact_groups):
    for contact_group in contact_groups:
        contact_group.pop("Contacts", None)


def format_payments(payments):
    for payment in payments:
        invoice = payment.get("Invoice", {})
        _format_nested_invoice(invoice)


def format_over_pre_payments(over_pre_payments):
    for payment in over_pre_payments:
        _format_allocations(payment.get("Allocations", []))


def strip_warnings(records):
    for record in records:
        record.pop("Warnings", None)

format_users = strip_warnings


def format_receipts(receipts):
    strip_warnings(receipts)
    for receipt in receipts:
        receipt.get("User", {}).pop("Warnings", None)
        receipt.get("Contact", {}).pop("Warnings", None)


def format_contacts(contacts):
    strip_warnings(contacts)
    for contact in contacts:
        format_contact_groups(contact["ContactGroups"])
