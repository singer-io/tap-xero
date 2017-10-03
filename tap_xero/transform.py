def format_allocations(allocations):
    for allocation in allocations:
        invoice = allocation.get("Invoice", {})
        invoice.pop("Prepayments", None)
        invoice.pop("Payments", None)
        invoice.pop("CreditNotes", None)
        invoice.pop("LineItems", None)
        invoice.pop("Overpayments", None)


def format_credit_notes(credit_notes):
    for credit_note in credit_notes:
        credit_note.pop("Payments", None)
        format_allocations(credit_note.get("Allocations", []))


def format_contact_groups(contact_groups):
    for contact_group in contact_groups:
        contact_group.pop("Contacts", None)
