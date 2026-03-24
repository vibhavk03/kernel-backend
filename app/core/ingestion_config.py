from app.transformers.crm_accounts_transformer import CRMAccountsTransformer
from app.transformers.crm_targeting_transformer import CRMTargetingTransformer
from app.transformers.iqvia_affiliation_transformer import IQVIAAffiliationTransformer
from app.transformers.iqvia_hco_transformer import IQVIAHCOTransformer
from app.transformers.iqvia_hcp_transformer import IQVIAHCPTransformer
from app.transformers.iqvia_rx_transformer import IQVIARXTransformer
from app.transformers.komodo_patient_events_transformer import (
    KomodoPatientEventsTransformer,
)

from app.validators.crm_accounts_validator import CRMAccountsValidator
from app.validators.crm_targeting_validator import CRMTargetingValidator
from app.validators.iqvia_affiliation_validator import IQVIAAffiliationValidator
from app.validators.iqvia_hco_validator import IQVIAHCOValidator
from app.validators.iqvia_hcp_validator import IQVIAHCPValidator
from app.validators.iqvia_rx_validator import IQVIARXValidator
from app.validators.komodo_patient_events_validator import KomodoPatientEventsValidator

INGESTION_FILE_CONFIGS = {
    "affiliation": {
        "key": "IQVIA_OneKey_Affiliations.csv",
        "target_table": "raw_iqvia_affiliations",
        "dtype": {
            "onekey_hcp_id": "string",
            "npi": "string",
            "onekey_hco_id": "string",
            "hco_name": "string",
            "affiliation_type": "string",
            "affiliation_status": "string",
        },
        "transformer": IQVIAAffiliationTransformer,
        "validator": IQVIAAffiliationValidator,
    },
    "hcp": {
        "key": "IQVIA_OneKey_HCP.csv",
        "target_table": "raw_iqvia_hcps",
        "dtype": {
            "onekey_hcp_id": "string",
            "npi": "string",
            "hcp_name": "string",
            "specialty": "string",
            "status": "string",
            "primary_address_line1": "string",
            "primary_address_line2": "string",
            "primary_city": "string",
            "primary_state": "string",
            "primary_zip": "string",
        },
        "transformer": IQVIAHCPTransformer,
        "validator": IQVIAHCPValidator,
    },
    "hco": {
        "key": "IQVIA_OneKey_HCO.csv",
        "target_table": "raw_iqvia_hcos",
        "dtype": {
            "onekey_hco_id": "string",
            "hco_name": "string",
            "hco_type": "string",
            "address_line1": "string",
            "address_line2": "string",
            "city": "string",
            "state": "string",
            "zip": "string",
            "status": "string",
            "parent_onekey_hco_id": "string",
            "parent_name": "string",
        },
        "transformer": IQVIAHCOTransformer,
        "validator": IQVIAHCOValidator,
    },
    "rx": {
        "key": "IQVIA_Xponent_Rx.csv",
        "target_table": "raw_iqvia_rxs",
        "dtype": {
            "rx_id": "string",
            "npi": "string",
            "prescriber_name": "string",
            "product": "string",
            "trx": "string",
            "week_start": "string",
            "site_name": "string",
            "site_address_line1": "string",
            "site_address_line2": "string",
            "site_city": "string",
            "site_state": "string",
            "site_zip": "string",
        },
        "transformer": IQVIARXTransformer,
        "validator": IQVIARXValidator,
    },
    "patient_events": {
        "key": "Komodo_Patient_Events.csv",
        "target_table": "raw_komodo_patient_events",
        "dtype": {
            "event_id": "string",
            "patient_id": "string",
            "event_date": "string",
            "event_type": "string",
            "rendering_npi": "string",
            "rendering_hcp_name": "string",
            "facility_name": "string",
            "facility_address_line1": "string",
            "facility_city": "string",
            "facility_state": "string",
            "facility_zip": "string",
            "diagnosis": "string",
        },
        "transformer": KomodoPatientEventsTransformer,
        "validator": KomodoPatientEventsValidator,
    },
    "crm_targeting": {
        "key": "CRM_Targeting.csv",
        "target_table": "raw_crm_targeting",
        "dtype": {
            "rep_id": "string",
            "rep_name": "string",
            "territory": "string",
            "crm_account_id": "string",
            "account_name": "string",
            "npi": "string",
            "hcp_name": "string",
            "target_tier": "string",
            "preferred_location_flag": "string",
        },
        "transformer": CRMTargetingTransformer,
        "validator": CRMTargetingValidator,
    },
    "crm_accounts": {
        "key": "CRM_Accounts.csv",
        "target_table": "raw_crm_accounts",
        "dtype": {
            "crm_account_id": "string",
            "account_name": "string",
            "account_type": "string",
            "address_line1": "string",
            "address_line2": "string",
            "city": "string",
            "state": "string",
            "zip": "string",
            "linked_onekey_hco_id": "string",
            "parent_account_name": "string",
            "notes": "string",
        },
        "transformer": CRMAccountsTransformer,
        "validator": CRMAccountsValidator,
    },
}
