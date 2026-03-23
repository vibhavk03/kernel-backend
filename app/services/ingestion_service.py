# from sqlalchemy.orm import Session
# from pathlib import Path
# import pandas as pd
# import math
# from datetime import datetime, date
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session
from app.repositories.ingestion_repo import IngestionRepository
from app.repositories.ingestion_repo import IngestionRepository
from app.services.s3_service import S3Service
from app.transformers.iqvia_affiliation_transformer import IQVIAAffiliationTransformer
from app.transformers.iqvia_hcp_transformer import IQVIAHCPTransformer
from app.transformers.iqvia_hco_transformer import IQVIAHCOTransformer
from app.transformers.iqvia_rx_transformer import IQVIARXTransformer
from app.transformers.komodo_patient_events_transformer import (
    KomodoPatientEventsTransformer,
)
from app.transformers.crm_targeting_transformer import CRMTargetingTransformer
from app.transformers.crm_accounts_transformer import CRMAccountsTransformer
from app.validators.iqvia_affiliation_validator import IQVIAAffiliationValidator
from app.validators.iqvia_hcp_validator import IQVIAHCPValidator
from app.validators.iqvia_hco_validator import IQVIAHCOValidator
from app.validators.iqvia_rx_validator import IQVIARXValidator
from app.validators.komodo_patient_events_validator import KomodoPatientEventsValidator
from app.validators.crm_targeting_validator import CRMTargetingValidator
from app.validators.crm_accounts_validator import CRMAccountsValidator


class IngestionService:

    @staticmethod
    def process_files(db: Session):
        # --- 1. EXTRACT ---
        s3_service = S3Service()

        expected_files = {
            "affiliation": "IQVIA_OneKey_Affiliations.csv",
            "hcp": "IQVIA_OneKey_HCP.csv",
            "hco": "IQVIA_OneKey_HCO.csv",
            "rx": "IQVIA_Xponent_Rx.csv",
            "patient_events": "Komodo_Patient_Events.csv",
            "crm_targeting": "CRM_Targeting.csv",
            "crm_accounts": "CRM_Accounts.csv",
        }

        # --- 1. EXTRACT FROM S3 ---
        df_affiliation = pd.read_csv(
            s3_service.get_file_bytes(expected_files["affiliation"]),
            # We specify dtypes to ensure consistent reading and to prevent pandas from inferring types that might lead to issues later
            dtype={
                "onekey_hcp_id": "string",
                "npi": "string",
                "onekey_hco_id": "string",
                "hco_name": "string",
                "affiliation_type": "string",
                "affiliation_status": "string",
            },
        )

        df_hcp = pd.read_csv(
            s3_service.get_file_bytes(expected_files["hcp"]),
            dtype={
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
        )

        df_hco = pd.read_csv(
            s3_service.get_file_bytes(expected_files["hco"]),
            dtype={
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
        )

        df_rx = pd.read_csv(
            s3_service.get_file_bytes(expected_files["rx"]),
            dtype={
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
        )

        df_patient_events = pd.read_csv(
            s3_service.get_file_bytes(expected_files["patient_events"]),
            dtype={
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
        )

        df_crm_targeting = pd.read_csv(
            s3_service.get_file_bytes(expected_files["crm_targeting"]),
            dtype={
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
        )

        df_crm_accounts = pd.read_csv(
            s3_service.get_file_bytes(expected_files["crm_accounts"]),
            dtype={
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
        )

        # --- 2. VALIDATE ---
        df_affiliation = IQVIAAffiliationTransformer.transform(df_affiliation)
        IQVIAAffiliationValidator.validate(df_affiliation)

        df_hcp = IQVIAHCPTransformer.transform(df_hcp)
        IQVIAHCPValidator.validate(df_hcp)

        df_hco = IQVIAHCOTransformer.transform(df_hco)
        IQVIAHCOValidator.validate(df_hco)

        df_rx = IQVIARXTransformer.transform(df_rx)
        IQVIARXValidator.validate(df_rx)

        df_patient_events = KomodoPatientEventsTransformer.transform(df_patient_events)
        KomodoPatientEventsValidator.validate(df_patient_events)

        df_crm_targeting = CRMTargetingTransformer.transform(df_crm_targeting)
        CRMTargetingValidator.validate(df_crm_targeting)

        df_crm_accounts = CRMAccountsTransformer.transform(df_crm_accounts)
        CRMAccountsValidator.validate(df_crm_accounts)

        # --- 3. TRANSFORM ---
        # toy transform
        df_affiliation["processed_by"] = "fastapi_service_layer"
        df_hcp["processed_by"] = "fastapi_service_layer"
        df_hco["processed_by"] = "fastapi_service_layer"
        df_rx["processed_by"] = "fastapi_service_layer"
        df_patient_events["processed_by"] = "fastapi_service_layer"
        df_crm_targeting["processed_by"] = "fastapi_service_layer"
        df_crm_accounts["processed_by"] = "fastapi_service_layer"
        # Drop empty columns, rename things, etc.

        # --- 3. LOAD (Pass to Repository) ---
        # We let the repository handle the actual database insertion
        affiliation_rows = IngestionRepository.save_dataframe(
            db, df_affiliation, "raw_iqvia_affiliations"
        )

        hcp_rows = IngestionRepository.save_dataframe(db, df_hcp, "raw_iqvia_hcps")

        hco_rows = IngestionRepository.save_dataframe(db, df_hco, "raw_iqvia_hcos")

        rx_rows = IngestionRepository.save_dataframe(db, df_rx, "raw_iqvia_rxs")

        patient_rows = IngestionRepository.save_dataframe(
            db, df_patient_events, "raw_komodo_patient_events"
        )
        crm_targeting_rows = IngestionRepository.save_dataframe(
            db, df_crm_targeting, "raw_crm_targeting"
        )
        crm_accounts_rows = IngestionRepository.save_dataframe(
            db, df_crm_accounts, "raw_crm_accounts"
        )

        return {
            "message": "ETL complete",
            "affiliation_rows": affiliation_rows,
            "hcp_rows": hcp_rows,
            "hco_rows": hco_rows,
            "rx_rows": rx_rows,
            "patient_rows": patient_rows,
            "crm_targeting_rows": crm_targeting_rows,
            "crm_accounts_rows": crm_accounts_rows,
        }
