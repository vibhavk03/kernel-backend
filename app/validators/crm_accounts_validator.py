import pandas as pd


class CRMAccountsValidator:
    REQUIRED_COLUMNS = [
        "crm_account_id",
        "account_name",
        "account_type",
        "address_line1",
        "city",
        "state",
        "zip",
    ]

    OPTIONAL_COLUMNS = [
        "address_line2",
        "linked_onekey_hco_id",
        "parent_account_name",
        "notes",
    ]

    ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

    ALLOWED_ACCOUNT_TYPES = {
        "Health System",
        "Clinic / Practice",
        "Independent Practice",
    }

    @classmethod
    def validate_columns(cls, df: pd.DataFrame) -> None:
        df.columns = [col.strip() for col in df.columns]

        missing = [col for col in cls.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @classmethod
    def validate_required_fields(cls, df: pd.DataFrame) -> None:
        errors = []

        for col in cls.REQUIRED_COLUMNS:
            null_count = int(df[col].isna().sum())
            blank_count = int(
                (df[col].fillna("").astype("string").str.strip() == "").sum()
            )

            if null_count > 0 or blank_count > 0:
                errors.append(
                    f"Column '{col}' has {null_count} null values and {blank_count} blank values"
                )

        if errors:
            raise ValueError("Required field validation failed: " + " | ".join(errors))

    @classmethod
    def validate_crm_account_id(cls, df: pd.DataFrame) -> None:
        invalid_account_id = df[
            ~df["crm_account_id"].fillna("").astype("string").str.fullmatch(r"A-\d+")
        ]
        if not invalid_account_id.empty:
            sample_rows = (invalid_account_id.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid crm_account_id values found. crm_account_id must match pattern 'A-<number>'. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_account_type(cls, df: pd.DataFrame) -> None:
        invalid_account_type = df[
            ~df["account_type"]
            .fillna("")
            .astype("string")
            .isin(cls.ALLOWED_ACCOUNT_TYPES)
        ]
        if not invalid_account_type.empty:
            bad_values = (
                invalid_account_type["account_type"]
                .dropna()
                .astype("string")
                .drop_duplicates()
                .tolist()
            )
            raise ValueError(f"Invalid account_type values: {bad_values}")

    @classmethod
    def validate_state(cls, df: pd.DataFrame) -> None:
        invalid_state = df[
            ~df["state"].fillna("").astype("string").str.fullmatch(r"[A-Z]{2}")
        ]
        if not invalid_state.empty:
            sample_rows = (invalid_state.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid state values found. State must be exactly 2 uppercase letters. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_zip(cls, df: pd.DataFrame) -> None:
        invalid_zip = df[~df["zip"].fillna("").astype("string").str.fullmatch(r"\d{5}")]
        if not invalid_zip.empty:
            sample_rows = (invalid_zip.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid zip values found. ZIP must be exactly 5 digits. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_linked_onekey_hco_id(cls, df: pd.DataFrame) -> None:
        if "linked_onekey_hco_id" not in df.columns:
            return

        populated = df["linked_onekey_hco_id"].dropna().astype("string")
        invalid_hco_ids = populated[~populated.str.fullmatch(r"[A-Z]{3,4}\d+")]

        if not invalid_hco_ids.empty:
            bad_values = invalid_hco_ids.drop_duplicates().tolist()
            raise ValueError(
                f"Invalid linked_onekey_hco_id values found. Examples: {bad_values[:10]}"
            )

    @classmethod
    def validate_duplicates(cls, df: pd.DataFrame) -> None:
        duplicate_rows = df[df.duplicated(subset=["crm_account_id"], keep=False)]

        if not duplicate_rows.empty:
            sample = (
                duplicate_rows[["crm_account_id", "account_name", "city", "state"]]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                f"Duplicate CRM account records found for crm_account_id. Examples: {sample}"
            )

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        cls.validate_columns(df)
        cls.validate_required_fields(df)
        cls.validate_crm_account_id(df)
        cls.validate_account_type(df)
        cls.validate_state(df)
        cls.validate_zip(df)
        cls.validate_linked_onekey_hco_id(df)
        # cls.validate_duplicates(df)
        return df
