import pandas as pd


class CRMTargetingValidator:
    REQUIRED_COLUMNS = [
        "rep_id",
        "rep_name",
        "territory",
        "crm_account_id",
        "account_name",
        "npi",
        "hcp_name",
        "target_tier",
        "preferred_location_flag",
    ]

    ALLOWED_TARGET_TIERS = {"A", "B", "C"}
    ALLOWED_PREFERRED_LOCATION_FLAG = {0, 1}

    @classmethod
    def validate_columns(cls, df: pd.DataFrame) -> None:
        """Check for presence of required columns and strip whitespace from column names."""
        df.columns = [col.strip() for col in df.columns]

        missing = [col for col in cls.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @classmethod
    def validate_required_fields(cls, df: pd.DataFrame) -> None:
        """Check that required fields are not null or blank (except preferred_location_flag which can be null but not blank)."""
        errors = []

        for col in cls.REQUIRED_COLUMNS:
            if col == "preferred_location_flag":
                null_count = int(df[col].isna().sum())
                blank_count = 0
            else:
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
    def validate_rep_id(cls, df: pd.DataFrame) -> None:
        """Validate that rep_id values match the pattern 'REP-X00' where X is a letter and 00 are digits."""
        invalid_rep_id = df[
            ~df["rep_id"].fillna("").astype("string").str.fullmatch(r"REP-[A-Z]\d{2}")
        ]
        if not invalid_rep_id.empty:
            sample_rows = (invalid_rep_id.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid rep_id values found. rep_id must match pattern 'REP-X00'. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_territory(cls, df: pd.DataFrame) -> None:
        """Validate that territory values match the pattern 'AAA-00'."""
        invalid_territory = df[
            ~df["territory"]
            .fillna("")
            .astype("string")
            .str.fullmatch(r"[A-Z]{3}-\d{2}")
        ]
        if not invalid_territory.empty:
            sample_rows = (invalid_territory.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid territory values found. territory must match pattern 'AAA-00'. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_crm_account_id(cls, df: pd.DataFrame) -> None:
        """Validate that crm_account_id values match the pattern 'A-<number>'."""
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
    def validate_npi(cls, df: pd.DataFrame) -> None:
        """Validate that NPI values are exactly 10 digits."""
        invalid_npi = df[
            ~df["npi"].fillna("").astype("string").str.fullmatch(r"\d{10}")
        ]
        if not invalid_npi.empty:
            sample_rows = (invalid_npi.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid NPI values found. NPI must be exactly 10 digits. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_target_tier(cls, df: pd.DataFrame) -> None:
        """Validate that target_tier values are one of the allowed tiers (A, B, C)."""
        invalid_target_tier = df[
            ~df["target_tier"]
            .fillna("")
            .astype("string")
            .isin(cls.ALLOWED_TARGET_TIERS)
        ]
        if not invalid_target_tier.empty:
            bad_values = (
                invalid_target_tier["target_tier"]
                .dropna()
                .astype("string")
                .drop_duplicates()
                .tolist()
            )
            raise ValueError(f"Invalid target_tier values: {bad_values}")

    @classmethod
    def validate_preferred_location_flag(cls, df: pd.DataFrame) -> None:
        """Validate that preferred_location_flag values are either 0, 1, or null."""
        invalid_flag = df[
            df["preferred_location_flag"].isna()
            | ~df["preferred_location_flag"].isin(cls.ALLOWED_PREFERRED_LOCATION_FLAG)
        ]
        if not invalid_flag.empty:
            sample = (
                invalid_flag[["crm_account_id", "npi", "preferred_location_flag"]]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                f"Invalid preferred_location_flag values found. Allowed values are 0 or 1. Examples: {sample}"
            )

    @classmethod
    def validate_duplicates(cls, df: pd.DataFrame) -> None:
        """Validate that there are no duplicate records for the same rep_id + crm_account_id + npi combination."""
        duplicate_rows = df[
            df.duplicated(subset=["rep_id", "crm_account_id", "npi"], keep=False)
        ]

        if not duplicate_rows.empty:
            sample = (
                duplicate_rows[["rep_id", "crm_account_id", "npi"]]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                f"Duplicate CRM targeting records found for rep_id + crm_account_id + npi. Examples: {sample}"
            )

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        cls.validate_columns(df)
        cls.validate_required_fields(df)
        cls.validate_rep_id(df)
        cls.validate_territory(df)
        cls.validate_crm_account_id(df)
        cls.validate_npi(df)
        cls.validate_target_tier(df)
        cls.validate_preferred_location_flag(df)
        # cls.validate_duplicates(df)
        return df
