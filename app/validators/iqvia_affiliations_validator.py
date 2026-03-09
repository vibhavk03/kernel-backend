import pandas as pd


class IQVIAAffiliationsValidator:
    REQUIRED_COLUMNS = [
        "onekey_hcp_id",
        "npi",
        "onekey_hco_id",
        "hco_name",
        "affiliation_type",
        "affiliation_status",
    ]

    ALLOWED_AFFILIATION_STATUS = {"Active", "Inactive"}
    ALLOWED_AFFILIATION_TYPE = {"Practices At"}

    @classmethod
    def validate_columns(cls, df: pd.DataFrame) -> None:
        """Validates that all required columns are present in the DataFrame."""
        df.columns = [col.strip() for col in df.columns]

        missing = [col for col in cls.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @classmethod
    def normalize(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Strips whitespace from column names and required string fields."""
        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        for col in cls.REQUIRED_COLUMNS:
            df[col] = df[col].astype("string").str.strip()

        return df

    @classmethod
    def validate_required_fields(cls, df: pd.DataFrame) -> None:
        """Checks for null or blank values in required fields."""
        errors = []

        for col in cls.REQUIRED_COLUMNS:
            null_count = df[col].isna().sum()
            blank_count = (df[col].fillna("").str.strip() == "").sum()

            if null_count > 0 or blank_count > 0:
                errors.append(
                    f"Column '{col}' has {null_count} null values and {blank_count} blank values"
                )

        if errors:
            raise ValueError("Required field validation failed: " + " | ".join(errors))

    @classmethod
    def validate_npi(cls, df: pd.DataFrame) -> None:
        """Checks that NPI values are exactly 10 digits."""
        invalid_npi = df[~df["npi"].fillna("").str.fullmatch(r"\d{10}")]
        if not invalid_npi.empty:
            sample_rows = (invalid_npi.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid NPI values found. NPI must be exactly 10 digits. Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_allowed_values(cls, df: pd.DataFrame) -> None:
        """Checks that affiliation_status and affiliation_type contain only allowed values."""
        invalid_status = df[
            ~df["affiliation_status"].fillna("").isin(cls.ALLOWED_AFFILIATION_STATUS)
        ]
        if not invalid_status.empty:
            bad_values = (
                invalid_status["affiliation_status"].dropna().drop_duplicates().tolist()
            )
            raise ValueError(f"Invalid affiliation_status values: {bad_values}")

        invalid_type = df[
            ~df["affiliation_type"].fillna("").isin(cls.ALLOWED_AFFILIATION_TYPE)
        ]
        if not invalid_type.empty:
            bad_values = (
                invalid_type["affiliation_type"].dropna().drop_duplicates().tolist()
            )
            raise ValueError(f"Invalid affiliation_type values: {bad_values}")

    @classmethod
    def validate_duplicates(cls, df: pd.DataFrame) -> None:
        """Checks for duplicate records based on the combination of onekey_hcp_id, onekey_hco_id, and affiliation_type."""
        duplicate_rows = df[
            df.duplicated(
                subset=["onekey_hcp_id", "onekey_hco_id", "affiliation_type"],
                keep=False,
            )
        ]

        if not duplicate_rows.empty:
            sample = (
                duplicate_rows[["onekey_hcp_id", "onekey_hco_id", "affiliation_type"]]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(f"Duplicate affiliation records found. Examples: {sample}")

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Performs all validation steps and returns the cleaned DataFrame."""
        cls.validate_columns(df)
        df = cls.normalize(df)
        cls.validate_required_fields(df)
        cls.validate_npi(df)
        cls.validate_allowed_values(df)
        # uncomment if we need strict duplicate checking
        # cls.validate_duplicates(df)
        return df
