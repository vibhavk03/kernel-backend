import pandas as pd


class IQVIAHCPValidator:
    REQUIRED_COLUMNS = [
        "onekey_hcp_id",
        "npi",
        "hcp_name",
        "specialty",
        "status",
        "primary_address_line1",
        "primary_city",
        "primary_state",
        "primary_zip",
    ]

    OPTIONAL_COLUMNS = [
        "primary_address_line2",
    ]

    ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

    ALLOWED_STATUS = {"Active", "Inactive"}

    @classmethod
    def validate_columns(cls, df: pd.DataFrame) -> None:
        """Check for required columns and strip whitespace from column names"""
        df.columns = [col.strip() for col in df.columns]

        missing = [col for col in cls.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @classmethod
    def validate_required_fields(cls, df: pd.DataFrame) -> None:
        """Check that required fields are not null or blank"""
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
    def validate_npi(cls, df: pd.DataFrame) -> None:
        """Check that NPI values are either null or exactly 10 digits"""
        invalid_npi = df[
            ~df["npi"].fillna("").astype("string").str.fullmatch(r"\d{10}")
        ]
        if not invalid_npi.empty:
            sample_rows = (invalid_npi.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid NPI values found. NPI must be exactly 10 digits. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_state(cls, df: pd.DataFrame) -> None:
        """Check that state values are either null or exactly 2 uppercase letters"""
        invalid_state = df[
            ~df["primary_state"].fillna("").astype("string").str.fullmatch(r"[A-Z]{2}")
        ]
        if not invalid_state.empty:
            sample_rows = (invalid_state.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid primary_state values found. State must be exactly 2 uppercase letters. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_zip(cls, df: pd.DataFrame) -> None:
        """Check that ZIP code values are either null or exactly 5 digits"""
        invalid_zip = df[
            ~df["primary_zip"].fillna("").astype("string").str.fullmatch(r"\d{5}")
        ]
        if not invalid_zip.empty:
            sample_rows = (invalid_zip.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid primary_zip values found. ZIP must be exactly 5 digits. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_allowed_values(cls, df: pd.DataFrame) -> None:
        """Check that 'status' column only contains allowed values"""
        invalid_status = df[
            ~df["status"].fillna("").astype("string").isin(cls.ALLOWED_STATUS)
        ]
        if not invalid_status.empty:
            bad_values = (
                invalid_status["status"]
                .dropna()
                .astype("string")
                .drop_duplicates()
                .tolist()
            )
            raise ValueError(f"Invalid status values: {bad_values}")

    @classmethod
    def validate_duplicates(cls, df: pd.DataFrame) -> None:
        """Check for duplicate onekey_hcp_id values"""
        duplicate_rows = df[df.duplicated(subset=["onekey_hcp_id"], keep=False)]

        if not duplicate_rows.empty:
            sample = (
                duplicate_rows[["onekey_hcp_id", "npi", "hcp_name"]]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                f"Duplicate HCP records found for onekey_hcp_id. Examples: {sample}"
            )

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        cls.validate_columns(df)
        cls.validate_required_fields(df)
        cls.validate_npi(df)
        cls.validate_state(df)
        cls.validate_zip(df)
        cls.validate_allowed_values(df)
        cls.validate_duplicates(df)
        return df
