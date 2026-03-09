import pandas as pd


class IQVIAHCOValidator:
    REQUIRED_COLUMNS = [
        "onekey_hco_id",
        "hco_name",
        "hco_type",
        "address_line1",
        "city",
        "state",
        "zip",
        "status",
    ]

    OPTIONAL_COLUMNS = [
        "address_line2",
        "parent_onekey_hco_id",
        "parent_name",
    ]

    ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

    ALLOWED_STATUS = {"Active", "Inactive"}

    @classmethod
    def validate_columns(cls, df: pd.DataFrame) -> None:
        """Ensure required columns are present and trim whitespace from column names"""
        df.columns = [col.strip() for col in df.columns]

        missing = [col for col in cls.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @classmethod
    def validate_required_fields(cls, df: pd.DataFrame) -> None:
        """Check that required fields are not null or blank and provide counts of each issue"""
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
    def validate_state(cls, df: pd.DataFrame) -> None:
        """Ensure state values are exactly 2 uppercase letters"""
        invalid_state = df[
            ~df["state"].fillna("").astype("string").str.fullmatch(r"[A-Z]{2}")
        ]
        if not invalid_state.empty:
            sample_rows = (invalid_state.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid state values found. State must be exactly 2 uppercase letters. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_zip(cls, df: pd.DataFrame) -> None:
        """Ensure zip code values are exactly 5 digits"""
        invalid_zip = df[~df["zip"].fillna("").astype("string").str.fullmatch(r"\d{5}")]
        if not invalid_zip.empty:
            sample_rows = (invalid_zip.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid zip values found. ZIP must be exactly 5 digits. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_allowed_values(cls, df: pd.DataFrame) -> None:
        """Ensure status column only contains allowed values"""
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
    def validate_hco_type(cls, df: pd.DataFrame) -> None:
        """Ensure hco_type column only contains allowed values"""
        allowed_hco_types = {"Health System", "Hospital", "Clinic"}

        invalid_hco_type = df[
            ~df["hco_type"].fillna("").astype("string").isin(allowed_hco_types)
        ]

        if not invalid_hco_type.empty:
            bad_values = (
                invalid_hco_type["hco_type"]
                .dropna()
                .astype("string")
                .drop_duplicates()
                .tolist()
            )
            raise ValueError(f"Invalid hco_type values: {bad_values}")

    @classmethod
    def validate_duplicates(cls, df: pd.DataFrame) -> None:
        """Check for duplicate onekey_hco_id values and provide sample of duplicates if found"""
        duplicate_rows = df[df.duplicated(subset=["onekey_hco_id"], keep=False)]

        if not duplicate_rows.empty:
            sample = (
                duplicate_rows[
                    [
                        "onekey_hco_id",
                        "hco_name",
                        "address_line1",
                        "city",
                        "state",
                        "zip",
                    ]
                ]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                f"Duplicate HCO records found for onekey_hco_id. Examples: {sample}"
            )

    @classmethod
    def validate_parent_reference(cls, df: pd.DataFrame) -> None:
        """Ensure that parent_onekey_hco_id values, if present, reference valid onekey_hco_id values"""
        if "parent_onekey_hco_id" not in df.columns:
            return

        valid_ids = set(df["onekey_hco_id"].dropna().astype("string"))
        parent_ids = df["parent_onekey_hco_id"].dropna().astype("string")

        invalid_parent_ids = sorted(set(parent_ids) - valid_ids)
        if invalid_parent_ids:
            raise ValueError(
                f"Invalid parent_onekey_hco_id values found that do not exist in onekey_hco_id: "
                f"{invalid_parent_ids[:10]}"
            )

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        cls.validate_columns(df)
        cls.validate_required_fields(df)
        cls.validate_state(df)
        cls.validate_zip(df)
        cls.validate_allowed_values(df)
        cls.validate_hco_type(df)
        cls.validate_duplicates(df)
        cls.validate_parent_reference(df)
        return df
