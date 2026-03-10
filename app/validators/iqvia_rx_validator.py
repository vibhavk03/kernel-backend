import pandas as pd


class IQVIARXValidator:
    REQUIRED_COLUMNS = [
        "rx_id",
        "npi",
        "prescriber_name",
        "product",
        "trx",
        "week_start",
        "site_name",
        "site_address_line1",
        "site_city",
        "site_state",
        "site_zip",
    ]

    OPTIONAL_COLUMNS = [
        "site_address_line2",
    ]

    ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

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
    def validate_npi(cls, df: pd.DataFrame) -> None:
        """Ensure NPI values are exactly 10 digits, allowing for leading zeros"""
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
        """Ensure site_state values are exactly 2 uppercase letters"""
        invalid_state = df[
            ~df["site_state"].fillna("").astype("string").str.fullmatch(r"[A-Z]{2}")
        ]
        if not invalid_state.empty:
            sample_rows = (invalid_state.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid site_state values found. State must be exactly 2 uppercase letters. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_zip(cls, df: pd.DataFrame) -> None:
        """Ensure site_zip values are exactly 5 digits"""
        invalid_zip = df[
            ~df["site_zip"].fillna("").astype("string").str.fullmatch(r"\d{5}")
        ]
        if not invalid_zip.empty:
            sample_rows = (invalid_zip.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid site_zip values found. ZIP must be exactly 5 digits. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_trx(cls, df: pd.DataFrame) -> None:
        invalid_trx = df[df["trx"].isna() | (df["trx"] < 0)]

        if not invalid_trx.empty:
            sample_rows = (invalid_trx.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid trx values found. trx must be a non-negative integer. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_week_start(cls, df: pd.DataFrame) -> None:
        """Ensure week_start values are valid dates"""
        invalid_week_start = df[df["week_start"].isna()]

        if not invalid_week_start.empty:
            sample_rows = (invalid_week_start.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid week_start values found. week_start must be a valid date. "
                f"Example Excel row numbers: {sample_rows}"
            )

    @classmethod
    def validate_duplicates(cls, df: pd.DataFrame) -> None:
        """Check for duplicate rx_id values and provide examples of duplicates found"""
        duplicate_rows = df[df.duplicated(subset=["rx_id"], keep=False)]

        if not duplicate_rows.empty:
            sample = (
                duplicate_rows[["rx_id", "npi", "product", "week_start"]]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                f"Duplicate RX records found for rx_id. Examples: {sample}"
            )

    @classmethod
    def validate_product(cls, df: pd.DataFrame) -> None:
        """Check for suspicious product values that may indicate parsing issues or bad data"""
        # Keep this simple for now:
        # only reject blank/null via validate_required_fields.
        # If you later want a strict product master, replace this method.
        suspicious = df[df["product"].fillna("").astype("string").str.len() < 3]

        if not suspicious.empty:
            sample = suspicious[["rx_id", "product"]].head(5).to_dict(orient="records")
            raise ValueError(f"Suspicious product values found. Examples: {sample}")

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        cls.validate_columns(df)
        cls.validate_required_fields(df)
        cls.validate_npi(df)
        cls.validate_state(df)
        cls.validate_zip(df)
        cls.validate_trx(df)
        cls.validate_week_start(df)
        # cls.validate_product(df)
        cls.validate_duplicates(df)
        return df
