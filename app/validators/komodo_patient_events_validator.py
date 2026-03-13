import pandas as pd


class KomodoPatientEventsValidator:
    REQUIRED_COLUMNS = [
        "event_id",
        "patient_id",
        "event_date",
        "event_type",
        "rendering_npi",
        "rendering_hcp_name",
        "facility_name",
        "facility_address_line1",
        "facility_city",
        "facility_state",
        "facility_zip",
        "diagnosis",
    ]

    ALLOWED_EVENT_TYPES = {
        "Derm Visit",
        "Biologic Administration",
    }

    @classmethod
    def validate_columns(cls, df: pd.DataFrame) -> None:
        """Ensure required columns are present and trim whitespace from column names"""
        df.columns = [col.strip() for col in df.columns]

        missing = [col for col in cls.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @classmethod
    def validate_required_fields(cls, df: pd.DataFrame) -> None:
        """Check for null or blank values in required fields and report counts"""
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
    def validate_event_id(cls, df: pd.DataFrame) -> None:
        """Check that event_id matches the pattern 'EV<number>'"""
        invalid_event_id = df[
            ~df["event_id"].fillna("").astype("string").str.fullmatch(r"EV\d+")
        ]
        if not invalid_event_id.empty:
            sample_rows = (invalid_event_id.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid event_id values found. event_id must match pattern 'EV<number>'. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_patient_id(cls, df: pd.DataFrame) -> None:
        """Check that patient_id matches the pattern 'P<number>-<number>'"""
        invalid_patient_id = df[
            ~df["patient_id"].fillna("").astype("string").str.fullmatch(r"P\d+-\d+")
        ]
        if not invalid_patient_id.empty:
            sample_rows = (invalid_patient_id.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid patient_id values found. patient_id must match pattern 'P<number>-<number>'. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_event_date(cls, df: pd.DataFrame) -> None:
        """Check that event_date can be parsed as a date"""
        invalid_event_date = df[df["event_date"].isna()]
        if not invalid_event_date.empty:
            sample_rows = (invalid_event_date.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid event_date values found. event_date must be a valid date. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_rendering_npi(cls, df: pd.DataFrame) -> None:
        """Check that rendering_npi is either empty or exactly 10 digits"""
        invalid_npi = df[
            ~df["rendering_npi"].fillna("").astype("string").str.fullmatch(r"\d{10}")
        ]
        if not invalid_npi.empty:
            sample_rows = (invalid_npi.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid rendering_npi values found. NPI must be exactly 10 digits. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_event_type(cls, df: pd.DataFrame) -> None:
        """Check that event_type is one of the allowed values"""
        invalid_event_type = df[
            ~df["event_type"].fillna("").astype("string").isin(cls.ALLOWED_EVENT_TYPES)
        ]
        if not invalid_event_type.empty:
            bad_values = (
                invalid_event_type["event_type"]
                .dropna()
                .astype("string")
                .drop_duplicates()
                .tolist()
            )
            raise ValueError(f"Invalid event_type values: {bad_values}")

    @classmethod
    def validate_state(cls, df: pd.DataFrame) -> None:
        """Check that facility_state is either empty or exactly 2 uppercase letters"""
        invalid_state = df[
            ~df["facility_state"].fillna("").astype("string").str.fullmatch(r"[A-Z]{2}")
        ]
        if not invalid_state.empty:
            sample_rows = (invalid_state.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid facility_state values found. State must be exactly 2 uppercase letters. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_zip(cls, df: pd.DataFrame) -> None:
        """Check that facility_zip is either empty or exactly 5 digits"""
        invalid_zip = df[
            ~df["facility_zip"].fillna("").astype("string").str.fullmatch(r"\d{5}")
        ]
        if not invalid_zip.empty:
            sample_rows = (invalid_zip.index + 2).tolist()[:5]
            raise ValueError(
                f"Invalid facility_zip values found. ZIP must be exactly 5 digits. "
                f"Example CSV row numbers: {sample_rows}"
            )

    @classmethod
    def validate_duplicates(cls, df: pd.DataFrame) -> None:
        """Check for duplicate event_id values and report examples if found"""
        duplicate_rows = df[df.duplicated(subset=["event_id"], keep=False)]

        if not duplicate_rows.empty:
            sample = (
                duplicate_rows[["event_id", "patient_id", "event_date", "event_type"]]
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                f"Duplicate patient event records found for event_id. Examples: {sample}"
            )

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        cls.validate_columns(df)
        cls.validate_required_fields(df)
        cls.validate_event_id(df)
        cls.validate_patient_id(df)
        cls.validate_event_date(df)
        cls.validate_rendering_npi(df)
        cls.validate_event_type(df)
        cls.validate_state(df)
        cls.validate_zip(df)
        # cls.validate_duplicates(df)
        return df
