import pandas as pd


class KomodoPatientEventsTransformer:
    STRING_COLUMNS = [
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

    @staticmethod
    def _clean_string(series: pd.Series) -> pd.Series:
        """Trim whitespace and convert empty strings to NA"""
        s = series.astype("string").str.strip()
        s = s.mask(s == "", pd.NA)
        return s

    @classmethod
    def _normalize_identifier(cls, series: pd.Series) -> pd.Series:
        """Remove trailing .0 from identifiers that may have been read as floats"""
        s = cls._clean_string(series)
        return s.str.replace(r"\.0$", "", regex=True)

    @classmethod
    def _normalize_npi(cls, series: pd.Series) -> pd.Series:
        """Remove trailing .0 and optionally zero-pad NPIs to 10 digits"""
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        # digit_mask = s.str.fullmatch(r"\d{1,10}").fillna(False)
        # s = s.where(~digit_mask, s.str.zfill(10))
        return s

    @classmethod
    def _normalize_zip(cls, series: pd.Series) -> pd.Series:
        """Remove trailing .0 and zero-pad zip codes to 5 digits"""
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        digit_mask = s.str.fullmatch(r"\d{1,5}").fillna(False)
        s = s.where(~digit_mask, s.str.zfill(5))
        return s

    @classmethod
    def _normalize_state(cls, series: pd.Series) -> pd.Series:
        """Trim, uppercase, and remove non-alphanumeric characters from state abbreviations"""
        s = cls._clean_string(series)
        return s.str.upper()

    @classmethod
    def _normalize_event_date(cls, series: pd.Series) -> pd.Series:
        s = cls._clean_string(series)

        parsed = pd.to_datetime(
            s,
            errors="raise",
            dayfirst=True,
            format="mixed",
        ).dt.date

        return parsed
        # return pd.to_datetime(series, errors="coerce").dt.date

    @classmethod
    def _normalized_key_part(cls, series: pd.Series) -> pd.Series:
        """Trim, uppercase, and remove non-alphanumeric characters for site match key parts"""
        s = cls._clean_string(series)
        s = s.str.upper().str.replace(r"[^A-Z0-9]+", " ", regex=True).str.strip()
        return s

    @classmethod
    def _build_facility_match_key(
        cls,
        facility_name: pd.Series,
        address_line1: pd.Series,
        city: pd.Series,
        state: pd.Series,
        zip_code: pd.Series,
    ) -> pd.Series:
        """Construct a facility match key by normalizing and concatenating key parts"""
        f = cls._normalized_key_part(facility_name).fillna("")
        a1 = cls._normalized_key_part(address_line1).fillna("")
        c = cls._normalized_key_part(city).fillna("")
        s = cls._normalized_key_part(state).fillna("")
        z = cls._normalized_key_part(zip_code).fillna("")

        key = f + "|" + a1 + "|" + c + "|" + s + "|" + z
        empty_mask = (f == "") & (a1 == "") & (c == "") & (s == "") & (z == "")
        key = key.mask(empty_mask, pd.NA)
        return key

    @classmethod
    def transform(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to the Komodo patient events DataFrame to clean and normalize data."""
        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        for col in cls.STRING_COLUMNS:
            if col in df.columns:
                df[col] = cls._clean_string(df[col])

        if "event_id" in df.columns:
            df["event_id"] = cls._normalize_identifier(df["event_id"])

        if "patient_id" in df.columns:
            df["patient_id"] = cls._normalize_identifier(df["patient_id"])

        if "rendering_npi" in df.columns:
            df["rendering_npi"] = cls._normalize_npi(df["rendering_npi"])

        if "facility_state" in df.columns:
            df["facility_state"] = cls._normalize_state(df["facility_state"])

        if "facility_zip" in df.columns:
            df["facility_zip"] = cls._normalize_zip(df["facility_zip"])

        if "event_date" in df.columns:
            df["event_date"] = cls._normalize_event_date(df["event_date"])

        if all(
            col in df.columns
            for col in [
                "facility_name",
                "facility_address_line1",
                "facility_city",
                "facility_state",
                "facility_zip",
            ]
        ):
            df["facility_match_key"] = cls._build_facility_match_key(
                df["facility_name"],
                df["facility_address_line1"],
                df["facility_city"],
                df["facility_state"],
                df["facility_zip"],
            )

        return df
