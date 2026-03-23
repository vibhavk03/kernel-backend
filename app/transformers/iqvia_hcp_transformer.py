import pandas as pd


class IQVIAHCPTransformer:
    STRING_COLUMNS = [
        "onekey_hcp_id",
        "npi",
        "hcp_name",
        "specialty",
        "status",
        "primary_address_line1",
        "primary_address_line2",
        "primary_city",
        "primary_state",
        "primary_zip",
    ]

    @staticmethod
    def _clean_string(series: pd.Series) -> pd.Series:
        """Strip whitespace and convert empty strings to pd.NA"""
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
        """Normalize NPI to be 10 digits, removing any trailing .0 and padding with zeros if needed"""
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        # digit_mask = s.str.fullmatch(r"\d{1,10}").fillna(False)
        # s = s.where(~digit_mask, s.str.zfill(10))
        return s

    @classmethod
    def _normalize_zip(cls, series: pd.Series) -> pd.Series:
        """Normalize ZIP code to be 5 digits, removing any trailing .0 and padding with zeros if needed"""
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        # digit_mask = s.str.fullmatch(r"\d{1,5}").fillna(False)
        # s = s.where(~digit_mask, s.str.zfill(5))
        return s

    @classmethod
    def _normalize_state(cls, series: pd.Series) -> pd.Series:
        """Normalize state to be 2-letter uppercase code"""
        s = cls._clean_string(series)
        return s.str.upper()

    @classmethod
    def _normalized_key_part(cls, series: pd.Series) -> pd.Series:
        s = cls._clean_string(series)
        s = s.str.upper().str.replace(r"[^A-Z0-9]+", " ", regex=True).str.strip()
        return s

    @classmethod
    def _build_site_match_key(
        cls,
        address_line1: pd.Series,
        city: pd.Series,
        state: pd.Series,
        zip_code: pd.Series,
    ) -> pd.Series:
        """Build a composite key for site matching based on normalized address components"""
        a1 = cls._normalized_key_part(address_line1).fillna("")
        c = cls._normalized_key_part(city).fillna("")
        s = cls._normalized_key_part(state).fillna("")
        z = cls._normalized_key_part(zip_code).fillna("")

        key = a1 + "|" + c + "|" + s + "|" + z
        empty_mask = (a1 == "") & (c == "") & (s == "") & (z == "")
        key = key.mask(empty_mask, pd.NA)
        return key

    @classmethod
    def transform(cls, df: pd.DataFrame) -> pd.DataFrame:

        # remove rows where every column is empty
        df = df.dropna(how="all")

        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        for col in cls.STRING_COLUMNS:
            if col in df.columns:
                df[col] = cls._clean_string(df[col])

        if "onekey_hcp_id" in df.columns:
            df["onekey_hcp_id"] = cls._normalize_identifier(df["onekey_hcp_id"])

        if "npi" in df.columns:
            df["npi"] = cls._normalize_npi(df["npi"])

        if "primary_state" in df.columns:
            df["primary_state"] = cls._normalize_state(df["primary_state"])

        if "primary_zip" in df.columns:
            df["primary_zip"] = cls._normalize_zip(df["primary_zip"])

        if all(
            col in df.columns
            for col in [
                "primary_address_line1",
                "primary_city",
                "primary_state",
                "primary_zip",
            ]
        ):
            df["site_match_key"] = cls._build_site_match_key(
                df["primary_address_line1"],
                df["primary_city"],
                df["primary_state"],
                df["primary_zip"],
            )

        return df
