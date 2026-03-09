import pandas as pd


class IQVIAHCOTransformer:
    STRING_COLUMNS = [
        "onekey_hco_id",
        "hco_name",
        "hco_type",
        "address_line1",
        "address_line2",
        "city",
        "state",
        "zip",
        "status",
        "parent_onekey_hco_id",
        "parent_name",
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
    def _normalized_key_part(cls, series: pd.Series) -> pd.Series:
        """Trim, uppercase, and remove non-alphanumeric characters for site match key parts"""
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
        """Construct a site match key by normalizing and concatenating address components"""
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
        """Perform all necessary transformations and standardizations on the HCO dataframe"""
        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        for col in cls.STRING_COLUMNS:
            if col in df.columns:
                df[col] = cls._clean_string(df[col])

        if "onekey_hco_id" in df.columns:
            df["onekey_hco_id"] = cls._normalize_identifier(df["onekey_hco_id"])

        if "parent_onekey_hco_id" in df.columns:
            df["parent_onekey_hco_id"] = cls._normalize_identifier(
                df["parent_onekey_hco_id"]
            )

        if "state" in df.columns:
            df["state"] = cls._normalize_state(df["state"])

        if "zip" in df.columns:
            df["zip"] = cls._normalize_zip(df["zip"])

        if all(col in df.columns for col in ["address_line1", "city", "state", "zip"]):
            df["site_match_key"] = cls._build_site_match_key(
                df["address_line1"],
                df["city"],
                df["state"],
                df["zip"],
            )

        return df
