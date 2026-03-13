import pandas as pd


class CRMTargetingTransformer:
    STRING_COLUMNS = [
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

    @staticmethod
    def _clean_string(series: pd.Series) -> pd.Series:
        """clear leading/trailing whitespace and convert empty strings to NA"""
        s = series.astype("string").str.strip()
        s = s.mask(s == "", pd.NA)
        return s

    @classmethod
    def _normalize_identifier(cls, series: pd.Series) -> pd.Series:
        """Remove trailing .0 from numeric identifiers and ensure they are treated as strings."""
        s = cls._clean_string(series)
        return s.str.replace(r"\.0$", "", regex=True)

    @classmethod
    def _normalize_npi(cls, series: pd.Series) -> pd.Series:
        """Normalize NPI by removing trailing .0 and optionallyzero-padding to 10 digits if it's numeric."""
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        # digit_mask = s.str.fullmatch(r"\d{1,10}").fillna(False)
        # s = s.where(~digit_mask, s.str.zfill(10))
        return s

    @classmethod
    def _normalize_flag(cls, series: pd.Series) -> pd.Series:
        """Normalize boolean flags to 1/0 integers."""
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        return pd.to_numeric(s, errors="coerce").astype("Int64")

    @classmethod
    def transform(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations to the CRM targeting DataFrame."""
        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        # Drop fully empty rows first
        df = df.dropna(how="all").reset_index(drop=True)

        for col in cls.STRING_COLUMNS:
            if col in df.columns:
                df[col] = cls._clean_string(df[col])

        if "rep_id" in df.columns:
            df["rep_id"] = cls._normalize_identifier(df["rep_id"])

        if "crm_account_id" in df.columns:
            df["crm_account_id"] = cls._normalize_identifier(df["crm_account_id"])

        if "npi" in df.columns:
            df["npi"] = cls._normalize_npi(df["npi"])

        if "target_tier" in df.columns:
            df["target_tier"] = cls._clean_string(df["target_tier"]).str.upper()

        if "preferred_location_flag" in df.columns:
            df["preferred_location_flag"] = cls._normalize_flag(
                df["preferred_location_flag"]
            )

        return df
