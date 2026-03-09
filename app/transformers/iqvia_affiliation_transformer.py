import pandas as pd


class IQVIAAffiliationTransformer:
    STRING_COLUMNS = [
        "onekey_hcp_id",
        "npi",
        "onekey_hco_id",
        "hco_name",
        "affiliation_type",
        "affiliation_status",
    ]

    @staticmethod
    def _clean_string(series: pd.Series) -> pd.Series:
        """Strips whitespace and converts empty strings to NA."""
        s = series.astype("string").str.strip()
        s = s.mask(s == "", pd.NA)
        return s

    @classmethod
    def _normalize_identifier(cls, series: pd.Series) -> pd.Series:
        """Cleans string and removes trailing .0 from numeric identifiers."""
        s = cls._clean_string(series)
        return s.str.replace(r"\.0$", "", regex=True)

    @classmethod
    def _normalize_npi(cls, series: pd.Series) -> pd.Series:
        """Specifically cleans NPI column, ensuring it's treated as a string and removing any trailing .0 from numeric values."""
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        # if we want to left pad the NPI to 10 digits, we could do that here
        # digit_mask = s.str.fullmatch(r"\d{1,10}").fillna(False)
        # s = s.where(~digit_mask, s.str.zfill(10))
        return s

    @classmethod
    def transform(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Applies all transformations to clean and normalize the DataFrame."""
        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        for col in cls.STRING_COLUMNS:
            if col in df.columns:
                df[col] = cls._clean_string(df[col])

        if "onekey_hcp_id" in df.columns:
            df["onekey_hcp_id"] = cls._normalize_identifier(df["onekey_hcp_id"])

        if "onekey_hco_id" in df.columns:
            df["onekey_hco_id"] = cls._normalize_identifier(df["onekey_hco_id"])

        if "npi" in df.columns:
            df["npi"] = cls._normalize_npi(df["npi"])

        return df
