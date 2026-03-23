import pandas as pd


class CRMAccountsTransformer:
    STRING_COLUMNS = [
        "crm_account_id",
        "account_name",
        "account_type",
        "address_line1",
        "address_line2",
        "city",
        "state",
        "zip",
        "linked_onekey_hco_id",
        "parent_account_name",
        "notes",
    ]

    @staticmethod
    def _clean_string(series: pd.Series) -> pd.Series:
        s = series.astype("string").str.strip()
        s = s.mask(s == "", pd.NA)
        return s

    @classmethod
    def _normalize_identifier(cls, series: pd.Series) -> pd.Series:
        s = cls._clean_string(series)
        return s.str.replace(r"\.0$", "", regex=True)

    @classmethod
    def _normalize_zip(cls, series: pd.Series) -> pd.Series:
        s = cls._clean_string(series).str.replace(r"\.0$", "", regex=True)
        digit_mask = s.str.fullmatch(r"\d{1,5}").fillna(False)
        s = s.where(~digit_mask, s.str.zfill(5))
        return s

    @classmethod
    def _normalize_state(cls, series: pd.Series) -> pd.Series:
        s = cls._clean_string(series)
        return s.str.upper()

    @classmethod
    def _normalized_key_part(cls, series: pd.Series) -> pd.Series:
        s = cls._clean_string(series)
        s = s.str.upper().str.replace(r"[^A-Z0-9]+", " ", regex=True).str.strip()
        return s

    @classmethod
    def _build_account_match_key(
        cls,
        account_name: pd.Series,
        address_line1: pd.Series,
        city: pd.Series,
        state: pd.Series,
        zip_code: pd.Series,
    ) -> pd.Series:
        a = cls._normalized_key_part(account_name).fillna("")
        a1 = cls._normalized_key_part(address_line1).fillna("")
        c = cls._normalized_key_part(city).fillna("")
        s = cls._normalized_key_part(state).fillna("")
        z = cls._normalized_key_part(zip_code).fillna("")

        key = a + "|" + a1 + "|" + c + "|" + s + "|" + z
        empty_mask = (a == "") & (a1 == "") & (c == "") & (s == "") & (z == "")
        key = key.mask(empty_mask, pd.NA)
        return key

    @classmethod
    def transform(cls, df: pd.DataFrame) -> pd.DataFrame:

        # remove rows where every column is empty
        df = df.dropna(how="all")

        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        # Drop fully empty rows first
        df = df.dropna(how="all").reset_index(drop=True)

        for col in cls.STRING_COLUMNS:
            if col in df.columns:
                df[col] = cls._clean_string(df[col])

        if "crm_account_id" in df.columns:
            df["crm_account_id"] = cls._normalize_identifier(df["crm_account_id"])

        if "linked_onekey_hco_id" in df.columns:
            df["linked_onekey_hco_id"] = cls._normalize_identifier(
                df["linked_onekey_hco_id"]
            )

        if "state" in df.columns:
            df["state"] = cls._normalize_state(df["state"])

        if "zip" in df.columns:
            df["zip"] = cls._normalize_zip(df["zip"])

        if all(
            col in df.columns
            for col in ["account_name", "address_line1", "city", "state", "zip"]
        ):
            df["account_match_key"] = cls._build_account_match_key(
                df["account_name"],
                df["address_line1"],
                df["city"],
                df["state"],
                df["zip"],
            )

        return df
