import pandas as pd


class IQVIARXTransformer:
    STRING_COLUMNS = [
        "rx_id",
        "npi",
        "prescriber_name",
        "product",
        "week_start",
        "site_name",
        "site_address_line1",
        "site_address_line2",
        "site_city",
        "site_state",
        "site_zip",
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
        """Remove trailing .0"""
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
    def _normalize_product(cls, series: pd.Series) -> pd.Series:
        """Trim, uppercase, and collapse multiple spaces for product names"""
        s = cls._clean_string(series)
        return s.str.upper()

    @classmethod
    def _normalize_trx(cls, series: pd.Series) -> pd.Series:
        """Trim and convert trx to numeric, coercing errors to NA"""
        s = series.astype("string").str.replace(",", "", regex=False).str.strip()
        s = s.mask(s == "", pd.NA)
        return pd.to_numeric(s, errors="coerce").astype("Int64")

    @classmethod
    def _normalize_week_start(cls, series: pd.Series) -> pd.Series:
        """Trim and convert week_start to date, handling common Excel/CSV date formats."""
        s = cls._clean_string(series)

        parsed = pd.to_datetime(
            s,
            errors="raise",
            dayfirst=True,
            format="mixed",
        ).dt.date

        return parsed

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
        """Build a site match key by normalizing and concatenating address components"""
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
        """Apply transformations to clean and normalize IQVIA RX data"""
        df = df.copy()
        df.columns = [col.strip() for col in df.columns]

        for col in cls.STRING_COLUMNS:
            if col in df.columns:
                df[col] = cls._clean_string(df[col])

        if "rx_id" in df.columns:
            df["rx_id"] = cls._normalize_identifier(df["rx_id"])

        if "npi" in df.columns:
            df["npi"] = cls._normalize_npi(df["npi"])

        if "product" in df.columns:
            df["product"] = cls._normalize_product(df["product"])

        if "trx" in df.columns:
            df["trx"] = cls._normalize_trx(df["trx"])

        if "week_start" in df.columns:
            df["week_start"] = cls._normalize_week_start(df["week_start"])

        if "site_state" in df.columns:
            df["site_state"] = cls._normalize_state(df["site_state"])

        if "site_zip" in df.columns:
            df["site_zip"] = cls._normalize_zip(df["site_zip"])

        if all(
            col in df.columns
            for col in [
                "site_address_line1",
                "site_city",
                "site_state",
                "site_zip",
            ]
        ):
            df["site_match_key"] = cls._build_site_match_key(
                df["site_address_line1"],
                df["site_city"],
                df["site_state"],
                df["site_zip"],
            )

        return df
