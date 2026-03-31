import pandas as pd


def build_analytics_table(processed_files: dict) -> pd.DataFrame:
    affiliation_df = processed_files["affiliation"]["df"].copy()
    hcp_df = processed_files["hcp"]["df"].copy()
    hco_df = processed_files["hco"]["df"].copy()
    crm_targeting_df = processed_files["crm_targeting"]["df"].copy()
    crm_accounts_df = processed_files["crm_accounts"]["df"].copy()
    rx_df = processed_files["rx"]["df"].copy()
    patient_events_df = processed_files["patient_events"]["df"].copy()

    # aggregate rx data at NPI level
    rx_summary = (
        rx_df.groupby("npi", dropna=False)
        .agg(
            total_trx=("trx", "sum"),
            rx_count=("rx_id", "count"),
            last_rx_week=("week_start", "max"),
        )
        .reset_index()
    )

    # aggregate patient events at NPI level
    patient_events_summary = (
        patient_events_df.groupby("rendering_npi", dropna=False)
        .agg(
            patient_event_count=("event_id", "count"),
            last_event_date=("event_date", "max"),
        )
        .reset_index()
        .rename(columns={"rendering_npi": "npi"})
    )

    # start with affiliation as the bridge between HCP and HCO
    analytics_df = affiliation_df.merge(
        hcp_df,
        on=["onekey_hcp_id", "npi"],
        how="left",
        suffixes=("", "_hcp"),
    )

    analytics_df = analytics_df.merge(
        hco_df,
        on="onekey_hco_id",
        how="left",
        suffixes=("", "_hco"),
    )

    analytics_df = analytics_df.merge(
        crm_targeting_df,
        on="npi",
        how="left",
        suffixes=("", "_crm"),
    )

    if (
        "crm_account_id" in analytics_df.columns
        and "crm_account_id" in crm_accounts_df.columns
    ):
        analytics_df = analytics_df.merge(
            crm_accounts_df,
            on="crm_account_id",
            how="left",
            suffixes=("", "_account"),
        )

    analytics_df = analytics_df.merge(
        rx_summary,
        on="npi",
        how="left",
    )

    analytics_df = analytics_df.merge(
        patient_events_summary,
        on="npi",
        how="left",
    )

    # optional cleanup / final column selection
    selected_columns = [
        "onekey_hcp_id",
        "npi",
        "hcp_name",
        "specialty",
        "status",
        "onekey_hco_id",
        "hco_name",
        "hco_type",
        "city",
        "state",
        "crm_account_id",
        "account_name",
        "account_type",
        "rep_id",
        "rep_name",
        "territory",
        "target_tier",
        "preferred_location_flag",
        "total_trx",
        "rx_count",
        "last_rx_week",
        "patient_event_count",
        "last_event_date",
    ]

    selected_columns = [col for col in selected_columns if col in analytics_df.columns]
    analytics_df = analytics_df[selected_columns].copy()

    return analytics_df
