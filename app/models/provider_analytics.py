from sqlalchemy import Column, Integer, String, BigInteger, Date
from app.db.session import Base


class ProviderAnalytics(Base):
    __tablename__ = "provider_analytics"

    id = Column(Integer, primary_key=True, index=True)

    # HCP identifiers
    onekey_hcp_id = Column(String, nullable=True, index=True)
    npi = Column(String, nullable=True, index=True)

    # HCP details
    hcp_name = Column(String, nullable=True)
    specialty = Column(String, nullable=True)
    status = Column(String, nullable=True)

    # HCO details
    onekey_hco_id = Column(String, nullable=True, index=True)
    hco_name = Column(String, nullable=True)
    hco_type = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)

    # CRM details
    crm_account_id = Column(String, nullable=True, index=True)
    account_name = Column(String, nullable=True)
    account_type = Column(String, nullable=True)
    rep_id = Column(String, nullable=True)
    rep_name = Column(String, nullable=True)
    territory = Column(String, nullable=True)
    target_tier = Column(String, nullable=True)
    preferred_location_flag = Column(String, nullable=True)

    # RX summary
    total_trx = Column(BigInteger, nullable=True)
    rx_count = Column(Integer, nullable=True)
    last_rx_week = Column(String, nullable=True)

    # Patient events summary
    patient_event_count = Column(Integer, nullable=True)
    last_event_date = Column(String, nullable=True)

    # Audit
    processed_by = Column(String, nullable=True)
