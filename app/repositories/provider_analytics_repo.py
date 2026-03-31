from sqlalchemy import text
from sqlalchemy.orm import Session


class ProviderAnalyticsRepository:
    @staticmethod
    def clear_table(db: Session, table_name: str) -> None:
        db.execute(text(f"DELETE FROM {table_name}"))
        db.commit()

    @staticmethod
    def save_dataframe(db: Session, df, table_name: str) -> int:
        df.to_sql(
            name=table_name,
            con=db.bind,
            if_exists="append",
            index=False,
        )
        db.commit()
        return len(df)
