import os
import logging
from typing import Tuple
import pandas as pd
import numpy as np

# Configure production logging framework
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SocialEngineETL")


class SocialEngineETL:
    def __init__(self, users_path: str, posts_path: str):
        self.users_path = users_path
        self.posts_path = posts_path
        self.users_df = pd.DataFrame()
        self.posts_df = pd.DataFrame()

    def load_raw_streams(self) -> None:
        """Inbound ingestion layer for degraded raw CSV streams."""
        logger.info("Ingesting raw stream artifacts...")
        if not os.path.exists(self.users_path) or not os.path.exists(self.posts_path):
            raise FileNotFoundError("Target raw stream files missing from execution workspace.")
        
        self.users_df = pd.read_csv(self.users_path)
        self.posts_df = pd.read_csv(self.posts_path)
        logger.info(f"Ingested {len(self.users_df):,} user entities and {len(self.posts_df):,} raw post events.")

    def clean_user_entities(self) -> pd.DataFrame:
        """Cleans and standardizes user dimension entities."""
        logger.info("Executing entity resolution on User Dimension...")
        df = self.users_df.copy()

        # Deduplicate on Primary Key
        initial_len = len(df)
        df = df.drop_duplicates(subset=['user_id'], keep='first')
        logger.info(f"User deduplication: Removed {initial_len - len(df)} duplicate records.")

        # Datetime Standardization
        df['account_created'] = pd.to_datetime(df['account_created'], errors='coerce')
        df['account_created'] = df['account_created'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Type constraints & null guards
        df['follower_count'] = df['follower_count'].fillna(0).astype(int)
        df['location'] = df['location'].fillna('Unspecified')
        df['language'] = df['language'].fillna('en')

        return df

    @staticmethod
    def _parse_corrupted_timestamp(val: str) -> pd.Timestamp:
        """Vector-compatible fallback datetime parser handling mixed formats."""
        if pd.isna(val):
            return pd.NaT
        val_str = str(val).strip()
        try:
            # Unix epoch timestamp detection (e.g., 1722528840)
            if val_str.isdigit():
                return pd.to_datetime(int(val_str), unit='s')
            # Standard string parsing (ISO 8601 & European formats)
            return pd.to_datetime(val_str, dayfirst=True)
        except Exception:
            return pd.NaT

    def clean_post_events(self) -> pd.DataFrame:
        """Cleans, imputes, and normalizes post event metrics."""
        logger.info("Executing transformation pipeline on Post Events Stream...")
        df = self.posts_df.copy()

        # 1. Deduplication
        initial_len = len(df)
        df = df.drop_duplicates(subset=['post_id'], keep='first')
        logger.info(f"Post deduplication: Removed {initial_len - len(df)} duplicate frame buffers.")

        # 2. Categorical Imputation
        df['platform'] = df['platform'].fillna('Unknown')
        df['text_content'] = df['text_content'].fillna('[Content Telemetry Lost]')

        # 3. Anomaly Correction: Bit-flip Negative Metrics Resolution
        negative_likes_count = (df['likes'] < 0).sum()
        if negative_likes_count > 0:
            logger.info(f"Corrected {negative_likes_count} bit-flipped negative engagement metrics via absolute transformation.")
            df['likes'] = df['likes'].abs()

        # 4. Grouped Median Imputation for Engagement Metrics
        df['likes'] = df.groupby('platform')['likes'].transform(lambda g: g.fillna(g.median()))
        df['likes'] = df['likes'].fillna(df['likes'].median()).round().astype(int)

        # 5. Timestamp Format Normalization
        df['timestamp'] = df['timestamp'].apply(self._parse_corrupted_timestamp)
        # Fallback for unparseable timestamps (forward fill temporal context)
        df['timestamp'] = df['timestamp'].ffill().bfill()
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # 6. Integer casting for quantitative counts
        df['shares'] = df['shares'].fillna(0).astype(int)
        df['comments'] = df['comments'].fillna(0).astype(int)

        return df

    def run_pipeline(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Main ETL Execution flow."""
        self.load_raw_streams()
        users_clean = self.clean_user_entities()
        posts_clean = self.clean_post_events()

        # Save production outputs
        users_clean.to_csv('Cleaned_Social_Engine_Users.csv', index=False)
        posts_clean.to_csv('Cleaned_Social_Engine_Posts.csv', index=False)

        logger.info("ETL Pipeline completed. Cleaned outputs serialized to disk.")
        return users_clean, posts_clean


if __name__ == "__main__":
    pipeline = SocialEngineETL(
        users_path='data/raw/Social_Engine_Users.csv',
        posts_path='data/raw/Social_Engine_Posts_Corrupted.csv'
    )
    users_cleaned, posts_cleaned = pipeline.run_pipeline()