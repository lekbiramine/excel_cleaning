import pandas as pd

def top_rejection_reasons(rejected_df: pd.DataFrame, top_n: int = 10, logger=None):
    if rejected_df.empty:
        if logger: logger.info("No rejected rows for top_rejection_reasons")
        return pd.DataFrame()
    
    result = (
        rejected_df['rejection_reason']
        .value_counts()
        .head(top_n)
        .reset_index()
    )
    result.columns = ['rejection_reason', 'count']
    
    if logger: logger.info(f"Top {top_n} rejection reasons calculated")
    return result

def null_ratios(df: pd.DataFrame, logger=None):
    ratios = df.isna().mean()
    if logger: logger.info("Null ratios calculated")
    return ratios

def duplicate_rate(df: pd.DataFrame, subset=None, logger=None):
    rate = df.duplicated(subset=subset).mean()
    if logger: logger.info(f"Duplicate rate calculated: {rate:.2%}")
    return rate