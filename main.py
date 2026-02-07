import pandas as pd
import logging
import json
import yaml
import smtplib
import ssl
import os
from dotenv import load_dotenv
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime, timezone

SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}
LOG_NAME = "excel_cleaning_pipeline"

class AppEnvironment:
    """
    Handles application-wide setup:
    - Base paths
    - Directory creation
    - Logging configuration
    """

    def __init__(self):
        self.base_dir = Path(__file__).parent

        # Input
        self.input_dir = self.base_dir / "input" / "raw_files"

        # Output
        self.output_dir = self.base_dir / "output"
        self.cleaned_dir = self.output_dir / "cleaned"
        self.rejected_dir = self.output_dir / "rejected"
        self.reports_dir = self.output_dir / "reports"
        self.logs_dir = self.output_dir / "logs"

        # Config
        self.config_dir = self.base_dir / "config"
        self.schema_path = self.config_dir / "schema.json"
        self.rules_path = self.config_dir / "rules.yaml"

        self.logger: logging.Logger | None = None

    def setup(self) -> None:
        self._create_directories()
        self._setup_logging()
    
    def _create_directories(self) -> None:
        for directory in [
            self.input_dir,
            self.cleaned_dir,
            self.rejected_dir,
            self.reports_dir,
            self.logs_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _setup_logging(self) -> None:
        self.logger = logging.getLogger(LOG_NAME)
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "(%(asctime)s) | %(name)s | %(levelname)s => '%(message)s'"
        )

        log_file = self.logs_dir / "pipeline.log"

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)
        
        self.logger.info("Application environment initialized.")

class FileLoader:
    """
    Loads Excel files safely.
    Adds metadata for traceability.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
    
    def load_files(self, files: list[Path]) -> list[pd.DataFrame]:
        """
        Loads files into DataFrames.
        Returns a list of valid DataFrames.
        """
        loaded_dfs : list[pd.DataFrame] = []

        for file in files:
            try:
                self.logger.info(f"Loading file: {file.name}")

                if file.suffix.lower() == ".xlsx":
                    df = pd.read_excel(file)
                
                else:
                    self.logger.warning(f"Unsupported file type: {file.name}")
                    continue

                if df.empty:
                    self.logger.warning(f"File is empty: {file.name}")
                
                # Adding metadata
                df["source_file"] = file.name
                df["load_time"] = datetime.now(timezone.utc)

                loaded_dfs.append(df)
                self.logger.info(f"Loaded {len(df)} rows from {file.name}")
            
            except Exception as e:
                self.logger.error(f"Failed to load {file.name} => {e}")
            
        if not loaded_dfs:
            self.logger.critical("No files were successfully loaded.")
        
        return loaded_dfs 
    
class SchemaAligner:
    """
    Aligns DataFrame columns to a standard schema.
    - Normalizes names
    - Adds missing required columns
    - Reorders columns
    """

    def __init__(self, logger: logging.Logger, schema_path: Path) -> None:
        self.logger = logger

        # Load schema from JSON
        try:
            with open(schema_path) as f:
                schema = json.load(f)
        except Exception as e:
            self.logger.critical(f"Failed to load {schema_path}: {e}")
            schema = {}
        
        # Required columns
        self.REQUIRED_COLUMNS = schema.get("required_columns", [])
        self.COLUMN_ALIASES:dict = schema.get("aliases", {})

        self.logger.info(
            f"Schema loaded. Required columns: {self.REQUIRED_COLUMNS},"
            f"Aliases: {list(self.COLUMN_ALIASES.keys())}"
        )
    
    def align(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a DataFrame with aligned schema.
        """
        self.logger.info("Starting schema alignment.")

        # Normalizing column names
        df.columns = [c.strip().lower() for c in df.columns]
        self.logger.info(f"Normalized columns: {df.columns.tolist()}")

        # Rename aliases
        df = df.rename(columns=self.COLUMN_ALIASES)
        self.logger.info(f"Rename columns: {df.columns.tolist()}")

        # Add missing required columns
        for col in self.REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None
                self.logger.info(f"Added missing column: {col}")
        
        # Reorder columns
        df = df[self.REQUIRED_COLUMNS]

        self.logger.info("Schema alignment completed.")
        return df

class DataCleaner:
    """
    Applies validation and cleaning rules to a DataFrame.

    This class enforces column-level rules defined in rules.yaml and 
    splits the input DataFrame into:
    - cleaned rows (valid)
    - rejected rows (invalid, with rejection reason)

    It does not perform file I/O or schema alignment.
    """
    def __init__(
            self,
            logger: logging.Logger,
            rules_path: Path
    ) -> None:
        self.logger = logger

        # Load rules.yaml
        try:
            with open(rules_path) as f:
                self.rules: dict = yaml.safe_load(f) or {}
        except Exception as e:
            self.logger.critical(f"Failed to load rules file {rules_path}: {e}")
            self.rules = {}
        
        self.logger.info(f"Rules loaded for columns: {list(self.rules.keys())}")
    
    def clean_and_validate(
            self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Applies validation rules from rules.yaml
        Returns (clean_df, rejected_df)
        """
        self.logger.info(
            f"Data validation started | Rows received: {len(df)}"
        )

        # Make a copy to avoid mutating original
        df = df.copy()
        # Column normalization (lowercase, strip)
        df.columns = (
            df.columns
            .str.lower()
            .str.strip()
        )

        self.logger.info(f"Normalized columns: {df.columns.tolist()}")

        # Add rejection reason column
        df["rejection_reason"] = ""

        # Apply rules column by column
        for column, rules in self.rules.items():
            rules: dict[str, object]

            if column not in df.columns:
                continue

            series = df[column]
            series: pd.Series

            # ---------- NULL CHECK ----------
            if not rules.get("allow_null", True):

                null_mask = series.isna() | (
                    series.astype(str).str.strip() == ""
                ) # this means -> When null_mask is empty

                df.loc[null_mask, "rejection_reason"] += f"{column}_null_not_allowed;"
                # null_mask there is the index

            # ---------- AMOUNT RULES ----------
            if column == "amount":

                df[column] = pd.to_numeric(series, errors="coerce")
                series = df[column]

                if "min" in rules:
                    min_mask = series < rules["min"]
                    df.loc[min_mask, "rejection_reason"] += f"{column}_below_min;"
                
                if "max" in rules:
                    max_mask = series > rules["max"]
                    df.loc[max_mask, "rejection_reason"] += f"{column}_above_max;"
            
            # ---------- DATE RULES ----------
            if column == "date":
                df[column] = pd.to_datetime(series, errors="coerce")
                series = df[column]

                if not rules.get("allow_future", True):
                    future_mask = series > pd.Timestamp.today()
                    df.loc[future_mask, "rejection_reason"] += f"{column}_future_not_allowed;"
            
            # ---------- ALLOWED VALUES ----------
            if column == "status":
                series = series.astype(str).str.lower().str.strip()
                allowed = set(rules["allowed_values"])

                invalid_mask = ~series.isin(allowed)
                df.loc[invalid_mask, "rejection_reason"] += f"{column}_invalid_value;"
        
        # Split clean vs rejected
        rejected_df = df[df["rejection_reason"] != ""].copy()
        cleaned_df = df[df["rejection_reason"] == ""].copy()

        # Drop helper column from clean data
        cleaned_df.drop(columns=["rejection_reason"], inplace=True)

        self.logger.info(
            f"Validation finished | Clean: {len(cleaned_df)} | Rejected: {len(rejected_df)}"
        )

        return cleaned_df, rejected_df

class FileProcessor:
    """
    Orchestrates end-to-end processing of raw input files.

    For each file, this class:
    - loads the file into a DataFrame
    - aligns columns using SchemaAligner
    - cleans and validates data using DataCleaner

    It aggregates cleaned and rejected rows across all files.
    """
    def __init__(
            self,
            logger: logging.Logger,
            input_dir: Path,
            schema_aligner: SchemaAligner,
            data_cleaner: DataCleaner,
    ) -> None:
        
        self.logger = logger
        self.input_dir = input_dir
        self.schema_aligner = schema_aligner
        self.data_cleaner = data_cleaner
    
    def _discover_files(self) -> list[Path]:
        """
        Find supported Excel files in input directory.
        """
        if not self.input_dir.exists():
            self.logger.info(f"Input directory not found: {self.input_dir}")
            return []
        
        files = [
            f for f in self.input_dir.iterdir()
            if f.is_file() and f.suffix.lower() in {".xlsx", ".xls"}
        ]

        if not files:
            self.logger.warning("No Excel files found in input directory.")
        
        self.logger.info(f"Discovered {len(files)} file(s) to process.")
        return files
    
    def process_files(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process all Excel files:
        - load
        - align schema 
        - clean & validate

        Returns:
            (cleaned_df, rejected_df)
        """
        cleaned_frames: list[pd.DataFrame] = []
        rejected_frames: list[pd.DataFrame] = []

        files = self._discover_files()

        for file_path in files:
            self.logger.info(f"Processing file: {file_path.name}")

            try:
                df = pd.read_excel(file_path)

            except Exception as e:
                self.logger.error(f"Failed to read {file_path.name}: {e}")
                continue

            if df.empty:
                self.logger.warning(f"file is empty {file_path.name}")
                continue

            # Traceability
            df["source_file"] = file_path.name
            df["processed_at"] = datetime.now(timezone.utc)

            # Schema alignment
            df = self.schema_aligner.align(df)

            # Cleaning & validation
            cleaned_df, rejected_df = self.data_cleaner.clean_and_validate(df)

            if not cleaned_df.empty:
                cleaned_frames.append(cleaned_df)
            
            if not rejected_df.empty:
                rejected_frames.append(rejected_df)
            
            self.logger.info(
                f"{file_path.name} | "
                f"cleaned: {len(cleaned_df)} | "
                f"rejected: {len(rejected_df)} | "
            )
        
        final_cleaned = (
            pd.concat(cleaned_frames, ignore_index=True)
            if cleaned_frames
            else pd.DataFrame()
        )

        final_rejected = (
            pd.concat(rejected_frames, ignore_index=True)
            if rejected_frames
            else pd.DataFrame() # This avoids : ValueError: No objects to concatenate
        )

        return final_cleaned, final_rejected

class OutputWriter:
    """
    Handles writing cleaned and rejected DataFrames to Excel files.

    - Writes cleaned data to a timestamped Excel file in the cleaned_dir.
    - Writes rejected data to a timestamped Excel file in the rejected_dir.
    - Returns paths of written files , or None if the corresponding DataFrame is empty.
    """

    def __init__(
            self,
            logger: logging.Logger,
            cleaned_dir: Path,
            rejected_dir: Path
    ) -> None:
        self.logger = logger
        self.cleaned_dir = cleaned_dir
        self.rejected_dir = rejected_dir

    def write_outputs(
            self,
            cleaned_df: pd.DataFrame,
            rejected_df: pd.DataFrame
    ) -> tuple[Path | None, Path | None]:
        """
        Write cleaned and rejected DataFrames to Excel.

        Returns:
            Tuple containing paths of the written cleaned and rejected files.
            Returns None if a DataFrame is empty.
        """

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        cleaned_path: Path | None = None
        rejected_path: Path | None = None

        # Write cleaned data
        if not cleaned_df.empty:

            cleaned_path = self.cleaned_dir / f"cleaned_{timestamp}.xlsx"
            cleaned_df.to_excel(cleaned_path, index=False, engine="openpyxl")
            self.logger.info(f"Cleaned file written: {cleaned_path}")
        
        else:
            self.logger.warning("Cleaned DataFrame is empty - nothing written ")
        
        # Write rejected data
        if not rejected_df.empty:

            rejected_path = self.rejected_dir / f"rejected_{timestamp}.xlsx"
            rejected_df.to_excel(rejected_path, index=False, engine="openpyxl")
            self.logger.info(f"Rejected file written: {rejected_path}")
        
        else:
            self.logger.info(f"Rejected DataFrame is empty - nothing written")
        
        return cleaned_path, rejected_path

class ReportWriter:
    """
    Generates an Excel report summarizing cleaned and rejected data.

    The report includes:
    - Overall processing summary
    - Rejection counts by reason
    """

    def __init__(
            self,
            logger: logging.Logger,
            report_dir: Path
    ) -> None:
        self.logger = logger
        self.report_dir = report_dir

    def generate_report(
            self,
            cleaned_df: pd.DataFrame,
            rejected_df: pd.DataFrame
    ) -> Path | None:
        """
        Generate an Excel report with summary information.

        Returns:
            Path to the generated report file, or None if no data exists.
        """

        if cleaned_df.empty and rejected_df.empty:
            self.logger.warning("No data available for report generation")
            return None
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        report_path = self.report_dir / f"report_{timestamp}.xlsx"

        # ---- Summary sheet ----
        summary_df = pd.DataFrame(
            {
                "metric": [
                    "processed_at",
                    "total_rows",
                    "cleaned_rows",
                    "rejected_rows"
                ],
                "value": [
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    len(cleaned_df) + len(rejected_df),
                    len(cleaned_df),
                    len(rejected_df)
                ]
            }

        )

        # ---- Rejection by reason ----
        if not rejected_df.empty and "rejection_reason" in rejected_df.columns:

            rejection_by_reason = (
                rejected_df["rejection_reason"]
                .value_counts()
                .reset_index()
            )

            rejection_by_reason.columns = ["rejection_reason", "count"]
        
        else:
            rejection_by_reason = pd.DataFrame(
                columns=["rejection_reason", "count"]
            )
        
        # ---- write excel report ----
        with pd.ExcelWriter(
            report_path,
            engine="openpyxl",
            date_format="YYYY-MM-DD"
        ) as writer:
            
            summary_df.to_excel(
                writer,
                index=False,
                sheet_name="summary"
            )

            rejection_by_reason.to_excel(
                writer,
                index=False,
                sheet_name="rejection_by_reason",
            )
        
        self.logger.info(f"Report generated: {report_path}")
        return report_path 

class EmailSender:
    """
    sends pipeline output files via email using SMTP with SSL
    """

    def __init__(self, logger: logging.Logger) -> None:
        load_dotenv()

        self.logger = logger
        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = int(os.getenv("SMTP_PORT"))
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.sender_password = os.getenv("SENDER_PASSWORD")
        self.receiver_email = os.getenv("RECEIVER_EMAIL")

    def send(
            self,
            attachments: list[Path],
            subject: str,
            body: str
    ) -> None:
        """
        Send an email with file attachments.
        """

        msg = EmailMessage()
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg["Subject"] = subject
        msg.set_content(body)

        for path in attachments:
            if path is None or not path.exists():
                continue

            with open(path, "rb") as f:
                file_data = f.read()
            
            msg.add_attachment(
                file_data,
                maintype="application",
                subtype="octet-stream",
                filename=path.name
            )
        
        context = ssl.create_default_context()

        try:
            with smtplib.SMTP_SSL(
                self.smtp_host,
                self.smtp_port,
                context=context
            ) as server:
                
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            self.logger.info("Email sent successfully")
        
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")

def main() -> None:

    # ---- environment & logging ----
    env = AppEnvironment()
    env.setup()
    logger = env.logger
    logger.info("Pipeline started")

    # ---- initialize components ----
    schema_aligner = SchemaAligner(
        logger=logger,
        schema_path=env.schema_path
    )

    data_cleaner = DataCleaner(
        logger=logger,
        rules_path=env.rules_path
    )

    file_loader = FileLoader(
        logger=logger
    )

    file_processor = FileProcessor(
        logger=logger,
        schema_aligner=schema_aligner,
        data_cleaner=data_cleaner,
        input_dir=env.input_dir
    )

    output_writer = OutputWriter(
        logger=logger,
        cleaned_dir=env.cleaned_dir,
        rejected_dir=env.rejected_dir
    )

    report_writer = ReportWriter(
        logger=logger,
        report_dir=env.reports_dir
    )

    email_sender = EmailSender(logger)

    # ---- load files ----
    dfs = file_loader.load_files(list(env.input_dir.glob("*.xlsx")))
    if not dfs:
        logger.warning("No input files found -- pipeline stopped")
        return
    
    # ---- process files ----
    cleaned_df, rejected_df = file_processor.process_files()

    # ---- write outputs ----
    cleaned_path, rejected_path = output_writer.write_outputs(
        cleaned_df,
        rejected_df
    )

    # ---- generate report ----
    report_path = report_writer.generate_report(
        cleaned_df,
        rejected_df
    )

    # ---- send email ----
    subject = "Excel Cleaning Pipeline Results"
    body = (
        "Hello,\n\n"
        "The Excel cleaning pipeline has completed successfully.\n\n"
        "Attached files:\n"
        "- Cleaned dataset\n"
        "- Rejected records\n"
        "- Processing report\n\n"
        "Regards,"
    )

    email_sender.send(
        attachments=[cleaned_path, rejected_path, report_path],
        subject=subject,
        body=body
    )

    logger.info("Pipeline finished successfully")

if __name__ == '__main__':
    main()