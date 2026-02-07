# Excel Data Cleaning Pipeline

An automated data cleaning and validation pipeline for Excel files. This tool processes raw Excel files, validates data against configurable rules, separates clean and rejected records, and generates comprehensive reports.

## Features

- **Schema Alignment**: Automatically normalizes column names and handles aliases
- **Data Validation**: Configurable validation rules (null checks, ranges, allowed values)
- **Error Tracking**: Detailed rejection reasons for invalid records
- **Report Generation**: Excel reports with summary statistics and rejection breakdowns
- **Email Notifications**: Automatically sends results via email
- **Comprehensive Logging**: Full audit trail of all operations

## Installation

1. Clone or download this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your email configuration:
```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_app_password
RECEIVER_EMAIL=recipient@example.com
```

## Project Structure

```
excel_cleaning/
├── main.py                 # Main pipeline code
├── config/
│   ├── schema.json        # Column schema and aliases
│   └── rules.yaml         # Validation rules
├── input/
│   └── raw_files/         # Place your Excel files here
├── output/
│   ├── cleaned/           # Cleaned data files
│   ├── rejected/          # Rejected records with reasons
│   ├── reports/           # Processing reports
│   └── logs/              # Application logs
└── requirements.txt       # Python dependencies
```

## Configuration

### Schema Configuration (`config/schema.json`)

Define required columns and column name aliases:
```json
{
  "required_columns": ["record_id", "customer", "amount", "date", "status"],
  "aliases": {
    "id": "record_id",
    "order_id": "record_id",
    "client": "customer"
  }
}
```

### Validation Rules (`config/rules.yaml`)

Define validation rules for each column:
```yaml
amount:
  allow_null: false
  min: 0
  max: 100000

date:
  allow_null: false
  allow_future: false

status:
  allow_null: false
  allowed_values:
    - pending
    - paid
    - cancelled
```

## Usage

1. Place your Excel files (`.xlsx`) in the `input/raw_files/` directory
2. Run the pipeline:
```bash
python main.py
```

3. Check the output directories:
   - `output/cleaned/` - Validated and cleaned data
   - `output/rejected/` - Invalid records with rejection reasons
   - `output/reports/` - Processing summary reports
   - `output/logs/` - Detailed processing logs

4. Results are automatically emailed to the configured recipient

## How It Works

1. **File Discovery**: Scans `input/raw_files/` for Excel files
2. **Schema Alignment**: Normalizes column names and maps aliases
3. **Data Validation**: Applies rules from `rules.yaml`
4. **Separation**: Splits data into clean and rejected datasets
5. **Output Generation**: Creates timestamped Excel files
6. **Reporting**: Generates summary reports with statistics
7. **Email Delivery**: Sends all output files via email

## Validation Rules

- **Null Checks**: Enforce required fields
- **Numeric Ranges**: Min/max validation for numeric columns
- **Date Validation**: Prevent future dates if configured
- **Allowed Values**: Whitelist validation for categorical data

## Requirements

- Python 3.8+
- pandas
- openpyxl
- PyYAML
- python-dotenv

See `requirements.txt` for complete list.

## License

This project is provided as-is for data cleaning and validation purposes.
