# Golden-Handcuffs Plan Monitor (Top Hat Plan) API Monitor

Python script to monitor United States Department of Labor Top Hat Plan Search API for new filings.

## What Are Top Hat Plans?

Top hat plans are "unfunded, nonqualified deferred compensation plans covering a 'select group of management or highly compensated employees.'"1

"Top hat pension plans are a type of nonqualified deferred compensation (NQDC) arrangement. Employers maintain NQDC plans for a number of reasons, including recruiting employees, incentivizing employees to remain employed for a specified period (sometimes referred to as a 'golden handcuffs' arrangement), and supplementing retirement benefits provided under tax-qualified retirement plans."1

## Install

1. Ensure Python 3.7+ installed, or install Python 3.7+
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Setup

### Address Reference File Setup (Optional but Recommended)

To include organization addresses in email notifications, provide a reference CSV file.

**Expected CSV format:**
```csv
SPONS_DFE_EIN_9DIGIT,SPONSOR_DFE_NAME,SPONS_DFE_MAIL_US_ADDRESS1,SPONS_DFE_MAIL_US_ADDRESS2,SPONS_DFE_MAIL_US_CITY,SPONS_DFE_MAIL_US_STATE,SPONS_DFE_MAIL_US_ZIP
123456789,Company Name,123 Main St,,New York,NY,10001
```

Place your reference file in the project directory as `reference.csv`.

### Email Configuration (Recommended)

For email notifications, first set up your email configuration:

1. Edit `email_config.json` with your SMTP settings:
```json
{
  "smtp_server": "smtp.gmail.com",
  "smtp_port": 587,
  "sender_email": "your-email@gmail.com",
  "sender_password": "your-app-password",
  "recipient_emails": ["recipient@example.com"]
}
```

How to set up a Google App Password: https://docs.contentstudio.io/article/1080-how-to-set-a-google-app-password


### Establish Baseline (if Not Already Established)

A baseline file, current to 02-13-2026, already exists in the project directory. In the event a new baseline file needs to be established, the monitor will create a baseline of all existing records:

```bash
python tophat_api_monitor.py \
  --email-config email_config.json \
  --reference-file reference.csv \
  --no-email
```

This will:
- Fetch ALL records from the API (~90,905 = ~15-20 minutes)
- Create `tophat_baseline.csv` with all record Ids in the project directory
- Save records to `tophat_data/fetched_records_TIMESTAMP.csv`
- `--no-email`: will sidestep generating an email digest listing 90k records)

### Daily Monitoring

On subsequent runs, the monitor fetches all records and compares to baseline:

```bash
python tophat_api_monitor.py \
  --email-config email_config.json \
  --reference-file reference.csv
```


### API changes

If the API structure changes, update the `fetch_page()` method parameters or the CSV fieldnames.

## API Endpoint

```
https://www.askebsa.dol.gov/tophatplansearch/Home/Search
```

**Parameters:**
- `form_type`: "tophat"
- `sort`: "DocId"
- `order`: "desc"
- `offset`: 0, 100, 200, ... (pagination)
- `limit`: 100 (max records per request)

### Acknowledgements
- Coded and debugged with the assistance of claude.ai
