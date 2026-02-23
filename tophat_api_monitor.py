#!/usr/bin/env python3
"""

python tophat_api_monitor.py [--full-scan]

"""

import argparse
import csv
import json
import logging
import os
import smtplib
import sys
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urlencode

import requests


# Configuration
BASE_URL = "https://www.askebsa.dol.gov/tophatplansearch/Home/Search"
RECORDS_PER_PAGE = 100
REQUEST_DELAY = 1.0  # Seconds between requests
PROPUBLICA_DELAY = 0.5 
STATE_FILE = "tophat_monitor_state.json"
BASELINE_FILE = "tophat_baseline.csv"
OUTPUT_DIR = "tophat_data"
LOG_FILE = "tophat_monitor.log"
AUTO_CLEANUP_KEEP = 2

# Set up logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TopHatAPIMonitor:
    
    def __init__(self, state_file: str = STATE_FILE, output_dir: str = OUTPUT_DIR, 
                 baseline_file: str = BASELINE_FILE, email_config: Optional[Dict] = None,
                 reference_file: Optional[str] = None, keep_files: int = AUTO_CLEANUP_KEEP):
        self.state_file = Path(state_file)
        self.output_dir = Path(output_dir)
        self.baseline_file = Path(baseline_file)
        self.output_dir.mkdir(exist_ok=True)
        self.email_config = email_config or {}
        self.reference_file = Path(reference_file) if reference_file else None
        self.keep_files = keep_files
        


        self.ein_to_address = {}
        if self.reference_file and self.reference_file.exists():
            self._load_reference_data()
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TopHat-Monitor/1.0'
        })
        
    def _load_reference_data(self):




        try:
            logger.info(f"Loading reference data from {self.reference_file}")
            with open(self.reference_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:



                    ein = row.get('SPONS_DFE_EIN_9DIGIT') or row.get('SPONS_DFE_EIN_FLOAT')
                    
                    if ein:



                        ein_clean = str(ein).strip().replace('-', '')
                        


                        address_info = {
                            'name': row.get('SPONSOR_DFE_NAME (SPONS_DFE_DBA_NAME)', '').strip(),
                            'address1': row.get('SPONS_DFE_MAIL_US_ADDRESS1', '').strip(),
                            'address2': row.get('SPONS_DFE_MAIL_US_ADDRESS2', '').strip(),
                            'city': row.get('SPONS_DFE_MAIL_US_CITY', '').strip(),
                            'state': row.get('SPONS_DFE_MAIL_US_STATE', '').strip(),
                            'zip': row.get('SPONS_DFE_MAIL_US_ZIP', '').strip()
                        }
                        


                        if any([address_info['address1'], address_info['city'], 
                               address_info['state'], address_info['zip']]):
                            self.ein_to_address[ein_clean] = address_info
            
            logger.info(f"Loaded {len(self.ein_to_address)} EIN-to-address mappings")
            
        except Exception as e:
            logger.error(f"Error loading reference data: {e}")
            self.ein_to_address = {}




    def get_address_for_ein(self, ein: str) -> Optional[Dict]:

        if not ein:
            return None
        
        ein_clean = str(ein).strip().replace('-', '')
        
        return self.ein_to_address.get(ein_clean)
    
    def check_propublica_nonprofit(self, ein: str) -> Optional[str]:


        """
        ProPublica: If EIN, populate with Explorer referral URL 
        
        """

        if not ein:
            return None
        
        ein_clean = str(ein).strip().replace('-', '')
        
        api_url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{ein_clean}.json"




        try:
            logger.debug(f"Checking ProPublica API for EIN {ein_clean}")
            response = self.session.get(api_url, timeout=10)


            if response.status_code == 200:
                data = response.json()


                if data.get('organization'):

                    propublica_url = f"https://projects.propublica.org/nonprofits/organizations/{ein_clean}"
                    logger.debug(f"Found nonprofit data for EIN {ein_clean}")
                    time.sleep(PROPUBLICA_DELAY)
                    return propublica_url




            # 404 means nonprofit not found
            elif response.status_code == 404:
                logger.debug(f"No nonprofit data for EIN {ein_clean}")
                time.sleep(PROPUBLICA_DELAY)
                return None
            
            else:
                logger.warning(f"ProPublica API returned status {response.status_code} for EIN {ein_clean}")
                return None



        except requests.exceptions.Timeout:
            logger.warning(f"ProPublica API timeout for EIN {ein_clean}")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"ProPublica API error for EIN {ein_clean}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error checking ProPublica for EIN {ein_clean}: {e}")
            return None




    def load_state(self) -> Dict:
        """Load last-recorded state"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    logger.info(f"Loaded state:"
                              f""
                              f"last_run={state.get('last_run')}")
                    return state
            except Exception as e:
                logger.warning(f"Couldn't load state file: {e}")
                return {}
        return {}
    



    def save_state(self, state: Dict):
        """Save current state"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.info(f"State saved:")
        except Exception as e:
            logger.error(f"Error saving state: {e}")






    def load_baseline(self) -> Set[str]:
        baseline_ids = set()
        if self.baseline_file.exists():
            try:
                with open(self.baseline_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:


                        if row.get('Id'):
                            baseline_ids.add(row['Id'])
                logger.info(f"Loaded {len(baseline_ids)} baseline Ids")
            except Exception as e:
                logger.warning(f"Error loading baseline file: {e}")
        else:
            logger.info("No baseline file - all records processed as new")
        return baseline_ids







    def save_baseline(self, records: List[Dict]):

        """Save the current records as the new baseline"""

        if not records:
            return
        
        try:

            sorted_records = sorted(records, key=lambda x: str(x.get('Id', '')))
            
            with open(self.baseline_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['Id', 'DocId', 'Employer', 'Ein', 'PlanName', 'DateReceived', 'Efile']
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(sorted_records)
            
            logger.info(f"Baseline saved with {len(records)} records")
        except Exception as e:
            logger.error(f"Error saving baseline: {e}")



    
    def identify_new_records(self, current_records: List[Dict], baseline_ids: Set[str]) -> List[Dict]:

        new_records = []
        for record in current_records:
            record_id = str(record.get('Id', ''))
            if record_id and record_id not in baseline_ids:
                new_records.append(record)
        
        logger.info(f"Identified {len(new_records)} new records")
        return new_records
    



    def cleanup_old_files(self):

        if self.keep_files <= 0:
            logger.debug("Auto-cleanup disabled (keep_files <= 0)")
            return
        
        logger.info(f"Running auto-cleanup: keeping {self.keep_files} most recent file sets")
        
        patterns = [
            'fetched_records_*.csv',
            'fetched_records_*.json',
            'new_records_*.csv',
            'new_records_*.json',
        ]
        
        total_deleted = 0
        
        for pattern in patterns:
            try:

                files_with_timestamps = []
                for filepath in self.output_dir.glob(pattern):
                    try:

                        filename = filepath.stem
                        parts = filename.split('_')
                        if len(parts) >= 3:
                            date_part = parts[-2]
                            time_part = parts[-1]
                            timestamp_str = f"{date_part}_{time_part}"
                            timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                            files_with_timestamps.append((filepath, timestamp))
                    except (ValueError, IndexError):
                        continue
                

                files_with_timestamps.sort(key=lambda x: x[1], reverse=True)
              


                

                files_to_delete = files_with_timestamps[self.keep_files:]
                for filepath, timestamp in files_to_delete:
                    try:
                        filepath.unlink()
                        logger.debug(f"Deleted old file: {filepath.name}")
                        total_deleted += 1
                    except Exception as e:
                        logger.warning(f"Could not delete {filepath.name}: {e}")
                
            except Exception as e:
                logger.warning(f"Error during cleanup of {pattern}: {e}")
        
        if total_deleted > 0:
            logger.info(f"Cleanup complete: deleted {total_deleted} old files")
        else:
            logger.info("Cleanup done: no old files to delete")




    def generate_pdf_link(self, record_id: str) -> str:
        multizero_id = str(record_id).zfill(13)
        return f"https://www.askebsa.dol.gov/tophatplansearch/Home/DownloadPdf?id={multizero_id}&form_type=Top%20Hat"
    
    def create_email_html(self, new_records: List[Dict]) -> str:


        sorted_records = sorted(new_records, key=lambda x: int(x.get('DocId', 0) or 0), reverse=True)




        html = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                }}
                .header {{
                    background-color: #2c3e50;
                    color: white;
                    padding: 20px;
                    text-align: center;
                }}
                .summary {{
                    background-color: #ecf0f1;
                    padding: 15px;
                    margin: 20px 0;
                    border-left: 4px solid #3498db;
                }}
                .record {{
                    border: 1px solid #ddd;
                    padding: 15px;
                    margin: 15px 0;
                    background-color: #fff;
                    border-radius: 5px;
                }}
                .record:hover {{
                    background-color: #f8f9fa;
                }}
                .field {{
                    margin: 5px 0;
                }}
                .label {{
                    font-weight: bold;
                    color: #2c3e50;
                    display: inline-block;
                    width: 150px;
                }}
                .value {{
                    color: #555;
                }}
                .address-section {{
                    background-color: #f0f8ff;
                    padding: 10px;
                    margin: 10px 0;
                    border-left: 3px solid #3498db;
                    border-radius: 3px;
                }}
                .address-label {{
                    font-weight: bold;
                    color: #2980b9;
                    margin-bottom: 5px;
                }}
                .address-text {{
                    color: #555;
                    font-size: 14px;
                    line-height: 1.4;
                }}
                .no-address {{
                    color: #95a5a6;
                    font-style: italic;
                    font-size: 14px;
                }}
                .nonprofit-link {{
                    background-color: #e8f5e9;
                    padding: 10px;
                    margin: 10px 0;
                    border-left: 3px solid #4caf50;
                    border-radius: 3px;
                }}
                .nonprofit-link a {{
                    color: #2e7d32;
                    text-decoration: none;
                    font-weight: bold;
                }}
                .nonprofit-link a:hover {{
                    color: #1b5e20;
                    text-decoration: underline;
                }}
                .nonprofit-icon {{
                    margin-right: 5px;
                }}
                .pdf-link {{
                    display: inline-block;
                    margin-top: 10px;
                    padding: 8px 15px;
                    background-color: #3498db;
                    color: #ffffff;
                    text-decoration: none;
                    border-radius: 3px;
                }}
                .footer {{
                    margin-top: 30px;
                    padding: 15px;
                    text-align: center;
                    color: #7f8c8d;
                    font-size: 12px;
                }}
                .doc-id {{
                    color: #e74c3c;
                    font-weight: bold;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>TopHat Filing Monitor</h1>
                <p>New Filings Digest</p>
            </div>
            
            <div class="summary">
                <h2>Summary</h2>
                <p><strong>{len(new_records)}</strong> new TopHat filings detected</p>
                <p><strong>Date:</strong> {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
            </div>
            
            <h2>New Filings</h2>
        """
        
        for record in sorted_records:
            doc_id = record.get('DocId', 'N/A')
            record_id = record.get('Id', doc_id)
            employer = record.get('Employer', 'N/A')
            ein = record.get('Ein', 'N/A')
            plan_name = record.get('PlanName', 'N/A') or 'Not specified'
            date_received = record.get('DateReceived', 'N/A')
            
            # Format date
            if date_received and 'T' in date_received:
                try:
                    dt = datetime.fromisoformat(date_received.replace('Z', '+00:00'))
                    date_received = dt.strftime('%B %d, %Y at %I:%M %p')
                except:
                    pass
            
            pdf_link = self.generate_pdf_link(record_id)
            



            # Get address information, via reference.csv
            address_info = self.get_address_for_ein(ein)
            
            # Check ProPublica Nonprofit Explorer
            propublica_url = self.check_propublica_nonprofit(ein)



            # Build address HTML section
            if address_info:
                address_html = '<div class="address-section">'
                address_html += '<div class="address-label">Org Address:</div>'
                address_html += '<div class="address-text">'
                
                # Build address lines
                address_parts = []
                if address_info.get('address1'):
                    address_parts.append(address_info['address1'])
                if address_info.get('address2'):
                    address_parts.append(address_info['address2'])
                
                # City, State, ZIP line
                city_state_zip = []
                if address_info.get('city'):
                    city_state_zip.append(address_info['city'])
                if address_info.get('state'):
                    state_part = address_info['state']
                    if address_info.get('zip'):
                        state_part += ' ' + address_info['zip']
                    city_state_zip.append(state_part)
                elif address_info.get('zip'):
                    city_state_zip.append(address_info['zip'])
                
                if city_state_zip:
                    address_parts.append(', '.join(city_state_zip))
                
                # Join all parts with line breaks
                if address_parts:
                    address_html += '<br>'.join(address_parts)
                else:
                    address_html += '<span class="no-address">Incomplete address</span>'
                
                address_html += '</div></div>'
            else:
                address_html = '<div class="no-address">No address information in reference file</div>'
            
            # Build ProPublica nonprofit link if available
            if propublica_url:
                nonprofit_html = f'''
                <div class="nonprofit-link">
                    <span class="nonprofit-icon"></span>
                    <a href="{propublica_url}" target="_blank">View Nonprofit Profile (ProPublica)</a>
                </div>
                '''
            else:
                nonprofit_html = ''
            
            html += f"""
            <div class="record">
                <div class="field">
                    <span class="label">DocId:</span>
                    <span class="value doc-id">{doc_id}</span>
                </div>
                <div class="field">
                    <span class="label">Employer:</span>
                    <span class="value">{employer}</span>
                </div>
                <div class="field">
                    <span class="label">EIN:</span>
                    <span class="value">{ein}</span>
                </div>
                {address_html}
                {nonprofit_html}
                <div class="field">
                    <span class="label">Plan Name:</span>
                    <span class="value">{plan_name}</span>
                </div>
                <div class="field">
                    <span class="label">Date Received:</span>
                    <span class="value">{date_received}</span>
                </div>
                <div class="field">
                    <span class="label">Record ID:</span>
                    <span class="value">{record_id}</span>
                </div>
                <div>
                    <a href="{pdf_link}" class="pdf-link">Download PDF</a>
                </div>
            </div>
            """
        
        html += """
            <div class="footer">
            </div>
        </body>
        </html>
        """
        
        return html
    
    def send_email(self, new_records: List[Dict]) -> bool:
        """Send email digest of new records"""
        if not self.email_config:
            logger.warning("Email configuration not provided, skipping email")
            return False
        
        if not new_records:
            logger.info("No new records to email")
            return True
        
        try:
            # Extract email configuration
            smtp_server = self.email_config.get('smtp_server')
            smtp_port = self.email_config.get('smtp_port', 587)
            sender_email = self.email_config.get('sender_email')
            sender_password = self.email_config.get('sender_password')
            recipient_emails = self.email_config.get('recipient_emails', [])
            
            if not all([smtp_server, sender_email, sender_password, recipient_emails]):
                logger.error("Incomplete email configuration")
                return False
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"TopHat Monitor: {len(new_records)} New Filing(s) Detected"
            msg['From'] = sender_email
            msg['To'] = ', '.join(recipient_emails)
            
            # Create HTML content
            html_content = self.create_email_html(new_records)
            
            # Create plain text version
            text_content = f"""
TopHat Filing Monitor - New Filings Digest

{len(new_records)} new TopHat filings detected on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}

New Filings:
{'='*60}
"""
            for record in sorted(new_records, key=lambda x: int(x.get('DocId', 0) or 0), reverse=True):
                ein = record.get('Ein', 'N/A')
                text_content += f"""
DocId: {record.get('DocId', 'N/A')}
Employer: {record.get('Employer', 'N/A')}
EIN: {ein}
"""
                # Add address if available
                address_info = self.get_address_for_ein(ein)
                if address_info:
                    text_content += "Address:\n"
                    if address_info.get('address1'):
                        text_content += f"  {address_info['address1']}\n"
                    if address_info.get('address2'):
                        text_content += f"  {address_info['address2']}\n"
                    
                    city_state_zip = []
                    if address_info.get('city'):
                        city_state_zip.append(address_info['city'])
                    if address_info.get('state'):
                        city_state_zip.append(address_info['state'])
                    if address_info.get('zip'):
                        city_state_zip.append(address_info['zip'])
                    
                    if city_state_zip:
                        text_content += f"  {', '.join(city_state_zip)}\n"
                else:
                    text_content += "Address: Not available in reference file\n"
                
                # Check ProPublica nonprofit data
                propublica_url = self.check_propublica_nonprofit(ein)
                if propublica_url:
                    text_content += f"Nonprofit Profile: {propublica_url}\n"
                
                text_content += f"""Plan Name: {record.get('PlanName', 'Not specified') or 'Not specified'}
Date Received: {record.get('DateReceived', 'N/A')}
PDF Link: {self.generate_pdf_link(record.get('Id', record.get('DocId', '')))}

{'-'*60}
"""
            
            # Attach parts
            part1 = MIMEText(text_content, 'plain')
            part2 = MIMEText(html_content, 'html')
            msg.attach(part1)
            msg.attach(part2)
            
            # Send email
            logger.info(f"Sending email to {len(recipient_emails)} recipient(s)")
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            
            logger.info("Email sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False
    
    def fetch_page(self, offset: int = 0) -> Optional[Dict]:



        params = {
            'form_type': 'tophat',
            'employer_name': '',
            'plan_name': '',
            'ein': '',
            'sort': 'DocId',
            'order': 'desc',
            'offset': offset,
            'limit': RECORDS_PER_PAGE
        }
        
        url = f"{BASE_URL}?{urlencode(params)}"
        
        try:
            logger.debug(f"Fetching offset {offset}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching offset {offset}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON at offset {offset}: {e}")
            return None
    
    def fetch_all_records(self, full_scan: bool = False) -> List[Dict]:
\





        all_records = []
        offset = 0
        total_records = None
        seen_ids: Set[str] = set()
        
        # Incremental runs, limit to 1000 records
        # For full scans, fetch everything
        max_records_to_fetch = None if full_scan else 1000
        
        if full_scan:
            logger.info(f"Starting to fetch all records")
        else:
            logger.info(f"Starting to fetch {max_records_to_fetch} recent records")
        
        while True:
 

            data = self.fetch_page(offset)
            
            if data is None:
                logger.error(f"Failed to fetch data fir offset {offset}")
                break
            

            if total_records is None:
                total_records = data.get('total', 0)
                logger.info(f"Total records in database: {total_records}")
            



            rows = data.get('rows', [])
            
            if not rows:
                logger.info(f"No more records at offset {offset}")
                break
            

            for row in rows:
                record_id = row.get('Id') 
                
                if record_id is None:
                    continue
                




                if record_id in seen_ids:
                    continue
                
                seen_ids.add(record_id)
                all_records.append(row)
            
            logger.info(f"Processed offset {offset}: found {len(rows)} records, "
                       f"{len(all_records)} total fetched")
            

            offset += RECORDS_PER_PAGE
            

            if max_records_to_fetch and len(all_records) >= max_records_to_fetch:
                logger.info(f"Reached fetch limit of {max_records_to_fetch} records")
                break
            

            if offset >= total_records:
                logger.info(f"Reached end of data at offset {offset}")
                break
            

            time.sleep(REQUEST_DELAY)
        
        logger.info(f"Fetch complete. Found {len(all_records)} records")
        return all_records
    
    def save_records_csv(self, records: List[Dict], filename: str):
        """Save to CSV"""
        if not records:
            logger.info("No records to save")
            return
        
        filepath = self.output_dir / filename
        



        fieldnames = [
            'DocId', 'Id', 'Employer', 'Ein', 'Pn', 'PlanName', 
            'FormType', 'DateReceived', 'PdfLink', 'PdfCreated',
            'TextFilePath', 'Efile'
        ]
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(records)
            
            logger.info(f"Saved {len(records)} records to {filepath}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
    
    def save_records_json(self, records: List[Dict], filename: str):




        if not records:
            return
        
        filepath = self.output_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(records, f, indent=2, default=str)
            
            logger.info(f"Saved {len(records)} records to {filepath}")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")
    
    def run(self, send_email_notification: bool = True):





        start_time = datetime.now()
        logger.info("="*60)
        logger.info(f"TopHat API Monitor started at {start_time}")
        logger.info(f"Mode: FULL SCAN (fetch all records, compare against baseline)")
        logger.info("="*60)
        



        baseline_ids = self.load_baseline()
        
        state = self.load_state()
        
        logger.info("Fetch all records from API")
        all_records = self.fetch_all_records(full_scan=True)
        
        if all_records:
            # Sort Id as descending (newest first)
            all_records.sort(key=lambda x: int(x.get('Id', 0) or 0), reverse=True)
            logger.info(f"Fetched {len(all_records)} records total:")
            new_records = self.identify_new_records(all_records, baseline_ids)
            


            timestamp = start_time.strftime('%Y%m%d_%H%M%S')
            self.save_records_csv(all_records, f"fetched_records_{timestamp}.csv")
            self.save_records_json(all_records, f"fetched_records_{timestamp}.json")
            


            # Save new records
            if new_records:
                self.save_records_csv(new_records, f"new_records_{timestamp}.csv")
                self.save_records_json(new_records, f"new_records_{timestamp}.json")
            


            # Append baseline with all current records
            all_current_records = list(all_records)
            self.save_baseline(all_current_records)
            

            new_state = {
                'last_run': start_time.isoformat(),
                'records_fetched': len(all_records),
                'new_records_found': len(new_records)
            }
            self.save_state(new_state)
    

            # Send email if there are new records
            if new_records and send_email_notification:
                logger.info(f"Preparing to send email")
                self.send_email(new_records)




            logger.info("="*60)
            logger.info(f"SUMMARY:")
            logger.info(f"  Records fetched: {len(all_records)}")
            logger.info(f"  New records (not in baseline): {len(new_records)}")
            if all_records:
                logger.info(f"  Date range: {all_records[-1].get('DateReceived', 'N/A')[:10]} to {all_records[0].get('DateReceived', 'N/A')[:10]}")
            logger.info("="*60)
            
        else:
            logger.info("No records fetched")
        





        # Auto-cleanup old files
        self.cleanup_old_files()
        
        elapsed = datetime.now() - start_time
        logger.info(f"Monitor completed in {elapsed.total_seconds():.2f} seconds")
        
        return all_records if all_records else []






def main():

    parser = argparse.ArgumentParser(
        description='Monitor the DOL TopHat Plan Search API for new filings'
    )
    parser.add_argument(
        '--state-file',
        default=STATE_FILE,
        help=f'Path to state file (default: {STATE_FILE})'
    )
    parser.add_argument(
        '--output-dir',
        default=OUTPUT_DIR,
        help=f'Directory for output files (default: {OUTPUT_DIR})'
    )
    parser.add_argument(
        '--baseline-file',
        default=BASELINE_FILE,
        help=f'Path to baseline file (default: {BASELINE_FILE})'
    )
    parser.add_argument(
        '--email-config',
        help='Path to email configuration JSON file'
    )



    parser.add_argument(
        '--reference-file',
        help='Path to reference CSV file with EIN-to-address mappings'
    )
    parser.add_argument(
        '--keep-files',
        type=int,
        default=AUTO_CLEANUP_KEEP,
        help=f'Number of recent file sets to keep (default: {AUTO_CLEANUP_KEEP}, 0=disable cleanup)'
    )
    parser.add_argument(
        '--no-email',
        action='store_true',
        help='Disable email'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()





    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Load email config
    email_config = None
    if args.email_config and not args.no_email:
        try:
            with open(args.email_config, 'r') as f:
                email_config = json.load(f)
            logger.info(f"Email configuration loaded from {args.email_config}")
        except Exception as e:
            logger.error(f"Error loading email configuration: {e}")
    




    monitor = TopHatAPIMonitor(
        state_file=args.state_file,
        output_dir=args.output_dir,
        baseline_file=args.baseline_file,
        email_config=email_config,
        reference_file=args.reference_file,
        keep_files=args.keep_files
    )
    





    # Override cleanup setting if specified
    if args.keep_files != AUTO_CLEANUP_KEEP:
        logger.info(f"Auto-cleanup: keeping {args.keep_files} file sets")
    
    try:
        monitor.run(send_email_notification=not args.no_email)
        return 0
    except KeyboardInterrupt:
        logger.info("User interruption")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1







if __name__ == '__main__':
    sys.exit(main())
