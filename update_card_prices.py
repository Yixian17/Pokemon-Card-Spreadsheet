import os
import time
import logging
import gspread
from google.oauth2.service_account import Credentials
import requests
from dotenv import load_dotenv

load_dotenv()

# Constants
GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
# SHEET_NAME = os.getenv('SHEET_NAME')
POKEMON_TCG_API_KEY = os.getenv('POKEMON_TCG_API_KEY')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets setup
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDENTIALS, scopes=SCOPES)
client = gspread.authorize(creds)
# sheet = client.open(SHEET_NAME).sheet1
sheet = client.open("POKEMON")
collection_sheet = sheet.worksheet("Collection")

updates = []

def fetch_card_price(unique_identifier, row, shiny):
    base_url = f"https://api.pokemontcg.io/v2/cards/{unique_identifier}"
    # headers = {'X-Api-Key': POKEMON_TCG_API_KEY}
    headers = {'X-Api-Key': POKEMON_TCG_API_KEY}

    response = requests.get(base_url, headers=headers)
    if response.status_code == 200:
        data = response.json().get('data', {})
        if data:
            if shiny == 'Normal':
                return updates.append((row, 10, data.get('tcgplayer', {}).get('prices', {}).get('normal', {}).get('market', ''))),
            elif shiny == 'Reverse Holofoil':
                return updates.append((row, 10, data.get('tcgplayer', {}).get('prices', {}).get('reverseHolofoil', {}).get('market', ''))),
            elif shiny == 'Holofoil':
                return updates.append((row, 10, data.get('tcgplayer', {}).get('prices', {}).get('holofoil', {}).get('market', '')))
    return None

# def update_price_in_sheet(row, price):
#     sheet.update_cell(row, 10, price)



def main():
    records = collection_sheet.get_all_records(expected_headers=['Name', 'Card Number', 'Quantity', 'Unique Identifier', 'Type',  'Rarity','Shiny','Set','Status','Price (USD)','SGD'])
    for i, record in enumerate(records, start=2):
        row = i
        unique_identifier = record['Unique Identifier']
        card_shiny = record['Shiny']
        # print(card_shiny)

        if unique_identifier:
            try:
                # logger.info(f'Processing row {row}: {record}')
                price = fetch_card_price(unique_identifier, row, card_shiny)
                if price is not None:
                    # update_price_in_sheet(row, price)
                    # updates.extend(price)
                    # print(updates)
                    # logger.info(f'Updated price for card with ID {unique_identifier}')
                    logger.info(f'Added price for card with ID {unique_identifier} into updates')
                else:
                    logger.warning(f'No price found for card with ID {unique_identifier}')
            except Exception as e:
                logger.error(f'Error processing row {row}: {e}')
            time.sleep(5)  # Avoid hitting the API rate limit
    
    batch_updates = [
    {"range": f"{gspread.utils.rowcol_to_a1(row, col)}", "values": [[value]]} for row, col, value in updates
    ]
    # Perform batch update in one API call
    collection_sheet.batch_update(batch_updates)

    logger.info("Batch update completed successfully!")
    print("Card Price Details updated successfully.")

if __name__ == '__main__':
    main()
