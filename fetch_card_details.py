import gspread
import os
from google.oauth2.service_account import Credentials
import requests
from time import sleep
import logging
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
SHEET_NAME = os.getenv('SHEET_NAME')
POKEMON_TCG_API_KEY = os.getenv('POKEMON_TCG_API_KEY')

# Authenticate and connect to Google Sheets
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    # creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    creds = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDENTIALS, scopes=scope)
    client = gspread.authorize(creds)
except Exception as e:
    logger.error(f"Error during Google Sheets authentication: {e}")
    raise

# Open the Google Sheet
try:
    sheet = client.open("POKEMON")
    collection_sheet = sheet.worksheet("Collection")
    try:
        search_results_sheet = sheet.worksheet("Search Results")
        search_results_sheet.append_row([
        "Card Name", "Set Name", "Trainer/Pokemon", "Card Number", "Rarity",
        "Normal", "Reverse Holofoil", "Holofoil", "Unique Identifier", "TCGPlayer URL"
    ])
    except gspread.exceptions.WorksheetNotFound:
        search_results_sheet = sheet.add_worksheet(title="Search Results", rows="100", cols="12")
except Exception as e:
    logger.error(f"Error opening Google Sheets: {e}")
    raise


# Function to fetch card details from the Pokémon TCG API
def fetch_card_details(query, retries=3):
    headers = {
        "X-APi-Key": POKEMON_TCG_API_KEY
    }
    # print('POKEMON KEY', POKEMON_TCG_API_KEY)
    api_url = f"https://api.pokemontcg.io/v2/cards?q={query}"

    for attempt in range(retries):
        try:
            logger.info(f"Fetching card details (Attempt {attempt+1}): {query}")
            response = requests.get(api_url, headers=headers)
            if response.status_code == 429:
                logger.warning("Rate limit hit. Retrying after 5 seconds...")
                sleep(5)
                continue
            response.raise_for_status()
            data = response.json()
            # print("data", data)
            if 'data' in data and data['data']:
                logger.info(f"Received {len(data['data'])} results for query {query}")
                return data['data']
            else:
                logger.warning(f"No data in API response for query: {query}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for query {query}: {e}")
            sleep(2)  # Short delay before retry

    logger.error(f"Failed to fetch data after {retries} retries for query: {query}")
    return None

# Read data from the collection sheet
try:
    # collection_data = collection_sheet.get_all_records(empty2zero=False, head=1)
    collection_data = collection_sheet.get_all_records(expected_headers=['Name', 'Card Number', 'Quantity', 'Unique Identifier', 'Type',  'Rarity','Shiny','Set','Status','Price (USD)','SGD'])
    logger.info(f"Headers: {collection_data[0].keys()}")
except Exception as e:
    logger.error(f"Error reading collection sheet: {e}")
    raise

# Helper function to safely strip strings
def safe_strip(value):
    return str(value).strip() if isinstance(value, str) else value

updates_to_write = []  # List to store cell updates

# Function to process search results
def process_search_results(card_name, cards, row_number, card_shiny):
    # Add matching cards to the temporary search results sheet
    for card_details in cards:
        new_row = [
            card_details.get('name', ''),
            card_details.get('set', {}).get('name', ''),
            card_details.get('supertype', ''),
            card_details.get('number', ''),
            card_details.get('rarity', ''),
            card_details.get('tcgplayer', {}).get('prices', {}).get('normal', {}).get('market', ''),
            card_details.get('tcgplayer', {}).get('prices', {}).get('reverseHolofoil', {}).get('market', ''),
            card_details.get('tcgplayer', {}).get('prices', {}).get('holofoil', {}).get('market', ''),
            card_details.get('id', ''),  # Using the unique identifier
            card_details.get('tcgplayer', {}).get('url', '')
        ]
        # print(new_row)
        search_results_sheet.append_row(new_row)

    print(f"Search results for '{card_name}' are available in the 'Search Results' sheet.")
    updates = [
        (row_number, 9, 'Fetched'),  # Mark row as fetched
        (row_number, 4, new_row[8]),  # Store the card's unique ID
        (row_number, 5, new_row[2])  # Store the pokemon card number
    ]

    # Store the correct price based on whether the card shiny or not
    if card_shiny == 'Normal':
        updates.append((row_number, 10, new_row[5]))
    elif card_shiny == 'Reverse Holofoil':
        updates.append((row_number, 10, new_row[6]))
    elif card_shiny == 'Holofoil':
        updates.append((row_number, 10, new_row[7]))

    return updates

# Process search status
for row_number, row in enumerate(collection_data, start=2):  # start=2 to account for header row
    try:
        status = safe_strip(row['Status']).lower()
        if status == 'search':
            card_name = safe_strip(row['Name'])
            card_set = row['Set']
            card_number = row['Card Number']
            card_rarity = row['Rarity']
            card_shiny = row['Shiny']
            if not card_name:
                logger.warning(f"Card name is empty for row {row_number}")
                continue
            # query = f'name:"{card_name}" set.name:"{card_set}" rarity:"{card_rarity}"'
            query = f'name:"{card_name}" set.name:"{card_set}" number:"{card_number}"'
            cards = fetch_card_details(query)
            if cards:
                updates = process_search_results(card_name, cards, row_number, card_shiny)
                updates_to_write.extend(updates)
            else:
                logger.warning(f"No cards found for {card_name}")
    except Exception as e:
        logger.error(f"Error processing row {row_number}: {e}")


batch_updates = [
    {"range": f"{gspread.utils.rowcol_to_a1(row, col)}", "values": [[value]]} for row, col, value in updates_to_write
]   
# Perform batch update in one API call
collection_sheet.batch_update(batch_updates)

logger.info("Batch update completed successfully!")
print("Card details updated successfully.")
