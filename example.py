from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import os

# Asegúrate de que el archivo google-ads.yaml esté en la ubicación correcta
os.environ["GOOGLE_ADS_CONFIGURATION_FILE_PATH"] = "./google-ads.yaml"

def get_account_info(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT
            customer.id,
            customer.descriptive_name,
            customer.currency_code
        FROM customer
        LIMIT 1"""

    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                print(f"Customer ID: {row.customer.id}")
                print(f"Descriptive Name: {row.customer.descriptive_name}")
                print(f"Currency Code: {row.customer.currency_code}")
    except GoogleAdsException as ex:
        print(f"An error occurred: {ex}")

def main():
    client = GoogleAdsClient.load_from_storage()
    customer_id = "4099398044"  # Reemplaza con tu ID de cliente de prueba
    get_account_info(client, customer_id)

if __name__ == "__main__":
    main()