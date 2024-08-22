from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import os

# Asegúrate de que el archivo google-ads.yaml esté en la ubicación correcta
os.environ["GOOGLE_ADS_CONFIGURATION_FILE_PATH"] = "./google-ads.yaml"

def get_google_ads_data(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT 
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date DURING LAST_30_DAYS
    """
    
    try:
        stream = ga_service.search_stream(customer_id=customer_id, query=query)
        
        campaigns = []
        for batch in stream:
            for row in batch.results:
                campaigns.append({
                    "id": row.campaign.id,
                    "name": row.campaign.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": row.metrics.conversions,
                    "cost": row.metrics.cost_micros / 1e6  # Convertir de micros a moneda
                })
        return campaigns
    
    except GoogleAdsException as ex:
        print(f"An error occurred: {ex}")
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        return None

def main():
    try:
        # Cargar las credenciales
        client = GoogleAdsClient.load_from_storage()
        
        # Reemplaza esto con tu ID de cliente real
        customer_id = "4099398044"  # Este es un ejemplo, usa tu ID real
        
        # Usar la función para obtener los datos
        campaigns = get_google_ads_data(client, customer_id)
        
        if campaigns:
            print("Campañas obtenidas:")
            for campaign in campaigns:
                print(campaign)
        else:
            print("No se pudieron obtener las campañas.")
    
    except Exception as e:
        print(f"Ocurrió un error al inicializar el cliente: {e}")

if __name__ == "__main__":
    main()