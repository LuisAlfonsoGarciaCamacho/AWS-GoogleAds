from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

import os

# Asegúrate de que el archivo google-ads.yaml esté en la ubicación correcta
os.environ["GOOGLE_ADS_CONFIGURATION_FILE_PATH"] = "./google-ads.yaml"

def create_test_campaign(client, customer_id):
    campaign_service = client.get_service("CampaignService")
    campaign_budget_service = client.get_service("CampaignBudgetService")
    
    # Crear un presupuesto de campaña
    campaign_budget_operation = client.get_type("CampaignBudgetOperation")
    campaign_budget = campaign_budget_operation.create
    campaign_budget.name = "Test Budget"
    campaign_budget.amount_micros = 500000  # $0.50
    campaign_budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD

    # Agregar el presupuesto
    try:
        campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id, operations=[campaign_budget_operation]
        )
    except GoogleAdsException as ex:
        print(f"Error al crear el presupuesto: {ex}")
        return

    # Crear una campaña
    campaign_operation = client.get_type("CampaignOperation")
    campaign = campaign_operation.create
    campaign.name = "Test Campaign"
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SEARCH
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.manual_cpc.enhanced_cpc_enabled = True
    campaign.campaign_budget = campaign_budget_response.results[0].resource_name

    # Agregar la campaña
    try:
        campaign_response = campaign_service.mutate_campaigns(
            customer_id=customer_id, operations=[campaign_operation]
        )
        print(f"Campaña creada con ID: {campaign_response.results[0].resource_name}")
    except GoogleAdsException as ex:
        print(f"Error al crear la campaña: {ex}")

def main():
    client = GoogleAdsClient.load_from_storage()
    customer_id = "4099398044"  # Reemplaza con tu ID de cliente de prueba
    create_test_campaign(client, customer_id)

if __name__ == "__main__":
    main()