import os
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta


class FuelPriceAPI:
    """Client for Daily Fuel Prices API"""
    
    BASE_URL = "https://daily-petrol-diesel-lpg-cng-fuel-prices-in-india.p.rapidapi.com/v1/fuel-prices"
    
    def __init__(self, api_key: Optional[str] = None, api_host: Optional[str] = None):
        self.api_key = api_key or os.getenv("RAPIDAPI_KEY")
        self.api_host = api_host or os.getenv(
            "RAPIDAPI_HOST", 
            "daily-petrol-diesel-lpg-cng-fuel-prices-in-india.p.rapidapi.com"
        )
        
        if not self.api_key:
            raise ValueError("RAPIDAPI_KEY is required")
        
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.api_host
        }
    
    def _request(self, endpoint: str) -> Dict[str, Any]:
        """Make API request"""
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def get_city_history(self, state_id: str, city_id: str) -> Dict[str, Any]:
        """
        Get 30-day price history for a city
        
        Args:
            state_id: State identifier (e.g., 'maharashtra')
            city_id: City identifier (e.g., 'mumbai')
        
        Returns:
            API response with history data
        """
        endpoint = f"history/india/{state_id}/{city_id}"
        return self._request(endpoint)
    
    def get_today_city(self, state_id: str, city_id: str) -> Dict[str, Any]:
        """Get today's prices for a city"""
        endpoint = f"today/india/{state_id}/{city_id}"
        return self._request(endpoint)
    
    def get_today_state(self, state_id: str) -> Dict[str, Any]:
        """Get today's prices for all cities in a state"""
        endpoint = f"today/india/{state_id}"
        return self._request(endpoint)
    
    def get_all_states(self) -> Dict[str, Any]:
        """Get today's prices for all states"""
        endpoint = "today/india"
        return self._request(endpoint)


# Default cities to track
DEFAULT_CITIES = [
    {"state_id": "maharashtra", "city_id": "mumbai"},
    {"state_id": "delhi", "city_id": "delhi"},
    {"state_id": "karnataka", "city_id": "bengaluru"},
    {"state_id": "tamil-nadu", "city_id": "chennai"},
    {"state_id": "west-bengal", "city_id": "kolkata"},
    {"state_id": "telangana", "city_id": "hyderabad"},
]


def get_fuel_data_for_cities(api_key: str, cities: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Fetch fuel data for multiple cities
    
    Args:
        api_key: RapidAPI key
        cities: List of dicts with state_id and city_id
    
    Returns:
        List of API responses
    """
    client = FuelPriceAPI(api_key=api_key)
    cities = cities or DEFAULT_CITIES
    
    results = []
    for city in cities:
        try:
            data = client.get_city_history(city["state_id"], city["city_id"])
            results.append(data)
        except Exception as e:
            print(f"Error fetching {city['city_id']}: {e}")
            continue
    
    return results
