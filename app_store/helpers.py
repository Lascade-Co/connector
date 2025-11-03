"""
Helper functions for App Store Connect API authentication and client setup.
"""
import time
import jwt
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional


def generate_jwt_token(
    key_id: str,
    issuer_id: str,
    private_key: str,
    expiration_minutes: int = 20
) -> str:
    """
    Generate a JWT token for App Store Connect API authentication.
    
    Args:
        key_id: Your private key ID from App Store Connect
        issuer_id: Your issuer ID from App Store Connect
        private_key: Your private key content (PEM format)
        expiration_minutes: Token expiration time (max 20 minutes)
    
    Returns:
        JWT token string
    """
    # Ensure expiration doesn't exceed 20 minutes (Apple's limit)
    expiration_minutes = min(expiration_minutes, 20)
    
    # Create token header
    headers = {
        "alg": "ES256",
        "kid": key_id,
        "typ": "JWT"
    }
    
    # Create token payload
    payload = {
        "iss": issuer_id,
        "iat": int(time.time()),
        "exp": int(time.time()) + (expiration_minutes * 60),
        "aud": "appstoreconnect-v1"
    }
    
    # Generate and return the token
    token = jwt.encode(
        payload=payload,
        key=private_key,
        algorithm="ES256",
        headers=headers
    )
    
    return token


class AppStoreClient:
    """
    Client for making authenticated requests to App Store Connect API.
    """
    
    BASE_URL = "https://api.appstoreconnect.apple.com"
    
    def __init__(self, key_id: str, issuer_id: str, private_key: str):
        """
        Initialize the App Store Connect API client.
        
        Args:
            key_id: Your private key ID from App Store Connect
            issuer_id: Your issuer ID from App Store Connect
            private_key: Your private key content (PEM format)
        """
        self.key_id = key_id
        self.issuer_id = issuer_id
        self.private_key = private_key
        self.token = None
        self.token_expiry = None
    
    def _ensure_valid_token(self):
        """Ensure we have a valid JWT token, refresh if needed."""
        now = datetime.now()
        
        # Generate new token if we don't have one or it's about to expire
        if not self.token or not self.token_expiry or now >= self.token_expiry:
            self.token = generate_jwt_token(
                self.key_id,
                self.issuer_id,
                self.private_key
            )
            # Set expiry to 15 minutes from now (5 min buffer before actual expiry)
            self.token_expiry = now + timedelta(minutes=15)
    
    def get_headers(self) -> Dict[str, str]:
        """
        Get headers for API requests with valid authentication.
        
        Returns:
            Dictionary of HTTP headers
        """
        self._ensure_valid_token()
        
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a GET request to the App Store Connect API.
        
        Args:
            endpoint: API endpoint (e.g., "/v1/apps")
            params: Optional query parameters
        
        Returns:
            JSON response as dictionary
        """
        url = f"{self.BASE_URL}{endpoint}"
        headers = self.get_headers()
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def post(self, endpoint: str, payload: Dict) -> Dict:
        """
        Make a POST request to the App Store Connect API.
        
        Args:
            endpoint: API endpoint (e.g., "/v1/analyticsReportRequests")
            payload: JSON payload to send
        
        Returns:
            JSON response as dictionary
        """
        url = f"{self.BASE_URL}{endpoint}"
        headers = self.get_headers()
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def get_paginated(self, endpoint: str, params: Optional[Dict] = None, limit: int = 200):
        """
        Get all pages of results from a paginated endpoint.
        
        Args:
            endpoint: API endpoint
            params: Optional query parameters
            limit: Number of results per page (max 200)
        
        Yields:
            Individual items from all pages
        """
        if params is None:
            params = {}
        
        params["limit"] = min(limit, 200)
        
        url = endpoint
        
        while url:
            # If url is a full URL (from next link), use it directly
            if url.startswith("http"):
                response = requests.get(url, headers=self.get_headers())
            else:
                response = self.get(url, params)
                data = response
                url = None  # Will be updated if there's a next page
            
            if isinstance(response, requests.Response):
                response.raise_for_status()
                data = response.json()
            
            # Yield all items from current page
            if "data" in data:
                for item in data["data"]:
                    yield item
            
            # Check for next page
            if "links" in data and "next" in data["links"]:
                url = data["links"]["next"]
            else:
                break


def get_app_store_client(key_id: str, issuer_id: str, private_key: str) -> AppStoreClient:
    """
    Create and return an App Store Connect API client.
    
    Args:
        key_id: Your private key ID from App Store Connect
        issuer_id: Your issuer ID from App Store Connect
        private_key: Your private key content (PEM format)
    
    Returns:
        AppStoreClient instance
    """
    return AppStoreClient(key_id, issuer_id, private_key)
