import requests
import csv
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GRAPHQL_ENDPOINT = "https://graphql-prod-4776.prod.aws.worldathletics.org/graphql"
API_KEY = os.getenv("WORLD_ATHLETICS_API_KEY")

HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": API_KEY,
    "Origin": "https://worldathletics.org",
    "Referer": "https://worldathletics.org/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"
}

def run_graphql_query(query, variables=None):
    payload = {
        "query": query,
        "variables": variables or {}
    }
    response = requests.post(GRAPHQL_ENDPOINT, json=payload, headers=HEADERS)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    query = """
    query GetChampionshipQualifications($competitionId: Int!, $eventId: Int, $country: String, $qualificationType: String) {
      getChampionshipQualifications(competitionId: $competitionId, eventId: $eventId, country: $country, qualificationType: $qualificationType) {
        events {
          genderCode
          eventId
          disciplineName
          __typename
        }
        __typename
      }
    }
    """

    variables = {
        "competitionId": 7190593
    }

    print("Fetching events for competition...")
    result = run_graphql_query(query, variables)
    
    event_info = result.get("data", {}).get("getChampionshipQualifications", {})
    events = event_info.get("events", [])
    
    print(f"Found {len(events)} events")
    
    # Export events to CSV
    if events:
        with open("events.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["eventId", "disciplineName", "genderCode"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for event in events:
                writer.writerow({
                    "eventId": event.get("eventId"),
                    "disciplineName": event.get("disciplineName"),
                    "genderCode": event.get("genderCode")
                })
        print("Exported events to events.csv")
        
        # Print events to console for quick reference
        print("\nEvents found:")
        for event in events:
            print(f"ID: {event.get('eventId')} | {event.get('disciplineName')} | Gender: {event.get('genderCode')}")
    else:
        print("No events found") 