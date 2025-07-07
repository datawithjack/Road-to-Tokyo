import requests
import csv
import asyncio
import aiohttp
import json
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

async def run_graphql_query_async(session, query, variables=None):
    payload = {
        "query": query,
        "variables": variables or {}
    }
    async with session.post(GRAPHQL_ENDPOINT, json=payload, headers=HEADERS) as response:
        response.raise_for_status()
        return await response.json()

def get_all_events():
    """Get all events for the competition"""
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
    
    variables = {"competitionId": 7190593}
    result = run_graphql_query(query, variables)
    event_info = result.get("data", {}).get("getChampionshipQualifications", {})
    return event_info.get("events", [])

async def process_event(session, event, main_query, ranking_query):
    """Process a single event and return its data"""
    event_id = event.get("eventId")
    discipline_name = event.get("disciplineName")
    gender_code = event.get("genderCode")
    
    print(f"Processing event: {discipline_name} (ID: {event_id}, Gender: {gender_code})")
    
    # Main query for this event
    variables = {
        "competitionId": 7190593,
        "eventId": event_id
    }
    
    try:
        result = await run_graphql_query_async(session, main_query, variables)
        event_info = result.get("data", {}).get("getChampionshipQualifications", {})
        qualifications = event_info.get("qualifications", [])
        
        print(f"  Found {len(qualifications)} qualifications for {discipline_name}")
        
        # Add event info to each qualification
        for q in qualifications:
            q["eventId"] = event_id
            q["disciplineName"] = discipline_name
            q["genderCode"] = gender_code
        
        # Get athlete results for those with calculationId
        athlete_results = []
        athletes_with_calculation = [q for q in qualifications if q.get("calculationId")]
        print(f"  Found {len(athletes_with_calculation)} athletes with calculationId")
        
        # Limit to first 10 athletes for testing (remove this limit later)
        athletes_to_process = athletes_with_calculation[:10]
        print(f"  Processing first {len(athletes_to_process)} athletes for testing...")
        
        for i, q in enumerate(athletes_to_process):
            calculation_id = q.get("calculationId")
            if calculation_id:
                try:
                    print(f"    Fetching results for athlete {i+1}/{len(athletes_to_process)} (ID: {calculation_id})...")
                    detail_variables = {"athleteId": int(calculation_id)}
                    detail_result = await run_graphql_query_async(session, ranking_query, detail_variables)
                    results = detail_result.get("data", {}).get("getRankingScoreCalculation", {}).get("results", [])
                    print(f"      Found {len(results)} results for athlete {calculation_id}")
                    for r in results:
                        r["athleteCalculationId"] = calculation_id
                        r["eventId"] = event_id
                        r["disciplineName"] = discipline_name
                        athlete_results.append(r)
                except Exception as e:
                    print(f"    Error fetching results for athlete {calculation_id}: {e}")
                    continue
        
        return {
            "event_info": event_info,
            "qualifications": qualifications,
            "athlete_results": athlete_results
        }
        
    except Exception as e:
        print(f"  Error processing event {event_id}: {e}")
        return None

async def main():
    print("Starting scraper with async processing...")
    
    # Get all events first
    print("Fetching all events...")
    events = get_all_events()
    print(f"Found {len(events)} events to process")
    
    # Define queries
    main_query = """
    query GetChampionshipQualifications($competitionId: Int!, $eventId: Int, $country: String, $qualificationType: String) {
      getChampionshipQualifications(competitionId: $competitionId, eventId: $eventId, country: $country, qualificationType: $qualificationType) {
        eventId
        groupByCountry
        entryNumber
        entryStandard
        alternativeEntryStandards {
          entryStandard
          event
          __typename
        }
        disciplineName
        maxCompetitorsByCoutnry
        firstQualificationDay
        lastQualificationDay
        firstRankingDay
        lastRankingDay
        rankDate
        numberOfCompetitorsQualifiedByEntryStandard
        numberOfCompetitorsQualifiedByTopList
        numberOfCompetitorsFilledUpByWorldRankings
        numberOfCompetitorsQualifiedByUniversalityPlaces
        numberOfCompetitorsQualifiedByDesignatedCompetition
        qualifications {
          qualifiedBy
          qualified
          qualificationPosition
          countryPosition
          name
          urlSlug
          iaafId
          birthDate
          competitorIaafId
          wind
          result
          venue
          date
          countryCode
          place
          score
          calculationId
          label
          average
          sum
          results {
            resultScore
            venue
            date
            team
            __typename
          }
          __typename
        }
        events {
          genderCode
          eventId
          disciplineName
          __typename
        }
        countries {
          shortname
          name
          __typename
        }
        qualificationTypes {
          id
          name
          __typename
        }
        __typename
      }
    }
    """

    ranking_query = """
    query GetRankingScoreCalculation($athleteId: Int!) {
      getRankingScoreCalculation(athleteId: $athleteId) {
        results {
          date
          competition
          country
          category
          disciplineCode
          disciplineNameUrlSlug
          typeNameUrlSlug
          indoor
          discipline
          race
          place
          mark
          wind
          drop
          resultScore
          worldRecord
          placingScore
          performanceScore
          monthCorrectionApplied
          __typename
        }
        __typename
      }
    }
    """

    # Process events concurrently
    async with aiohttp.ClientSession() as session:
        # Only process the first event
        first_event = events[0] if events else None
        if first_event:
            print(f"Processing only the first event: {first_event.get('disciplineName')} (ID: {first_event.get('eventId')})")
            result = await process_event(session, first_event, main_query, ranking_query)
            results = [result] if result else []
        else:
            results = []
    
    # Collect all data
    all_qualifications = []
    all_event_info = []
    all_athlete_results = []
    
    for result in results:
        if result and isinstance(result, dict):
            if result.get("qualifications"):
                all_qualifications.extend(result["qualifications"])
            if result.get("event_info"):
                all_event_info.append(result["event_info"])
            if result.get("athlete_results"):
                all_athlete_results.extend(result["athlete_results"])
    
    print(f"\nTotal qualifications found: {len(all_qualifications)}")
    print(f"Total events processed: {len(all_event_info)}")
    print(f"Total athlete results found: {len(all_athlete_results)}")
    
    # Export data
    print("\nExporting data to CSV files...")
    
    # Export qualifications (excluding 'average' and 'sum')
    if all_qualifications:
        ranking_info_keys = set()
        for q in all_qualifications:
            ranking_info_keys.update(k for k in q.keys() if k not in ("average", "sum"))
        
        with open("ranking_info.csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(ranking_info_keys))
            writer.writeheader()
            for q in all_qualifications:
                filtered_q = {k: v for k, v in q.items() if k not in ("average", "sum")}
                writer.writerow(filtered_q)
        print("Exported all qualifications to ranking_info.csv")
    
    # Export event info
    if all_event_info:
        event_fields = [
            "eventId", "groupByCountry", "entryNumber", "entryStandard", "disciplineName",
            "maxCompetitorsByCoutnry", "firstQualificationDay", "lastQualificationDay",
            "firstRankingDay", "lastRankingDay", "rankDate",
            "numberOfCompetitorsQualifiedByEntryStandard", "numberOfCompetitorsQualifiedByTopList",
            "numberOfCompetitorsFilledUpByWorldRankings", "numberOfCompetitorsQualifiedByUniversalityPlaces",
            "numberOfCompetitorsQualifiedByDesignatedCompetition"
        ]
        
        with open("event_info.csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=event_fields)
            writer.writeheader()
            for event_info in all_event_info:
                writer.writerow({field: event_info.get(field) for field in event_fields})
        print("Exported event info to event_info.csv")
    
    # Export athlete results
    if all_athlete_results:
        result_keys = set()
        for r in all_athlete_results:
            result_keys.update(r.keys())
        
        with open("athlete_results.csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(result_keys))
            writer.writeheader()
            for r in all_athlete_results:
                writer.writerow(r)
        print("Exported athlete results to athlete_results.csv")
    
    print("\nScraping complete!")

if __name__ == "__main__":
    asyncio.run(main()) 