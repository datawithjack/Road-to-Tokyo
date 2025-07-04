import requests
import csv

GRAPHQL_ENDPOINT = "https://graphql-prod-4776.prod.aws.worldathletics.org/graphql"
API_KEY = "da2-q2nzlr3kvvht3n45ut5w7i6p7q"

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

    variables = {
        "competitionId": 7190593
    }

    print("Starting main GraphQL query for qualifications...")
    result = run_graphql_query(query, variables)
    print("Main query complete. Parsing event and qualification data...")

    event_info = result.get("data", {}).get("getChampionshipQualifications", {})
    qualifications = event_info.get("qualifications", [])
    print(f"Found {len(qualifications)} qualifications.")

    # Export all fields for inspection (excluding 'average' and 'sum')
    print("Exporting all qualification fields to all.csv...")
    if qualifications:
        # Collect all unique keys across all qualification dicts, excluding 'average' and 'sum'
        all_keys = set()
        for q in qualifications:
            all_keys.update(k for k in q.keys() if k not in ("average", "sum"))

        # Write to all.csv
        with open("all.csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(all_keys))
            writer.writeheader()
            for q in qualifications:
                # Exclude 'average' and 'sum' from each row
                filtered_q = {k: v for k, v in q.items() if k not in ("average", "sum")}
                writer.writerow(filtered_q)

        print("Exported all fields to all.csv")
    else:
        print("No qualifications data found to export.")

    print("Exporting event info to event_info.csv...")
    event_fields = [
        "eventId",
        "groupByCountry",
        "entryNumber",
        "entryStandard",
        "disciplineName",
        "maxCompetitorsByCoutnry",
        "firstQualificationDay",
        "lastQualificationDay",
        "firstRankingDay",
        "lastRankingDay",
        "rankDate",
        "numberOfCompetitorsQualifiedByEntryStandard",
        "numberOfCompetitorsQualifiedByTopList",
        "numberOfCompetitorsFilledUpByWorldRankings",
        "numberOfCompetitorsQualifiedByUniversalityPlaces",
        "numberOfCompetitorsQualifiedByDesignatedCompetition"
    ]
    with open("event_info.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=event_fields)
        writer.writeheader()
        writer.writerow({field: event_info.get(field) for field in event_fields})
    print("Exported event info to event_info.csv")

    # Second query for athlete details (only for those with calculationId)
    print("Running secondary queries for athlete details (with calculationId)...")
    ranking_query = """
    query GetRankingScoreCalculation($athleteId: Int) {
      getRankingScoreCalculation(athleteId: $athleteId) {
        rankDate
        eventGroup
        male
        athlete
        athleteUrlSlug
        birthDate
        country
        countryFullName
        place
        withWind
        withDrop
        withWorldRecord
        withMonthCorrection
        averagePerformanceScore
        rankingScore
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

    athlete_details = []
    for i, q in enumerate(qualifications):
        calculation_id = q.get("calculationId")
        if calculation_id:
            print(f"Fetching details for athlete {i+1}/{len(qualifications)} with calculationId {calculation_id}...")
            variables = {"athleteId": int(calculation_id)}
            detail_result = run_graphql_query(ranking_query, variables)
            detail = detail_result.get("data", {}).get("getRankingScoreCalculation", {})
            if detail:
                athlete_details.append(detail)

    print(f"Fetched details for {len(athlete_details)} athletes. Exporting to athlete_details.csv...")
    # Export athlete details to a separate CSV (flattening only top-level fields)
    if athlete_details:
        # Collect all unique keys across all athlete detail dicts (excluding 'results' which is a list)
        detail_keys = set()
        for d in athlete_details:
            detail_keys.update(k for k in d.keys() if k != "results")
        with open("athlete_details.csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(detail_keys))
            writer.writeheader()
            for d in athlete_details:
                row = {k: v for k, v in d.items() if k != "results"}
                writer.writerow(row)
        print("Exported athlete details to athlete_details.csv")
    else:
        print("No athlete details found to export.") 