import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'road_to_tokyo'),
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            print("Database connection established successfully")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            raise
    
    def create_tables(self):
        """Create all tables if they don't exist"""
        try:
            cursor = self.connection.cursor()
            
            # Create ranking_info table
            ranking_info_table = """
            CREATE TABLE IF NOT EXISTS ranking_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                row_number INT,
                scrape_datestamp DATETIME,
                eventId INT,
                disciplineName VARCHAR(255),
                genderCode VARCHAR(10),
                qualifiedBy VARCHAR(255),
                qualified BOOLEAN,
                qualificationPosition INT,
                countryPosition INT,
                name VARCHAR(255),
                urlSlug VARCHAR(255),
                iaafId VARCHAR(255),
                birthDate DATE,
                competitorIaafId VARCHAR(255),
                wind VARCHAR(50),
                result VARCHAR(255),
                venue VARCHAR(255),
                date DATE,
                countryCode VARCHAR(10),
                place INT,
                score DECIMAL(10,2),
                calculationId INT,
                label VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_eventId (eventId),
                INDEX idx_disciplineName (disciplineName),
                INDEX idx_countryCode (countryCode),
                INDEX idx_scrape_datestamp (scrape_datestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            # Create event_info table
            event_info_table = """
            CREATE TABLE IF NOT EXISTS event_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                row_number INT,
                scrape_datestamp DATETIME,
                eventId INT,
                groupByCountry BOOLEAN,
                entryNumber INT,
                entryStandard VARCHAR(255),
                disciplineName VARCHAR(255),
                maxCompetitorsByCoutnry INT,
                firstQualificationDay DATE,
                lastQualificationDay DATE,
                firstRankingDay DATE,
                lastRankingDay DATE,
                rankDate DATE,
                numberOfCompetitorsQualifiedByEntryStandard INT,
                numberOfCompetitorsQualifiedByTopList INT,
                numberOfCompetitorsFilledUpByWorldRankings INT,
                numberOfCompetitorsQualifiedByUniversalityPlaces INT,
                numberOfCompetitorsQualifiedByDesignatedCompetition INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_eventId (eventId),
                INDEX idx_disciplineName (disciplineName),
                INDEX idx_scrape_datestamp (scrape_datestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            # Create athlete_results table
            athlete_results_table = """
            CREATE TABLE IF NOT EXISTS athlete_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                row_number INT,
                scrape_datestamp DATETIME,
                athleteCalculationId INT,
                eventId INT,
                disciplineName VARCHAR(255),
                date DATE,
                competition VARCHAR(255),
                country VARCHAR(255),
                category VARCHAR(255),
                disciplineCode VARCHAR(50),
                disciplineNameUrlSlug VARCHAR(255),
                typeNameUrlSlug VARCHAR(255),
                indoor BOOLEAN,
                discipline VARCHAR(255),
                race VARCHAR(255),
                place INT,
                mark VARCHAR(255),
                wind VARCHAR(50),
                drop VARCHAR(50),
                resultScore DECIMAL(10,2),
                worldRecord BOOLEAN,
                placingScore DECIMAL(10,2),
                performanceScore DECIMAL(10,2),
                monthCorrectionApplied BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_athleteCalculationId (athleteCalculationId),
                INDEX idx_eventId (eventId),
                INDEX idx_disciplineName (disciplineName),
                INDEX idx_date (date),
                INDEX idx_scrape_datestamp (scrape_datestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            # Execute table creation
            cursor.execute(ranking_info_table)
            cursor.execute(event_info_table)
            cursor.execute(athlete_results_table)
            
            self.connection.commit()
            print("All tables created successfully")
            
        except Error as e:
            print(f"Error creating tables: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def insert_ranking_info(self, data, scrape_datestamp):
        """Insert ranking info data"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            INSERT INTO ranking_info (
                row_number, scrape_datestamp, eventId, disciplineName, genderCode,
                qualifiedBy, qualified, qualificationPosition, countryPosition,
                name, urlSlug, iaafId, birthDate, competitorIaafId, wind,
                result, venue, date, countryCode, place, score, calculationId, label
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            for i, row in enumerate(data, 1):
                values = (
                    i, scrape_datestamp,
                    row.get('eventId'), row.get('disciplineName'), row.get('genderCode'),
                    row.get('qualifiedBy'), row.get('qualified'), row.get('qualificationPosition'),
                    row.get('countryPosition'), row.get('name'), row.get('urlSlug'),
                    row.get('iaafId'), row.get('birthDate'), row.get('competitorIaafId'),
                    row.get('wind'), row.get('result'), row.get('venue'), row.get('date'),
                    row.get('countryCode'), row.get('place'), row.get('score'),
                    row.get('calculationId'), row.get('label')
                )
                cursor.execute(query, values)
            
            self.connection.commit()
            print(f"Inserted {len(data)} rows into ranking_info table")
            
        except Error as e:
            print(f"Error inserting ranking_info: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def insert_event_info(self, data, scrape_datestamp):
        """Insert event info data"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            INSERT INTO event_info (
                row_number, scrape_datestamp, eventId, groupByCountry, entryNumber,
                entryStandard, disciplineName, maxCompetitorsByCoutnry,
                firstQualificationDay, lastQualificationDay, firstRankingDay,
                lastRankingDay, rankDate, numberOfCompetitorsQualifiedByEntryStandard,
                numberOfCompetitorsQualifiedByTopList, numberOfCompetitorsFilledUpByWorldRankings,
                numberOfCompetitorsQualifiedByUniversalityPlaces, numberOfCompetitorsQualifiedByDesignatedCompetition
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            for i, row in enumerate(data, 1):
                values = (
                    i, scrape_datestamp,
                    row.get('eventId'), row.get('groupByCountry'), row.get('entryNumber'),
                    row.get('entryStandard'), row.get('disciplineName'), row.get('maxCompetitorsByCoutnry'),
                    row.get('firstQualificationDay'), row.get('lastQualificationDay'),
                    row.get('firstRankingDay'), row.get('lastRankingDay'), row.get('rankDate'),
                    row.get('numberOfCompetitorsQualifiedByEntryStandard'),
                    row.get('numberOfCompetitorsQualifiedByTopList'),
                    row.get('numberOfCompetitorsFilledUpByWorldRankings'),
                    row.get('numberOfCompetitorsQualifiedByUniversalityPlaces'),
                    row.get('numberOfCompetitorsQualifiedByDesignatedCompetition')
                )
                cursor.execute(query, values)
            
            self.connection.commit()
            print(f"Inserted {len(data)} rows into event_info table")
            
        except Error as e:
            print(f"Error inserting event_info: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def insert_athlete_results(self, data, scrape_datestamp):
        """Insert athlete results data"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            INSERT INTO athlete_results (
                row_number, scrape_datestamp, athleteCalculationId, eventId, disciplineName,
                date, competition, country, category, disciplineCode, disciplineNameUrlSlug,
                typeNameUrlSlug, indoor, discipline, race, place, mark, wind, drop,
                resultScore, worldRecord, placingScore, performanceScore, monthCorrectionApplied
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            for i, row in enumerate(data, 1):
                values = (
                    i, scrape_datestamp,
                    row.get('athleteCalculationId'), row.get('eventId'), row.get('disciplineName'),
                    row.get('date'), row.get('competition'), row.get('country'), row.get('category'),
                    row.get('disciplineCode'), row.get('disciplineNameUrlSlug'), row.get('typeNameUrlSlug'),
                    row.get('indoor'), row.get('discipline'), row.get('race'), row.get('place'),
                    row.get('mark'), row.get('wind'), row.get('drop'), row.get('resultScore'),
                    row.get('worldRecord'), row.get('placingScore'), row.get('performanceScore'),
                    row.get('monthCorrectionApplied')
                )
                cursor.execute(query, values)
            
            self.connection.commit()
            print(f"Inserted {len(data)} rows into athlete_results table")
            
        except Error as e:
            print(f"Error inserting athlete_results: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")

if __name__ == "__main__":
    # Test database connection and table creation
    db = DatabaseManager()
    db.create_tables()
    db.close() 