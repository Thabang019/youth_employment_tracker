import logging
import pandas as pd
from sqlalchemy import create_engine, text
from utils.helpers import get_db_connection_string

logger = logging.getLogger(__name__)

class DatabaseLoader:
    def __init__(self):
        self.engine = None
        self.connection_string = get_db_connection_string()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.engine = create_engine(self.connection_string)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def load_to_warehouse(self, transformed_data):
        """Load transformed data into data warehouse"""
        if not self.connect():
            return False
        
        try:
            # Load dimension tables first
            self._load_dimensions(transformed_data)
            
            # Load fact tables
            self._load_facts(transformed_data)
            
            # Refresh materialized views
            self._refresh_views()
            
            logger.info("Data successfully loaded to warehouse")
            return True
            
        except Exception as e:
            logger.error(f"Database loading failed: {e}")
            return False
    
    def _load_dimensions(self, data):
        """Load dimension tables"""
        if 'enhanced_candidates' in data:
            candidates_df = data['enhanced_candidates'][[
                'CandidateID', 'FirstName', 'LastName', 'Email', 'Gender', 
                'Age', 'AgeGroup', 'ProvinceID', 'CohortID', 'TeamID', 'EnrollmentDate'
            ]].rename(columns={
                'CandidateID': 'candidate_id',
                'FirstName': 'first_name', 
                'LastName': 'last_name',
                'Email': 'email',
                'Gender': 'gender',
                'Age': 'age',
                'AgeGroup': 'age_group',
                'ProvinceID': 'province_id',
                'CohortID': 'cohort_id',
                'TeamID': 'team_id',
                'EnrollmentDate': 'enrollment_date'
            })
            
            with self.engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE dim_candidates RESTART IDENTITY CASCADE"))

            candidates_df.to_sql('dim_candidates', self.engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(candidates_df)} records to dim_candidates")

    
    def _load_facts(self, data):
        """Load fact tables"""
        if 'placement_analysis' in data:
            placements_df = data['placement_analysis'][[
                'PlacementID', 'CandidateID', 'CompanyName', 'PlacementStatus', 
                'StartDate'
            ]].rename(columns={
                'PlacementID': 'placement_id',
                'CandidateID': 'candidate_id',
                'CompanyName': 'company_name',
                'PlacementStatus': 'placement_status',
                'StartDate': 'start_date'
            })
            
            with self.engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE fact_placements RESTART IDENTITY CASCADE"))
            
            placements_df.to_sql('fact_placements', self.engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(placements_df)} records to fact_placements")
        
    
    def _refresh_views(self):
        """Refresh materialized views"""
        with self.engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_placement_rates"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_cohort_performance"))
            conn.commit()
            logger.info("Materialized views refreshed")
    
    def execute_query(self, query):
        """Execute SQL query and return results"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return result.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []