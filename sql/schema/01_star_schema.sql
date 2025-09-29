-- Dimension Tables
CREATE TABLE dim_candidates (
    candidate_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    gender VARCHAR(20),
    age INTEGER,
    age_group VARCHAR(20),
    province_id VARCHAR(50),
    cohort_id VARCHAR(50),
    team_id VARCHAR(50),
    enrollment_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_cohorts (
    cohort_id VARCHAR(50) PRIMARY KEY,
    cohort_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_teams (
    team_id VARCHAR(50) PRIMARY KEY,
    team_name VARCHAR(100),
    cohort_id VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_provinces (
    province_id VARCHAR(50) PRIMARY KEY,
    province_name VARCHAR(100),
    region VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_programs (
    program_id SERIAL PRIMARY KEY,
    program_name VARCHAR(100),
    program_type VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Tables
CREATE TABLE fact_placements (
    placement_id VARCHAR(50) PRIMARY KEY,
    candidate_id VARCHAR(50) REFERENCES dim_candidates(candidate_id),
    company_name VARCHAR(100),
    placement_status VARCHAR(50),
    start_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_coursera (
    progress_id VARCHAR(50) PRIMARY KEY,
    candidate_id VARCHAR(50) REFERENCES dim_candidates(candidate_id),
    course_name VARCHAR(150),
    date_completed DATE,
    grade_achieved DECIMAL(5,2),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_scrums (
    scrum_id VARCHAR(50) PRIMARY KEY,
    team_id VARCHAR(50) REFERENCES dim_teams(team_id),
    session_date DATE,
    attendance_count INTEGER,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_projects (
    project_id VARCHAR(50) PRIMARY KEY,
    team_id VARCHAR(50) REFERENCES dim_teams(team_id),
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);