# Revenue Data Analysis
This repository contains a data analysis solution for understanding revenue attribution issues in a billing system. 
The project analyzes invoice, position, and customer data to identify discrepancies and propose improvements to the existing data pipeline.

## Setup Instructions
### Prerequisites
- Docker
- Python 3.8+
- Git

### Getting Started
1. Clone the repository:
```bash
git clone https://github.com/yankh764/revenue-data-analysis
cd revenue-data-analysis
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
# Activate on macOS/Linux
source venv/bin/activate
```

3. Install the required Python packages:
```bash
pip install -r requirements.txt
```

4. Create an `.env` file based on the provided `.env.example`:
```bash
cp .env.example .env
```

### Database Setup
1. Pull and run the Microsoft SQL Server Docker image:
```bash
docker pull mcr.microsoft.com/mssql/server:latest
docker run -v ./sql:/sql -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=SimplePassword123" \
   -p 1433:1433 --name sql1 --hostname sql1 \
   -d mcr.microsoft.com/mssql/server:latest
```

2. Create the database and required tables:
```bash
docker exec -it sql1 /opt/mssql-tools18/bin/sqlcmd -C \
   -S localhost -U sa -P SimplePassword123 -i /sql/schema.sql
```

Alternatively, you can use a database management tool like Azure Data Studio to execute the SQL script.

### Data Loading
With your virtual environment activated and database set up, run the data loader script to populate the database with the sample data:
```bash
python -m scripts.data_loader
```

This script will read the CSV files from the `data` directory and load them into the corresponding tables in the database.


### Running the Analysis Queries
To run the analysis queries against your database:
```bash
docker exec -it sql1 /opt/mssql-tools18/bin/sqlcmd -C \
   -S localhost -U sa -P SimplePassword123 -i /sql/analysis.sql
```

You can also run individual queries from the analysis.sql file using your preferred database client.

## Project Structure
```
revenue-data-analysis/
├── docs                       # Documentation files for analysis and enhancement proposals
│   ├── analysis_findings.md   # Data analysis findings
│   ├── pipeline_proposal.md   # Enhanced data pipeline proposal
│   └── modern_tooling.md      # Modern tooling proposal (optional)
├── data                       # Source CSV files (mock data)
│   ├── customers.csv          # Customer information
│   ├── invoices.csv           # Invoice data
│   └── positions.csv          # Invoice line items
├── scripts                    # Python scripts
│   ├── __init__.py
│   ├── data_loader.py         # Script for loading data into database
│   ├── data_quality.py        # Script for checking and reporting data quality (proposal)
│   └── data_validator.py      # Script for simple preprocessing and data validation (proposal)
├── sql                        # SQL scripts
│   ├── schema.sql             # Database schema definition
│   ├── analysis.sql           # Data analysis queries
│   ├── quality.sql            # Data quality table (proposal)
│   └── view.sql               # Data report view (proposal)
├── requirements.txt           # Python dependencies
└── .env.example               # Template for required environment variables (DB credenitals)
```
