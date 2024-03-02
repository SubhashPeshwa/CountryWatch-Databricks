## Data eto extract:-

Demographics and Census Data: Population statistics, demographic trends, and information from national censuses.
Health and Education Data: Information about healthcare facilities, disease statistics, education enrollment, literacy rates, and related indicators.
Infrastructure Data: Data related to transportation, energy, water supply, and other essential infrastructure.
Agriculture and Rural Development: Agricultural production data, land utilization, rural development projects, and related metrics.
Public Services: Data on government schemes, public distribution systems, and various citizen services.
Governance and Administrative Data: Information about government organizations, budgets, expenditure, and administrative structures.
Elections and Political Data: Electoral information, voting trends, political parties' data, and election results.

# Data Model:-

Government_Organization:

org_id (Primary Key)
org_name
org_type
org_hierarchy (Ministry, Department, Division, etc.)
org_head_name
org_head_designation
contact_info (Phone, Email, Address, Website)
Government_Budget:

budget_id (Primary Key)
year
org_id (Foreign Key referencing Government_Organization table)
budget_allocated
budget_spent
Government_Expenditure:

expenditure_id (Primary Key)
year
org_id (Foreign Key referencing Government_Organization table)
expenditure_type
expenditure_amount
Public_Services:

service_id (Primary Key)
service_name
org_id (Foreign Key referencing Government_Organization table)
service_description
service_coverage (Geographic coverage)
service_providers (Government bodies responsible)
Administrative_Structure:

admin_id (Primary Key)
org_id (Foreign Key referencing Government_Organization table)
administrative_level (Central, State, Local)
administrative_area_name
administrative_area_population
Election_Info:

election_id (Primary Key)
election_type
election_date
org_id (Foreign Key referencing Government_Organization table)
winning_party_name
voting_turnout_percentage

## Query Examples:

Who is heading the Ministry of Health and what's their contact information?
How much was allocated and spent by the Ministry of Education in the last fiscal year?
Which public services are provided by the Department of Transportation in rural areas?
How has administrative area population changed in the last decade for different levels of governance?
What was the voter turnout in the last general election, and who won?