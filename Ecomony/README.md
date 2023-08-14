## Data eto extract:-

Gross Domestic Product (GDP): Economic indicators, GDP growth rates, and sector-wise contributions to the economy.
Trade and Commerce: Export and import data, trade balances, commodity-wise trade statistics.
Industrial Data: Manufacturing production data, industrial growth rates, and factory output.
Financial Data: Banking and financial sector statistics, inflation rates, interest rates, and fiscal policies.
Labor and Employment Data: Unemployment rates, employment trends, workforce demographics.
Business and Investment: Information on business registration, investment policies, and investment trends.


## Data Model

Economy_Data:

economy_id (Primary Key)
year
quarter
sector_name
gdp_value
inflation_rate
export_value
import_value
employment_count
Trade_Balance:

trade_id (Primary Key)
year
quarter
country_name
export_value
import_value
trade_balance
Industrial_Production:

ind_production_id (Primary Key)
year
quarter
sector_name
industrial_growth_rate
factory_output
Financial_Indicators:

financial_id (Primary Key)
year
quarter
indicator_name
indicator_value
Employment_Data:

employment_id (Primary Key)
year
quarter
sector_name
employment_count
Business_Registration:

business_id (Primary Key)
year
quarter
business_name
industry_sector
registration_status
Investment_Trends:

investment_id (Primary Key)
year
quarter
investment_type
sector_name
investment_amount

## Query Examples:

How did India's GDP growth rate correlate with export values over the last decade?
Which sectors saw the highest industrial growth rates in the past quarter?
What's the trend in employment counts for different sectors over the past five years?
How have financial indicators like interest rates and inflation impacted business investments?