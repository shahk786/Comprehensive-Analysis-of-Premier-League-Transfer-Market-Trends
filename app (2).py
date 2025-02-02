import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# run this in your terminal
    # pip install seaborn 

# create db connection

db_host = ''
db_user = ''
db_password = ''
db_name = ''
db_port = ''

engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")


conn = mysql.connector.connect(
    host = db_host,
    user = db_user,
    password = db_password,
    database = db_name,
    port = db_port
)

if conn.is_connected:
    print("Sucessfully connected to the db")
else:
    print("Failed to connect to db")


query = """
    SELECT
        name,
        position,
        age,
        market_value,
        country_from,
        league_from,
        club_from,
        country_to,
        league_to,
        club_to,
        fee,
        loan
    FROM
        football_summer_transfers_2022_2023
"""

df = pd.read_sql(query, engine)

conn.close()

def clean_money(value):
    if pd.isnull(value):
        return np.nan

    value = str(value)

    if 'undisclosed' in value.lower() or 'unknown' in value.lower():
        return np.nan
    if 'free' in value.lower() or 'loan' in value.lower():
        return '0.0'

    value = value.strip()

    # error handling
    try:
        value = int(value)
        value *= 1000000
        return value
    except:
        return np.nan

df['fee_clean'] = df['fee'].apply(clean_money)
df['market_value_clean'] = df['market_value'].apply(clean_money)





# Standardize 'position' column

position_mapping = {
    'Centre-Forward': 'Forward',
    'Left Winger': 'Forward',
    'Right Winger': 'Forward',
    'Second Striker': 'Forward',
    'Attacking Midfield': 'Midfielder',
    'Central Midfield': 'Midfielder',
    'Defensive Midfield': 'Midfielder',
    'Left Midfield': 'Midfielder',
    'Right Midfield': 'Midfielder',
    'Left-Back': 'Defender',
    'Right-Back': 'Defender',
    'Centre-Back': 'Defender',
    'Sweeper': 'Defender',
    'Goalkeeper': 'Goalkeeper',
}

# here we use the .map functionality to map the various possitions with the standard possition we provided above.

df['position_standardized'] = df['position'].map(position_mapping)
df['position_standardized'] = df['position_standardized'].fillna('Other')

#  this if for the league mapping

league_name_mapping = {
    'Epl': 'Premier League',
    'Premiership': 'Premier League',
    'La Liga': 'LaLiga',
    'Bundesliga 1': 'Bundesliga',
    'Serie A': 'Serie A',
    'Ligue 1': 'Ligue 1',
    'Championship': 'EFL Championship'
}

# here we create a new column(league_from_standardized, league_to_standardized) that copies all the data from the original column (league_from, league_to)

df['league_from_standardized'] = df['league_from'].str.strip().str.title()
df['league_to_standardized'] = df['league_to'].str.strip().str.title()

# we use the .replace functionality to change the values in our new column. Works just like the .map() but replaces all the values

df['league_from_standardized'] = df['league_from_standardized'].replace(league_name_mapping)
df['league_to_standardized'] = df['league_to_standardized'].replace(league_name_mapping)

# Convert 'loan' column to boolean

df['loan'] = df['loan'].astype(str).str.lower()

loan_mapping = {
    'yes': True,
    'no': False,
    '1': True,
    '0': False,
    'true': True,
    'false': False,
    'nan': False  # Treat NaN as False unless specified
}

df['loan'] = df['loan'].map(loan_mapping)
df['loan'] = df['loan'].fillna(False)

# print(df.head())

# creating a subset of data to work with that has only the clean values

final_columns = [
    'name',
    'position_standardized',
    'age',
    'market_value_clean',
    'country_from',
    'league_from_standardized',
    'club_from',
    'country_to',
    'league_to_standardized',
    'club_to',
    'fee_clean',
    'loan'
]

cleaned_df = df[final_columns]

cleaned_df = cleaned_df.rename(columns={
    'position_standardized': 'position',
    'market_value_clean': 'market_value',
    'league_from_standardized': 'league_from',
    'league_to_standardized': 'league_to',
    'fee_clean': 'fee'
})

# Replace dashes with NaN
cleaned_df['age'] = cleaned_df['age'].replace('-', np.nan)
cleaned_df['fee'] = cleaned_df['fee'].replace('-', np.nan)
cleaned_df['market_value'] = cleaned_df['market_value'].replace('-', np.nan)

# Convert column to numeric, forcing non-numeric entries to become NaN
cleaned_df['age'] = pd.to_numeric(cleaned_df['age'], errors='coerce')
cleaned_df['fee'] = pd.to_numeric(cleaned_df['fee'], errors='coerce')
cleaned_df['market_value'] = pd.to_numeric(cleaned_df['market_value'], errors='coerce')

# Fill or handle missing values if needed
median_age = cleaned_df['age'].median()
cleaned_df['age'] = cleaned_df['age'].fillna(median_age)

# Set reasonable age limits (optional)
cleaned_df.loc[(cleaned_df['age'] < 15) | (cleaned_df['age'] > 45), 'age'] = median_age

# print(cleaned_df.head())

# 1st step of anylysis.
# statistical descriptive summary of all eligible columns

# print(cleaned_df.describe(include='all'))

# correlation anaylysis


numerical_cols = ['age', 'market_value', 'fee']

corr_matrix = cleaned_df[numerical_cols].corr()

# print(corr_matrix)
# sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
# plt.title('Correlation Matrix')
# plt.show()


# Exploring Factors Influencing Player Fees

# 1 Position-Wise Fee Analysis
# Hypothesis: Forwards may command higher transfer fees than defenders or goalkeepers.

# Group by position and calculate average fee
fee_by_position = cleaned_df.groupby('position')['fee'].mean().sort_values(ascending=False)

# print(fee_by_position)

# # Visualize
# fee_by_position.plot(kind='bar', figsize=(8, 5))
# plt.ylabel('Average Fee (in GBP)')
# plt.title('Average Transfer Fee by Player Position')
# plt.show()

# 2 Age vs. Fee
# Hypothesis: Younger players with high potential might command higher fees.

# plt.figure(figsize=(8,6))
# sns.scatterplot(data=cleaned_df, x='age', y='fee')
# plt.title('Age vs. Transfer Fee')
# plt.xlabel('Age')
# plt.ylabel('Transfer Fee (in GBP)')
# plt.show()

# 3 Market Value vs. Fee
# Hypothesis: Transfer fees closely track players’ market values. Players with high market values should generally have high transfer fees.

# plt.figure(figsize=(8,6))
# sns.scatterplot(data=cleaned_df, x='market_value', y='fee')
# plt.title('Market Value vs. Transfer Fee')
# plt.xlabel('Market Value (in GBP)')
# plt.ylabel('Transfer Fee (in GBP)')
# plt.show()


# Club-Specific Analysis

# 1 Fee by Buying Club
# Hypothesis: Wealthier clubs tend to spend more on average. Even for a single year, certain clubs might dominate spending.

fee_by_buying_club = cleaned_df.groupby('club_to')['fee'].mean().sort_values(ascending=False)
# print(fee_by_buying_club)

# Plot the top 10 clubs by average fee spent
# top_10_spending_clubs = fee_by_buying_club.head(10)
# top_10_spending_clubs.plot(kind='bar', figsize=(10,5))
# plt.ylabel('Average Fee (in GBP)')
# plt.title('Top 10 Clubs by Average Transfer Fee Spent')
# plt.show()


# 2 Fee by Selling Club
# Hypothesis: Certain clubs have a reputation for selling players at high fees (e.g., feeder clubs or clubs known for developing talent).

# fee_by_selling_club = cleaned_df.groupby('club_from')['fee'].mean().sort_values(ascending=False)
# print(fee_by_selling_club.head(10))


# Country and League Analysis

# 1 Country of Origin vs. Fee
# Hypothesis: Players from certain countries might command higher fees due to visibility, brand value, or historical success rates.

# fee_by_country_from = cleaned_df.groupby('country_from')['fee'].mean().sort_values(ascending=False)
# print(fee_by_country_from.head(10))


# 2 From-League vs. Fee
# Hypothesis: Transferring from a top-tier league (like the Premier League, LaLiga, Bundesliga) might influence fees.

# fee_by_league_from = cleaned_df.groupby('league_from')['fee'].mean().sort_values(ascending=False)
# print(fee_by_league_from)

# fee_by_league_from.plot(kind='bar', figsize=(10,5))
# plt.ylabel('Average Fee (in GBP)')
# plt.title('Average Transfer Fee by Source League')
# plt.show()


# Loans vs. Permanent Transfers


# loan_count = cleaned_df['loan'].value_counts()
# print(loan_count)

# # If 'loan' is True, the fee should often be zero or very low for that record.
# loan_fees = cleaned_df.groupby('loan')['fee'].mean()
# print(loan_fees)


# Identifying Outliers


plt.figure(figsize=(8,6))
sns.boxplot(data=cleaned_df, x='position', y='fee')
plt.title('Box Plot of Transfer Fees by Position')
plt.show()
