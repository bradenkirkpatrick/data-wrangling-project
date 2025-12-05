import duckdb
import pandas as pd
# if their is not saves folder create it
import os
if not os.path.exists('saves'):
    os.makedirs('saves')

#download data
exit_velocity_data = pd.read_csv('data/mlb-batter-exit-velocity.csv')

# run sql file to create tables
con = duckdb.connect('baseball_data.db').execute(open('baseball_data.sql').read())

# Create a DataFrame from a query
batting = con.execute("SELECT * FROM batting").df()

# Close the connection to the database
con.close()

exit_velocity_data['name'] = exit_velocity_data['player'].str.split(', ').str[1] + ' ' + exit_velocity_data['player'].str.split(', ').str[0].str.strip()
batting['name'] = batting['name'].str.strip()
df = batting.merge(exit_velocity_data, how='inner', left_on=['name', 'Year'], right_on=['name', 'year'])

#total bases
df['TB'] = df['H'] + df['2B'] + 2 * df['3B'] + 3 * df['HR']

#Bat Avg
df['AVG'] = (df['H'] / df['AB']).round(3) #, where=df['AB'] != 0

#on base
numerator_obp = df['H'] + df['BB'] + df['HBP']
denominator_obp = df['AB'] + df['BB'] + df['HBP'] + df['SF']
obp_calculation = numerator_obp / denominator_obp
df['OBP'] = obp_calculation.round(3)# , where=denominator_obp != 0

#slugging
slugging_calculation = df['TB'] / df['AB']
df['SLG'] = slugging_calculation.round(3)# , where=df['AB'] != 0

#on base + slugging 
df['OPS'] = (obp_calculation + slugging_calculation).round(3)

df[['name', 'Year', 'AB', 'H', 'TB', 'AVG', 'OBP', 'SLG', 'OPS']].to_csv('saves/batting_stats_calculations.csv', index=False)

# batting = con.execute("SELECT * FROM batting").df()

stats_to_pivot = ['G', 'PA', 'AB', 'R', 'H', 'HR', 'RBI']

# - index: The column(s) to remain as rows (Player/Name)
# - columns: The column whose values will become the new column names (Year)
# - values: The columns whose values will populate the new wide columns (Selected Stats)
wide_batting_df = batting.pivot_table(
    index=['name'],           # one row per player
    columns='Year',           # Use Year values  as new columns
    values=stats_to_pivot,    # stats to pivot
    aggfunc='sum'             # Use 'sum' to aggregate if a player has multiple entries in a year,
).reset_index()

wide_batting_df['name'] = wide_batting_df['name'].str.strip()
wide_batting_df['name'] = wide_batting_df['name'].str.replace('*', '', regex=False)
wide_batting_df['name'] = wide_batting_df['name'].str.replace('#', '', regex=False)
# if duplicate names exist sum all their stats
wide_batting_df = wide_batting_df.groupby('name', as_index=False).sum(numeric_only=False)

wide_batting_df.to_csv('saves/wide_batting_stats.csv')

print("\nData processing complete. Files saved in 'saves' folder.")