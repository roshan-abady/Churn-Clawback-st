import time
import datetime
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, datediff


# Function to fetch unique values for a given column from Snowflake
def fetch_unique_values(session, column_name, table_name):
    unique_values_df = session.table(table_name).select(col(column_name)).distinct().to_pandas()
    return unique_values_df[column_name].tolist()

# Title and description
st.title("Churn Analysis Visualization 📈")
st.write(
    """This visualization showcases the percentage of customers churning within various timeframes of their billing start date."""
)

# Date filter with default value set to October 1, 2021
selected_date = st.sidebar.date_input("Select a date for analysis", value=datetime.date(2021, 10, 1))

# Create a session
session = Session.builder.configs(st.secrets["snowflake"]).create()

# Define table_name before fetching unique values
table_name = 'OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_COMBINED_ARR_FOR_LEADERBOARDS_LOAD_TABLE'

# Fetch unique values for each filter and add 'All' option
unique_product_groups = ['All'] + fetch_unique_values(session, "PRODUCT_GROUP", table_name)
unique_channels = ['All'] + fetch_unique_values(session, "CHANNEL", table_name)
unique_award_agent_teams = ['All'] + fetch_unique_values(session, "AWARD_AGENT_TEAM_ROLL_UP", table_name)

# Additional filters with 'All' option
selected_product_group = st.sidebar.selectbox("Select Product Group", unique_product_groups)
selected_channel = st.sidebar.selectbox("Select Channel", unique_channels)
selected_award_agent_team = st.sidebar.selectbox("Select Award Agent Team Rollup", unique_award_agent_teams)

# Function to extract and filter data
def extract_data(session, selected_date, selected_product_group, selected_channel, selected_award_agent_team):
    tableName = 'OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_COMBINED_ARR_FOR_LEADERBOARDS_LOAD_TABLE'
    selected_date_str = selected_date.strftime('%Y-%m-%d')
    
    # Start with basic filter for churn flag and date
    dataframe = session.table(tableName).filter(
        (col("CUSTOMER_CHURN_FLAG") == 'Y') & 
        (col("AGREEMENT_BILLING_START_DATE") == selected_date_str) 
    )
    
    # Add additional filters if 'All' is not selected
    if selected_product_group != 'All':
        dataframe = dataframe.filter(col("PRODUCT_GROUP") == selected_product_group)
    if selected_channel != 'All':
        dataframe = dataframe.filter(col("CHANNEL") == selected_channel)
    if selected_award_agent_team != 'All':
        dataframe = dataframe.filter(col("AWARD_AGENT_TEAM_ROLL_UP") == selected_award_agent_team)
    
    dataframe = dataframe.select(
        col("CLIENT_ID"),
        col("AGREEMENT_BILLING_START_DATE"),
        col("REPORTING_DATE_START_OF_MONTH"),
        col("AGREEMENT_EFF_END_DATE"),
        col("CUSTOMER_CHURN_FLAG"),
        col("CHANNEL"),
        col("PRODUCT_GROUP"),
        col("AWARD_AGENT_TEAM_ROLL_UP")
    ).to_pandas()
    
    # Convert columns to appropriate datetime format if they're not already
    dataframe['AGREEMENT_BILLING_START_DATE'] = pd.to_datetime(dataframe['AGREEMENT_BILLING_START_DATE'])
    dataframe['REPORTING_DATE_START_OF_MONTH'] = pd.to_datetime(dataframe['REPORTING_DATE_START_OF_MONTH'])
    dataframe['AGREEMENT_EFF_END_DATE'] = pd.to_datetime(dataframe['AGREEMENT_EFF_END_DATE'])
    
    return dataframe

def analyze_churn(dataframe, month, date_col):
    dataframe[date_col] = pd.to_datetime(dataframe[date_col])
    
    dataframe['days_to_churn'] = (dataframe[date_col] - dataframe['AGREEMENT_BILLING_START_DATE']).dt.days
    
    total_count = len(dataframe)
    if total_count == 0:
        return np.array([[month, 0]])

    filtered_count = len(dataframe[dataframe['days_to_churn'] <= (30 * month)])
    churn_rate = filtered_count / total_count
    return np.array([[month, churn_rate]])


# Extract data
dataframe = extract_data(session, selected_date, selected_product_group, selected_channel, selected_award_agent_team)

# Setup for animation
progress_bar = st.progress(0)
status_text = st.empty()
churn_month = 1
initial_data = analyze_churn(dataframe, churn_month, 'AGREEMENT_EFF_END_DATE')
initial_data_reporting = analyze_churn(dataframe, churn_month, 'REPORTING_DATE_START_OF_MONTH')

df_billing_start = pd.DataFrame(initial_data, columns=["Months", "Churn Rate"])
df_reporting_start = pd.DataFrame(initial_data_reporting, columns=["Months", "Churn Rate"])

fig = go.Figure()
fig.add_trace(go.Scatter(x=df_billing_start['Months'], y=df_billing_start['Churn Rate'], mode='lines+markers', name='Billing Start Date'))
fig.add_trace(go.Scatter(x=df_reporting_start['Months'], y=df_reporting_start['Churn Rate'], mode='lines+markers', name='Reporting Date Start of Month'))

fig.update_layout(yaxis_tickformat = '%')
chart = st.plotly_chart(fig)

# Animate churn analysis
for month in range(2, 13):
    new_data = analyze_churn(dataframe, month, 'AGREEMENT_EFF_END_DATE')
    new_data_reporting = analyze_churn(dataframe, month, 'REPORTING_DATE_START_OF_MONTH')

    df_billing_start = pd.concat([df_billing_start, pd.DataFrame(new_data, columns=["Months", "Churn Rate"])])
    df_reporting_start = pd.concat([df_reporting_start, pd.DataFrame(new_data_reporting, columns=["Months", "Churn Rate"])])

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_billing_start['Months'], y=df_billing_start['Churn Rate'], mode='lines+markers', name='Billing Start Date'))
    fig.add_trace(go.Scatter(x=df_reporting_start['Months'], y=df_reporting_start['Churn Rate'], mode='lines+markers', name='Reporting Date Start of Month'))

    fig.update_layout(yaxis_tickformat = '%')

    fig.update_layout(
    autosize=True,
    yaxis_tickformat = '.0%',
    xaxis_title="(#) Months in agreement",
    yaxis_title="Churn Rate (%)",
    xaxis_showline=True,
    yaxis_showline=True,
    xaxis_linewidth=2,
    yaxis_linewidth=2,
    title="Accumulated Churn Rate Over Time",
    legend_title="Legend",
    yaxis=dict(
        range=[0, 1]  # Set y-axis range to [0, 1]
        ),
    xaxis=dict(
        dtick=1  # Set x-axis tick interval to 1
        )
    )
    status_text.text(f"{int((month / 12) * 100)}% Complete")
    chart.plotly_chart(fig)
    progress_bar.progress(int((month / 12) * 100))
    time.sleep(0.05)

status_text.text("Analysis Completed!")
st.write(f'Churn Rate for agreements with billing starting on {selected_date}.') 
st.button("Re-run")

# Display raw data
st.subheader("Raw Data")
st.dataframe(dataframe)
