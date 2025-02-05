#!/usr/bin/env python
# coding: utf-8

# In[7]:


import streamlit as st
import pyodbc
import pandas as pd
import google.generativeai as genai
import matplotlib.pyplot as plt
from io import BytesIO

# Configure Google Gemini API Key
genai.configure(api_key="AIzaSyBl9bHsi7jRqeY5or__wJyDAFZPXrufuZk")

# Function to load Google Gemini Model and provide queries as a response
def get_gemini_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    return response.text.strip()

# Function to validate SQL queries before execution
def validate_sql_query(sql):
    forbidden_keywords = ["DROP", "DELETE", "TRUNCATE", "ALTER"]
    for word in forbidden_keywords:
        if word in sql.upper():
            return False
    return True

# Function to retrieve and format query results from the SQL Server database
def read_sql_query(sql, conn_str):
    try:
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        return f"Error executing SQL query: {e}"

# Function to refine LLM output (show summary statistics)
def format_llm_response(df):
    summary_stats = df.describe().to_string()
    model = genai.GenerativeModel('gemini-pro')
    prompt = f"""
    Here is the summary of the retrieved data:
    {summary_stats}

    Please provide key insights.
    """
    response = model.generate_content(prompt)
    return response.text.strip()

# Define Your Prompt with SQL Examples
prompt = [
    """
            You are an expert in converting English questions to efficient SQL queries!
    The SQL database has the following columns in the `MED1` table:
    - Rndrng_NPI (int, National Provider Identifier)
    - Rndrng_Prvdr_Last_Org_Name (nvarchar, Last Name/Organization Name)
    - Rndrng_Prvdr_First_Name (nvarchar, First Name)
    - Rndrng_Prvdr_MI (nvarchar, Middle Initial)
    - Rndrng_Prvdr_Crdntls (nvarchar, Credentials)
    - Rndrng_Prvdr_Gndr (nvarchar, Gender)
    - Rndrng_Prvdr_Ent_Cd (nvarchar, Entity Code)
    - Rndrng_Prvdr_St1 (nvarchar, Street Address 1)
    - Rndrng_Prvdr_St2 (nvarchar, Street Address 2)
    - Rndrng_Prvdr_City (nvarchar, City)
    - Rndrng_Prvdr_State_Abrvtn (nvarchar, State Abbreviation)
    - Rndrng_Prvdr_State_FIPS (nvarchar, State FIPS Code)
    - Rndrng_Prvdr_Zip5 (nvarchar, Zip Code)
    - Rndrng_Prvdr_RUCA (float, Rural-Urban Commuting Area Code)
    - Rndrng_Prvdr_RUCA_Desc (nvarchar, RUCA Description)
    - Rndrng_Prvdr_Cntry (nvarchar, Country)
    - Rndrng_Prvdr_Type (nvarchar, Provider Type)
    - Rndrng_Prvdr_Mdcr_Prtcptg_Ind (nvarchar, Medicare Participating Indicator)
    - HCPCS_Cd (nvarchar, HCPCS Code)
    - HCPCS_Desc (nvarchar, HCPCS Description)
    - HCPCS_Drug_Ind (nvarchar, HCPCS Drug Indicator)
    - Place_Of_Srvc (nvarchar, Place of Service)
    - Tot_Benes (int, Total Beneficiaries)
    - Tot_Srvcs (float, Total Services)
    - Tot_Bene_Day_Srvcs (int, Total Beneficiary Day Services)
    - Avg_Sbmtd_Chrg (float, Average Submitted Charge)
    - Avg_Mdcr_Alowd_Amt (float, Average Medicare Allowed Amount)
    - Avg_Mdcr_Pymt_Amt (float, Average Medicare Payment Amount)
    - Avg_Mdcr_Stdzd_Amt (float, Average Medicare Standardized Amount)

    **Guidelines for Generating Efficient SQL Queries:**

    1. **Minimize Data Retrieval:** Only select the columns necessary for the query. Avoid `SELECT *` unless absolutely required.
    2. **Use WHERE Clause Effectively:** Filter data as early as possible using the `WHERE` clause to reduce the amount of data processed.  Use appropriate operators (=, <, >, <=, >=, BETWEEN, LIKE, IN) and combine conditions with AND/OR.
    3. **Optimize Joins:** If joining tables, use appropriate join types (INNER JOIN, LEFT JOIN, RIGHT JOIN) and ensure that join conditions are indexed.  Avoid Cartesian joins (implicit joins without a join condition).  (This prompt assumes queries are against a single table, but this is important for future expansion).
    4. **Use Indexes:**  The database has indexes on key columns.  Use these indexed columns in your `WHERE` clauses and `JOIN` conditions for faster lookups.  (This is an instruction to the LLM, not something you write in the SQL).
    5. **Avoid Functions in WHERE Clause:**  Applying functions to columns in the `WHERE` clause can prevent the database from using indexes.  Try to perform calculations *after* filtering the data.
    6. **Use Appropriate Data Types:**  Use the correct data types in your queries.  For example, compare numeric columns with numbers, not strings.
    7. **Consider `EXISTS` instead of `COUNT`:** If you only need to check if a record exists, use `EXISTS` instead of `COUNT(*) > 0`. `EXISTS` can be faster as it stops searching as soon as a match is found.
    8. **Use `TOP` or `LIMIT` for Top-N Queries:**  Use `TOP` (SQL Server) or `LIMIT` (other databases) to retrieve only the top N results.  This is especially useful for ranking and sorting.
    9. **Use `GROUP BY` and Aggregations Efficiently:**  Use `GROUP BY` to group data and aggregate functions (SUM, AVG, COUNT, MIN, MAX) to calculate statistics.  Filter groups using the `HAVING` clause.
    10. **Avoid Subqueries Where Possible:**  Subqueries can sometimes be inefficient.  Consider using joins or other techniques to achieve the same result.  (However, sometimes subqueries are the most clear and maintainable solution, so use your judgment).
    11. **Use Window Functions (When Appropriate):** Window functions (e.g., `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`) can be very efficient for calculations that involve related rows within a partition.

   Example 1:
    Input: "What is the total Medicare payment for each provider type in New York?"
    Output:
    "SELECT Rndrng_Prvdr_Type AS Provider_Type, SUM(Avg_Mdcr_Pymt_Amt) AS Total_Medicare_Payment
    FROM MED1
    WHERE Rndrng_Prvdr_State_Abrvtn = 'NY'
    GROUP BY Rndrng_Prvdr_Type
    ORDER BY Total_Medicare_Payment DESC"

    Example 2:
    Input: "List the top 5 providers in terms of Medicare payments in Florida."
    Output:
    "SELECT TOP 5 Rndrng_NPI AS Provider_NPI, Rndrng_Prvdr_Type AS Provider_Type, SUM(Avg_Mdcr_Pymt_Amt) AS Total_Payment
    FROM MED1 
    WHERE Rndrng_Prvdr_State_Abrvtn = 'FL'
    GROUP BY Rndrng_NPI, Rndrng_Prvdr_Type
    ORDER BY Total_Payment DESC"

    Example 3:
    Input: "Which state had the highest average submitted charge for services, and what was the amount?"
    Output:
    "SELECT TOP 1 Rndrng_Prvdr_State_Abrvtn AS State, AVG(Avg_Sbmtd_Chrg) AS Avg_Submitted_Charge
    FROM MED1 
    GROUP BY Rndrng_Prvdr_State_Abrvtn
    ORDER BY Avg_Submitted_Charge DESC"

    Example 4:
    Input: "Classify providers in Texas based on their total Medicare payment amount as 'Low', 'Medium', or 'High'."
    Output:
    "SELECT Rndrng_NPI AS Provider_NPI, Rndrng_Prvdr_Type AS Provider_Type,
        SUM(Avg_Mdcr_Pymt_Amt) AS Total_Payment,
        CASE
            WHEN SUM(Avg_Mdcr_Pymt_Amt) < 50000 THEN 'Low'
            WHEN SUM(Avg_Mdcr_Pymt_Amt) BETWEEN 50000 AND 200000 THEN 'Medium'
            ELSE 'High'
        END AS Payment_Category
    FROM MED1
    WHERE Rndrng_Prvdr_State_Abrvtn = 'TX'
    GROUP BY Rndrng_NPI, Rndrng_Prvdr_Type
    ORDER BY Total_Payment DESC"

    Example 5:
    Input: "Find the Top 3 Most Commonly Billed HCPCS Codes Per State."
    Output:
    "SELECT TOP 3 Rndrng_Prvdr_State_Abrvtn AS State,
    HCPCS_Cd AS HCPCS_Code,
    SUM(Tot_Srvcs) AS Total_Services
    FROM MED1
    GROUP BY Rndrng_Prvdr_State_Abrvtn, HCPCS_Cd
    ORDER BY Rndrng_Prvdr_State_Abrvtn, Total_Services DESC"  # Removed redundant PARTITION BY.  ORDER BY already handles this

    Example 6:  # Added example using more columns
    Input: "What is the average Medicare payment amount for each provider type in California, broken down by gender?"
    Output:
    "SELECT Rndrng_Prvdr_Type, Rndrng_Prvdr_Gndr, AVG(Avg_Mdcr_Pymt_Amt) AS Average_Payment
    FROM MED1
    WHERE Rndrng_Prvdr_State_Abrvtn = 'CA'
    GROUP BY Rndrng_Prvdr_Type, Rndrng_Prvdr_Gndr
    ORDER BY Rndrng_Prvdr_Type, Rndrng_Prvdr_Gndr"


    Now, when you receive an English question, convert it into the corresponding *efficient* SQL query without using \`\`\` or the word "SQL" in the output.  Adhere to the guidelines above to generate optimized queries.
    """
]

# Streamlit App
st.set_page_config(page_title="SQL Query & LLM Explanation", layout="wide")
st.header("MED-BOT")

# Initialize session state if not already done
if "history" not in st.session_state:
    st.session_state.history = []

question = st.text_input("Enter your question:", key="input")
submit = st.button("Fetch Data")

# SQL Server Connection String
conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=192.168.1.25\SQLSERVER2017;'
    r'DATABASE=MED_Database;'
    r'UID=MED_Login;'
    r'PWD=Test@321;'
)

# If submit is clicked
if submit:
    with st.spinner("Generating SQL query..."):
        sql_query = get_gemini_response(question, prompt)

    # Validate SQL Query
    if validate_sql_query(sql_query):
        with st.spinner("Fetching data from database..."):
            data = read_sql_query(sql_query, conn_str)

        if isinstance(data, pd.DataFrame) and not data.empty:
            st.subheader("Query Results:")
            st.dataframe(data)  # Displaying results as a table

            # Generate AI-powered insights
            with st.spinner("Generating insights..."):
                llm_response = format_llm_response(data)

            st.subheader("Summary:")
            st.write(llm_response)

            # Save current interaction in history
            st.session_state.history.append({
                "question": question,
                "sql_query": sql_query,
                "data": data,
                "summary": llm_response
            })

            # Show generated SQL query after the refined output
            st.subheader("SQL Query:")
            st.code(sql_query, language="sql")

            # # Chart selection and visualization
            # chart_type = st.selectbox("Choose Chart Type", ["None", "Bar Chart", "Line Chart", "Pie Chart"])
            # if chart_type != "None":
            #     if chart_type == "Bar Chart":
            #         st.bar_chart(data.set_index(data.columns[0]))

            #     elif chart_type == "Line Chart":
            #         st.line_chart(data.set_index(data.columns[0]))

            #     elif chart_type == "Pie Chart":
            #         fig, ax = plt.subplots()
            #         ax.pie(data.iloc[:, 1], labels=data.iloc[:, 0], autopct="%1.1f%%")
            #         st.pyplot(fig)

            # Export options (CSV, Excel) aligned to the right side
            col1, col2 = st.columns([8, 2])  # Adjusted column layout for download buttons

            with col2:
                csv = data.to_csv(index=False).encode('utf-8')
                st.download_button(label="Download CSV", data=csv, file_name="query_results.csv", mime="text/csv")

                # Export Excel using BytesIO with openpyxl engine
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    data.to_excel(writer, index=False, sheet_name='Query Results')
                excel_buffer.seek(0)
                st.download_button(label="Download Excel", data=excel_buffer, file_name="query_results.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        else:
            st.error("No data found or error in query.")
    else:
        st.error("Invalid SQL query detected! Please refine your input.")

# Display query history in the sidebar
st.sidebar.subheader("Query History")
if len(st.session_state.history) > 0:
    for idx, entry in enumerate(reversed(st.session_state.history), start=1):
        with st.sidebar.expander(f"Query {len(st.session_state.history) - idx + 1}: {entry['question']}"):
            st.markdown(f"**SQL Query:** {entry['sql_query']}")
            st.markdown(f"**AI Summary:** {entry['summary']}")
else:
    st.sidebar.markdown("No history available.")




# In[ ]:




