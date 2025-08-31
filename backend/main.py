import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from backend.db_connector import connect_db
import google.generativeai as genai
from dotenv import load_dotenv

app = FastAPI()

# Updated CORS configuration for deployment
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # For local development
        "https://rishav-agarwwal.github.io",  # Your GitHub Pages domain
        "https://rishavagarwwal.pythonanywhere.com"  # Your backend domain
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Initialize the Google Gemini Client ---
try:
    load_dotenv()
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    if not GEMINI_API_KEY:
        print("⚠️  GEMINI_API_KEY environment variable not set. AI queries will not work.")
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
except Exception as e:
    print(f"❌ Error initializing Gemini client: {e}")
    model = None

# Helper function to get the database schema
def get_db_schema(conn):
    """Gets the schema of relevant tables."""
    cur = conn.cursor()
    tables = ['ar_invoices', 'customers', 'payments']
    schema = ""
    for table in tables:
        cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}';")
        schema += f"\n-- Table: {table}\n"
        for row in cur.fetchall():
            schema += f"-- {row[0]} ({row[1]})\n"
    cur.close()
    return schema

@app.get("/")
def root():
    """Health check endpoint"""
    return {"status": "API is running", "timestamp": datetime.now().isoformat()}

@app.get("/api/analyze")
def analyze_live_data():
    """
    Connects to the DB, runs analysis, and returns data for the dashboard.
    """
    try:
        conn = connect_db()
        
        ar_open_query = "SELECT * FROM ar_invoices WHERE status = 1"
        customers_query = "SELECT customer_id, customer_name FROM customers"
        
        ar_open_df = pd.read_sql(ar_open_query, conn)
        customers_df = pd.read_sql(customers_query, conn)
        conn.close()

        if ar_open_df.empty:
            return { "data": { "dso": 0, "totalOutstanding": 0, "overduePercentage": 0, "topRiskyCustomers": [], "agingBuckets": [], "trends": [] }, "timestamp": datetime.now().isoformat() }

        today = datetime.today().date()
        ar_open_df['due_date'] = pd.to_datetime(ar_open_df['due_date']).dt.date
        ar_open_df['days_overdue'] = (today - ar_open_df['due_date']).apply(lambda x: x.days)

        # --- 1. Calculate KPIs ---
        total_outstanding = ar_open_df['amountall'].sum()
        overdue_invoices = ar_open_df[ar_open_df['days_overdue'] > 0]
        overdue_percentage = (overdue_invoices['amountall'].sum() / total_outstanding) * 100 if total_outstanding > 0 else 0
        dso = ar_open_df['days_overdue'].mean()

        # --- 2. Create Aging Buckets ---
        def bucket(days):
            if days <= 0: return "Current"
            elif days <= 30: return "1-30 days"
            elif days <= 60: return "31-60 days"
            elif days <= 90: return "61-90 days"
            else: return "90+ days"
        ar_open_df["aging_bucket"] = ar_open_df["days_overdue"].apply(bucket)
        aging_summary = ar_open_df.groupby("aging_bucket").agg(amount=("amountall", "sum"), count=("invoice_no", "count")).reset_index().rename(columns={"aging_bucket": "bucket"})

        # --- 3. Identify Top Risky Customers ---
        risky_customers = overdue_invoices.groupby('customer_id')['amountall'].sum().nlargest(3).reset_index()
        risky_customers = risky_customers.merge(customers_df, on='customer_id', how='left')
        avg_days_past_due = overdue_invoices.groupby('customer_id')['days_overdue'].mean().reset_index()
        risky_customers = risky_customers.merge(avg_days_past_due, on='customer_id', how='left')
        risky_customers['riskLevel'] = risky_customers['days_overdue'].apply(lambda x: "High" if x > 60 else "Medium")
        risky_customers = risky_customers.rename(columns={'amountall': 'amount', 'customer_name': 'name', 'days_overdue': 'daysPastDue'})

        # --- 4. Format for Frontend ---
        analysis_payload = {
            "data": {
                "dso": round(dso, 1),
                "totalOutstanding": round(total_outstanding, 2),
                "overduePercentage": round(overdue_percentage, 1),
                "topRiskyCustomers": risky_customers.to_dict(orient='records'),
                "agingBuckets": aging_summary.to_dict(orient='records'),
                "trends": [
                  { "month": "Jan", "dso": 48, "collections": 890000 },
                  { "month": "Feb", "dso": 44, "collections": 920000 },
                  { "month": "Mar", "dso": 45, "collections": 950000 },
                ]
            },
            "timestamp": datetime.now().isoformat()
        }
        return analysis_payload
    
    except Exception as e:
        print(f"❌ Error in analyze_live_data: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection or analysis error: {str(e)}")

@app.post("/api/query")
async def handle_query(request: dict):
    user_question = request.get("question")
    if not user_question or not model:
        raise HTTPException(status_code=400, detail="Question is missing or AI client is not initialized.")

    try:
        conn = connect_db()
        db_schema = get_db_schema(conn)
        
        prompt = f"""
        You are an expert PostgreSQL developer. Based on the database schema below, write a single, safe, read-only SQL query to answer the user's question.
        - Only use the tables and columns provided in the schema.
        - Do not use any DML (INSERT, UPDATE, DELETE) or DDL (CREATE, ALTER, DROP) commands.
        - If the question cannot be answered with a single query, return an error message.

        -- SCHEMA START --
        {db_schema}
        -- SCHEMA END --

        User Question: "{user_question}"

        SQL Query:
        """
        
        sql_response = model.generate_content(prompt)
        generated_sql = sql_response.text.strip().replace("```sql", "").replace("```", "")
        
        # --- NEW: Execute the generated query ---
        query_results_df = pd.read_sql(generated_sql, conn)
        conn.close()
        
        query_results_json = query_results_df.to_dict(orient='records')

        # --- NEW: Summarize the actual results ---
        summary_prompt = f"""
        A user asked: '{user_question}'.
        A SQL query was run and returned the following data in JSON format:
        {str(query_results_json)}

        Please provide a concise, natural language summary of this data that directly answers the user's question.
        """
        summary_response = model.generate_content(summary_prompt)

        return {
            "answer": summary_response.text, 
            "sql_query": generated_sql,
            "data": query_results_json
        }
        
    except Exception as e:
        # Close the connection if it's still open on error
        if 'conn' in locals() and not conn.closed:
            conn.close()
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)