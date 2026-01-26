"""
LLM-Powered Sales Agent - Aligned with Client's Database Structure
"""

import os
import json
import re
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import streamlit as st

from database.db_connection import db

load_dotenv()


class SalesAgent:
    """LLM-Powered Sales Intelligence Agent - Dynamic Query Generation"""

    def __init__(self):
        self.api_key = st.secrets.get("GROQ_API_KEY")

        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found")

        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.model = "llama-3.1-8b-instant"
        self.schema = self._load_schema()

        self.forbidden_keywords = [
            "INSERT", "UPDATE", "DELETE", "DROP", "CREATE",
            "ALTER", "TRUNCATE", "REPLACE", "MERGE",
            "GRANT", "REVOKE"
        ]

    # ------------------------------------------------------------------
    # GROQ API
    # ------------------------------------------------------------------

    def _call_groq(self, prompt, max_tokens=1000):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a SQL expert. "
                        "Return ONLY a valid SQL SELECT query. "
                        "No markdown. No explanation."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
            "max_tokens": max_tokens
        }

        response = requests.post(
            self.api_url,
            headers=headers,
            json=payload,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        return data["choices"][0]["message"]["content"].strip()

    # ------------------------------------------------------------------
    # DATABASE SCHEMA
    # ------------------------------------------------------------------

    def _load_schema(self):
        return """
DATABASE SCHEMA (Client-Specific)

sales_invoice(invoice_id, company_id, warehouse_id, customer_id,
invoice_num, invoice_date, duedate, status,
total, subtotal, total_tax, discount, salesman)

sales_items(invoice_id, product_id, quantity, price, discount)

stock(product_id, company_id, invoice_id, quantity, cost, stock_type)

products(product_id, company_id, category_id, name)

products_category(category_id, title)

contacts(contact_id, company_id, company)

users(user_id, firstname, lastname)

warehouses(warehouse_id, title)
"""

    # ------------------------------------------------------------------
    # MAIN ENTRY
    # ------------------------------------------------------------------

    def process_query(self, message, company_id):
        try:
            date_context = self._extract_date_context(message)
            sql = self._generate_sql(message, company_id, date_context)

            if not sql:
                return "❌ Could not generate SQL."

            if not self._is_safe_query(sql):
                return "🚫 Only SELECT queries are allowed."

            result = db.execute_query(sql, ())
            return self._format_results(message, result, date_context)

        except Exception as e:
            return f"❌ Error: {str(e)}"

    # ------------------------------------------------------------------
    # DATE EXTRACTION
    # ------------------------------------------------------------------

    def _extract_date_context(self, message):
        msg = message.lower()
        today = datetime.now().date()

        if "today" in msg:
            return {
                "label": "Today",
                "filter": (
                    "AND si.invoice_date >= CURDATE() "
                    "AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
                )
            }

        if "this month" in msg:
            return {
                "label": "This Month",
                "filter": (
                    "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01') "
                    "AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
                )
            }

        if "this year" in msg:
            return {
                "label": "This Year",
                "filter": (
                    "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01') "
                    "AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
                )
            }

        return {"label": "All Time", "filter": ""}

    # ------------------------------------------------------------------
    # SQL GENERATION
    # ------------------------------------------------------------------

    def _generate_sql(self, user_question, company_id, date_context):
        user_q = user_question.lower()

        if "compare" in user_q and "month" in user_q:
            return f"""
SELECT
    COALESCE(SUM(CASE
        WHEN invoice_date >= DATE_FORMAT(CURDATE(),'%Y-%m-01')
        THEN total - COALESCE(total_tax,0)
    END),0) AS this_month,
    COALESCE(SUM(CASE
        WHEN invoice_date >= DATE_FORMAT(CURDATE()-INTERVAL 1 MONTH,'%Y-%m-01')
         AND invoice_date < DATE_FORMAT(CURDATE(),'%Y-%m-01')
        THEN total - COALESCE(total_tax,0)
    END),0) AS last_month
FROM sales_invoice
WHERE company_id = {company_id}
AND status NOT IN ('draft','draft_return','return','canceled')
""".strip()

        prompt = f"""
{self.schema}

User Question: "{user_question}"

Rules:
- Always filter company_id = {company_id}
- Revenue = total - COALESCE(total_tax,0)
- status NOT IN ('draft','draft_return','return','canceled')
- Apply date filter:
{date_context['filter']}

Generate ONLY SQL:
"""

        try:
            sql = self._call_groq(prompt, 600)
            sql = re.sub(r"```sql|```", "", sql).strip()
            return sql
        except Exception:
            return None

    # ------------------------------------------------------------------
    # SAFETY
    # ------------------------------------------------------------------

    def _is_safe_query(self, sql):
        sql_upper = sql.upper()

        if not sql_upper.startswith("SELECT"):
            return False

        for keyword in self.forbidden_keywords:
            if keyword in sql_upper:
                return False

        return True

    # ------------------------------------------------------------------
    # FORMATTING
    # ------------------------------------------------------------------

    def _format_results(self, question, results, date_context):
        if not results:
            return f"No data found ({date_context['label']})"

        return json.dumps(results, indent=2, default=str)

    # ------------------------------------------------------------------
    # BASIC FALLBACK FORMATTER
    # ------------------------------------------------------------------

    def _basic_format_results(self, results, date_context):
        """Fallback basic formatting"""
        return json.dumps(results, indent=2, default=str)


# ----------------------------------------------------------------------
# RUN TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    agent = SalesAgent()
    print(agent.process_query("What are my sales this month?", 922))
