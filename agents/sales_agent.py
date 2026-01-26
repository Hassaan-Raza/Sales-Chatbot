"""
LLM-Powered Sales Agent - Aligned with Client's Database Structure
"""
import os
import json
import re
from datetime import datetime, timedelta
from database.db_connection import db
import requests
from dotenv import load_dotenv
import streamlit as st
load_dotenv()


class SalesAgent:
    """LLM-Powered Sales Intelligence Agent - Dynamic Query Generation"""

    def __init__(self):
        # Initialize Groq
        self.api_key = st.secrets["GROQ_API_KEY"]
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")

        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.model = "llama-3.1-8b-instant"
        self.schema = self._load_schema()

        self.forbidden_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'TRUNCATE', 'REPLACE', 'MERGE', 'GRANT', 'REVOKE'
        ]

    def _call_groq(self, prompt, max_tokens=1000):
        """Call Groq API"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a SQL expert. Generate only valid SQL queries. Return ONLY the SQL query without any explanation or markdown formatting."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": max_tokens,
            "temperature": 0.1
        }

        try:
            response = requests.post(self.api_url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            result = response.json()

            if 'choices' not in result or len(result['choices']) == 0:
                raise Exception(f"Invalid response structure: {result}")

            return result['choices'][0]['message']['content'].strip()

        except requests.exceptions.RequestException as e:
            print(f"Groq API Error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            raise Exception(f"Failed to get response from Groq: {str(e)}")

    def _load_schema(self):
        """Load database schema for LLM context - ALIGNED WITH CLIENT'S STRUCTURE"""
        return """
DATABASE SCHEMA (Client-Specific):

**sales_invoice** - Main invoice table
- invoice_id (PRI), company_id, warehouse_id, customer_id
- invoice_num, invoice_date, duedate, status
- total, subtotal, total_tax, discount, salesman, payment_term
- created_at, finalized_at, paid_at

**sales_items** - Invoice line items
- item_id (PRI), company_id, invoice_id, product_id
- quantity, price, item_cost, discount, discount_amount
- total, subtotal, tax

**stock** - Product stock movements (IMPORTANT for product analytics)
- product_id, company_id, invoice_id, quantity, cost
- stock_type (values: 'sales', 'purchase', etc.)
- quantity is NEGATIVE for sales

**products** - Product catalog
- product_id (PRI), company_id, category_id, name, sku
- price, cost, saleable, product_type

**products_category** - Product categories
- category_id (PRI), title (category name)

**contacts** - Customers
- contact_id (PRI), company_id, company (customer name - NOT 'name'!)
- customer_amount (credit limit), is_active, email, phone, region

**users** - Salespeople
- user_id (PRI), firstname, lastname, company_id

**warehouses** - Branches/Warehouses
- warehouse_id (PRI), company_id, title (warehouse name)

KEY RELATIONSHIPS:
- sales_invoice.customer_id → contacts.contact_id
- sales_invoice.salesman → users.user_id (filter: salesman > 0)
- sales_invoice.warehouse_id → warehouses.warehouse_id
- sales_items.invoice_id → sales_invoice.invoice_id
- sales_items.product_id → products.product_id
- stock.invoice_id → sales_invoice.invoice_id
- products.category_id → products_category.category_id

CRITICAL BUSINESS RULES (Client-Specific):
1. **REVENUE CALCULATION:** Use `total - COALESCE(total_tax, 0)` for net sales
2. **STATUS FILTER:** Use `status NOT IN ('draft', 'draft_return', 'return', 'canceled')`
3. **CUSTOMER NAME:** Use `contacts.company` (NOT contacts.name!)
4. **WAREHOUSE NAME:** Use `warehouses.title`
5. **SALESPERSON:** Use `CONCAT(u.firstname, ' ', u.lastname)` and filter `si.salesman > 0`
6. **CATEGORY NAME:** Use `products_category.title`
7. **DATE FILTERING:** Use CURDATE() and DATE_FORMAT()
"""

    def process_query(self, message, company_id):
        """Main query processor - uses LLM to generate and execute SQL"""
        try:
            date_context = self._extract_date_context(message)
            sql_query = self._generate_sql(message, company_id, date_context)

            if not sql_query:
                return "❌ Could not generate a valid query. Please rephrase your question."

            if not self._is_safe_query(sql_query):
                return "🚫 Safety violation: Query attempted to modify data. Only SELECT queries are allowed."

            result = db.execute_query(sql_query, ())
            formatted_response = self._format_results(message, result, date_context)

            return formatted_response

        except Exception as e:
            print(f"Error in process_query: {e}")
            return f"❌ Error processing query: {str(e)}\n\nPlease try rephrasing your question."

    def _extract_date_context(self, message):
        """Extract date range from natural language"""
        msg = message.lower()
        today = datetime.now().date()

        if 'today' in msg:
            return {'label': 'Today', 'filter': "AND si.invoice_date >= CURDATE() AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"}
        if 'this month' in msg:
            return {'label': 'This Month', 'filter': "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"}
        if 'this year' in msg:
            return {'label': 'This Year', 'filter': "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01') AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"}
        
        return {'label': 'All Time', 'filter': ''}

    def _generate_sql(self, user_question, company_id, date_context):
        """Use LLM to generate SQL query from natural language"""
        user_question_lower = user_question.lower()

        # Handle comparisons with hardcoded queries
        if 'compare' in user_question_lower and 'month' in user_question_lower and 'year' not in user_question_lower:
            return f"""SELECT 
    COALESCE(SUM(CASE WHEN sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled') THEN sales_invoice.total - COALESCE(sales_invoice.total_tax, 0) ELSE 0 END), 0) AS total_sales_this_month,
    COALESCE(SUM(CASE WHEN sales_invoice.invoice_date >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01') AND sales_invoice.invoice_date < DATE_FORMAT(CURDATE(), '%Y-%m-01') AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled') THEN sales_invoice.total - COALESCE(sales_invoice.total_tax, 0) ELSE 0 END), 0) AS total_sales_last_month
FROM sales_invoice WHERE sales_invoice.company_id = {company_id}"""

        # LLM generation for other queries
        prompt = f"""You are a SQL expert. Generate ONLY the SQL query.

{self.schema}

USER QUESTION: "{user_question}"
Company ID: {company_id}
Date Filter: {date_context['filter']}

**CRITICAL RULES:**

1. QUANTITY vs REVENUE Detection:
   - "by value", "by revenue", "by sales", "revenue", "value", "worth", "earnings" → REVENUE QUERY
   - "by quantity", "by units", "quantity", "units", "volume", "pieces" → QUANTITY QUERY
   - Default "top products" alone = QUANTITY

2. Column Names (VERY IMPORTANT):
   - Quantity queries: Use `total_sold_qty` 
   - Revenue queries: Use `total_sales_value` or `total_revenue` (NEVER use total_sold_qty!)
   - Profit queries: Use `total_profit`

3. For QUANTITY products:
```sql
SELECT p.name AS product_name, SUM(ABS(s.quantity)) AS total_sold_qty
FROM stock s
JOIN products p ON s.product_id = p.product_id
JOIN sales_invoice si ON si.invoice_id = s.invoice_id
WHERE s.company_id = {company_id} AND s.quantity < 0 AND s.stock_type = 'sales' AND si.status != 'canceled'
GROUP BY s.product_id, p.name ORDER BY total_sold_qty DESC LIMIT 10
```

4. For REVENUE/VALUE products (MUST join with product_id!):
```sql
SELECT p.name AS product_name, SUM(ABS(s.quantity) * (si_item.price - si_item.discount)) AS total_sales_value
FROM stock s
JOIN products p ON s.product_id = p.product_id
JOIN sales_invoice si ON si.invoice_id = s.invoice_id
JOIN sales_items si_item ON si_item.invoice_id = si.invoice_id AND si_item.product_id = s.product_id
WHERE s.company_id = {company_id} AND s.stock_type = 'sales' AND s.quantity < 0 AND si.status != 'canceled'
GROUP BY s.product_id, p.name ORDER BY total_sales_value DESC LIMIT 10
```

5. For other sales queries:
   - Use `SUM(si.total - COALESCE(si.total_tax, 0))` for revenue
   - Status: `si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')`
   - Customer: `c.company` (NOT c.name)
   - Category: `pc.title` (NOT pc.name)

Generate ONLY the SQL query:"""

        try:
            sql_query = self._call_groq(prompt, max_tokens=600)
            sql_query = re.sub(r'```sql\n?', '', sql_query)
            sql_query = re.sub(r'```\n?', '', sql_query)
            sql_query = sql_query.strip()

            print("="*80)
            print("GENERATED SQL QUERY:")
            print(sql_query)
            print("="*80)

            # Fix common errors
            if "status IN ('paid', 'unpaid', 'remaining')" in sql_query:
                sql_query = sql_query.replace("status IN ('paid', 'unpaid', 'remaining')", "status NOT IN ('draft', 'draft_return', 'return', 'canceled')")
            
            sql_query = re.sub(r'\bc\.name\b', 'c.company', sql_query)
            sql_query = re.sub(r'\bpc\.name\b', 'pc.title', sql_query)

            return sql_query

        except Exception as e:
            print(f"Error generating SQL: {e}")
            return None

    def _is_safe_query(self, sql_query):
        """Verify query is read-only (SELECT only)"""
        sql_upper = sql_query.upper()
        for keyword in self.forbidden_keywords:
            if keyword in sql_upper:
                return False
        if not sql_upper.strip().startswith('SELECT'):
            return False
        if ';' in sql_query[:-1]:
            return False
        return True

    def _format_results(self, user_question, results, date_context):
        """Format query results"""
        if not results:
            return f"ℹ️ No data found.\n\n**Period:** {date_context['label']}"

        # Detect comparison queries
        is_comparison = any(field in str(results[0].keys()) for field in ['total_sales_this_month', 'total_sales_last_month'])
        
        if is_comparison:
            return self._format_comparison(results)
        
        # List vs summary
        if len(results) > 3:
            return self._format_table(user_question, results, date_context)
        else:
            return self._format_summary(user_question, results, date_context)

    def _format_comparison(self, results):
        """Format comparison results"""
        result = results[0]
        if 'total_sales_this_month' in result:
            this_period = float(result['total_sales_this_month'])
            last_period = float(result['total_sales_last_month'])
            period_label = "Month"
        else:
            this_period = float(result['total_sales_this_year'])
            last_period = float(result['total_sales_last_year'])
            period_label = "Year"

        difference = this_period - last_period
        percent_change = (difference / last_period * 100) if last_period > 0 else 0
        trend_emoji = "📈" if difference > 0 else "📉"
        sign = "+" if difference > 0 else ""

        return f"""**📊 SALES COMPARISON - This {period_label} vs Last {period_label}**

**This {period_label}:** ${this_period:,.2f} 💰
**Last {period_label}:** ${last_period:,.2f} 💰

**Difference:** {sign}${abs(difference):,.2f} {trend_emoji}
**Change:** {sign}{percent_change:.1f}% {trend_emoji}"""

    def _format_table(self, question, results, date_context):
        """Format list results"""
        response = f"**📊 {question.upper()}**\n**📅 Period:** {date_context['label']}\n**📈 Found {len(results)} results**\n\n"

        for idx, row in enumerate(results, 1):
            response += f"\n**📦 #{idx}**\n"
            for key, value in row.items():
                if value is None:
                    continue
                formatted_key = key.replace('_', ' ').title()
                
                if isinstance(value, (int, float)):
                    # Check if it's a revenue/value field
                    if any(k in key.lower() for k in ['revenue', 'value', 'sales_value', 'total_sales_value', 'profit']) and 'qty' not in key.lower():
                        response += f"  💰 **{formatted_key}:** ${value:,.2f}\n"
                    # Check if it's a quantity field
                    elif any(k in key.lower() for k in ['qty', 'quantity', 'sold_qty', 'units']):
                        response += f"  📦 **{formatted_key}:** {int(value):,} units\n"
                    else:
                        response += f"  📌 **{formatted_key}:** {value:,.2f}\n"
                else:
                    response += f"  📝 **{formatted_key}:** {value}\n"

        return response

    def _format_summary(self, question, results, date_context):
        """Format summary results"""
        result = results[0]
        response = f"**📊 {question.upper()}**\n**📅 Period:** {date_context['label']}\n\n"
        
        for key, value in result.items():
            if value is None:
                continue
            formatted_key = key.replace('_', ' ').title()
            
            if isinstance(value, (int, float)):
                if any(k in key.lower() for k in ['revenue', 'sales', 'total', 'amount']) and 'qty' not in key.lower():
                    response += f"💰 **{formatted_key}:** ${value:,.2f}\n"
                elif any(k in key.lower() for k in ['count', 'invoices']):
                    response += f"📊 **{formatted_key}:** {int(value):,}\n"
                else:
                    response += f"**{formatted_key}:** {value:,.2f}\n"
            else:
                response += f"**{formatted_key}:** {value}\n"

        return response

    # Compatibility methods
    def get_sales_today(self, company_id, date_range=None):
        return self.process_query("What are my total sales today?", company_id)

    def get_sales_this_month(self, company_id, date_range=None):
        return self.process_query("What are my total sales this month?", company_id)

    def compare_this_month_vs_last_month(self, company_id, date_range=None):
        return self.process_query("Compare sales this month vs last month", company_id)

    def get_top_selling_products(self, company_id, date_range=None):
        return self.process_query("What are my top-selling products?", company_id)

    def get_highest_revenue_customers(self, company_id, date_range=None):
        return self.process_query("Who are my highest revenue customers?", company_id)


if __name__ == "__main__":
    agent = SalesAgent()
    print(agent.process_query("top 10 products by value", 1336))