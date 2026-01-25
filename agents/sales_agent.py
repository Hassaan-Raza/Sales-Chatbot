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

**sales_payments** - Payment records
- payment_id (PRI), company_id, payment_date, amount

**sales_payment_items** - Payment allocations to invoices
- payment_id, invoice_id, amount

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

**credit_notes** - Sales returns
- note_id (PRI), company_id, invoice_date, total

**sale_order** - Sales orders
- order_id (PRI), company_id, customer_id, status, order_date

**advances** - Customer advance payments
- advance_id (PRI), company_id, contact_id, amount, remaining_amount, status

**origins** - Regions/Cities
- id (PRI), company_id, title (region name)

KEY RELATIONSHIPS:
- sales_invoice.customer_id → contacts.contact_id
- sales_invoice.salesman → users.user_id (filter: salesman > 0)
- sales_invoice.warehouse_id → warehouses.warehouse_id
- sales_items.invoice_id → sales_invoice.invoice_id
- sales_items.product_id → products.product_id
- stock.invoice_id → sales_invoice.invoice_id
- products.category_id → products_category.category_id
- contacts.region → origins.id

CRITICAL BUSINESS RULES (Client-Specific):
1. **REVENUE CALCULATION:** Use `total - COALESCE(total_tax, 0)` for net sales (NOT just total!)
2. **STATUS FILTER:** Use `status NOT IN ('draft', 'draft_return', 'return', 'canceled')` for valid sales
   - Valid statuses are: 'paid', 'unpaid', 'remaining' (and other non-draft statuses)
3. **CUSTOMER NAME:** Use `contacts.company` (NOT contacts.name!)
4. **WAREHOUSE NAME:** Use `warehouses.title` for warehouse/branch names
5. **SALESPERSON:** Use `CONCAT(u.firstname, ' ', u.lastname)` and filter `si.salesman > 0`
6. **PRODUCT ANALYTICS:** Use `stock` table with:
   - `stock_type = 'sales'`
   - `quantity < 0` (sales are negative)
   - `ABS(quantity)` for actual sold quantity
   - Join with sales_invoice to filter canceled: `si.status != 'canceled'`
7. **CATEGORY NAME:** Use `products_category.title` (NOT products_category.name!)
8. **DATE FILTERING:** Use CURDATE() and DATE_FORMAT() for date ranges
9. Always filter by company_id for data isolation
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
            return {
                'label': 'Today',
                'start_date': today.strftime('%Y-%m-%d'),
                'end_date': today.strftime('%Y-%m-%d'),
                'filter': f"AND si.invoice_date >= CURDATE() AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
            }

        if 'yesterday' in msg:
            yesterday = today - timedelta(days=1)
            return {
                'label': 'Yesterday',
                'start_date': yesterday.strftime('%Y-%m-%d'),
                'end_date': yesterday.strftime('%Y-%m-%d'),
                'filter': f"AND si.invoice_date = '{yesterday.strftime('%Y-%m-%d')}'"
            }

        if 'this month' in msg:
            return {
                'label': 'This Month',
                'start_date': None,
                'end_date': None,
                'filter': "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
            }

        if 'last month' in msg:
            return {
                'label': 'Last Month',
                'start_date': None,
                'end_date': None,
                'filter': "AND si.invoice_date >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01') AND si.invoice_date < DATE_FORMAT(CURDATE(), '%Y-%m-01')"
            }

        if 'this year' in msg:
            return {
                'label': 'This Year',
                'start_date': None,
                'end_date': None,
                'filter': "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01') AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
            }

        if 'last year' in msg:
            return {
                'label': 'Last Year',
                'start_date': None,
                'end_date': None,
                'filter': "AND si.invoice_date >= DATE_FORMAT(CURDATE() - INTERVAL 1 YEAR, '%Y-01-01') AND si.invoice_date < DATE_FORMAT(CURDATE(), '%Y-01-01')"
            }

        if 'last 90 days' in msg or 'last 3 months' in msg:
            return {
                'label': 'Last 90 Days',
                'start_date': None,
                'end_date': None,
                'filter': "AND si.invoice_date >= CURDATE() - INTERVAL 90 DAY"
            }

        if 'last 12 months' in msg or 'last year trend' in msg:
            return {
                'label': 'Last 12 Months',
                'start_date': None,
                'end_date': None,
                'filter': "AND si.invoice_date >= DATE_FORMAT(CURDATE() - INTERVAL 11 MONTH, '%Y-%m-01')"
            }

        return {
            'label': 'All Time',
            'start_date': None,
            'end_date': None,
            'filter': ''
        }

    def _generate_sql(self, user_question, company_id, date_context):
        """Use LLM to generate SQL query from natural language"""

        prompt = f"""You are a SQL expert for a sales analytics system. Generate a READ-ONLY SQL query based on the user's question.

{self.schema}

USER QUESTION: {user_question}

CONTEXT:
- Company ID: {company_id}
- Date Range: {date_context['label']}
- Date Filter: {date_context['filter']}

CRITICAL REQUIREMENTS - MUST FOLLOW CLIENT'S PATTERNS:

1. **REVENUE CALCULATION:** Always use `SUM(si.total - COALESCE(si.total_tax, 0))` for net sales
2. **STATUS FILTER:** Always use `si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')`
3. **CUSTOMER NAME:** Use `c.company` (NOT c.name!)
4. **WAREHOUSE NAME:** Use `w.title` for warehouse names
5. **SALESPERSON:** Use `CONCAT(u.firstname, ' ', u.lastname)` and filter `si.salesman > 0`
6. **CATEGORY NAME:** Use `c.title` or `pc.title` for category (NOT name!)
7. **PRODUCT ANALYTICS:** For product quantity/sales, use:
   ```
   FROM stock s
   JOIN products p ON s.product_id = p.product_id
   JOIN sales_invoice si ON si.invoice_id = s.invoice_id
   WHERE s.company_id = {company_id}
     AND s.quantity < 0
     AND s.stock_type = 'sales'
     AND si.status != 'canceled'
   ```
   Then use `SUM(ABS(s.quantity))` for total quantity sold

8. **DATE FILTERING:** Use client's pattern with CURDATE() and DATE_FORMAT()
9. Always include `WHERE [table].company_id = {company_id}`
10. Use LEFT JOIN for optional relationships
11. LIMIT 10 for list queries
12. Return ONLY the SQL query - no explanations

EXAMPLES (Client's Actual Patterns):

Q: "What are my total sales today?"
A: SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_sales FROM sales_invoice WHERE sales_invoice.company_id = {company_id} AND sales_invoice.invoice_date >= CURDATE() AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')

Q: "What are my total sales this month?"
A: SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_sales FROM sales_invoice WHERE sales_invoice.company_id = {company_id} AND sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')

Q: "Who are my highest revenue customers?"
A: SELECT c.company AS customer_name, SUM(si.total - COALESCE(si.total_tax, 0)) AS total_revenue FROM sales_invoice si JOIN contacts c ON c.contact_id = si.customer_id WHERE si.company_id = {company_id} AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled') GROUP BY si.customer_id, c.company ORDER BY total_revenue DESC LIMIT 10

Q: "What are my top-selling products?"
A: SELECT p.name AS product_name, SUM(ABS(s.quantity)) AS total_sold_qty FROM stock s JOIN products p ON s.product_id = p.product_id JOIN sales_invoice si ON si.invoice_id = s.invoice_id WHERE s.company_id = {company_id} AND s.quantity < 0 AND s.stock_type = 'sales' AND si.status != 'canceled' GROUP BY s.product_id, p.name ORDER BY total_sold_qty DESC LIMIT 10

Q: "Which branch has highest sales?"
A: SELECT w.title AS branch_name, SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales FROM sales_invoice si JOIN warehouses w ON si.warehouse_id = w.warehouse_id WHERE si.company_id = {company_id} AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled') GROUP BY si.warehouse_id, w.title ORDER BY total_sales DESC LIMIT 1

Q: "Show sales by salesperson"
A: SELECT CONCAT(u.firstname, ' ', u.lastname) AS salesperson_name, SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales FROM sales_invoice si LEFT JOIN users u ON si.salesman = u.user_id WHERE si.company_id = {company_id} AND si.salesman > 0 AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled') GROUP BY si.salesman, u.firstname, u.lastname ORDER BY total_sales DESC

Q: "Which category has highest sales?"
A: SELECT pc.title AS category_name, SUM(ABS(s.quantity)) AS total_sold_qty FROM stock s JOIN products p ON p.product_id = s.product_id JOIN products_category pc ON pc.category_id = p.category_id JOIN sales_invoice si ON si.invoice_id = s.invoice_id WHERE s.company_id = {company_id} AND s.stock_type = 'sales' AND s.quantity < 0 AND si.status != 'canceled' GROUP BY pc.category_id, pc.title ORDER BY total_sold_qty DESC LIMIT 1

Generate ONLY the SQL query following these exact patterns:"""

        try:
            sql_query = self._call_groq(prompt, max_tokens=600)
            sql_query = re.sub(r'```sql\n?', '', sql_query)
            sql_query = re.sub(r'```\n?', '', sql_query)
            sql_query = sql_query.strip()

            print("="*80)
            print("GENERATED SQL QUERY:")
            print(sql_query)
            print("="*80)

            sql_query = self._fix_common_sql_errors(sql_query)
            return sql_query

        except Exception as e:
            print(f"Error generating SQL: {e}")
            return None

    def _fix_common_sql_errors(self, sql_query):
        """Fix common SQL generation errors - client-aligned"""
        original = sql_query

        # Fix 1: Replace incorrect status filters
        if "status IN ('paid', 'unpaid', 'remaining')" in sql_query:
            print("⚠️ Fixing status filter to match client's pattern...")
            sql_query = sql_query.replace(
                "status IN ('paid', 'unpaid', 'remaining')",
                "status NOT IN ('draft', 'draft_return', 'return', 'canceled')"
            )

        # Fix 2: Replace contacts.name with contacts.company
        if 'c.name' in sql_query and 'contacts c' in sql_query:
            print("⚠️ Fixing customer name field (c.name → c.company)...")
            sql_query = re.sub(r'\bc\.name\b', 'c.company', sql_query)

        # Fix 3: Replace category.name with category.title
        if 'category_id' in sql_query.lower():
            sql_query = re.sub(r'\bc\.name\b(?=.*category)', 'c.title', sql_query)
            sql_query = re.sub(r'\bpc\.name\b', 'pc.title', sql_query)
            sql_query = re.sub(r'products_category\.name', 'products_category.title', sql_query)

        # Fix 4: Clean up extra commas
        sql_query = re.sub(r',\s*,', ',', sql_query)
        sql_query = re.sub(r',\s*(FROM|WHERE|GROUP|ORDER|LIMIT)', r' \1', sql_query)
        sql_query = re.sub(r'SELECT\s+,', 'SELECT ', sql_query)

        if sql_query != original:
            print(f"✅ SQL query was automatically fixed to match client patterns")
            print("="*80)
            print("FIXED SQL QUERY:")
            print(sql_query)
            print("="*80)

        return sql_query

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
        """Format query results - shows complete data for lists"""

        if not results:
            return f"ℹ️ No data found for your query.\n\n**Period:** {date_context['label']}"

        # Detect query type
        is_list_query = len(results) > 3
        is_summary_query = len(results) == 1 and len(results[0]) <= 5

        # For list queries with many results, use table format instead of LLM
        if is_list_query:
            return self._format_table_results(user_question, results, date_context)

        # For summaries, use LLM formatting
        return self._format_with_llm(user_question, results, date_context)

    def _format_table_results(self, user_question, results, date_context):
        """Format list results as a clean table - shows ALL records"""

        response = f"**📊 {user_question.upper()}**\n"
        response += f"**📅 Period:** {date_context['label']}\n"
        response += f"**📈 Found {len(results)} results**\n\n"

        # Add appropriate emoji based on content
        if 'customer' in user_question.lower():
            emoji = "👤"
        elif 'product' in user_question.lower():
            emoji = "📦"
        elif 'invoice' in user_question.lower():
            emoji = "📄"
        elif 'sales' in user_question.lower() or 'branch' in user_question.lower():
            emoji = "💰"
        elif 'salesperson' in user_question.lower():
            emoji = "👨‍💼"
        else:
            emoji = "📊"

        # Format each result
        for idx, row in enumerate(results, 1):
            response += f"\n**{emoji} #{idx}**\n"

            for key, value in row.items():
                formatted_key = key.replace('_', ' ').title()

                # Skip showing None/NULL values prominently
                if value is None:
                    continue

                # Format based on field type
                if isinstance(value, (int, float)):
                    if any(k in key.lower() for k in ['revenue', 'amount', 'total', 'sales', 'price', 'cost', 'profit', 'value']):
                        response += f"  💰 **{formatted_key}:** ${value:,.2f}\n"
                    elif any(k in key.lower() for k in ['count', 'quantity', 'invoices', 'orders', 'units', 'qty']):
                        response += f"  📊 **{formatted_key}:** {int(value):,}\n"
                    elif 'percent' in key.lower() or 'margin' in key.lower():
                        response += f"  📈 **{formatted_key}:** {value:.2f}%\n"
                    elif 'days' in key.lower():
                        response += f"  ⏰ **{formatted_key}:** {int(value)} days\n"
                    else:
                        response += f"  📌 **{formatted_key}:** {value:,.2f}\n"
                elif 'date' in key.lower():
                    response += f"  📅 **{formatted_key}:** {value}\n"
                else:
                    # Text fields (names, IDs, etc.)
                    response += f"  📝 **{formatted_key}:** {value}\n"

        return response

    def _format_with_llm(self, user_question, results, date_context):
        """Use LLM for formatting summary queries only"""

        results_json = json.dumps(results, default=str, indent=2)
        available_fields = set()
        if results:
            for row in results:
                available_fields.update(row.keys())
        field_list = ", ".join(available_fields)

        prompt = f"""You are a sales analytics assistant. Format this summary data into a clear, concise report.

USER QUESTION: {user_question}
PERIOD: {date_context['label']}

QUERY RESULTS:
{results_json}

AVAILABLE FIELDS: {field_list}

**REQUIREMENTS:**
1. Start with a bold header with emoji
2. Show ALL fields from results clearly
3. Use emojis: 💰 money, 📊 stats, 🏆 winners, ⚠️ warnings, 📅 dates
4. Format numbers: Currency $1,234.56, Quantities 1,234, Percentages 45.2%
5. Keep it concise - this is a SUMMARY, not a detailed list
6. Add 1 brief actionable insight

Generate the summary:"""

        try:
            formatted_text = self._call_groq(prompt, max_tokens=600)
            return formatted_text
        except Exception as e:
            print(f"Error formatting with LLM: {e}")
            return self._basic_format_results(results, date_context)

    def _basic_format_results(self, results, date_context):
        """Fallback basic formatting"""
        response = f"**📊 QUERY RESULTS**\n**📅 Period:** {date_context['label']}\n\n"

        if len(results) == 1:
            result = results[0]
            for key, value in result.items():
                formatted_key = key.replace('_', ' ').title()
                if value is None:
                    response += f"**{formatted_key}:** N/A\n"
                elif isinstance(value, (int, float)):
                    if any(k in key.lower() for k in ['revenue', 'amount', 'total', 'sales', 'price', 'cost', 'profit']):
                        response += f"💰 **{formatted_key}:** ${value:,.2f}\n"
                    elif any(k in key.lower() for k in ['count', 'quantity', 'invoices', 'orders', 'customers']):
                        response += f"📊 **{formatted_key}:** {int(value):,}\n"
                    elif 'percent' in key.lower():
                        response += f"📈 **{formatted_key}:** {value:.2f}%\n"
                    else:
                        response += f"**{formatted_key}:** {value:,.2f}\n"
                elif 'date' in key.lower():
                    response += f"📅 **{formatted_key}:** {value}\n"
                else:
                    response += f"**{formatted_key}:** {value}\n"
        else:
            response += f"**Found {len(results)} results**\n\n"
            for idx, row in enumerate(results[:10], 1):
                row_items = []
                for key, value in row.items():
                    if isinstance(value, (int, float)):
                        if any(k in key.lower() for k in ['revenue', 'amount', 'total', 'sales']):
                            row_items.append(f"{key}: ${value:,.2f}")
                        elif any(k in key.lower() for k in ['count', 'quantity']):
                            row_items.append(f"{key}: {int(value):,}")
                        else:
                            row_items.append(f"{key}: {value:,.2f}")
                    else:
                        row_items.append(f"{key}: {value}")
                response += f"{idx}. " + " | ".join(row_items) + "\n"

        return response

    # ============================================================================
    # COMPATIBILITY METHODS - All FAQs supported
    # ============================================================================

    def get_sales_today(self, company_id, date_range=None):
        return self.process_query("What are my total sales today?", company_id)

    def get_sales_this_month(self, company_id, date_range=None):
        return self.process_query("What are my total sales this month?", company_id)

    def get_sales_this_year(self, company_id, date_range=None):
        return self.process_query("What are my total sales this year?", company_id)

    def compare_this_month_vs_last_month(self, company_id, date_range=None):
        return self.process_query("Compare sales of this month with last month", company_id)

    def compare_this_year_vs_last_year(self, company_id, date_range=None):
        return self.process_query("Compare sales of this year with last year", company_id)

    def get_highest_sales_day(self, company_id, date_range=None):
        return self.process_query("Which day had the highest sales?", company_id)

    def get_lowest_sales_day(self, company_id, date_range=None):
        return self.process_query("Which day had the lowest sales?", company_id)

    def get_sales_trend_12_months(self, company_id, date_range=None):
        return self.process_query("Show sales trend for the last 12 months", company_id)

    def get_total_invoices(self, company_id, date_range=None):
        return self.process_query("What is the total number of sales invoices?", company_id)

    def get_highest_sales_branch(self, company_id, date_range=None):
        return self.process_query("Which branch has the highest sales?", company_id)

    def get_lowest_sales_branch(self, company_id, date_range=None):
        return self.process_query("Which branch has the lowest sales?", company_id)

    def get_sales_by_salesperson(self, company_id, date_range=None):
        return self.process_query("Show sales by salesperson", company_id)

    def get_top_salesperson(self, company_id, date_range=None):
        return self.process_query("Which salesperson has the highest sales?", company_id)

    def get_lowest_salesperson(self, company_id, date_range=None):
        return self.process_query("Which salesperson has the lowest sales?", company_id)

    def get_top_selling_products(self, company_id, date_range=None):
        return self.process_query("What are my top-selling products?", company_id)

    def get_slow_moving_products(self, company_id, date_range=None):
        return self.process_query("Show slow-moving products", company_id)

    def get_highest_sales_category(self, company_id, date_range=None):
        return self.process_query("Which category has the highest sales?", company_id)

    def get_lowest_sales_category(self, company_id, date_range=None):
        return self.process_query("Which category has the lowest sales?", company_id)

    def get_highest_revenue_product(self, company_id, date_range=None):
        return self.process_query("Which product has the highest revenue?", company_id)

    def get_lowest_revenue_product(self, company_id, date_range=None):
        return self.process_query("Which product has the lowest revenue?", company_id)

    def get_highest_profit_product(self, company_id, date_range=None):
        return self.process_query("Which product has the highest profit?", company_id)

    def get_lowest_profit_product(self, company_id, date_range=None):
        return self.process_query("Which product has the lowest profit?", company_id)

    def get_highest_revenue_customers(self, company_id, date_range=None):
        return self.process_query("Who are my highest revenue customers?", company_id)

    def get_lowest_revenue_customers(self, company_id, date_range=None):
        return self.process_query("Who are my lowest revenue customers?", company_id)

    def get_customer_wise_sales(self, company_id, date_range=None):
        return self.process_query("Show customer-wise sales summary", company_id)

    def get_inactive_customers_30_days(self, company_id, date_range=None):
        return self.process_query("Which customers have not purchased in last 30 days?", company_id)

    def get_inactive_customers_60_days(self, company_id, date_range=None):
        return self.process_query("Which customers have not purchased in last 60 days?", company_id)

    def get_inactive_customers_90_days(self, company_id, date_range=None):
        return self.process_query("Which customers have not purchased in last 90 days?", company_id)

    def get_sales_overview(self, company_id, date_range=None):
        return self.process_query("Show me sales overview", company_id)


if __name__ == "__main__":
    agent = SalesAgent()
    print(agent.process_query("What are my sales this month?", 922))