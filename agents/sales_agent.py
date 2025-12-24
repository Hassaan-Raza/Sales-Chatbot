"""
LLM-Powered Sales Agent - Fixed to show complete results
"""
import os
import json
import re
from datetime import datetime, timedelta
from database.db_connection import db
import requests
from dotenv import load_dotenv

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
        """Load database schema for LLM context"""
        return """
DATABASE SCHEMA:

**sales_invoice** - Main invoice table
- invoice_id (PRI), company_id, warehouse_id, customer_id
- invoice_num, invoice_date, duedate, status (paid/unpaid/remaining)
- total, subtotal, discount, salesman, payment_term
- created_at, finalized_at, paid_at

**sales_items** - Invoice line items
- item_id (PRI), company_id, invoice_id, product_id
- quantity, price, item_cost, discount, discount_amount
- total, subtotal, tax

**sales_payments** - Payment records
- payment_id (PRI), company_id, payment_date, amount

**sales_payment_items** - Payment allocations to invoices
- payment_id, invoice_id, amount

**products** - Product catalog
- product_id (PRI), company_id, category_id, name, sku
- price, cost, saleable, product_type (product/service)

**products_category** - Product categories
- category_id (PRI), name

**contacts** - Customers
- contact_id (PRI), company_id, name, region
- customer_amount (credit limit), is_active, email, phone

**users** - Salespeople
- user_id (PRI), firstname, lastname, company_id

**warehouses** - Branches/Warehouses (IMPORTANT: No 'name' column!)
- warehouse_id (PRI), company_id
- NOTE: Warehouses table has NO name column - just use warehouse_id

**credit_notes** - Sales returns
- note_id (PRI), company_id, invoice_date, total

**credit_note_items** - Return line items
- item_id, note_id, product_id, quantity, total

**sale_order** - Sales orders
- order_id (PRI), company_id, customer_id, status, order_date

**advances** - Customer advance payments
- advance_id (PRI), company_id, contact_id, amount, remaining_amount, status

**origins** - Regions/Cities
- id (PRI), company_id, title (region name)

KEY RELATIONSHIPS:
- sales_invoice.customer_id → contacts.contact_id
- sales_invoice.salesman → users.user_id
- sales_invoice.warehouse_id → warehouses.warehouse_id
- sales_items.invoice_id → sales_invoice.invoice_id
- sales_items.product_id → products.product_id
- products.category_id → products_category.category_id
- contacts.region → origins.id

IMPORTANT BUSINESS RULES:
- Valid invoice statuses: 'paid', 'unpaid', 'remaining'
- Always filter by company_id for data isolation
- Use LEFT JOIN for optional relationships (customer names, product names)
- Dates are in YYYY-MM-DD format
- **CRITICAL:** warehouses table has NO name column - display only warehouse_id
- When joining warehouses, do NOT try to access w.name - it doesn't exist!
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
                'filter': f"AND si.invoice_date = '{today.strftime('%Y-%m-%d')}'"
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
            start = today.replace(day=1)
            return {
                'label': 'This Month',
                'start_date': start.strftime('%Y-%m-%d'),
                'end_date': today.strftime('%Y-%m-%d'),
                'filter': f"AND si.invoice_date >= '{start.strftime('%Y-%m-%d')}' AND si.invoice_date <= '{today.strftime('%Y-%m-%d')}'"
            }

        if 'last month' in msg:
            last_month_end = today.replace(day=1) - timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            return {
                'label': 'Last Month',
                'start_date': last_month_start.strftime('%Y-%m-%d'),
                'end_date': last_month_end.strftime('%Y-%m-%d'),
                'filter': f"AND si.invoice_date >= '{last_month_start.strftime('%Y-%m-%d')}' AND si.invoice_date <= '{last_month_end.strftime('%Y-%m-%d')}'"
            }

        if 'this year' in msg:
            start = today.replace(month=1, day=1)
            return {
                'label': 'This Year',
                'start_date': start.strftime('%Y-%m-%d'),
                'end_date': today.strftime('%Y-%m-%d'),
                'filter': f"AND si.invoice_date >= '{start.strftime('%Y-%m-%d')}' AND si.invoice_date <= '{today.strftime('%Y-%m-%d')}'"
            }

        if 'last 90 days' in msg or 'last 3 months' in msg:
            start = today - timedelta(days=90)
            return {
                'label': 'Last 90 Days',
                'start_date': start.strftime('%Y-%m-%d'),
                'end_date': today.strftime('%Y-%m-%d'),
                'filter': f"AND si.invoice_date >= '{start.strftime('%Y-%m-%d')}'"
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

CRITICAL REQUIREMENTS:
1. ONLY generate SELECT queries - NO INSERT, UPDATE, DELETE, or any data modification
2. ALWAYS include "WHERE sit.company_id = {company_id}" or "WHERE si.company_id = {company_id}"
3. ALWAYS use status filter: "AND si.status IN ('paid', 'unpaid', 'remaining')"
4. Apply the date filter: {date_context['filter']}
5. Use LEFT JOIN for optional relationships (names, descriptions)
6. **IMPORTANT:** For list queries (customers, invoices, products), use LIMIT 10 to show top results
7. **IMPORTANT - Complete Metrics:** For sales summaries, ALWAYS include ALL relevant metrics:
   - COUNT(DISTINCT si.invoice_id) as invoices
   - COALESCE(SUM(sit.total), 0) as revenue
   - COALESCE(SUM(sit.quantity), 0) as units
8. Use COUNT(DISTINCT ...) to avoid duplicate counting
9. Handle NULL values with COALESCE
10. Return the SQL query ONLY - no explanations, no markdown formatting

EXAMPLES:

Q: "What are my total sales today?"
A: SELECT COUNT(DISTINCT si.invoice_id) as invoices, COALESCE(SUM(sit.total), 0) as revenue, COALESCE(SUM(sit.quantity), 0) as units FROM sales_items sit INNER JOIN sales_invoice si ON si.invoice_id = sit.invoice_id WHERE sit.company_id = {company_id} AND si.status IN ('paid', 'unpaid', 'remaining') AND si.invoice_date = CURDATE()

Q: "Who are my top customers?"
A: SELECT si.customer_id, c.name, COALESCE(SUM(sit.total), 0) as revenue, COUNT(DISTINCT si.invoice_id) as invoices FROM sales_items sit INNER JOIN sales_invoice si ON si.invoice_id = sit.invoice_id LEFT JOIN contacts c ON c.contact_id = si.customer_id WHERE sit.company_id = {company_id} AND si.status IN ('paid', 'unpaid', 'remaining') GROUP BY si.customer_id, c.name ORDER BY revenue DESC LIMIT 10

Q: "Which invoices are overdue?"
A: SELECT si.invoice_id, si.invoice_num, si.invoice_date, si.duedate, c.name as customer_name, si.total, DATEDIFF(CURDATE(), si.duedate) as days_overdue FROM sales_invoice si LEFT JOIN contacts c ON c.contact_id = si.customer_id WHERE si.company_id = {company_id} AND si.status IN ('unpaid', 'remaining') AND si.duedate < CURDATE() ORDER BY days_overdue DESC LIMIT 10

Generate ONLY the SQL query:"""

        try:
            sql_query = self._call_groq(prompt, max_tokens=500)
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
        """Fix common SQL generation errors"""
        original = sql_query

        # Fix 1: Remove w.name references
        if 'w.name' in sql_query or 'warehouses.name' in sql_query:
            print("⚠️ Detected w.name - fixing...")
            sql_query = re.sub(r'w\.name,?\s*', '', sql_query, flags=re.IGNORECASE)
            sql_query = re.sub(r'warehouses\.name,?\s*', '', sql_query, flags=re.IGNORECASE)
            sql_query = re.sub(r'GROUP BY\s+w\.name', 'GROUP BY si.warehouse_id', sql_query, flags=re.IGNORECASE)
            sql_query = re.sub(r',\s*w\.name\s*(?=FROM|GROUP|ORDER|LIMIT)', '', sql_query, flags=re.IGNORECASE)
            if 'w.' not in sql_query.lower() and 'warehouses.' not in sql_query.lower():
                sql_query = re.sub(r'INNER JOIN warehouses w ON w\.warehouse_id = si\.warehouse_id\s*', '', sql_query, flags=re.IGNORECASE)
                sql_query = re.sub(r'LEFT JOIN warehouses w ON w\.warehouse_id = si\.warehouse_id\s*', '', sql_query, flags=re.IGNORECASE)

        # Fix 2: Remove duplicate joins
        if 'sit2' in sql_query:
            print("⚠️ Detected unnecessary duplicate joins - simplifying...")
            sql_query = re.sub(r'LEFT JOIN sales_items sit2.*?(?=LEFT JOIN|WHERE|GROUP BY|ORDER BY|LIMIT|$)', '', sql_query, flags=re.IGNORECASE | re.DOTALL)

        # Fix 3: Clean up extra commas
        sql_query = re.sub(r',\s*,', ',', sql_query)
        sql_query = re.sub(r',\s*(FROM|WHERE|GROUP|ORDER|LIMIT)', r' \1', sql_query)
        sql_query = re.sub(r'SELECT\s+,', 'SELECT ', sql_query)

        if sql_query != original:
            print(f"✅ SQL query was automatically fixed")
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
        elif 'sales' in user_question.lower():
            emoji = "💰"
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
                    if any(k in key.lower() for k in ['revenue', 'amount', 'total', 'sales', 'price', 'cost', 'profit']):
                        response += f"  💰 **{formatted_key}:** ${value:,.2f}\n"
                    elif any(k in key.lower() for k in ['count', 'quantity', 'invoices', 'orders', 'units']):
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
    # COMPATIBILITY METHODS - All 134 FAQs supported via dynamic LLM generation
    # ============================================================================

    def get_sales_overview(self, company_id, date_range=None):
        return self.process_query("Show me sales overview", company_id)

    def get_profit_by_product(self, company_id, date_range=None):
        return self.process_query("Show profit by product", company_id)

    def get_profit_by_customer(self, company_id, date_range=None):
        return self.process_query("Show profit by customer", company_id)

    def get_profit_margin_percentage(self, company_id, date_range=None):
        return self.process_query("What is the profit margin percentage?", company_id)

    def get_cost_of_goods_sold(self, company_id, date_range=None):
        return self.process_query("What is the cost of goods sold?", company_id)

    def get_overdue_invoices(self, company_id, date_range=None):
        return self.process_query("Which invoices are overdue?", company_id)

    def get_paid_invoices(self, company_id, date_range=None):
        return self.process_query("Show paid invoices", company_id)

    def get_unpaid_invoices(self, company_id, date_range=None):
        return self.process_query("Show unpaid invoices", company_id)

    def get_partially_paid_invoices(self, company_id, date_range=None):
        return self.process_query("Show partially paid invoices", company_id)

    def get_total_receivables(self, company_id, date_range=None):
        return self.process_query("What is the total receivables?", company_id)

    def get_aging_report_30_days(self, company_id, date_range=None):
        return self.process_query("Show receivables aging 0-30 days", company_id)

    def get_aging_report_60_days(self, company_id, date_range=None):
        return self.process_query("Show receivables aging 31-60 days", company_id)

    def get_aging_report_90_days(self, company_id, date_range=None):
        return self.process_query("Show receivables aging 61-90 days", company_id)

    def get_aging_report_over_90_days(self, company_id, date_range=None):
        return self.process_query("Show receivables aging over 90 days", company_id)

    def get_total_cash_received(self, company_id, date_range=None):
        return self.process_query("What is the total cash received?", company_id)

    def get_total_payments_received(self, company_id, date_range=None):
        return self.process_query("What is the total payments received?", company_id)

    def get_payment_collection_rate(self, company_id, date_range=None):
        return self.process_query("What is the payment collection rate?", company_id)

    def get_average_collection_period(self, company_id, date_range=None):
        return self.process_query("What is the average collection period?", company_id)

    def get_customers_with_credit_limit_exceeded(self, company_id, date_range=None):
        return self.process_query("Which customers have exceeded credit limit?", company_id)

    def get_customers_near_credit_limit(self, company_id, date_range=None):
        return self.process_query("Which customers are near credit limit?", company_id)

    def get_credit_notes_issued(self, company_id, date_range=None):
        return self.process_query("Show credit notes issued", company_id)

    def get_total_returns(self, company_id, date_range=None):
        return self.process_query("What is the total sales returns?", company_id)

    def get_return_rate_percentage(self, company_id, date_range=None):
        return self.process_query("What is the return rate percentage?", company_id)

    def get_most_returned_products(self, company_id, date_range=None):
        return self.process_query("Which products have the most returns?", company_id)

    def get_customers_with_most_returns(self, company_id, date_range=None):
        return self.process_query("Which customers have the most returns?", company_id)

    def get_unadjusted_advances(self, company_id, date_range=None):
        return self.process_query("Show unadjusted customer advances", company_id)

    def get_total_advances_received(self, company_id, date_range=None):
        return self.process_query("What is the total advances received?", company_id)

    def get_advances_by_customer(self, company_id, date_range=None):
        return self.process_query("Show advances by customer", company_id)

    def get_sales_orders_pending(self, company_id, date_range=None):
        return self.process_query("Show pending sales orders", company_id)

    def get_sales_orders_completed(self, company_id, date_range=None):
        return self.process_query("Show completed sales orders", company_id)

    def get_sales_orders_cancelled(self, company_id, date_range=None):
        return self.process_query("Show cancelled sales orders", company_id)

    def get_order_to_invoice_conversion_rate(self, company_id, date_range=None):
        return self.process_query("What is the order to invoice conversion rate?", company_id)

    def get_average_order_value(self, company_id, date_range=None):
        return self.process_query("What is the average order value?", company_id)

    def get_total_tax_collected(self, company_id, date_range=None):
        return self.process_query("What is the total tax collected?", company_id)

    def get_tax_by_category(self, company_id, date_range=None):
        return self.process_query("Show tax collected by category", company_id)

    def get_sales_by_payment_method(self, company_id, date_range=None):
        return self.process_query("Show sales by payment method", company_id)

    def get_cash_sales(self, company_id, date_range=None):
        return self.process_query("What are the cash sales?", company_id)

    def get_credit_sales(self, company_id, date_range=None):
        return self.process_query("What are the credit sales?", company_id)

    def get_cheque_sales(self, company_id, date_range=None):
        return self.process_query("What are the cheque sales?", company_id)

    def get_sales_by_warehouse(self, company_id, date_range=None):
        return self.process_query("Show sales by warehouse", company_id)

    def get_inventory_turnover_ratio(self, company_id, date_range=None):
        return self.process_query("What is the inventory turnover ratio?", company_id)

    def get_days_sales_outstanding(self, company_id, date_range=None):
        return self.process_query("What are the days sales outstanding?", company_id)

    def get_sales_velocity(self, company_id, date_range=None):
        return self.process_query("What is the sales velocity?", company_id)

    def get_customer_acquisition_rate(self, company_id, date_range=None):
        return self.process_query("What is the customer acquisition rate?", company_id)

    def get_customer_retention_rate(self, company_id, date_range=None):
        return self.process_query("What is the customer retention rate?", company_id)

    def get_customer_lifetime_value(self, company_id, date_range=None):
        return self.process_query("What is the customer lifetime value?", company_id)

    def get_sales_forecast_next_month(self, company_id, date_range=None):
        return self.process_query("What is the sales forecast for next month?", company_id)

    def get_sales_target_achievement(self, company_id, date_range=None):
        return self.process_query("What is the sales target achievement?", company_id)

    def get_salesperson_target_vs_achievement(self, company_id, date_range=None):
        return self.process_query("Show salesperson target vs achievement", company_id)

    def get_product_performance_analysis(self, company_id, date_range=None):
        return self.process_query("Show product performance analysis", company_id)

    def get_customer_segmentation(self, company_id, date_range=None):
        return self.process_query("Show customer segmentation", company_id)

    def get_sales_by_region_comparison(self, company_id, date_range=None):
        return self.process_query("Compare sales by region", company_id)

    def get_seasonal_sales_trends(self, company_id, date_range=None):
        return self.process_query("Show seasonal sales trends", company_id)

    def get_sales_by_product_category(self, company_id, date_range=None):
        return self.process_query("Show sales by product category", company_id)

    def get_top_selling_combinations(self, company_id, date_range=None):
        return self.process_query("What are the top-selling product combinations?", company_id)

    def get_cross_selling_opportunities(self, company_id, date_range=None):
        return self.process_query("Show cross-selling opportunities", company_id)

    def get_upselling_opportunities(self, company_id, date_range=None):
        return self.process_query("Show upselling opportunities", company_id)

    def get_customer_churn_rate(self, company_id, date_range=None):
        return self.process_query("What is the customer churn rate?", company_id)

    def get_customers_at_risk(self, company_id, date_range=None):
        return self.process_query("Which customers are at risk of churning?", company_id)

    def get_new_customers_this_month(self, company_id, date_range=None):
        return self.process_query("Show new customers this month", company_id)

    def get_lost_customers(self, company_id, date_range=None):
        return self.process_query("Show lost customers", company_id)

    def get_sales_conversion_funnel(self, company_id, date_range=None):
        return self.process_query("Show sales conversion funnel", company_id)

    def get_quotation_to_order_conversion(self, company_id, date_range=None):
        return self.process_query("What is the quotation to order conversion rate?", company_id)

    def get_average_sales_cycle_length(self, company_id, date_range=None):
        return self.process_query("What is the average sales cycle length?", company_id)

    def get_win_loss_ratio(self, company_id, date_range=None):
        return self.process_query("What is the win/loss ratio?", company_id)

    def get_sales_pipeline_value(self, company_id, date_range=None):
        return self.process_query("What is the sales pipeline value?", company_id)

    def get_top_customers_by_frequency(self, company_id, date_range=None):
        return self.process_query("Who are the top customers by purchase frequency?", company_id)

    def get_one_time_buyers(self, company_id, date_range=None):
        return self.process_query("Show one-time buyers", company_id)

    def get_customer_purchase_patterns(self, company_id, date_range=None):
        return self.process_query("Show customer purchase patterns", company_id)

    def get_best_selling_hours(self, company_id, date_range=None):
        return self.process_query("What are the best-selling hours?", company_id)

    def get_best_selling_days(self, company_id, date_range=None):
        return self.process_query("What are the best-selling days of the week?", company_id)

    def get_monthly_comparison_last_3_months(self, company_id, date_range=None):
        return self.process_query("Compare sales for last 3 months", company_id)

    def get_year_over_year_growth(self, company_id, date_range=None):
        return self.process_query("What is the year-over-year growth?", company_id)

    def get_quarter_over_quarter_growth(self, company_id, date_range=None):
        return self.process_query("What is the quarter-over-quarter growth?", company_id)

    def get_declining_products(self, company_id, date_range=None):
        return self.process_query("Which products are declining in sales?", company_id)

    def get_growing_products(self, company_id, date_range=None):
        return self.process_query("Which products are growing in sales?", company_id)

    def get_products_needing_promotion(self, company_id, date_range=None):
        return self.process_query("Which products need promotion?", company_id)

    def get_overstocked_products(self, company_id, date_range=None):
        return self.process_query("Which products are overstocked?", company_id)

    def get_understocked_products(self, company_id, date_range=None):
        return self.process_query("Which products are understocked?", company_id)

    def get_stockout_products(self, company_id, date_range=None):
        return self.process_query("Which products are out of stock?", company_id)

    def get_reorder_point_alerts(self, company_id, date_range=None):
        return self.process_query("Show products at reorder point", company_id)

    def get_sales_performance_summary(self, company_id, date_range=None):
        return self.process_query("Show sales performance summary", company_id)

    def get_key_metrics_dashboard(self, company_id, date_range=None):
        return self.process_query("Show key sales metrics", company_id)

    def get_revenue_breakdown(self, company_id, date_range=None):
        return self.process_query("Show revenue breakdown", company_id)

    def get_profit_and_loss_summary(self, company_id, date_range=None):
        return self.process_query("Show profit and loss summary", company_id)

    def get_commission_by_salesperson(self, company_id, date_range=None):
        return self.process_query("Show commission by salesperson", company_id)

    def get_total_commissions_paid(self, company_id, date_range=None):
        return self.process_query("What is the total commissions paid?", company_id)

    def get_sales_efficiency_ratio(self, company_id, date_range=None):
        return self.process_query("What is the sales efficiency ratio?", company_id)

    def get_revenue_per_salesperson(self, company_id, date_range=None):
        return self.process_query("What is the revenue per salesperson?", company_id)

    def get_average_deal_size(self, company_id, date_range=None):
        return self.process_query("What is the average deal size?", company_id)

    def get_largest_deals_closed(self, company_id, date_range=None):
        return self.process_query("Show largest deals closed", company_id)

    def get_smallest_deals_closed(self, company_id, date_range=None):
        return self.process_query("Show smallest deals closed", company_id)

    def get_customer_satisfaction_indicators(self, company_id, date_range=None):
        return self.process_query("Show customer satisfaction indicators", company_id)

    def get_repeat_purchase_rate(self, company_id, date_range=None):
        return self.process_query("What is the repeat purchase rate?", company_id)

    def get_referral_customers(self, company_id, date_range=None):
        return self.process_query("Show referral customers", company_id)

    def get_promotional_sales_impact(self, company_id, date_range=None):
        return self.process_query("What is the promotional sales impact?", company_id)

    def get_discount_effectiveness(self, company_id, date_range=None):
        return self.process_query("Show discount effectiveness", company_id)

    def get_sales_by_customer_type(self, company_id, date_range=None):
        return self.process_query("Show sales by customer type", company_id)

    def get_b2b_vs_b2c_sales(self, company_id, date_range=None):
        return self.process_query("Compare B2B vs B2C sales", company_id)

    def get_export_vs_local_sales(self, company_id, date_range=None):
        return self.process_query("Compare export vs local sales", company_id)

    def get_online_vs_offline_sales(self, company_id, date_range=None):
        return self.process_query("Compare online vs offline sales", company_id)

    def get_sales_by_channel(self, company_id, date_range=None):
        return self.process_query("Show sales by channel", company_id)

    def get_multi_channel_customers(self, company_id, date_range=None):
        return self.process_query("Show multi-channel customers", company_id)

    def get_customer_contact_effectiveness(self, company_id, date_range=None):
        return self.process_query("Show customer contact effectiveness", company_id)

    def get_follow_up_conversion_rate(self, company_id, date_range=None):
        return self.process_query("What is the follow-up conversion rate?", company_id)

    def get_sales_activities_summary(self, company_id, date_range=None):
        return self.process_query("Show sales activities summary", company_id)

    def get_top_performing_regions(self, company_id, date_range=None):
        return self.process_query("Show top-performing regions", company_id)

    def get_underperforming_regions(self, company_id, date_range=None):
        return self.process_query("Show underperforming regions", company_id)

    def get_market_penetration_by_region(self, company_id, date_range=None):
        return self.process_query("Show market penetration by region", company_id)

    def get_regional_growth_opportunities(self, company_id, date_range=None):
        return self.process_query("Show regional growth opportunities", company_id)

    def get_sales_trend_analysis(self, company_id, date_range=None):
        return self.process_query("Show sales trend analysis", company_id)

    def get_revenue_concentration_risk(self, company_id, date_range=None):
        return self.process_query("Show revenue concentration risk", company_id)

    def get_product_mix_analysis(self, company_id, date_range=None):
        return self.process_query("Show product mix analysis", company_id)

    def get_sales_volatility(self, company_id, date_range=None):
        return self.process_query("What is the sales volatility?", company_id)

    def get_customer_concentration(self, company_id, date_range=None):
        return self.process_query("Show customer concentration analysis", company_id)

    def get_sales_this_month(self, company_id, date_range=None):
        return self.process_query("What are my total sales this month?", company_id)

    def get_sales_this_year(self, company_id, date_range=None):
        return self.process_query("What are my total sales this year?", company_id)

    def compare_this_month_vs_last_month(self, company_id, date_range=None):
        return self.process_query("Compare sales of this month with last month", company_id)

    def compare_this_year_vs_last_year(self, company_id, date_range=None):
        return self.process_query("Compare sales of this year with last year", company_id)

    def get_sales_growth_percentage(self, company_id, date_range=None):
        return self.process_query("What is the sales growth percentage?", company_id)

    def get_highest_sales_day(self, company_id, date_range=None):
        return self.process_query("Which day had the highest sales?", company_id)

    def get_lowest_sales_day(self, company_id, date_range=None):
        return self.process_query("Which day had the lowest sales?", company_id)

    def get_average_daily_sales(self, company_id, date_range=None):
        return self.process_query("What is the average daily sales?", company_id)

    def get_average_monthly_sales(self, company_id, date_range=None):
        return self.process_query("What is the average monthly sales?", company_id)

    def get_sales_trend_12_months(self, company_id, date_range=None):
        return self.process_query("Show sales trend for the last 12 months", company_id)

    def get_total_invoices(self, company_id, date_range=None):
        return self.process_query("What is the total number of sales invoices?", company_id)

    def get_average_invoice_value(self, company_id, date_range=None):
        return self.process_query("What is the average invoice value?", company_id)

    def get_highest_sales_branch(self, company_id, date_range=None):
        return self.process_query("Which branch has the highest sales?", company_id)

    def get_lowest_sales_branch(self, company_id, date_range=None):
        return self.process_query("Which branch has the lowest sales?", company_id)

    def get_sales_by_region(self, company_id, date_range=None):
        return self.process_query("Show sales by region", company_id)

    def get_sales_by_salesperson(self, company_id, date_range=None):
        return self.process_query("Show sales by salesperson", company_id)

    def get_top_salesperson(self, company_id, date_range=None):
        return self.process_query("Which salesperson has the highest sales?", company_id)

    def get_lowest_salesperson(self, company_id, date_range=None):
        return self.process_query("Which salesperson has the lowest sales?", company_id)

    def get_top_selling_products(self, company_id, date_range=None):
        return self.process_query("What are my top-selling products?", company_id)

    def get_lowest_selling_products(self, company_id, date_range=None):
        return self.process_query("What are my lowest-selling products?", company_id)

    def get_highest_revenue_product(self, company_id, date_range=None):
        return self.process_query("Which product generates the highest revenue?", company_id)

    def get_lowest_revenue_product(self, company_id, date_range=None):
        return self.process_query("Which product generates the lowest revenue?", company_id)

    def get_slow_moving_products(self, company_id, date_range=None):
        return self.process_query("Show slow-moving products", company_id)

    def get_zero_sales_products(self, company_id, date_range=None):
        return self.process_query("Which products have zero sales?", company_id)

    def get_highest_sales_category(self, company_id, date_range=None):
        return self.process_query("Which category has the highest sales?", company_id)

    def get_lowest_sales_category(self, company_id, date_range=None):
        return self.process_query("Which category has the lowest sales?", company_id)

    def get_products_80_percent_sales(self, company_id, date_range=None):
        return self.process_query("Which products contribute 80% of sales?", company_id)

    def get_highest_revenue_customers(self, company_id, date_range=None):
        return self.process_query("Who are my highest revenue customers?", company_id)

    def get_top_paying_customers(self, company_id, date_range=None):
        return self.process_query("Who are my top-paying customers?", company_id)

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

    def get_repeat_buyers(self, company_id, date_range=None):
        return self.process_query("Which customers are repeat buyers?", company_id)

    def get_most_frequent_customer(self, company_id, date_range=None):
        return self.process_query("Who is my most frequent customer?", company_id)

    def get_average_sales_per_customer(self, company_id, date_range=None):
        return self.process_query("What is the average sales per customer?", company_id)

    def get_total_discounts(self, company_id, date_range=None):
        return self.process_query("What is the total discount given?", company_id)

    def get_customer_highest_discount(self, company_id, date_range=None):
        return self.process_query("Which customer received highest discount?", company_id)

    def get_product_highest_discount(self, company_id, date_range=None):
        return self.process_query("Which product has highest discount?", company_id)

    def get_average_discount_per_invoice(self, company_id, date_range=None):
        return self.process_query("What is average discount per invoice?", company_id)

    def get_gross_profit(self, company_id, date_range=None):
        return self.process_query("What is the gross profit from sales?", company_id)

    def get_products_lowest_margin(self, company_id, date_range=None):
        return self.process_query("Which products have lowest margin?", company_id)

    def get_products_highest_margin(self, company_id, date_range=None):
        return self.process_query("Which products have highest margin?", company_id)

    def get_sales_overview(self, company_id, date_range=None):
        return self.process_query("Show me sales overview", company_id)


if __name__ == "__main__":
    agent = SalesAgent()
    print(agent.process_query("What are my sales this month?", 922))