"""
LLM-Powered Sales Agent - Handles All 134 FAQs Dynamically
Uses Ollama (Local LLM) - NO API KEY NEEDED!
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
        # Ollama configuration - runs locally, no API key needed!
        self.ollama_url = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')

        # Recommended models for Ollama (in order of preference):
        # 1. mistral:7b-instruct (best for SQL, 4GB RAM)
        # 2. llama3.1:8b (great all-around, 5GB RAM)
        # 3. qwen2.5:7b (very good, 4GB RAM)
        # 4. phi3:mini (lightweight, 2GB RAM)
        self.model = os.getenv('OLLAMA_MODEL', 'mistral:7b-instruct')

        # Database schema for context
        self.schema = self._load_schema()

        # Safety keywords to prevent data modification
        self.forbidden_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'TRUNCATE', 'REPLACE', 'MERGE', 'GRANT', 'REVOKE'
        ]

    def _call_ollama(self, prompt, max_tokens=2000):
        """Call Ollama API running locally"""

        data = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,  # Low temperature for deterministic SQL
                "num_predict": max_tokens
            }
        }

        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=data,
                timeout=60  # Longer timeout for local processing
            )

            # Print status for debugging
            print(f"Ollama Status: {response.status_code}")

            response.raise_for_status()
            result = response.json()

            # Ollama returns response in 'response' field
            if 'response' not in result:
                raise Exception(f"Invalid Ollama response: {result}")

            return result['response'].strip()

        except requests.exceptions.ConnectionError:
            raise Exception("""
⚠️ Cannot connect to Ollama!

Please make sure Ollama is running:
1. Install Ollama from https://ollama.ai/download
2. Open terminal and run: ollama serve
3. Pull the model: ollama pull mistral:7b-instruct
4. Try again!
""")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Ollama error: {str(e)}")

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
- customer_amount (credit limit), is_active

**users** - Salespeople
- user_id (PRI), firstname, lastname, company_id

**warehouses** - Branches/Warehouses
- warehouse_id (PRI), company_id, name

**credit_notes** - Sales returns
- note_id (PRI), company_id, invoice_date, total

**credit_note_items** - Return line items
- item_id, note_id, product_id, quantity, total

**sale_order** - Sales orders
- order_id (PRI), company_id, customer_id, status, order_date

**advances** - Customer advance payments
- advance_id (PRI), company_id, contact_id, amount, remaining_amount

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
- credit_notes returns reference sales_invoice via return_from

IMPORTANT BUSINESS RULES:
- Valid invoice statuses: 'paid', 'unpaid', 'remaining'
- Always filter by company_id for data isolation
- Use LEFT JOIN for optional relationships (customer names, product names)
- Dates are in YYYY-MM-DD format
"""

    def process_query(self, message, company_id):
        """Main query processor - uses LLM to generate and execute SQL"""
        try:
            # Extract date range from message
            date_context = self._extract_date_context(message)

            # Generate SQL query using LLM
            sql_query = self._generate_sql(message, company_id, date_context)

            if not sql_query:
                return "❌ Could not generate a valid query. Please rephrase your question."

            # Safety check - prevent data modification
            if not self._is_safe_query(sql_query):
                return "🚫 Safety violation: Query attempted to modify data. Only SELECT queries are allowed."

            # Execute query
            result = db.execute_query(sql_query, ())

            # Format results using LLM
            formatted_response = self._format_results(message, result, date_context)

            return formatted_response

        except Exception as e:
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
2. ALWAYS include "WHERE si.company_id = {company_id}" or appropriate table alias
3. ALWAYS use status filter: "AND si.status IN ('paid', 'unpaid', 'remaining')"
4. Apply the date filter: {date_context['filter']}
5. Use LEFT JOIN for optional relationships (names, descriptions)
6. Limit results to top 20 for list queries
7. Use proper aggregations (SUM, COUNT, AVG) where needed
8. Handle NULL values with COALESCE
9. Return ONLY the SQL query - no explanations, no markdown, no backticks

EXAMPLES:

Q: "What are my total sales today?"
A: SELECT COUNT(DISTINCT si.invoice_id) as invoices, COALESCE(SUM(si.total), 0) as revenue FROM sales_items sit INNER JOIN sales_invoice si ON si.invoice_id = sit.invoice_id WHERE sit.company_id = {company_id} AND si.status IN ('paid', 'unpaid', 'remaining') AND si.invoice_date = CURDATE()

Q: "Who are my top customers?"
A: SELECT si.customer_id, c.name, SUM(sit.total) as revenue FROM sales_items sit INNER JOIN sales_invoice si ON si.invoice_id = sit.invoice_id LEFT JOIN contacts c ON c.contact_id = si.customer_id WHERE sit.company_id = {company_id} AND si.status IN ('paid', 'unpaid', 'remaining') GROUP BY si.customer_id, c.name ORDER BY revenue DESC LIMIT 20

Generate the SQL query now (SQL ONLY, no other text):"""

        try:
            sql_query = self._call_ollama(prompt, max_tokens=500)

            # Clean up the response
            sql_query = re.sub(r'```sql\n?', '', sql_query)
            sql_query = re.sub(r'```\n?', '', sql_query)
            sql_query = sql_query.strip()

            # Remove any text before SELECT
            if 'SELECT' in sql_query.upper():
                sql_query = sql_query[sql_query.upper().find('SELECT'):]

            return sql_query

        except Exception as e:
            print(f"Error generating SQL: {e}")
            return None

    def _is_safe_query(self, sql_query):
        """Verify query is read-only (SELECT only)"""
        sql_upper = sql_query.upper()

        # Check for forbidden keywords
        for keyword in self.forbidden_keywords:
            if keyword in sql_upper:
                return False

        # Must start with SELECT
        if not sql_upper.strip().startswith('SELECT'):
            return False

        # Additional safety: no semicolons (prevent multiple statements)
        if ';' in sql_query[:-1]:  # Allow trailing semicolon
            return False

        return True

    def _format_results(self, user_question, results, date_context):
        """Use LLM to format query results into human-readable response"""

        if not results:
            return f"ℹ️ No data found for your query.\n\n**Period:** {date_context['label']}"

        # Convert results to JSON for LLM
        results_json = json.dumps(results, default=str, indent=2)

        prompt = f"""Format these sales analytics results into a clear business report.

QUESTION: {user_question}
PERIOD: {date_context['label']}
RESULTS: {results_json}

FORMAT RULES:
1. Bold header with emoji
2. Format ALL numbers with commas: $3,835,451.42 NOT 3835451.4150
3. Use 💰 for money, 📊 for stats, 🏆 for top items
4. For lists: "1. Name - Qty: 904,894 | Revenue: $25,212,465.47"
5. Add 1 insight with 💡
6. Keep it concise

Generate report now:"""

        try:
            formatted_text = self._call_ollama(prompt, max_tokens=800)
            return formatted_text
        except Exception as e:
            return self._basic_format_results(results, date_context)

    def _basic_format_results(self, results, date_context):
        """Fallback basic formatting with markdown tables"""
        response = f"**📊 QUERY RESULTS**\n**📅 Period:** {date_context['label']}\n\n"

        if len(results) == 1 and len(results[0]) <= 5:
            for key, value in results[0].items():
                formatted_key = key.replace('_', ' ').title()
                if isinstance(value, (int, float)):
                    if any(keyword in key.lower() for keyword in
                           ['revenue', 'amount', 'total', 'sales', 'price', 'cost', 'profit', 'discount', 'balance',
                            'payment']):
                        response += f"**💰 {formatted_key}:** ${value:,.2f}\n"
                    elif any(keyword in key.lower() for keyword in
                             ['count', 'quantity', 'number', 'invoices', 'orders', 'customers']):
                        response += f"**📊 {formatted_key}:** {int(value):,}\n"
                    elif 'percent' in key.lower() or 'rate' in key.lower():
                        response += f"**📈 {formatted_key}:** {value:.2f}%\n"
                    else:
                        response += f"**{formatted_key}:** {value:,.2f}\n"
                else:
                    response += f"**{formatted_key}:** {value}\n"
        else:
            response += f"**📋 Found {len(results)} results:**\n\n"
            if results:
                headers = list(results[0].keys())
                header_row = "| " + " | ".join([h.replace('_', ' ').title() for h in headers]) + " |"
                separator = "| " + " | ".join(["---" for _ in headers]) + " |"
                response += header_row + "\n" + separator + "\n"

                for row in results[:20]:
                    formatted_values = []
                    for key in headers:
                        value = row.get(key, '')
                        if isinstance(value, (int, float)):
                            if any(keyword in key.lower() for keyword in
                                   ['revenue', 'amount', 'total', 'sales', 'price', 'cost', 'profit']):
                                formatted_values.append(f"${value:,.2f}")
                            elif any(keyword in key.lower() for keyword in ['id', 'count', 'number', 'quantity']):
                                formatted_values.append(f"{int(value):,}")
                            else:
                                formatted_values.append(f"{value:,.2f}")
                        else:
                            str_value = str(value) if value else ''
                            formatted_values.append(str_value[:40] + "..." if len(str_value) > 40 else str_value)

                    response += "| " + " | ".join(formatted_values) + " |\n"

        return response

    # Compatibility methods
    def get_sales_today(self, company_id, date_range=None):
        return self.process_query("What are my total sales today?", company_id)

    def get_sales_this_month(self, company_id, date_range=None):
        return self.process_query("What are my total sales this month?", company_id)

    def get_sales_this_year(self, company_id, date_range=None):
        return self.process_query("What are my total sales this year?", company_id)

    # ... (add other compatibility methods as needed)


if __name__ == "__main__":
    agent = SalesAgent()
    company_id = 922

    print("Testing Ollama-powered Sales Agent...")
    print("=" * 80)
    print(agent.process_query("What are my sales today?", company_id))