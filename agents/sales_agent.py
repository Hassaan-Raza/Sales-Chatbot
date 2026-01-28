"""
Complete Hybrid Sales Agent - LLM for Intent + ALL Hardcoded SQL Queries
Includes ALL queries from the provided documentation
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
    """Complete hybrid approach with all documented queries"""

    def __init__(self):
        # Initialize Groq
        self.api_key = st.secrets["GROQ_API_KEY"]
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")

        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.model = "llama-3.1-8b-instant"
        
        # Load all hardcoded query templates
        self.query_templates = self._load_query_templates()

    def _call_groq(self, prompt, max_tokens=800, temperature=0.1):
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
                    "content": "You are an intent classifier for a sales analytics system. Return ONLY valid JSON, no other text."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
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
            raise Exception(f"Failed to get response from Groq: {str(e)}")

    def _load_query_templates(self):
        """Load ALL hardcoded SQL query templates from documentation"""
        return {
            # ============================================================================
            # SALES QUERIES (Document 1)
            # ============================================================================
            "sales_today": """
                SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_sales
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= CURDATE()
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
            """,
            
            "sales_this_month": """
                SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_sales
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
            """,
            
            "sales_this_year": """
                SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_sales
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01')
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
            """,
            
            # ============================================================================
            # RETURNS QUERIES (Document 1)
            # ============================================================================
            "returns_today": """
                SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_returns
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= CURDATE()
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status = 'return'
            """,
            
            "returns_this_month": """
                SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_returns
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status = 'return'
            """,
            
            "returns_this_year": """
                SELECT SUM(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)) AS total_returns
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01')
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status = 'return'
            """,
            
            # ============================================================================
            # NET SALES QUERIES (Document 1)
            # ============================================================================
            "net_sales_today": """
                SELECT COALESCE(
                    SUM(CASE
                        WHEN sales_invoice.status = 'return' THEN -(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0))
                        ELSE (sales_invoice.total - COALESCE(sales_invoice.total_tax, 0))
                    END), 0) AS net_sales
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= CURDATE()
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status NOT IN('draft', 'draft_return', 'canceled')
            """,
            
            "net_sales_this_month": """
                SELECT COALESCE(
                    SUM(CASE
                        WHEN sales_invoice.status = 'return' THEN -(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0))
                        ELSE (sales_invoice.total - COALESCE(sales_invoice.total_tax, 0))
                    END), 0) AS net_sales
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status NOT IN('draft', 'draft_return', 'canceled')
            """,
            
            "net_sales_this_year": """
                SELECT COALESCE(
                    SUM(CASE
                        WHEN sales_invoice.status = 'return' THEN -(sales_invoice.total - COALESCE(sales_invoice.total_tax, 0))
                        ELSE (sales_invoice.total - COALESCE(sales_invoice.total_tax, 0))
                    END), 0) AS net_sales
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
                  AND sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01')
                  AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                  AND sales_invoice.status NOT IN('draft', 'draft_return', 'canceled')
            """,
            
            # ============================================================================
            # COMPARISON QUERIES (Document 1)
            # ============================================================================
            "compare_month": """
                SELECT
                    COALESCE(SUM(CASE
                        WHEN sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
                         AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                         AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                        THEN sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)
                        ELSE 0
                    END), 0) AS total_sales_this_month,
                    
                    COALESCE(SUM(CASE
                        WHEN sales_invoice.invoice_date >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01')
                         AND sales_invoice.invoice_date < DATE_FORMAT(CURDATE(), '%Y-%m-01')
                         AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                        THEN sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)
                        ELSE 0
                    END), 0) AS total_sales_last_month
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
            """,
            
            "compare_year": """
                SELECT
                    COALESCE(SUM(CASE
                        WHEN sales_invoice.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01')
                         AND sales_invoice.invoice_date < CURDATE() + INTERVAL 1 DAY
                         AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                        THEN sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)
                        ELSE 0
                    END), 0) AS total_sales_this_year,
                    
                    COALESCE(SUM(CASE
                        WHEN sales_invoice.invoice_date >= DATE_FORMAT(CURDATE() - INTERVAL 1 YEAR, '%Y-01-01')
                         AND sales_invoice.invoice_date < DATE_FORMAT(CURDATE(), '%Y-01-01')
                         AND sales_invoice.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                        THEN sales_invoice.total - COALESCE(sales_invoice.total_tax, 0)
                        ELSE 0
                    END), 0) AS total_sales_last_year
                FROM sales_invoice
                WHERE sales_invoice.company_id = {company_id}
            """,
            
            # ============================================================================
            # DAY EXTREMES & SUMMARY (Document 1)
            # ============================================================================
            "highest_sales_day": """
                SELECT DATE(invoice_date) AS sales_day,
                    SUM(total - COALESCE(total_tax, 0)) AS total_sales
                FROM sales_invoice
                WHERE company_id = {company_id}
                  AND status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY DATE(invoice_date)
                ORDER BY total_sales DESC
                LIMIT 1
            """,
            
            "lowest_sales_day": """
                SELECT DATE(invoice_date) AS sales_day,
                    SUM(total - COALESCE(total_tax, 0)) AS total_sales
                FROM sales_invoice
                WHERE company_id = {company_id}
                  AND status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY DATE(invoice_date)
                ORDER BY total_sales ASC
                LIMIT 1
            """,
            
            "total_invoices": """
                SELECT COUNT(invoice_id) AS total_sales_invoices
                FROM sales_invoice
                WHERE company_id = {company_id}
                  AND status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
            """,
            
            # ============================================================================
            # SALES TREND (Document 1)
            # ============================================================================
            "sales_trend_12_months": """
                SELECT DATE_FORMAT(invoice_date, '%Y-%m') AS month,
                    SUM(total - COALESCE(total_tax, 0)) AS total_sales
                FROM sales_invoice
                WHERE company_id = {company_id}
                  AND invoice_date >= DATE_FORMAT(CURDATE() - INTERVAL 11 MONTH, '%Y-%m-01')
                  AND status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                GROUP BY DATE_FORMAT(invoice_date, '%Y-%m')
                ORDER BY month ASC
            """,
            
            # ============================================================================
            # BRANCH/WAREHOUSE QUERIES (Document 1)
            # ============================================================================
            "highest_sales_branch": """
                SELECT w.title AS branch_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales
                FROM sales_invoice si
                JOIN warehouses w ON si.warehouse_id = w.warehouse_id
                WHERE si.company_id = {company_id}
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY si.warehouse_id, w.title
                ORDER BY total_sales DESC
                LIMIT 1
            """,
            
            "lowest_sales_branch": """
                SELECT w.title AS branch_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales
                FROM sales_invoice si
                JOIN warehouses w ON si.warehouse_id = w.warehouse_id
                WHERE si.company_id = {company_id}
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY si.warehouse_id, w.title
                ORDER BY total_sales ASC
                LIMIT 1
            """,
            
            "top_branches": """
                SELECT w.title AS branch_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales
                FROM sales_invoice si
                JOIN warehouses w ON si.warehouse_id = w.warehouse_id
                WHERE si.company_id = {company_id}
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY si.warehouse_id, w.title
                ORDER BY total_sales DESC
                LIMIT {limit}
            """,
            
            # ============================================================================
            # SALESPERSON QUERIES (Document 2)
            # ============================================================================
            "sales_by_salesperson": """
                SELECT CONCAT(u.firstname, ' ', u.lastname) AS salesperson_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales
                FROM sales_invoice si
                LEFT JOIN users u ON si.salesman = u.user_id
                WHERE si.company_id = {company_id}
                  AND si.salesman > 0
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY si.salesman, u.firstname, u.lastname
                ORDER BY total_sales DESC
            """,
            
            "highest_salesperson": """
                SELECT CONCAT(u.firstname, ' ', u.lastname) AS salesperson_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales
                FROM sales_invoice si
                LEFT JOIN users u ON si.salesman = u.user_id
                WHERE si.company_id = {company_id}
                  AND si.salesman > 0
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY si.salesman, u.firstname, u.lastname
                ORDER BY total_sales DESC
                LIMIT 1
            """,
            
            "lowest_salesperson": """
                SELECT CONCAT(u.firstname, ' ', u.lastname) AS salesperson_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales
                FROM sales_invoice si
                LEFT JOIN users u ON si.salesman = u.user_id
                WHERE si.company_id = {company_id}
                  AND si.salesman > 0
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY si.salesman, u.firstname, u.lastname
                ORDER BY total_sales ASC
                LIMIT 1
            """,
            
            "top_salespeople": """
                SELECT CONCAT(u.firstname, ' ', u.lastname) AS salesperson_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_sales,
                    COUNT(si.invoice_id) AS invoice_count
                FROM sales_invoice si
                LEFT JOIN users u ON si.salesman = u.user_id
                WHERE si.company_id = {company_id}
                  AND si.salesman > 0
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY si.salesman, u.firstname, u.lastname
                ORDER BY total_sales DESC
                LIMIT {limit}
            """,
            
            # ============================================================================
            # PRODUCT QUERIES - BY QUANTITY (Document 3)
            # ============================================================================
            "top_products_by_quantity": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity)) AS total_sold_qty
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON si.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_sold_qty DESC
                LIMIT {limit}
            """,
            
            "slow_moving_products": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity)) AS total_sold_qty
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON si.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_sold_qty ASC
                LIMIT {limit}
            """,
            
            # ============================================================================
            # CATEGORY QUERIES (Document 3)
            # ============================================================================
            "highest_sales_category": """
                SELECT c.title AS category_name,
                    SUM(ABS(s.quantity)) AS total_sold_qty
                FROM stock s
                JOIN products p ON p.product_id = s.product_id
                JOIN products_category c ON c.category_id = p.category_id
                JOIN sales_invoice si ON si.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.stock_type = 'sales'
                  AND s.quantity < 0
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY c.category_id, c.title
                ORDER BY total_sold_qty DESC
                LIMIT 1
            """,
            
            "lowest_sales_category": """
                SELECT c.title AS category_name,
                    SUM(ABS(s.quantity)) AS total_sold_qty
                FROM stock s
                JOIN products p ON p.product_id = s.product_id
                JOIN products_category c ON c.category_id = p.category_id
                JOIN sales_invoice si ON si.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.stock_type = 'sales'
                  AND s.quantity < 0
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY c.category_id, c.title
                ORDER BY total_sold_qty ASC
                LIMIT 1
            """,
            
            "top_categories": """
                SELECT c.title AS category_name,
                    SUM(ABS(s.quantity)) AS total_sold_qty,
                    SUM(ABS(s.quantity) * si_item.price) AS total_revenue
                FROM stock s
                JOIN products p ON p.product_id = s.product_id
                JOIN products_category c ON c.category_id = p.category_id
                JOIN sales_invoice si ON si.invoice_id = s.invoice_id
                JOIN sales_items si_item ON si_item.product_id = s.product_id AND si_item.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.stock_type = 'sales'
                  AND s.quantity < 0
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY c.category_id, c.title
                ORDER BY total_sold_qty DESC
                LIMIT {limit}
            """,
            
            # ============================================================================
            # PRODUCT QUERIES - BY REVENUE & PROFIT (Document 3)
            # ============================================================================
            "top_products_by_revenue": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity) * (si_item.price - si_item.discount)) AS total_revenue
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON s.invoice_id = si.invoice_id
                JOIN sales_items si_item ON si_item.product_id = s.product_id AND si_item.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_revenue DESC
                LIMIT {limit}
            """,
            
            "highest_revenue_product": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity) * (si_item.price - si_item.discount)) AS total_revenue
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON s.invoice_id = si.invoice_id
                JOIN sales_items si_item ON si_item.product_id = s.product_id AND si_item.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_revenue DESC
                LIMIT 1
            """,
            
            "lowest_revenue_product": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity) * (si_item.price - si_item.discount)) AS total_revenue
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON s.invoice_id = si.invoice_id
                JOIN sales_items si_item ON si_item.product_id = s.product_id AND si_item.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_revenue ASC
                LIMIT 1
            """,
            
            "highest_profit_product": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity) * ((si_item.price - si_item.discount) - s.cost)) AS total_profit
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON s.invoice_id = si.invoice_id
                JOIN sales_items si_item ON si_item.product_id = s.product_id AND si_item.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_profit DESC
                LIMIT 1
            """,
            
            "lowest_profit_product": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity) * ((si_item.price - si_item.discount) - s.cost)) AS total_profit
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON s.invoice_id = si.invoice_id
                JOIN sales_items si_item ON si_item.product_id = s.product_id AND si_item.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_profit ASC
                LIMIT 1
            """,
            
            "top_products_by_profit": """
                SELECT p.name AS product_name,
                    SUM(ABS(s.quantity) * ((si_item.price - si_item.discount) - s.cost)) AS total_profit,
                    SUM(ABS(s.quantity)) AS total_quantity
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                JOIN sales_invoice si ON s.invoice_id = si.invoice_id
                JOIN sales_items si_item ON si_item.product_id = s.product_id AND si_item.invoice_id = s.invoice_id
                WHERE s.company_id = {company_id}
                  AND s.quantity < 0
                  AND s.stock_type = 'sales'
                  AND si.status != 'canceled'
                  {date_filter}
                GROUP BY s.product_id, p.name
                ORDER BY total_profit DESC
                LIMIT {limit}
            """,
            
            # ============================================================================
            # CUSTOMER QUERIES (Document 4)
            # ============================================================================
            "highest_revenue_customers": """
                SELECT c.company AS customer_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_revenue
                FROM sales_invoice si
                JOIN contacts c ON c.contact_id = si.customer_id
                WHERE si.company_id = {company_id}
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY c.contact_id, c.company
                ORDER BY total_revenue DESC
                LIMIT {limit}
            """,
            
            "lowest_revenue_customers": """
                SELECT c.company AS customer_name,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS total_revenue
                FROM sales_invoice si
                JOIN contacts c ON c.contact_id = si.customer_id
                WHERE si.company_id = {company_id}
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY c.contact_id, c.company
                ORDER BY total_revenue ASC
                LIMIT {limit}
            """,
            
            "customer_wise_sales": """
                SELECT c.company AS customer_name,
                    COUNT(si.invoice_id) AS total_invoices,
                    SUM(si.total) AS gross_sales,
                    SUM(COALESCE(si.total_tax, 0)) AS total_tax,
                    SUM(si.total - COALESCE(si.total_tax, 0)) AS net_sales
                FROM sales_invoice si
                JOIN contacts c ON c.contact_id = si.customer_id
                WHERE si.company_id = {company_id}
                  AND si.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                  {date_filter}
                GROUP BY c.contact_id, c.company
                ORDER BY net_sales DESC
                LIMIT {limit}
            """,
            
            "inactive_customers_30_days": """
                SELECT c.company AS customer_name,
                    MAX(si_all.invoice_date) AS last_invoice_date
                FROM contacts c
                LEFT JOIN sales_invoice si_recent
                    ON si_recent.customer_id = c.contact_id
                    AND si_recent.invoice_date >= CURDATE() - INTERVAL 30 DAY
                    AND si_recent.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                    AND si_recent.company_id = {company_id}
                LEFT JOIN sales_invoice si_all
                    ON si_all.customer_id = c.contact_id
                    AND si_all.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                    AND si_all.company_id = {company_id}
                WHERE c.company_id = {company_id}
                  AND c.is_active = 1
                  AND si_recent.invoice_id IS NULL
                GROUP BY c.contact_id, c.company
                HAVING MAX(si_all.invoice_date) IS NOT NULL
                ORDER BY last_invoice_date DESC
                LIMIT {limit}
            """,
            
            "inactive_customers_60_days": """
                SELECT c.company AS customer_name,
                    MAX(si_all.invoice_date) AS last_invoice_date
                FROM contacts c
                LEFT JOIN sales_invoice si_recent
                    ON si_recent.customer_id = c.contact_id
                    AND si_recent.invoice_date >= CURDATE() - INTERVAL 60 DAY
                    AND si_recent.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                    AND si_recent.company_id = {company_id}
                LEFT JOIN sales_invoice si_all
                    ON si_all.customer_id = c.contact_id
                    AND si_all.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                    AND si_all.company_id = {company_id}
                WHERE c.company_id = {company_id}
                  AND c.is_active = 1
                  AND si_recent.invoice_id IS NULL
                GROUP BY c.contact_id, c.company
                HAVING MAX(si_all.invoice_date) IS NOT NULL
                ORDER BY last_invoice_date DESC
                LIMIT {limit}
            """,
            
            "inactive_customers_90_days": """
                SELECT c.company AS customer_name,
                    MAX(si_all.invoice_date) AS last_invoice_date
                FROM contacts c
                LEFT JOIN sales_invoice si_recent
                    ON si_recent.customer_id = c.contact_id
                    AND si_recent.invoice_date >= CURDATE() - INTERVAL 90 DAY
                    AND si_recent.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                    AND si_recent.company_id = {company_id}
                LEFT JOIN sales_invoice si_all
                    ON si_all.customer_id = c.contact_id
                    AND si_all.status NOT IN ('draft', 'draft_return', 'return', 'canceled')
                    AND si_all.company_id = {company_id}
                WHERE c.company_id = {company_id}
                  AND c.is_active = 1
                  AND si_recent.invoice_id IS NULL
                GROUP BY c.contact_id, c.company
                HAVING MAX(si_all.invoice_date) IS NOT NULL
                ORDER BY last_invoice_date DESC
                LIMIT {limit}
            """,
        }

    def _classify_intent(self, message):
        """Use LLM to classify user intent and extract parameters"""
        
        # List all available query types
        available_queries = list(self.query_templates.keys())
        
        prompt = f"""Analyze this sales query and classify the intent.

USER QUERY: "{message}"

AVAILABLE QUERY TYPES (choose the most specific one):
{json.dumps(available_queries, indent=2)}

Extract and return ONLY a JSON object:
{{
    "query_type": "<one of the available query types>",
    "time_period": "<today|this_month|this_year|last_X_days|all_time>",
    "limit": <number if specified like "top 5", "top 10", otherwise 10>,
    "days": <number of days if applicable>,
    "confidence": <0.0 to 1.0>
}}

MATCHING RULES:
- "returns today" → returns_today
- "sales this month" → sales_this_month  
- "top 5 products by value/revenue/money" → top_products_by_revenue (limit=5)
- "top products by quantity/units sold" → top_products_by_quantity
- "most profitable products" → top_products_by_profit
- "inactive customers 60 days" → inactive_customers_60_days
- "slow moving products" → slow_moving_products
- "best salesperson" → highest_salesperson
- "worst branch" → lowest_sales_branch
- "sales trend" → sales_trend_12_months
- "compare this month vs last month" → compare_month
- "net sales" → net_sales_[period]

Return ONLY the JSON, no other text."""

        try:
            response = self._call_groq(prompt, max_tokens=500)
            response = response.strip()
            response = re.sub(r'^```json\s*', '', response)
            response = re.sub(r'\s*```$', '', response)
            
            intent = json.loads(response)
            
            if intent.get('query_type') not in self.query_templates:
                print(f"⚠️ Unknown query type: {intent.get('query_type')}, defaulting")
                intent['query_type'] = 'sales_this_month'
            
            intent.setdefault('limit', 10)
            intent.setdefault('days', 30)
            intent.setdefault('confidence', 0.0)
            
            return intent
            
        except Exception as e:
            print(f"Error classifying intent: {e}")
            return {
                "query_type": "sales_this_month",
                "time_period": "this_month",
                "limit": 10,
                "days": 30,
                "confidence": 0.0
            }

    def _get_date_filter(self, time_period):
        """Generate date filter SQL fragment"""
        if time_period == "today":
            return "AND si.invoice_date >= CURDATE() AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
        elif time_period == "this_month":
            return "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
        elif time_period == "this_year":
            return "AND si.invoice_date >= DATE_FORMAT(CURDATE(), '%Y-01-01') AND si.invoice_date < CURDATE() + INTERVAL 1 DAY"
        elif time_period.startswith("last_"):
            days = time_period.replace("last_", "").replace("_days", "")
            return f"AND si.invoice_date >= CURDATE() - INTERVAL {days} DAY"
        else:
            return ""

    def _get_date_label(self, time_period):
        """Get human-readable label"""
        labels = {
            "today": "Today",
            "this_month": "This Month",
            "this_year": "This Year",
            "last_30_days": "Last 30 Days",
            "last_60_days": "Last 60 Days",
            "last_90_days": "Last 90 Days",
            "all_time": "All Time"
        }
        return labels.get(time_period, time_period.replace("_", " ").title())

    def process_query(self, message, company_id):
        """Main query processor"""
        try:
            # Step 1: LLM classifies intent
            intent = self._classify_intent(message)
            print(f"🎯 Intent: {json.dumps(intent, indent=2)}")
            
            # Step 2: Get hardcoded SQL template
            query_type = intent['query_type']
            sql_template = self.query_templates[query_type]
            
            # Step 3: Build date filter
            date_filter = self._get_date_filter(intent['time_period'])
            
            # Step 4: Format SQL with parameters
            sql_query = sql_template.format(
                company_id=company_id,
                limit=intent['limit'],
                days=intent['days'],
                date_filter=date_filter
            )
            
            print(f"📝 Query type: {query_type}")
            print(f"🔍 SQL:\n{sql_query.strip()}")
            
            # Step 5: Execute
            result = db.execute_query(sql_query, ())
            
            # Step 6: Format results
            date_label = self._get_date_label(intent['time_period'])
            formatted_response = self._format_results(
                message, result, {'label': date_label}, query_type
            )
            
            # Add transparency
            formatted_response += f"\n\n---\n**🎯 Query Type:** `{query_type}`"
            formatted_response += f"\n**📊 Limit:** {intent['limit']}"
            formatted_response += f"\n**🔍 Confidence:** {intent['confidence']:.0%}"
            formatted_response += f"\n\n**SQL:**\n```sql\n{sql_query.strip()}\n```"
            
            return formatted_response
            
        except Exception as e:
            print(f"❌ Error: {e}")
            error_msg = f"❌ Error: {str(e)}\n\n"
            if 'sql_query' in locals():
                error_msg += f"**SQL:**\n```sql\n{sql_query}\n```"
            return error_msg

    def _format_results(self, user_question, results, date_context, query_type):
        """Format results using LLM"""
        
        if not results or len(results) == 0:
            return f"ℹ️ **No data found.**\n**📅 Period:** {date_context['label']}"

        results_json = json.dumps(results[:20], default=str, indent=2)
        
        prompt = f"""Format this sales data into a clear summary.

USER QUESTION: {user_question}
QUERY TYPE: {query_type}
PERIOD: {date_context['label']}
RESULTS:
{results_json}

RULES:
1. Bold header with emoji
2. For single values: clean metric card
3. For lists: formatted table
4. Money: $1,234.56 | Quantities: 1,234 units | Counts: 1,234
5. Add ONE brief actionable insight
6. Keep concise

Generate summary:"""

        try:
            formatted_text = self._call_groq(prompt, max_tokens=800)
            return formatted_text
        except Exception as e:
            print(f"Formatting error: {e}")
            return self._basic_format(results, date_context)

    def _basic_format(self, results, date_context):
        """Fallback formatting"""
        response = f"**📊 RESULTS** | **📅 {date_context['label']}**\n\n"
        
        if len(results) == 1:
            for key, value in results[0].items():
                formatted_key = key.replace('_', ' ').title()
                if isinstance(value, (int, float)):
                    if any(k in key.lower() for k in ['revenue', 'sales', 'total', 'amount']):
                        response += f"💰 **{formatted_key}:** ${value:,.2f}\n"
                    else:
                        response += f"**{formatted_key}:** {value:,.0f}\n"
                else:
                    response += f"**{formatted_key}:** {value}\n"
        else:
            for idx, row in enumerate(results[:10], 1):
                items = [f"{k}: {v}" for k, v in row.items()]
                response += f"{idx}. " + " | ".join(items) + "\n"
        
        return response

    # Compatibility methods
    def get_sales_today(self, company_id, date_range=None):
        return self.process_query("What are my total sales today?", company_id)

    def get_sales_this_month(self, company_id, date_range=None):
        return self.process_query("What are my total sales this month?", company_id)

    def get_top_selling_products(self, company_id, date_range=None):
        return self.process_query("What are my top-selling products?", company_id)

    def get_inactive_customers_30_days(self, company_id, date_range=None):
        return self.process_query("Which customers have not purchased in last 30 days?", company_id)


if __name__ == "__main__":
    agent = SalesAgent()
    