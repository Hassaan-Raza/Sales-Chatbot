"""
Database Connection Module - READ ONLY
Highest security standards with SQL injection prevention
"""
import mysql.connector
import streamlit as st
from mysql.connector import Error
import pandas as pd
import os
from dotenv import load_dotenv
import re

load_dotenv()


class DatabaseConnection:
    """Secure read-only database connection with validation"""

    def __init__(self):
        self.connection = None
        self.current_company_id = None

        # Load config with validation
        self.config = {
            'host': st.secrets["DB_HOST"],
            'database': st.secrets["DB_NAME"],
            'user': st.secrets["DB_USER"],
            'password': st.secrets["DB_PASSWORD"],
            'port': int(self._get_config(st.secrets["DB_PORT"], '3306')),
            'connection_timeout': 30,
            'connect_timeout': 30,
            'use_pure': True,
            'buffered': True
        }

        # Validate all required fields
        required = ['host', 'database', 'user', 'password']
        missing = [k for k in required if not self.config.get(k)]
        if missing:
            raise ValueError(f"Missing required database configuration: {', '.join(missing)}")

    def _get_config(self, key, default=None):
        """Get config from Streamlit secrets or environment variables"""
        try:
            return st.secrets.get(key, os.getenv(key, default))
        except:
            return os.getenv(key, default)

    def set_company_id(self, company_id):
        """Set and validate company context"""
        try:
            self.current_company_id = int(company_id)
        except (ValueError, TypeError):
            raise ValueError(f"Invalid company_id: {company_id}. Must be numeric.")

    def get_connection(self):
        """Get or create database connection with retry logic"""
        try:
            if self.connection is None:
                self.connection = mysql.connector.connect(**self.config)
                if self.connection.is_connected():
                    return self.connection
                return None
            else:
                # Verify existing connection
                try:
                    self.connection.ping(reconnect=True, attempts=3, delay=5)
                    if self.connection.is_connected():
                        return self.connection
                except Error:
                    # Reconnect if ping fails
                    self.connection = mysql.connector.connect(**self.config)
                    return self.connection if self.connection.is_connected() else None
        except Error as e:
            st.error(f"Database connection error: {str(e)}")
            self.connection = None
            return None

    def close_connection(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
        self.connection = None

    def execute_query(self, query, params=None):
        """
        Execute READ-ONLY query with security validation

        Args:
            query: SQL query string with %s placeholders
            params: Tuple of parameters for the query

        Returns:
            List of dictionaries with query results
        """
        # SECURITY: Block all write operations
        self._validate_read_only(query)

        # Get connection
        connection = self.get_connection()
        if not connection:
            return None

        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            st.error(f"Query execution error: {str(e)}")
            self.close_connection()
            return None

    def execute_query_dataframe(self, query, params=None):
        """Execute query and return as pandas DataFrame"""
        result = self.execute_query(query, params)
        return pd.DataFrame(result) if result else pd.DataFrame()

    def _validate_read_only(self, query):
        """
        Validate query is read-only (SELECT only)
        Raises exception if write operations detected
        """
        # Remove string literals to avoid false positives
        string_pattern = r'(\"[^\"]*\"|\'[^\']*\')'
        query_clean = re.sub(string_pattern, "''", query)

        # Remove comments
        comment_pattern = r'(--[^\n]*|/\*.*?\*/)'
        query_clean = re.sub(comment_pattern, '', query_clean, flags=re.DOTALL)

        # Check for write operations
        query_upper = query_clean.upper().strip()
        write_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP',
            'CREATE', 'ALTER', 'TRUNCATE', 'REPLACE'
        ]

        statements = [s.strip() for s in query_upper.split(';') if s.strip()]
        for statement in statements:
            first_word = statement.split()[0] if statement.split() else ''
            if first_word in write_keywords:
                raise Exception(
                    f"Security violation: Write operation '{first_word}' blocked. "
                    f"This is a read-only connection."
                )


# Global database instance
db = DatabaseConnection()