"""
Sales AI Chatbot - Clean, CEO-Ready Interface
Simplified design for business users
"""
import streamlit as st
from agents.sales_agent import SalesAgent
from database.db_connection import db

# Initialize
sales_agent = SalesAgent()

# Page config
st.set_page_config(
    page_title="Sales AI Assistant",
    page_icon="🤖",
    layout="centered",  # Centered for cleaner look
    initial_sidebar_state="collapsed"  # Hide sidebar completely
)

# Minimal custom CSS
st.markdown("""
<style>
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Clean header */
    .main-header {
        font-size: 2rem;
        font-weight: 600;
        color: #1a1a1a;
        margin-bottom: 1rem;
        text-align: center;
    }

    /* Company selector styling */
    .company-selector {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 2rem;
    }

    /* Chat container */
    .stChatMessage {
        padding: 1rem;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)


def get_available_companies():
    """Get ALL companies with sales data"""
    try:
        # Get ALL companies that have sales data
        query = """
                SELECT DISTINCT company_id
                FROM sales_items
                WHERE company_id IS NOT NULL
                ORDER BY company_id
                """
        result = db.execute_query(query)

        if result and len(result) > 0:
            all_companies = [str(company['company_id']) for company in result]
            print(f"✅ Loaded {len(all_companies)} companies with sales data")
            return all_companies

        # Fallback if query fails
        print("⚠️ Failed to load companies, using fallback")
        return ["922", "1336", "1387", "1415"]

    except Exception as e:
        print(f"❌ Error loading companies: {e}")
        return ["922", "1336", "1387", "1415"]


def main():
    # Header
    st.markdown('<div class="main-header">🤖 Sales AI Assistant</div>', unsafe_allow_html=True)

    # Company Selection (Top of page, minimal)
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            available_companies = get_available_companies()
            selected_company = st.selectbox(
                "Company",
                available_companies,
                index=0,
                help="Select your company to analyze sales data",
                label_visibility="collapsed"
            )

            # Subtle company indicator
            st.caption(f"📊 Analyzing data for Company {selected_company}")

    # Set company context
    try:
        db.set_company_id(selected_company)
    except ValueError as e:
        st.error(f"Invalid company ID: {e}")
        st.stop()

    st.markdown("---")

    # Chat Interface
    chat_interface(selected_company)


def chat_interface(company_id):
    """Clean chat interface with OpenRouter-powered AI"""

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": f"""👋 Hello! I'm your Sales AI Assistant for **Company {company_id}**.

I can help you with:
• 📊 **Sales Overview** - Revenue, orders, and performance metrics
• 💰 **Profit Analysis** - Margins, top products, and financial insights
• 👥 **Customer Insights** - Customer behavior and retention
• 📦 **Product Performance** - Best sellers and inventory analysis
• 📈 **Sales Trends** - Growth patterns and forecasts
• 🚀 **Recommendations** - AI-driven strategies to maximize profit

**Powered by OpenRouter LLM** - Ask me anything in natural language!

What would you like to know?"""
            }
        ]

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask me anything about your sales data..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.markdown(prompt)

        # Get AI response
        with st.chat_message("assistant"):
            with st.spinner("🤖 Analyzing your data with AI..."):
                try:
                    # Use the OpenRouter-powered agent
                    response = sales_agent.process_query(prompt, company_id)

                    # Check if it's a credit error
                    if "CREDITS EXHAUSTED" in response or "402" in response:
                        st.error("⚠️ **OpenRouter Credits Exhausted**")
                        st.warning("""
                        Your credits have been exhausted.
                        """)
                        response = "⚠️ **API Credits Exhausted** - Please add credits to continue using the AI assistant."

                except Exception as e:
                    error_msg = str(e)
                    if "402" in error_msg or "CREDITS EXHAUSTED" in error_msg:
                        st.error("⚠️ **Credits Exhausted**")
                        st.info("Your credits have been exhausted.")
                        response = "⚠️ **API Credits Exhausted** - Please add credits to continue."
                    else:
                        response = f"❌ Sorry, I encountered an error: {error_msg}\n\nPlease try rephrasing your question."

                st.markdown(response)

        # Add assistant response to history
        st.session_state.messages.append({"role": "assistant", "content": response})

    # Show tips in sidebar if user wants
    with st.expander("💡 Example Questions", expanded=False):
        st.markdown("""
        **Sales Queries:**
        - What are my total sales today?
        - Show me sales trend for last 12 months
        - Compare this month vs last month
        
        **Customer Queries:**
        - Who are my top 10 customers?
        - Which customers haven't purchased in 90 days?
        - Show me repeat buyers
        
        **Product Queries:**
        - What products have zero sales?
        - Which products have the highest margin?
        - Show me slow-moving products
        
        **Financial Queries:**
        - What is my gross profit?
        - Show receivables aging 0-30 days
        - What's the average invoice value?
        """)

    # Add clear chat button
    if st.button("🗑️ Clear Chat History"):
        st.session_state.messages = [st.session_state.messages[0]]  # Keep welcome message
        st.rerun()


if __name__ == "__main__":
    main()