import streamlit as st
import time
import json
import logging
import sys
import os
import asyncio
import pandas as pd
import msal
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# --- ASYNCIO FIX FOR WINDOWS ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# --- URL ROUTING FUNCTIONS ---
def get_current_route():
    """Get the current route from URL parameters"""
    query_params = st.query_params
    route = query_params.get("page", "")
    
    # Handle legacy URLs and default routing
    if not route:
        # Check if user is authenticated
        if st.session_state.get("auth_logged_in", False):
            return "home"
        else:
            return "login"
    
    return route

def navigate_to(route):
    """Navigate to a specific route"""
    st.query_params["page"] = route
    st.rerun()

def get_base_url():
    """Get the base URL for the application"""
    return "http://localhost:8005/retail-agent"

def update_browser_url(route):
    """Update browser URL to reflect the current route using query parameters"""
    if route == "login":
        target_path = "/retail-agent?page=login"
        display_text = "Retail Agent - Login"
    elif route == "home":
        target_path = "/retail-agent?page=home"
        display_text = "Retail Agent - Dashboard"
    else:
        target_path = "/retail-agent"
        display_text = "Retail Agent"
    
    # Use JavaScript to update the browser URL and title
    st.markdown(f"""
    <script>
        (function() {{
            const currentSearch = window.location.search;
            const targetSearch = "{target_path.split('?')[1] if '?' in target_path else ''}";
            const newSearch = targetSearch ? "?" + targetSearch : "";
            
            if (currentSearch !== newSearch) {{
                const newUrl = window.location.protocol + "//" + window.location.host + "/retail-agent" + newSearch;
                window.history.replaceState(null, null, newUrl);
            }}
            
            document.title = "{display_text}";
        }})();
    </script>
    """, unsafe_allow_html=True)

# --- LOAD ENVIRONMENT VARIABLES ---
def load_env_from_file():
    """Load environment variables from setup_aws_env.sh"""
    env_file = os.path.join(os.path.dirname(__file__), 'setup_aws_env.sh')
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('export '):
                    line = line[7:]
                    if '=' in line:
                        key, value = line.split('=', 1)
                        value = value.strip('"').strip("'")
                        if '#' in value:
                            value = value.split('#')[0].strip()
                        value = value.strip('"').strip("'")
                        os.environ[key] = value


load_env_from_file()

# --- MICROSOFT AUTH CONFIG ---
MICROSOFT_CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID", "")
MICROSOFT_TENANT_ID = os.getenv("MICROSOFT_TENANT_ID", "")
MICROSOFT_CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET", "")
MICROSOFT_REDIRECT_URI = os.getenv("MICROSOFT_REDIRECT_URI")

# New MSAL redirect URI configuration
REDIRECT_URI = os.getenv("MSAL_REDIRECT_URI")

AUTHORITY = f"https://login.microsoftonline.com/{MICROSOFT_TENANT_ID}" if MICROSOFT_TENANT_ID else ""
SCOPE = ["User.Read"]


def build_msal_app():
    return msal.ConfidentialClientApplication(
        client_id=MICROSOFT_CLIENT_ID,
        authority=AUTHORITY,
        client_credential=MICROSOFT_CLIENT_SECRET
    )


def init_auth_session():
    if "auth_logged_in" not in st.session_state:
        st.session_state.auth_logged_in = False
    if "user_name" not in st.session_state:
        st.session_state.user_name = ""
    if "user_email" not in st.session_state:
        st.session_state.user_email = ""
    if "access_token" not in st.session_state:
        st.session_state.access_token = None


def logout():
    st.session_state.auth_logged_in = False
    st.session_state.user_name = ""
    st.session_state.user_email = ""
    st.session_state.access_token = None
    # Navigate to login page after logout
    navigate_to("login")


def render_signin_page():
    # Update browser URL to show login route
    update_browser_url("login")
    
    if not MICROSOFT_CLIENT_ID or not MICROSOFT_TENANT_ID or not MICROSOFT_CLIENT_SECRET:
        st.error("Microsoft authentication is not configured. Please check setup_aws_env.sh")
        st.stop()

    msal_app = build_msal_app()
    # Use MSAL_REDIRECT_URI if available, otherwise fall back to MICROSOFT_REDIRECT_URI
    redirect_uri = REDIRECT_URI if REDIRECT_URI else MICROSOFT_REDIRECT_URI
    auth_url = msal_app.get_authorization_request_url(
        scopes=SCOPE,
        redirect_uri=redirect_uri,
        prompt="select_account"
    )

    st.markdown("""
    <style>
        .login-wrapper {
            max-width: 500px;
            margin: 80px auto;
            padding: 40px;
            border-radius: 20px;
            background: rgba(10, 15, 20, 0.94);
            border: 1px solid rgba(0, 204, 255, 0.22);
            box-shadow: 0 0 30px rgba(0, 204, 255, 0.12);
            text-align: center;
        }
        .login-badge {
            display: inline-block;
            margin-bottom: 14px;
            padding: 6px 12px;
            border-radius: 999px;
            background: rgba(0,255,65,0.08);
            border: 1px solid rgba(0,255,65,0.2);
            color: #00ff41;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
        }
        .login-title {
            font-size: 2.1rem;
            font-weight: 800;
            margin-bottom: 8px;
            color: white;
            letter-spacing: 1px;
            font-family: 'Orbitron', sans-serif;
            text-transform: uppercase;
        }
        .login-subtitle {
            color: #b8c4d6;
            margin-bottom: 26px;
            font-size: 0.98rem;
            font-family: 'Inter', sans-serif;
        }
        .ms-login-btn {
            display: inline-block;
            width: 100%;
            padding: 14px 18px;
            border-radius: 12px;
            background: linear-gradient(135deg, #2563eb, #0ea5e9);
            color: white !important;
            text-decoration: none !important;
            font-weight: 700;
            font-size: 1rem;
            font-family: 'Inter', sans-serif;
            box-sizing: border-box;
        }
        .ms-login-btn:hover {
            opacity: 0.95;
            box-shadow: 0 0 20px rgba(14, 165, 233, 0.35);
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="login-wrapper">
        <div class="login-badge">RETAIL_AGENT // SECURE ACCESS</div>
        <div class="login-title">Microsoft Sign In</div>
        <div class="login-subtitle">
            Sign in with your Microsoft organization account to access the Retail Agent dashboard.
        </div>
        <a class="ms-login-btn" href="{auth_url}">Sign in with Microsoft</a>
    </div>
    """, unsafe_allow_html=True)


def handle_auth_callback():
    query_params = st.query_params

    if st.session_state.auth_logged_in:
        return True

    if "code" not in query_params:
        return False

    auth_code = query_params.get("code")

    try:
        msal_app = build_msal_app()
        # Use MSAL_REDIRECT_URI if available, otherwise fall back to MICROSOFT_REDIRECT_URI
        redirect_uri = REDIRECT_URI if REDIRECT_URI else MICROSOFT_REDIRECT_URI
        token_result = msal_app.acquire_token_by_authorization_code(
            code=auth_code,
            scopes=SCOPE,
            redirect_uri=redirect_uri
        )

        if "access_token" in token_result:
            claims = token_result.get("id_token_claims", {})

            st.session_state.auth_logged_in = True
            st.session_state.access_token = token_result["access_token"]
            st.session_state.user_name = claims.get("name", "Authenticated User")
            st.session_state.user_email = (
                claims.get("preferred_username")
                or claims.get("email")
                or "No email found"
            )

            # Clear query params and navigate to home
            st.query_params.clear()
            navigate_to("home")
        else:
            st.error("Microsoft login failed.")
            st.write(token_result.get("error_description", "Unknown authentication error"))
            return False

    except Exception as e:
        st.error(f"Authentication error: {str(e)}")
        return False

    return st.session_state.auth_logged_in


def require_microsoft_login():
    init_auth_session()
    
    current_route = get_current_route()
    
    # Handle authentication callback
    if handle_auth_callback():
        # If authenticated and on login route, redirect to home
        if current_route == "login" and st.session_state.auth_logged_in:
            navigate_to("home")
        return True

    # If not authenticated and trying to access home, redirect to login
    if not st.session_state.auth_logged_in:
        if current_route == "home":
            navigate_to("login")
        render_signin_page()
        st.stop()

    return True


# --- BACKEND IMPORTS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    import az_scraper
    import flipkart_scraper
    import similarity
except ImportError as e:
    st.error(f"Failed to import backend modules: {e}")
    st.stop()

# Configure logging to show in terminal
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- CUSTOM LOG HANDLER ---
class StreamlitLogHandler(logging.Handler):
    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder
        self.logs = []
        self.formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%H:%M:%S'
        )

    def emit(self, record):
        try:
            msg = self.format(record)
            if record.levelno >= logging.ERROR:
                msg = f"<span style='color:#ff0033'>{msg}</span>"
            elif record.levelno >= logging.WARNING:
                msg = f"<span style='color:#ffcc00'>{msg}</span>"
            else:
                msg = f"<span style='color:#00ff41'>{msg}</span>"

            self.logs.append(msg)
            if len(self.logs) > 100:
                self.logs.pop(0)

            log_html = "<br>".join(self.logs)
            self.placeholder.markdown(f"""
                <div class="terminal-box">
                    {log_html}<br>
                    <span style='animation: blink 1s infinite;'>_</span>
                </div>
            """, unsafe_allow_html=True)
        except Exception:
            self.handleError(record)


# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="RETAIL_AGENT // V2",
    layout="wide",
    initial_sidebar_state="collapsed",
    page_icon="👁️"
)

require_microsoft_login()

# --- ROUTING LOGIC ---
current_route = get_current_route()

# Route to appropriate page
if current_route == "login":
    # Update browser URL to show login route
    update_browser_url("login")
    # If authenticated user tries to access login, redirect to home
    if st.session_state.get("auth_logged_in", False):
        navigate_to("home")
    else:
        # This should not happen as require_microsoft_login handles login page
        st.stop()
elif current_route == "home":
    # Update browser URL to show home route
    update_browser_url("home")
    # Continue to main application content below
else:
    # Default route - redirect based on authentication status
    if st.session_state.get("auth_logged_in", False):
        navigate_to("home")
    else:
        navigate_to("login")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=Orbitron:wght@400;700;900&family=JetBrains+Mono:wght@400;700&display=swap');

    :root {
        --primary-color: #00ff41;
        --secondary-color: #00ccff;
        --accent-color: #6d28d9;
        --bg-color: #050505;
        --card-bg: #0a0f14;
        --text-color: #e0e0e0;
        --font-main: 'Inter', sans-serif;
        --font-header: 'Orbitron', sans-serif;
        --font-mono: 'JetBrains Mono', monospace;
    }

    .stApp {
        background-color: #030305;
        background-image:
            radial-gradient(at 0% 0%, rgba(124, 58, 237, 0.15) 0%, transparent 50%),
            radial-gradient(at 100% 0%, rgba(0, 204, 255, 0.15) 0%, transparent 50%),
            radial-gradient(at 50% 100%, rgba(0, 255, 65, 0.1) 0%, transparent 50%),
            linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
            linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px);
        background-size: 100% 100%, 100% 100%, 100% 100%, 50px 50px, 50px 50px;
        background-attachment: fixed;
    }

    .block-container {
        padding-top: 3rem;
        padding-bottom: 3rem;
        max-width: 95rem;
    }

    h1, h2, h3, h4, h5, h6 {
        font-family: var(--font-header) !important;
        color: white !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    p, label, .stMarkdown, .stText {
        font-family: var(--font-main) !important;
        color: var(--text-color) !important;
    }

    .stTextInput > div > div > input,
    .stNumberInput > div > div > input {
        background-color: var(--card-bg);
        color: var(--secondary-color);
        border: 1px solid #333;
        border-radius: 4px;
        font-family: var(--font-mono);
        padding: 0.5rem;
        transition: all 0.3s ease;
    }

    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus {
        border-color: var(--secondary-color);
        box-shadow: 0 0 15px rgba(0, 204, 255, 0.2);
    }

    div[data-baseweb="select"] > div {
        background-color: var(--card-bg) !important;
        border: 1px solid #333 !important;
        border-radius: 4px !important;
        color: white !important;
    }

    div[data-baseweb="select"] > div:hover {
        border-color: var(--primary-color) !important;
    }

    div[data-baseweb="popover"] {
        background-color: #000 !important;
        border: 1px solid #333 !important;
    }

    div[data-baseweb="menu"] {
        background-color: #000 !important;
    }

    li[data-baseweb="menu-item"] {
        color: #bbb !important;
        font-family: var(--font-mono) !important;
    }

    li[data-baseweb="menu-item"]:hover {
        background-color: #1a1f26 !important;
        color: var(--primary-color) !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] span {
        color: var(--primary-color) !important;
        font-family: var(--font-mono) !important;
    }

    .stButton > button {
        width: 100%;
        background: linear-gradient(135deg, var(--accent-color), #0ea5e9);
        color: white;
        font-family: var(--font-header);
        font-weight: bold;
        border: none;
        border-radius: 4px;
        height: 3.5em;
        transition: all 0.3s ease;
        text-shadow: 0 1px 2px rgba(0,0,0,0.3);
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 0 20px rgba(14, 165, 233, 0.6);
        background: linear-gradient(135deg, #7c3aed, #38bdf8);
    }

    .stButton > button:active {
        transform: translateY(0);
    }

    .streamlit-expanderHeader {
        background-color: #0f1520 !important;
        border: 1px solid #333 !important;
        border-radius: 4px !important;
        color: white !important;
        font-family: var(--font-mono) !important;
    }

    .streamlit-expanderContent {
        background-color: rgba(10, 15, 20, 0.7) !important;
        border: 1px solid #333;
        border-top: none;
        border-radius: 0 0 4px 4px !important;
        backdrop-filter: blur(10px);
    }

    .terminal-box {
        background-color: #000000;
        border: 1px solid #333;
        border-left: 3px solid var(--primary-color);
        padding: 15px;
        font-family: var(--font-mono);
        font-size: 13px;
        color: var(--primary-color);
        height: 350px;
        overflow-y: auto;
        box-shadow: inset 0 0 30px rgba(0,0,0,0.5);
    }

    div[data-testid="stMetricValue"] {
        font-family: var(--font-header);
        color: var(--primary-color);
        text-shadow: 0 0 15px rgba(0, 255, 65, 0.4);
        font-size: 2.5rem !important;
    }

    div[data-testid="stMetricLabel"] {
        font-family: var(--font-mono);
        color: #888;
        font-size: 0.9rem;
        text-transform: uppercase;
    }

    div[data-testid="stToast"] {
        background-color: var(--card-bg) !important;
        border: 1px solid var(--primary-color) !important;
        color: var(--primary-color) !important;
        font-family: var(--font-mono) !important;
        box-shadow: 0 0 15px rgba(0, 255, 65, 0.2) !important;
    }

    div[data-testid="stTable"] {
        font-family: var(--font-mono);
    }

    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }

    ::-webkit-scrollbar-track {
        background: #000;
    }

    ::-webkit-scrollbar-thumb {
        background: #333;
        border-radius: 5px;
    }

    ::-webkit-scrollbar-thumb:hover {
        background: #555;
    }
</style>
""", unsafe_allow_html=True)

# --- HEADER SECTION ---
col1, col2, col3 = st.columns([3, 1, 1])
with col1:
    st.markdown(
        "<h1>RETAIL_<span style='color:#00ccff; text-shadow: 0 0 10px #00ccff;'>AGENT</span> // V2.0</h1>",
        unsafe_allow_html=True
    )

with col2:
    st.markdown(
        f"<div style='text-align:right; font-family:JetBrains Mono; color:#00ff41; padding-top: 1.5rem;'>USER: <b>{st.session_state.user_name}</b></div>",
        unsafe_allow_html=True
    )

with col3:
    st.markdown("<div style='padding-top: 0.8rem;'></div>", unsafe_allow_html=True)
    if st.button("LOGOUT"):
        logout()

st.markdown("---")

# --- CONTROL DECK ---
st.markdown("### INPUT_PARAMETERS")
with st.container():
    c1, c2, c3, c4 = st.columns([3, 3, 2, 2])

    with c1:
        asin_input = st.text_input("AMAZON ASIN", placeholder="e.g. B0DZNRHP1M")
    with c2:
        flipkart_url_input = st.text_input("FLIPKART URL", placeholder="https://flipkart.com/...")
    with c3:
        price_target_input = st.number_input("NUDGE PRICE (₹)", value=24000.0)
    with c4:
        st.write("##")
        run_btn = st.button("INITIALIZE SCAN")

# --- MAIN DASHBOARD LOGIC ---
main_area = st.empty()

if run_btn:
    if not asin_input or not flipkart_url_input:
        st.error("MISSING INPUTS: Please provide both Amazon ASIN and Flipkart URL.")
    else:
        with main_area.container():
            col_term, col_vis = st.columns([1, 2])

            with col_term:
                st.markdown("**LIVE_LOGS**")
                terminal_placeholder = st.empty()

            with col_vis:
                st.markdown("**VISUAL_RECON**")
                visual_placeholder = st.empty()
                visual_placeholder.markdown("""
                    <div style='height:300px; border:1px solid #333; display:flex; align-items:center; justify-content:center; background:#0a0f14;'>
                        <span style='color:#333; font-family:Orbitron;'>CONNECTING...</span>
                    </div>
                """, unsafe_allow_html=True)

        st_handler = StreamlitLogHandler(terminal_placeholder)
        root_logger = logging.getLogger()
        root_logger.addHandler(st_handler)

        logger.info(f"System Initialized. Target ASIN: {asin_input}")

        try:
            logger.info("initiating_amazon_scrape_protocol...")

            clean_asin = asin_input.strip()
            if "amazon" in clean_asin.lower() and "/dp/" in clean_asin.lower():
                extracted = az_scraper.extract_asin_from_url(clean_asin)
                if extracted:
                    clean_asin = extracted
                    logger.info(f"Extracted ASIN from URL: {clean_asin}")

            amazon_url = az_scraper.construct_amazon_url(clean_asin)
            logger.info(f"Constructed URL: {amazon_url}")

            az_start = time.time()
            amazon_data = az_scraper.get_amazon_product_details(amazon_url)
            az_time = time.time() - az_start

            if not amazon_data:
                raise Exception("Amazon scrape returned no data (Soft Block or Invalid ASIN).")

            az_title = amazon_data.get('title', 'Unknown')
            az_price = amazon_data.get('price', 0)
            az_img = amazon_data.get('images', [])[0] if amazon_data.get('images') else None
            az_size = amazon_data.get('size')

            logger.info(f"Amazon Scrape Complete ({az_time:.2f}s)")
            logger.info(f"Found: {az_title[:30]}...")
            logger.info(f"Price: {az_price}")
            logger.info(f"Size Detected: {az_size}")

            if az_img:
                visual_placeholder.image(az_img, caption="SRC: AMAZON", width="stretch")

            logger.info(f"initiating_flipkart_recon... Target Size: {az_size}")

            fk_start = time.time()
            flipkart_data = flipkart_scraper.scrape_single_url(flipkart_url_input, target_size=az_size)
            fk_time = time.time() - fk_start

            if flipkart_data.get('error'):
                error_msg = flipkart_data.get('error')
                error_details = flipkart_data.get('error_details', '')

                logger.error(f"❌ FLIPKART SCRAPING FAILED: {error_msg}")
                if error_details:
                    logger.error(f"Details: {error_details}")

                st.error(f"### ⚠️ FLIPKART SCRAPING FAILED\n\n**Error:** {error_msg}\n\n{error_details}")
                st.warning("**Process stopped** to prevent unnecessary resource usage.")
                st.info("**Possible Solutions:**\n- Wait a few minutes and try again\n- Check if the Flipkart URL is valid\n- Verify the product is still available on Flipkart")

                try:
                    failed_result = {
                        "workflow_status": "SCRAPER_FAILED",
                        "termination_reason": error_msg,
                        "approved_match": False,
                        "overall_similarity_percentage": 0.0,
                        "step_completed": 0,
                        "product_az": amazon_data,
                        "product_ic": {
                            "url": flipkart_url_input,
                            "error": error_msg,
                            "error_details": error_details
                        },
                        "flipkart_url": flipkart_url_input,
                        "amazon_url": amazon_url,
                        "critical_failures": [error_msg],
                        "failed_steps": ["flipkart_scraping"],
                        "dict_parameter_scores": {}
                    }

                    failure_id = similarity.store_result_in_dynamodb(failed_result)
                    if failure_id:
                        logger.info(f"💾 Saved FAILURE record to DynamoDB: {failure_id}")
                except Exception as e:
                    logger.error(f"Failed to log error to DynamoDB: {e}")

                st.stop()

            fk_title = flipkart_data.get('title', 'Unknown')
            fk_price = flipkart_data.get('price', 0)
            fk_img = flipkart_data.get('images', [])[0] if flipkart_data.get('images') else None
            fk_stock = flipkart_data.get('instock', 'Unknown')

            logger.info(f"Flipkart Scrape Complete ({fk_time:.2f}s)")
            logger.info(f"Found: {fk_title[:30]}...")
            logger.info(f"Price: {fk_price}")
            logger.info(f"MRP: {flipkart_data.get('mrp')}")
            logger.info(f"Stock Status: {fk_stock}")

            if fk_img:
                visual_placeholder.image(fk_img, caption="SRC: FLIPKART", width="stretch")

            logger.info("running_similarity_matrix...")

            amazon_data['nudge_price'] = price_target_input
            comp_result = similarity.compare_products(amazon_data, flipkart_data)

            comp_result["flipkart_url"] = flipkart_url_input
            comp_result["amazon_url"] = amazon_url

            try:
                success_id = similarity.store_result_in_dynamodb(comp_result)
                if success_id:
                    logger.info(f"💾 Saved SUCCESS record to DynamoDB: {success_id}")
            except Exception as e:
                logger.error(f"Failed to save result to DynamoDB: {e}")

            logger.info("Analysis Complete.")
            logger.info(f"Match Score: {comp_result.get('overall_similarity_percentage', 0) * 100:.1f}%")

            time.sleep(1)
            main_area.empty()

            with st.container():
                st.markdown("<br>", unsafe_allow_html=True)

                rec_action = comp_result.get('recommendation_action')
                if rec_action == "approve":
                    st.success("SCAN COMPLETE // TARGET MATCHED ✅")
                elif rec_action == "manual_review":
                    reason = comp_result.get('manual_review_reason', 'Ambiguity Detected')
                    st.warning(f"SCAN COMPLETE // MANUAL REVIEW REQUIRED ⚠️\n\n**Reason:** {reason}")
                else:
                    critical = comp_result.get('critical_failures', [])
                    if critical:
                        reasons = [c.replace('_', ' ').upper() for c in critical]
                        st.error(f"SCAN COMPLETE // CRITICAL FAILURE (REJECTED): {', '.join(reasons)} ❌")
                    else:
                        st.error("SCAN COMPLETE // TARGET REJECTED ❌")

                r1, r2, r3 = st.columns([1, 1, 1])
                confidence = comp_result.get('overall_similarity_percentage', 0) * 100

                with r1:
                    st.markdown("### MATCH SCORE")
                    color = "#00ff41" if confidence > 80 else "#ffcc00" if confidence > 50 else "#ff0033"
                    st.markdown(
                        f"<h1 style='font-size: 80px; color: {color}; margin:0;'>{confidence:.1f}%</h1>",
                        unsafe_allow_html=True
                    )
                    st.caption("CONFIDENCE INTERVAL")

                    recommendation = str(comp_result.get('recommendation_action', 'UNKNOWN')).replace('_', ' ').upper()
                    st.markdown(f"**ACTION:** `{recommendation}`")

                    primary_reason = comp_result.get('primary_failure_reason')
                    if primary_reason:
                        st.markdown(f"**PRIMARY REASON:** `{primary_reason}`")

                    with st.expander("DETAILS"):
                        st.json(comp_result.get('dict_parameter_scores', {}))

                    genai_reason = comp_result.get('genai_reason')
                    genai_attributes = comp_result.get('genai_attributes')

                    if genai_reason or genai_attributes:
                        with st.expander("GENAI ANALYSIS"):
                            if genai_reason:
                                st.markdown(f"**Reasoning:**\n{genai_reason}")
                            if genai_attributes:
                                st.markdown("**Attributes:**")
                                st.json(genai_attributes)

                with r2:
                    st.markdown("### AMAZON")
                    if az_img:
                        st.image(az_img, width="stretch")
                    st.markdown(f"**{az_title}**")
                    st.metric("Price", f"₹{az_price}", delta="Target" if not comp_result.get('approved_match') else None)
                    st.caption(f"MRP: ₹{amazon_data.get('mrp', 0)}")

                with r3:
                    st.markdown("### FLIPKART")
                    if comp_result.get('approved_match'):
                        st.markdown(":white_check_mark: **MATCHED**")

                    if fk_img:
                        st.image(fk_img, width="stretch")
                    st.markdown(f"**{fk_title}**")

                    if fk_price is not None and az_price is not None:
                        price_delta = fk_price - az_price
                        delta_color = "normal" if price_delta <= 0 else "inverse"
                        st.metric("Best Price", f"₹{fk_price}", delta=f"{price_delta:.2f}", delta_color=delta_color)
                        st.caption(f"MRP: ₹{flipkart_data.get('mrp', 0)}")
                    else:
                        st.metric("Best Price", f"₹{fk_price or 'N/A'}", delta=None)
                        st.caption(f"MRP: ₹{flipkart_data.get('mrp', 0)}")

                    st.caption(f"Stock: {fk_stock}")

                failed_steps = comp_result.get('failed_steps', [])
                if 'title_similarity' in failed_steps or 'content_similarity' in failed_steps or confidence < 80:
                    st.markdown("---")
                    st.subheader("CONTENT MISMATCH ANALYSIS")
                    st.caption("Side-by-side comparison triggered by low similarity score.")

                    cc1, cc2 = st.columns(2)
                    with cc1:
                        st.markdown("**AMAZON (SOURCE)**")
                        st.info(az_title)
                    with cc2:
                        st.markdown("**FLIPKART (CANDIDATE)**")
                        st.warning(fk_title)

                    param_scores = comp_result.get('dict_parameter_scores', {})
                    genai_attrs = comp_result.get('genai_attributes', {})

                    az_content = (amazon_data.get('content') or amazon_data.get('description') or "")[:200]
                    fk_content = (flipkart_data.get('content') or flipkart_data.get('description') or "")[:200]

                    comparison_data = [
                        {
                            "Field": "📝 Title",
                            "Amazon": az_title,
                            "Flipkart": fk_title,
                            "Status": "✅ PASS" if param_scores.get('title', 0) >= 0.6 else "❌ FAIL",
                            "Score": f"{param_scores.get('title', 0):.3f}"
                        },
                        {
                            "Field": "📄 Content",
                            "Amazon": az_content + ("..." if len(az_content) == 200 else ""),
                            "Flipkart": fk_content + ("..." if len(fk_content) == 200 else ""),
                            "Status": "✅ PASS" if param_scores.get('content', 0) >= 0.6 else "❌ FAIL",
                            "Score": f"{param_scores.get('content', 0):.3f}"
                        },
                        {
                            "Field": "🖼️ Image",
                            "Amazon": comp_result.get('image_comparison', {}).get('genai_analysis', 'N/A')[:120] if comp_result.get('image_comparison') else "N/A",
                            "Flipkart": "",
                            "Status": "✅ PASS" if param_scores.get('image', 0) >= 0.65 else "❌ FAIL",
                            "Score": f"{param_scores.get('image', 0):.3f}"
                        },
                    ]

                    attr_labels = {
                        "brand": "🏷️ Brand",
                        "quantity": "📦 Quantity",
                        "color": "🎨 Color",
                        "gender": "👤 Gender",
                        "item_dimensions": "📏 Dimensions",
                        "item_weight": "⚖️ Weight",
                    }
                    status_icons = {"MATCH": "✅ MATCH", "MISMATCH": "❌ MISMATCH", "UNKNOWN": "❓ UNKNOWN"}

                    for attr_key, label in attr_labels.items():
                        attr_data = genai_attrs.get(attr_key, {})
                        if isinstance(attr_data, dict):
                            status = attr_data.get("status", "UNKNOWN")
                            val_a = str(attr_data.get("value_a", "N/A"))
                            val_b = str(attr_data.get("value_b", "N/A"))
                        else:
                            status = str(attr_data) if attr_data else "UNKNOWN"
                            val_a = "N/A"
                            val_b = "N/A"

                        comparison_data.append({
                            "Field": label,
                            "Amazon": val_a,
                            "Flipkart": val_b,
                            "Status": status_icons.get(status, status),
                            "Score": ""
                        })

                    st.table(comparison_data)

                st.markdown("---")
                st.subheader("FAILURE ANALYSIS")

                failure_details = comp_result.get('failure_details', {})
                combined = comp_result.get('combined_failures', [])

                if not combined:
                    st.success("✅ NO FAILURES DETECTED")
                else:
                    ilp = failure_details.get('incorrect_list_price', [])
                    if ilp:
                        for f in ilp:
                            st.error(f"🏷️ INCORRECT LIST PRICE: {f}")

                    icd = failure_details.get('incorrect_catalog_data', [])
                    if icd:
                        for f in icd:
                            st.warning(f"📋 INCORRECT CATALOG DATA: {f}")

                    cmt = failure_details.get('cmt_mismatch', [])
                    if cmt:
                        for f in cmt:
                            st.error(f"🔴 CMT MISMATCH: {f}")

                    ops = failure_details.get('operational', [])
                    if ops:
                        for f in ops:
                            st.warning(f"⚙️ OPERATIONAL: {f}")

                    st.caption(f"Total failures: {len(combined)} | Combined: {', '.join(combined)}")

                st.markdown("---")
                st.subheader("SIZE ANALYSIS")

                size_data = comp_result.get('size_analysis', {})
                if size_data:
                    sa1, sa2, sa3 = st.columns(3)

                    with sa1:
                        st.markdown("**TARGET (AMAZON)**")
                        target = size_data.get('target', {})
                        st.write(f"Raw: `{target.get('original')}`")
                        st.write(f"Norm: `{target.get('normalized')}`")
                        st.write(f"Type: `{target.get('type')}`")
                        st.caption(f"Equiv: {target.get('equivalents')}")

                    with sa2:
                        st.markdown("**SELECTED (FLIPKART)**")
                        selected = size_data.get('selected', {})
                        st.write(f"Raw: `{selected.get('original')}`")
                        st.write(f"Norm: `{selected.get('normalized')}`")
                        st.write(f"Type: `{selected.get('type')}`")
                        st.caption(f"Equiv: {selected.get('equivalents')}")

                    with sa3:
                        st.markdown("**FK SIZES**")
                        st.caption("Available:")
                        st.write(size_data.get('available_sizes', []))
                        st.caption("Purchasable:")
                        st.write(size_data.get('purchasable_sizes', []))
                        st.markdown(f"**STATUS**: `{size_data.get('selection_status')}`")
                else:
                    st.info("Size analysis data not available for this run.")

                st.markdown("---")
                with st.expander("DATA INSPECTOR (RAW JSON)", expanded=False):
                    tab1, tab2 = st.tabs(["AMAZON DATA", "FLIPKART DATA"])
                    with tab1:
                        st.json(comp_result.get('product_az', {}))
                    with tab2:
                        st.json(comp_result.get('product_ic', {}))

                st.markdown("---")
                report_data = json.dumps(comp_result, indent=2, default=str)
                st.download_button(
                    "DOWNLOAD JSON REPORT",
                    data=report_data,
                    file_name="report.json",
                    mime="application/json"
                )

        except Exception as e:
            logger.critical(f"CRITICAL FAILURE: {str(e)}")
            st.error(f"System Error: {str(e)}")
            logger.error("Scan failed", exc_info=True)

        finally:
            root_logger.removeHandler(st_handler)

else:
    st.info("Awaiting Input... Enter product details above OR use Batch Processing below.")
    st.markdown("---")

    st.markdown("### BATCH_PROCESSING")
    st.markdown("""
    <div style='background: linear-gradient(135deg, rgba(109, 40, 217, 0.1), rgba(14, 165, 233, 0.1));
                border: 1px solid #333; border-radius: 8px; padding: 15px; margin-bottom: 20px;'>
        <p style='color: #00ccff; font-family: JetBrains Mono; font-size: 14px; margin: 0;'>
            📁 <b>DROP FILE TO PROCESS MULTIPLE PRODUCTS</b><br>
            <span style='color: #888;'>Supported formats: CSV, XLSX, XLS</span>
        </p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("📋 EXPECTED FILE FORMAT", expanded=False):
        st.markdown("""
        **Required Columns:**
        - `amazon_asin` - The Amazon ASIN (e.g., B0DZNRHP1M)
        - `flipkart_url` or `fk_link` or `link` - The Flipkart product URL

        **Optional Columns:**
        - `nudge_price` - Target price for comparison (default: 0)
        - `amazon_domain` - Amazon domain (default: 'in')

        **Example CSV:**
        ```
        nudge_price,amazon_asin,flipkart_url,amazon_domain
        499,B0DZNRHP1M,https://flipkart.com/product-url,in
        599,B0XYZ123,https://flipkart.com/another-url,in
        ```
        """)

    uploaded_file = st.file_uploader(
        "DROP OR SELECT FILE",
        type=['csv', 'xlsx', 'xls'],
        help="Upload a CSV or Excel file with product data to process in batch"
    )

    if uploaded_file is not None:
        file_type = uploaded_file.name.split('.')[-1].lower()

        try:
            if file_type == 'csv':
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            df.columns = [col.strip().lower() for col in df.columns]

            st.success(f"✅ File loaded: **{uploaded_file.name}** ({len(df)} products found)")

            with st.expander(f"📊 DATA PREVIEW (All {len(df)} rows)", expanded=True):
                display_df = df.copy()
                display_df.index = range(1, len(df) + 1)
                st.dataframe(display_df)

            req_cols_map = {
                'amazon_asin': 'amazon_asin',
                'nudge_price': 'nudge_price'
            }

            if 'flipkart_url' in df.columns:
                req_cols_map['url'] = 'flipkart_url'
            elif 'fk_link' in df.columns:
                req_cols_map['url'] = 'fk_link'
            elif 'link' in df.columns:
                req_cols_map['url'] = 'link'
            else:
                req_cols_map['url'] = None

            if 'amazon_asin' not in df.columns:
                st.error("❌ Missing required column: `amazon_asin`")
                st.stop()

            if not req_cols_map['url']:
                st.error("❌ Missing required column: `flipkart_url` or `fk_link` or `link`")
                st.stop()

            if 'nudge_price' not in df.columns:
                st.error("❌ Missing required column: `nudge_price`. Please include it in the file.")
                st.stop()

            def is_valid_row(row):
                try:
                    if pd.isna(row['amazon_asin']) or not str(row['amazon_asin']).strip():
                        return False
                    if pd.isna(row[req_cols_map['url']]) or not str(row[req_cols_map['url']]).strip():
                        return False
                    if pd.isna(row['nudge_price']):
                        return False
                    return True
                except Exception:
                    return False

            df['is_valid'] = df.apply(is_valid_row, axis=1)
            valid_df = df[df['is_valid']].copy()
            invalid_df = df[~df['is_valid']].copy()

            batch_cols = st.columns([1, 1, 1, 1, 1])

            with batch_cols[0]:
                max_products = len(valid_df)
                start_row = st.number_input(
                    "START ROW",
                    min_value=1,
                    max_value=max(max_products, 1),
                    value=1,
                    step=1
                )

            with batch_cols[1]:
                end_row = st.number_input(
                    "END ROW",
                    min_value=1,
                    max_value=max(max_products, 1),
                    value=min(max_products, 20) if max_products > 0 else 1,
                    step=1
                )

            with batch_cols[2]:
                st.metric("✅ ACCEPTED", len(valid_df))

            with batch_cols[3]:
                st.metric("❌ REJECTED", len(invalid_df))

            with batch_cols[4]:
                st.write("##")
                if 'stop_batch' not in st.session_state:
                    st.session_state.stop_batch = False
                start_btn = st.button("🚀 START", type="primary")

            if not invalid_df.empty:
                with st.expander("⚠️ VIEW REJECTED CASES (MISSING DATA)"):
                    st.dataframe(invalid_df)

            if start_btn:
                if valid_df.empty:
                    st.warning("No valid rows to process.")
                elif start_row > end_row:
                    st.error(f"❌ Invalid Range: Start Row ({start_row}) cannot be greater than End Row ({end_row}).")
                else:
                    st.session_state.stop_batch = False

                    start_idx = start_row - 1
                    end_idx = end_row
                    df_to_process = valid_df.iloc[start_idx:end_idx].copy()

                    st.markdown("---")
                    st.markdown(f"### BATCH_EXECUTION (Rows {start_row}-{end_row})")
                    st.info("💡 Processing started. To STOP, click the 'Stop' button in the toolbar or close the tab.")

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    batch_terminal = st.empty()
                    batch_logs = []

                    def add_batch_log(msg, level="INFO"):
                        timestamp = datetime.now().strftime('%H:%M:%S')
                        color = "#00ccff"
                        if level == "ERROR":
                            color = "#ff0033"
                        elif level == "WARNING":
                            color = "#ffcc00"
                        elif level == "SUCCESS":
                            color = "#00ff41"

                        batch_logs.append(f"<span style='color:{color}'>[{timestamp}] {msg}</span>")
                        if len(batch_logs) > 50:
                            batch_logs.pop(0)

                        batch_terminal.markdown(f"""
                            <div class="terminal-box">
                                {"<br>".join(batch_logs)}<br>
                                <span style='animation: blink 1s infinite;'>_</span>
                            </div>
                        """, unsafe_allow_html=True)

                    output_dir = os.path.join(os.path.dirname(__file__), 'outputs')
                    os.makedirs(output_dir, exist_ok=True)
                    output_file_name = f"batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    output_path = os.path.join(output_dir, output_file_name)

                    current_results = {
                        'summary': {'total': len(df_to_process), 'processed': 0, 'passed': 0, 'failed': 0, 'errors': 0},
                        'results': [],
                        'timestamp': datetime.now().isoformat()
                    }

                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(current_results, f, ensure_ascii=False, indent=2)

                    passed_count = 0
                    failed_count = 0
                    error_count = 0
                    processed_count = 0
                    start_time = datetime.now()

                    for idx, row in df_to_process.iterrows():
                        processed_count += 1
                        asin = str(row['amazon_asin']).strip()
                        fk_url = str(row[req_cols_map['url']]).strip()
                        nudge = float(row['nudge_price'])
                        domain = str(row.get('amazon_domain', 'in')).strip()

                        progress = processed_count / len(df_to_process)
                        progress_bar.progress(progress)
                        status_text.markdown(f"**Processing [{processed_count}/{len(df_to_process)}]:** `{asin}`")
                        add_batch_log(f"Processing ({processed_count}/{len(df_to_process)}) ASIN: {asin}")

                        result_entry = {
                            'index': idx + 1,
                            'amazon_asin': asin,
                            'flipkart_url': fk_url,
                            'nudge_price': nudge,
                            'status': 'UNKNOWN'
                        }

                        try:
                            clean_asin = asin
                            if "amazon" in clean_asin.lower() and "/dp/" in clean_asin.lower():
                                clean_asin = az_scraper.extract_asin_from_url(clean_asin) or clean_asin

                            amazon_url = az_scraper.construct_amazon_url(clean_asin, domain)
                            amazon_data = az_scraper.get_amazon_product_details(amazon_url)

                            if not amazon_data:
                                raise Exception("Amazon scrape empty")

                            az_size = amazon_data.get('size')
                            az_price = amazon_data.get('price', 0)
                            add_batch_log(f"  → Amazon: ₹{az_price} | Size: {az_size}")

                            flipkart_data = flipkart_scraper.scrape_single_url(fk_url, target_size=az_size)
                            if flipkart_data.get('error'):
                                raise Exception(f"Flipkart: {flipkart_data.get('error')}")

                            fk_price = flipkart_data.get('price', 0)
                            fk_available_sizes = flipkart_data.get('available_sizes', [])
                            add_batch_log(f"  → Flipkart: ₹{fk_price}")

                            amazon_data['nudge_price'] = nudge
                            comp_result = similarity.compare_products(amazon_data, flipkart_data)

                            is_match = comp_result.get('approved_match', False)
                            confidence = comp_result.get('overall_similarity_percentage', 0) * 100

                            result_entry['status'] = 'PASSED' if is_match else 'FAILED'
                            result_entry['confidence'] = f"{confidence:.1f}%"
                            result_entry['az_price'] = az_price
                            result_entry['fk_price'] = fk_price
                            result_entry['az_mrp'] = amazon_data.get('mrp', 0)
                            result_entry['fk_mrp'] = flipkart_data.get('mrp', 0)
                            result_entry['az_size'] = az_size
                            result_entry['fk_available_size'] = fk_available_sizes
                            result_entry['fk_purchasable_size'] = flipkart_data.get('purchasable_sizes', [])
                            result_entry['primary_failure_reason'] = comp_result.get('primary_failure_reason', '')
                            result_entry['critical_failures'] = comp_result.get('critical_failures', [])
                            result_entry['informational_failures'] = comp_result.get('informational_failures', [])
                            result_entry['combined_failures'] = comp_result.get('combined_failures', [])
                            result_entry['failure_details'] = comp_result.get('failure_details', {})

                            if is_match:
                                passed_count += 1
                                add_batch_log(f"  ✅ PASSED ({confidence:.1f}%)", "SUCCESS")
                            else:
                                failed_count += 1
                                add_batch_log(f"  ❌ FAILED ({confidence:.1f}%)", "WARNING")

                        except Exception as e:
                            error_count += 1
                            result_entry['status'] = 'ERROR'
                            result_entry['error'] = str(e)
                            add_batch_log(f"  ⚠️ ERROR: {str(e)[:40]}", "ERROR")

                        current_results['results'].append(result_entry)
                        current_results['summary']['processed'] = processed_count
                        current_results['summary']['passed'] = passed_count
                        current_results['summary']['failed'] = failed_count
                        current_results['summary']['errors'] = error_count

                        with open(output_path, 'w', encoding='utf-8') as f:
                            json.dump(current_results, f, ensure_ascii=False, indent=2)

                        if processed_count < len(df_to_process):
                            time.sleep(1)

                    total_time = (datetime.now() - start_time).total_seconds()
                    add_batch_log(f"Batch Complete. Time: {total_time:.1f}s", "SUCCESS")
                    progress_bar.progress(1.0)
                    status_text.success("✅ OPTIMIZATION COMPLETE")

                    st.markdown("### RESULTS")
                    res_df = pd.DataFrame(current_results['results'])

                    if not res_df.empty:
                        if 'index' in res_df.columns:
                            res_df.set_index('index', inplace=True)

                        st.dataframe(res_df)

                        c_dl1, c_dl2 = st.columns(2)
                        with c_dl1:
                            st.download_button(
                                "📥 DOWNLOAD JSON",
                                data=json.dumps(current_results, indent=2, default=str),
                                file_name="batch_results.json",
                                mime="application/json"
                            )
                        with c_dl2:
                            st.download_button(
                                "📥 DOWNLOAD CSV",
                                data=res_df.to_csv(index=True),
                                file_name="batch_results.csv",
                                mime="text/csv"
                            )
                    else:
                        st.info("No results generated.")

        except Exception as e:
            st.error(f"❌ Error reading file: {str(e)}")
            logger.error(f"Batch file read error: {e}", exc_info=True)

    else:
        st.markdown("""
        <div style="height: 200px; border: 2px dashed #333; border-radius: 8px; display: flex;
                    align-items: center; justify-content: center; opacity: 0.5; margin-top: 20px;">
            <div style="text-align: center;">
                <h3 style="color: #333; margin: 0;">📁 NO FILE UPLOADED</h3>
                <p style="color: #555; font-family: JetBrains Mono;">Drop a CSV or Excel file above to start batch processing</p>
            </div>
        </div>
        """, unsafe_allow_html=True)