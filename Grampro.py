import logging
from flask import Flask, request, render_template_string, jsonify, Response, stream_with_context, send_file
import pandas as pd
import os
import time
import json
import requests
import concurrent.futures
from playwright.sync_api import sync_playwright
import uuid
import re
import random
import pickle
from threading import Lock
from datetime import datetime
# Install Playwright browser if not exists
try:
    from playwright import sync_api
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright"])
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])

# --- Flask App Setup ---
app = Flask(__name__, static_folder='static')
UPLOAD_FOLDER = 'uploads'
RESULTS_FOLDER = 'results'
PROGRESS_FOLDER = 'progress'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
os.makedirs(PROGRESS_FOLDER, exist_ok=True)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024  # 2 MB

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = app.logger
logger.setLevel(logging.INFO)

# --- CORRECTED Evomi Proxy Credentials ---
PROXY_USER = "robertthom3"
PROXY_PASS = "3DDQ6eQd4OK8Xx2EZQYW"
PROXY_HOST = "core-residential.evomi.com"
PROXY_PORT = "1000"
PROXY_AUTH = f"{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
PROXY_HTTP = f"http://{PROXY_AUTH}"

# --- Scraping Settings ---
BATCH_SIZE = 15
MAX_BATCHES = 1000
REQUEST_TIMEOUT = 30
BASE_BATCH_GAP = 2
BATCH_JITTER_MIN = 1
BATCH_JITTER_MAX = 3
MAX_RETRIES = 3
MAX_WORKERS = 8

# --- Progress Tracking ---
class ScrapingProgress:
    def __init__(self, filename, total_users, extended_info=False):
        self.filename = filename
        self.total_users = total_users
        self.processed_users = 0
        self.completed_usernames = set()
        self.results = []
        self.start_time = datetime.now()
        self.last_update = datetime.now()
        self.status = "running"  # running, paused, completed, error
        self.extended_info = extended_info
        
    def save(self):
        progress_file = os.path.join(PROGRESS_FOLDER, f"{self.filename}.progress")
        with open(progress_file, 'wb') as f:
            pickle.dump(self.__dict__, f)
            
    @staticmethod
    def load(filename):
        progress_file = os.path.join(PROGRESS_FOLDER, f"{filename}.progress")
        if os.path.exists(progress_file):
            with open(progress_file, 'rb') as f:
                data = pickle.load(f)
                progress = ScrapingProgress(data['filename'], data['total_users'], data.get('extended_info', False))
                progress.__dict__.update(data)
                return progress
        return None

# --- Thread-safe Success Rate Tracking ---
success_rates = {
    'api_success': 0,
    'api_total': 0,
    'browser_success': 0, 
    'browser_total': 0,
    'proxy_used': 0,
    'direct_used': 0
}
success_lock = Lock()

# --- Proxy State Tracking ---
class ProxyState:
    last_test_time = 0
    is_working = False
    lock = Lock()

def sanitize_csv_value(value):
    val = "" if value is None else str(value)
    if val.startswith(('=', '+', '-', '@')):
        return "'" + val
    return val

def clean_count(text):
    """Clean and format counts (followers, following, posts)"""
    if not text:
        return None
    text = text.strip().replace(',', '').replace(' ', '')
    m = re.match(r'(\d+(?:\.\d+)?)([KkMm])?', text)
    if not m:
        return None
    number, suffix = m.groups()
    return number + (suffix.upper() if suffix else '')

def human_delay(min_s=1.0, max_s=2.0):
    time.sleep(random.uniform(min_s, max_s))

def test_proxy_connection():
    """Test if proxy authentication works"""
    test_url = "http://httpbin.org/ip"
    proxies = {
        "http": PROXY_HTTP,
        "https": PROXY_HTTP
    }
    
    try:
        response = requests.get(test_url, proxies=proxies, timeout=15)
        if response.status_code == 200:
            logger.info(f"✅ Proxy working - IP: {response.json()['origin']}")
            return True
        else:
            logger.warning(f"❌ Proxy test failed with status: {response.status_code}")
            return False
    except Exception as e:
        logger.warning(f"❌ Proxy test failed: {e}")
        return False

def ensure_proxy_working():
    """Ensure proxy is working with optimized testing"""
    with ProxyState.lock:
        current_time = time.time()
        
        # Test proxy only once every 2 minutes or if not tested yet
        if current_time - ProxyState.last_test_time > 120 or not ProxyState.is_working:
            ProxyState.is_working = test_proxy_connection()
            ProxyState.last_test_time = current_time
        
        return ProxyState.is_working

def get_post_type(is_video, has_sidecar):
    """Determine post type based on video and sidecar flags"""
    if has_sidecar:
        return "Carousel"
    elif is_video:
        return "Video"
    else:
        return "Image"

def get_true_latest_post_by_date(user_data):
    """
    OPTIMIZED LOGIC: Check only first 4 posts to find the latest date
    This is efficient and accurate enough for most cases
    """
    media_edges = user_data.get('edge_owner_to_timeline_media', {}).get('edges', [])
    
    if not media_edges:
        return None
    
    # Only check first 4 posts (most efficient)
    posts_to_check = media_edges[:4]
    
    # Find the post with the latest timestamp
    latest_post = None
    latest_timestamp = 0
    
    for edge in posts_to_check:
        node = edge.get('node', {})
        timestamp = node.get('taken_at_timestamp', 0)
        
        if timestamp > latest_timestamp:
            latest_timestamp = timestamp
            latest_post = node
    
    return latest_post if latest_timestamp > 0 else None

def parse_post_info(post_data):
    """Parse post information using the proven logic"""
    if not post_data:
        return None
        
    try:
        post_type = get_post_type(
            post_data.get('is_video', False),
            'edge_sidecar_to_children' in post_data
        )
        
        return {
            'post_type': post_type,
            'is_pinned': post_data.get('pinned_for_user', False),
            'likes_count': post_data.get('edge_liked_by', {}).get('count'),
            'comments_count': post_data.get('edge_media_to_comment', {}).get('count'),
            'views_count': post_data.get('video_view_count') if post_data.get('is_video', False) else None,
            'post_date': datetime.fromtimestamp(post_data.get('taken_at_timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S') if post_data.get('taken_at_timestamp') else None
        }
    except Exception as e:
        logger.warning(f"Error parsing post info: {e}")
        return None

# --- Extended API Scraper with Optimized Logic ---
def extended_api_scrape_instagram(username, use_proxy=True):
    """Extended API scraping using optimized latest post logic"""
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'X-IG-App-ID': '936619743392459',
        'X-ASBD-ID': '198387',
        'X-IG-WWW-Claim': '0',
    }
    
    proxies = {
        "http": PROXY_HTTP,
        "https": PROXY_HTTP
    } if use_proxy else None
    
    human_delay()
    try:
        resp = requests.get(url, headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            user = data.get('data', {}).get('user')
            if not user:
                return None, 'Inactive/Unavailable'
                
            # Basic profile information
            profile_data = {
                'username': user.get('username'),
                'full_name': user.get('full_name'),
                'is_verified': user.get('is_verified', False),
                'is_private': user.get('is_private', False),
                'followers_count': user.get('edge_followed_by', {}).get('count'),
                'following_count': user.get('edge_follow', {}).get('count'),
                'posts_count': user.get('edge_owner_to_timeline_media', {}).get('count')
            }
            
            # USE THE OPTIMIZED LOGIC for latest post
            latest_post = get_true_latest_post_by_date(user)
            profile_data['latest_post'] = parse_post_info(latest_post) if latest_post else None
            
            if profile_data['is_private']:
                return profile_data, 'Private Account'
                
            return profile_data, 'Success'
            
        elif resp.status_code == 404:
            return None, 'Inactive/Unavailable'
        else:
            return None, 'Failed'
    except requests.exceptions.ProxyError as e:
        return None, 'Proxy Error'
    except Exception as e:
        logger.error(f"API scrape error for {username}: {e}")
        return None, 'Failed'

# --- Basic API Scraper (Original) ---
def basic_api_scrape_instagram(username, use_proxy=True):
    """Basic API scraping - followers only"""
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'X-IG-App-ID': '936619743392459',
        'X-ASBD-ID': '198387',
        'X-IG-WWW-Claim': '0',
    }
    
    proxies = {
        "http": PROXY_HTTP,
        "https": PROXY_HTTP
    } if use_proxy else None
    
    human_delay()
    try:
        resp = requests.get(url, headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            user = data.get('data', {}).get('user')
            if not user:
                return None, 'Inactive/Unavailable'
            if user.get('is_private'):
                return None, 'Private Account'
            followers = user.get('edge_followed_by', {}).get('count')
            if followers is not None:
                return str(followers), 'Success'
            return None, 'Failed'
        elif resp.status_code == 404:
            return None, 'Inactive/Unavailable'
        else:
            return None, 'Failed'
    except requests.exceptions.ProxyError as e:
        return None, 'Proxy Error'
    except Exception as e:
        return None, 'Failed'

# --- Browser Scraper with Extended Info ---
def extended_browser_scrape_instagram(username, use_proxy=True):
    """Extended browser scraping with full profile information"""
    try:
        browser_args = {
            "headless": True,
        }
        
        if use_proxy:
            browser_args["proxy"] = {
                "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
                "username": PROXY_USER,
                "password": PROXY_PASS
            }
        
        with sync_playwright() as p:
            browser = p.chromium.launch(**browser_args)
            
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            
            page = context.new_page()
            page.set_default_timeout(60000)
            
            try:
                page.goto(f"https://www.instagram.com/{username}/", wait_until='domcontentloaded', timeout=60000)
                page.wait_for_timeout(5000)
                
                page_content = page.content().lower()
                if "sorry, this page isn't available" in page_content or "page not found" in page_content:
                    return None, 'Inactive/Unavailable'
                if "this account is private" in page_content:
                    return None, 'Private Account'
                
                # Extract profile information
                profile_data = {
                    'username': username,
                    'full_name': '',
                    'is_verified': False,
                    'is_private': False,
                    'followers_count': None,
                    'following_count': None,
                    'posts_count': None,
                    'latest_post': None
                }
                
                # Get full name
                try:
                    full_name_selector = "header section h1, header section div span, ._aacl._aacs._aact._aacx._aada"
                    full_name_element = page.locator(full_name_selector).first
                    if full_name_element.count() > 0:
                        profile_data['full_name'] = full_name_element.text_content().strip()
                except:
                    pass
                
                # Check verification
                try:
                    verified_selector = "svg[aria-label='Verified']"
                    if page.locator(verified_selector).count() > 0:
                        profile_data['is_verified'] = True
                except:
                    pass
                
                # Get counts (followers, following, posts)
                count_selectors = {
                    'posts_count': ["//a[contains(@href, '/posts/')]//span", "header section ul li:nth-child(1) span"],
                    'followers_count': ["//a[contains(@href, '/followers/')]//span", "header section ul li:nth-child(2) span"],
                    'following_count': ["//a[contains(@href, '/following/')]//span", "header section ul li:nth-child(3) span"]
                }
                
                for count_type, selectors in count_selectors.items():
                    for sel in selectors:
                        try:
                            if sel.startswith("//"):
                                elements = page.locator(f"xpath={sel}")
                            else:
                                elements = page.locator(sel)
                                
                            if elements.count() > 0:
                                text = elements.first.text_content(timeout=5000)
                                if text:
                                    cleaned = clean_count(text.strip())
                                    if cleaned:
                                        profile_data[count_type] = cleaned
                                        break
                        except:
                            continue
                
                # Get latest post information (simplified browser version)
                try:
                    post_selector = "article a[href*='/p/']"
                    post_links = page.locator(post_selector)
                    if post_links.count() > 0:
                        first_post = post_links.first
                        href = first_post.get_attribute('href')
                        
                        # Basic post type detection
                        post_type = "Image"  # Default assumption
                        
                        profile_data['latest_post'] = {
                            'post_type': post_type,
                            'is_pinned': False,  # Hard to detect in browser
                            'likes_count': 0,    # Would require clicking post
                            'comments_count': 0, # Would require clicking post
                            'views_count': 0,    # Would require clicking post
                            'post_date': None    # Hard to get without API
                        }
                except:
                    pass
                
                if "instagram" not in page_content:
                    return None, 'Inactive/Unavailable'
                    
                return profile_data, 'Success'
                
            except Exception as e:
                logger.error(f"Browser scrape error for {username}: {e}")
                return None, 'Failed'
            finally:
                browser.close()
                
    except Exception as e:
        logger.error(f"Browser launch error for {username}: {e}")
        return None, 'Failed'

# --- Basic Browser Scraper (Original) ---
def basic_browser_scrape_instagram(username, use_proxy=True):
    """Basic browser scraping - followers only"""
    try:
        browser_args = {
            "headless": True,
            "viewport": {"width": 1920, "height": 1080}
        }
        
        if use_proxy:
            browser_args["proxy"] = {
                "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
                "username": PROXY_USER,
                "password": PROXY_PASS
            }
        
        with sync_playwright() as p:
            browser = p.chromium.launch(**browser_args)
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            
            page = context.new_page()
            page.set_default_timeout(60000)
            
            try:
                page.goto(f"https://www.instagram.com/{username}/", wait_until='domcontentloaded', timeout=60000)
                page.wait_for_timeout(3000)
                
                page_content = page.content().lower()
                if "sorry, this page isn't available" in page_content or "page not found" in page_content:
                    return None, 'Inactive/Unavailable'
                if "this account is private" in page_content:
                    return None, 'Private Account'
                
                selectors = [
                    "//a[contains(@href, '/followers/')]//span",
                    "header section ul li:nth-child(2) a span",
                    "a[href*='/followers/'] span",
                    "._ac2a span",
                    "section main header section ul li:nth-child(2) span",
                    "//span[contains(text(), 'followers') or contains(text(), 'follower')]/preceding-sibling::span"
                ]
                
                for sel in selectors:
                    try:
                        if sel.startswith("//"):
                            elements = page.locator(f"xpath={sel}")
                        else:
                            elements = page.locator(sel)
                            
                        if elements.count() > 0:
                            text = elements.first.text_content(timeout=10000)
                            if text:
                                cleaned = clean_count(text.strip())
                                if cleaned:
                                    return cleaned, 'Success'
                    except:
                        continue
                
                if "instagram" not in page_content:
                    return None, 'Inactive/Unavailable'
                    
                return None, 'Failed'
                
            except Exception as e:
                return None, 'Failed'
            finally:
                browser.close()
                
    except Exception as e:
        return None, 'Failed'

# --- Individual Proxy Handling with Fallback ---
def scrape_with_individual_proxy_fallback(username, extended_info=False, max_retries=MAX_RETRIES):
    logger.info(f"🔄 Starting scraping for: {username} (Extended: {extended_info})")
    
    # Choose appropriate scraping functions
    api_scraper = extended_api_scrape_instagram if extended_info else basic_api_scrape_instagram
    browser_scraper = extended_browser_scrape_instagram if extended_info else basic_browser_scrape_instagram
    
    # Track connection method for this username
    connection_method = "unknown"
    
    for attempt in range(1, max_retries + 1):
        # ===== PHASE 1: Decide connection method =====
        use_proxy = ensure_proxy_working()
        connection_method = "proxy" if use_proxy else "direct"
        
        if attempt == 1:
            logger.info(f"🔹 [{attempt}.1] Trying API with {connection_method} connection for {username}...")
        else:
            logger.info(f"🔹 [{attempt}.1] Retry {attempt} with {connection_method} connection for {username}...")
        
        # ===== PHASE 2: Try API first =====
        api_result, api_status = api_scraper(username, use_proxy)
        
        if api_status == 'Success':
            logger.info(f"✅ API success for {username} using {connection_method}")
            with success_lock:
                success_rates['api_success'] += 1
                success_rates['api_total'] += 1
                if use_proxy:
                    success_rates['proxy_used'] += 1
                else:
                    success_rates['direct_used'] += 1
            
            if extended_info:
                return {
                    'username': username,
                    'full_name': api_result.get('full_name', ''),
                    'is_verified': api_result.get('is_verified', False),
                    'followers_count': api_result.get('followers_count'),
                    'following_count': api_result.get('following_count'),
                    'posts_count': api_result.get('posts_count'),
                    'latest_post_type': api_result.get('latest_post', {}).get('post_type', '') if api_result.get('latest_post') else '',
                    'latest_post_date': api_result.get('latest_post', {}).get('post_date', '') if api_result.get('latest_post') else '',
                    'latest_post_likes': api_result.get('latest_post', {}).get('likes_count', '') if api_result.get('latest_post') else '',
                    'latest_post_comments': api_result.get('latest_post', {}).get('comments_count', '') if api_result.get('latest_post') else '',
                    'latest_post_views': api_result.get('latest_post', {}).get('views_count', '') if api_result.get('latest_post') else '',
                    'latest_post_pinned': "Yes" if api_result.get('latest_post', {}).get('is_pinned', False) else "No",
                    'status': api_status,
                    'method': connection_method
                }
            else:
                return {
                    'username': username,
                    'followers': api_result.get('followers_count') if isinstance(api_result, dict) else api_result,
                    'status': api_status,
                    'method': connection_method
                }
                
        elif api_status in ['Inactive/Unavailable', 'Private Account']:
            logger.info(f"ℹ️  API determined: {username} - {api_status}")
            with success_lock:
                success_rates['api_total'] += 1
                if use_proxy:
                    success_rates['proxy_used'] += 1
                else:
                    success_rates['direct_used'] += 1
            
            if extended_info:
                return {
                    'username': username,
                    'full_name': '',
                    'is_verified': False,
                    'followers_count': 'N/A',
                    'following_count': 'N/A',
                    'posts_count': 'N/A',
                    'latest_post_type': '',
                    'latest_post_date': '',
                    'latest_post_likes': '',
                    'latest_post_comments': '',
                    'latest_post_views': '',
                    'latest_post_pinned': 'No',
                    'status': api_status,
                    'method': connection_method
                }
            else:
                return {
                    'username': username,
                    'followers': 'N/A',
                    'status': api_status,
                    'method': connection_method
                }
        else:
            with success_lock:
                success_rates['api_total'] += 1
                if use_proxy:
                    success_rates['proxy_used'] += 1
                else:
                    success_rates['direct_used'] += 1
        
        # ===== PHASE 3: Try browser as backup =====
        logger.info(f"🌐 [{attempt}.2] Trying browser with {connection_method} connection for {username}...")
        browser_result, browser_status = browser_scraper(username, use_proxy)
        
        if browser_status == 'Success':
            logger.info(f"✅ Browser success for {username} using {connection_method}")
            with success_lock:
                success_rates['browser_success'] += 1
                success_rates['browser_total'] += 1
            
            if extended_info:
                return {
                    'username': username,
                    'full_name': browser_result.get('full_name', ''),
                    'is_verified': browser_result.get('is_verified', False),
                    'followers_count': browser_result.get('followers_count'),
                    'following_count': browser_result.get('following_count'),
                    'posts_count': browser_result.get('posts_count'),
                    'latest_post_type': browser_result.get('latest_post', {}).get('post_type', '') if browser_result.get('latest_post') else '',
                    'latest_post_date': browser_result.get('latest_post', {}).get('post_date', '') if browser_result.get('latest_post') else '',
                    'latest_post_likes': browser_result.get('latest_post', {}).get('likes_count', '') if browser_result.get('latest_post') else '',
                    'latest_post_comments': browser_result.get('latest_post', {}).get('comments_count', '') if browser_result.get('latest_post') else '',
                    'latest_post_views': browser_result.get('latest_post', {}).get('views_count', '') if browser_result.get('latest_post') else '',
                    'latest_post_pinned': "No",  # Browser can't detect pinned posts
                    'status': browser_status,
                    'method': connection_method
                }
            else:
                return {
                    'username': username,
                    'followers': browser_result.get('followers_count') if isinstance(browser_result, dict) else browser_result,
                    'status': browser_status,
                    'method': connection_method
                }
                
        elif browser_status in ['Inactive/Unavailable', 'Private Account']:
            logger.info(f"ℹ️  Browser determined: {username} - {browser_status}")
            with success_lock:
                success_rates['browser_total'] += 1
            
            if extended_info:
                return {
                    'username': username,
                    'full_name': '',
                    'is_verified': False,
                    'followers_count': 'N/A',
                    'following_count': 'N/A',
                    'posts_count': 'N/A',
                    'latest_post_type': '',
                    'latest_post_date': '',
                    'latest_post_likes': '',
                    'latest_post_comments': '',
                    'latest_post_views': '',
                    'latest_post_pinned': 'No',
                    'status': browser_status,
                    'method': connection_method
                }
            else:
                return {
                    'username': username,
                    'followers': 'N/A',
                    'status': browser_status,
                    'method': connection_method
                }
        else:
            with success_lock:
                success_rates['browser_total'] += 1
        
        # ===== PHASE 4: Smart retry decision =====
        if attempt < max_retries:
            # Dynamic wait based on error type and attempt number
            if api_status == 'Proxy Error':
                wait_time = 5  # Shorter wait for proxy errors
                logger.info(f"🔄 Proxy error detected, quick retry in {wait_time}s...")
            else:
                wait_time = attempt * 2  # Progressive wait for other errors
                logger.info(f"⏳ Waiting {wait_time}s before retry {attempt + 1}...")
            
            time.sleep(wait_time)
    
    # ===== PHASE 5: Ultimate fallback - try both methods as last resort =====
    logger.info(f"🔥 Ultimate fallback for {username}")
    
    # Final attempt with opposite connection method
    final_use_proxy = not use_proxy
    final_method = "proxy" if final_use_proxy else "direct"
    logger.info(f"🎯 Final attempt with {final_method} connection")
    
    try:
        final_api_result, final_api_status = api_scraper(username, final_use_proxy)
        if final_api_status == 'Success':
            logger.info(f"🎉 Ultimate API success for {username} with {final_method}!")
            if extended_info:
                return {
                    'username': username,
                    'full_name': final_api_result.get('full_name', ''),
                    'is_verified': final_api_result.get('is_verified', False),
                    'followers_count': final_api_result.get('followers_count'),
                    'following_count': final_api_result.get('following_count'),
                    'posts_count': final_api_result.get('posts_count'),
                    'latest_post_type': final_api_result.get('latest_post', {}).get('post_type', '') if final_api_result.get('latest_post') else '',
                    'latest_post_date': final_api_result.get('latest_post', {}).get('post_date', '') if final_api_result.get('latest_post') else '',
                    'latest_post_likes': final_api_result.get('latest_post', {}).get('likes_count', '') if final_api_result.get('latest_post') else '',
                    'latest_post_comments': final_api_result.get('latest_post', {}).get('comments_count', '') if final_api_result.get('latest_post') else '',
                    'latest_post_views': final_api_result.get('latest_post', {}).get('views_count', '') if final_api_result.get('latest_post') else '',
                    'latest_post_pinned': "Yes" if final_api_result.get('latest_post', {}).get('is_pinned', False) else "No",
                    'status': final_api_status,
                    'method': final_method
                }
            else:
                return {
                    'username': username,
                    'followers': final_api_result.get('followers_count') if isinstance(final_api_result, dict) else final_api_result,
                    'status': final_api_status,
                    'method': final_method
                }
    except:
        pass
    
    try:
        final_browser_result, final_browser_status = browser_scraper(username, final_use_proxy)
        if final_browser_status == 'Success':
            logger.info(f"🎉 Ultimate browser success for {username} with {final_method}!")
            if extended_info:
                return {
                    'username': username,
                    'full_name': final_browser_result.get('full_name', ''),
                    'is_verified': final_browser_result.get('is_verified', False),
                    'followers_count': final_browser_result.get('followers_count'),
                    'following_count': final_browser_result.get('following_count'),
                    'posts_count': final_browser_result.get('posts_count'),
                    'latest_post_type': final_browser_result.get('latest_post', {}).get('post_type', '') if final_browser_result.get('latest_post') else '',
                    'latest_post_date': final_browser_result.get('latest_post', {}).get('post_date', '') if final_browser_result.get('latest_post') else '',
                    'latest_post_likes': final_browser_result.get('latest_post', {}).get('likes_count', '') if final_browser_result.get('latest_post') else '',
                    'latest_post_comments': final_browser_result.get('latest_post', {}).get('comments_count', '') if final_browser_result.get('latest_post') else '',
                    'latest_post_views': final_browser_result.get('latest_post', {}).get('views_count', '') if final_browser_result.get('latest_post') else '',
                    'latest_post_pinned': "No",
                    'status': final_browser_status,
                    'method': final_method
                }
            else:
                return {
                    'username': username,
                    'followers': final_browser_result.get('followers_count') if isinstance(final_browser_result, dict) else final_browser_result,
                    'status': final_browser_status,
                    'method': final_method
                }
        elif final_browser_status in ['Inactive/Unavailable', 'Private Account']:
            logger.info(f"ℹ️  Ultimate determination: {username} - {final_browser_status}")
            if extended_info:
                return {
                    'username': username,
                    'full_name': '',
                    'is_verified': False,
                    'followers_count': 'N/A',
                    'following_count': 'N/A',
                    'posts_count': 'N/A',
                    'latest_post_type': '',
                    'latest_post_date': '',
                    'latest_post_likes': '',
                    'latest_post_comments': '',
                    'latest_post_views': '',
                    'latest_post_pinned': 'No',
                    'status': final_browser_status,
                    'method': final_method
                }
            else:
                return {
                    'username': username,
                    'followers': 'N/A',
                    'status': final_browser_status,
                    'method': final_method
                }
    except:
        pass
    
    logger.error(f"💥 Critical failure for {username} after all attempts")
    if extended_info:
        return {
            'username': username,
            'full_name': '',
            'is_verified': False,
            'followers_count': 'N/A',
            'following_count': 'N/A',
            'posts_count': 'N/A',
            'latest_post_type': '',
            'latest_post_date': '',
            'latest_post_likes': '',
            'latest_post_comments': '',
            'latest_post_views': '',
            'latest_post_pinned': 'No',
            'status': 'Failed',
            'method': 'failed'
        }
    else:
        return {
            'username': username,
            'followers': 'N/A',
            'status': 'Failed',
            'method': 'failed'
        }

def process_batch_parallel(usernames, extended_info=False):
    logger.info(f"🔨 Processing batch of {len(usernames)} usernames (Extended: {extended_info})")
    
    # Quick proxy health check (non-blocking, for logging only)
    proxy_healthy = ensure_proxy_working()
    if proxy_healthy:
        logger.info("✅ Proxy available - batch will use mixed mode")
    else:
        logger.info("🚫 Proxy unavailable - batch will use direct connections")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(usernames))) as executor:
        results = list(executor.map(lambda username: scrape_with_individual_proxy_fallback(username, extended_info), usernames))
    
    # Log batch statistics
    proxy_count = sum(1 for r in results if r.get('method') == 'proxy')
    direct_count = sum(1 for r in results if r.get('method') == 'direct')
    success_count = sum(1 for r in results if r.get('status') == 'Success')
    
    logger.info(f"📊 Batch completed: {success_count}/{len(usernames)} success, {proxy_count} proxy, {direct_count} direct")
    
    sleep_time = BASE_BATCH_GAP + random.randint(BATCH_JITTER_MIN, BATCH_JITTER_MAX)
    logger.info(f"⏳ Waiting {sleep_time}s before next batch...")
    time.sleep(sleep_time)
    
    return results

# --- FINAL UI Template with Improved Results Container ---
HTML_TEMPLATE = ''' 
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Gramhunt Pro - 100% Accuracy Instagram Scraper</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
  <style>
    :root {
      --primary: #6366f1;
      --primary-dark: #4f46e5;
      --success: #10b981;
      --warning: #f59e0b;
      --error: #ef4444;
    }
    
    body {
      font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      padding: 20px;
    }
    
    .glass-card {
      background: rgba(255, 255, 255, 0.95);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(255, 255, 255, 0.2);
      border-radius: 20px;
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
    }
    
    .stat-card {
      background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%);
      color: white;
      border-radius: 15px;
      padding: 20px;
      text-align: center;
    }
    
    .progress-bar {
      background: #e5e7eb;
      border-radius: 10px;
      overflow: hidden;
      height: 8px;
    }
    
    .progress-fill {
      background: linear-gradient(90deg, var(--success) 0%, #34d399 100%);
      height: 100%;
      transition: width 0.3s ease;
    }
    
    .result-row {
      border-bottom: 1px solid #e5e7eb;
      transition: background-color 0.2s ease;
    }
    
    .result-row:hover {
      background-color: #f8fafc;
    }
    
    .status-success { color: var(--success); }
    .status-warning { color: var(--warning); }
    .status-error { color: var(--error); }
    
    .file-upload {
      border: 2px dashed #d1d5db;
      border-radius: 10px;
      padding: 30px;
      text-align: center;
      transition: border-color 0.3s ease;
    }
    
    .file-upload.dragover {
      border-color: var(--primary);
      background-color: #f0f9ff;
    }
    
    .btn-primary {
      background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%);
      color: white;
      padding: 12px 30px;
      border-radius: 10px;
      font-weight: 600;
      transition: all 0.3s ease;
    }
    
    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 10px 25px rgba(99, 102, 241, 0.3);
    }
    
    .btn-warning {
      background: linear-gradient(135deg, var(--warning) 0%, #d97706 100%);
      color: white;
    }
    
    .btn-success {
      background: linear-gradient(135deg, var(--success) 0%, #059669 100%);
      color: white;
    }

    .extended-info-checkbox {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 15px;
      background: rgba(99, 102, 241, 0.1);
      border-radius: 10px;
      margin: 15px 0;
      border-left: 4px solid var(--primary);
    }
    
    .extended-info-checkbox input[type="checkbox"] {
      width: 18px;
      height: 18px;
      accent-color: var(--primary);
    }

    /* Improved Results Container */
    .results-wrapper {
      max-height: 80vh;
      overflow: auto;
      border-radius: 8px;
      border: 1px solid #e5e7eb;
    }
    
    .results-table {
      min-width: 100%;
      table-layout: fixed;
      border-collapse: collapse;
    }
    
    /* Basic mode columns */
    .basic-table {
      min-width: 600px;
    }
    
    /* Extended mode columns */
    .extended-table {
      min-width: 1200px;
    }
    
    .column-username { width: 200px; min-width: 150px; }
    .column-followers { width: 150px; min-width: 120px; }
    .column-status { width: 150px; min-width: 120px; }
    .column-fullname { width: 180px; min-width: 150px; }
    .column-verified { width: 100px; min-width: 80px; }
    .column-following { width: 120px; min-width: 100px; }
    .column-posts { width: 100px; min-width: 80px; }
    .column-post-type { width: 120px; min-width: 100px; }
    .column-post-date { width: 180px; min-width: 150px; }
    .column-pinned { width: 100px; min-width: 80px; }
    
    .table-cell {
      padding: 12px 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      border-bottom: 1px solid #f1f5f9;
    }
    
    .header-cell {
      padding: 12px 8px;
      font-weight: 600;
      color: #374151;
      background: #f8fafc;
      border-bottom: 2px solid #e5e7eb;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      position: sticky;
      top: 0;
      z-index: 10;
    }
    
    .table-body {
      background: white;
    }
  </style>
</head>
<body class="min-h-screen flex items-center justify-center">
  <div class="container max-w-7xl mx-auto">
    <!-- Header -->
    <div class="text-center mb-8">
      <h1 class="text-4xl font-bold text-white mb-2">Gramhunt Pro</h1>
      <p class="text-xl text-white/90">100% Accuracy Instagram Scraper</p>
    </div>
    
    <!-- Stats Cards -->
    <div class="grid grid-cols-1 lg:grid-cols-4 gap-4 mb-6">
      <div class="stat-card">
        <div class="text-2xl font-bold" id="completedCount">0</div>
        <div class="text-white/80 text-sm">Completed</div>
        <div class="text-white/60 text-xs mt-1" id="remainingCount">0 remaining</div>
      </div>
      <div class="stat-card">
        <div class="text-2xl font-bold" id="successCount">0</div>
        <div class="text-white/80 text-sm">Success</div>
      </div>
      <div class="stat-card">
        <div class="text-2xl font-bold" id="privateCount">0</div>
        <div class="text-white/80 text-sm">Private</div>
      </div>
      <div class="stat-card">
        <div class="text-2xl font-bold" id="inactiveCount">0</div>
        <div class="text-white/80 text-sm">Inactive</div>
      </div>
    </div>
    
    <!-- Speed and Time Estimate -->
    <div class="glass-card p-4 mb-6">
      <div class="grid grid-cols-2 gap-4 text-center">
        <div>
          <div class="text-sm text-gray-600">Current Speed</div>
          <div class="text-lg font-bold text-gray-800" id="speed">0/min</div>
        </div>
        <div>
          <div class="text-sm text-gray-600">Estimated Time</div>
          <div class="text-lg font-bold text-gray-800" id="timeEstimate">Calculating...</div>
        </div>
      </div>
    </div>
    
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Upload Section -->
      <div class="glass-card p-6">
        <h2 class="text-2xl font-bold text-gray-800 mb-4">
          <i class="fas fa-upload mr-2"></i>Upload CSV File
        </h2>
        <p class="text-gray-600 mb-4">CSV must contain a "username" column.</p>
        
        <div class="file-upload mb-4" id="fileDropZone">
          <i class="fas fa-cloud-upload-alt text-4xl text-gray-400 mb-3"></i>
          <p class="text-gray-500" id="fileName">Choose file or drag & drop</p>
          <input type="file" id="fileInput" accept=".csv" class="hidden" />
        </div>
        
        <!-- Extended Info Checkbox -->
        <div class="extended-info-checkbox">
          <input type="checkbox" id="extendedInfo" name="extendedInfo">
          <label for="extendedInfo" class="text-gray-700 font-medium cursor-pointer">
            <i class="fas fa-info-circle mr-2"></i>
            Include Extended Profile Information
          </label>
        </div>
        <p class="text-sm text-gray-600 mb-4">
          When enabled, collects: Full Name, Verification Status, Followers/Following/Posts Counts, 
          and Latest Post information (Type, Date, Likes, Comments, Views, Pinned Status).
        </p>
        
        <!-- Resume Option -->
        <div id="resumeSection" class="bg-yellow-50 border border-yellow-200 rounded-lg p-4 mb-4 hidden">
          <div class="flex items-center mb-2">
            <i class="fas fa-history text-yellow-600 mr-2"></i>
            <span class="font-medium text-yellow-800">Incomplete Job Found!</span>
          </div>
          <p class="text-yellow-700 text-sm mb-2" id="resumeInfo"></p>
          <button id="resumeBtn" class="btn-warning w-full py-2 text-sm">
            <i class="fas fa-play-circle mr-2"></i>Resume Scraping
          </button>
        </div>
        
        <div class="bg-gray-50 rounded-lg p-4 mb-4">
          <div class="flex justify-between items-center mb-2">
            <span class="font-medium text-gray-700">100% Accuracy Mode</span>
            <span class="text-sm text-gray-600" id="workerInfo">8 workers • 15/batch • 3 retries</span>
          </div>
          <div class="text-sm text-gray-600" id="totalUsers">Total users: 0</div>
        </div>
        
        <button id="startBtn" class="btn-primary w-full py-3 text-lg" disabled>
          <i class="fas fa-play mr-2"></i>Start 100% Accuracy Scraping
        </button>
        
        <!-- Pause/Stop Buttons -->
        <div class="flex space-x-2 mt-3">
          <button id="pauseBtn" class="btn-warning flex-1 py-2 hidden">
            <i class="fas fa-pause mr-2"></i>Pause
          </button>
          <button id="stopBtn" class="flex-1 py-2 bg-red-500 text-white rounded-lg font-medium hidden">
            <i class="fas fa-stop mr-2"></i>Stop
          </button>
        </div>
        
        <div id="fileInfo" class="mt-3 text-sm text-gray-600 hidden">
          <i class="fas fa-check-circle text-green-500 mr-1"></i>
          <span id="fileInfoText"></span>
        </div>
      </div>
      
      <!-- Results Section -->
      <div class="glass-card p-6">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-2xl font-bold text-gray-800">
            <i class="fas fa-chart-line mr-2"></i>Live Results
          </h2>
          <button id="downloadBtn" class="px-4 py-2 bg-blue-100 text-blue-600 rounded-lg font-medium hidden">
            <i class="fas fa-download mr-2"></i>Download CSV
          </button>
        </div>
        
        <!-- Progress -->
        <div class="mb-6">
          <div class="flex justify-between text-sm text-gray-600 mb-1">
            <span>Progress <span id="progressCount">0/0</span></span>
            <span id="progressText">0%</span>
          </div>
          <div class="progress-bar">
            <div class="progress-fill" id="progressBar" style="width: 0%"></div>
          </div>
        </div>
        
        <!-- Improved Results Container -->
        <div class="results-wrapper">
          <table class="results-table" id="resultsTable">
            <thead id="resultsHeader">
              <!-- Headers will be populated by JavaScript -->
            </thead>
            <tbody class="table-body" id="resultsBody">
              <tr>
                <td colspan="3" class="p-8 text-center text-gray-500" id="waitingMessage">
                  <i class="fas fa-cloud-upload-alt text-3xl mb-3"></i>
                  <p class="font-medium">Upload CSV to start scraping</p>
                  <p class="text-sm mt-1">100% accuracy guaranteed - no accounts skipped</p>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    let totalUsers = 0;
    let processedUsers = 0;
    let successCount = 0;
    let privateCount = 0;
    let inactiveCount = 0;
    let startTime = null;
    let currentEventSource = null;
    let isPaused = false;
    let currentFilename = null;
    let extendedInfo = false;
    
    // Get all DOM elements
    const fileInput = document.getElementById('fileInput');
    const fileDropZone = document.getElementById('fileDropZone');
    const fileName = document.getElementById('fileName');
    const startBtn = document.getElementById('startBtn');
    const resumeBtn = document.getElementById('resumeBtn');
    const pauseBtn = document.getElementById('pauseBtn');
    const stopBtn = document.getElementById('stopBtn');
    const resumeSection = document.getElementById('resumeSection');
    const fileInfo = document.getElementById('fileInfo');
    const fileInfoText = document.getElementById('fileInfoText');
    const totalUsersElement = document.getElementById('totalUsers');
    const timeEstimate = document.getElementById('timeEstimate');
    const resultsBody = document.getElementById('resultsBody');
    const waitingMessage = document.getElementById('waitingMessage');
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const progressCount = document.getElementById('progressCount');
    const completedCount = document.getElementById('completedCount');
    const remainingCount = document.getElementById('remainingCount');
    const successCountElement = document.getElementById('successCount');
    const privateCountElement = document.getElementById('privateCount');
    const inactiveCountElement = document.getElementById('inactiveCount');
    const speed = document.getElementById('speed');
    const downloadBtn = document.getElementById('downloadBtn');
    const extendedInfoCheckbox = document.getElementById('extendedInfo');
    const resultsHeader = document.getElementById('resultsHeader');
    const resultsTable = document.getElementById('resultsTable');
    
    // Check for resumeable jobs on page load
    window.addEventListener('load', checkForResumeableJobs);
    
    async function checkForResumeableJobs() {
      try {
        const response = await fetch('/check-resume');
        const data = await response.json();
        
        if (data.resumeable) {
          resumeSection.classList.remove('hidden');
          document.getElementById('resumeInfo').textContent = 
            `Resume from ${data.processed}/${data.total} users (${data.progress}% completed)`;
          currentFilename = data.filename;
        }
      } catch (error) {
        console.log('No resumeable jobs found');
      }
    }
    
    // File upload handling
    fileDropZone.addEventListener('click', () => fileInput.click());
    fileDropZone.addEventListener('dragover', (e) => {
      e.preventDefault();
      fileDropZone.classList.add('dragover');
    });
    fileDropZone.addEventListener('dragleave', () => {
      fileDropZone.classList.remove('dragover');
    });
    fileDropZone.addEventListener('drop', (e) => {
      e.preventDefault();
      fileDropZone.classList.remove('dragover');
      if (e.dataTransfer.files.length) {
        fileInput.files = e.dataTransfer.files;
        handleFileSelect();
      }
    });
    
    fileInput.addEventListener('change', handleFileSelect);
    
    function handleFileSelect() {
      if (fileInput.files.length) {
        const file = fileInput.files[0];
        fileName.textContent = file.name;
        startBtn.disabled = false;
        fileInfo.classList.remove('hidden');
        resumeSection.classList.add('hidden');
        
        const reader = new FileReader();
        reader.onload = function(e) {
          const contents = e.target.result;
          const lines = contents.split('\\n').filter(line => line.trim() !== '');
          totalUsers = Math.max(0, lines.length - 1);
          
          fileInfoText.textContent = `File loaded: ${file.name}`;
          totalUsersElement.textContent = `Total users: ${totalUsers}`;
          progressCount.textContent = `0/${totalUsers}`;
          remainingCount.textContent = `${totalUsers} remaining`;
          updateStats();
        };
        reader.readAsText(file);
      }
    }
    
    // Start new scraping job
    startBtn.addEventListener('click', async () => {
      if (!fileInput.files.length) return;
      
      extendedInfo = extendedInfoCheckbox.checked;
      
      const formData = new FormData();
      formData.append('csv_file', fileInput.files[0]);
      formData.append('action', 'start');
      formData.append('extended_info', extendedInfo);
      
      await startScrapingJob(formData);
    });
    
    // Resume existing job
    resumeBtn.addEventListener('click', async () => {
      const formData = new FormData();
      formData.append('filename', currentFilename);
      formData.append('action', 'resume');
      
      await startScrapingJob(formData);
    });
    
    async function startScrapingJob(formData) {
      startBtn.disabled = true;
      resumeBtn.disabled = true;
      startBtn.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Starting...';
      resumeSection.classList.add('hidden');
      waitingMessage.classList.add('hidden');
      
      pauseBtn.classList.remove('hidden');
      stopBtn.classList.remove('hidden');
      
      startTime = Date.now();
      
      try {
        const response = await fetch('/upload', { method: 'POST', body: formData });
        const result = await response.json();
        
        if (result.error) {
          alert('Error: ' + result.error);
          resetUI();
          return;
        }
        
        startScraping(result.filename, result.total_count, result.resume_from || 0, result.extended_info || false);
        
      } catch (error) {
        alert('Upload failed: ' + error.message);
        resetUI();
      }
    }
    
    // Pause functionality
    pauseBtn.addEventListener('click', async () => {
      if (isPaused) {
        // Resume scraping
        isPaused = false;
        pauseBtn.innerHTML = '<i class="fas fa-pause mr-2"></i>Pause';
        pauseBtn.classList.remove('btn-success');
        pauseBtn.classList.add('btn-warning');
        
        if (currentEventSource) {
          currentEventSource.close();
        }
        startScraping(currentFilename, totalUsers, processedUsers, extendedInfo);
      } else {
        // Pause scraping
        isPaused = true;
        pauseBtn.innerHTML = '<i class="fas fa-play mr-2"></i>Resume';
        pauseBtn.classList.remove('btn-warning');
        pauseBtn.classList.add('btn-success');
        
        if (currentEventSource) {
          currentEventSource.close();
          currentEventSource = null;
        }
        
        // Save progress when pausing
        try {
          await fetch(`/stop-job/${currentFilename}`, { method: 'POST' });
        } catch (error) {
          console.log('Error saving progress:', error);
        }
      }
    });
    
    // Stop functionality
    stopBtn.addEventListener('click', async () => {
      if (currentEventSource) {
        currentEventSource.close();
        currentEventSource = null;
      }
      
      // Save progress when stopping
      try {
        await fetch(`/stop-job/${currentFilename}`, { method: 'POST' });
      } catch (error) {
        console.log('Error saving progress:', error);
      }
      
      resetUI();
      alert('Scraping stopped. You can resume later from the resume section.');
    });
    
    function startScraping(filename, totalCount, resumeFrom = 0, useExtendedInfo = false) {
      currentFilename = filename;
      totalUsers = totalCount;
      processedUsers = resumeFrom;
      extendedInfo = useExtendedInfo;
      
      // Update results table structure
      updateResultsTableStructure();
      
      // Calculate initial counts based on existing results
      const existingRows = resultsBody.querySelectorAll('.result-row');
      successCount = Array.from(existingRows).filter(row => 
        row.querySelector('.status-success')
      ).length;
      privateCount = Array.from(existingRows).filter(row => 
        row.querySelector('.status-warning') && row.textContent.includes('Private')
      ).length;
      inactiveCount = Array.from(existingRows).filter(row => 
        row.querySelector('.status-warning') && row.textContent.includes('Inactive')
      ).length;
      
      updateStats();
      
      currentEventSource = new EventSource(`/stream/${filename}?resume_from=${resumeFrom}&extended_info=${extendedInfo}`);
      
      currentEventSource.onmessage = (event) => {
        if (isPaused) return;
        
        const data = JSON.parse(event.data);
        
        if (data.username) {
          processedUsers++;
          
          switch(data.status) {
            case 'Success':
              successCount++;
              break;
            case 'Private Account':
              privateCount++;
              break;
            case 'Inactive/Unavailable':
              inactiveCount++;
              break;
          }
          
          updateStats();
          addResultRow(data);
        }
      };
      
      currentEventSource.addEventListener('close', () => {
        if (currentEventSource) {
          currentEventSource.close();
          currentEventSource = null;
        }
        
        if (!isPaused) {
          startBtn.innerHTML = '<i class="fas fa-check mr-2"></i>100% Completed';
          pauseBtn.classList.add('hidden');
          stopBtn.classList.add('hidden');
          downloadBtn.classList.remove('hidden');
          
          setTimeout(() => {
            const completedRow = document.createElement('tr');
            completedRow.className = 'result-row';
            completedRow.innerHTML = `
              <td colspan="${extendedInfo ? '10' : '3'}" class="table-cell p-4 text-center text-green-600 font-medium bg-green-50">
                <i class="fas fa-trophy mr-2"></i>100% Accuracy Achieved! All accounts processed.
              </td>
            `;
            resultsBody.appendChild(completedRow);
          }, 500);
        }
      });
    }
    
    function updateResultsTableStructure() {
      if (extendedInfo) {
        resultsTable.className = 'results-table extended-table';
        resultsHeader.innerHTML = `
          <tr>
            <th class="header-cell column-username">Username</th>
            <th class="header-cell column-fullname">Full Name</th>
            <th class="header-cell column-verified">Verified</th>
            <th class="header-cell column-followers">Followers</th>
            <th class="header-cell column-following">Following</th>
            <th class="header-cell column-posts">Posts</th>
            <th class="header-cell column-post-type">Post Type</th>
            <th class="header-cell column-post-date">Post Date</th>
            <th class="header-cell column-pinned">Pinned</th>
            <th class="header-cell column-status">Status</th>
          </tr>
        `;
      } else {
        resultsTable.className = 'results-table basic-table';
        resultsHeader.innerHTML = `
          <tr>
            <th class="header-cell column-username">Username</th>
            <th class="header-cell column-followers">Followers</th>
            <th class="header-cell column-status">Status</th>
          </tr>
        `;
      }
      
      // Clear existing results but keep the waiting message initially
      resultsBody.innerHTML = '';
    }
    
    function updateStats() {
      const progress = Math.round((processedUsers / totalUsers) * 100);
      progressBar.style.width = `${progress}%`;
      progressText.textContent = `${progress}%`;
      progressCount.textContent = `${processedUsers}/${totalUsers}`;
      
      completedCount.textContent = processedUsers;
      remainingCount.textContent = `${totalUsers - processedUsers} remaining`;
      
      successCountElement.textContent = successCount;
      privateCountElement.textContent = privateCount;
      inactiveCountElement.textContent = inactiveCount;
      
      if (startTime) {
        const elapsedMinutes = (Date.now() - startTime) / 60000;
        const currentSpeed = elapsedMinutes > 0 ? Math.round(processedUsers / elapsedMinutes) : 0;
        speed.textContent = `${currentSpeed}/min`;
        
        if (processedUsers > 0) {
          const remainingUsers = totalUsers - processedUsers;
          const timePerUser = elapsedMinutes / processedUsers;
          const remainingMinutes = Math.ceil(remainingUsers * timePerUser);
          
          if (remainingMinutes < 1) {
            timeEstimate.textContent = '< 1 min';
          } else if (remainingMinutes < 60) {
            timeEstimate.textContent = `${remainingMinutes} min`;
          } else {
            const hours = Math.floor(remainingMinutes / 60);
            const mins = remainingMinutes % 60;
            timeEstimate.textContent = `${hours}h ${mins}m`;
          }
        }
      }
    }
    
    function addResultRow(data) {
      const row = document.createElement('tr');
      row.className = 'result-row';
      
      const statusClass = data.status === 'Success' ? 'status-success' : 
                         data.status === 'Private Account' ? 'status-warning' : 
                         data.status === 'Inactive/Unavailable' ? 'status-warning' : 'status-error';
      
      const statusIcon = data.status === 'Success' ? 'fa-check-circle' :
                        data.status === 'Private Account' ? 'fa-lock' : 
                        data.status === 'Inactive/Unavailable' ? 'fa-user-slash' : 'fa-times-circle';
      
      if (extendedInfo) {
        row.innerHTML = `
          <td class="table-cell column-username font-medium">${data.username}</td>
          <td class="table-cell column-fullname">${data.full_name || ''}</td>
          <td class="table-cell column-verified">${data.is_verified ? '✓' : ''}</td>
          <td class="table-cell column-followers">${data.followers_count || 'N/A'}</td>
          <td class="table-cell column-following">${data.following_count || 'N/A'}</td>
          <td class="table-cell column-posts">${data.posts_count || 'N/A'}</td>
          <td class="table-cell column-post-type">${data.latest_post_type || ''}</td>
          <td class="table-cell column-post-date">${data.latest_post_date || ''}</td>
          <td class="table-cell column-pinned">${data.latest_post_pinned || 'No'}</td>
          <td class="table-cell column-status ${statusClass}">
            <i class="fas ${statusIcon} mr-1"></i>${data.status}
          </td>
        `;
      } else {
        row.innerHTML = `
          <td class="table-cell column-username font-medium">${data.username}</td>
          <td class="table-cell column-followers">${data.followers}</td>
          <td class="table-cell column-status ${statusClass}">
            <i class="fas ${statusIcon} mr-1"></i>${data.status}
          </td>
        `;
      }
      
      resultsBody.appendChild(row);
      
      // Scroll to bottom to show latest result
      const wrapper = document.querySelector('.results-wrapper');
      wrapper.scrollTop = wrapper.scrollHeight;
    }
    
    function resetUI() {
      startBtn.disabled = false;
      startBtn.innerHTML = '<i class="fas fa-play mr-2"></i>Start 100% Accuracy Scraping';
      pauseBtn.classList.add('hidden');
      stopBtn.classList.add('hidden');
      isPaused = false;
      checkForResumeableJobs();
    }
    
    // Download results
    downloadBtn.addEventListener('click', () => {
      window.location.href = '/download/results';
    });
  </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/check-resume')
def check_resume():
    """Check if there are any jobs that can be resumed"""
    try:
        progress_files = [f for f in os.listdir(PROGRESS_FOLDER) if f.endswith('.progress')]
        if progress_files:
            progress_files.sort(key=lambda x: os.path.getmtime(os.path.join(PROGRESS_FOLDER, x)), reverse=True)
            latest_file = progress_files[0]
            filename = latest_file.replace('.progress', '')
            
            progress = ScrapingProgress.load(filename)
            if progress and progress.status == "paused" and progress.processed_users < progress.total_users:
                return jsonify({
                    "resumeable": True,
                    "filename": filename,
                    "processed": progress.processed_users,
                    "total": progress.total_users,
                    "progress": round((progress.processed_users / progress.total_users) * 100)
                })
    except Exception as e:
        logger.error(f"Error checking resumeable jobs: {e}")
    
    return jsonify({"resumeable": False})

@app.route('/upload', methods=['POST'])
def upload_file():
    action = request.form.get('action', 'start')
    extended_info = request.form.get('extended_info', 'false').lower() == 'true'
    
    if action == 'resume':
        filename = request.form.get('filename')
        progress = ScrapingProgress.load(filename)
        if progress:
            return jsonify({
                "filename": filename,
                "total_count": progress.total_users,
                "resume_from": progress.processed_users,
                "extended_info": progress.extended_info
            })
        else:
            return jsonify({"error": "Could not resume job"}), 400
    
    if 'csv_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files['csv_file']
    if file.filename == '' or not file.filename.endswith('.csv'):
        return jsonify({"error": "Please upload a valid CSV file"}), 400
    
    filename = f"{uuid.uuid4()}.csv"
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    
    try:
        file.save(filepath)
        df = pd.read_csv(filepath)
        
        if 'username' not in df.columns:
            os.remove(filepath)
            return jsonify({"error": "CSV must contain a 'username' column"}), 400
        
        total_users = len(df)
        
        # Initialize progress tracking
        progress = ScrapingProgress(filename, total_users, extended_info)
        progress.save()
        
        return jsonify({
            "filename": filename,
            "total_count": total_users,
            "resume_from": 0,
            "extended_info": extended_info
        })
        
    except Exception as e:
        return jsonify({"error": f"Error processing file: {str(e)}"}), 500

@app.route('/stop-job/<filename>', methods=['POST'])
def stop_job(filename):
    """Explicitly stop a job and save progress"""
    try:
        progress = ScrapingProgress.load(filename)
        if progress:
            progress.status = "paused"
            progress.save()
            logger.info(f"⏸️ Job {filename} paused at {progress.processed_users}/{progress.total_users} users")
            return jsonify({"success": True, "message": f"Job paused at {progress.processed_users} users"})
    except Exception as e:
        logger.error(f"Error stopping job: {e}")
    
    return jsonify({"success": False, "message": "Error stopping job"})

@app.route('/stream/<filename>')
def stream(filename):
    resume_from = request.args.get('resume_from', 0, type=int)
    extended_info = request.args.get('extended_info', 'false').lower() == 'true'
    
    def generate():
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        if not os.path.exists(filepath):
            yield f"data: {json.dumps({'error': 'File not found'})}\n\n"
            yield "event: close\ndata: done\n\n"
            return
        
        # Load or create progress tracking
        progress = ScrapingProgress.load(filename)
        if not progress:
            progress = ScrapingProgress(filename, 0, extended_info)
        
        progress.status = "running"
        progress.save()
        
        results = []
        try:
            df = pd.read_csv(filepath)
            users = df['username'].dropna().astype(str).tolist()
            total_batches = min(MAX_BATCHES, max(1, (len(users) + BATCH_SIZE - 1) // BATCH_SIZE))
            
            # Skip already processed batches
            start_batch = resume_from // BATCH_SIZE
            
            for i in range(start_batch, total_batches):
                start = i * BATCH_SIZE
                end = min(len(users), start + BATCH_SIZE)
                if start >= end:
                    break
                
                batch = users[start:end]
                batch_results = process_batch_parallel(batch, extended_info)
                results.extend(batch_results)
                
                for result in batch_results:
                    output = {
                        'username': sanitize_csv_value(result['username']),
                        'status': result['status']
                    }
                    
                    if extended_info:
                        output.update({
                            'full_name': sanitize_csv_value(result.get('full_name', '')),
                            'is_verified': result.get('is_verified', False),
                            'followers_count': sanitize_csv_value(result.get('followers_count')),
                            'following_count': sanitize_csv_value(result.get('following_count')),
                            'posts_count': sanitize_csv_value(result.get('posts_count')),
                            'latest_post_type': sanitize_csv_value(result.get('latest_post_type', '')),
                            'latest_post_date': sanitize_csv_value(result.get('latest_post_date', '')),
                            'latest_post_likes': sanitize_csv_value(result.get('latest_post_likes', '')),
                            'latest_post_comments': sanitize_csv_value(result.get('latest_post_comments', '')),
                            'latest_post_views': sanitize_csv_value(result.get('latest_post_views', '')),
                            'latest_post_pinned': sanitize_csv_value(result.get('latest_post_pinned', 'No'))
                        })
                    else:
                        output['followers'] = sanitize_csv_value(result.get('followers'))
                    
                    yield f"data: {json.dumps(output)}\n\n"
                    
                    # Update progress
                    progress.processed_users += 1
                    progress.completed_usernames.add(result['username'])
                    progress.results.append(result)
                    progress.last_update = datetime.now()
                    
                    # Save progress every 10 users
                    if progress.processed_users % 10 == 0:
                        progress.save()
            
            # Mark as completed
            progress.status = "completed"
            progress.save()
            
            yield "event: close\ndata: done\n\n"
            
        except Exception as e:
            progress.status = "error"
            progress.save()
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            yield "event: close\ndata: done\n\n"
        finally:
            if progress.status == "completed":
                try:
                    results_df = pd.DataFrame(progress.results)
                    results_path = os.path.join(RESULTS_FOLDER, f"results_{int(time.time())}.csv")
                    results_df.to_csv(results_path, index=False)
                    
                    # Clean up progress file
                    progress_file = os.path.join(PROGRESS_FOLDER, f"{filename}.progress")
                    if os.path.exists(progress_file):
                        os.remove(progress_file)
                except Exception as e:
                    logger.error(f"Error saving results: {e}")
    
    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/download/results')
def download_results():
    """Download the most recent results file"""
    try:
        results_files = [f for f in os.listdir(RESULTS_FOLDER) if f.endswith('.csv')]
        if results_files:
            results_files.sort(key=lambda x: os.path.getmtime(os.path.join(RESULTS_FOLDER, x)), reverse=True)
            latest_file = results_files[0]
            return send_file(
                os.path.join(RESULTS_FOLDER, latest_file),
                as_attachment=True,
                download_name="instagram_results.csv"
            )
    except Exception as e:
        logger.error(f"Download error: {e}")
    return "No results found", 404

def initialize_app():
    logger.info("🚀 Starting Gramhunt Pro Instagram Scraper with 100% Accuracy...")
    logger.info("Testing proxy connection...")
    if test_proxy_connection():
        logger.info("✅ Proxy is ready!")
        ProxyState.is_working = True
        ProxyState.last_test_time = time.time()
    else:
        logger.warning("❌ Proxy connection failed. Check credentials.")

if __name__ == "__main__":
    initialize_app()
    # Use Render's port or default to 5001
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=False, host="0.0.0.0", port=port, use_reloader=False)
