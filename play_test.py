from playwright.sync_api import sync_playwright
import requests
import io
from pdfminer.high_level import extract_text
from urllib.parse import urlparse

from enum import Enum

class URLType(Enum):
    HTML = 0
    PDF = 1
    DOCX = 2
    ERROR = -1

def is_pdf_url(url):
    """Checks if a URL points to a PDF by inspecting Content-Type header or URL suffix."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'Accept': 'text/html,application/pdf,*/*;q=0.9',  # 明确声明可以接受 PDF
        }
        
        # 先检查 URL 后缀（快速判断，避免网络请求）
        url_lower = url.lower()
        if url_lower.endswith('.pdf'):
            return URLType.PDF
        if url_lower.endswith('.docx'):
            return URLType.DOCX
        
        # 发送 GET 请求（只获取头部，stream=True 避免下载内容）
        response = requests.get(
            url,
            headers=headers,
            stream=True,  # 不下载内容，只获取头
            allow_redirects=True,
            timeout=5,
        )
        
        # 确保 HTTP 请求成功
        response.raise_for_status()  
        
        # 获取 Content-Type
        content_type = response.headers.get('Content-Type', '').lower()
        
        # 显式关闭连接（避免未关闭的连接）
        response.close()
        
        if 'application/pdf' in content_type:
            return URLType.PDF
        elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
            return URLType.DOCX
        else:
            return URLType.HTML  # Likely a webpage or HTML content
    
    except requests.exceptions.RequestException as e:
        print(f"Error checking {url}: {e}")
        return URLType.ERROR

def is_valid_url(url):
    """
    Checks if a URL is valid (has a scheme and a network location).
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


def extract_text_from_pdf_url(url):
    """Downloads a PDF from a URL and extracts the text."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        pdf_file = io.BytesIO(response.content)
        text = extract_text(pdf_file)
        return text
    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF: {e}")
        return None
    except Exception as e:
        print(f"Error processing PDF: {e}")
        return None


def filter_html(html, domain=''):
    """
    Filters HTML content by removing specified tags and elements.
    """
    from bs4 import BeautifulSoup, Comment
    from urllib.parse import urljoin

    soup = BeautifulSoup(html, 'lxml')
    if not soup.body:
        soup = BeautifulSoup(f"<body>{html}</body>", "lxml")

    # Remove all comments
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.decompose()

    excluded_tags = [
        "nav",
        "footer",
        "header",
        "br"
    ]

    for tag in excluded_tags:
        for element in soup.find_all(tag):
            element.decompose()

    body = soup.find("body")

    def pure_link(url):
        if len(url) == 0:
            return ''
        if url.startswith('//'):
            url = 'http:' + url
        elif domain and not url.startswith(('http://', 'https://')):
            url = urljoin(f"http://{domain}", url)
        return url

    images = []
    for img in body.find_all('img'):
        if 'src' in img.attrs:
            img_url = img['src']
            if img_url.find('base64') >= 0:
                img.decompose()
                continue
            if len(img_url) == 0:
                continue
            img_url = pure_link(img_url)
            images.append(img_url)
        img.decompose()

    pdf_links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href and '.pdf' in href.lower():
            pdf_url = pure_link(href)
            pdf_links.append(pdf_url)
    return str(body), images, pdf_links


from urllib.parse import urljoin

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    )

    # url = "https://www.fortleenj.org/DocumentCenter/View/5453/Business-Registration-081522-PDF"  # 测试 URL
    # url = "https://www.fortleenj.org/DocumentCenter/View/4156/FEMA-Assistance-for-NJ-Survivors-Affected-by-Hurricane-Ida-PDF" # doc
    # url = "https://www.fortleenj.org/common/modules/iCalendar/iCalendar.aspx?feed=calendar&eventID=1391" # ics
    
    # url = "https://www.fortleenj.org/246/Rescue-Company-Number-2"
    # url = "https://macsdefense.com/"
    # url = "https://www.fortleenj.org/731/Fort-Lee-On-Demand"
    url = "https://www.creatif.com/livingston-nj/camp-calendar"  # 测试 URL

    try:
        if is_valid_url(url):
            url_type = is_pdf_url(url)
            if url_type == URLType.PDF:
                # # 处理 PDF 文件
                # pdf_text = extract_text_from_pdf_url(url)
                # if pdf_text:
                #     print("PDF Text (truncated):\n", pdf_text[:500])  # Print first 500 chars
                # else:
                #     print("Could not extract text from PDF.")

                # Modify HTML content to include the PDF url
                html_content = f'<html><body><a href="{url}">PDF Document</a></body></html>'
                remaining_content, images, pdfs = filter_html(html_content, domain=urlparse(url).netloc)
                print("Modified HTML Content (truncated):\n", remaining_content[:])  # Print first 500 chars
                print("PDFs:", pdfs)

            else:
                # 处理 HTML 网页
                try:
                    page.goto(url, wait_until='networkidle', timeout=60000)
                    html_content = page.content()
                    remaining_content, images, pdfs = filter_html(html_content, domain=urlparse(url).netloc)
                    print("HTML Content (truncated):\n", remaining_content[:])  # Print first 500 chars
                except Exception as e:
                    print(f"Error loading or processing HTML: {e}")
        else:
            print(f"Invalid URL: {url}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        browser.close()