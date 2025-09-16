#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class FastCampusCrawler:
    """FastCampus 강의 데이터 크롤러."""
    
    # 클래스 상수
    MAIN_CATEGORIES = {
        'AI TECH', 'AI CREATIVE', 'AI/업무생산성', '개발/데이터', 
        '디자인', '영상/3D', '금융/투자', '드로잉/일러스트', 
        '비즈니스/기획'
    }
    
    CRAWLING_STEPS = [
        "초기화",
        "메인 카테고리 수집",
        "하위 카테고리 수집", 
        "HTML 저장",
        "강의 목록 수집",
        "강의 상세 정보 수집",
        "이미지 다운로드",
        "데이터 저장",
        "완료"
    ]
    
    TIMEOUTS = {
        'default': 30000,
        'navigation': 60000,
        'image_load': 10000
    }
    
    HEADERS = {
        'default': {
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36'),
            'Accept': ('text/html,application/xhtml+xml,'
                      'application/xml;q=0.9,image/webp,*/*;q=0.8'),
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        },
        'image': {
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36'),
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8'
        }
    }
    
    SELECTORS = {
        'main_category': ('div.GNBDesktopCategoryItem_container__ln5E6 '
                         'a[href*="category_online"]'),
        'sub_category': ('li.GNBDesktopCategoryItem_subCategory__twmcG '
                        'a[href*="category_online"]'),
        'course_card': '[data-e2e="course-card"], .course-card, .course-item',
        'course_link': 'a[href*="/data_online_"]',
        'category_nav': '[data-e2e="navigation-category"]'
    }
    
    # 이미지 관련 상수
    VALID_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg']
    MIN_IMAGE_SIZE = 1000
    MAX_THUMBNAILS = 2
    
    def __init__(self, base_url="https://fastcampus.co.kr/", 
                 output_dir="./json_mvp"):
        """크롤러 초기화.
        
        Args:
            base_url (str): 크롤링할 기본 URL
            output_dir (str): 출력 디렉토리 경로
        """
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.data = {
            'main_categories': [],
            'sub_categories': [],
            'courses': [],
            'course_details': []
        }
        self.seen_urls = set()
        self.current_step = 0
        self.start_time = None
        self.course_titles = {}  # URL -> 제목 매핑 저장
        
        self._setup_directories()
        self._log_progress("크롤러 초기화 완료")
    
    def _setup_directories(self):
        """디렉토리 구조 생성."""
        directories = [
            "main_categories", "sub_categories", "courses",
            "sumnail_images", "lect_images", "json_files"
        ]
        
        for directory in directories:
            (self.output_dir / directory).mkdir(exist_ok=True)
    
    def _log_progress(self, message, step=None):
        """진행 상황 로깅.
        
        Args:
            message (str): 로그 메시지
            step (int, optional): 현재 단계 번호
        """
        if step is not None:
            self.current_step = step
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        step_name = (self.CRAWLING_STEPS[self.current_step] 
                    if self.current_step < len(self.CRAWLING_STEPS) 
                    else "알 수 없음")
        
        print(f"[{timestamp}] [{step_name}] {message}")
        sys.stdout.flush()
    
    def _log_step_start(self, step_name):
        """단계 시작 로깅.
        
        Args:
            step_name (str): 단계 이름
        """
        self.current_step = self.CRAWLING_STEPS.index(step_name)
        self._log_progress(f"{step_name} 시작")
    
    def _log_step_complete(self, step_name, count=None):
        """단계 완료 로깅.
        
        Args:
            step_name (str): 단계 이름
            count (int, optional): 수집된 항목 수
        """
        if count is not None:
            self._log_progress(f"{step_name} 완료 - {count}개 항목 수집")
        else:
            self._log_progress(f"{step_name} 완료")
    
    def _log_error(self, message, error=None):
        """에러 로깅.
        
        Args:
            message (str): 에러 메시지
            error (Exception, optional): 예외 객체
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        if error:
            print(f"[{timestamp}] [ERROR] {message}: {str(error)}")
        else:
            print(f"[{timestamp}] [ERROR] {message}")
        sys.stdout.flush()
    
    def _setup_browser(self, playwright):
        """브라우저 설정.
        
        Args:
            playwright: Playwright 인스턴스
            
        Returns:
            tuple: (browser, page) 튜플
        """
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=self.HEADERS['default']['User-Agent'],
            extra_http_headers=self.HEADERS['default']
        )
        page = context.new_page()
        page.set_default_timeout(self.TIMEOUTS['default'])
        page.set_default_navigation_timeout(self.TIMEOUTS['navigation'])
        return browser, page
    
    def _safe_page_load(self, page, url, retries=3):
        """안전한 페이지 로딩.
        
        Args:
            page: Playwright 페이지 객체
            url (str): 로드할 URL
            retries (int): 재시도 횟수
            
        Returns:
            bool: 로딩 성공 여부
        """
        for attempt in range(retries):
            try:
                page.goto(url, timeout=self.TIMEOUTS['navigation'])
                page.wait_for_load_state("networkidle", timeout=30000)
                return True
            except Exception:
                if attempt < retries - 1:
                    page.wait_for_timeout(3000)
        return False
    
    def _scroll_page(self, page):
        """동적 콘텐츠 로드를 위한 스크롤.
        
        Args:
            page: Playwright 페이지 객체
        """
        last_height = page.evaluate("document.body.scrollHeight")
        
        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            new_height = page.evaluate("document.body.scrollHeight")
            
            if new_height == last_height:
                break
            last_height = new_height
    
    def _prepare_navigation(self, page):
        """네비게이션 준비.
        
        Args:
            page: Playwright 페이지 객체
        """
        try:
            # 팝업 마스크 제거
            popup_mask = page.locator('.fc-popup-mask')
            if popup_mask.is_visible():
                popup_mask.evaluate('element => element.remove()')
                page.wait_for_timeout(1000)
        except Exception:
            pass
        
        try:
            # 페이지가 완전히 로드될 때까지 대기
            page.wait_for_load_state("networkidle", timeout=30000)
            
            # 카테고리 버튼 클릭 - 여러 선택자 시도
            selectors = [
                '[data-e2e="navigation-category"]',
                '.category-button',
                '.nav-category', 
                '.gnb-category',
                'button[aria-label*="카테고리"]',
                'button:has-text("카테고리")'
            ]
            
            clicked = False
            for selector in selectors:
                try:
                    button = page.locator(selector).first
                    if button.is_visible(timeout=5000):
                        button.click(timeout=10000)
                        page.wait_for_timeout(3000)
                        clicked = True
                        self._log_progress(f"카테고리 버튼 클릭 성공: {selector}")
                        break
                except Exception:
                    continue
            
            if not clicked:
                self._log_error("모든 카테고리 버튼 선택자 실패")
                
        except Exception as e:
            self._log_error(f"네비게이션 준비 실패", e)
    
    def _extract_main_categories(self, page):
        """메인 카테고리 추출.
        
        Args:
            page: Playwright 페이지 객체
        """
        self._log_step_start("메인 카테고리 수집")
        self._prepare_navigation(page)
        links = page.locator(self.SELECTORS['main_category']).all()
        seen = set()

        for i, link in enumerate(links, 1):
            try:
                name = link.text_content().strip()
                url = link.get_attribute('href')

                if (name and url and name in self.MAIN_CATEGORIES 
                    and name not in seen):
                    seen.add(name)
                    
                    if url.startswith('/'):
                        url = urljoin(self.base_url, url)
                    
                    self.data['main_categories'].append({
                        "메인카테고리": name,
                        "메인카테고리링크": url,
                        "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    self._log_progress(f"메인 카테고리 발견: {name}")
            except Exception as e:
                self._log_error(f"메인 카테고리 처리 중 오류 (링크 {i})", e)
                continue
        
        self._log_step_complete("메인 카테고리 수집", 
                               len(self.data['main_categories']))
    
    def _find_parent_category(self, sub_link):
        """상위 카테고리 찾기.
        
        Args:
            sub_link: 하위 카테고리 링크 요소
            
        Returns:
            str: 상위 카테고리 이름 또는 None
        """
        try:
            xpath = ('xpath=ancestor::div[contains(@class, '
                    '"GNBDesktopCategoryItem_container__ln5E6")]')
            parent = sub_link.locator(xpath).first
            
            if parent.count() > 0:
                main_link = parent.locator('a[href*="category_online"]').first
                if main_link.count() > 0:
                    return main_link.text_content().strip()
        except Exception:
            pass
        return None
    
    def _extract_sub_categories(self, page):
        """하위 카테고리 추출.
        
        Args:
            page: Playwright 페이지 객체
        """
        self._log_step_start("하위 카테고리 수집")
        self._prepare_navigation(page)
        links = page.locator(self.SELECTORS['sub_category']).all()
        
        for i, link in enumerate(links, 1):
            try:
                name = link.text_content().strip()
                url = link.get_attribute('href')

                if not name or not url or url in self.seen_urls:
                    continue
                
                self.seen_urls.add(url)
                parent = self._find_parent_category(link)
                
                if parent:
                    if url.startswith('/'):
                        url = urljoin(self.base_url, url)
                    
                    self.data['sub_categories'].append({
                        "메인카테고리": parent,
                        "하위카테고리": name,
                        "하위카테고리링크": url,
                        "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    self._log_progress(f"하위 카테고리 발견: {parent} > {name}")
            except Exception as e:
                self._log_error(f"하위 카테고리 처리 중 오류 (링크 {i})", e)
                continue
        
        self._log_step_complete("하위 카테고리 수집", 
                               len(self.data['sub_categories']))
    
    def _save_html(self, page, name, category_type):
        """HTML 콘텐츠 저장.
        
        Args:
            page: Playwright 페이지 객체
            name (str): 파일명으로 사용할 이름
            category_type (str): 카테고리 타입 (폴더명)
        """
        html_content = page.content()
        safe_name = name.replace('/', '_').replace(' ', '_')
        html_file = self.output_dir / f"{category_type}/{safe_name}.html"
        
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _extract_title(self, card, course_url=None):
        """강의 제목 추출.
        
        Args:
            card: 강의 카드 요소
            course_url (str, optional): 강의 URL
            
        Returns:
            str: 강의 제목
        """
        # 먼저 저장된 매핑에서 찾기
        if course_url and course_url in self.course_titles:
            return self.course_titles[course_url]
        
        # DOM에서 추출 - 사용자가 제공한 정확한 선택자 우선 사용
        selectors = [
            '.CourseCard_courseCardTitle__1HQgO',  # 정확한 클래스명
            '[data-e2e="display-card"]',  # data-e2e 속성
            'h3', 'h4', '.course-title', '.title', 
            '[data-e2e="course-title"]', '.course-name',
            '[class*="title"]', '[class*="Title"]'
        ]
        
        for selector in selectors:
            element = card.locator(selector).first
            if element.count() > 0:
                title = element.text_content().strip()
                if (title and len(title) > 3 and not title.isdigit() 
                    and not title.endswith('+')):
                    self._log_progress(f"강의 제목 추출 성공: {title[:50]}... "
                                     f"(선택자: {selector})")
                    return title
        
        # 마지막 fallback - 숫자가 아닌 텍스트 찾기
        full_text = card.text_content().strip()
        lines = [line.strip() for line in full_text.split('\n') 
                if line.strip()]
        
        for line in lines:
            if (len(line) > 3 and not line.isdigit() 
                and not line.startswith(('+', '⚠️')) 
                and not line.endswith('+')):
                self._log_progress(f"강의 제목 추출 성공 (fallback): "
                                 f"{line[:50]}...")
                return line
                
        return "제목 없음"
    
    def _extract_course_url(self, card):
        """강의 URL 추출.
        
        Args:
            card: 강의 카드 요소
            
        Returns:
            str: 강의 URL 또는 None
        """
        try:
            url = card.get_attribute('href')
            if url:
                return url
        except Exception:
            pass
        
        try:
            link = card.locator(self.SELECTORS['course_link']).first
            if link.count() > 0:
                return link.get_attribute('href')
        except Exception:
            pass
        
        return None
    
    def _normalize_url(self, url):
        """URL 정규화.
        
        Args:
            url (str): 정규화할 URL
            
        Returns:
            str: 정규화된 URL
        """
        if url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            return urljoin(self.base_url, url)
        elif not url.startswith('http'):
            return urljoin(self.base_url, url)
        return url.replace('hhttps://', 'https://')
    
    def _download_image(self, url, retries=3):
        """이미지 다운로드.
        
        Args:
            url (str): 이미지 URL
            retries (int): 재시도 횟수
            
        Returns:
            requests.Response: 응답 객체 또는 None
        """
        for attempt in range(retries):
            try:
                response = requests.get(
                    url, 
                    timeout=15, 
                    headers=self.HEADERS['image']
                )
                if response.status_code == 200:
                    return response
                elif attempt < retries - 1:
                    time.sleep(1)
            except Exception:
                if attempt < retries - 1:
                    time.sleep(1)
        return None
    
    def _is_valid_image_url(self, url):
        """유효한 이미지 URL인지 확인.
        
        Args:
            url (str): 확인할 URL
            
        Returns:
            bool: 유효한 이미지 URL 여부
        """
        if not url or len(url) < 10:
            return False
        
        url_lower = url.lower()
        
        # URL에 이미지 확장자가 있거나, CDN URL인 경우
        return (any(ext in url_lower for ext in self.VALID_IMAGE_EXTENSIONS) 
                or 'cdn' in url_lower 
                or 'image' in url_lower
                or 'thumbnail' in url_lower)
    
    def _collect_thumbnails(self, card, course_url):
        """썸네일 이미지 수집.
        
        Args:
            card: 강의 카드 요소
            course_url (str): 강의 URL
        """
        course_name = course_url.split('/')[-1]
        thumbnail_dir = self.output_dir / "sumnail_images" / course_name
        thumbnail_dir.mkdir(parents=True, exist_ok=True)
        
        # 정확한 썸네일 선택자만 사용 (우선순위 순)
        img_selectors = [
            'img[alt="강의 대표이미지"]',  # 가장 정확한 선택자
            'img[class*="CourseCard"][data-nimg="fill"]',  # 조합 선택자
            'img[class*="CourseCard"]',  # CourseCard 관련
            'img[data-nimg="fill"]'  # Next.js 이미지
        ]
        
        saved_count = 0
        saved_urls = set()  # 중복 URL 체크용
        
        for selector in img_selectors:
            if saved_count >= self.MAX_THUMBNAILS:
                break
                
            try:
                images = card.locator(selector).all()
                for img in images:
                    if saved_count >= self.MAX_THUMBNAILS:
                        break
                        
                    url = (img.get_attribute('src') or 
                           img.get_attribute('data-src'))
                    
                    if url:
                        url = self._normalize_url(url)
                        
                        # 중복 URL 체크
                        if url in saved_urls:
                            continue
                        
                        # 유효한 이미지 URL인지 확인
                        if not self._is_valid_image_url(url):
                            continue
                            
                        response = self._download_image(url)
                        
                        if (response and 
                            len(response.content) > self.MIN_IMAGE_SIZE):
                            ext = (os.path.splitext(urlparse(url).path)[1] 
                                   or '.webp')
                            filename = f"thumbnail_{saved_count + 1}{ext}"
                            
                            with open(thumbnail_dir / filename, 'wb') as f:
                                f.write(response.content)
                            
                            saved_urls.add(url)
                            saved_count += 1
                            self._log_progress(f"썸네일 이미지 저장: {filename} "
                                             f"({len(response.content)} bytes)")
            except Exception as e:
                self._log_error(f"썸네일 이미지 처리 중 오류", e)
                continue
    
    def _extract_course_titles_from_json(self, page):
        """페이지의 JSON 데이터에서 강의 제목 추출.
        
        Args:
            page: Playwright 페이지 객체
            
        Returns:
            bool: 추출 성공 여부
        """
        try:
            json_scripts = page.locator('script[type="application/json"]').all()
            
            for script in json_scripts:
                try:
                    json_text = script.text_content()
                    if not json_text:
                        continue
                        
                    data = json.loads(json_text)
                    
                    # JSON 구조 탐색하여 강의 데이터 찾기
                    def find_courses(obj, path=""):
                        courses = []
                        
                        if isinstance(obj, dict):
                            if 'publicTitle' in obj and 'slug' in obj:
                                # 강의 데이터 발견
                                title = obj.get('publicTitle', '')
                                slug = obj.get('slug', '')
                                if title and slug:
                                    courses.append({
                                        'title': title,
                                        'slug': slug,
                                        'url': f"https://fastcampus.co.kr/{slug}"
                                    })
                            
                            # 중첩된 객체들도 탐색
                            for key, value in obj.items():
                                courses.extend(find_courses(value, 
                                                          f"{path}.{key}"))
                                
                        elif isinstance(obj, list):
                            for i, item in enumerate(obj):
                                courses.extend(find_courses(item, 
                                                          f"{path}[{i}]"))
                        
                        return courses
                    
                    courses = find_courses(data)
                    
                    # URL -> 제목 매핑 저장
                    for course in courses:
                        self.course_titles[course['url']] = course['title']
                        self._log_progress(f"강의 제목 매핑: {course['title']}")
                    
                    if courses:
                        self._log_progress(f"JSON에서 {len(courses)}개 강의 제목 "
                                         f"추출 완료")
                        return True
                        
                except Exception as e:
                    self._log_error(f"JSON 파싱 중 오류", e)
                    continue
                    
        except Exception as e:
            self._log_error(f"강의 제목 추출 중 오류", e)
        
        return False
    
    def _extract_courses(self, page, subcategory, max_courses=3):
        """강의 정보 추출.
        
        Args:
            page: Playwright 페이지 객체
            subcategory (dict): 하위 카테고리 정보
            max_courses (int): 최대 수집할 강의 수
        """
        self._log_progress(f"강의 수집 시작: {subcategory['메인카테고리']} > "
                          f"{subcategory['하위카테고리']}")
        
        if not self._safe_page_load(page, subcategory['하위카테고리링크']):
            self._log_error(f"페이지 로드 실패: {subcategory['하위카테고리링크']}")
            return
        
        self._scroll_page(page)
        page.wait_for_timeout(3000)
        
        # 먼저 JSON에서 강의 제목들 추출
        self._extract_course_titles_from_json(page)
        
        # 강의 카드 컨테이너 찾기 - 여러 선택자 시도
        card_selectors = [
            '[data-e2e="course-card"]',
            '.course-card', 
            '.course-item',
            'div[class*="CourseCard"]',
            'div[class*="courseCard"]',
            'div[class*="course-card"]',
            'article[class*="course"]',
            'div[class*="card"]'
        ]
        
        cards = []
        for selector in card_selectors:
            found_cards = page.locator(selector).all()
            if found_cards:
                cards = found_cards
                self._log_progress(f"강의 카드 발견: {len(cards)}개 "
                                 f"(선택자: {selector})")
                break
        
        if not cards:
            # 마지막 fallback - 링크 요소들을 카드로 사용
            cards = page.locator(self.SELECTORS['course_link']).all()
            self._log_progress(f"대체 선택자로 강의 카드 발견: {len(cards)}개 "
                             f"(선택자: {self.SELECTORS['course_link']})")
        
        collected = 0
        for i, card in enumerate(cards, 1):
            if collected >= max_courses:
                break
                
            try:
                url = self._extract_course_url(card)
                if not url:
                    continue
                
                if url.startswith('/'):
                    url = urljoin(self.base_url, url)
                
                if '/event_online_' in url or url in self.seen_urls:
                    continue
                
                self.seen_urls.add(url)
                title = self._extract_title(card, url)
                
                self.data['courses'].append({
                    "메인카테고리": subcategory['메인카테고리'],
                    "하위카테고리": subcategory['하위카테고리'],
                    "강의제목": title,
                    "강의링크": url,
                    "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
                self._log_progress(f"강의 발견: {title}")
                self._collect_thumbnails(card, url)
                collected += 1
                
            except Exception as e:
                self._log_error(f"강의 처리 중 오류 (카드 {i})", e)
                continue
        
        self._log_progress(f"강의 수집 완료: {collected}개")
    
    def _extract_page_content(self, soup):
        """페이지 텍스트 콘텐츠 추출.
        
        Args:
            soup: BeautifulSoup 객체
            
        Returns:
            str: 추출된 텍스트
        """
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        main_content = soup.find('main') or soup.find('body') or soup
        text = main_content.get_text()
        
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return '\n'.join(lines) if lines else "텍스트 없음"
    
    def _extract_course_images(self, soup, course_url):
        """강의 페이지 이미지 추출.
        
        Args:
            soup: BeautifulSoup 객체
            course_url (str): 강의 URL
            
        Returns:
            list: 이미지 정보 리스트
        """
        images = []
        course_name = course_url.split('/')[-1]
        image_dir = self.output_dir / "lect_images" / course_name
        image_dir.mkdir(parents=True, exist_ok=True)
        
        for i, img in enumerate(soup.find_all('img')):
            try:
                url = (img.get('src') or img.get('data-src'))
                if not url:
                    continue
                
                url = self._normalize_url(url)
                response = self._download_image(url)
                
                if response and len(response.content) > 0:
                    ext = os.path.splitext(urlparse(url).path)[1] or '.jpg'
                    filename = f"image_{i+1}{ext}"
                    
                    with open(image_dir / filename, 'wb') as f:
                        f.write(response.content)
                    
                    images.append({
                        "url": url,
                        "filename": filename,
                        "local_path": str(image_dir / filename),
                        "size_bytes": len(response.content)
                    })
            except Exception:
                continue
        
        return images
    
    def _extract_course_detail(self, page, course_url):
        """강의 상세 정보 추출.
        
        Args:
            page: Playwright 페이지 객체
            course_url (str): 강의 URL
            
        Returns:
            dict: 강의 상세 정보 또는 None
        """
        course_name = course_url.split('/')[-1]
        self._log_progress(f"강의 상세 정보 수집: {course_name}")
        
        if not self._safe_page_load(page, course_url):
            self._log_error(f"강의 페이지 로드 실패: {course_url}")
            return None
        
        self._scroll_page(page)
        page.wait_for_timeout(3000)
        
        html_content = page.content()
        safe_name = course_url.split('/')[-1]
        
        with open(self.output_dir / f"courses/{safe_name}.html", 
                  'w', encoding='utf-8') as f:
            f.write(html_content)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        title_selectors = ['h1', '[data-e2e="course-title"]', '.title']
        title = "제목 없음"
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element and element.get_text().strip():
                title = element.get_text().strip()
                if title != "root layout":
                    break
        
        self._log_progress(f"강의 상세 정보 수집 완료: {title}")
        
        return {
            "강의명": title,
            "추출된_텍스트": self._extract_page_content(soup),
            "강의링크": course_url,
            "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "이미지_정보": self._extract_course_images(soup, course_url)
        }
    
    def _validate_data(self, data_type, data):
        """데이터 품질 검증.
        
        Args:
            data_type (str): 데이터 타입
            data (list): 검증할 데이터
            
        Returns:
            bool: 검증 통과 여부
        """
        if not data:
            return False
        
        validators = {
            "main_categories": lambda d: (len(d) == 9 and 
                                        all(item.get('메인카테고리') 
                                            for item in d)),
            "sub_categories": lambda d: (len(d) >= 40 and 
                                       all(item.get('하위카테고리') 
                                           for item in d)),
            "courses": lambda d: all(item.get('강의제목') != "제목 없음" 
                                   for item in d),
            "course_details": lambda d: all(item.get('강의명') != "제목 없음" 
                                          for item in d)
        }
        
        validator = validators.get(data_type)
        return validator(data) if validator else True
    
    def _save_json(self, data, filename):
        """JSON 파일 저장.
        
        Args:
            data (list): 저장할 데이터
            filename (str): 파일명
        """
        data_type = filename.replace('.json', '')
        
        if data and self._validate_data(data_type, data):
            json_path = self.output_dir / "json_files" / filename
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    
    def run(self):
        """크롤링 실행."""
        self.start_time = time.time()
        self._log_progress("FastCampus 크롤링 시작!")
        
        with sync_playwright() as p:
            browser, page = self._setup_browser(p)
            
            try:
                self._log_progress("브라우저 설정 완료")
                
                if not self._safe_page_load(page, self.base_url):
                    self._log_error("메인 페이지 로드 실패")
                    return
                
                self._log_progress("메인 페이지 로드 완료")
                
                # 메인 카테고리 수집
                self._extract_main_categories(page)
                
                # 하위 카테고리 수집
                self._extract_sub_categories(page)
                
                # HTML 저장
                self._log_step_start("HTML 저장")
                for i, category in enumerate(self.data['main_categories'][:2], 1):
                    self._log_progress(f"메인 카테고리 HTML 저장 ({i}/2): "
                                     f"{category['메인카테고리']}")
                    if self._safe_page_load(page, category['메인카테고리링크']):
                        self._scroll_page(page)
                        self._save_html(page, category['메인카테고리'], 
                                      "main_categories")
                
                for i, category in enumerate(self.data['sub_categories'][:2], 1):
                    self._log_progress(f"하위 카테고리 HTML 저장 ({i}/2): "
                                     f"{category['하위카테고리']}")
                    if self._safe_page_load(page, category['하위카테고리링크']):
                        self._scroll_page(page)
                        self._save_html(page, category['하위카테고리'], 
                                      "sub_categories")
                self._log_step_complete("HTML 저장")
                
                # 강의 목록 수집
                self._log_step_start("강의 목록 수집")
                for i, subcategory in enumerate(self.data['sub_categories'][:2], 1):
                    self._log_progress(f"강의 수집 진행 ({i}/2): "
                                     f"{subcategory['하위카테고리']}")
                    self._extract_courses(page, subcategory, max_courses=2)
                self._log_step_complete("강의 목록 수집", 
                                      len(self.data['courses']))
                
                # 강의 상세 정보 수집
                self._log_step_start("강의 상세 정보 수집")
                for i, course in enumerate(self.data['courses'][:2], 1):
                    self._log_progress(f"강의 상세 정보 수집 ({i}/2): "
                                     f"{course['강의제목']}")
                    detail = self._extract_course_detail(page, 
                                                       course['강의링크'])
                    if detail:
                        self.data['course_details'].append(detail)
                self._log_step_complete("강의 상세 정보 수집", 
                                      len(self.data['course_details']))
                
                # 데이터 저장
                self._log_step_start("데이터 저장")
                self._save_json(self.data['main_categories'], 
                               "main_categories.json")
                self._save_json(self.data['sub_categories'], 
                               "sub_categories.json")
                self._save_json(self.data['courses'], "courses_list.json")
                self._save_json(self.data['course_details'], 
                               "course_details.json")
                self._log_step_complete("데이터 저장")
                
                # 완료
                elapsed_time = time.time() - self.start_time
                self._log_progress(f"크롤링 완료! 총 소요시간: "
                                 f"{elapsed_time:.2f}초")
                self._log_progress(f"수집 결과:")
                self._log_progress(f"   - 메인 카테고리: "
                                 f"{len(self.data['main_categories'])}개")
                self._log_progress(f"   - 하위 카테고리: "
                                 f"{len(self.data['sub_categories'])}개")
                self._log_progress(f"   - 강의 목록: "
                                 f"{len(self.data['courses'])}개")
                self._log_progress(f"   - 강의 상세: "
                                 f"{len(self.data['course_details'])}개")
                
            except Exception as e:
                self._log_error("크롤링 중 치명적 오류 발생", e)
            finally:
                browser.close()
                self._log_progress("브라우저 종료")

def main():
    """Application entry point."""
    crawler = FastCampusCrawler()
    crawler.run()


if __name__ == "__main__":
    main()