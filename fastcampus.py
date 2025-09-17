#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class CrawlerConfig:
    """크롤러 설정 관리 클래스."""
    def __init__(self):
        # 기본 설정 
        self.base_url = "https://fastcampus.co.kr/"
        self.output_dir = "./optimized_crawl"

        # 성능 설정 (M3 칩 최적화)
        self.max_workers = 12
        self.max_concurrent_courses = 20
        self.max_retries = 3

        # 타임아웃 설정 (초) - 참고 코드와 동일하게 조정
        self.timeouts = {
            'default': 15,  # 15초로 단축 (속도 우선)
            'navigation': 20,  # 20초로 단축
            'image_load': 60,  # 60초로 단축 (속도와 안정성 균형)
            'network_idle': 10  # 10초로 단축 (빠른 처리)
        }

        # 이미지 설정
        self.valid_image_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg']
        self.min_image_size = 1000
        self.max_thumbnails = None  # 제한 없음

        # 메인 카테고리
        self.main_categories = {
            'AI TECH', 'AI CREATIVE', 'AI/업무생산성', '개발/데이터',
            '디자인', '영상/3D', '금융/투자', '드로잉/일러스트',
            '비즈니스/기획'
        }

        # 선택자 설정
        self.selectors = {
            'main_category': ('div.GNBDesktopCategoryItem_container__ln5E6 '
                              'a[href*="category_online"]'),
            'sub_category': ('li.GNBDesktopCategoryItem_subCategory__twmcG '
                             'a[href*="category_online"]'),
            'course_card': [
                '[data-e2e="course-card"]',
                'div[class*="CourseCard_courseCard__"]',
                'div[class*="CourseCard"][class*="courseCard__"]',
                'article[class*="course"]',
                'div[class*="course-card"]',
                'div[class*="card"]',
                'a[href*="/data_online_"]',
                'a[href*="/dgn_online_"]',
                'a[href*="/biz_online_"]',
                'a[href*="/course_"]',
                'a[href*="/online_"]',
                'a[href^="/"]'
            ],
            'course_link': [
                'a[href*="/data_online_"]',
                'a[href*="/dgn_online_"]',
                'a[href*="/biz_online_"]',
                'a[href*="/course_"]',
                'a[href*="/online_"]',
                'a[href^="/"]'
            ],
            'category_nav': '[data-e2e="navigation-category"]'
        }

        # HTTP 헤더
        self.headers = {
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


class Logger:
    """로깅 시스템 관리 클래스."""

    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger('FastCampusCrawler')
        self.logger.setLevel(log_level)

        # 콘솔 핸들러
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)

        # 포맷터
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(formatter)

        # 핸들러 추가
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)

    def info(self, message: str):
        """정보 로그."""
        self.logger.info(message)

    def error(self, message: str, exception: Optional[Exception] = None):
        """에러 로그."""
        if exception:
            self.logger.error(f"{message}: {str(exception)}")
        else:
            self.logger.error(message)

    def warning(self, message: str):
        """경고 로그."""
        self.logger.warning(message)


class DataManager:
    """데이터 관리 클래스."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.data = {
            'main_categories': [],
            'sub_categories': [],
            'courses': [],
            'course_details': []
        }
        self.seen_urls: Set[str] = set()
        self.course_titles: Dict[str, str] = {}

        # 진행률 추적
        self.progress = {
            'total_subcategories': 0,
            'completed_subcategories': 0,
            'total_courses': 0,
            'completed_courses': 0,
            'total_details': 0,
            'completed_details': 0
        }

    def add_main_category(self, category_data: Dict):
        """메인 카테고리 추가."""
        self.data['main_categories'].append(category_data)

    def add_sub_category(self, category_data: Dict):
        """하위 카테고리 추가."""
        self.data['sub_categories'].append(category_data)
        self.progress['total_subcategories'] = len(self.data['sub_categories'])

    def add_course(self, course_data: Dict):
        """강의 추가."""
        self.data['courses'].append(course_data)

    def add_course_detail(self, detail_data: Dict):
        """강의 상세 정보 추가."""
        self.data['course_details'].append(detail_data)
        self.progress['completed_details'] += 1

    def save_json(self, filename: str, data: List[Dict]):
        """JSON 파일 저장."""
        json_path = self.output_dir / "json_files" / filename
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def is_url_seen(self, url: str) -> bool:
        """URL 중복 체크."""
        return url in self.seen_urls

    def mark_url_seen(self, url: str):
        """URL을 본 것으로 표시."""
        self.seen_urls.add(url)


class BrowserManager:
    """브라우저 관리 클래스."""

    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.browser = None
        self.context = None

    def setup_browser(self, playwright):
        """브라우저 설정."""
        self.browser = playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=self.config.headers['default']['User-Agent'],
            extra_http_headers=self.config.headers['default']
        )
        return self.browser, self.context

    def create_page(self):
        """새 페이지 생성."""
        page = self.context.new_page()
        page.set_default_timeout(self.config.timeouts['default'] * 1000)
        page.set_default_navigation_timeout(self.config.timeouts['navigation'] * 1000)
        return page

    def close(self):
        """브라우저 종료."""
        if self.browser:
            self.browser.close()


class ImageProcessor:
    """이미지 처리 클래스."""

    def __init__(self, config: CrawlerConfig, logger: Logger):
        self.config = config
        self.logger = logger

    def download_image(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """이미지 다운로드."""
        for attempt in range(retries):
            try:
                response = requests.get(
                    url,
                    timeout=30,
                    headers=self.config.headers['image']
                )
                if response.status_code == 200:
                    return response
                elif attempt < retries - 1:
                    time.sleep(1)
            except Exception:
                if attempt < retries - 1:
                    time.sleep(1)
        return None

    def is_valid_image_url(self, url: str) -> bool:
        """유효한 이미지 URL인지 확인."""
        if not url or len(url) < 10:
            return False

        url_lower = url.lower()
        return (any(ext in url_lower for ext in self.config.valid_image_extensions)
                or 'cdn' in url_lower
                or 'image' in url_lower
                or 'thumbnail' in url_lower)

    def normalize_url(self, url: str, base_url: str) -> str:
        """URL 정규화."""
        if url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            return urljoin(base_url, url)
        elif not url.startswith('http'):
            return urljoin(base_url, url)
        return url.replace('hhttps://', 'https://')


class OptimizedParallelCrawler:
    """최적화된 병렬 크롤러 메인 클래스."""

    def __init__(self, config: Optional[CrawlerConfig] = None):
        self.config = config or CrawlerConfig()
        self.logger = Logger()
        self.data_manager = DataManager(Path(self.config.output_dir))
        self.image_processor = ImageProcessor(self.config, self.logger)

        # 디렉토리 구조 생성
        self._setup_directories()

        self.logger.info("최적화된 병렬 크롤러 초기화 완료")

    def _setup_directories(self):
        """디렉토리 구조 생성."""
        directories = [
            "main_categories", "sub_categories", "courses",
            "sumnail_images", "lect_images", "json_files"
        ]
        
        # 먼저 기본 디렉토리 생성
        Path(self.config.output_dir).mkdir(exist_ok=True)
        
        for directory in directories:
            (Path(self.config.output_dir) / directory).mkdir(parents=True, exist_ok=True)

    def _safe_page_load(self, page, url: str, retries: int = 3) -> bool:
        """안전한 페이지 로딩 - 개선된 버전."""
        for attempt in range(retries):
            try:
                page.goto(url, timeout=self.config.timeouts['navigation'] * 1000)
                
                # networkidle 대신 domcontentloaded 사용 (더 안정적)
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=10000)  # 10초
                    # 추가로 짧은 네트워크 대기 시도
                    page.wait_for_load_state("networkidle", timeout=5000)  # 5초로 단축
                except Exception:
                    # networkidle 실패해도 domcontentloaded는 성공했으므로 계속 진행
                    pass
                
                return True
            except Exception as e:
                if attempt < retries - 1:
                    self.logger.warning(f"페이지 로드 재시도 {attempt + 1}/{retries}: {str(e)[:100]}")
                    page.wait_for_timeout(3000)
                else:
                    self.logger.error(f"페이지 로드 최종 실패: {url}", e)
        return False

    def _scroll_page(self, page):
        """동적 콘텐츠 로드를 위한 스크롤."""
        last_height = page.evaluate("document.body.scrollHeight")

        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            new_height = page.evaluate("document.body.scrollHeight")

            if new_height == last_height:
                break
            last_height = new_height

    def _wait_for_images_to_load(self, page, timeout: Optional[int] = None):
        """이미지 완전 로딩 대기."""
        if timeout is None:
            timeout = self.config.timeouts['image_load'] * 1000

        try:
            # lazy loading 이미지들 강제 로드
            page.evaluate("""
                () => {
                    const lazyImages = document.querySelectorAll('img[data-src]');
                    lazyImages.forEach(img => {
                        if (img.dataset.src) {
                            img.src = img.dataset.src;
                        }
                    });
                }
            """)

            # 이미지 로딩 대기 (80% 이상 로드되면 성공으로 간주)
            try:
                page.wait_for_function("""
                    () => {
                        const images = Array.from(document.querySelectorAll('img'));
                        if (images.length === 0) return true;

                        const loadedImages = images.filter(img => {
                            return img.complete && img.naturalHeight !== 0;
                        });

                        return (loadedImages.length / images.length) >= 0.8;
                    }
                """, timeout=timeout)

                self.logger.info("이미지 로딩 완료 (80% 이상)")
                return True

            except Exception:
                self.logger.warning("이미지 로딩 타임아웃 - 부분적 로딩으로 진행")
                return True

        except Exception as e:
            self.logger.error("이미지 로딩 대기 중 오류", e)
            return False

    def _extract_main_categories(self, page):
        """메인 카테고리 추출."""
        self.logger.info("메인 카테고리 수집 시작")

        # 팝업 제거 및 네비게이션 준비
        try:
            popup_mask = page.locator('.fc-popup-mask')
            if popup_mask.is_visible():
                popup_mask.evaluate('element => element.remove()')
                page.wait_for_timeout(1000)
        except Exception:
            pass

        # 카테고리 버튼 클릭
        try:
            button = page.locator(self.config.selectors['category_nav']).first
            if button.is_visible(timeout=5000):
                button.click(timeout=10000)
                page.wait_for_timeout(3000)
        except Exception as e:
            self.logger.error("카테고리 버튼 클릭 실패", e)

        # 메인 카테고리 링크 추출
        links = page.locator(self.config.selectors['main_category']).all()
        seen = set()

        for i, link in enumerate(links, 1):
            try:
                name = link.text_content().strip()
                url = link.get_attribute('href')

                if (name and url and name in self.config.main_categories
                        and name not in seen):
                    seen.add(name)

                    if url.startswith('/'):
                        url = urljoin(self.config.base_url, url)

                    category_data = {
                        "메인카테고리": name,
                        "메인카테고리링크": url,
                        "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    self.data_manager.add_main_category(category_data)
                    self.logger.info(f"메인 카테고리 발견: {name}")
            except Exception as e:
                self.logger.error(f"메인 카테고리 처리 중 오류 (링크 {i})", e)
                continue

        self.logger.info(f"메인 카테고리 수집 완료: {len(self.data_manager.data['main_categories'])}개")

    def _extract_sub_categories(self, page):
        """하위 카테고리 추출."""
        self.logger.info("하위 카테고리 수집 시작")

        # 여러 선택자로 하위 카테고리 찾기
        sub_category_selectors = [
            'li.GNBDesktopCategoryItem_subCategory__twmcG a[href*="category_online"]',
            'li[class*="subCategory"] a[href*="category_online"]',
            'li[class*="sub-category"] a[href*="category_online"]',
            'a[href*="category_online"]:not([class*="main"])',
            'a[href*="category_online"]'
        ]

        links = []
        for selector in sub_category_selectors:
            try:
                found_links = page.locator(selector).all()
                if found_links:
                    links = found_links
                    self.logger.info(f"하위 카테고리 선택자 성공: {selector} ({len(links)}개)")
                    break
            except Exception as e:
                self.logger.error(f"선택자 실패: {selector}", e)
                continue

        if not links:
            self.logger.error("모든 하위 카테고리 선택자 실패")
            return

        for i, link in enumerate(links, 1):
            try:
                # 타임아웃을 더 짧게 설정하여 안정성 향상
                name = link.text_content(timeout=5000).strip()
                url = link.get_attribute('href', timeout=5000)

                if not name or not url or self.data_manager.is_url_seen(url):
                    continue

                self.data_manager.mark_url_seen(url)

                # 상위 카테고리 찾기
                parent = self._find_parent_category(link)

                if parent:
                    if url.startswith('/'):
                        url = urljoin(self.config.base_url, url)

                    category_data = {
                        "메인카테고리": parent,
                        "하위카테고리": name,
                        "하위카테고리링크": url,
                        "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    self.data_manager.add_sub_category(category_data)
                    self.logger.info(f"하위 카테고리 발견: {parent} > {name}")
            except Exception as e:
                self.logger.warning(f"하위 카테고리 처리 중 오류 (링크 {i}): {str(e)[:100]}")
                continue

        self.logger.info(f"하위 카테고리 수집 완료: {len(self.data_manager.data['sub_categories'])}개")

    def _find_parent_category(self, sub_link) -> Optional[str]:
        """상위 카테고리 찾기."""
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

    def _extract_courses_from_subcategory(self, subcategory: Dict, max_courses: Optional[int] = None) -> List[Dict]:
        """하위 카테고리에서 강의 정보 추출 (병렬 처리용)."""
        with sync_playwright() as p:
            browser_manager = BrowserManager(self.config)
            browser, context = browser_manager.setup_browser(p)

            try:
                self.logger.info(f"강의 수집 시작: {subcategory['메인카테고리']} > "
                                 f"{subcategory['하위카테고리']}")

                page = browser_manager.create_page()

                if not self._safe_page_load(page, subcategory['하위카테고리링크']):
                    self.logger.error(f"페이지 로드 실패: {subcategory['하위카테고리링크']}")
                    return []

                self._scroll_page(page)
                page.wait_for_timeout(3000)

                # 먼저 JSON에서 강의 제목들 추출
                self._extract_course_titles_from_json(page)

                # 강의 카드 컨테이너 찾기 - 단순화된 방식
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
                    try:
                        found_cards = page.locator(selector).all()
                        if found_cards:
                            cards = found_cards
                            self.logger.info(f"강의 카드 발견: {len(cards)}개 (선택자: {selector})")
                            break
                    except Exception:
                        continue

                if not cards:
                    # 대체 선택자들 시도
                    for selector in self.config.selectors['course_link']:
                        try:
                            cards = page.locator(selector).all()
                            if cards:
                                self.logger.info(f"대체 선택자로 강의 카드 발견: {len(cards)}개 (선택자: {selector})")
                                break
                        except Exception as e:
                            self.logger.warning(f"선택자 실패: {selector}", e)
                            continue
                    
                    if not cards:
                        self.logger.error(f"강의 카드를 찾을 수 없음: {subcategory['하위카테고리링크']}")
                        return []

                collected_courses = []
                collected = 0

                for i, card in enumerate(cards, 1):
                    if max_courses is not None and collected >= max_courses:
                        break

                    try:
                        url = self._extract_course_url(card)
                        if not url:
                            continue

                        # URL 정규화
                        if url.startswith('/'):
                            url = urljoin(self.config.base_url, url)

                        # 유효한 강의 URL인지 확인
                        if not self._is_valid_course_url(url) or self.data_manager.is_url_seen(url):
                            continue

                        self.data_manager.mark_url_seen(url)
                        title = self._extract_title(card, url)

                        course_data = {
                            "메인카테고리": subcategory['메인카테고리'],
                            "하위카테고리": subcategory['하위카테고리'],
                            "강의제목": title,
                            "강의링크": url,
                            "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }

                        collected_courses.append(course_data)
                        self.logger.info(f"강의 발견: {title} -> {url}")
                        self._collect_thumbnails(card, url)
                        collected += 1

                    except Exception as e:
                        self.logger.error(f"강의 처리 중 오류 (카드 {i})", e)
                        continue

                self.logger.info(f"강의 수집 완료: {collected}개 (총 {len(cards)}개 카드 중)")
                return collected_courses

            finally:
                browser_manager.close()

    def _extract_course_url(self, card) -> Optional[str]:
        """강의 URL 추출 - 단순화된 버전."""
        # 1. 카드 자체가 링크인 경우
        try:
            url = card.get_attribute('href')
            if url:
                return url
        except Exception:
            pass

        # 2. 카드 내부의 링크 찾기
        try:
            link = card.locator(self.config.selectors['course_link']).first
            if link.count() > 0:
                return link.get_attribute('href')
        except Exception:
            pass

        # 3. 모든 링크 요소를 찾아서 검사
        try:
            all_links = card.locator('a').all()
            for link in all_links:
                try:
                    url = link.get_attribute('href')
                    if url and not url.startswith('#') and not url.startswith('javascript:'):
                        return url
                except Exception:
                    continue
        except Exception:
            pass

        return None
    
    def _is_valid_course_url(self, url: str) -> bool:
        """유효한 강의 URL인지 확인."""
        if not url or len(url) < 5:
            return False
            
        # 제외할 URL 패턴 - 이벤트만 제외
        exclude_patterns = [
            '/event_online_',  # 이벤트 페이지만 제외
            'javascript:',
            'mailto:',
            'tel:',
            '#',
            '/category_online'  # 카테고리 페이지도 제외
        ]
        
        # 제외 패턴 체크
        for pattern in exclude_patterns:
            if pattern in url:
                return False
        
        # FastCampus 도메인이면서 온라인 강의 관련 URL이면 모두 허용
        if url.startswith('http'):
            # 절대 URL인 경우 FastCampus 도메인 체크
            if 'fastcampus.co.kr' in url:
                # 온라인 강의 관련 패턴이 있으면 허용
                online_patterns = [
                    '/data_online_',
                    '/dgn_online_', 
                    '/biz_online_',
                    '/course_',
                    '/online_',
                    'sharex.fastcampus.co.kr'  # 추가 도메인
                ]
                return any(pattern in url for pattern in online_patterns)
        elif url.startswith('/'):
            # 상대 URL인 경우 온라인 강의 패턴이 있으면 허용
            online_patterns = [
                '/data_online_',
                '/dgn_online_', 
                '/biz_online_',
                '/course_',
                'online_'  # /를 제거하여 더 포괄적으로
            ]
            return any(pattern in url for pattern in online_patterns)
        
        return False

    def _extract_title(self, card, course_url: Optional[str] = None) -> str:
        """강의 제목 추출 - 개선된 버전."""
        # 먼저 저장된 매핑에서 찾기
        if course_url and course_url in self.data_manager.course_titles:
            return self.data_manager.course_titles[course_url]

        # DOM에서 추출 - 더 정확한 선택자들
        selectors = [
            '.CourseCard_courseCardTitle__1HQgO',  # 정확한 클래스명
            '[data-e2e="course-title"]',  # data-e2e 속성
            'h3', 'h4', '.course-title', '.title', 
            '.course-name', '[class*="title"]', '[class*="Title"]'
        ]

        for selector in selectors:
            element = card.locator(selector).first
            if element.count() > 0:
                title = element.text_content().strip()
                if (title and len(title) > 3 and not title.isdigit() 
                    and not title.endswith('+') and not title.startswith('사전')):
                    return title

        # 마지막 fallback - 더 엄격한 필터링
        full_text = card.text_content().strip()
        lines = [line.strip() for line in full_text.split('\n') 
                if line.strip()]

        # 제외할 패턴들
        exclude_patterns = [
            '사전 설문', '고민을 들어보았습니다', '강사님 한마디',
            '수강생', '설문', '고민', '한마디', '질문', '답변'
        ]

        for line in lines:
            if (len(line) > 3 and not line.isdigit() 
                and not line.startswith(('+', '⚠️', '사전', '고민', '강사님')) 
                and not line.endswith('+') 
                and not any(pattern in line for pattern in exclude_patterns)):
                return line
                
        return "제목 없음"

    def _collect_thumbnails(self, card, course_url: str):
        """썸네일 이미지 수집."""
        course_name = course_url.split('/')[-1]
        thumbnail_dir = Path(self.config.output_dir) / "sumnail_images" / course_name
        thumbnail_dir.mkdir(parents=True, exist_ok=True)

        # 썸네일 선택자
        img_selectors = [
            'img[alt="강의 대표이미지"]',
            'img[class*="CourseCard"][data-nimg="fill"]',
            'img[class*="CourseCard"]',
            'img[data-nimg="fill"]'
        ]

        saved_count = 0
        saved_urls = set()

        for selector in img_selectors:
            if self.config.max_thumbnails and saved_count >= self.config.max_thumbnails:
                break

            try:
                images = card.locator(selector).all()
                for img in images:
                    if self.config.max_thumbnails and saved_count >= self.config.max_thumbnails:
                        break

                    url = (img.get_attribute('src') or
                           img.get_attribute('data-src'))

                    if url:
                        url = self.image_processor.normalize_url(url, self.config.base_url)

                        if url in saved_urls:
                            continue

                        if not self.image_processor.is_valid_image_url(url):
                            continue

                        response = self.image_processor.download_image(url)

                        if (response and
                                len(response.content) > self.config.min_image_size):
                            ext = (os.path.splitext(urlparse(url).path)[1]
                                   or '.webp')
                            filename = f"thumbnail_{saved_count + 1}{ext}"

                            with open(thumbnail_dir / filename, 'wb') as f:
                                f.write(response.content)

                            saved_urls.add(url)
                            saved_count += 1
                            self.logger.info(f"썸네일 이미지 저장: {filename} "
                                             f"({len(response.content)} bytes)")
            except Exception as e:
                self.logger.error("썸네일 이미지 처리 중 오류", e)
                continue

    def _extract_course_titles_from_json(self, page):
        """페이지의 JSON 데이터에서 강의 제목 추출."""
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
                        self.data_manager.course_titles[course['url']] = course['title']
                    
                    if courses:
                        self.logger.info(f"JSON에서 {len(courses)}개 강의 제목 "
                                         f"추출 완료")
                        return True
                        
                except Exception as e:
                    self.logger.error(f"JSON 파싱 중 오류", e)
                    continue
                    
        except Exception as e:
            self.logger.error(f"강의 제목 추출 중 오류", e)
        
        return False

    def _extract_course_detail(self, course_url: str) -> Optional[Dict]:
        """강의 상세 정보 추출 (병렬 처리용)."""
        with sync_playwright() as p:
            browser_manager = BrowserManager(self.config)
            browser, context = browser_manager.setup_browser(p)

            try:
                course_name = course_url.split('/')[-1]
                self.logger.info(f"강의 상세 정보 수집: {course_name}")

                page = browser_manager.create_page()

                if not self._safe_page_load(page, course_url):
                    self.logger.error(f"강의 페이지 로드 실패: {course_url}")
                    return None

                self._scroll_page(page)
                page.wait_for_timeout(3000)

                # 이미지 최적화 적용
                self._wait_for_images_to_load(page)

                # HTML 저장
                html_content = page.content()
                safe_name = course_url.split('/')[-1]

                with open(Path(self.config.output_dir) / f"courses/{safe_name}.html",
                          'w', encoding='utf-8') as f:
                    f.write(html_content)

                self.logger.info(f"강의 HTML 저장 완료: {safe_name}.html")

                soup = BeautifulSoup(html_content, 'html.parser')

                # 더 정확한 강의명 추출
                title_selectors = [
                    'h1[data-e2e="course-title"]',  # 가장 정확한 선택자
                    'h1.course-title',
                    'h1.title',
                    '[data-e2e="course-title"]',
                    '.course-title',
                    '.title'
                ]
                title = "제목 없음"

                # 제외할 패턴들
                exclude_patterns = [
                    'root layout', '강사님 한마디', '강사님 한 마디',
                    '사전 설문', '고민을 들어보았습니다', '정가',
                    'SHARE XONLINE DESIGN ACADEMY', 'root', 'layout',
                    '강사님', '한마디', '사전', '고민', '설문'
                ]

                for selector in title_selectors:
                    element = soup.select_one(selector)
                    if element and element.get_text().strip():
                        candidate_title = element.get_text().strip()

                        # 제외 패턴 체크
                        if (len(candidate_title) > 5 and 
                            not any(pattern in candidate_title for pattern in exclude_patterns) and
                            not candidate_title.startswith(('사전', '고민', '강사님', '정가', 'SHARE'))):
                            title = candidate_title
                            break

                # 여전히 제목을 찾지 못한 경우, 페이지 제목(title 태그) 확인
                if title == "제목 없음":
                    title_tag = soup.find('title')
                    if title_tag:
                        page_title = title_tag.get_text().strip()
                        # "| 패스트캠퍼스" 같은 접미사 제거
                        if '| 패스트캠퍼스' in page_title:
                            page_title = page_title.split('| 패스트캠퍼스')[0].strip()

                        if (len(page_title) > 5 and 
                            not any(pattern in page_title for pattern in exclude_patterns) and
                            not page_title.startswith(('사전', '고민', '강사님', '정가', 'SHARE'))):
                            title = page_title

                # 여전히 제목을 찾지 못한 경우, 페이지 전체에서 가장 긴 텍스트 찾기
                if title == "제목 없음":
                    all_text_elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'div', 'span'], 
                                                    class_=lambda x: x and any(keyword in x.lower() 
                                                    for keyword in ['title', 'name', 'course', 'lecture']))
                    
                    for element in all_text_elements:
                        candidate_title = element.get_text().strip()
                        if (len(candidate_title) > 10 and len(candidate_title) < 200 and
                            not any(pattern in candidate_title for pattern in exclude_patterns) and
                            not candidate_title.startswith(('사전', '고민', '강사님', '정가', 'SHARE'))):
                            title = candidate_title
                            break

                # 강의 상세 페이지에서 이미지 수집
                self._collect_lecture_images(page, course_name)

                self.logger.info(f"강의 상세 정보 수집 완료: {title}")

                return {
                    "강의명": title,
                    "강의링크": course_url,
                    "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "추출된_텍스트": self._extract_page_content(soup)
                }

            finally:
                browser_manager.close()

    def _extract_page_content(self, soup) -> str:
        """페이지 텍스트 콘텐츠 추출."""
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()

        main_content = soup.find('main') or soup.find('body') or soup
        text = main_content.get_text()

        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return '\n'.join(lines) if lines else "텍스트 없음"

    def _collect_lecture_images(self, page, course_name: str):
        """강의 상세 페이지에서 이미지 수집."""
        try:
            lecture_dir = Path(self.config.output_dir) / "lect_images" / course_name
            lecture_dir.mkdir(parents=True, exist_ok=True)

            # 강의 상세 페이지의 모든 이미지 수집
            img_selectors = [
                'img[alt*="강의"]',
                'img[alt*="과정"]',
                'img[alt*="커리큘럼"]',
                'img[alt*="프로젝트"]',
                'img[class*="lecture"]',
                'img[class*="course"]',
                'img[class*="curriculum"]',
                'img[class*="project"]',
                'img[data-nimg="fill"]',
                'img[src*="course"]',
                'img[src*="lecture"]',
                'img[src*="curriculum"]'
            ]

            saved_count = 0
            saved_urls = set()

            for selector in img_selectors:
                try:
                    images = page.locator(selector).all()
                    for img in images:
                        url = (img.get_attribute('src') or
                               img.get_attribute('data-src'))

                        if url:
                            url = self.image_processor.normalize_url(url, self.config.base_url)

                            if url in saved_urls:
                                continue

                            if not self.image_processor.is_valid_image_url(url):
                                continue

                            response = self.image_processor.download_image(url)

                            if (response and
                                    len(response.content) > self.config.min_image_size):
                                ext = (os.path.splitext(urlparse(url).path)[1]
                                       or '.webp')
                                filename = f"lecture_{saved_count + 1}{ext}"

                                with open(lecture_dir / filename, 'wb') as f:
                                    f.write(response.content)

                                saved_urls.add(url)
                                saved_count += 1
                                self.logger.info(f"강의 이미지 저장: {filename} "
                                                 f"({len(response.content)} bytes)")
                except Exception as e:
                    self.logger.error("강의 이미지 처리 중 오류", e)
                    continue

            if saved_count > 0:
                self.logger.info(f"강의 이미지 수집 완료: {saved_count}개 "
                                 f"({course_name}) - 제한 없음")
            else:
                self.logger.info(f"강의 이미지 없음: {course_name}")

        except Exception as e:
            self.logger.error(f"강의 이미지 수집 중 오류: {course_name}", e)

    def run(self):
        """크롤링 실행 (병렬 처리)."""
        start_time = time.time()
        self.logger.info("최적화된 병렬 크롤링 시작!")

        with sync_playwright() as p:
            browser_manager = BrowserManager(self.config)
            browser, context = browser_manager.setup_browser(p)

            try:
                self.logger.info("브라우저 설정 완료")

                page = browser_manager.create_page()

                if not self._safe_page_load(page, self.config.base_url):
                    self.logger.error("메인 페이지 로드 실패")
                    return

                self.logger.info("메인 페이지 로드 완료")

                # 메인 카테고리 수집
                self._extract_main_categories(page)

                # 하위 카테고리 수집
                self._extract_sub_categories(page)

                # 강의 목록 수집 (병렬 처리)
                self.logger.info("강의 목록 수집 시작")

                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    futures = []
                    for subcategory in self.data_manager.data['sub_categories']:
                        future = executor.submit(
                            self._extract_courses_from_subcategory,
                            subcategory,
                            max_courses=None
                        )
                        futures.append(future)

                    # 결과 수집
                    for i, future in enumerate(futures):
                        try:
                            courses = future.result(timeout=180)
                            if courses:
                                self.data_manager.data['courses'].extend(courses)
                                self.data_manager.progress['completed_subcategories'] += 1
                                self.logger.info(f"하위 카테고리 완료: {i + 1}/{len(futures)}")
                        except Exception as e:
                            self.logger.error(f"하위 카테고리 처리 실패: {i + 1}", e)

                self.logger.info(f"강의 목록 수집 완료: {len(self.data_manager.data['courses'])}개")

                # 강의 상세 정보 수집 (병렬 처리)
                self.logger.info("강의 상세 정보 수집 시작")
                self.data_manager.progress['total_details'] = len(self.data_manager.data['courses'])

                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    futures = []
                    for course in self.data_manager.data['courses']:
                        future = executor.submit(
                            self._extract_course_detail,
                            course['강의링크']
                        )
                        futures.append(future)

                    # 결과 수집
                    for i, future in enumerate(futures):
                        try:
                            detail = future.result(timeout=120)
                            if detail:
                                self.data_manager.add_course_detail(detail)
                                if self.data_manager.progress['completed_details'] % 10 == 0:
                                    self.logger.info(f"강의 상세 완료: "
                                                     f"{self.data_manager.progress['completed_details']}/"
                                                     f"{self.data_manager.progress['total_details']}")
                        except Exception as e:
                            self.logger.error(f"강의 상세 처리 실패: {i + 1}", e)

                self.logger.info(f"강의 상세 정보 수집 완료: "
                                 f"{len(self.data_manager.data['course_details'])}개")

                # 데이터 저장
                self.logger.info("데이터 저장 시작")
                self.data_manager.save_json("main_categories.json",
                                            self.data_manager.data['main_categories'])
                self.data_manager.save_json("sub_categories.json",
                                            self.data_manager.data['sub_categories'])
                self.data_manager.save_json("courses_list.json",
                                            self.data_manager.data['courses'])
                self.data_manager.save_json("course_details.json",
                                            self.data_manager.data['course_details'])
                self.logger.info("데이터 저장 완료")

                # 완료
                elapsed_time = time.time() - start_time
                self.logger.info(f"병렬 크롤링 완료! 총 소요시간: {elapsed_time:.2f}초")
                self.logger.info("수집 결과:")
                self.logger.info(f"   - 메인 카테고리: {len(self.data_manager.data['main_categories'])}개")
                self.logger.info(f"   - 하위 카테고리: {len(self.data_manager.data['sub_categories'])}개")
                self.logger.info(f"   - 강의 목록: {len(self.data_manager.data['courses'])}개")
                self.logger.info(f"   - 강의 상세: {len(self.data_manager.data['course_details'])}개")

            except Exception as e:
                self.logger.error("크롤링 중 치명적 오류 발생", e)
            finally:
                browser_manager.close()
                self.logger.info("브라우저 종료")


def main():
    """Application entry point."""
    # 설정 커스터마이징 가능
    config = CrawlerConfig()
    config.max_workers = 12  # M3 칩 최적화
    config.max_concurrent_courses = 20

    crawler = OptimizedParallelCrawler(config)
    crawler.run()


if __name__ == "__main__":
    main()
