import asyncio
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class IncrementalCrawler:
    """FastCampus 증분 크롤러 - 변경된 데이터만 수집하는 스마트 크롤러."""

    # 클래스 상수
    MAIN_CATEGORIES = {
        'AI TECH', 'AI CREATIVE', 'AI/업무생산성', '개발/데이터', 
        '디자인', '영상/3D', '금융/투자', '드로잉/일러스트', 
        '비즈니스/기획'
    }

    CRAWLING_STEPS = [
        "초기화",
        "이전 데이터 로드",
        "메인 카테고리 확인",
        "하위 카테고리 확인", 
        "강의 목록 확인",
        "HTML 저장",
        "강의 상세 정보 수집",
        "이미지 다운로드",
        "데이터 저장",
        "완료"
    ]

    # M3 칩 최적화 설정
    MAX_WORKERS = 4  # 스레드 풀 워커 수
    MAX_CONCURRENT_COURSES = 8  # 동시 강의 수집 수

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
                 output_dir="./incremental_crawl"):
        """증분 크롤러 초기화."""
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # 현재 크롤링 데이터
        self.current_data = {
            'main_categories': [],
            'sub_categories': [],
            'courses': [],
            'course_details': []
        }

        # 이전 크롤링 데이터
        self.previous_data = {
            'main_categories': [],
            'sub_categories': [],
            'courses': [],
            'course_details': []
        }

        # 증분 크롤링 통계
        self.incremental_stats = {
            'main_categories': {'new': 0, 'updated': 0, 'removed': 0, 'unchanged': 0},
            'sub_categories': {'new': 0, 'updated': 0, 'removed': 0, 'unchanged': 0},
            'courses': {'new': 0, 'updated': 0, 'removed': 0, 'unchanged': 0},
            'course_details': {'new': 0, 'updated': 0, 'removed': 0, 'unchanged': 0}
        }

        self.seen_urls = set()
        self.current_step = 0
        self.start_time = None
        self.course_titles = {}
        self.is_first_run = False

        # 진행률 추적
        self.progress = {
            'total_subcategories': 0,
            'completed_subcategories': 0,
            'total_courses': 0,
            'completed_courses': 0,
            'total_details': 0,
            'completed_details': 0
        }

        self._setup_directories()
        self._log_progress("증분 크롤러 초기화 완료")

    def _setup_directories(self):
        """디렉토리 구조 생성."""
        directories = [
            "main_categories", "sub_categories", "courses",
            "sumnail_images", "lect_images", "json_files", "incremental_logs"
        ]

        for directory in directories:
            (self.output_dir / directory).mkdir(exist_ok=True)

    def _log_progress(self, message, step=None):
        """진행 상황 로깅."""
        if step is not None:
            self.current_step = step

        timestamp = datetime.now().strftime("%H:%M:%S")
        step_name = (self.CRAWLING_STEPS[self.current_step] 
                    if self.current_step < len(self.CRAWLING_STEPS) 
                    else "알 수 없음")

        print(f"[{timestamp}] [{step_name}] {message}")
        sys.stdout.flush()

    def _log_step_start(self, step_name):
        """단계 시작 로깅."""
        self.current_step = self.CRAWLING_STEPS.index(step_name)
        self._log_progress(f"{step_name} 시작")

    def _log_step_complete(self, step_name, count=None):
        """단계 완료 로깅."""
        if count is not None:
            self._log_progress(f"{step_name} 완료 - {count}개 항목 수집")
        else:
            self._log_progress(f"{step_name} 완료")

    def _log_error(self, message, error=None):
        """에러 로깅."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if error:
            print(f"[{timestamp}] [ERROR] {message}: {str(error)}")
        else:
            print(f"[{timestamp}] [ERROR] {message}")
        sys.stdout.flush()

    def _log_incremental_stats(self):
        """증분 크롤링 통계 로깅."""
        self._log_progress("=== 증분 크롤링 통계 ===")
        
        for data_type, stats in self.incremental_stats.items():
            total = sum(stats.values())
            if total > 0:
                self._log_progress(f"{data_type}: 신규 {stats['new']}개, "
                                 f"업데이트 {stats['updated']}개, "
                                 f"삭제 {stats['removed']}개, "
                                 f"변경없음 {stats['unchanged']}개")

    def _generate_content_hash(self, content):
        """콘텐츠의 해시값 생성."""
        if isinstance(content, dict):
            content = json.dumps(content, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(str(content).encode()).hexdigest()

    def _load_previous_data(self):
        """이전 크롤링 데이터 로드."""
        self._log_step_start("이전 데이터 로드")
        
        data_files = {
            'main_categories': 'main_categories.json',
            'sub_categories': 'sub_categories.json',
            'courses': 'courses_list.json',
            'course_details': 'course_details.json'
        }

        for data_type, filename in data_files.items():
            file_path = self.output_dir / "json_files" / filename
            
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        self.previous_data[data_type] = json.load(f)
                    self._log_progress(f"{data_type} 이전 데이터 로드 완료: "
                                     f"{len(self.previous_data[data_type])}개")
                except Exception as e:
                    self._log_error(f"{data_type} 이전 데이터 로드 실패", e)
                    self.previous_data[data_type] = []
            else:
                self._log_progress(f"{data_type} 이전 데이터 없음 (첫 실행)")
                self.previous_data[data_type] = []
                self.is_first_run = True

        self._log_step_complete("이전 데이터 로드")

    def _compare_data_changes(self, data_type, current_data, previous_data):
        """데이터 변경사항 비교."""
        current_urls = set()
        previous_urls = set()
        
        # URL 기반 비교를 위한 데이터 준비
        if data_type in ['main_categories', 'sub_categories']:
            current_urls = set(item.get('메인카테고리링크', item.get('하위카테고리링크', '')) 
                             for item in current_data)
            previous_urls = set(item.get('메인카테고리링크', item.get('하위카테고리링크', '')) 
                              for item in previous_data)
        elif data_type == 'courses':
            current_urls = set(item.get('강의링크', '') for item in current_data)
            previous_urls = set(item.get('강의링크', '') for item in previous_data)
        elif data_type == 'course_details':
            current_urls = set(item.get('강의링크', '') for item in current_data)
            previous_urls = set(item.get('강의링크', '') for item in previous_data)

        # 변경사항 분류
        new_items = current_urls - previous_urls
        removed_items = previous_urls - current_urls
        unchanged_items = current_urls & previous_urls

        # 업데이트된 항목 찾기 (URL은 같지만 내용이 다른 경우)
        updated_items = set()
        if data_type in ['courses', 'course_details']:
            for item in current_data:
                url = item.get('강의링크', '')
                if url in unchanged_items:
                    # 이전 데이터에서 같은 URL의 항목 찾기
                    previous_item = next((p for p in previous_data 
                                        if p.get('강의링크', '') == url), None)
                    if previous_item:
                        current_hash = self._generate_content_hash(item)
                        previous_hash = self._generate_content_hash(previous_item)
                        if current_hash != previous_hash:
                            updated_items.add(url)

        # 통계 업데이트
        self.incremental_stats[data_type] = {
            'new': len(new_items),
            'updated': len(updated_items),
            'removed': len(removed_items),
            'unchanged': len(unchanged_items) - len(updated_items)
        }

        return {
            'new': new_items,
            'updated': updated_items,
            'removed': removed_items,
            'unchanged': unchanged_items - updated_items
        }

    def _setup_browser(self, playwright):
        """브라우저 설정."""
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
        """안전한 페이지 로딩."""
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
        """동적 콘텐츠 로드를 위한 스크롤."""
        last_height = page.evaluate("document.body.scrollHeight")

        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            new_height = page.evaluate("document.body.scrollHeight")

            if new_height == last_height:
                break
            last_height = new_height

    def _wait_for_images_to_load(self, page, timeout=30000):
        """이미지 완전 로딩 대기."""
        try:
            # 모든 이미지가 로드될 때까지 대기
            page.wait_for_function("""
                () => {
                    const images = Array.from(document.querySelectorAll('img'));
                    return images.every(img => {
                        return img.complete && img.naturalHeight !== 0;
                    });
                }
            """, timeout=timeout)

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

            # srcset 속성 정규화
            page.evaluate("""
                () => {
                    const sources = document.querySelectorAll('source[srcset]');
                    sources.forEach(source => {
                        if (source.srcset && source.srcset.trim()) {
                            const baseUrl = window.location.origin;
                            source.srcset = source.srcset.split(',').map(src => {
                                src = src.trim().split(' ')[0];
                                if (src.startsWith('/')) {
                                    return baseUrl + src;
                                } else if (src.startsWith('//')) {
                                    return 'https:' + src;
                                }
                                return src;
                            }).join(', ');
                        }
                    });
                }
            """)

            self._log_progress("이미지 로딩 완료")
            return True

        except Exception as e:
            self._log_error(f"이미지 로딩 대기 중 오류", e)
            return False

    def _prepare_navigation(self, page):
        """네비게이션 준비."""
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
        """메인 카테고리 추출."""
        self._log_step_start("메인 카테고리 확인")
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

                    self.current_data['main_categories'].append({
                        "메인카테고리": name,
                        "메인카테고리링크": url,
                        "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
            except Exception as e:
                self._log_error(f"메인 카테고리 처리 중 오류 (링크 {i})", e)
                continue

        # 변경사항 비교
        changes = self._compare_data_changes('main_categories', 
                                           self.current_data['main_categories'],
                                           self.previous_data['main_categories'])

        if changes['new'] or changes['updated'] or changes['removed']:
            self._log_progress(f"메인 카테고리 변경사항 발견: "
                             f"신규 {len(changes['new'])}개, "
                             f"업데이트 {len(changes['updated'])}개, "
                             f"삭제 {len(changes['removed'])}개")
        else:
            self._log_progress("메인 카테고리 변경사항 없음")

        self._log_step_complete("메인 카테고리 확인", 
                               len(self.current_data['main_categories']))

    def _find_parent_category(self, sub_link):
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

    def _extract_sub_categories(self, page):
        """하위 카테고리 추출."""
        self._log_step_start("하위 카테고리 확인")
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

                    self.current_data['sub_categories'].append({
                        "메인카테고리": parent,
                        "하위카테고리": name,
                        "하위카테고리링크": url,
                        "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
            except Exception as e:
                self._log_error(f"하위 카테고리 처리 중 오류 (링크 {i})", e)
                continue

        # 변경사항 비교
        changes = self._compare_data_changes('sub_categories',
                                           self.current_data['sub_categories'],
                                           self.previous_data['sub_categories'])

        if changes['new']:
            self._log_progress(f"신규 하위 카테고리 {len(changes['new'])}개 발견")
            for new_url in changes['new']:
                new_category = next((cat for cat in self.current_data['sub_categories']
                                   if cat.get('하위카테고리링크') == new_url), None)
                if new_category:
                    self._log_progress(f"  - {new_category['메인카테고리']} > "
                                     f"{new_category['하위카테고리']}")

        self.progress['total_subcategories'] = len(self.current_data['sub_categories'])
        self._log_step_complete("하위 카테고리 확인", 
                               len(self.current_data['sub_categories']))

    def _collect_courses_incremental(self):
        """증분 강의 수집."""
        self._log_step_start("강의 목록 확인")
        
        # 신규 하위 카테고리에서 강의 수집
        new_subcategories = []
        for category in self.current_data['sub_categories']:
            url = category['하위카테고리링크']
            if not any(p.get('하위카테고리링크') == url 
                      for p in self.previous_data['sub_categories']):
                new_subcategories.append(category)

        if new_subcategories:
            self._log_progress(f"신규 하위 카테고리 {len(new_subcategories)}개에서 "
                             f"강의 수집 시작")
            
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures = []
                for subcategory in new_subcategories:
                    future = executor.submit(
                        self._extract_courses_from_subcategory, 
                        subcategory, 
                        max_courses=20
                    )
                    futures.append(future)

                for i, future in enumerate(futures):
                    try:
                        courses = future.result(timeout=300)
                        if courses:
                            self.current_data['courses'].extend(courses)
                            self._log_progress(f"신규 카테고리 강의 수집 완료: "
                                             f"{len(courses)}개")
                    except Exception as e:
                        self._log_error(f"신규 카테고리 강의 수집 실패: {i+1}", e)

        # 기존 카테고리에서 강의 목록만 빠르게 확인
        self._check_existing_courses()

        # 변경사항 비교
        changes = self._compare_data_changes('courses',
                                           self.current_data['courses'],
                                           self.previous_data['courses'])

        if changes['new']:
            self._log_progress(f"신규 강의 {len(changes['new'])}개 발견")
        if changes['updated']:
            self._log_progress(f"업데이트된 강의 {len(changes['updated'])}개 발견")

        self._log_step_complete("강의 목록 확인", len(self.current_data['courses']))

    def _check_existing_courses(self):
        """기존 카테고리에서 강의 목록 빠른 확인."""
        # 기존 하위 카테고리에서 강의 목록만 빠르게 확인
        existing_categories = []
        for category in self.current_data['sub_categories']:
            url = category['하위카테고리링크']
            if any(p.get('하위카테고리링크') == url 
                  for p in self.previous_data['sub_categories']):
                existing_categories.append(category)

        if existing_categories:
            self._log_progress(f"기존 카테고리 {len(existing_categories)}개에서 "
                             f"강의 목록 빠른 확인")
            
            # 샘플링으로 일부 카테고리만 확인 (성능 최적화)
            sample_size = min(5, len(existing_categories))
            sample_categories = existing_categories[:sample_size]
            
            for category in sample_categories:
                try:
                    with sync_playwright() as p:
                        browser, page = self._setup_browser(p)
                        
                        if self._safe_page_load(page, category['하위카테고리링크']):
                            self._scroll_page(page)
                            page.wait_for_timeout(2000)
                            
                            # 강의 카드 개수만 빠르게 확인
                            cards = page.locator(self.SELECTORS['course_card']).all()
                            self._log_progress(f"카테고리 '{category['하위카테고리']}': "
                                             f"{len(cards)}개 강의 확인")
                        
                        browser.close()
                except Exception as e:
                    self._log_error(f"카테고리 확인 실패: {category['하위카테고리']}", e)

    def _extract_courses_from_subcategory(self, subcategory, max_courses=20):
        """하위 카테고리에서 강의 정보 추출."""
        with sync_playwright() as p:
            browser, page = self._setup_browser(p)

            try:
                if not self._safe_page_load(page, subcategory['하위카테고리링크']):
                    return []

                self._scroll_page(page)
                page.wait_for_timeout(3000)

                # 강의 카드 찾기
                card_selectors = [
                    '[data-e2e="course-card"]',
                    '.course-card', 
                    '.course-item',
                    'div[class*="CourseCard"]'
                ]

                cards = []
                for selector in card_selectors:
                    found_cards = page.locator(selector).all()
                    if found_cards:
                        cards = found_cards
                        break

                if not cards:
                    cards = page.locator(self.SELECTORS['course_link']).all()

                collected_courses = []
                for i, card in enumerate(cards[:max_courses], 1):
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

                        course_data = {
                            "메인카테고리": subcategory['메인카테고리'],
                            "하위카테고리": subcategory['하위카테고리'],
                            "강의제목": title,
                            "강의링크": url,
                            "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }

                        collected_courses.append(course_data)
                    except Exception as e:
                        continue

                return collected_courses

            finally:
                browser.close()

    def _extract_course_url(self, card):
        """강의 URL 추출."""
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

    def _extract_title(self, card, course_url=None):
        """강의 제목 추출."""
        selectors = [
            '.CourseCard_courseCardTitle__1HQgO',
            '[data-e2e="display-card"]',
            'h3', 'h4', '.course-title', '.title', 
            '[data-e2e="course-title"]', '.course-name'
        ]

        for selector in selectors:
            element = card.locator(selector).first
            if element.count() > 0:
                title = element.text_content().strip()
                if (title and len(title) > 3 and not title.isdigit() 
                    and not title.endswith('+')):
                    return title

        return "제목 없음"

    def _collect_course_details_incremental(self):
        """증분 강의 상세 정보 수집."""
        self._log_step_start("강의 상세 정보 수집")
        
        # 신규/업데이트된 강의만 상세 수집
        target_courses = []
        changes = self._compare_data_changes('courses',
                                           self.current_data['courses'],
                                           self.previous_data['courses'])
        
        # 신규 강의
        for course in self.current_data['courses']:
            if course['강의링크'] in changes['new']:
                target_courses.append(course)
        
        # 업데이트된 강의
        for course in self.current_data['courses']:
            if course['강의링크'] in changes['updated']:
                target_courses.append(course)

        if not target_courses:
            self._log_progress("처리할 강의 상세 정보 없음")
            self._log_step_complete("강의 상세 정보 수집")
            return

        self._log_progress(f"신규/업데이트 강의 {len(target_courses)}개 상세 수집 시작")

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = []
            for course in target_courses:
                future = executor.submit(
                    self._extract_course_detail, 
                    course['강의링크']
                )
                futures.append(future)

            for i, future in enumerate(futures):
                try:
                    detail = future.result(timeout=300)
                    if detail:
                        self.current_data['course_details'].append(detail)
                        if (i + 1) % 5 == 0:
                            self._log_progress(f"강의 상세 완료: {i+1}/{len(futures)}")
                except Exception as e:
                    self._log_error(f"강의 상세 처리 실패: {i+1}", e)

        self._log_step_complete("강의 상세 정보 수집", 
                              len(self.current_data['course_details']))

    def _extract_course_detail(self, course_url):
        """강의 상세 정보 추출."""
        with sync_playwright() as p:
            browser, page = self._setup_browser(p)

            try:
                course_name = course_url.split('/')[-1]

                if not self._safe_page_load(page, course_url):
                    return None

                self._scroll_page(page)
                page.wait_for_timeout(3000)

                # HTML 저장
                html_content = page.content()
                safe_name = course_url.split('/')[-1]

                with open(self.output_dir / f"courses/{safe_name}.html", 
                          'w', encoding='utf-8') as f:
                    f.write(html_content)

                soup = BeautifulSoup(html_content, 'html.parser')

                # 제목 추출
                title_selectors = ['h1', '[data-e2e="course-title"]', '.title']
                title = "제목 없음"

                for selector in title_selectors:
                    element = soup.select_one(selector)
                    if element and element.get_text().strip():
                        title = element.get_text().strip()
                        if title != "root layout":
                            break

                return {
                    "강의명": title,
                    "강의링크": course_url,
                    "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "추출된_텍스트": self._extract_page_content(soup)
                }

            finally:
                browser.close()

    def _extract_page_content(self, soup):
        """페이지 텍스트 콘텐츠 추출."""
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()

        main_content = soup.find('main') or soup.find('body') or soup
        text = main_content.get_text()

        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return '\n'.join(lines) if lines else "텍스트 없음"

    def _save_incremental_data(self):
        """증분 데이터 저장."""
        self._log_step_start("데이터 저장")
        
        # 기존 데이터와 병합
        merged_data = {}
        for data_type in ['main_categories', 'sub_categories', 'courses', 'course_details']:
            if data_type == 'courses':
                # 강의 데이터는 URL 기반으로 중복 제거
                existing_urls = set(item.get('강의링크', '') 
                                  for item in self.previous_data[data_type])
                new_items = [item for item in self.current_data[data_type]
                           if item.get('강의링크', '') not in existing_urls]
                merged_data[data_type] = self.previous_data[data_type] + new_items
            else:
                # 다른 데이터는 현재 데이터로 교체
                merged_data[data_type] = self.current_data[data_type]

        # JSON 파일 저장
        data_files = {
            'main_categories': 'main_categories.json',
            'sub_categories': 'sub_categories.json',
            'courses': 'courses_list.json',
            'course_details': 'course_details.json'
        }

        for data_type, filename in data_files.items():
            json_path = self.output_dir / "json_files" / filename
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(merged_data[data_type], f, ensure_ascii=False, indent=2)
            self._log_progress(f"{data_type} 저장 완료: {len(merged_data[data_type])}개")

        # 증분 로그 저장
        self._save_incremental_log()

        self._log_step_complete("데이터 저장")

    def _save_incremental_log(self):
        """증분 크롤링 로그 저장."""
        log_data = {
            "실행일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "첫실행여부": self.is_first_run,
            "증분통계": self.incremental_stats,
            "처리시간": time.time() - self.start_time
        }

        log_file = self.output_dir / "incremental_logs" / f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)

    def _log_performance_improvement(self, elapsed_time):
        """성능 개선 효과 로깅."""
        estimated_full_time = 900  # 15분 예상
        improvement_percent = ((estimated_full_time - elapsed_time) / estimated_full_time) * 100
        
        self._log_progress("=== 성능 개선 효과 ===")
        self._log_progress(f"예상 전체 크롤링 시간: {estimated_full_time}초")
        self._log_progress(f"실제 증분 크롤링 시간: {elapsed_time:.2f}초")
        self._log_progress(f"시간 단축: {improvement_percent:.1f}%")
        
        if improvement_percent > 90:
            self._log_progress("🚀 우수한 성능 개선!")
        elif improvement_percent > 70:
            self._log_progress("✅ 좋은 성능 개선!")
        else:
            self._log_progress("⚠️ 성능 개선 필요")

    def run(self):
        """증분 크롤링 실행."""
        self.start_time = time.time()
        self._log_progress("FastCampus 증분 크롤링 시작!")

        # 이전 데이터 로드
        self._load_previous_data()

        if self.is_first_run:
            self._log_progress("첫 실행 감지 - 전체 크롤링 모드")
            # 첫 실행 시에는 기존 크롤러와 동일하게 작동
            # (여기서는 간단히 메인 카테고리만 확인)
        else:
            self._log_progress("재실행 감지 - 증분 크롤링 모드")

        with sync_playwright() as p:
            browser, page = self._setup_browser(p)

            try:
                self._log_progress("브라우저 설정 완료")

                if not self._safe_page_load(page, self.base_url):
                    self._log_error("메인 페이지 로드 실패")
                    return

                self._log_progress("메인 페이지 로드 완료")

                # 메인 카테고리 확인
                self._extract_main_categories(page)

                # 하위 카테고리 확인
                self._extract_sub_categories(page)

                # 신규/업데이트된 하위 카테고리에서 강의 수집
                self._collect_courses_incremental()

                # 신규/업데이트된 강의의 상세 정보 수집
                self._collect_course_details_incremental()

                # 데이터 저장
                self._save_incremental_data()

                # 증분 크롤링 통계 출력
                self._log_incremental_stats()

                # 완료
                elapsed_time = time.time() - self.start_time
                self._log_progress(f"증분 크롤링 완료! 총 소요시간: "
                                 f"{elapsed_time:.2f}초")
                
                # 성능 개선 효과 표시
                if not self.is_first_run:
                    self._log_performance_improvement(elapsed_time)

            except Exception as e:
                self._log_error("크롤링 중 치명적 오류 발생", e)
            finally:
                browser.close()
                self._log_progress("브라우저 종료")


def main():
    """Application entry point."""
    crawler = IncrementalCrawler()
    crawler.run()


if __name__ == "__main__":
    main()
