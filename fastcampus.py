#!/usr/bin/env python3
import asyncio
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


class SimpleParallelCrawler:
    """FastCampus 간단한 병렬 크롤러."""
    
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
    
    # M3 칩 최적화 설정 (16GB 메모리) - 속도 우선
    MAX_WORKERS = 12  # 스레드 풀 워커 수 (M3 8코어 + 하이퍼스레딩)
    MAX_CONCURRENT_COURSES = 20  # 동시 강의 수집 수 
    
    TIMEOUTS = {
        'default': 15000,  # 15초로 단축 (속도 우선)
        'navigation': 20000,  # 20초로 단축
        'image_load': 60000,  # 60초로 단축 (속도와 안정성 균형)
        'network_idle': 10000  # 10초로 단축 (빠른 처리)
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
    
    # 개선된 선택자들
    SELECTORS = {
        'main_category': ('div.GNBDesktopCategoryItem_container__ln5E6 '
                         'a[href*="category_online"]'),
        'sub_category': ('li.GNBDesktopCategoryItem_subCategory__twmcG '
                        'a[href*="category_online"]'),
        # 강의 카드 선택자 개선
        'course_card': [
            '[data-e2e="course-card"]',
            'div[class*="CourseCard_courseCard__"]',
            'div[class*="CourseCard"][class*="courseCard__"]',
            'article[class*="course"]',
            'div[class*="course-card"]',
            'div[class*="card"]'
        ],
        # 강의 링크 선택자 개선 및 우선순위 부여
        'course_link': [
            'a[href*="/data_online_"]',
            'a[href*="/dgn_online_"]', 
            'a[href*="/biz_online_"]',
            'a[href*="/course_"]',
            'a[href*="/online_"]',
            'a[href^="/"]'  # 상대 경로 링크도 포함
        ],
        'category_nav': '[data-e2e="navigation-category"]'
    }
    
    # 이미지 관련 상수
    VALID_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg']
    MIN_IMAGE_SIZE = 1000
    MAX_THUMBNAILS = 2
    
    def __init__(self, base_url="https://fastcampus.co.kr/", 
                 output_dir="./simple_parallel_crawl"):
        """크롤러 초기화."""
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
        self.course_titles = {}
        
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
        self._log_progress("간단한 병렬 크롤러 초기화 완료")
    
    def _setup_directories(self):
        """디렉토리 구조 생성."""
        directories = [
            "main_categories", "sub_categories", "courses",
            "sumnail_images", "lect_images", "json_files"
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
    
    def _log_progress_stats(self):
        """진행률 통계 로깅."""
        if self.progress['total_subcategories'] > 0:
            sub_pct = (self.progress['completed_subcategories'] / 
                      self.progress['total_subcategories'] * 100)
            self._log_progress(f"하위 카테고리 진행률: {sub_pct:.1f}% "
                             f"({self.progress['completed_subcategories']}/"
                             f"{self.progress['total_subcategories']})")
        
        if self.progress['total_courses'] > 0:
            course_pct = (self.progress['completed_courses'] / 
                         self.progress['total_courses'] * 100)
            self._log_progress(f"강의 수집 진행률: {course_pct:.1f}% "
                             f"({self.progress['completed_courses']}/"
                             f"{self.progress['total_courses']})")
        
        if self.progress['total_details'] > 0:
            detail_pct = (self.progress['completed_details'] / 
                         self.progress['total_details'] * 100)
            self._log_progress(f"강의 상세 진행률: {detail_pct:.1f}% "
                             f"({self.progress['completed_details']}/"
                             f"{self.progress['total_details']})")
    
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
        """안전한 페이지 로딩 - 개선된 버전."""
        for attempt in range(retries):
            try:
                page.goto(url, timeout=self.TIMEOUTS['navigation'])
                
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
                    self._log_progress(f"페이지 로드 재시도 {attempt + 1}/{retries}: {str(e)[:100]}")
                    page.wait_for_timeout(3000)
                else:
                    self._log_error(f"페이지 로드 최종 실패: {url}", e)
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
    
    def _wait_for_images_to_load(self, page, timeout=None):
        """이미지 완전 로딩 대기 - 개선된 버전."""
        if timeout is None:
            timeout = self.TIMEOUTS['image_load']
            
        try:
            # 1단계: lazy loading 이미지들 먼저 강제 로드
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
            
            # 2단계: srcset 속성 정규화
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
            
            # 3단계: 이미지 로딩 대기 (더 관대한 조건)
            try:
                page.wait_for_function("""
                    () => {
                        const images = Array.from(document.querySelectorAll('img'));
                        if (images.length === 0) return true;
                        
                        // 80% 이상의 이미지가 로드되면 성공으로 간주
                        const loadedImages = images.filter(img => {
                            return img.complete && img.naturalHeight !== 0;
                        });
                        
                        return (loadedImages.length / images.length) >= 0.8;
                    }
                """, timeout=timeout)
                
                self._log_progress("이미지 로딩 완료 (80% 이상)")
                return True
                
            except Exception:
                # 타임아웃이 발생해도 계속 진행 (부분적 로딩 허용)
                self._log_progress("이미지 로딩 타임아웃 - 부분적 로딩으로 진행")
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
            page.wait_for_load_state("networkidle", timeout=self.TIMEOUTS['network_idle'])
            
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
        self._log_step_start("하위 카테고리 수집")
        self._prepare_navigation(page)
        
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
                    self._log_progress(f"하위 카테고리 선택자 성공: {selector} ({len(links)}개)")
                    break
            except Exception as e:
                self._log_error(f"선택자 실패: {selector}", e)
                continue
        
        if not links:
            self._log_error("모든 하위 카테고리 선택자 실패")
            self.progress['total_subcategories'] = 0
            self._log_step_complete("하위 카테고리 수집", 0)
            return
        
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
        
        self.progress['total_subcategories'] = len(self.data['sub_categories'])
        self._log_step_complete("하위 카테고리 수집", 
                               len(self.data['sub_categories']))
    
    def _save_html(self, page, name, category_type):
        """HTML 콘텐츠 저장 (이미지 최적화)."""
        # 이미지 완전 로딩 대기
        self._wait_for_images_to_load(page)
        
        # HTML 콘텐츠 가져오기 전에 추가 정리
        page.evaluate("""
            () => {
                // 빈 srcset 속성 제거
                const sources = document.querySelectorAll('source[srcset=""]');
                sources.forEach(source => source.remove());
                
                // 상대 경로 이미지 URL들을 절대 경로로 변환
                const images = document.querySelectorAll('img[src]');
                const baseUrl = window.location.origin;
                images.forEach(img => {
                    if (img.src.startsWith('/')) {
                        img.src = baseUrl + img.src;
                    } else if (img.src.startsWith('//')) {
                        img.src = 'https:' + img.src;
                    }
                });
                
                // data-src를 src로 변환 (lazy loading 해제)
                const lazyImages = document.querySelectorAll('img[data-src]');
                lazyImages.forEach(img => {
                    if (img.dataset.src) {
                        img.src = img.dataset.src;
                        img.removeAttribute('data-src');
                    }
                });
            }
        """)
        
        html_content = page.content()
        safe_name = name.replace('/', '_').replace(' ', '_')
        html_file = self.output_dir / f"{category_type}/{safe_name}.html"
        
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # 이미지 상태 검증
        self._validate_html_images(html_file)
        
        self._log_progress(f"HTML 저장 완료: {safe_name}.html")
    
    def _extract_title(self, card, course_url=None):
        """강의 제목 추출 - 개선된 버전."""
        # 먼저 저장된 매핑에서 찾기
        if course_url and course_url in self.course_titles:
            return self.course_titles[course_url]
        
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
    
    def _extract_course_url(self, card):
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
            link = card.locator(self.SELECTORS['course_link']).first
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
    
    def _extract_course_url_with_debug(self, card, card_index):
        """디버깅 정보와 함께 강의 URL 추출."""
        debug_info = []
        
        # 1. 카드 자체가 링크인지 확인
        try:
            url = card.get_attribute('href')
            if url:
                debug_info.append(f"카드 자체 href: {url}")
                if self._is_valid_course_url(url):
                    return url
        except Exception as e:
            debug_info.append(f"카드 href 추출 실패: {e}")
        
        # 2. 내부 링크들 확인
        for i, link_selector in enumerate(self.SELECTORS['course_link']):
            try:
                links = card.locator(link_selector).all()
                for j, link in enumerate(links):
                    try:
                        url = link.get_attribute('href')
                        if url:
                            debug_info.append(f"선택자 {i} 링크 {j}: {url}")
                            if self._is_valid_course_url(url):
                                return url
                    except Exception:
                        continue
            except Exception as e:
                debug_info.append(f"선택자 {i} 실패: {e}")
        
        # 3. 모든 a 태그 확인
        try:
            all_links = card.locator('a').all()
            for j, link in enumerate(all_links):
                try:
                    url = link.get_attribute('href')
                    if url:
                        debug_info.append(f"모든 링크 {j}: {url}")
                        if self._is_valid_course_url(url):
                            return url
                except Exception:
                    continue
        except Exception as e:
            debug_info.append(f"모든 링크 검색 실패: {e}")
        
        # 디버깅 정보가 있으면 로깅 (실패한 경우에만)
        if not any("유효한 URL" in info for info in debug_info):
            self._log_progress(f"카드 {card_index} URL 추출 디버깅:")
            for info in debug_info[:3]:  # 처음 3개만 출력
                self._log_progress(f"  {info}")
        
        return None
    
    def _analyze_card_content(self, card, card_index):
        """카드 내용 분석 (디버깅용)."""
        try:
            # 카드의 기본 정보
            tag_name = card.evaluate('el => el.tagName')
            class_name = card.get_attribute('class') or 'no-class'
            
            # 텍스트 내용 (처음 100자만)
            text_content = card.text_content().strip()[:100]
            
            # 내부 링크 개수
            link_count = len(card.locator('a').all())
            
            return f"태그:{tag_name}, 클래스:{class_name[:50]}, 링크수:{link_count}, 텍스트:{text_content}"
            
        except Exception as e:
            return f"분석 실패: {e}"
    
    def _is_valid_course_url(self, url):
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
    
    def _normalize_url(self, url):
        """URL 정규화."""
        if url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            return urljoin(self.base_url, url)
        elif not url.startswith('http'):
            return urljoin(self.base_url, url)
        return url.replace('hhttps://', 'https://')
    
    def _download_image(self, url, retries=3):
        """이미지 다운로드."""
        for attempt in range(retries):
            try:
                response = requests.get(
                    url, 
                    timeout=30,  # 30초로 증가 (이미지 안정성 우선)
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
        """유효한 이미지 URL인지 확인."""
        if not url or len(url) < 10:
            return False
        
        url_lower = url.lower()
        
        # URL에 이미지 확장자가 있거나, CDN URL인 경우
        return (any(ext in url_lower for ext in self.VALID_IMAGE_EXTENSIONS) 
                or 'cdn' in url_lower 
                or 'image' in url_lower
                or 'thumbnail' in url_lower)
    
    def _collect_thumbnails(self, card, course_url):
        """썸네일 이미지 수집."""
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
                        self.course_titles[course['url']] = course['title']
                    
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
    
    def _extract_courses_from_subcategory(self, subcategory, max_courses=None):
        """하위 카테고리에서 강의 정보 추출 (병렬 처리용) - URL 추출 개선."""
        with sync_playwright() as p:
            browser, page = self._setup_browser(p)
            
            try:
                self._log_progress(f"강의 수집 시작: {subcategory['메인카테고리']} > "
                                  f"{subcategory['하위카테고리']}")
                
                if not self._safe_page_load(page, subcategory['하위카테고리링크']):
                    self._log_error(f"페이지 로드 실패: {subcategory['하위카테고리링크']}")
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
                            self._log_progress(f"강의 카드 발견: {len(cards)}개 "
                                             f"(선택자: {selector})")
                            break
                    except Exception:
                        continue
                
                if not cards:
                    # 마지막 fallback - 링크 요소들을 카드로 사용
                    try:
                        cards = page.locator(self.SELECTORS['course_link']).all()
                        if cards:
                            self._log_progress(f"대체 선택자로 강의 카드 발견: {len(cards)}개")
                        else:
                            self._log_error(f"강의 카드를 찾을 수 없음: {subcategory['하위카테고리링크']}")
                            return []
                    except Exception as e:
                        self._log_error(f"강의 카드 검색 실패", e)
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
                            url = urljoin(self.base_url, url)
                        
                        # 단순한 필터링 (작동하는 코드와 동일)
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
                        self._log_progress(f"강의 발견: {title} -> {url}")
                        self._collect_thumbnails(card, url)
                        collected += 1
                        
                    except Exception as e:
                        self._log_error(f"강의 처리 중 오류 (카드 {i})", e)
                        continue
                
                self._log_progress(f"강의 수집 완료: {collected}개 (총 {len(cards)}개 카드 중)")
                return collected_courses
                
            finally:
                browser.close()
    
    def _extract_course_detail(self, course_url):
        """강의 상세 정보 추출 (병렬 처리용)."""
        with sync_playwright() as p:
            browser, page = self._setup_browser(p)
            
            try:
                course_name = course_url.split('/')[-1]
                self._log_progress(f"강의 상세 정보 수집: {course_name}")
                
                if not self._safe_page_load(page, course_url):
                    self._log_error(f"강의 페이지 로드 실패: {course_url}")
                    return None
                
                self._scroll_page(page)
                page.wait_for_timeout(3000)
                
                # 강의 상세 페이지에서도 이미지 최적화 적용
                self._wait_for_images_to_load(page)
                
                # HTML 저장 전 추가 정리
                page.evaluate("""
                    () => {
                        // 빈 srcset 속성 제거
                        const sources = document.querySelectorAll('source[srcset=""]');
                        sources.forEach(source => source.remove());
                        
                        // 상대 경로 이미지 URL들을 절대 경로로 변환
                        const images = document.querySelectorAll('img[src]');
                        const baseUrl = window.location.origin;
                        images.forEach(img => {
                            if (img.src.startsWith('/')) {
                                img.src = baseUrl + img.src;
                            } else if (img.src.startsWith('//')) {
                                img.src = 'https:' + img.src;
                            }
                        });
                        
                        // data-src를 src로 변환 (lazy loading 해제)
                        const lazyImages = document.querySelectorAll('img[data-src]');
                        lazyImages.forEach(img => {
                            if (img.dataset.src) {
                                img.src = img.dataset.src;
                                img.removeAttribute('data-src');
                            }
                        });
                    }
                """)
                
                html_content = page.content()
                safe_name = course_url.split('/')[-1]
                
                with open(self.output_dir / f"courses/{safe_name}.html", 
                          'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                # 이미지 상태 검증
                self._validate_html_images(self.output_dir / f"courses/{safe_name}.html")
                
                self._log_progress(f"강의 HTML 저장 완료: {safe_name}.html")
                
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
                
                self._log_progress(f"강의 상세 정보 수집 완료: {title}")
                
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
    
    def _validate_html_images(self, html_file):
        """HTML 파일의 이미지 상태 검증 - 개선된 버전."""
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # 이미지 통계 수집
            total_images = len(soup.find_all('img'))
            broken_images = 0
            empty_srcset = 0
            relative_urls = 0
            
            for img in soup.find_all('img'):
                src = img.get('src', '')
                data_src = img.get('data-src', '')
                
                # 깨진 이미지 체크 (더 관대한 기준)
                if not src and not data_src:
                    broken_images += 1
                elif src and (src.startswith('/') or src.startswith('//')):
                    relative_urls += 1
            
            # 빈 srcset 체크
            for source in soup.find_all('source'):
                srcset = source.get('srcset', '')
                if not srcset or srcset.strip() == '':
                    empty_srcset += 1
            
            # 검증 결과 로깅 (더 관대한 기준)
            broken_ratio = broken_images / total_images if total_images > 0 else 0
            
            if broken_ratio > 0.3:  # 30% 이상 깨진 이미지가 있을 때만 경고
                self._log_error(f"이미지 문제 발견: {html_file.name}")
                self._log_error(f"  - 총 이미지: {total_images}개")
                self._log_error(f"  - 깨진 이미지: {broken_images}개 ({broken_ratio:.1%})")
                self._log_error(f"  - 빈 srcset: {empty_srcset}개")
                self._log_error(f"  - 상대 경로: {relative_urls}개")
            else:
                self._log_progress(f"이미지 검증 통과: {html_file.name} "
                                 f"({total_images}개 이미지, 깨진 이미지: {broken_images}개)")
                
        except Exception as e:
            self._log_error(f"이미지 검증 중 오류: {html_file}", e)
    
    def _collect_lecture_images(self, page, course_name):
        """강의 상세 페이지에서 이미지 수집."""
        try:
            lecture_dir = self.output_dir / "lect_images" / course_name
            lecture_dir.mkdir(parents=True, exist_ok=True)
            
            # 강의 상세 페이지의 모든 이미지 수집
            img_selectors = [
                'img[alt*="강의"]',  # 강의 관련 이미지
                'img[alt*="과정"]',  # 과정 관련 이미지
                'img[alt*="커리큘럼"]',  # 커리큘럼 이미지
                'img[alt*="프로젝트"]',  # 프로젝트 이미지
                'img[class*="lecture"]',  # 강의 관련 클래스
                'img[class*="course"]',  # 코스 관련 클래스
                'img[class*="curriculum"]',  # 커리큘럼 관련 클래스
                'img[class*="project"]',  # 프로젝트 관련 클래스
                'img[data-nimg="fill"]',  # Next.js 이미지
                'img[src*="course"]',  # course가 포함된 src
                'img[src*="lecture"]',  # lecture가 포함된 src
                'img[src*="curriculum"]'  # curriculum이 포함된 src
            ]
            
            saved_count = 0
            saved_urls = set()
            # 갯수 제한 없이 모든 강의 관련 이미지 수집
            
            for selector in img_selectors:
                try:
                    images = page.locator(selector).all()
                    for img in images:
                            
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
                                filename = f"lecture_{saved_count + 1}{ext}"
                                
                                with open(lecture_dir / filename, 'wb') as f:
                                    f.write(response.content)
                                
                                saved_urls.add(url)
                                saved_count += 1
                                self._log_progress(f"강의 이미지 저장: {filename} "
                                                 f"({len(response.content)} bytes)")
                except Exception as e:
                    self._log_error(f"강의 이미지 처리 중 오류", e)
                    continue
            
            if saved_count > 0:
                self._log_progress(f"강의 이미지 수집 완료: {saved_count}개 "
                                 f"({course_name}) - 제한 없음")
            else:
                self._log_progress(f"강의 이미지 없음: {course_name}")
                
        except Exception as e:
            self._log_error(f"강의 이미지 수집 중 오류: {course_name}", e)
    
    def _validate_data(self, data_type, data):
        """데이터 품질 검증."""
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
        """JSON 파일 저장."""
        data_type = filename.replace('.json', '')
        
        if data and self._validate_data(data_type, data):
            json_path = self.output_dir / "json_files" / filename
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    
    def run(self):
        """크롤링 실행 (병렬 처리)."""
        self.start_time = time.time()
        self._log_progress("FastCampus 병렬 크롤링 시작!")
        
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
                
                # HTML 저장 (전체)
                self._log_step_start("HTML 저장")
                self._log_progress(f"메인 카테고리 HTML 저장 시작: {len(self.data['main_categories'])}개")
                for i, category in enumerate(self.data['main_categories'], 1):
                    self._log_progress(f"메인 카테고리 HTML 저장 ({i}/{len(self.data['main_categories'])}): "
                                     f"{category['메인카테고리']}")
                    if self._safe_page_load(page, category['메인카테고리링크']):
                        self._scroll_page(page)
                        self._save_html(page, category['메인카테고리'], 
                                      "main_categories")
                
                self._log_progress(f"하위 카테고리 HTML 저장 시작: {len(self.data['sub_categories'])}개")
                for i, category in enumerate(self.data['sub_categories'], 1):
                    self._log_progress(f"하위 카테고리 HTML 저장 ({i}/{len(self.data['sub_categories'])}): "
                                     f"{category['하위카테고리']}")
                    if self._safe_page_load(page, category['하위카테고리링크']):
                        self._scroll_page(page)
                        self._save_html(page, category['하위카테고리'], 
                                      "sub_categories")
                self._log_step_complete("HTML 저장")
                
                # 강의 목록 수집 (병렬 처리)
                self._log_step_start("강의 목록 수집")
                self.progress['total_courses'] = len(self.data['sub_categories']) * 20  # 예상 강의 수
                
                # 스레드 풀을 사용한 병렬 처리
                with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                    # 하위 카테고리별로 병렬 처리
                    futures = []
                    for subcategory in self.data['sub_categories']:
                        future = executor.submit(
                            self._extract_courses_from_subcategory, 
                            subcategory, 
                            max_courses=None  # 제한 없음
                        )
                        futures.append(future)
                    
                    # 결과 수집
                    for i, future in enumerate(futures):
                        try:
                            courses = future.result(timeout=180)  # 3분 타임아웃 (M3 성능 활용)
                            if courses:
                                self.data['courses'].extend(courses)
                                self.progress['completed_subcategories'] += 1
                                self._log_progress(f"하위 카테고리 완료: {i+1}/{len(futures)}")
                        except Exception as e:
                            self._log_error(f"하위 카테고리 처리 실패: {i+1}", e)
                
                self._log_step_complete("강의 목록 수집", len(self.data['courses']))
                
                # 강의 상세 정보 수집 (병렬 처리)
                self._log_step_start("강의 상세 정보 수집")
                self.progress['total_details'] = len(self.data['courses'])
                
                with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                    # 강의별로 병렬 처리
                    futures = []
                    for course in self.data['courses']:
                        future = executor.submit(
                            self._extract_course_detail, 
                            course['강의링크']
                        )
                        futures.append(future)
                    
                    # 결과 수집
                    for i, future in enumerate(futures):
                        try:
                            detail = future.result(timeout=120)  # 2분 타임아웃 (M3 성능 활용)
                            if detail:
                                self.data['course_details'].append(detail)
                                self.progress['completed_details'] += 1
                                if self.progress['completed_details'] % 10 == 0:
                                    self._log_progress(f"강의 상세 완료: {self.progress['completed_details']}/{self.progress['total_details']}")
                        except Exception as e:
                            self._log_error(f"강의 상세 처리 실패: {i+1}", e)
                
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
                self._log_progress(f"병렬 크롤링 완료! 총 소요시간: "
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
    crawler = SimpleParallelCrawler()
    crawler.run()


if __name__ == "__main__":
    main()