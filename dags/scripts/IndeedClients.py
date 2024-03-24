from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta, timezone
import time


class IndeedClient:

    def __init__(self, internal_port=None):
        self.output = []
        self.internal_port = internal_port

    def create_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        chrome_options.add_argument("--disable-dev-shm-usage")

        if self.internal_port:
            self.driver = webdriver.Remote(
                f"{self.internal_port}:4444/wd/hub",
                options=chrome_options,
            )
        else:
            self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(5)
        self.wait = WebDriverWait(self.driver, 5)

    def get_scraped_items(self):
        return self.output

    def close(self):
        if self.internal_port:
            self.driver.quit()
        else:
            self.driver.close()


class IndeedJobClient(IndeedClient):

    @staticmethod
    def _extract_shorthand(url):
        return url.split("/")[-1]

    @staticmethod
    def _create_review_url(url):
        return f"{url}/reviews"

    def _scrape_all_pages(self, job):
        all_scraped_information = []
        has_next_page: bool = True
        page_number = 0
        curr_url = self.generate_job_listing_url(job, page_number)

        while has_next_page:
            try:
                # Gets a maximum of 150 job postings for each job
                if page_number >= 10:
                    break

                print(f"scraping page {page_number + 1} of {job}")

                page_number += 1
                self.driver.get(curr_url)
                job_postings_div = self.driver.find_element(
                    By.ID, "mosaic-provider-jobcards"
                )
                job_postings_list = job_postings_div.find_element(
                    By.TAG_NAME, "ul"
                ).find_elements(By.XPATH, "./li")

                if len(job_postings_list) != 18:
                    has_next_page = False

                collection = []
                self._scrape_one_page(job, job_postings_list, collection)
                all_scraped_information.extend(collection)
                curr_url = self.generate_job_listing_url(job, page_number)

                time.sleep(5)
            except Exception as e:
                print(e)

        print(f"scraping for {job} is completed")
        return all_scraped_information

    def _scrape_one_page(self, default_job, job_postings_list, collection):
        for index, posting in enumerate(job_postings_list):
            if index not in (5, 11, 17):
                collection.append(self._scrape_one_posting(default_job, posting))

    def _scrape_one_posting(self, default_job, posting):
        output = {}

        # Scroll to where the posting exists
        self.driver.execute_script("arguments[0].scrollIntoView(true);", posting)
        time.sleep(2)

        # Show the job posting information on the right side of the screen
        posting.click()
        posting_description = self.wait.until(
            EC.presence_of_element_located((By.ID, "jobsearch-ViewjobPaneWrapper"))
        )

        # Company Name
        try:
            company_name = posting_description.find_element(By.TAG_NAME, "a").text
        except:
            return

        # Company URL
        try:
            company_url = (
                posting_description.find_element(By.TAG_NAME, "a")
                .get_attribute("href")
                .split("?")[0]
            )
        except:
            return

        # Job Title
        try:
            job_title = (
                posting_description.find_element(
                    By.XPATH, "//h2[@data-testid='jobsearch-JobInfoHeader-title']"
                )
                .find_element(By.TAG_NAME, "span")
                .text.split("\n")[0]
            )
        except:
            job_title = default_job

        # Apply Now URL
        try:
            apply_container = posting_description.find_element(
                By.ID, "applyButtonLinkContainer"
            )
            apply_now_url = apply_container.find_element(
                By.TAG_NAME, "button"
            ).get_attribute("href")
        except:
            apply_now_url = None

        # Job description
        try:
            description = posting_description.find_element(
                By.ID, "jobDescriptionText"
            ).text
        except:
            return

        # Output all the information
        output["mainJob"] = default_job
        output["companyName"] = company_name
        output["companyUrl"] = company_url
        output["companyReviewUrl"] = self._create_review_url(company_url)
        output["companyShorthand"] = self._extract_shorthand(company_url)
        output["jobTitle"] = job_title
        output["applyNowUrl"] = apply_now_url
        output["jobDescription"] = description

        return output

    def scrape_job_listings(self, lst_of_jobs: list[str]):

        for job in lst_of_jobs:
            self.create_driver()
            self.perform_initial_job_cleanups()
            self.perform_initial_job_cleanups()
            self.output.extend(self._scrape_all_pages(job))
            time.sleep(5)

            self.close()

        return self.add_expiration_date()

    def perform_initial_job_cleanups(self):
        self.driver.get(self.generate_job_listing_url("software engineer", 0))
        time.sleep(3)
        self.clear_popups()

    def clear_popups(self):
        self.driver.refresh()
        time.sleep(3)

    def generate_job_listing_url(self, job, page_number):
        base_url = f'https://sg.indeed.com/jobs?q={"+".join(job.split())}&l=Singapore&radius=10&fromage=1&start={page_number * 10}'
        return base_url

    def add_expiration_date(self):
        time_scraped = datetime.now(timezone.utc)
        expiration_time = time_scraped + timedelta(days=30)
        for dic in self.output:
            dic["dateCreated"] = time_scraped
            dic["expiration_date"] = expiration_time
        return time_scraped


class IndeedCompanyClient(IndeedClient):

    def _is_correct_url(self, url):
        return "sg.indeed.com" in url

    def _is_float(self, s):
        try:
            float(s)
            return True
        except:
            return False

    def _get_ratings_by_category(self, categories_block, company_info_output):
        categorical_elements = categories_block.find_elements(By.XPATH, "./child::div")
        for item in categorical_elements:
            list_of_items = item.text.split("\n")

            rating = list_of_items[0]
            category = list_of_items[1].replace(" ", "").replace("/", "")

            company_info_output[f"company{category}Rating"] = (
                None if not self._is_float(rating) else float(rating)
            )

    def _get_overall_rating(self, overall_rating_block, company_info_output):
        rating, review_total_string = overall_rating_block.text.split("\n")
        counts_string = review_total_string.split("Based on ")[1].split(" ")[0]

        company_info_output["companyOverallRating"] = float(rating)
        company_info_output["companyReviewCounts"] = int(
            "".join(counts_string.split(","))
        )

    def _get_histogram_rating(self, histogram_block, company_info_output):
        histogram_elements = histogram_block.find_element(
            By.TAG_NAME, "div"
        ).find_elements(By.XPATH, "./child::div")

        for elem in histogram_elements:
            rating_value, rating_counts = elem.text.split("\n")
            if "K" in rating_counts:
                rating_counts = int(float(rating_counts.split("K")[0]) * 1000)
            else:
                rating_counts = int(rating_counts)

            company_info_output[f"companyTotal{rating_value}Star"] = rating_counts

    def _get_company_name(self, company_info_output, company_shorthand):
        try:
            company_name = self.driver.find_element(
                By.CSS_SELECTOR, ".css-19rjr9w.e1wnkr790"
            ).text
            company_info_output["companyName"] = company_name
        except:
            company_info_output["companyName"] = company_shorthand

    def _scrape_company_stats(self, company_url, company_shorthand, output):
        self.driver.get(company_url)
        time.sleep(1)
        self._get_company_name(output, company_shorthand)

        try:
            rating_block = self.driver.find_element(
                By.XPATH, "//div[@data-tn-component='rating-histogram']"
            )
            overall_rating_block, histogram_block, category_ratings_block = (
                rating_block.find_elements(By.XPATH, "./child::div")
            )

            self._get_overall_rating(overall_rating_block, output)
            self._get_histogram_rating(histogram_block, output)
            self._get_ratings_by_category(category_ratings_block, output)

        except Exception as e:
            print(e)

    def scrape_companies_stats(self, company_urls: list[dict]):
        for i in range(0, len(company_urls), 20):
            self.create_driver()
            for company_dict in company_urls[i : i + 20]:

                review_url = company_dict.get("companyReviewUrl")
                company_url = company_dict.get("companyUrl")
                company_shorthand = company_dict.get("companyShorthand")

                print(f"scraping stats for {company_shorthand}")

                if self._is_correct_url(review_url):
                    company_info = {}
                    company_info["companyShorthand"] = company_shorthand
                    company_info["companyUrl"] = company_url
                    company_info["companyReviewUrl"] = review_url

                    self._scrape_company_stats(
                        review_url, company_shorthand, company_info
                    )
                    self.output.append(company_info)
                    time.sleep(2)

            self.close()
