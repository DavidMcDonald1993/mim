from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from time import sleep

def initialise_chrome_driver():
    opts = webdriver.ChromeOptions()
    # opts.add_experimental_option("detach", True)
    # opts.add_argument("--disable-gpu")
    # opts.add_argument("--headless")
    return webdriver.Chrome('/usr/local/bin/chromedriver',
        options=opts)

def sign_in_to_chrome(driver):

    driver.get("https://accounts.google.com/")

    username = driver.find_element_by_id("identifierId")

    # send_keys() to simulate key strokes
    username.send_keys('davemcdonald93@gmail.com')


def initialise_firefox_driver():
    return webdriver.Firefox(executable_path="/Users/id301004/Downloads/geckodriver")

def get_url(driver, url, retries=5):
    for retry in range(retries):
        try:
            driver.get(url)
            return
        except WebDriverException:
            print (f"retry {retry} failed")
            sleep(1)
            pass 
    raise Exception

def click_element(driver, element, delay=3):
    current_url = driver.current_url

    element.click()

    try:
        WebDriverWait(driver, delay).until(EC.url_changes(current_url))
    except TimeoutException:
        print ("Loading took too much time!")
        # assert False
        raise NoSuchElementException

def get_element_by_xpath(driver, xpath, delay=3):
    try:
        return WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, xpath)))
    except TimeoutException:
        print ("Loading took too much time!")
        # assert False
        raise NoSuchElementException

