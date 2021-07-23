

import sys
import os.path

sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from urllib.parse import quote

# import web driver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException

from dotenv import load_dotenv
load_dotenv()

import os 

from time import sleep

import pandas as pd 

from utils.scraping_utils import clean_directors_name
from utils.selenium_utils import initialise_chrome_driver, get_element_by_xpath, click_element
from utils.io import read_json, write_json

delay = 2

def login(driver,
    email=os.getenv("LINKEDINEMAIL"),
    password=os.getenv("LINKEDINPASSWORD")):

    driver.get('https://www.linkedin.com')
    sleep(delay)

    print ("logging in")


    print ("finding username")

    # locate email form by_class_name
    # username = driver.find_element_by_id('username')
    # username = driver.find_element_by_xpath("//*/input[@id='session_key']")
    username = get_element_by_xpath(driver, "//*/input[@id='session_key']")

    # send_keys() to simulate key strokes
    username.send_keys(email)

    print ("finding password")

    # locate password form by_class_name
    # password = driver.find_element_by_id('password')
    # password_field = driver.find_element_by_xpath("//*/input[@id='session_password']")
    password_field = get_element_by_xpath(driver, "//*/input[@id='session_password']")

    # send_keys() to simulate key strokes
    password_field.send_keys(password)

    print ("finding log in button")

    # locate submit button by_class_name
    # login_button = driver.find_element_by_class_name('btn__primary--large')
    # login_button = driver.find_element_by_xpath("//*/button[@class='sign-in-form__submit-button']")
    login_button = get_element_by_xpath(driver, "//*/button[@class='sign-in-form__submit-button']")

    # .click() to mimic button click
    # login_button.click()
    click_element(driver, login_button)
    # sleep(1)

def search_linked_in(driver, director_name, company_name):
    company_name  = company_name.lower()
    company_name = company_name.replace(".", "")
    company_name = company_name.replace("limited", "")
    query = f"{director_name} {company_name}"
    query = quote(query)
    print ("searching for", query)

    # search_bar = driver.find_element_by_class_name("search-global-typeahead__input")
    # search_bar.send_keys(query)
    # search_bar.send_keys(Keys.RETURN)
    # search_bar.clear()

    '''
    https://www.linkedin.com/search/results/all/?keywords=Gary%20Fox%20%22v%22%20installations%20mechanical%20handling%20

    '''
    results_url = f"https://www.linkedin.com/search/results/all/?keywords={query}"
    print ("going to url", results_url)
    driver.get(results_url)
    sleep(delay)

    # link = driver.find_element_by_class_name("app-aware-link")
    # director_name = director_name.lower().replace(" ", "-")
    # link = driver.find_element_by_xpath(
    #     "//a[contains(href(), 'https://www.linkedin.com/in/')]")
    # links = driver.find_element_by_css_selector(
        # 'a[href*="https://www.linkedin.com/in/"]')
    links = driver.find_elements_by_tag_name("a")
    for link in links:
        href = link.get_attribute("href")
        if href.startswith("https://www.linkedin.com/in/"):
            return href
    return None

def search_directors():
    directors = pd.read_csv("WM_company_info_endole.csv", 
        index_col=0)

    driver = None

    output_file = "director_linked_in_pages.json"
    if os.path.exists(output_file):
        director_linked_in = read_json(output_file)
    else:
        director_linked_in = dict()

    for company_name, row in directors.iterrows():

        for i in range(4):
            col = f"director_{i+1}_name"
            if not pd.isnull(row[col]):
                director_name = row[col]
                if "Director" not in director_name:
                    continue
                director_name = director_name.split("Director")[0] 
                director_name = clean_directors_name(director_name)

                if director_name in director_linked_in:
                    continue

                if driver is None:
                    driver = initialise_chrome_driver()
                    login(driver)

                linkedin_url = search_linked_in(driver, 
                    director_name, company_name)

                director_linked_in[director_name] =\
                    {"company": company_name, "linkedin_url": linkedin_url}
                write_json(director_linked_in, output_file)

    director_linked_in = pd.DataFrame(director_linked_in, ).T
    director_linked_in.to_csv("director_linked_in_pages.csv")


def go_to_connections(driver):
    url = "https://www.linkedin.com/mynetwork/invite-connect/connections/"
    driver.get(url)
    sleep(delay)
   
    # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    # html = driver.find_element_by_tag_name('html')
    # for _ in range(3):
    #     html.send_keys(Keys.END)
    #     sleep(1)

def find_contacts(driver, max_contacts=4000):

    num_connections_element = get_element_by_xpath(driver, "//header[@class='mn-connections__header']")
    num_connections = int(num_connections_element.text.split()[0].replace(",", ""))
    num_connections = min(num_connections, max_contacts)
    print ("NUMBER OF CONNECTIONS", num_connections)

    contacts = driver.find_elements_by_xpath("//li[starts-with(@id,'ember')]")
    print ("FOUND", len(contacts), "CONTACTS")

    html = driver.find_element_by_tag_name('html')

    while len(contacts) < num_connections:

        html.send_keys(Keys.END)
        sleep(1)
        contacts = driver.find_elements_by_xpath("//li[starts-with(@id,'ember')]")

        print ("FOUND", len(contacts), "CONTACTS")

    contacts_json = []

    for i, contact in enumerate(contacts):
        name = contact.find_element_by_xpath(".//span[contains(@class, 'mn-connection-card__name')]").text
        occupation = contact.find_element_by_xpath(".//span[contains(@class, 'mn-connection-card__occupation')]").text
        link = contact.find_element_by_xpath(".//a[starts-with(@id, 'ember')]")
        link = link.get_attribute("href")

        print(i, name, occupation, link)
        contacts_json.append({
            "contact_id": i,
            "contact_name": name,
            "contact_occupation": occupation,
            "linkedin_page": link,
        })
        print ()

    return contacts_json

def process_contact(driver, contact):

    contact_name = contact["contact_name"]
    url = contact["linkedin_page"]
    print ("identifying connections for contact", contact_name, "using url", url)

    driver.get(url)
    sleep(delay)

    # contact_element = driver.find_element_by_xpath("//ul[@class='pv-top-card--list pv-top-card--list-bullet display-flex pb1']")
    contact_element = get_element_by_xpath(driver, "//ul[@class='pv-top-card--list pv-top-card--list-bullet display-flex pb1']")
    
    # public_connections = False

    try:
        link = contact_element.find_element_by_xpath(".//*/a[starts-with(@id, 'ember')]")
        link_url = link.get_attribute("href")
        print ("FOUND LINK", link_url)

        # current_url = driver.current_url
        # public_connections = True

        # driver.get(link_url)
        # link.click()
        # sleep(1)

        # WebDriverWait(driver, 15).until(EC.url_changes(current_url))
        click_element(driver, link)

        return scrape_contact_connections(driver)

    except NoSuchElementException:
        print ("COULD NOT FIND LINK")
        return []
    # print ()

def scrape_contact_connections(driver):
    print ("SCRAPING CONTACT CONNECTIONS")

    found_contacts = []
    num_connections_element = get_element_by_xpath(driver, "//div[@class='pb2 t-black--light t-14']")
    num_connections = num_connections_element.text
    num_connections = int(num_connections.split()[0])

    print ("CONTACT HAS", num_connections, "CONNECTIONS")

    while len(found_contacts) < num_connections:

        contacts_on_page = driver.find_elements_by_xpath("//li[@class='reusable-search__result-container ']")

        for contact_on_page in contacts_on_page:

            try:
                # name = contact_on_page.find_element_by_xpath(".//*/span[@aria-hidden='true']").text
                name = get_element_by_xpath(contact_on_page, ".//*/span[@aria-hidden='true']").text
            except NoSuchElementException:
                name = ""
            # print ("NAME", name)

            try:
                # connection_level = contact_on_page.find_element_by_xpath(".//*/span[@class='image-text-lockup__text entity-result__badge-text']").text.split()[-1]
                connection_level = get_element_by_xpath(contact_on_page, ".//*/span[@class='image-text-lockup__text entity-result__badge-text']").text.split()[-1]
            except NoSuchElementException:
                connection_level = ""
            # print ("CONNECTION LEVEL", connection_level, connection_level=="2nd")

            try:
                # occupation = contact_on_page.find_element_by_xpath(".//*/div[@class='entity-result__primary-subtitle t-14 t-black']").text
                occupation = get_element_by_xpath(contact_on_page, ".//*/div[@class='entity-result__primary-subtitle t-14 t-black']").text
            except NoSuchElementException:
                occupation = ""
            # print ("OCCUPATION", occupation)

            try:
                # link = contact_on_page.find_element_by_xpath(".//*/a[@class='app-aware-link']")
                link = get_element_by_xpath(contact_on_page, ".//*/a[@class='app-aware-link']")
                link_url = link.get_attribute("href")
            except NoSuchElementException:
                link_url = ""

            found_contacts.append({
                "contact_name": name,
                "contact_connection_level": connection_level,
                "contact_occupation": occupation,
                "contact_url": link_url,
            })

        if len(found_contacts) == num_connections:
            break

        '''
        click next button
        '''

        # flag = False
        # for i in range(3):
        try:
            html = driver.find_element_by_tag_name('html')
            html.send_keys(Keys.END)
            # sleep(1)
            # next_button = html.find_element_by_xpath(".//*/button[@aria-label='Next']")
            next_button = get_element_by_xpath( html, ".//*/button[@aria-label='Next']")
            
            print ("TRYING TO CLICKING NEXT", )
            # next_button.click()
            click_element(driver, next_button)
            # sleep(1)
            # flag = True
        # break 
        except NoSuchElementException:
            break
        # assert flag
        # if not flag:
            # break

        print()
    # raise Exception
    print("completed scraping contact connections")
    print("found", len(found_contacts), "connections")
    return found_contacts

def scrape_data(driver, url):

    driver.get(url)
    sleep(delay)

    try:
        company = get_element_by_xpath(driver, "//h2[@class='text-heading-small align-self-center flex-1']")
        company = company.text
    except NoSuchElementException:
        company = None
    print ("WORKS AT COMPANY", company)
    

    # get experience
    experiences = []

    experience_list = driver.find_elements_by_xpath( "//li[contains(@class, 'pv-entity__position-group-pager')]")

    print ("FOUND", len(experience_list), "EXPERIENCES")

    for e in experience_list:

        title = get_element_by_xpath(e, ".//*/h3").text
        print ("EXPERIENCE:", title)
        
        # TODO improve?
        experiences.append(title)


    contact_link = get_element_by_xpath(driver, "//a[contains(@href, 'contact-info')]")

    # contact_link.click()
    click_element(driver, contact_link)

    sections = driver.find_elements_by_xpath("//*/section[contains(@class, 'pv-contact-info__contact-type')]")

    print ("found", len(sections), "contact details")

    contact_info = []
    for section in sections:
        # print (section.get_attribute("innerHTML"))
        section_name = get_element_by_xpath(section, ".//header[contains(@class, 'pv-contact-info__header')]")
        print (section_name.text)

        # section_body = get_element_by_xpath(section, ".//div[contains(@class, 'pv-contact-info__ci-container')]")
        # section_body = get_element_by_xpath(section, ".//div[contains(@class, 'pv-contact-info__ci-container')]")
        section_body = section.find_element_by_class_name("pv-contact-info__ci-container")
        print (section_body.text)

        contact_info.append({
            "key": section_name.text,
            "value": section_body.text,
        })

   
    print ()
    return company, experiences, contact_info

def initialise_driver(linkedin_credentials):
    driver = initialise_chrome_driver()
    #driver.set_window_position(-10000,0)

    login(driver, 
        email=linkedin_credentials["email"],
        password=linkedin_credentials["password"])    
    return driver

def get_all_contacts(
    driver, 
    contact_filename, 
    linkedin_credentials):
   
    if os.path.exists(contact_filename):
        contacts = read_json(contact_filename)
    else:
        if driver is None:
            driver = initialise_driver(linkedin_credentials)

        go_to_connections(driver)

        contacts = find_contacts(driver)
        write_json(contacts, contact_filename)
  
    return contacts

def get_all_second_order_connection_urls(
    driver, 
    contacts, 
    second_order_connections_filename, 
    linkedin_credentials):

    if os.path.exists(second_order_connections_filename):
        all_contact_connections = read_json(second_order_connections_filename)
    else:
        all_contact_connections = dict()

    for contact in contacts[:100]:
        contact_id = str(contact["contact_id"])
        
        if contact_id in all_contact_connections:
            continue

        print ("PROCESSING CONTACT", contact_id, ":", contact["contact_name"])

        if driver is None:
            driver = initialise_driver(linkedin_credentials)

        contact_connections = process_contact(driver, contact)
        contact["contact_connections"] = contact_connections
        all_contact_connections[contact_id] = contact
        write_json(all_contact_connections, second_order_connections_filename)
    return all_contact_connections

def scrape_pages_of_second_order_connections(
    driver, 
    all_contact_connections,
    second_order_connections_filename,
    linkedin_credentials):

    if os.path.exists(second_order_connections_filename):
        second_degree_connections = read_json(second_order_connections_filename)
    
    else:
        second_degree_connections = dict()

    # # iterate over first order connections
    for contact_id, contact in all_contact_connections.items():
        # contact = all_contact_connections[contact_id]
        
        for second_degree_connection in contact["contact_connections"]:
            if second_degree_connection["contact_connection_level"] != "2nd":
                # skip first order
                continue

            second_degree_connection_url = second_degree_connection["contact_url"]
            second_degree_connection_url = second_degree_connection_url.split("?")[0]
            second_degree_connection_id = second_degree_connection_url.split("https://www.linkedin.com/in/")[1]

            second_degree_connection["contact_url"] = second_degree_connection_url
            second_degree_connection["contact_id"] = second_degree_connection_id
                
            if second_degree_connection_id not in second_degree_connections:
                # add current dict of data about second degree connection
                second_degree_connections[second_degree_connection_id] = second_degree_connection

                # scrape second order connection page -- only needed on first visit
                second_degree_connection_url = second_degree_connection["contact_url"]
                if driver is None:
                    driver = initialise_driver(linkedin_credentials)
                company_name, experiences, contact_info = scrape_data(driver, second_degree_connection_url)
                second_degree_connections[second_degree_connection_id]["company_name"] = company_name
                second_degree_connections[second_degree_connection_id]["work_experience"] = experiences
                second_degree_connections[second_degree_connection_id]["contact_info"] = contact_info

                # initialise collection of first order connections
                second_degree_connections[second_degree_connection_id]["common_contacts"] = dict()

                # write json after adding new second order connection
                write_json(second_degree_connections, second_order_connections_filename)
          
            # add first order connection
            second_degree_connections[second_degree_connection_id]["common_contacts"][contact_id] = {
                k: contact[k] 
                    for k in ("contact_id", "contact_name", "contact_occupation", "linkedin_page")
            }

        # write second degree connectinos json after processing each first order connection
        write_json(second_degree_connections, second_order_connections_filename)

    # add number of common connections
    for _id in second_degree_connections:
        second_degree_connections[_id]["number_common_contacts"] = \
            len(second_degree_connections[_id]["common_contacts"])
    write_json(second_degree_connections, second_order_connections_filename)
    

    return second_degree_connections
            

def main():

    # linkedin_credentials_filename = "david-linkedin-credentials.json"
    # linkedin_credentials_filename = "jason-linkedin-credentials.json"
    linkedin_credentials_filename = "ilona-linkedin-credentials.json"
    linkedin_credentials = read_json(linkedin_credentials_filename)
    email = linkedin_credentials["email"]

    driver = None

    '''
    FIND ALL FIRST ORDER CONNECTIONS
    '''
    contact_filename = f"{email}-linked-in-contacts.json"
    contacts = get_all_contacts(driver, contact_filename, linkedin_credentials)

    '''
    FIND ALL CONNECTIONS TO FIRST ORDER CONNECTIONS
    '''

    contact_connection_filename = f"{email}-linked-in-contact-connections.json"
    all_contact_connections = get_all_second_order_connection_urls(
        driver, 
        contacts,
        contact_connection_filename, 
        linkedin_credentials)
    
    # second_degree_connections = dict()

    # for contact_id in all_contact_connections:
    #     contact = all_contact_connections[contact_id]
        
    #     for connection in contact["contact_connections"]:
    #         if connection["contact_connection_level"] != "2nd":
    #             continue
    #         connection_name = connection["contact_name"]
          
    #         if connection_name not in second_degree_connections:
    #             second_degree_connections[connection_name] = connection
    #             second_degree_connections[connection_name]["common_contacts"] = []
          
    #         second_degree_connections[connection_name]["common_contacts"].append(
    #             {k: contact[k] for k in ("contact_id", "contact_name", "contact_occupation", "linkedin_page")}
    #         )

    # # add number of common connections
    # for second_degree_connection in second_degree_connections:
    #     second_degree_connections[second_degree_connection]["number_common_contacts"] = \
    #         len(second_degree_connections[second_degree_connection]["common_contacts"])

    # # scrape second-degree connection pages
    # for name, second_degree_connection in second_degree_connections.items():
    #     url = second_degree_connection["contact_url"]

    #     if driver is None:
    #         driver = initialise_chrome_driver()
    #         # driver.set_window_position(-10000,0)

    #         login(driver, 
    #         email=email,
    #         password=linkedin_credentials["password"])
       
    #     if "company" not in second_degree_connection:
    #         company, contact_info = scrape_data(driver, url)
    #         second_degree_connection["company"] = company
    #         second_degree_connection["contact_info"] = contact_info


    #     second_degree_connections_[name] = second_degree_connection
   
    # second_degree_connection = second_degree_connections_

    second_degree_connections_filename = f"{email}-linked-in-second-degree-connections.json"
    # write_json(second_degree_connections, second_degree_connections_filename)
    second_degree_connections = scrape_pages_of_second_order_connections(
        driver,
        all_contact_connections,
        second_degree_connections_filename,
        linkedin_credentials)

    second_degree_connections_csv_filename = f"{email}-linked-in-second-degree-connections.csv"
    second_degree_connections_df = pd.DataFrame(second_degree_connections).T
    second_degree_connections_df = second_degree_connections_df.sort_values(["number_common_contacts", "contact_name"], ascending=[False, True])
    second_degree_connections_df.to_csv(second_degree_connections_csv_filename)

    print ("identified", len(second_degree_connections), "unique second degree connections")
   

if __name__ == "__main__":
    main()