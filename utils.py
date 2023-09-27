import os
import re

from database.schemas import PersonSchema


def get_database_url():
    url = "mongodb+srv://admin123:admin123@cluster0.wo754tu.mongodb.net/dev-zoom-us?retryWrites=true&w=majority"
    # url = "mongodb://localhost:27017/"
    return url


def get_files():
    data_directory = 'ZoomInfo'
    dist_path = os.path.join(os.getcwd(), data_directory)
    if not os.path.exists(dist_path):
        return False

    # List all files in the directory and filter PDF files
    data_files = [filename for filename in os.listdir(data_directory) if filename.endswith('.txt')]
    return data_files


def get_person_data_from_csv(row):
    return PersonSchema(
        full_name=str(row['Full name']),
        industry=row['Industry'],
        job_title=row['Job title'],
        sub_role=row['Sub Role'],
        industry_2=row['Industry 2'],
        emails=row['Emails'],
        mobile=row['Mobile'],
        phone_numbers=row['Phone numbers'],
        company_name=row['Company Name'],
        company_industry=row['Company Industry'],
        company_website=row['Company Website'],
        company_size=row['Company Size'],
        company_founded=row['Company Founded'],
        location=row['Location'],
        locality=row['Locality'],
        metro=row['Metro'],
        region=row['Region'],
        skills=row['Skills'],
        first_name=row['First Name'],
        middle_initial=row['Middle Initial'],
        middle_name=row['Middle Name'],
        last_name=row['Last Name'],
        birth_year=row['Birth Year'],
        birth_date=row['Birth Date'],
        gender=row['Gender'],
        linkedin_url=row['LinkedIn Url'],
        linkedin_username=row['LinkedIn Username'],
        facebook_url=row['Facebook Url'],
        facebook_username=row['Facebook Username'],
        twitter_url=row['Twitter Url'],
        twitter_username=row['Twitter Username'],
        github_url=row['Github Url'],
        github_username=row['Github Username'],
        company_linkedin_url=row['Company Linkedin Url'],
        company_facebook_url=row['Company Facebook Url'],
        company_twitter_url=row['Company Twitter Url'],
        company_location_name=row['Company Location Name'],
        company_location_locality=row['Company Location Locality'],
        company_location_metro=row['Company Location Metro'],
        company_location_region=row['Company Location Region'],
        company_location_geo=row['Company Location Geo'],
        company_location_street_address=row['Company Location Street Address'],
        company_location_address_line_2=row['Company Location Address Line 2'],
        company_location_postal_code=row['Company Location Postal Code'],
        company_location_country=row['Company Location Country'],
        company_location_continent=row['Company Location Continent'],
        last_updated=row['Last Updated'],
        start_date=row['Start Date'],
        job_summary=row['Job Summary'],
        location_country=row['Location Country'],
        location_continent=row['Location Continent'],
        street_address=row['Street Address'],
        address_line_2=row['Address Line 2'],
        postal_code=row['Postal Code'],
        location_geo=row['Location Geo'],
        linkedin_connections=row['Linkedin Connections'],
        inferred_salary=row['Inferred Salary'],
        years_experience=row['Years Experience'],
        summary=row['Summary'],
        countries=row['Countries'],
        interests=row['Interests']
    )


# Define ANSI escape codes for colors
GREEN = '\033[92m'
END = '\033[0m'

not_allowed_email_domains = ['gmail.com', 'yahoo.com', 'hotmail.com']

# Define regex patterns for allowed industry names and email patterns
industry_pattern = re.compile(
    r'.*(computer|software|development|cyber security|systems|games|network|database|devops|cybersecurity|data|cloud|ui/ux|qa|it project|technical support|machine learning|it consultant|full stack).*',
    re.IGNORECASE)


def business_email_found(person: PersonSchema):
    if person.emails == "" or person.emails is None:
        return False

    # Split the emails by comma
    emails = person.emails.split(', ')
    count_email = 0
    for email in emails:
        parts = email.split('@')
        if len(parts) == 2:  # Ensure there are exactly two parts (username and domain)
            domain = parts[1]
            if domain in not_allowed_email_domains:
                count_email += 1
        else:
            continue
    if count_email == len(emails):
        return False

    return True
