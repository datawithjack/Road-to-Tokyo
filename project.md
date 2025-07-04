# Road to Tokyo Scraper Project Guide

## 1. Project Structure & Setup

**Recommended folders/files:**
```
Road to Tokyo/
│
├── src/                  # Python source code
│   ├── scraper.py        # Main scraping logic
│   ├── db.py             # Database connection & table setup
│   └── utils.py          # Helper functions (optional)
│
├── requirements.txt      # Python dependencies
├── .github/
│   └── workflows/
│       └── scrape.yml    # GitHub Actions workflow
└── README.md             # Project documentation
```

---

## 2. Step-by-Step Plan

### A. Initial Setup
- Create a virtual environment:  
  `python -m venv venv`
- Activate it and install dependencies (see below).

### B. Dependencies
Add to `requirements.txt`:
```
requests
beautifulsoup4
mysql-connector-python
```
(You may add others as needed.)

### C. Scraping Logic
- Use `requests` to fetch the page.
- Use `BeautifulSoup` to parse the HTML and extract the data you need.

### D. Database Logic
- Use `mysql-connector-python` to connect to your MySQL database.
- Write a function to create the necessary tables if they don't exist.
- Write functions to insert/update scraped data.

### E. Automation with GitHub Actions
- Create a workflow file in `.github/workflows/scrape.yml` to run your script daily.
- Store your MySQL credentials as GitHub secrets.

---

## 3. How to Start

### Step 1: Initialize the Project
- Create the folder structure above.
- Initialize a git repo:  
  `git init`
- Add a `README.md` describing your project.

### Step 2: Set Up the Scraper
- In `src/scraper.py`, write code to fetch and parse the target page.
- Print the data you want to extract to verify parsing works.

### Step 3: Set Up the Database
- In `src/db.py`, write code to connect to your MySQL database.
- Write a function to create tables (based on the data you want to store).
- Test inserting dummy data.

### Step 4: Integrate Scraper and Database
- After scraping, insert/update the data in your MySQL tables.

### Step 5: Automate with GitHub Actions
- Write a workflow to run your script daily.
- Use GitHub secrets for DB credentials.

---

## 4. Next Steps

- Decide which part to implement first: the scraper, the database setup, or the GitHub Actions workflow.
- Follow the step-by-step plan above for a smooth development process. 