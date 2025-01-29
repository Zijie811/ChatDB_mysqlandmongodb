ChatDB: Natural Language to Database Query System

Overview: 
ChatDB is a command-line application that allows users to interact with SQL and NoSQL databases using natural language queries. The system parses user input, generates the corresponding queries, executes them, and displays the results.

---

Commands to Run the Program

1. Install Dependencies
   pip install pymongo mysql-connector-python nltk pandas
   Additionally, download the NLTK stopwords dataset:
   python
   import nltk
   nltk.download('stopwords')
   nltk.download('punkt')


2. Start the Application
   Run the 'main.py' script to start the program:
   python main.py
   

3. Choose Database
   The application will prompt you to select a database type (MySQL or MongoDB):
   please select a database type: (MySQL or MongoDB):
   

4. Explore Options
   After selecting the database, navigate the menu options to:
   - Explore database structure.
   - Generate sample queries.
   - Process natural language queries.
   - Upload CSV files.

5.Please also change the mysql and mongodb connection to your own connection.
for instance, in mysql:
host="yourhost",
user="username",
password="yourpassword"
---

File Structure

1. Main Directory: main.py
   The entry point of the application. Manages database selection and workflow navigation.

2. Core Modules
   - db_connection.py
   Handles database connections for MySQL and MongoDB, including schema retrieval.

   - explore_db.py
   Contains functions to explore database structures and retrieve table data.

   - nlp_handler.py
   Processes natural language queries, tokenizes input, and matches patterns to generate SQL queries.

   - query_generator.py
   Dynamically generates NoSQL queries for MongoDB based on user input.

   - upload_csv.py
   Facilitates the upload of CSV files into the database, supporting new table creation if necessary.

   - user_interface.py
   Provides the command-line interface for user interaction and menu navigation.

3. Supporting Modules
   - sample_queries.py
   Provides predefined query samples and handles SQL constructs like JOIN, GROUP BY, and ORDER BY.

4. Upload Directory
   A directory named 'upload' where CSV files should be placed for database uploads.

---

Features
- Natural Language Processing: Translates user input into SQL/NoSQL queries.
- Multi-Database Support: Works with MySQL (SQL) and MongoDB (NoSQL).
- Interactive Interface: Allows exploration of database schema and execution of queries.
- CSV Uploads: Supports adding new data to existing or new tables from CSV files.

---

Future Enhancements
- Implementing a graphical user interface.
- Adding machine learning models for more accurate natural language understanding.
- Expanding support for additional database types.
