# Data-Harvesting-and-Warehousing-Project
Youtube-Data-Harvesting-And-Warehousing YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and analyse data from numerous YouTube channels. SQL, MongoDB, and Streamlit are used in the project to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.
TOOLS AND LIBRARIES USED: this project requires the following components:
STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.
PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.
GOOGLE API CLIENT: The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.
MySQL is an open-source relational database management system (RDBMS) that allows users to store, manage, and retrieve data efficiently. It's widely used for various applications ranging from small-scale websites to large-scale enterprise systems. MySQL uses SQL (Structured Query Language) for managing and querying data and offers features like scalability, reliability, and ease of use.
MONGODB: MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

REQUIRED LIBRARIES:
1.googleapiclient.discovery
2.streamlit
3.psycopg2
4.pymongo
5.pandas

FEATURES: The following functions are available in the YouTube Data Harvesting and Warehousing application: Retrieval of channel and video data from YouTube using the YouTube API.
Storage of data in a MongoDB database as a data lake.
Migration of data from the data lake to a SQL database for efficient querying and analysis.
Search and retrieval of data from the SQL database using different search options.
