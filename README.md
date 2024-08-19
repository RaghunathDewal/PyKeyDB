# NoSQL Key-Value Database

This project is a custom-built NoSQL key-value database implemented using B-Trees and disk-based storage in Python. It provides basic functionality for creating, reading, updating, and deleting key-value pairs, as well as managing collections of key-value pairs.

## Features

- **B-Tree Based Storage**: Efficient key-value pair storage using B-Trees for fast data retrieval and management.
- **Disk-Based Storage**: Data is stored on disk, allowing persistence across sessions.
- **Customizable**: Flexible options for configuring page size, fill percentages, and more.
- **Transaction Management**: Supports read and write transactions with commit and rollback functionality.
- **Collection Management**: Allows creating, finding, and deleting named collections of key-value pairs.
- **Freelist Management**: Efficiently manages free pages for reuse.
- **Data Access Layer (DAL)**: Abstracts the storage layer, providing methods for reading and writing data to disk.

## Project Structure

- **`DataBase/`**: Contains the core database implementation.
  - **`DATABASE.py`**: Manages the database operations and transactions.
  - **`node.py`**: Defines the Node structure used in the B-Tree.
  - **`collection.py`**: Handles collections of key-value pairs.
  - **`const.py`**: Contains constant values and error definitions.
  - **`dal.py`**: Implements the Data Access Layer, handling low-level disk operations, including reading and writing pages.
  - **`freelist.py`**: Manages free pages in the database, ensuring efficient reuse of disk space.
  - **`meta.py`**: Manages metadata related to the database, including the state of the database and its collections.
- **`main.py`**: Example usage of the database, demonstrating basic operations.

For more detailed instructions and information, you can visit [this URL](https://betterprogramming.pub/build-a-nosql-database-from-the-scratch-in-1000-lines-of-code-8ed1c15ed924).
