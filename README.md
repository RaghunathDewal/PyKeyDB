# NoSQL Key-Value Database

This project is a custom-built NoSQL key-value database implemented using B-Trees and disk-based storage in Python. It provides basic functionality for creating, reading, updating, and deleting key-value pairs, as well as managing collections of key-value pairs.

## Features

- **B-Tree Based Storage**: Efficient key-value pair storage using B-Trees for fast data retrieval and management.
- **Disk-Based Storage**: Data is stored on disk, allowing persistence across sessions.
- **Customizable**: Flexible options for configuring page size, fill percentages, and more.
- **Transaction Management**: Supports read and write transactions with commit and rollback functionality.
- **Collection Management**: Allows creating, finding, and deleting named collections of key-value pairs.

## Project Structure

- **`DataBase/`**: Contains the core database implementation.
  - **`DATABASE.py`**: Manages the database operations and transactions.
  - **`node.py`**: Defines the Node structure used in the B-Tree.
  - **`collection.py`**: Handles collections of key-value pairs.
  - **`const.py`**: Contains constant values and error definitions.
- **`main.py`**: Example usage of the database, demonstrating basic operations.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
