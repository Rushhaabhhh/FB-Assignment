# FB Messenger Backend Implementation with Cassandra

This project is a high-performance messaging backend inspired by Facebook Messenger, built using FastAPI and powered by Apache Cassandra for distributed data storage. Designed for scalability, it efficiently supports large-scale message traffic through well-structured and optimized data models.

## Architecture

The application follows a layered FastAPI structure:

  ```
app/               // Main application package
├── api/           // API routes and endpoints
│   └── routes/    // Routes connection logic
├── controllers/   // Controller logic
├── db/            // Database connection utilities
├── models/        // Database models
├── schemas/       // Pydantic models for request/response validation
└── main.py
```

## Requirements

- Docker and Docker Compose (for containerized development environment)
- Python 3.11+ (for local development)

## Setup Instructions

1. Clone this repository
2. Make sure Docker and Docker Compose are installed on your system
3. Run the initialization script:
   ```
   ./init.sh
   ```

This will:
- Start both FastAPI application and Cassandra containers
- Initialize the Cassandra keyspace and tables
- Optionally generate test data for development
- Make the application available at http://localhost:8000

Access the API documentation at http://localhost:8000/docs

To stop the application:
```
docker-compose down
```

### Test Data

The setup script includes an option to generate test data for development purposes. This will create:

- 10 test users (with IDs 1-10)
- 15 conversations between random pairs of users
- Multiple messages in each conversation with realistic timestamps

You can use these IDs for testing your API implementations. If you need to regenerate the test data:

```
docker-compose exec app python scripts/generate_test_data.py
```

## Manual Setup (Alternative)

If you prefer not to use Docker, you can set up the environment manually:

1. Clone this repository
2. Install Cassandra locally and start it
3. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install dependencies:
   ```
   uv pip install -r requirements.txt
   ```
5. Run the setup script to initialize Cassandra:
   ```
   python scripts/setup_db.py
   ```
6. Start the application:
   ```
   uvicorn app.main:app --reload
   ```

## Cassandra Data Model

The application uses these Cassandra tables:

- **messages**: Messages stored by conversation_id
- **messages_by_user**: Messages indexed by user
- **conversations**: Conversation metadata
- **conversations_by_user**: Conversations indexed by user

This design enables efficient queries for:
- Fetching conversation messages with pagination
- Retrieving messages before a specific timestamp
- Getting a user's conversations
- Accessing conversation details

## API Endpoints

### Messages

- `POST /api/messages/`: Send a message from one user to another
- `GET /api/messages/conversation/{conversation_id}`: Get all messages in a conversation
- `GET /api/messages/conversation/{conversation_id}/before`: Get messages before a timestamp

### Conversations

- `GET /api/conversations/user/{user_id}`: Get all conversations for a user
- `GET /api/conversations/{conversation_id}`: Get a specific conversation


## Database Schema

### Keyspace

```cql
CREATE KEYSPACE IF NOT EXISTS messenger
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};
```

### Tables

**Users**
```cql
CREATE TABLE IF NOT EXISTS users (
    user_id uuid,
    username text,
    created_at timestamp,
    PRIMARY KEY (user_id)
)
```

**Messages**
```cql
CREATE TABLE messages (
  conversation_id INT,
  timestamp TIMESTAMP,
  message_id UUID,
  sender_id uuid,
  receiver_id uuid,
  content TEXT,
  read_at TIMESTAMP,
  PRIMARY KEY ((conversation_id), timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC);
```

**Messages By User**
```cql
CREATE TABLE messages_by_user (
  user_id uuid,
  conversation_id INT,
  timestamp TIMESTAMP,
  message_id UUID,
  sender_id uuid,
  receiver_id uuid,
  content TEXT,
  PRIMARY KEY ((user_id), conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (conversation_id ASC, timestamp DESC, message_id ASC);
```

**Conversations**
```cql
CREATE TABLE conversations (
  conversation_id INT,
  user1_id uuid,
  user2_id uuid,
  created_at TIMESTAMP,
  last_message_at TIMESTAMP,
  last_message_content TEXT,
  PRIMARY KEY (conversation_id)
);
```

**Conversations By User**
```cql
CREATE TABLE conversations_by_user (
  user_id uuid,
  conversation_id INT,
  other_user_id uuid,
  last_message_at TIMESTAMP,
  last_message_content TEXT,
  PRIMARY KEY ((user_id), last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);
```