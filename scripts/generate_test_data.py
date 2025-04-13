"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import random
import logging
from cassandra.cluster import Cluster
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def tables_exist(session):
    """Verify the presence of required tables in the keyspace."""
    logger.info("Validating required tables...")

    required_tables = ['users', 'messages', 'messages_by_user', 'conversations', 'conversations_by_user']
    keyspace_metadata = session.cluster.metadata.keyspaces[CASSANDRA_KEYSPACE]
    existing_tables = keyspace_metadata.tables.keys()

    missing = [table for table in required_tables if table not in existing_tables]

    if missing:
        logger.info(f"Missing tables detected: {', '.join(missing)}")
        return False

    logger.info("All necessary tables are present.")
    return True

def create_tables(session):
    """Define and create all necessary tables if not already existing."""
    logger.info("Creating necessary tables...")

    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id uuid,
            username text,
            created_at timestamp,
            PRIMARY KEY (user_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            conversation_id int,
            timestamp timestamp,
            message_id uuid,
            sender_id uuid,
            receiver_id uuid,
            content text,
            PRIMARY KEY (conversation_id, timestamp, message_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS messages_by_user (
            user_id uuid,
            conversation_id int,
            timestamp timestamp,
            message_id uuid,
            sender_id uuid,
            receiver_id uuid,
            content text,
            PRIMARY KEY ((user_id), conversation_id, timestamp, message_id)
        ) WITH CLUSTERING ORDER BY (conversation_id ASC, timestamp DESC, message_id ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations_by_user (
            user_id uuid,
            conversation_id int,
            other_user_id uuid,
            last_message_at timestamp,
            last_message_content text,
            PRIMARY KEY (user_id, last_message_at, conversation_id)
        ) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            conversation_id int,
            user1_id uuid,
            user2_id uuid,
            created_at timestamp,
            last_message_at timestamp,
            last_message_content text,
            PRIMARY KEY (conversation_id)
        )
    """)

    logger.info("Table creation completed.")

def generate_test_data(session):
    """Populate Cassandra with sample users, conversations, and messages."""
    logger.info("Starting test data generation...")

    # Insert sample users
    logger.info("Generating users...")
    user_ids = []
    for i in range(1, NUM_USERS + 1):
        user_id = uuid.uuid4()
        user_ids.append(user_id)
        username = f"user{i}"
        created_at = datetime.utcnow() - timedelta(days=random.randint(1, 30))

        session.execute(
            """
            INSERT INTO users (user_id, username, created_at)
            VALUES (%s, %s, %s)
            """,
            (user_id, username, created_at)
        )

    # Insert conversations
    logger.info("Creating conversations...")
    conversations = []
    for conv_id in range(1, NUM_CONVERSATIONS + 1):
        user1_id, user2_id = random.sample(user_ids, 2)
        created_at = datetime.utcnow() - timedelta(days=random.randint(1, 20))
        last_message_at = created_at

        session.execute(
            """
            INSERT INTO conversations
            (conversation_id, user1_id, user2_id, created_at, last_message_at, last_message_content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (conv_id, user1_id, user2_id, created_at, last_message_at, None)
        )

        conversations.append((conv_id, user1_id, user2_id, created_at))

    # Insert messages
    logger.info("Generating messages for conversations...")
    sample_messages = [
        "Hey, how are you?",
        "What's up?",
        "Can we meet tomorrow?",
        "I'm busy right now",
        "Let's catch up soon",
        "Did you see that movie?",
        "Have you done the assignment?",
        "I'll call you later",
        "Thanks for your help!",
        "Congratulations!"
    ]

    for conv_id, user1_id, user2_id, base_time in conversations:
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        current_time = base_time
        latest_message = None

        for i in range(num_messages):
            current_time += timedelta(minutes=random.randint(1, 60))
            sender_id = user1_id if i % 2 == 0 else user2_id
            receiver_id = user2_id if i % 2 == 0 else user1_id
            content = random.choice(sample_messages)
            latest_message = content
            message_id = uuid.uuid4()

            session.execute(
                """
                INSERT INTO messages
                (conversation_id, timestamp, message_id, sender_id, receiver_id, content)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conv_id, current_time, message_id, sender_id, receiver_id, content)
            )

            for user_id in [sender_id, receiver_id]:
                session.execute(
                    """
                    INSERT INTO messages_by_user
                    (user_id, conversation_id, timestamp, message_id, sender_id, receiver_id, content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (user_id, conv_id, current_time, message_id, sender_id, receiver_id, content)
                )

        # Update conversation metadata
        session.execute(
            """
            UPDATE conversations
            SET last_message_at = %s, last_message_content = %s
            WHERE conversation_id = %s
            """,
            (current_time, latest_message, conv_id)
        )

        for user_id in [user1_id, user2_id]:
            other_user = user2_id if user_id == user1_id else user1_id
            session.execute(
                """
                INSERT INTO conversations_by_user
                (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, conv_id, other_user, current_time, latest_message)
            )

    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with sample messages.")
    logger.info("User creation completed with UUIDs. Use them for API testing.")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        if not tables_exist(session):
            create_tables(session)
        generate_test_data(session)
        logger.info("Test data generation completed successfully.")
    except Exception as e:
        logger.error(f"Error during test data generation: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 