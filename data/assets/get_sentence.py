import spacy
import unicodedata
import sys
import duckdb
from datetime import datetime

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("spaCy en_core_web_sm model not found. Please install it with:")
    print("uv pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl")
    sys.exit(1)


def normalize_quotes(text):
    """Replace smart quotes with standard quotes and ensure proper escaping."""
    if text is None:
        return None
    text = unicodedata.normalize("NFKC", text)  # Normalize Unicode characters
    text = text.replace(""", '"').replace(""", '"')  # Convert smart double quotes
    text = text.replace("'", "'").replace("'", "'")  # Convert smart single quotes
    return text.strip()


def extract_sentences(conn, media_cloud_id, body):
    """Extract sentences containing 'opensecrets' and insert into stage_sentence."""
    longest_match = ""
    max_length = 0

    # Process text with spaCy
    doc = nlp(body)
    
    for sent in doc.sents:
        sentence_text = sent.text.strip()
        normalized_text = sentence_text.replace(".", "")
        
        open_secrets_match = False
        lower_text = normalized_text.lower()        
        if "opensecrets" in lower_text:
            open_secrets_match = True
        
        if open_secrets_match:
            if len(sentence_text) > max_length:
                max_length = len(sentence_text)
                longest_match = sentence_text

    # Always insert a record - with the longest matching sentence or blank if none found
    import_date = datetime.now()
    sentence_to_insert = normalize_quotes(longest_match) if longest_match else ""
    conn.execute(
        "INSERT INTO stage_sentence (media_cloud_id, import_date, sentence) VALUES (?, ?, ?)",
        [media_cloud_id, import_date, sentence_to_insert]
    )


def populate_stage_sentence():
    """Main function to populate the stage_sentence table."""
    db_path = "db/data.duckdb"
    conn = duckdb.connect(db_path)
    
    try:
        # Query all stories that haven't been processed yet
        result = conn.execute("""
            SELECT media_cloud_id, body 
            FROM story
            WHERE body <> '' 
            AND media_cloud_id NOT IN (SELECT media_cloud_id FROM stage_sentence)
        """).fetchall()
        
        total = len(result)
        print(f"Found {total} stories to process...")
        
        count = 0
        for row in result:
            extract_sentences(conn, row[0], row[1])  # media_cloud_id and body
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} of {total} stories...")
                    
        print(f"Complete! Processed {count} of {total} stories.")
        
    finally:
        conn.close()


if __name__ == "__main__":
    populate_stage_sentence()