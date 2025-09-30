import spacy
import unicodedata
import sys
import duckdb

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


def get_sentences(conn, id, body):
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

    # After checking all sentences, insert only the longest one if found
    if longest_match:
        conn.execute(
            "UPDATE story SET sentence = ? WHERE id = ?",
            [normalize_quotes(longest_match), id]
        )


def add_sentences():
    """Main function to add sentences to the story table."""
    db_path = "db/data.duckdb"
    conn = duckdb.connect(db_path)
    
    try:
        result = conn.execute("""
            SELECT id, body 
            FROM story
            WHERE body <> '' 
            AND sentence = ''
            AND body LIKE '%OpenSecrets%'
        """).fetchall()
        
        count = 0
        for row in result:
            get_sentences(conn, row[0], row[1])  # id and body
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} stories...")
                    
        print(f"Complete! Processed {count} total stories.")
        
    finally:
        conn.close()


if __name__ == "__main__":
    add_sentences()