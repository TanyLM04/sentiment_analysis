import re
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import nltk


# |---------------------> Download only once <---------------------|
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('wordnet')

def clean_text_es(text):
    if not isinstance(text, str):
        return ""

    # Lowercase
    text = text.lower()

    # Remove HTML tags
    text = BeautifulSoup(text, "html.parser").get_text()

    # Mask URLs emails and users (keep tokens, not the content)
    text = re.sub(r'\S+@\S+', 'EMAIL', text)
    text = re.sub(r'http\S+', 'ENLACE', text)                        # URLs → ENLACE
    text = re.sub(r'@\w+', 'USUARIO', text)                          # mentions → USUARIO
    text = re.sub(r'&amp;', '&', text)                               # HTML entities
    text = re.sub(r'&gt;', '', text)
    text = re.sub(r'&lt;', '', text)
    text = re.sub(r'&#\d+;', '', text)
    text = re.sub(r'\*+', '', text)  # strip all asterisks
    text = re.sub(r'_+', '', text)   # strip underscores (italic in markdown)             # bold/italic
    text = re.sub(r'\s+', ' ', text).strip()

    # Normalize repeated punctuation and letters (but keep the fact they are repeated)
    text = re.sub(r'(.)\1{3,}', r'\1\1\1', text)  # cap repetitions at 3

    # Keep printable chars (letters, punctuation, emojis)
    text = ''.join(ch for ch in text if ch.isprintable())

    # Strip extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text