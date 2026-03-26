import pandas as pd
from utils.clean_text import clean_text
from tqdm import tqdm

tqdm.pandas()

# Load CSV file
df = pd.read_csv('data/spanish_pushshift.csv')

# Clean the email text
print("Cleaning email text")
print(df.columns)
df['clean_text'] = df['Email Text'].progress_apply(clean_text)

# Save cleaned data to new CSV file
df.to_csv('data/cleaned_pushshift.csv', index=False)
print("\nCleaned dataset saved")