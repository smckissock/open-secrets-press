# OpenSecrets Press

### Data
- Use MediaCloud API to access OpenSecrets press coverage
- Scrape articles written by OpenSecrets staff from OPenSecret site
- Use manually intered information about media outlets from a Google Sheet
- Use Newspaper3k to extract text, photo URL, etc.
- Use Dagster to get raw data, populate a star schema, and produce flat CSV for the website

### Web
- All data in gzipped csv
- Use d3.js / dc.js / crossfilter to render data
- Allow filtering by date, publication, author, publication type, right/left and tag
- Display publication, date, headline, author, photo, quote. Click to go to url 