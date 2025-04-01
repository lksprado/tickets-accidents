# TICKETS AND ACCIDENTS IN FEDERAL HIGHWAYS IN BRAZIL 2007-2024

This project aims to explore the relationship between traffic tickets and road accidents on Brazil’s federal highways. A common belief suggests that stricter enforcement of traffic laws—through fines and monitoring—leads to fewer accidents, as drivers tend to be more cautious. But does the data support this claim? Through descriptive analytics, we seek to uncover patterns and insights that shed light on this debate.

Brazil has 91 federal highways, covering approximately 68,000 km, patrolled by around 12,000 officers. The scale of this infrastructure makes it a compelling case study for analyzing how regulation impacts road safety.

source: https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf

### CHALLENGES & SOLUTIONS
- The traffic ticket datasets can reach 25GB, requiring an optimized approach to data processing. To overcome memory constraints, I leveraged Polars, a high-performance DataFrame library, along with memory-efficient techniques to process large CSV files without overwhelming RAM.
- The datasets varied in column structures, naming conventions, encodings, and delimiters, requiring careful preprocessing.
To ensure consistency, I first mapped all column names, standardized formats, and selected key attributes for analysis.