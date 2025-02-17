# DB-check-utility
RMS database health check CLI utility

## Overview

RMS Database Check Utility is a command-line tool designed to validate database consistency, integrity, and performance. It automates routine database health checks, helping developers and database administrators identify and resolve potential issues before they affect application performance.

Features

* Schema Validation: Ensures database schema matches expected structure.

* Data Consistency Checks: Detects missing or duplicate records.

* Index Analysis: Identifies missing or inefficient indexes.

* Query Performance Monitoring: Detects slow queries affecting database performance.

* Automated Reports: Generates detailed logs and reports for further analysis.

* Match Trade Data: Compares trade records across multiple databases to verify consistency.

* MTM and Quantity Matching: Validates trade quantities and mark-to-market values.

* User-wise Matching: Compares user-wise financial records for discrepancies.
### Screenshots of outputs
1) Entering date for which to match trades
![Date input](outputs_screenshots/date.PNG)
2) Utility menu options page
![Utility menu](outputs_screenshots/menu.PNG)
3) Match Count option
![Count trades](outputs_screenshots/count.PNG)
4) Match Quantity option
![Count trades](outputs_screenshots/quantity.PNG)
5) Matching Mark to Market (MTM) - When it is matched
![Count trades](outputs_screenshots/mtm-matched.PNG)
6) Matching Mark to Market (MTM) - When it is unmatched
![Count trades](outputs_screenshots/mtm-unmatched.PNG)
7) Matching Mark to Market (MTM) for a userid (omitted userid) - When it is matched
![Count trades](outputs_screenshots/mtm-userid-matched-1.PNG)
![Count trades](outputs_screenshots/mtm-userid-matched-2.PNG)
8) Matching Mark to Market (MTM) for a userid (omitted userid) - When it is unmatched
![Count trades](outputs_screenshots/mtm-userid-unmatched-1.PNG)
![Count trades](outputs_screenshots/mtm-userid-unmatched-2.PNG)


