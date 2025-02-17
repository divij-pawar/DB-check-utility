# DB-check-utility
CLI Health check utility for Risk Management System database 

## Overview

RMS Database Check Utility is a command-line tool designed to validate database consistency and integrity. It automates routine database health checks, helping developers and database administrators identify and resolve potential issues before they affect application performance. <br>
This utility fast-tracks End-Of-Day procedures.

## Features

* Ensures database schema matches expected structure.

* Detects missing or duplicate records.

* Index Analysis: Identifies missing or inefficient indexes.

* Generates detailed logs and reports for further analysis.

* Compares trade records across multiple databases to verify consistency.

* Validates trade quantities and mark-to-market values.

* Compares user-wise records for discrepancies.
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
