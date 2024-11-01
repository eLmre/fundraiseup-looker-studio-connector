FundraiseUp REST API - Looker Studio Connector
=============
This repository contains a [community connector](https://developers.google.com/looker-studio/connector "community connector") script to fetch donations data from FundraiseUp and visualize it in Looker Studio using Google Apps Script.

For a detailed walkthrough of FundraiseUp REST API, check out [documentation](https://fundraiseup.com/docs/rest-api/ "documentation").

Features
-------------
- Fetch dynamic donations data based on API key input.
- No authentication required.
- Seamlessly integrates with Looker Studio for data visualization.
- Stable works with scheduled email delivery.

Usage
-------------
#### Setup in Google Apps Script
- Create a new [Google Apps Script](https://script.google.com/ "Google Apps Script") project.
- Copy and paste the provided script into the editor.
- Save and deploy the script as a web app.

#### Integration in Looker Studio
- Use the deployed script URL as the data source in Looker Studio.
- To obtain API key open [FundraiseUp Dashboard](https://dashboard.fundraiseup.com/ "FundraiseUp Dashboard") > Settings > API keys and click Create API key.
- Configure the data source by specifying the desired API key.
- Visualize the fetched data using Looker Studio tools.

Schema
-------------
Supports 100+ fields declared in documentation. Do not support ```questions``` field.

Example
-------------
An example of data visualize by Looker Studio report.
![](https://iili.io/2nSwI2f.png)

License
-------------
This is an unofficial project made for educational purpose. Licensed under the MIT License. See the LICENSE file for more details.
