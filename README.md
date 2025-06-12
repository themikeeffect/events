# 📊 Growth Analytics Stack

## Overview

This project analyzes user behavior and growth using event data. It builds foundational tables (`dim_users`, `fct_events`) and tracks key growth metrics like:

- Active, New, Retained, Resurrected, and Churned users
- Daily, Weekly, Monthly cohort retention
- Triangle retention visualizations
- Transaction type classification (Engagement vs. Miles)

# 📂 Stack & Tools

| Layer          | Tool          | Purpose                              |
| -------------- | ------------- | ------------------------------------ |
| Warehouse      | BigQuery      | Stores raw + transformed event data  |
| Transformation | dbt           | Cleans, models, and builds metrics   |
| Dashboard      | Google Looker | Visualizes growth & retention trends |

## 📁 Folder Structure

```
├── models/
│   ├── calendar
│   ├── dim_users
│   ├── fct_events
│   ├── growth_accounting_daily
│   ├── growth_accounting_weekly
│   ├── growth_accounting_monthly
│   ├── triangle_retention_daily
│   ├── triangle_retention_weekly
│   ├── triangle_retention_monthly
├── seeds/
├── dbt_project.yml
├── README.md
└── schema.yml
```

## 🧪 Metrics Tracked

- **Active Users**: Users with at least 1 event in a period
- **New Users**: First-time seen in current period
- **Retained**: Active both current and previous period
- **Resurrected**: Inactive previous, but active current period
- **Churned**: Was active previously, but not anymore
- **Triangle Retention**: % of cohort returning after N periods

## 📊 Dashboard : User Events Dashboard
The following are the contents of the dashboard:
- **Growth Accounting**: Shows user status by day/week/month
- **Cohort Retention**: Triangle heatmaps by cohort start
- **Transaction Breakdown**: Miles vs Engagement vs Mixed

## Dashboard screenshot is here: https://github.com/themikeeffect/events/tree/event_analytics/snapshots
## Live Dashboard is here: https://lookerstudio.google.com/reporting/36fa1101-ad0a-465f-9cb5-eefba3059b36

📣 Contributors
	•	Michael Minorca — Design, modeling, analytics 

