# Fintech Analytics Warehouse (dbt + Postgres)

A complete end‑to‑end **Fintech Analytics & Marketing Attribution warehouse**, modeled using **dbt**, powered by a 
simulated **PostgreSQL** dataset generated via Faker.

This project mirrors the data foundations of modern consumer fintech apps like **Revolut, Chime, Kuda, CashApp**, with 
production‑style pipelines and analytics marts.

---

## 🚀 Project Overview

This repo simulates and models a full lifecycle fintech analytics stack:

* **User acquisition & onboarding funnel**
* **Daily activity & engagement tracking**
* **Transaction revenue modeling**
* **LTV, CAC, ROI, Payback, Profitability**
* **Marketing channel performance analysis**

All datasets flow through a structured analytics engineering pipeline:

1. **Staging Layer** – clean source tables (users, events, transactions, marketing)
2. **Intermediate Layer** – pivoted events, activity models, user-level transaction aggregates
3. **Marts Layer** – business-critical dashboards: cohorts, KPIs, LTV, trends, revenue, profitability
4. **Marketing Layer** – channel performance, ROI efficiency, attribution summaries

This is designed exactly like a real analytics warehouse you'd build at a top fintech.

---

## 🧱 Tech Stack

* **PostgreSQL** — data storage
* **dbt Core** — modeling, tests, documentation
* **Python (Faker)** — synthetic data generator
* **GitHub** — version control

---

## 📂 Project Structure

```bash
├── data_generation/
│   └── python scripts generating fake users, events, transactions, marketing spend
│
├── models/
│   ├── staging/
│   ├── intermediate/
│   │   ├── int_events_pivot.sql
│   │   ├── int_user_activity.sql
│   │   ├── int_user_txn_agg.sql
│   │   └── int_marketing_daily.sql
│   │
│   ├── marts/
│   │   ├── core/
│   │   │   ├── daily_kpis.sql
│   │   │   ├── user_funnel.sql
│   │   │   ├── cohorts.sql
│   │   │   ├── marketing_attribution.sql
│   │   │   └── user_value.sql
│   │   │
│   │   ├── finance/
│   │   │   ├── revenue_summary.sql
│   │   │   ├── customer_ltv.sql
│   │   │   ├── payback_recovery.sql
│   │   │   ├── revenue_trends.sql
│   │   │   └── profitability_dashboard.sql
│   │   │
│   │   ├── marketing/
│   │   │   ├── channel_performance.sql
│   │   │   └── roi_efficiency.sql
│   │
│   └── schema.yml files per directory
│
└── README.md
```

---

## 📊 Business Questions This Warehouse Answers

### **User Engagement & Retention**

* How many users sign up daily, weekly, monthly?
* What percentage complete KYC? Activate? Become power users?
* DAU trends across mobile (Android/iOS) and web?
* How long do users stay active after signup?

### **Marketing Performance**

* Which channels drive the cheapest signups & activations?
* What is CAC, CPA, and spend efficiency per channel?
* What’s the ROI per month and per acquisition source?

### **Revenue & Profitability**

* What’s the take rate across transaction flows?
* GMV per user, fees per user, transaction frequency?
* Which months/channels achieve ROI > 1.5x?
* Month-over-month revenue growth per channel?

### **LTV & Payback**

* 30‑day, 90‑day, and total LTV curves
* Payback ratios relative to CAC
* When each channel recovers its acquisition spend

---

## 🛠️ How to Run This Project

### **1. Install dbt core**

```bash
pip install dbt-postgres
```

### **2. Set up a Postgres profile**

Add to `~/.dbt/profiles.yml`:

```yaml
fintech_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: YOUR_USER
      password: YOUR_PASSWORD
      dbname: fintech
      schema: analytics
      port: 5432
```

### **3. Seed or load your fake data**

Run your python generator, then load the CSVs into Postgres or insert directly.

### **4. Run dbt models**

```bash
dbt run --project-dir .
```

### **5. Run tests**

```bash
dbt test
```

### **6. View dbt docs**

```bash
dbt docs generate
dbt docs serve
```

---

## 🧠 Key Analytics Logic Implemented

### 🚀 **User Funnel Model**

Signup → KYC Start → KYC Complete → Add Money → Activation

### 📈 **Daily KPIs Model**

* DAU
* New signups
* New KYC
* New activations
* GMV
* Fees
* Marketing spend
* GMV per active user
* Activation → signup conversion

### 💰 **LTV Models**

* 30‑day LTV
* 90‑day LTV
* Total LTV
* Revenue per user
* Days between first & last transaction

### 🔁 **Payback Recovery Model**

For every acquisition channel:

```
Payback Ratio = LTV / CAC
```

And 90‑day payback status.

### 📉 **Revenue Trends Model**

Month‑over‑month growth per channel:

* Revenue
* GMV
* Engagement span
* Active contributing customers

### 📊 **Profitability Dashboard**

* Revenue vs spend
* Net profit
* ROI
* Profit margin
* High‑ROI flag

### 📣 **Marketing Channel Performance**

* CAC
* CPA
* ROI
* Conversion rate (signup → activation)
* Revenue contribution

---

## 🧭 Roadmap / Future Improvements

* Add forecasting models (ARIMA / Prophet)
* Add customer segmentation (RFM, clustering)
* Add fraud modeling dataset
* Add BI dashboard templates (Looker/Metabase/Streamlit)
* Add incremental dbt models

---

## 👤 Author

**James Essiet**
Analytics Engineer / Fintech Data Lead
This project is part of an ongoing effort to build production‑grade analytics foundations from scratch.

---

If you're reviewing this repo and want to collaborate or provide feedback, feel free to reach out!
