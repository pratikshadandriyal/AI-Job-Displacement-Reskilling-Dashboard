# 🤖 AI Job Displacement & Reskilling Dashboard

![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)



---

## 📌 Overview
An end-to-end data analytics project analyzing the impact of AI on job roles, salaries, and reskilling urgency across 9 countries and 8 industries from **2020–2026**.

---

## 🔄 Project Workflow
```
Clean CSV → Python (dirty data) → SQL Server (cleaning) → Power BI (dashboard)
```

---

## 🛠️ Tools Used
| Tool | Purpose |
|---|---|
| Python (Pandas) | Generating dirty dataset |
| SQL Server (SSMS) | Data storage & cleaning |
| Power BI + DAX | Dashboard & KPIs |
| Power Query | Data transformation |

---

## 🧹 Data Cleaning
**Issues introduced (Python):** NULLs, duplicates, inconsistent casing, outliers, invalid values, wrong data types

**Fixed in SQL:** Removed duplicates, standardized casing, fixed invalid categories, added `salary_trend` and `education_label` columns

**Fixed in Power BI:** Rounded decimals, removed outliers, corrected data types

---

## 📊 Dashboard Pages

| Page | Title | Key Visuals |
|---|---|---|
| 1 | Overview | KPI Cards, Line Charts |
| 2 | AI Risk Analysis | Column Chart, Stacked Bar, Table, Line |
| 3 | Salary Impact | Clustered Bar, Donut, Treemap, Column |
| 4 | Skills & Reskilling | Bar Charts, Line, Donut |

---

## 💡 Key Insights
- **27.78%** of jobs are at high automation risk
- **Energy & Finance** have the highest AI disruption intensity
- Average salary **increased by ~$3,500** after AI adoption
- **Teachers & Truck Drivers** have the highest skill gap index
- **42.65%** of workers saw salary growth post-AI adoption

---

## 📁 Repository Structure
```
├── ai_job_replacement_2020_2026_v2.csv
│── ai_jobs_dirty.csv
├── make_data_dirty.py
├── cleaning_queries.sql
├── ai_job_project.pbix 
├── screenshots/
└── README.md
```

---

## 👤 Author

Pratiksha Dandriyal
Data Analyst  
[LinkedIn Profile](http://www.linkedin.com/in/pratikshadandriyal) | [GitHub Profile] (https://github.com/pratikshadandriyal)
